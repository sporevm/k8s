package agenthttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sporevm/k8s/internal/agent"
	"github.com/sporevm/k8s/internal/fleet"
)

// Client calls a node-local agent HTTP API.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

// Status fetches compact agent status.
func (c Client) Status(ctx context.Context) (fleet.AgentStatus, error) {
	var status fleet.AgentStatus
	if err := c.getJSON(ctx, "/status", &status); err != nil {
		return fleet.AgentStatus{}, err
	}
	return status, status.Validate()
}

// InspectRunBundle validates immutable bundle metadata through an agent.
func (c Client) InspectRunBundle(ctx context.Context, run fleet.BundleRun) (fleet.BundleInspection, error) {
	var inspection fleet.BundleInspection
	if err := c.postJSON(ctx, "/inspect-run-bundle", run, &inspection); err != nil {
		return fleet.BundleInspection{}, err
	}
	return inspection, nil
}

// PrepareBundle prepares, forks, packs, and inspects a source run on the agent.
func (c Client) PrepareBundle(ctx context.Context, run fleet.Run) (fleet.PreparedBundle, error) {
	var prepared fleet.PreparedBundle
	if err := c.postJSON(ctx, "/prepare-bundle", run, &prepared); err != nil {
		return fleet.PreparedBundle{}, err
	}
	if _, err := run.Compile(prepared); err != nil {
		return fleet.PreparedBundle{}, err
	}
	return prepared, nil
}

// PrepareLocal prepares and forks a source run on the agent without creating a portable bundle.
func (c Client) PrepareLocal(ctx context.Context, run fleet.Run) (fleet.PreparedBundle, error) {
	var prepared fleet.PreparedBundle
	if err := c.postJSON(ctx, "/prepare-local", run, &prepared); err != nil {
		return fleet.PreparedBundle{}, err
	}
	if _, err := run.Compile(prepared); err != nil {
		return fleet.PreparedBundle{}, err
	}
	return prepared, nil
}

// CreateSandbox starts one named sandbox on the agent.
func (c Client) CreateSandbox(ctx context.Context, req agent.CreateVMRequest) error {
	var response struct {
		Name string `json:"name"`
	}
	return c.postJSON(ctx, "/sandboxes", req, &response)
}

// ExecSandbox runs one command inside a named sandbox.
func (c Client) ExecSandbox(ctx context.Context, name string, command []string) ([]agent.RunEvent, error) {
	var events []agent.RunEvent
	if err := c.postJSON(ctx, "/sandboxes/"+url.PathEscape(name)+"/exec", agent.ExecRequest{Command: command}, &events); err != nil {
		return nil, err
	}
	return events, nil
}

// RemoveSandbox deletes one named sandbox.
func (c Client) RemoveSandbox(ctx context.Context, name string) error {
	var response struct {
		Name string `json:"name"`
	}
	return c.deleteJSON(ctx, "/sandboxes/"+url.PathEscape(name), &response)
}

// RunShard executes one shard lease on the agent.
func (c Client) RunShard(ctx context.Context, req fleet.ShardExecutionRequest) ([]fleet.AttemptResult, error) {
	var results []fleet.AttemptResult
	if err := c.postJSON(ctx, "/run-shard", req, &results); err != nil {
		return nil, err
	}
	for _, result := range results {
		if err := result.Validate(req.Run); err != nil {
			return nil, err
		}
	}
	return results, nil
}

func (c Client) getJSON(ctx context.Context, path string, out any) error {
	endpoint, err := c.endpoint(path)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	return c.do(req, out)
}

func (c Client) postJSON(ctx context.Context, path string, in any, out any) error {
	endpoint, err := c.endpoint(path)
	if err != nil {
		return err
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(in); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &body)
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	return c.do(req, out)
}

func (c Client) deleteJSON(ctx context.Context, path string, out any) error {
	endpoint, err := c.endpoint(path)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}
	return c.do(req, out)
}

func (c Client) endpoint(path string) (string, error) {
	if c.BaseURL == "" {
		return "", errors.New("agent base URL is required")
	}
	base, err := url.Parse(c.BaseURL)
	if err != nil {
		return "", err
	}
	if base.Scheme == "" || base.Host == "" {
		return "", fmt.Errorf("agent base URL %q must include scheme and host", c.BaseURL)
	}
	base.Path = strings.TrimRight(base.Path, "/") + path
	return base.String(), nil
}

func (c Client) do(req *http.Request, out any) error {
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return decodeResponseError(resp)
	}
	decoder := json.NewDecoder(resp.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var extra struct{}
	if err := decoder.Decode(&extra); err == nil {
		return errors.New("agent response contained multiple JSON documents")
	} else if !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func decodeResponseError(resp *http.Response) error {
	var body struct {
		Error string `json:"error"`
	}
	payload, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return readErr
	}
	if err := json.Unmarshal(payload, &body); err == nil && body.Error != "" {
		return fmt.Errorf("agent returned %s: %s", resp.Status, body.Error)
	}
	return fmt.Errorf("agent returned %s: %s", resp.Status, strings.TrimSpace(string(payload)))
}
