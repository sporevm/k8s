package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sporevm/k8s/internal/agent"
	"github.com/sporevm/k8s/internal/agenthttp"
	"github.com/sporevm/k8s/internal/fleet"
)

const testBundleDigest = "sha256:2222222222222222222222222222222222222222222222222222222222222222"

func TestRunCoordinatorGenericRunPreparesAndExecutesOnSameAgent(t *testing.T) {
	generic := testGenericRun()
	runPath := writeJSONFile(t, generic)
	spore := &fakeSporeClient{digest: testBundleDigest, childCount: 1}
	server := newTestAgentServer(t, spore, 1)

	var stdout bytes.Buffer
	err := runCoordinator(context.Background(), coordinatorConfig{
		GenericRunPath:  runPath,
		ResultStoreRoot: t.TempDir(),
		Timeout:         time.Minute,
		AgentURLs:       agentURLsFlag{server.URL},
		HTTPClient:      server.Client(),
	}, &stdout)
	if err != nil {
		t.Fatalf("runCoordinator: %v\nstdout=%s", err, stdout.String())
	}

	var report fleet.RuntimeReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode report: %v\n%s", err, stdout.String())
	}
	if report.Summary.State != "succeeded" || report.Summary.CompletedChildren != 1 {
		t.Fatalf("summary = %+v", report.Summary)
	}
	if len(report.Plan.Leases) != 1 || report.Plan.Leases[0].AgentID != "spore-agent-test-0001" {
		t.Fatalf("leases = %+v", report.Plan.Leases)
	}

	if got := spore.runCaptureCount(); got != 1 {
		t.Fatalf("run capture count = %d, want 1", got)
	}
	pulls := spore.pullRequests()
	if len(pulls) != 1 {
		t.Fatalf("pull count = %d, want 1", len(pulls))
	}
	if !strings.HasPrefix(pulls[0].Source, "file://") {
		t.Fatalf("pull source = %q, want local prepared file bundle", pulls[0].Source)
	}
}

func TestRunCoordinatorGenericRunReturnsErrorForFailedRuntimeReport(t *testing.T) {
	generic := testGenericRun()
	runPath := writeJSONFile(t, generic)
	spore := &fakeSporeClient{digest: testBundleDigest, childCount: 1, resumeExitCode: 1}
	server := newTestAgentServer(t, spore, 1)

	var stdout bytes.Buffer
	err := runCoordinator(context.Background(), coordinatorConfig{
		GenericRunPath:  runPath,
		ResultStoreRoot: t.TempDir(),
		Timeout:         time.Minute,
		AgentURLs:       agentURLsFlag{server.URL},
		HTTPClient:      server.Client(),
	}, &stdout)
	if err == nil {
		t.Fatalf("runCoordinator succeeded; stdout=%s", stdout.String())
	}
	if !strings.Contains(err.Error(), "runtime report failed") {
		t.Fatalf("runCoordinator error = %v, want runtime report failure", err)
	}

	var report fleet.RuntimeReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode report: %v\n%s", err, stdout.String())
	}
	if report.Summary.State != "failed" || report.Summary.FailedChildren != 1 {
		t.Fatalf("summary = %+v", report.Summary)
	}
}

func TestRunCoordinatorGenericRunCapacityErrorDoesNotWriteEmptyReport(t *testing.T) {
	generic := testGenericRun()
	generic.Fork.Count = 2
	generic.Children.Count = 2
	runPath := writeJSONFile(t, generic)
	server := newTestAgentServer(t, &fakeSporeClient{digest: testBundleDigest, childCount: 2}, 1)

	var stdout bytes.Buffer
	err := runCoordinator(context.Background(), coordinatorConfig{
		GenericRunPath:  runPath,
		ResultStoreRoot: t.TempDir(),
		Timeout:         time.Minute,
		AgentURLs:       agentURLsFlag{server.URL},
		HTTPClient:      server.Client(),
	}, &stdout)
	if !errors.Is(err, fleet.ErrInsufficientCapacity) {
		t.Fatalf("runCoordinator error = %v, want ErrInsufficientCapacity", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty on pre-run error", stdout.String())
	}
}

func TestSelectGenericPrepareEndpointRequiresSingleAgentCapacity(t *testing.T) {
	generic := testGenericRun()
	generic.Children.Count = 2
	generic.Fork.Count = 2
	endpoint := agentEndpoint{Status: testAgentStatus()}
	endpoint.Status.ExecutionSlots.Available = 1

	_, err := selectGenericPrepareEndpoint(generic, []agentEndpoint{endpoint})
	if !errors.Is(err, fleet.ErrInsufficientCapacity) {
		t.Fatalf("selectGenericPrepareEndpoint error = %v, want ErrInsufficientCapacity", err)
	}
}

func newTestAgentServer(t *testing.T, spore *fakeSporeClient, slots int) *httptest.Server {
	t.Helper()
	store, err := agent.NewLocalResultStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalResultStore: %v", err)
	}
	runner, err := agent.NewRunner(
		slots,
		agent.WithSporeClient(spore),
		agent.WithResultStore(store),
		agent.WithWorkRoot(t.TempDir()),
	)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	handler, err := (&agenthttp.Server{
		Runner:  runner,
		Client:  spore,
		AgentID: "spore-agent-test-0001",
		CellID:  "test-cell-0001",
		Region:  "us-east-1",
		Backend: "kvm",
		Now:     func() time.Time { return time.Date(2026, 6, 24, 8, 0, 0, 0, time.UTC) },
	}).Handler()
	if err != nil {
		t.Fatalf("Handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func writeJSONFile(t *testing.T, value any) string {
	t.Helper()
	path := t.TempDir() + "/run.json"
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("write json: %v", err)
	}
	return path
}

func testGenericRun() fleet.GenericRun {
	return fleet.GenericRun{
		RunID: "rails-rspec-20260624",
		Source: fleet.GenericSource{
			Image:    "example.com/sporevm/rails-rspec:sha-1111111",
			Platform: "linux/arm64",
		},
		Prepare: fleet.PrepareSpec{
			Command:       []string{"/bin/bash", "/usr/local/bin/sporevm-rails-coordinator", "--capture-delay", "2"},
			CaptureSignal: "USR1",
			ReadyMarker:   "SPOREVM_RAILS_READY",
		},
		Fork: fleet.ForkSpec{Count: 1},
		Children: fleet.GenericChildren{
			Start:   0,
			Count:   1,
			Command: []string{"/usr/local/bin/sporevm-rspec-shard"},
		},
		Execution:   fleet.Execution{ChildrenPerShard: 1, MaxInFlightPerAgent: 1},
		RetryPolicy: fleet.RetryPolicy{MaxAttemptsPerChild: 1},
		SideEffects: fleet.SideEffects{IdempotencyRequired: true},
		ResultStore: "s3://example-sporevm-results/rails-rspec-20260624/",
	}
}

func testAgentStatus() fleet.AgentStatus {
	return fleet.AgentStatus{
		AgentID:    "spore-agent-test-0001",
		CellID:     "test-cell-0001",
		ObservedAt: time.Date(2026, 6, 24, 8, 0, 0, 0, time.UTC),
		HostClass: fleet.HostClass{
			ID:                   "linux-aarch64-kvm-graviton1-v0",
			SporePlatformVersion: "v0",
			Architecture:         "aarch64",
			Backend:              "kvm",
			CPUProfile:           "graviton1",
			DeviceModel:          "sporevm-aarch64-v0",
		},
		ExecutionSlots: fleet.ExecutionSlots{Total: 1, Available: 1},
		Pressure:       fleet.Pressure{Disk: fleet.PressureNormal, Memory: fleet.PressureNormal},
		Healthy:        true,
	}
}

type fakeSporeClient struct {
	mu             sync.Mutex
	digest         string
	childCount     int
	resumeExitCode int
	runCaptures    []agent.RunCaptureRequest
	pulls          []agent.PullRequest
}

func (c *fakeSporeClient) HostInfo(context.Context) (agent.HostInfo, error) {
	return agent.HostInfo{
		Schema:        "spore.host-info.v1",
		SchemaVersion: 1,
		HostClass:     "linux-aarch64-kvm",
		Platform: agent.PlatformFacts{
			OS:                 "linux",
			Arch:               "aarch64",
			CPUProfile:         "graviton1",
			DeviceModelVersion: 0,
		},
		Backends: []agent.BackendAvailability{{
			Name:      "kvm",
			Supported: true,
			Available: true,
		}},
	}, nil
}

func (c *fakeSporeClient) InspectBundle(context.Context, agent.InspectBundleRequest) (agent.InspectBundleResult, error) {
	childCount := c.childCount
	if childCount == 0 {
		childCount = 1
	}
	return agent.InspectBundleResult{
		Schema:        "spore.bundle.inspect.v1",
		SchemaVersion: 1,
		BundleDigest:  testDigest(c.digest),
		ChildCount:    childCount,
	}, nil
}

func (c *fakeSporeClient) RunCapture(_ context.Context, req agent.RunCaptureRequest) ([]agent.RunEvent, error) {
	c.mu.Lock()
	c.runCaptures = append(c.runCaptures, req)
	c.mu.Unlock()
	exitCode := 0
	path := req.CaptureDir
	return []agent.RunEvent{{
		Schema:        "spore.run-events.v1",
		SchemaVersion: 1,
		Event:         "exit",
		Command:       "run",
		ExitCode:      &exitCode,
		Captured:      true,
		CapturePath:   &path,
	}}, nil
}

func (c *fakeSporeClient) Fork(context.Context, agent.ForkRequest) error {
	return nil
}

func (c *fakeSporeClient) Pack(context.Context, agent.PackRequest) error {
	return nil
}

func (c *fakeSporeClient) Pull(_ context.Context, req agent.PullRequest) (agent.PullResult, error) {
	c.mu.Lock()
	c.pulls = append(c.pulls, req)
	c.mu.Unlock()
	return agent.PullResult{
		Schema:        "spore.pull.result.v1",
		SchemaVersion: 1,
		OutDir:        req.OutDir,
		BundleDigest:  testDigest(c.digest),
	}, nil
}

func (c *fakeSporeClient) Resume(context.Context, agent.ResumeRequest) ([]agent.RunEvent, error) {
	exitCode := c.resumeExitCode
	return []agent.RunEvent{{
		Schema:        "spore.run-events.v1",
		SchemaVersion: 1,
		Event:         "exit",
		Command:       "resume",
		ExitCode:      &exitCode,
		Timings:       &agent.RunEventTimings{ExecResponseMS: 7},
	}}, nil
}

func (c *fakeSporeClient) runCaptureCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.runCaptures)
}

func (c *fakeSporeClient) pullRequests() []agent.PullRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]agent.PullRequest(nil), c.pulls...)
}

func testDigest(raw string) agent.DigestRef {
	return agent.DigestRef{
		Algorithm: "sha256",
		Hex:       raw[len("sha256:"):],
	}
}
