package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sporevm/k8s/internal/agent"
	"github.com/sporevm/k8s/internal/agenthttp"
	"github.com/sporevm/k8s/internal/fleet"
)

const testBundleDigest = "sha256:2222222222222222222222222222222222222222222222222222222222222222"

func TestRunCoordinatorRunPreparesAndExecutesOnSameAgent(t *testing.T) {
	source := testRun()
	runPath := writeJSONFile(t, source)
	spore := &fakeSporeClient{digest: testBundleDigest, childCount: 1}
	server := newTestAgentServer(t, spore, 1)

	var stdout bytes.Buffer
	err := runCoordinator(context.Background(), coordinatorConfig{
		RunPath:         runPath,
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
	if report.Prepare == nil || report.Prepare.TimingsMS.RunSave < 0 || report.Prepare.TimingsMS.Pack < 0 {
		t.Fatalf("prepare summary = %+v", report.Prepare)
	}

	if got := spore.runCaptureCount(); got != 1 {
		t.Fatalf("run capture count = %d, want 1", got)
	}
	if pulls := spore.pullRequests(); len(pulls) != 0 {
		t.Fatalf("pulls = %+v, want prepared local child fast path", pulls)
	}
}

func TestCoordinatorAPIExecutesRun(t *testing.T) {
	source := testRun()
	spore := &fakeSporeClient{digest: testBundleDigest, childCount: 1}
	agentServer := newTestAgentServer(t, spore, 1)
	handler, err := coordinatorHandler(coordinatorConfig{
		ResultStoreRoot: t.TempDir(),
		Timeout:         time.Minute,
		AgentURLs:       agentURLsFlag{agentServer.URL},
		HTTPClient:      agentServer.Client(),
	})
	if err != nil {
		t.Fatalf("coordinatorHandler: %v", err)
	}

	payload, err := json.Marshal(source)
	if err != nil {
		t.Fatalf("marshal source run: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/runs", bytes.NewReader(payload))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}

	var report fleet.RuntimeReport
	if err := json.Unmarshal(rec.Body.Bytes(), &report); err != nil {
		t.Fatalf("decode report: %v\n%s", err, rec.Body.String())
	}
	if report.Summary.State != "succeeded" || report.Summary.CompletedChildren != 1 {
		t.Fatalf("summary = %+v", report.Summary)
	}
	if got := spore.runCaptureCount(); got != 1 {
		t.Fatalf("run capture count = %d, want 1", got)
	}
}

func TestCoordinatorAPIProxiesSandbox(t *testing.T) {
	spore := &fakeSporeClient{digest: testBundleDigest, childCount: 1}
	agentServer := newTestAgentServer(t, spore, 1)
	handler, err := coordinatorHandler(coordinatorConfig{
		ResultStoreRoot: t.TempDir(),
		Timeout:         time.Minute,
		AgentURLs:       agentURLsFlag{agentServer.URL},
		HTTPClient:      agentServer.Client(),
	})
	if err != nil {
		t.Fatalf("coordinatorHandler: %v", err)
	}

	createPayload, err := json.Marshal(agent.CreateVMRequest{
		Name:    "sporevm-sandbox-node",
		Image:   "docker.io/library/node:22-bookworm-slim",
		Command: []string{"/bin/sh", "-lc", "node -v >/dev/null"},
	})
	if err != nil {
		t.Fatalf("marshal create request: %v", err)
	}
	createReq := httptest.NewRequest(http.MethodPost, "/sandboxes", bytes.NewReader(createPayload))
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body=%s", createRec.Code, createRec.Body.String())
	}

	execPayload, err := json.Marshal(agent.ExecRequest{Command: []string{"/bin/sh", "-lc", "node -v"}})
	if err != nil {
		t.Fatalf("marshal exec request: %v", err)
	}
	execReq := httptest.NewRequest(http.MethodPost, "/sandboxes/sporevm-sandbox-node/exec", bytes.NewReader(execPayload))
	execRec := httptest.NewRecorder()
	handler.ServeHTTP(execRec, execReq)
	if execRec.Code != http.StatusOK {
		t.Fatalf("exec status = %d, body=%s", execRec.Code, execRec.Body.String())
	}
	var events []agent.RunEvent
	if err := json.Unmarshal(execRec.Body.Bytes(), &events); err != nil {
		t.Fatalf("decode exec events: %v", err)
	}
	terminal, err := agent.TerminalEvent(events)
	if err != nil {
		t.Fatalf("TerminalEvent: %v", err)
	}
	if terminal.ExitCode == nil || *terminal.ExitCode != 0 {
		t.Fatalf("terminal = %+v", terminal)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/sandboxes/sporevm-sandbox-node", nil)
	deleteRec := httptest.NewRecorder()
	handler.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body=%s", deleteRec.Code, deleteRec.Body.String())
	}
}

func TestRunCoordinatorRunExecutesSingleAgentSequentially(t *testing.T) {
	source := testRun()
	source.Fork.Count = 2
	source.Children.Count = 2
	source.Execution.ChildrenPerShard = 1
	source.Execution.MaxInFlightPerAgent = 1
	runPath := writeJSONFile(t, source)
	spore := &fakeSporeClient{digest: testBundleDigest, childCount: 2}
	server := newTestAgentServer(t, spore, 1)

	var stdout bytes.Buffer
	err := runCoordinator(context.Background(), coordinatorConfig{
		RunPath:         runPath,
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
	if report.Summary.State != "succeeded" || report.Summary.CompletedChildren != 2 {
		t.Fatalf("summary = %+v", report.Summary)
	}
	if len(report.Plan.Leases) != 1 || report.Plan.Leases[0].ChildCount != 2 {
		t.Fatalf("leases = %+v", report.Plan.Leases)
	}
	if pulls := spore.pullRequests(); len(pulls) != 0 {
		t.Fatalf("pulls = %+v, want prepared local child fast path", pulls)
	}
}

func TestRunCoordinatorRunReturnsErrorForFailedRuntimeReport(t *testing.T) {
	source := testRun()
	runPath := writeJSONFile(t, source)
	spore := &fakeSporeClient{digest: testBundleDigest, childCount: 1, execExitCode: 1}
	server := newTestAgentServer(t, spore, 1)

	var stdout bytes.Buffer
	err := runCoordinator(context.Background(), coordinatorConfig{
		RunPath:         runPath,
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

func TestRunCoordinatorRunCapacityErrorDoesNotWriteEmptyReport(t *testing.T) {
	source := testRun()
	source.Fork.Count = 2
	source.Children.Count = 2
	source.Execution.ChildrenPerShard = 2
	source.Execution.MaxInFlightPerAgent = 2
	runPath := writeJSONFile(t, source)
	server := newTestAgentServer(t, &fakeSporeClient{digest: testBundleDigest, childCount: 2}, 1)

	var stdout bytes.Buffer
	err := runCoordinator(context.Background(), coordinatorConfig{
		RunPath:         runPath,
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

func TestSelectPrepareEndpointRequiresSingleAgentInFlightCapacity(t *testing.T) {
	source := testRun()
	source.Children.Count = 2
	source.Fork.Count = 2
	source.Execution.ChildrenPerShard = 2
	source.Execution.MaxInFlightPerAgent = 2
	endpoint := agentEndpoint{Status: testAgentStatus()}
	endpoint.Status.ExecutionSlots.Available = 1

	_, err := selectPrepareEndpoint(source, []agentEndpoint{endpoint})
	if !errors.Is(err, fleet.ErrInsufficientCapacity) {
		t.Fatalf("selectPrepareEndpoint error = %v, want ErrInsufficientCapacity", err)
	}
}

func TestSelectPrepareEndpointAllowsSequentialCapacity(t *testing.T) {
	source := testRun()
	source.Children.Count = 2
	source.Fork.Count = 2
	source.Execution.ChildrenPerShard = 1
	source.Execution.MaxInFlightPerAgent = 1
	endpoint := agentEndpoint{Status: testAgentStatus()}
	endpoint.Status.ExecutionSlots.Available = 1

	selected, err := selectPrepareEndpoint(source, []agentEndpoint{endpoint})
	if err != nil {
		t.Fatalf("selectPrepareEndpoint: %v", err)
	}
	if selected.Status.AgentID != endpoint.Status.AgentID {
		t.Fatalf("selected agent = %q, want %q", selected.Status.AgentID, endpoint.Status.AgentID)
	}
}

func TestSelectSandboxEndpointRequiresSingleCompatibleAgent(t *testing.T) {
	first := agentEndpoint{Status: testAgentStatus()}
	first.Status.AgentID = "spore-agent-test-0001"
	second := agentEndpoint{Status: testAgentStatus()}
	second.Status.AgentID = "spore-agent-test-0002"

	first.Status.Pressure.Disk = fleet.PressureCritical
	selected, err := selectSandboxEndpoint([]agentEndpoint{first, second})
	if err != nil {
		t.Fatalf("selectSandboxEndpoint: %v", err)
	}
	if selected.Status.AgentID != second.Status.AgentID {
		t.Fatalf("selected pressured agent %q, want %q", selected.Status.AgentID, second.Status.AgentID)
	}

	first.Status.Pressure.Disk = fleet.PressureNormal
	if _, err := selectSandboxEndpoint([]agentEndpoint{first, second}); err == nil {
		t.Fatal("selectSandboxEndpoint with two compatible agents succeeded")
	}

	first.Status.Healthy = false
	second.Status.Healthy = false
	if _, err := selectSandboxEndpoint([]agentEndpoint{first, second}); !errors.Is(err, fleet.ErrNoCompatibleAgents) {
		t.Fatalf("selectSandboxEndpoint error = %v, want ErrNoCompatibleAgents", err)
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

func testRun() fleet.Run {
	return fleet.Run{
		RunID: "rails-rspec-20260624",
		Source: fleet.RunSource{
			Image:    "example.com/sporevm/rails-rspec:sha-1111111",
			Platform: "linux/arm64",
		},
		Prepare: fleet.PrepareSpec{
			Command:       []string{"/bin/bash", "/usr/local/bin/sporevm-rails-coordinator", "--capture-delay", "2"},
			CaptureSignal: "USR1",
			ReadyMarker:   "SPOREVM_RAILS_READY",
		},
		Fork: fleet.ForkSpec{Count: 1},
		Children: fleet.RunChildren{
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
	execExitCode   int
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

func (c *fakeSporeClient) CreateVM(context.Context, agent.CreateVMRequest) error {
	return nil
}

func (c *fakeSporeClient) Fork(_ context.Context, req agent.ForkRequest) error {
	childCount := c.childCount
	if childCount == 0 {
		childCount = 1
	}
	for i := 0; i < childCount; i++ {
		childDir := filepath.Join(req.OutDir, fmt.Sprintf("%06d", i))
		if err := os.MkdirAll(childDir, 0o755); err != nil {
			return err
		}
	}
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

func (c *fakeSporeClient) Exec(context.Context, agent.ExecRequest) ([]agent.RunEvent, error) {
	exitCode := c.execExitCode
	return []agent.RunEvent{{
		Schema:        "spore.run-events.v1",
		SchemaVersion: 1,
		Event:         "exit",
		Command:       "exec",
		ExitCode:      &exitCode,
	}}, nil
}

func (c *fakeSporeClient) RemoveVM(context.Context, agent.RemoveVMRequest) error {
	return nil
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
