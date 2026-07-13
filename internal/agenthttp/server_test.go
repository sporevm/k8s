package agenthttp

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sporevm/k8s/internal/agent"
	"github.com/sporevm/k8s/internal/fleet"
)

const testPinnedImage = "docker.io/library/node@sha256:6db9be2ebb4bafb687a078ef5ba1b1dd256e8004d246a31fd210b6b848ab6be2"

func TestClientServerRoundTripRunsShard(t *testing.T) {
	run := testBundleRun()
	fake := fakeSporeClient{digest: run.Bundle.Digest}
	store, err := agent.NewLocalResultStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalResultStore: %v", err)
	}
	runner, err := agent.NewRunner(
		1,
		agent.WithSporeClient(fake),
		agent.WithResultStore(store),
		agent.WithWorkRoot(t.TempDir()),
	)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	handler, err := (&Server{
		Runner:  runner,
		Client:  fake,
		AgentID: "spore-agent-test-0001",
		CellID:  "test-cell-0001",
		Region:  "us-east-1",
		Backend: "kvm",
		Now:     func() time.Time { return time.Date(2026, 6, 21, 8, 0, 0, 0, time.UTC) },
	}).Handler()
	if err != nil {
		t.Fatalf("Handler: %v", err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	client := Client{BaseURL: server.URL, HTTPClient: server.Client()}
	status, err := client.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.AgentID != "spore-agent-test-0001" || status.ExecutionSlots.Available != 1 {
		t.Fatalf("status = %+v", status)
	}

	inspection, err := client.InspectRunBundle(context.Background(), run)
	if err != nil {
		t.Fatalf("InspectRunBundle: %v", err)
	}
	if err := inspection.Validate(run); err != nil {
		t.Fatalf("inspection Validate: %v", err)
	}

	lease := fleet.ShardLease{
		RunID:         run.RunID,
		BundleDigest:  run.Bundle.Digest,
		ShardID:       "ruby-counter-20260620-shard-0001",
		ChildStart:    0,
		ChildCount:    1,
		AttemptBudget: 1,
		HostClassID:   run.HostClass.ID,
		AgentID:       status.AgentID,
		LeaseDeadline: time.Now().Add(time.Minute),
	}
	results, err := client.RunShard(context.Background(), fleet.ShardExecutionRequest{
		Run:     run,
		Lease:   lease,
		Attempt: 1,
	})
	if err != nil {
		t.Fatalf("RunShard: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("result count = %d", len(results))
	}
	if results[0].Status != fleet.AttemptSucceeded || !results[0].Terminal {
		t.Fatalf("result = %+v", results[0])
	}
}

func TestServerCachesCacheRootStatusBriefly(t *testing.T) {
	bundleRoot := t.TempDir()
	rootFSRoot := t.TempDir()
	now := time.Date(2026, 7, 11, 1, 0, 0, 0, time.UTC)
	server := &Server{
		BundleCacheRoot: bundleRoot,
		RootFSCacheRoot: rootFSRoot,
		Now:             func() time.Time { return now },
	}

	first, err := server.cacheStatus()
	if err != nil {
		t.Fatalf("first cacheStatus: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rootFSRoot, "new-entry"), []byte("cache"), 0o644); err != nil {
		t.Fatalf("write cache entry: %v", err)
	}
	second, err := server.cacheStatus()
	if err != nil {
		t.Fatalf("second cacheStatus: %v", err)
	}
	if second != first {
		t.Fatalf("cached status = %+v, want %+v", second, first)
	}

	now = now.Add(statusCacheTTL)
	refreshed, err := server.cacheStatus()
	if err != nil {
		t.Fatalf("refreshed cacheStatus: %v", err)
	}
	if refreshed.RootFSCacheEntries != 1 || refreshed.RootFSCacheBytes != int64(len("cache")) {
		t.Fatalf("refreshed status = %+v", refreshed)
	}
}

func TestClientServerRoundTripPreparesBundle(t *testing.T) {
	source := testRun()
	client := newTestHTTPClientWithSporeClient(t, testBundleRun(), fakeSporeClient{
		digest:     "sha256:2222222222222222222222222222222222222222222222222222222222222222",
		childCount: 1,
	})

	prepared, err := client.PrepareBundle(context.Background(), source)
	if err != nil {
		t.Fatalf("PrepareBundle: %v", err)
	}
	if prepared.Bundle.Digest != "sha256:2222222222222222222222222222222222222222222222222222222222222222" {
		t.Fatalf("prepared digest = %q", prepared.Bundle.Digest)
	}
	if prepared.ChildCount != 1 {
		t.Fatalf("prepared child count = %d", prepared.ChildCount)
	}
	if prepared.Local == nil || prepared.Local.ChildrenDir == "" || prepared.Local.BundleDir == "" {
		t.Fatalf("prepared local metadata = %+v", prepared.Local)
	}
	if _, err := source.Compile(prepared); err != nil {
		t.Fatalf("Compile prepared bundle: %v", err)
	}
}

func TestClientServerRoundTripSandbox(t *testing.T) {
	client := newTestHTTPClient(t, testBundleRun())
	ctx := context.Background()
	response, err := client.CreateSandbox(ctx, agent.SandboxCreateRequest{
		Name:  "sporevm-sandbox-node",
		Image: testPinnedImage,
	})
	if err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	if response.Name != "sporevm-sandbox-node" || response.Template.ID == "" {
		t.Fatalf("CreateSandbox response = %+v", response)
	}
	events, err := client.ExecSandbox(ctx, "sporevm-sandbox-node", []string{"/bin/sh", "-lc", "node -v"})
	if err != nil {
		t.Fatalf("ExecSandbox: %v", err)
	}
	terminal, err := agent.TerminalEvent(events)
	if err != nil {
		t.Fatalf("TerminalEvent: %v", err)
	}
	if terminal.ExitCode == nil || *terminal.ExitCode != 0 {
		t.Fatalf("terminal = %+v", terminal)
	}
	if err := client.RemoveSandbox(ctx, "sporevm-sandbox-node"); err != nil {
		t.Fatalf("RemoveSandbox: %v", err)
	}
}

func TestServerRejectsLeaseForDifferentAgent(t *testing.T) {
	run := testBundleRun()
	client := newTestHTTPClient(t, run)
	lease := fleet.ShardLease{
		RunID:         run.RunID,
		BundleDigest:  run.Bundle.Digest,
		ShardID:       "ruby-counter-20260620-shard-0001",
		ChildStart:    0,
		ChildCount:    1,
		AttemptBudget: 1,
		HostClassID:   run.HostClass.ID,
		AgentID:       "spore-agent-other-0001",
		LeaseDeadline: time.Now().Add(time.Minute),
	}
	_, err := client.RunShard(context.Background(), fleet.ShardExecutionRequest{
		Run:     run,
		Lease:   lease,
		Attempt: 1,
	})
	if err == nil {
		t.Fatal("RunShard succeeded with a foreign lease agent id")
	}
}

func TestServerReturnsShardResultsWhenRunnerReportsChildError(t *testing.T) {
	run := testBundleRun()
	client := newTestHTTPClientWithSporeClient(t, run, fakeSporeClient{
		digest:    run.Bundle.Digest,
		resumeErr: errors.New("resume failed"),
	})
	lease := fleet.ShardLease{
		RunID:         run.RunID,
		BundleDigest:  run.Bundle.Digest,
		ShardID:       "ruby-counter-20260620-shard-0001",
		ChildStart:    0,
		ChildCount:    1,
		AttemptBudget: 1,
		HostClassID:   run.HostClass.ID,
		AgentID:       "spore-agent-test-0001",
		LeaseDeadline: time.Now().Add(time.Minute),
	}
	results, err := client.RunShard(context.Background(), fleet.ShardExecutionRequest{
		Run:     run,
		Lease:   lease,
		Attempt: 1,
	})
	if err != nil {
		t.Fatalf("RunShard: %v", err)
	}
	if len(results) != 1 || results[0].Status != fleet.AttemptFailed {
		t.Fatalf("results = %+v", results)
	}
}

func TestShardResultCountIgnoresEmptySlots(t *testing.T) {
	results := []fleet.AttemptResult{
		{},
		{AttemptID: "ruby-counter-20260620-child-0-attempt-01"},
	}
	if got := shardResultCount(results); got != 1 {
		t.Fatalf("shardResultCount = %d, want 1", got)
	}
}

func testBundleRun() fleet.BundleRun {
	return fleet.BundleRun{
		RunID: "ruby-counter-20260620",
		Bundle: fleet.Bundle{
			URI:    "s3://example-sporevm-artifacts/runs/ruby-counter.bundle",
			Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		},
		Children: fleet.ChildRange{Start: 0, Count: 1},
		HostClass: fleet.HostClass{
			ID:                   "linux-aarch64-kvm-graviton1-v0",
			SporePlatformVersion: "v0",
			Architecture:         "aarch64",
			Backend:              "kvm",
			CPUProfile:           "graviton1",
			DeviceModel:          "sporevm-aarch64-v0",
		},
		Execution:   fleet.Execution{ChildrenPerShard: 1, MaxInFlightPerAgent: 1},
		RetryPolicy: fleet.RetryPolicy{MaxAttemptsPerChild: 1},
		SideEffects: fleet.SideEffects{IdempotencyRequired: true},
		ResultStore: "s3://example-sporevm-results/ruby-counter-20260620/",
	}
}

func newTestHTTPClient(t *testing.T, run fleet.BundleRun) Client {
	t.Helper()
	return newTestHTTPClientWithSporeClient(t, run, fakeSporeClient{digest: run.Bundle.Digest})
}

func newTestHTTPClientWithSporeClient(t *testing.T, run fleet.BundleRun, fake fakeSporeClient) Client {
	t.Helper()
	store, err := agent.NewLocalResultStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalResultStore: %v", err)
	}
	runner, err := agent.NewRunner(
		1,
		agent.WithSporeClient(fake),
		agent.WithResultStore(store),
		agent.WithWorkRoot(t.TempDir()),
	)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	handler, err := (&Server{
		Runner:  runner,
		Client:  fake,
		AgentID: "spore-agent-test-0001",
		CellID:  "test-cell-0001",
		Region:  "us-east-1",
		Backend: "kvm",
		Now:     func() time.Time { return time.Date(2026, 6, 21, 8, 0, 0, 0, time.UTC) },
	}).Handler()
	if err != nil {
		t.Fatalf("Handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return Client{BaseURL: server.URL, HTTPClient: server.Client()}
}

type fakeSporeClient struct {
	digest     string
	childCount int
	resumeErr  error
}

func (c fakeSporeClient) Version(context.Context) (string, error) {
	return "spore 0.13.0 (ReleaseSafe)", nil
}

func (c fakeSporeClient) HostInfo(context.Context) (agent.HostInfo, error) {
	return agent.HostInfo{
		Schema:        "spore.host-info.v1",
		SchemaVersion: 1,
		HostClass:     "linux-aarch64-kvm",
		Platform: agent.PlatformFacts{
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

func (c fakeSporeClient) InspectBundle(context.Context, agent.InspectBundleRequest) (agent.InspectBundleResult, error) {
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

func (c fakeSporeClient) RunCapture(_ context.Context, req agent.RunCaptureRequest) ([]agent.RunEvent, error) {
	if err := os.MkdirAll(req.CaptureDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(req.CaptureDir, "manifest.json"), []byte("{}\n"), 0o644); err != nil {
		return nil, err
	}
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

func (c fakeSporeClient) Fork(context.Context, agent.ForkRequest) error {
	return nil
}

func (c fakeSporeClient) Pack(context.Context, agent.PackRequest) error {
	return nil
}

func (c fakeSporeClient) Pull(_ context.Context, req agent.PullRequest) (agent.PullResult, error) {
	return agent.PullResult{
		Schema:        "spore.pull.result.v1",
		SchemaVersion: 1,
		OutDir:        req.OutDir,
		BundleDigest:  testDigest(c.digest),
	}, nil
}

func (c fakeSporeClient) Resume(context.Context, agent.ResumeRequest) ([]agent.RunEvent, error) {
	if c.resumeErr != nil {
		return nil, c.resumeErr
	}
	exitCode := 0
	return []agent.RunEvent{{
		Schema:        "spore.run-events.v1",
		SchemaVersion: 1,
		Event:         "exit",
		Command:       "resume",
		ExitCode:      &exitCode,
		Timings:       &agent.RunEventTimings{ExecResponseMS: 7},
	}}, nil
}

func (c fakeSporeClient) RestoreNamed(_ context.Context, req agent.RestoreNamedRequest) (agent.NamedLifecycleResult, error) {
	if c.resumeErr != nil {
		return agent.NamedLifecycleResult{}, c.resumeErr
	}
	return agent.NamedLifecycleResult{
		Schema:        "spore.lifecycle.v1",
		SchemaVersion: 1,
		Action:        "restored",
		Name:          req.Name,
		State:         "ready",
		Timing:        &agent.NamedLifecycleTiming{},
	}, nil
}

func (c fakeSporeClient) RunFrom(context.Context, agent.RunFromRequest) ([]agent.RunEvent, error) {
	exitCode := 0
	return []agent.RunEvent{{
		Schema:        "spore.run-events.v1",
		SchemaVersion: 1,
		Event:         "exit",
		Command:       "run",
		ExitCode:      &exitCode,
		Timings:       &agent.RunEventTimings{ExecResponseMS: 7},
	}}, nil
}

func (c fakeSporeClient) Exec(context.Context, agent.ExecRequest) ([]agent.RunEvent, error) {
	exitCode := 0
	return []agent.RunEvent{{
		Schema:        "spore.run-events.v1",
		SchemaVersion: 1,
		Event:         "exit",
		Command:       "exec",
		ExitCode:      &exitCode,
	}}, nil
}

func (c fakeSporeClient) RemoveVM(context.Context, agent.RemoveVMRequest) error {
	return nil
}

func (c fakeSporeClient) RemoveSavedSpore(context.Context, agent.RemoveSavedSporeRequest) error {
	return nil
}

func testDigest(raw string) agent.DigestRef {
	return agent.DigestRef{
		Algorithm: "sha256",
		Hex:       raw[len("sha256:"):],
	}
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
