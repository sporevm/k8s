package agenthttp

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sporevm/k8s/internal/agent"
	"github.com/sporevm/k8s/internal/fleet"
)

func TestClientServerRoundTripRunsShard(t *testing.T) {
	run := testRun()
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

func TestClientServerRoundTripPreparesBundle(t *testing.T) {
	generic := testGenericRun()
	client := newTestHTTPClientWithSporeClient(t, testRun(), fakeSporeClient{
		digest:     "sha256:2222222222222222222222222222222222222222222222222222222222222222",
		childCount: 1,
	})

	prepared, err := client.PrepareBundle(context.Background(), generic)
	if err != nil {
		t.Fatalf("PrepareBundle: %v", err)
	}
	if prepared.Bundle.Digest != "sha256:2222222222222222222222222222222222222222222222222222222222222222" {
		t.Fatalf("prepared digest = %q", prepared.Bundle.Digest)
	}
	if prepared.ChildCount != 1 {
		t.Fatalf("prepared child count = %d", prepared.ChildCount)
	}
	if _, err := generic.Compile(prepared); err != nil {
		t.Fatalf("Compile prepared bundle: %v", err)
	}
}

func TestServerRejectsLeaseForDifferentAgent(t *testing.T) {
	run := testRun()
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
	run := testRun()
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

func testRun() fleet.Run {
	return fleet.Run{
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

func newTestHTTPClient(t *testing.T, run fleet.Run) Client {
	t.Helper()
	return newTestHTTPClientWithSporeClient(t, run, fakeSporeClient{digest: run.Bundle.Digest})
}

func newTestHTTPClientWithSporeClient(t *testing.T, run fleet.Run, fake fakeSporeClient) Client {
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

func (c fakeSporeClient) RunCapture(context.Context, agent.RunCaptureRequest) ([]agent.RunEvent, error) {
	exitCode := 0
	path := "/work/parent.spore"
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

func testDigest(raw string) agent.DigestRef {
	return agent.DigestRef{
		Algorithm: "sha256",
		Hex:       raw[len("sha256:"):],
	}
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
