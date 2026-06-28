package fleet

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCoordinatorRunExecutesSingleCellPlan(t *testing.T) {
	run := loadExampleRun(t)
	agents := makeAgents(t, 10, 100, run.HostClass)
	store := newFakeTerminalStore()
	executors := make(map[string]ShardExecutor, len(agents))
	for _, agent := range agents {
		executors[agent.AgentID] = &fakeShardExecutor{
			fn: func(_ context.Context, req ShardExecutionRequest) ([]AttemptResult, error) {
				results := make([]AttemptResult, 0, req.Lease.ChildCount)
				for childID := req.Lease.ChildStart; childID < req.Lease.ChildRange().End(); childID++ {
					result := runtimeAttemptResult(run, req.Lease, childID, req.Attempt, AttemptSucceeded, true)
					store.commit(result)
					results = append(results, result)
				}
				return results, nil
			},
		}
	}
	coordinator := newTestCoordinator(t, run, agents, store, executors, nil)

	report, err := coordinator.Run(context.Background(), run)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.Summary.State != "succeeded" {
		t.Fatalf("state = %q", report.Summary.State)
	}
	if report.Summary.CompletedChildren != 1000 || report.Summary.SucceededChildren != 1000 {
		t.Fatalf("summary = %+v", report.Summary)
	}
	if report.Summary.AssignedShards != 10 || report.Summary.AssignedChildren != 1000 {
		t.Fatalf("assignment summary = %+v", report.Summary)
	}
	if report.Summary.AttemptCount != 1000 {
		t.Fatalf("attempt count = %d", report.Summary.AttemptCount)
	}
	if err := ValidateCompleteCoverage(run, report.Plan.Leases); err != nil {
		t.Fatalf("ValidateCompleteCoverage: %v", err)
	}
}

func TestCoordinatorRejectsBundleDigestMismatchBeforePlanning(t *testing.T) {
	run := loadExampleRun(t)
	agents := makeAgents(t, 10, 100, run.HostClass)
	executor := &fakeShardExecutor{}
	coordinator, err := NewCoordinator(
		agents,
		fakeBundleInspector{inspection: BundleInspection{
			BundleDigest: "sha256:2222222222222222222222222222222222222222222222222222222222222222",
			ChildCount:   run.Children.Count,
			HostClass:    run.HostClass,
		}},
		newFakeTerminalStore(),
		map[string]ShardExecutor{agents[0].AgentID: executor},
		CoordinatorOptions{},
	)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}

	_, err = coordinator.Run(context.Background(), run)
	if !errors.Is(err, ErrBundleMetadataMismatch) {
		t.Fatalf("Run error = %v, want ErrBundleMetadataMismatch", err)
	}
	if executor.callCount() != 0 {
		t.Fatalf("executor calls = %d, want 0", executor.callCount())
	}
}

func TestCoordinatorRejectsInspectedHostClassMismatch(t *testing.T) {
	run := loadExampleRun(t)
	agents := makeAgents(t, 10, 100, run.HostClass)
	executor := &fakeShardExecutor{}
	hostClass := run.HostClass
	hostClass.CPUProfile = "graviton2"
	hostClass.ID = "linux-aarch64-kvm-graviton2-v0"
	coordinator, err := NewCoordinator(
		agents,
		fakeBundleInspector{inspection: BundleInspection{
			BundleDigest: run.Bundle.Digest,
			ChildCount:   run.Children.Count,
			HostClass:    hostClass,
		}},
		newFakeTerminalStore(),
		map[string]ShardExecutor{agents[0].AgentID: executor},
		CoordinatorOptions{},
	)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}

	_, err = coordinator.Run(context.Background(), run)
	if !errors.Is(err, ErrBundleMetadataMismatch) {
		t.Fatalf("Run error = %v, want ErrBundleMetadataMismatch", err)
	}
	if executor.callCount() != 0 {
		t.Fatalf("executor calls = %d, want 0", executor.callCount())
	}
}

func TestCoordinatorRetriesNonTerminalAttempts(t *testing.T) {
	run := smallRuntimeRun(t, 1, 2)
	agents := makeAgents(t, 1, 1, run.HostClass)
	store := newFakeTerminalStore()
	executor := &fakeShardExecutor{
		fn: func(_ context.Context, req ShardExecutionRequest) ([]AttemptResult, error) {
			if req.Attempt == 1 {
				return []AttemptResult{
					runtimeAttemptResult(run, req.Lease, 0, req.Attempt, AttemptFailed, false),
				}, errors.New("transient runtime failure")
			}
			result := runtimeAttemptResult(run, req.Lease, 0, req.Attempt, AttemptSucceeded, true)
			store.commit(result)
			return []AttemptResult{result}, nil
		},
	}
	coordinator := newTestCoordinator(t, run, agents, store, map[string]ShardExecutor{agents[0].AgentID: executor}, nil)

	report, err := coordinator.Run(context.Background(), run)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if executor.callCount() != 2 {
		t.Fatalf("executor calls = %d, want 2", executor.callCount())
	}
	if report.Summary.State != "succeeded" || report.Summary.CompletedChildren != 1 {
		t.Fatalf("summary = %+v", report.Summary)
	}
	if report.Summary.NonTerminalFailures != 1 || report.Summary.AttemptCount != 2 {
		t.Fatalf("retry summary = %+v", report.Summary)
	}
}

func TestCoordinatorChecksTerminalStoreBeforeRetry(t *testing.T) {
	run := smallRuntimeRun(t, 2, 2)
	agents := makeAgents(t, 1, 2, run.HostClass)
	store := newFakeTerminalStore()
	executor := &fakeShardExecutor{
		fn: func(_ context.Context, req ShardExecutionRequest) ([]AttemptResult, error) {
			child0 := runtimeAttemptResult(run, req.Lease, 0, req.Attempt, AttemptSucceeded, true)
			child1 := runtimeAttemptResult(run, req.Lease, 1, req.Attempt, AttemptFailed, false)
			store.commit(child0)
			store.commit(runtimeAttemptResult(run, req.Lease, 1, req.Attempt, AttemptSucceeded, true))
			return []AttemptResult{child0, child1}, errors.New("transient child failure")
		},
	}
	coordinator := newTestCoordinator(t, run, agents, store, map[string]ShardExecutor{agents[0].AgentID: executor}, nil)

	report, err := coordinator.Run(context.Background(), run)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if executor.callCount() != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.callCount())
	}
	if report.Summary.CompletedChildren != 2 || report.Summary.SucceededChildren != 2 {
		t.Fatalf("summary = %+v", report.Summary)
	}
}

func TestCoordinatorDoesNotRetryPlatformMismatch(t *testing.T) {
	run := smallRuntimeRun(t, 1, 2)
	agents := makeAgents(t, 1, 1, run.HostClass)
	executor := &fakeShardExecutor{
		fn: func(_ context.Context, req ShardExecutionRequest) ([]AttemptResult, error) {
			return []AttemptResult{
				runtimeAttemptResult(run, req.Lease, 0, req.Attempt, AttemptPlatformMismatch, false),
			}, nil
		},
	}
	coordinator := newTestCoordinator(t, run, agents, newFakeTerminalStore(), map[string]ShardExecutor{agents[0].AgentID: executor}, nil)

	report, err := coordinator.Run(context.Background(), run)
	if !errors.Is(err, ErrPlatformMismatch) {
		t.Fatalf("Run error = %v, want ErrPlatformMismatch", err)
	}
	if executor.callCount() != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.callCount())
	}
	if report.Summary.PlatformMismatches != 1 || report.Summary.State != "failed" {
		t.Fatalf("summary = %+v", report.Summary)
	}
}

func TestCoordinatorFailsExpiredLeaseBeforeExecution(t *testing.T) {
	run := smallRuntimeRun(t, 1, 1)
	agents := makeAgents(t, 1, 1, run.HostClass)
	base := time.Date(2026, 6, 20, 4, 0, 0, 0, time.UTC)
	executor := &fakeShardExecutor{}
	coordinator := newTestCoordinator(t, run, agents, newFakeTerminalStore(), map[string]ShardExecutor{agents[0].AgentID: executor}, sequenceClock(
		base,
		base.Add(11*time.Minute),
		base.Add(11*time.Minute),
	))

	report, err := coordinator.Run(context.Background(), run)
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("Run error = %v, want ErrLeaseExpired", err)
	}
	if executor.callCount() != 0 {
		t.Fatalf("executor calls = %d, want 0", executor.callCount())
	}
	if report.Summary.State != "failed" || report.Summary.LeaseErrors != 1 {
		t.Fatalf("summary = %+v", report.Summary)
	}
}

func TestCoordinatorReturnsErrorWhenLeaseExhaustsAttemptsIncomplete(t *testing.T) {
	run := smallRuntimeRun(t, 1, 1)
	agents := makeAgents(t, 1, 1, run.HostClass)
	executor := &fakeShardExecutor{
		fn: func(context.Context, ShardExecutionRequest) ([]AttemptResult, error) {
			return nil, nil
		},
	}
	coordinator := newTestCoordinator(t, run, agents, newFakeTerminalStore(), map[string]ShardExecutor{agents[0].AgentID: executor}, nil)

	report, err := coordinator.Run(context.Background(), run)
	if !errors.Is(err, ErrLeaseIncomplete) {
		t.Fatalf("Run error = %v, want ErrLeaseIncomplete", err)
	}
	if report.Summary.State != "failed" || report.Summary.CompletedChildren != 0 {
		t.Fatalf("summary = %+v", report.Summary)
	}
}

func smallRuntimeRun(t *testing.T, childCount int, attempts int) Run {
	t.Helper()
	run := loadExampleRun(t)
	run.Children = ChildRange{Start: 0, Count: childCount}
	run.Execution.ChildrenPerShard = childCount
	run.Execution.MaxInFlightPerAgent = childCount
	run.RetryPolicy.MaxAttemptsPerChild = attempts
	return run
}

func newTestCoordinator(
	t *testing.T,
	run Run,
	agents []AgentStatus,
	store *fakeTerminalStore,
	executors map[string]ShardExecutor,
	now func() time.Time,
) *Coordinator {
	t.Helper()
	coordinator, err := NewCoordinator(
		agents,
		fakeBundleInspector{inspection: BundleInspection{
			BundleDigest: run.Bundle.Digest,
			ChildCount:   run.Children.End(),
			HostClass:    run.HostClass,
		}},
		store,
		executors,
		CoordinatorOptions{Now: now, LeaseTTL: 10 * time.Minute},
	)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	return coordinator
}

type fakeBundleInspector struct {
	inspection BundleInspection
	err        error
}

func (i fakeBundleInspector) InspectRunBundle(context.Context, Run) (BundleInspection, error) {
	if i.err != nil {
		return BundleInspection{}, i.err
	}
	return i.inspection, nil
}

type fakeTerminalStore struct {
	mu       sync.Mutex
	terminal map[int]AttemptResult
}

func newFakeTerminalStore() *fakeTerminalStore {
	return &fakeTerminalStore{terminal: make(map[int]AttemptResult)}
}

func (s *fakeTerminalStore) TerminalResult(_ context.Context, _ Run, childID int) (AttemptResult, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, ok := s.terminal[childID]
	return result, ok, nil
}

func (s *fakeTerminalStore) commit(result AttemptResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.terminal[result.ChildID] = result
}

type fakeShardExecutor struct {
	mu    sync.Mutex
	calls []ShardExecutionRequest
	fn    func(context.Context, ShardExecutionRequest) ([]AttemptResult, error)
}

func (e *fakeShardExecutor) RunShard(ctx context.Context, req ShardExecutionRequest) ([]AttemptResult, error) {
	e.mu.Lock()
	e.calls = append(e.calls, req)
	e.mu.Unlock()
	if e.fn == nil {
		return nil, nil
	}
	return e.fn(ctx, req)
}

func (e *fakeShardExecutor) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.calls)
}

func runtimeAttemptResult(run Run, lease ShardLease, childID int, attempt int, status AttemptStatus, terminal bool) AttemptResult {
	now := time.Date(2026, 6, 20, 4, 0, 1, 0, time.UTC)
	result := AttemptResult{
		RunID:        run.RunID,
		BundleDigest: run.Bundle.Digest,
		ChildID:      childID,
		AttemptID:    FormatAttemptID(run.RunID, childID, attempt),
		AgentID:      lease.AgentID,
		ShardID:      lease.ShardID,
		Status:       status,
		StartedAt:    now,
		FinishedAt:   now.Add(time.Second),
		Terminal:     terminal,
	}
	if terminal {
		result.ResultURI = fmt.Sprintf("%schildren/%d/terminal.json", run.ResultStore, childID)
	}
	if status == AttemptFailed || status == AttemptPlatformMismatch {
		result.Error = &AttemptError{Code: "runtime.execution_failed", Message: "test failure"}
	}
	return result
}

func sequenceClock(times ...time.Time) func() time.Time {
	var mu sync.Mutex
	var index int
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		if index >= len(times) {
			return times[len(times)-1]
		}
		value := times[index]
		index++
		return value
	}
}
