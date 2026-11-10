package fleet

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCoordinatorRunExecutesSingleAgentSequentialPlan(t *testing.T) {
	run := loadExampleRun(t)
	agents := makeAgents(t, 1, 100, run.HostClass)
	store := newFakeTerminalStore()
	executor := &fakeShardExecutor{
		fn: func(_ context.Context, req ShardExecutionRequest) ([]AttemptResult, error) {
			if req.Lease.ChildCount != 1000 {
				t.Fatalf("lease child count = %d, want 1000", req.Lease.ChildCount)
			}
			results := make([]AttemptResult, 0, req.Lease.ChildCount)
			for childID := req.Lease.ChildStart; childID < req.Lease.ChildRange().End(); childID++ {
				result := runtimeAttemptResult(run, req.Lease, childID, req.Attempt, AttemptSucceeded, true)
				store.commit(result)
				results = append(results, result)
			}
			return results, nil
		},
	}
	coordinator, err := NewCoordinator(
		agents,
		map[string]CheckpointInspector{agents[0].AgentID: fakeBundleInspector{inspection: CheckpointInspection{
			CheckpointDigest: run.Checkpoint.Digest,
			ChildCount:       run.Children.End(),
			HostClass:        run.HostClass,
		}}},
		store,
		map[string]ShardExecutor{agents[0].AgentID: executor},
		CoordinatorOptions{},
	)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}

	report, err := coordinator.Run(context.Background(), run)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if executor.callCount() != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.callCount())
	}
	if report.Summary.State != "succeeded" || report.Summary.CompletedChildren != 1000 {
		t.Fatalf("summary = %+v", report.Summary)
	}
	if report.Summary.AssignedShards != 1 || report.Summary.AssignedChildren != 1000 {
		t.Fatalf("assignment summary = %+v", report.Summary)
	}
	if report.Summary.AttemptCount != 1000 {
		t.Fatalf("attempt count = %d", report.Summary.AttemptCount)
	}
	if report.Timings == nil {
		t.Fatal("timings are nil")
	}
	if report.Timings.StagePercentilesMS.Resume.P50 != 30 {
		t.Fatalf("resume p50 = %v, want 30", report.Timings.StagePercentilesMS.Resume.P50)
	}
}

func TestCoordinatorTimingIncludesEveryDestinationAdmission(t *testing.T) {
	run := smallRuntimeRun(t, 2, 1)
	agents := makeAgents(t, 2, 1, run.HostClass)
	store := newFakeTerminalStore()
	clock := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	inspector := fakeBundleInspector{
		inspection: CheckpointInspection{CheckpointDigest: run.Checkpoint.Digest, HostClass: run.HostClass},
		onInspect:  func() { clock = clock.Add(9 * time.Second) },
	}
	executor := &fakeShardExecutor{fn: func(_ context.Context, req ShardExecutionRequest) ([]AttemptResult, error) {
		result := runtimeAttemptResult(run, req.Lease, req.Lease.ChildStart, req.Attempt, AttemptSucceeded, true)
		result.StartedAt, result.FinishedAt = clock, clock
		result.TimingsMS = AttemptTimings{}
		store.commit(result)
		return []AttemptResult{result}, nil
	}}
	coordinator, err := NewCoordinator(agents,
		map[string]CheckpointInspector{agents[0].AgentID: inspector, agents[1].AgentID: inspector},
		store, map[string]ShardExecutor{agents[0].AgentID: executor, agents[1].AgentID: executor},
		CoordinatorOptions{Now: func() time.Time { return clock }})
	if err != nil {
		t.Fatal(err)
	}
	report, err := coordinator.Run(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := report.Summary.FinishedAt.Sub(report.Summary.StartedAt); elapsed != 18*time.Second {
		t.Fatalf("aggregate duration = %v, want both nine-second admissions", elapsed)
	}
	if report.Timings == nil || report.Timings.ChildReadyMS.P50 != 18000 {
		t.Fatalf("child-ready timings = %+v, want admission-inclusive 18000ms", report.Timings)
	}
}

func TestCoordinatorRunsAgentsConcurrentlyAndRetriesOnlyUnfinishedChildren(t *testing.T) {
	run := loadExampleRun(t)
	run.Children = ChildRange{Start: 17, Count: 5}
	run.Execution.MaxInFlightPerAgent = 2
	run.RetryPolicy.MaxAttemptsPerChild = 2
	agents := makeAgents(t, 2, 2, run.HostClass)
	agents[0].ExecutionSlots.Available = 1
	store := newFakeTerminalStore()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered := make(chan string, 2)
	proceed := make(chan struct{})
	executor := &fakeShardExecutor{fn: func(ctx context.Context, req ShardExecutionRequest) ([]AttemptResult, error) {
		if req.Attempt == 1 {
			entered <- req.Lease.AgentID
			select {
			case <-proceed:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		var results []AttemptResult
		for id := req.Lease.ChildStart; id < req.Lease.ChildRange().End(); id++ {
			if _, exists, _ := store.TerminalResult(ctx, run, id); exists {
				continue
			}
			terminal := id != 17 || req.Attempt > 1
			status := AttemptSucceeded
			if !terminal {
				status = AttemptFailed
			}
			result := runtimeAttemptResult(run, req.Lease, id, req.Attempt, status, terminal)
			if terminal {
				store.commit(result)
			}
			results = append(results, result)
		}
		return results, nil
	}}
	coordinator := newTestCoordinator(t, run, agents, store, map[string]ShardExecutor{
		agents[0].AgentID: executor, agents[1].AgentID: executor,
	}, nil)
	type outcome struct {
		report RuntimeReport
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		report, err := coordinator.Run(ctx, run)
		done <- outcome{report, err}
	}()
	seen := make(map[string]bool)
	for len(seen) < 2 {
		select {
		case id := <-entered:
			seen[id] = true
		case <-ctx.Done():
			t.Fatal("agents did not start concurrently")
		}
	}
	close(proceed)
	result := <-done
	if result.err != nil || result.report.Summary.SucceededChildren != 5 || result.report.Summary.AttemptCount != 6 || result.report.Summary.AssignedShards != 2 {
		t.Fatalf("report = %+v, error = %v", result.report, result.err)
	}
	for id := 17; id < 22; id++ {
		terminal, exists, err := store.TerminalResult(ctx, run, id)
		if err != nil || !exists || terminal.Status != AttemptSucceeded {
			t.Fatalf("child %d = %+v, exists %v, error %v", id, terminal, exists, err)
		}
	}
	replay, err := coordinator.Run(ctx, run)
	if err != nil || replay.Summary.AttemptCount != 0 || replay.Summary.SucceededChildren != 5 {
		t.Fatalf("replay = %+v, error = %v", replay, err)
	}
}

func TestCoordinatorRejectsBundleDigestMismatchBeforePlanning(t *testing.T) {
	run := loadExampleRun(t)
	agents := makeAgents(t, 1, 100, run.HostClass)
	executor := &fakeShardExecutor{}
	coordinator, err := NewCoordinator(
		agents,
		map[string]CheckpointInspector{agents[0].AgentID: fakeBundleInspector{inspection: CheckpointInspection{
			CheckpointDigest: "blake3:2222222222222222222222222222222222222222222222222222222222222222",
			ChildCount:       run.Children.Count,
			HostClass:        run.HostClass,
		}}},
		newFakeTerminalStore(),
		map[string]ShardExecutor{agents[0].AgentID: executor},
		CoordinatorOptions{},
	)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}

	_, err = coordinator.Run(context.Background(), run)
	if !errors.Is(err, ErrBundleMetadataMismatch) {
		t.Fatalf("BundleRun error = %v, want ErrBundleMetadataMismatch", err)
	}
	if executor.callCount() != 0 {
		t.Fatalf("executor calls = %d, want 0", executor.callCount())
	}
}

func TestCoordinatorRejectsInspectedHostClassMismatch(t *testing.T) {
	run := loadExampleRun(t)
	agents := makeAgents(t, 1, 100, run.HostClass)
	executor := &fakeShardExecutor{}
	hostClass := run.HostClass
	hostClass.CPUProfile = "graviton2"
	hostClass.ID = "linux-aarch64-kvm-graviton2-v0"
	coordinator, err := NewCoordinator(
		agents,
		map[string]CheckpointInspector{agents[0].AgentID: fakeBundleInspector{inspection: CheckpointInspection{
			CheckpointDigest: run.Checkpoint.Digest,
			ChildCount:       run.Children.Count,
			HostClass:        hostClass,
		}}},
		newFakeTerminalStore(),
		map[string]ShardExecutor{agents[0].AgentID: executor},
		CoordinatorOptions{},
	)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}

	_, err = coordinator.Run(context.Background(), run)
	if !errors.Is(err, ErrBundleMetadataMismatch) {
		t.Fatalf("BundleRun error = %v, want ErrBundleMetadataMismatch", err)
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
		t.Fatalf("BundleRun error = %v, want ErrPlatformMismatch", err)
	}
	if executor.callCount() != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.callCount())
	}
	if report.Summary.PlatformMismatches != 1 || report.Summary.State != "failed" {
		t.Fatalf("summary = %+v", report.Summary)
	}
}

func TestCoordinatorRetriesAfterLongFirstAttempt(t *testing.T) {
	run := smallRuntimeRun(t, 1, 2)
	agents := makeAgents(t, 1, 1, run.HostClass)
	store := newFakeTerminalStore()
	clock := time.Date(2026, 6, 20, 4, 0, 0, 0, time.UTC)
	executor := &fakeShardExecutor{
		fn: func(_ context.Context, req ShardExecutionRequest) ([]AttemptResult, error) {
			if req.Attempt == 1 {
				clock = clock.Add(11 * time.Minute)
				return []AttemptResult{
					runtimeAttemptResult(run, req.Lease, 0, req.Attempt, AttemptFailed, false),
				}, errors.New("transient runtime failure")
			}
			result := runtimeAttemptResult(run, req.Lease, 0, req.Attempt, AttemptSucceeded, true)
			store.commit(result)
			return []AttemptResult{result}, nil
		},
	}
	coordinator := newTestCoordinator(t, run, agents, store, map[string]ShardExecutor{agents[0].AgentID: executor}, func() time.Time { return clock })

	report, err := coordinator.Run(context.Background(), run)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if executor.callCount() != 2 || report.Summary.State != "succeeded" {
		t.Fatalf("executor calls = %d, summary = %+v", executor.callCount(), report.Summary)
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
		t.Fatalf("BundleRun error = %v, want ErrLeaseIncomplete", err)
	}
	if report.Summary.State != "failed" || report.Summary.CompletedChildren != 0 {
		t.Fatalf("summary = %+v", report.Summary)
	}
}

func smallRuntimeRun(t *testing.T, childCount int, attempts int) CheckpointRun {
	t.Helper()
	run := loadExampleRun(t)
	run.Children = ChildRange{Start: 0, Count: childCount}
	run.Execution.MaxInFlightPerAgent = childCount
	run.RetryPolicy.MaxAttemptsPerChild = attempts
	return run
}

func newTestCoordinator(
	t *testing.T,
	run CheckpointRun,
	agents []AgentStatus,
	store *fakeTerminalStore,
	executors map[string]ShardExecutor,
	now func() time.Time,
) *Coordinator {
	t.Helper()
	inspectors := make(map[string]CheckpointInspector, len(agents))
	for _, agent := range agents {
		inspectors[agent.AgentID] = fakeBundleInspector{inspection: CheckpointInspection{
			CheckpointDigest: run.Checkpoint.Digest,
			ChildCount:       run.Children.End(),
			HostClass:        run.HostClass,
		}}
	}
	coordinator, err := NewCoordinator(
		agents,
		inspectors,
		store,
		executors,
		CoordinatorOptions{Now: now},
	)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	return coordinator
}

type fakeBundleInspector struct {
	inspection CheckpointInspection
	err        error
	onInspect  func()
}

func (i fakeBundleInspector) InspectRunCheckpoint(context.Context, CheckpointRun) (CheckpointInspection, error) {
	if i.onInspect != nil {
		i.onInspect()
	}
	if i.err != nil {
		return CheckpointInspection{}, i.err
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

func (s *fakeTerminalStore) TerminalResult(_ context.Context, _ CheckpointRun, childID int) (AttemptResult, bool, error) {
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

func runtimeAttemptResult(run CheckpointRun, lease ShardLease, childID int, attempt int, status AttemptStatus, terminal bool) AttemptResult {
	now := time.Date(2026, 6, 20, 4, 0, 1, 0, time.UTC)
	result := AttemptResult{
		RunID:            run.RunID,
		CheckpointDigest: run.Checkpoint.Digest,
		ChildID:          childID,
		AttemptID:        FormatAttemptID(run.RunID, childID, attempt),
		AgentID:          lease.AgentID,
		ShardID:          lease.ShardID,
		Status:           status,
		StartedAt:        now,
		FinishedAt:       now.Add(105 * time.Millisecond),
		TimingsMS: AttemptTimings{
			ArtifactPull:    10,
			Materialization: 20,
			Resume:          30,
			GuestReady:      40,
			ResultCommit:    5,
		},
		Terminal: terminal,
	}
	if terminal {
		result.ResultURI = fmt.Sprintf("%schildren/%d/terminal.json", run.ResultStore, childID)
	}
	if status == AttemptFailed || status == AttemptPlatformMismatch {
		result.Error = &AttemptError{Code: "runtime.execution_failed", Message: "test failure"}
	}
	return result
}
