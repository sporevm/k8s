package fleet

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrCoordinatorNotConfigured means a required coordinator dependency is absent.
	ErrCoordinatorNotConfigured = errors.New("coordinator not configured")
	// ErrBundleMetadataMismatch means inspected bundle metadata does not match the run.
	ErrBundleMetadataMismatch = errors.New("bundle metadata mismatch")
	// ErrAgentExecutorMissing means no runtime executor is registered for a lease agent.
	ErrAgentExecutorMissing = errors.New("agent executor missing")
	// ErrLeaseExpired means a shard lease expired before or during execution.
	ErrLeaseExpired = errors.New("shard lease expired")
	// ErrLeaseIncomplete means a shard exhausted attempts without terminal results.
	ErrLeaseIncomplete = errors.New("shard lease incomplete")
	// ErrPlatformMismatch means an agent reported an incompatible SporeVM platform.
	ErrPlatformMismatch = errors.New("platform mismatch")
)

// BundleInspector validates immutable bundle metadata before run admission.
type BundleInspector interface {
	InspectRunBundle(context.Context, Run) (BundleInspection, error)
}

// TerminalResultReader checks whether a child already has a committed result.
type TerminalResultReader interface {
	TerminalResult(context.Context, Run, int) (AttemptResult, bool, error)
}

// ShardExecutor runs one shard lease on an agent.
type ShardExecutor interface {
	RunShard(context.Context, ShardExecutionRequest) ([]AttemptResult, error)
}

// PlanBuilder assigns shard leases for a validated run and agent inventory.
type PlanBuilder func(Run, []AgentStatus, DryRunOptions) (Plan, error)

// CoordinatorOptions configures a single-cell coordinator.
type CoordinatorOptions struct {
	Now         func() time.Time
	LeaseTTL    time.Duration
	PlanBuilder PlanBuilder
}

// Coordinator owns run admission and static single-cell shard execution.
type Coordinator struct {
	agents    []AgentStatus
	inspector BundleInspector
	terminal  TerminalResultReader
	executors map[string]ShardExecutor
	now       func() time.Time
	leaseTTL  time.Duration
	plan      PlanBuilder
}

// NewCoordinator creates a single-cell coordinator over a static agent inventory.
func NewCoordinator(
	agents []AgentStatus,
	inspector BundleInspector,
	terminal TerminalResultReader,
	executors map[string]ShardExecutor,
	opts CoordinatorOptions,
) (*Coordinator, error) {
	if inspector == nil || terminal == nil {
		return nil, ErrCoordinatorNotConfigured
	}
	if len(executors) == 0 {
		return nil, ErrCoordinatorNotConfigured
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	leaseTTL := opts.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = 10 * time.Minute
	}
	planBuilder := opts.PlanBuilder
	if planBuilder == nil {
		planBuilder = BuildDryRunPlan
	}
	copiedAgents := append([]AgentStatus(nil), agents...)
	copiedExecutors := make(map[string]ShardExecutor, len(executors))
	for agentID, executor := range executors {
		if executor == nil {
			return nil, ErrCoordinatorNotConfigured
		}
		copiedExecutors[agentID] = executor
	}
	return &Coordinator{
		agents:    copiedAgents,
		inspector: inspector,
		terminal:  terminal,
		executors: copiedExecutors,
		now:       now,
		leaseTTL:  leaseTTL,
		plan:      planBuilder,
	}, nil
}

// Run admits, plans, and executes one run across the coordinator's static agents.
func (c *Coordinator) Run(ctx context.Context, run Run) (RuntimeReport, error) {
	startedAt := c.now()
	if err := run.Validate(); err != nil {
		return RuntimeReport{}, err
	}
	inspection, err := c.inspector.InspectRunBundle(ctx, run)
	if err != nil {
		return RuntimeReport{}, err
	}
	if err := inspection.Validate(run); err != nil {
		return RuntimeReport{}, err
	}

	plan, err := c.plan(run, c.agents, DryRunOptions{
		Now:      startedAt,
		LeaseTTL: c.leaseTTL,
	})
	if err != nil {
		return RuntimeReport{Plan: plan}, err
	}
	if err := ValidateCompleteCoverage(run, plan.Leases); err != nil {
		return RuntimeReport{Plan: plan}, err
	}

	state := newRuntimeState(run, plan, startedAt)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	for _, lease := range plan.Leases {
		lease := lease
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.runLease(ctx, run, lease, state); err != nil {
				state.recordLeaseError()
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	summary := state.summary(c.now(), errors.Join(errs...))
	return RuntimeReport{
		Plan:    plan,
		Summary: summary,
	}, errors.Join(errs...)
}

// Validate checks inspected metadata against the submitted run.
func (i BundleInspection) Validate(run Run) error {
	if i.BundleDigest != run.Bundle.Digest {
		return fmt.Errorf("%w: inspected digest %q does not match run digest %q", ErrBundleMetadataMismatch, i.BundleDigest, run.Bundle.Digest)
	}
	if i.ChildCount < run.Children.End() {
		return fmt.Errorf("%w: bundle child count %d does not cover run end child %d", ErrBundleMetadataMismatch, i.ChildCount, run.Children.End())
	}
	if i.HostClass.ID != "" && !i.HostClass.Equal(run.HostClass) {
		return fmt.Errorf("%w: inspected host class %q does not match run host class %q", ErrBundleMetadataMismatch, i.HostClass.ID, run.HostClass.ID)
	}
	return nil
}

func (c *Coordinator) runLease(ctx context.Context, run Run, lease ShardLease, state *runtimeState) error {
	executor, ok := c.executors[lease.AgentID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrAgentExecutorMissing, lease.AgentID)
	}
	for attempt := 1; attempt <= lease.AttemptBudget; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if c.now().After(lease.LeaseDeadline) {
			return fmt.Errorf("%w: %s", ErrLeaseExpired, lease.ShardID)
		}
		pending, err := c.leaseHasPendingChildren(ctx, run, lease, state)
		if err != nil {
			return err
		}
		if !pending {
			return nil
		}

		results, err := executor.RunShard(ctx, ShardExecutionRequest{
			Run:     run,
			Lease:   lease,
			Attempt: attempt,
		})
		state.recordResults(results)
		if hasPlatformMismatch(results) {
			return fmt.Errorf("%w: %s", ErrPlatformMismatch, lease.ShardID)
		}
		pendingAfterAttempt, pendingErr := c.leaseHasPendingChildren(ctx, run, lease, state)
		if pendingErr != nil {
			return pendingErr
		}
		if !pendingAfterAttempt {
			return nil
		}
		if attempt == lease.AttemptBudget {
			if err != nil {
				return err
			}
			return fmt.Errorf("%w: %s", ErrLeaseIncomplete, lease.ShardID)
		}
	}
	return nil
}

func hasPlatformMismatch(results []AttemptResult) bool {
	for _, result := range results {
		if result.Status == AttemptPlatformMismatch {
			return true
		}
	}
	return false
}

func (c *Coordinator) leaseHasPendingChildren(ctx context.Context, run Run, lease ShardLease, state *runtimeState) (bool, error) {
	pending := false
	for childID := lease.ChildStart; childID < lease.ChildRange().End(); childID++ {
		if state.terminalKnown(childID) {
			continue
		}
		result, ok, err := c.terminal.TerminalResult(ctx, run, childID)
		if err != nil {
			return false, err
		}
		if ok {
			state.recordExistingTerminal(childID, result.Status)
			continue
		}
		pending = true
	}
	return pending, nil
}

type runtimeState struct {
	mu                  sync.Mutex
	run                 Run
	plan                Plan
	startedAt           time.Time
	terminal            map[int]AttemptStatus
	attemptCount        int
	platformMismatches  int
	nonTerminalFailures int
	leaseErrors         int
}

func newRuntimeState(run Run, plan Plan, startedAt time.Time) *runtimeState {
	return &runtimeState{
		run:       run,
		plan:      plan,
		startedAt: startedAt,
		terminal:  make(map[int]AttemptStatus, run.Children.Count),
	}
}

func (s *runtimeState) terminalKnown(childID int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.terminal[childID]
	return ok
}

func (s *runtimeState) recordExistingTerminal(childID int, status AttemptStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.terminal[childID]; ok {
		return
	}
	if status == "" {
		status = AttemptSkippedTerminalExists
	}
	s.terminal[childID] = status
}

func (s *runtimeState) recordResults(results []AttemptResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, result := range results {
		if result.AttemptID == "" {
			continue
		}
		s.attemptCount++
		if result.Status == AttemptPlatformMismatch {
			s.platformMismatches++
		}
		if result.Status == AttemptFailed && !result.Terminal {
			s.nonTerminalFailures++
		}
		if !result.Terminal {
			continue
		}
		if _, exists := s.terminal[result.ChildID]; exists && result.Status == AttemptSkippedTerminalExists {
			continue
		}
		s.terminal[result.ChildID] = result.Status
	}
}

func (s *runtimeState) recordLeaseError() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leaseErrors++
}

func (s *runtimeState) summary(finishedAt time.Time, runErr error) RuntimeSummary {
	s.mu.Lock()
	defer s.mu.Unlock()

	summary := RuntimeSummary{
		RunID:               s.run.RunID,
		State:               "succeeded",
		ChildCount:          s.run.Children.Count,
		ShardCount:          s.plan.Summary.ShardCount,
		AssignedChildren:    s.plan.Summary.AssignedChildren,
		AssignedShards:      s.plan.Summary.AssignedShards,
		AttemptCount:        s.attemptCount,
		CompletedChildren:   len(s.terminal),
		PlatformMismatches:  s.platformMismatches,
		NonTerminalFailures: s.nonTerminalFailures,
		LeaseErrors:         s.leaseErrors,
		StartedAt:           s.startedAt,
		FinishedAt:          finishedAt,
	}
	for _, status := range s.terminal {
		switch status {
		case AttemptSucceeded:
			summary.SucceededChildren++
		case AttemptFailed:
			summary.FailedChildren++
		case AttemptSkippedTerminalExists:
			summary.SkippedTerminalChildren++
		}
	}
	if runErr != nil || summary.CompletedChildren != summary.ChildCount || summary.FailedChildren > 0 || summary.PlatformMismatches > 0 {
		summary.State = "failed"
	}
	return summary
}
