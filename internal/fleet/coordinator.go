package fleet

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

var (
	// ErrNoCompatibleAgents means no healthy agent matches the run host class.
	ErrNoCompatibleAgents = errors.New("no compatible agents")
	// ErrInsufficientCapacity means compatible agents cannot cover the run.
	ErrInsufficientCapacity = errors.New("insufficient agent capacity")
)

// DryRunOptions controls deterministic lease construction.
type DryRunOptions struct {
	Now      time.Time
	LeaseTTL time.Duration
}

type candidateAgent struct {
	status    AgentStatus
	remaining int
}

// RequiredInFlightSlots returns the child slots needed to run a lease without
// exceeding the run's shard and per-agent concurrency limits.
func RequiredInFlightSlots(childCount int, execution Execution) int {
	return min(childCount, execution.ChildrenPerShard, execution.MaxInFlightPerAgent)
}

// ShardSlotDemand returns the execution slots a lease can consume at once.
func ShardSlotDemand(run Run, lease ShardLease) int {
	return RequiredInFlightSlots(lease.ChildCount, run.Execution)
}

// DeriveShardRanges expands a run into non-overlapping global child ranges.
func DeriveShardRanges(run Run) ([]ChildRange, error) {
	if err := run.Validate(); err != nil {
		return nil, err
	}

	ranges := make([]ChildRange, 0, (run.Children.Count+run.Execution.ChildrenPerShard-1)/run.Execution.ChildrenPerShard)
	for child := run.Children.Start; child < run.Children.End(); {
		count := min(run.Execution.ChildrenPerShard, run.Children.End()-child)
		ranges = append(ranges, ChildRange{Start: child, Count: count})
		child += count
	}
	return ranges, nil
}

// BuildDryRunPlan assigns run shards to a static agent inventory.
//
// The dry-run is deliberately strict: because the first target is a fully
// concurrent 1,000-child run, compatible available child slots must cover the
// full child count before any leases are emitted.
func BuildDryRunPlan(run Run, agents []AgentStatus, opts DryRunOptions) (Plan, error) {
	if err := run.Validate(); err != nil {
		return Plan{}, err
	}
	opts = normalizeDryRunOptions(opts)

	ranges, err := DeriveShardRanges(run)
	if err != nil {
		return Plan{}, err
	}

	candidates, summary, err := candidateAgents(run, agents)
	summary.RunID = run.RunID
	summary.State = "refused"
	summary.ChildCount = run.Children.Count
	summary.ShardCount = len(ranges)
	if err != nil {
		return Plan{Summary: summary}, err
	}
	if summary.AvailableChildSlots < run.Children.Count {
		return Plan{Summary: summary}, fmt.Errorf(
			"%w: need %d compatible child slots, have %d",
			ErrInsufficientCapacity,
			run.Children.Count,
			summary.AvailableChildSlots,
		)
	}

	leases := make([]ShardLease, 0, len(ranges))
	for index, childRange := range ranges {
		candidateIndex := firstCandidateWithCapacity(candidates, childRange.Count)
		if candidateIndex < 0 {
			return Plan{Summary: summary, Leases: leases}, fmt.Errorf(
				"%w: no agent can fit shard %s-shard-%04d with %d children",
				ErrInsufficientCapacity,
				run.RunID,
				index,
				childRange.Count,
			)
		}

		candidate := &candidates[candidateIndex]
		candidate.remaining -= childRange.Count
		leases = append(leases, ShardLease{
			RunID:         run.RunID,
			BundleDigest:  run.Bundle.Digest,
			ShardID:       fmt.Sprintf("%s-shard-%04d", run.RunID, index),
			ChildStart:    childRange.Start,
			ChildCount:    childRange.Count,
			AttemptBudget: run.RetryPolicy.MaxAttemptsPerChild,
			HostClassID:   run.HostClass.ID,
			AgentID:       candidate.status.AgentID,
			LeaseDeadline: opts.Now.Add(opts.LeaseTTL).UTC(),
		})
	}

	summary.AssignedShards = len(leases)
	for _, lease := range leases {
		summary.AssignedChildren += lease.ChildCount
	}
	summary.State = "planned"
	return Plan{Summary: summary, Leases: leases}, nil
}

// BuildSingleAgentSequentialPlan assigns an entire run to one compatible agent.
//
// This is used for local generic runs where the prepared bundle only exists on
// the preparing agent. The lease may cover more children than the agent can run
// concurrently; execution remains bounded by RequiredInFlightSlots.
func BuildSingleAgentSequentialPlan(run Run, agents []AgentStatus, opts DryRunOptions) (Plan, error) {
	if err := run.Validate(); err != nil {
		return Plan{}, err
	}
	opts = normalizeDryRunOptions(opts)

	candidates, summary, err := candidateAgents(run, agents)
	summary.RunID = run.RunID
	summary.State = "refused"
	summary.ChildCount = run.Children.Count
	summary.ShardCount = 1
	if err != nil {
		return Plan{Summary: summary}, err
	}

	required := RequiredInFlightSlots(run.Children.Count, run.Execution)
	candidateIndex := firstCandidateWithCapacity(candidates, required)
	if candidateIndex < 0 {
		return Plan{Summary: summary}, fmt.Errorf(
			"%w: need %d compatible in-flight child slots on one agent, have %d",
			ErrInsufficientCapacity,
			required,
			bestCandidateCapacity(candidates),
		)
	}

	candidate := candidates[candidateIndex]
	lease := ShardLease{
		RunID:         run.RunID,
		BundleDigest:  run.Bundle.Digest,
		ShardID:       fmt.Sprintf("%s-shard-%04d", run.RunID, 0),
		ChildStart:    run.Children.Start,
		ChildCount:    run.Children.Count,
		AttemptBudget: run.RetryPolicy.MaxAttemptsPerChild,
		HostClassID:   run.HostClass.ID,
		AgentID:       candidate.status.AgentID,
		LeaseDeadline: opts.Now.Add(opts.LeaseTTL).UTC(),
	}

	summary.AssignedShards = 1
	summary.AssignedChildren = run.Children.Count
	summary.State = "planned"
	return Plan{Summary: summary, Leases: []ShardLease{lease}}, nil
}

// ValidateCompleteCoverage checks that shard leases exactly cover the run.
func ValidateCompleteCoverage(run Run, leases []ShardLease) error {
	if err := run.Validate(); err != nil {
		return err
	}
	if len(leases) == 0 {
		return contractError("no shard leases provided")
	}

	sorted := append([]ShardLease(nil), leases...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ChildStart < sorted[j].ChildStart
	})

	cursor := run.Children.Start
	for _, lease := range sorted {
		if err := lease.Validate(run); err != nil {
			return err
		}
		if lease.ChildStart < cursor {
			return contractError("shard leases must not overlap")
		}
		if lease.ChildStart != cursor {
			return contractError("shard leases have a gap")
		}
		cursor = lease.ChildRange().End()
	}
	if cursor != run.Children.End() {
		return contractError("shard leases do not cover the run child range")
	}
	return nil
}

func candidateAgents(run Run, agents []AgentStatus) ([]candidateAgent, PlanSummary, error) {
	summary := PlanSummary{AgentCount: len(agents)}
	candidates := make([]candidateAgent, 0, len(agents))

	sorted := append([]AgentStatus(nil), agents...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].AgentID < sorted[j].AgentID
	})

	for _, agent := range sorted {
		if err := agent.Validate(); err != nil {
			return nil, summary, err
		}
		if agent.Healthy {
			summary.HealthyAgents++
		}
		if !agent.Healthy || agent.Pressure.Critical() || !agent.HostClass.Equal(run.HostClass) {
			continue
		}

		summary.CompatibleAgents++
		available := min(agent.ExecutionSlots.Available, run.Execution.MaxInFlightPerAgent)
		summary.AvailableChildSlots += available
		if available > 0 {
			candidates = append(candidates, candidateAgent{
				status:    agent,
				remaining: available,
			})
		}
	}

	if summary.CompatibleAgents == 0 {
		return nil, summary, ErrNoCompatibleAgents
	}
	return candidates, summary, nil
}

func firstCandidateWithCapacity(candidates []candidateAgent, required int) int {
	for i, candidate := range candidates {
		if candidate.remaining >= required {
			return i
		}
	}
	return -1
}

func bestCandidateCapacity(candidates []candidateAgent) int {
	best := 0
	for _, candidate := range candidates {
		if candidate.remaining > best {
			best = candidate.remaining
		}
	}
	return best
}

func normalizeDryRunOptions(opts DryRunOptions) DryRunOptions {
	if opts.Now.IsZero() {
		opts.Now = time.Now().UTC()
	}
	if opts.LeaseTTL <= 0 {
		opts.LeaseTTL = 10 * time.Minute
	}
	return opts
}
