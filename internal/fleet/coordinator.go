package fleet

import (
	"errors"
	"fmt"
	"sort"
)

var (
	// ErrNoCompatibleAgents means no healthy agent matches the run host class.
	ErrNoCompatibleAgents = errors.New("no compatible agents")
	// ErrInsufficientCapacity means compatible agents cannot cover the run.
	ErrInsufficientCapacity = errors.New("insufficient agent capacity")
)

type candidateAgent struct {
	status    AgentStatus
	remaining int
}

// RequiredInFlightSlots returns the child slots needed to run a lease without
// exceeding the run's per-agent concurrency limit.
func RequiredInFlightSlots(childCount int, execution Execution) int {
	return min(childCount, execution.MaxInFlightPerAgent)
}

// ShardSlotDemand returns the execution slots a lease can consume at once.
func ShardSlotDemand(run CheckpointRun, lease ShardLease) int {
	demand := RequiredInFlightSlots(lease.ChildCount, run.Execution)
	if lease.MaxInFlight > 0 {
		demand = min(demand, lease.MaxInFlight)
	}
	return demand
}

// BuildPlan divides the global child range across compatible agents in proportion
// to their available execution slots. Each agent receives one bounded lease.
func BuildPlan(run CheckpointRun, agents []AgentStatus) (Plan, error) {
	if err := run.Validate(); err != nil {
		return Plan{}, err
	}

	candidates, summary, err := candidateAgents(run, agents)
	summary.RunID = run.RunID
	summary.State = "refused"
	summary.ChildCount = run.Children.Count
	if err != nil {
		return Plan{Summary: summary}, err
	}

	if len(candidates) == 0 {
		return Plan{Summary: summary}, ErrInsufficientCapacity
	}

	var leases []ShardLease
	remaining := run.Children.Count
	slots := summary.AvailableChildSlots
	start := run.Children.Start
	for _, candidate := range candidates {
		count := remaining/slots*candidate.remaining + (remaining%slots)*candidate.remaining/slots
		slots -= candidate.remaining
		if count == 0 {
			continue
		}
		leases = append(leases, ShardLease{
			RunID:            run.RunID,
			CheckpointDigest: run.Checkpoint.Digest,
			ShardID:          fmt.Sprintf("%s-shard-%04d", run.RunID, len(leases)),
			ChildStart:       start,
			ChildCount:       count,
			MaxInFlight:      min(count, candidate.remaining),
			AttemptBudget:    run.RetryPolicy.MaxAttemptsPerChild,
			HostClassID:      run.HostClass.ID,
			AgentID:          candidate.status.AgentID,
		})
		start += count
		remaining -= count
	}

	summary.ShardCount = len(leases)
	summary.AssignedShards = len(leases)
	summary.AssignedChildren = run.Children.Count
	summary.State = "planned"
	return Plan{Summary: summary, Leases: leases}, nil
}

func candidateAgents(run CheckpointRun, agents []AgentStatus) ([]candidateAgent, PlanSummary, error) {
	summary := PlanSummary{AgentCount: len(agents)}
	candidates := make([]candidateAgent, 0, len(agents))
	seen := make(map[string]bool, len(agents))

	sorted := append([]AgentStatus(nil), agents...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].AgentID < sorted[j].AgentID
	})

	for _, agent := range sorted {
		if err := agent.Validate(); err != nil {
			return nil, summary, err
		}
		if seen[agent.AgentID] {
			return nil, summary, contractError("duplicate agentID %q", agent.AgentID)
		}
		seen[agent.AgentID] = true
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
