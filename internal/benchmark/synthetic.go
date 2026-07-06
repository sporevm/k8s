package benchmark

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sporevm/k8s/internal/fleet"
)

const (
	defaultAgentCount    = 10
	defaultSlotsPerAgent = 100
)

// SyntheticOptions configures the local coordinator benchmark harness.
type SyntheticOptions struct {
	AgentCount        int
	SlotsPerAgent     int
	TargetConcurrency int
	CachePosture      CachePosture
	SubmittedAt       time.Time
	AdmissionDelay    time.Duration
}

// SyntheticResult contains the coordinator report and benchmark summary.
type SyntheticResult struct {
	Report       fleet.RuntimeReport
	Summary      Summary
	Observations []Observation
}

// RunSynthetic runs the single-cell coordinator path with deterministic executors.
func RunSynthetic(ctx context.Context, run fleet.BundleRun, opts SyntheticOptions) (SyntheticResult, error) {
	if err := run.Validate(); err != nil {
		return SyntheticResult{}, err
	}
	opts = defaultSyntheticOptions(run, opts)
	if err := validateSyntheticOptions(run, opts); err != nil {
		return SyntheticResult{}, err
	}

	state := newSyntheticState(run, opts)
	agents := syntheticAgents(run, opts)
	executors := make(map[string]fleet.ShardExecutor, len(agents))
	for _, agent := range agents {
		executors[agent.AgentID] = syntheticExecutor{state: state}
	}

	coordinator, err := fleet.NewCoordinator(
		agents,
		syntheticInspector{inspection: fleet.BundleInspection{
			BundleDigest: run.Bundle.Digest,
			ChildCount:   run.Children.End(),
			HostClass:    run.HostClass,
		}},
		state,
		executors,
		fleet.CoordinatorOptions{
			Now: func() time.Time { return opts.SubmittedAt.Add(opts.AdmissionDelay) },
		},
	)
	if err != nil {
		return SyntheticResult{}, err
	}

	report, err := coordinator.Run(ctx, run)
	if err != nil {
		return SyntheticResult{}, err
	}
	observations := state.snapshotObservations()
	summary, err := BuildSummary(report, observations, SummaryOptions{
		TargetConcurrency: opts.TargetConcurrency,
		CachePosture:      opts.CachePosture,
		SubmittedAt:       opts.SubmittedAt,
		AdmittedAt:        opts.SubmittedAt.Add(opts.AdmissionDelay),
	})
	if err != nil {
		return SyntheticResult{}, err
	}
	return SyntheticResult{
		Report:       report,
		Summary:      summary,
		Observations: observations,
	}, nil
}

func defaultSyntheticOptions(run fleet.BundleRun, opts SyntheticOptions) SyntheticOptions {
	if opts.AgentCount == 0 {
		opts.AgentCount = defaultAgentCount
	}
	if opts.SlotsPerAgent == 0 {
		opts.SlotsPerAgent = defaultSlotsPerAgent
	}
	if opts.TargetConcurrency == 0 {
		opts.TargetConcurrency = run.Children.Count
	}
	if opts.CachePosture == "" {
		opts.CachePosture = CachePostureWarmBundleColdMaterialization
	}
	if opts.SubmittedAt.IsZero() {
		opts.SubmittedAt = time.Date(2026, 6, 20, 4, 0, 0, 0, time.UTC)
	}
	if opts.AdmissionDelay == 0 {
		opts.AdmissionDelay = 50 * time.Millisecond
	}
	return opts
}

func validateSyntheticOptions(run fleet.BundleRun, opts SyntheticOptions) error {
	if opts.AgentCount < 1 {
		return fmt.Errorf("%w: synthetic agent count must be >= 1", ErrInvalidBenchmark)
	}
	if opts.SlotsPerAgent < 1 {
		return fmt.Errorf("%w: synthetic slots per agent must be >= 1", ErrInvalidBenchmark)
	}
	if opts.TargetConcurrency < 1 || opts.TargetConcurrency > run.Children.Count {
		return fmt.Errorf("%w: synthetic target concurrency must be between 1 and child count", ErrInvalidBenchmark)
	}
	if !validCachePosture(opts.CachePosture) {
		return fmt.Errorf("%w: unsupported cache posture %q", ErrInvalidBenchmark, opts.CachePosture)
	}
	if opts.AdmissionDelay < 0 {
		return fmt.Errorf("%w: synthetic admission delay must be >= 0", ErrInvalidBenchmark)
	}
	return nil
}

func syntheticAgents(run fleet.BundleRun, opts SyntheticOptions) []fleet.AgentStatus {
	agents := make([]fleet.AgentStatus, 0, opts.AgentCount)
	for i := 0; i < opts.AgentCount; i++ {
		agents = append(agents, fleet.AgentStatus{
			AgentID:    fmt.Sprintf("spore-agent-synthetic-%04d", i+1),
			CellID:     "synthetic-cell-0001",
			ObservedAt: opts.SubmittedAt,
			HostClass:  run.HostClass,
			ExecutionSlots: fleet.ExecutionSlots{
				Total:     opts.SlotsPerAgent,
				Available: opts.SlotsPerAgent,
			},
			Pressure: fleet.Pressure{
				Disk:   fleet.PressureNormal,
				Memory: fleet.PressureNormal,
			},
			Healthy: true,
		})
	}
	return agents
}

type syntheticInspector struct {
	inspection fleet.BundleInspection
}

func (i syntheticInspector) InspectRunBundle(context.Context, fleet.BundleRun) (fleet.BundleInspection, error) {
	return i.inspection, nil
}

type syntheticState struct {
	mu           sync.Mutex
	run          fleet.BundleRun
	opts         SyntheticOptions
	terminal     map[int]fleet.AttemptResult
	observations []Observation
}

func newSyntheticState(run fleet.BundleRun, opts SyntheticOptions) *syntheticState {
	return &syntheticState{
		run:      run,
		opts:     opts,
		terminal: make(map[int]fleet.AttemptResult, run.Children.Count),
	}
}

func (s *syntheticState) TerminalResult(_ context.Context, _ fleet.BundleRun, childID int) (fleet.AttemptResult, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, ok := s.terminal[childID]
	return result, ok, nil
}

func (s *syntheticState) commit(result fleet.AttemptResult, observation Observation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.terminal[result.ChildID] = result
	s.observations = append(s.observations, observation)
}

func (s *syntheticState) snapshotObservations() []Observation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Observation(nil), s.observations...)
}

type syntheticExecutor struct {
	state *syntheticState
}

func (e syntheticExecutor) RunShard(ctx context.Context, req fleet.ShardExecutionRequest) ([]fleet.AttemptResult, error) {
	results := make([]fleet.AttemptResult, 0, req.Lease.ChildCount)
	for childID := req.Lease.ChildStart; childID < req.Lease.ChildRange().End(); childID++ {
		if err := ctx.Err(); err != nil {
			return results, err
		}
		observation := syntheticObservation(e.state.run, e.state.opts, req.Lease, childID)
		result := syntheticAttemptResult(e.state.run, req.Lease, childID, req.Attempt, observation)
		e.state.commit(result, observation)
		results = append(results, result)
	}
	return results, nil
}

func syntheticObservation(run fleet.BundleRun, opts SyntheticOptions, lease fleet.ShardLease, childID int) Observation {
	localIndex := childID - lease.ChildStart
	startOffsetMS := float64(localIndex % max(1, run.Execution.MaxInFlightPerAgent))
	timings := syntheticTimings(childID)
	originBytes, hits, misses := syntheticCacheMetrics(opts.CachePosture, childID)
	return Observation{
		RunID:           run.RunID,
		ChildID:         childID,
		Succeeded:       true,
		StartedAt:       opts.SubmittedAt.Add(opts.AdmissionDelay).Add(10 * time.Millisecond).Add(durationMS(startOffsetMS)),
		TimingsMS:       timings,
		OriginBytesRead: originBytes,
		CacheHitCount:   hits,
		CacheMissCount:  misses,
	}
}

func syntheticTimings(childID int) StageTimings {
	return StageTimings{
		ArtifactPull:    12 + float64(childID%11),
		Materialization: 90 + float64(childID%17),
		Resume:          20 + float64(childID%7),
		GuestReady:      700 + float64(childID%101),
		ResultCommit:    15 + float64(childID%13),
	}
}

func syntheticCacheMetrics(posture CachePosture, childID int) (int64, int, int) {
	switch posture {
	case CachePostureColdOriginColdCache:
		return int64(1_048_576 + (childID%8)*4096), 2, 10
	case CachePostureWarmNodeLocalCache:
		return 0, 12, 0
	default:
		return int64(8192 + (childID%4)*1024), 8, 3
	}
}

func syntheticAttemptResult(run fleet.BundleRun, lease fleet.ShardLease, childID int, attempt int, observation Observation) fleet.AttemptResult {
	finishedAt := observation.ReadyAt().Add(durationMS(observation.TimingsMS.ResultCommit))
	return fleet.AttemptResult{
		RunID:        run.RunID,
		BundleDigest: run.Bundle.Digest,
		ChildID:      childID,
		AttemptID:    fleet.FormatAttemptID(run.RunID, childID, attempt),
		AgentID:      lease.AgentID,
		ShardID:      lease.ShardID,
		Status:       fleet.AttemptSucceeded,
		StartedAt:    observation.StartedAt,
		FinishedAt:   finishedAt,
		TimingsMS: fleet.AttemptTimings{
			ArtifactPull:    observation.TimingsMS.ArtifactPull,
			Materialization: observation.TimingsMS.Materialization,
			Resume:          observation.TimingsMS.Resume,
			GuestReady:      observation.TimingsMS.GuestReady,
			ResultCommit:    observation.TimingsMS.ResultCommit,
		},
		Terminal:  true,
		ResultURI: run.ResultStore + "children/" + fmt.Sprint(childID) + "/terminal.json",
	}
}
