// Package agent contains the node-local SporeVM agent skeleton.
package agent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sporevm/k8s/internal/fleet"
)

// ponytail: inline previews keep terminal results useful; move to log object URIs when output size matters.
const attemptOutputPreviewLimit = 16 * 1024

var (
	// ErrOversubscribed means the agent cannot reserve the requested slots.
	ErrOversubscribed = errors.New("agent execution slots oversubscribed")
	// ErrUnsafePressure means resource pressure forbids new work.
	ErrUnsafePressure = errors.New("agent resource pressure is unsafe")
	// ErrInvalidLease means the shard lease is not safe to admit.
	ErrInvalidLease = errors.New("invalid shard lease")
	// ErrInvalidPressure means pressure data is missing or unsupported.
	ErrInvalidPressure = errors.New("invalid pressure")
	// ErrSporeClientNotConfigured means the runner cannot invoke SporeVM.
	ErrSporeClientNotConfigured = errors.New("spore client not configured")
	// ErrChildOutsideLease means a child id is not assigned by the shard lease.
	ErrChildOutsideLease = errors.New("child outside shard lease")
	// ErrWorkRootNotConfigured means child materialization has no safe parent.
	ErrWorkRootNotConfigured = errors.New("work root not configured")
)

// SporeClient is the narrow boundary to the SporeVM machine contract.
//
// The concrete subprocess implementation should wait for SporeVM's stable JSON
// contract. Tests and coordinator dry-runs can use fakes behind this interface.
type SporeClient interface {
	HostInfo(context.Context) (HostInfo, error)
	InspectBundle(context.Context, InspectBundleRequest) (InspectBundleResult, error)
	RunCapture(context.Context, RunCaptureRequest) ([]RunEvent, error)
	Fork(context.Context, ForkRequest) error
	Pack(context.Context, PackRequest) error
	Pull(context.Context, PullRequest) (PullResult, error)
	Resume(context.Context, ResumeRequest) ([]RunEvent, error)
	Exec(context.Context, ExecRequest) ([]RunEvent, error)
	RemoveVM(context.Context, RemoveVMRequest) error
}

// RunnerOption configures a node-local Runner.
type RunnerOption func(*Runner)

// WithSporeClient configures the SporeVM machine-interface client.
func WithSporeClient(client SporeClient) RunnerOption {
	return func(r *Runner) {
		r.client = client
	}
}

// WithResultStore configures per-child attempt and terminal result storage.
func WithResultStore(store ResultStore) RunnerOption {
	return func(r *Runner) {
		r.results = store
	}
}

// WithWorkRoot configures the parent directory for materialized child spores.
func WithWorkRoot(root string) RunnerOption {
	return func(r *Runner) {
		r.workRoot = root
	}
}

// WithBackend configures the SporeVM backend requested for resume operations.
func WithBackend(backend string) RunnerOption {
	return func(r *Runner) {
		r.backend = backend
	}
}

// WithChildTimeout configures an optional per-child resume timeout.
func WithChildTimeout(timeout time.Duration) RunnerOption {
	return func(r *Runner) {
		r.childTimeout = timeout
	}
}

// WithMetricsSink configures an in-process sink for child attempt metrics.
func WithMetricsSink(sink MetricsSink) RunnerOption {
	return func(r *Runner) {
		r.metrics = sink
	}
}

// MetricsSink receives child attempt metrics after the attempt record is written.
type MetricsSink interface {
	ObserveAttempt(AttemptMetrics)
}

// AttemptMetrics is the agent-local metric surface for one child attempt.
type AttemptMetrics struct {
	RunID                    string
	BundleDigest             string
	ChildID                  int
	AttemptID                string
	AgentID                  string
	ShardID                  string
	Status                   fleet.AttemptStatus
	TimingsMS                fleet.AttemptTimings
	OriginBytesRead          int64
	PeerBytesRead            int64
	ChunkCacheHits           int
	ChunkCacheMisses         int
	RootFSCacheHits          int
	RootFSCacheMisses        int
	MaterializedChunkCount   int
	LinkedChunkCount         int
	CopiedChunkCount         int
	PlatformMismatch         bool
	TerminalCommitAttempted  bool
	TerminalCommitSuccessful bool
}

// SlotLimiter accounts for child execution slots on a node agent.
type SlotLimiter struct {
	mu    sync.Mutex
	total int
	used  int
}

// NewSlotLimiter creates a limiter with total execution slots.
func NewSlotLimiter(total int) (*SlotLimiter, error) {
	if total < 1 {
		return nil, errors.New("total slots must be >= 1")
	}
	return &SlotLimiter{total: total}, nil
}

// Total returns the total configured slot count.
func (s *SlotLimiter) Total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total
}

// Available returns the current available slot count.
func (s *SlotLimiter) Available() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total - s.used
}

// TryAcquire reserves n slots and returns a release function.
func (s *SlotLimiter) TryAcquire(n int) (func(), bool) {
	if n < 1 {
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.total-s.used < n {
		return nil, false
	}
	s.used += n

	var once sync.Once
	release := func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.used -= n
		})
	}
	return release, true
}

// Runner owns node-local admission state for shard execution.
type Runner struct {
	slots        *SlotLimiter
	client       SporeClient
	results      ResultStore
	workRoot     string
	backend      string
	now          func() time.Time
	metrics      MetricsSink
	childTimeout time.Duration
}

// NewRunner creates a Runner with totalSlots local child execution slots.
func NewRunner(totalSlots int, opts ...RunnerOption) (*Runner, error) {
	slots, err := NewSlotLimiter(totalSlots)
	if err != nil {
		return nil, err
	}
	runner := &Runner{
		slots:    slots,
		workRoot: filepath.Join(os.TempDir(), "sporevm-agent"),
		backend:  "kvm",
		now:      func() time.Time { return time.Now().UTC() },
	}
	for _, opt := range opts {
		opt(runner)
	}
	return runner, nil
}

// AvailableSlots returns the runner's available child execution slots.
func (r *Runner) AvailableSlots() int {
	return r.slots.Available()
}

// AdmitShard reserves slots for a shard until the returned release is called.
func (r *Runner) AdmitShard(lease fleet.ShardLease, pressure fleet.Pressure) (func(), error) {
	if lease.ChildCount < 1 {
		return nil, ErrInvalidLease
	}
	if err := pressure.Validate("pressure"); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidPressure, err)
	}
	if pressure.Critical() {
		return nil, ErrUnsafePressure
	}
	release, ok := r.slots.TryAcquire(lease.ChildCount)
	if !ok {
		return nil, ErrOversubscribed
	}
	return release, nil
}

// StatusRequest describes dynamic data needed to report compact agent status.
type StatusRequest struct {
	AgentID    string
	CellID     string
	Backend    string
	Cache      fleet.CacheStatus
	Pressure   fleet.Pressure
	ObservedAt time.Time
}

// PrepareBundleRequest describes one local generic-run preparation.
type PrepareBundleRequest struct {
	Run     fleet.GenericRun
	Backend string
}

// Status reports the runner's current fleet status using `spore host-info`.
func (r *Runner) Status(ctx context.Context, req StatusRequest) (fleet.AgentStatus, error) {
	if r.client == nil {
		return fleet.AgentStatus{}, ErrSporeClientNotConfigured
	}
	backend := req.Backend
	if backend == "" {
		backend = r.backend
	}
	observedAt := req.ObservedAt
	if observedAt.IsZero() {
		observedAt = r.now()
	}

	info, err := r.client.HostInfo(ctx)
	if err != nil {
		return fleet.AgentStatus{}, err
	}
	hostClass, err := info.FleetHostClass(backend)
	if err != nil {
		return fleet.AgentStatus{}, err
	}
	status := fleet.AgentStatus{
		AgentID:    req.AgentID,
		CellID:     req.CellID,
		ObservedAt: observedAt,
		HostClass:  hostClass,
		ExecutionSlots: fleet.ExecutionSlots{
			Total:     r.slots.Total(),
			Available: r.slots.Available(),
		},
		Cache:    req.Cache,
		Pressure: req.Pressure,
		Healthy:  !req.Pressure.Critical(),
	}
	return status, status.Validate()
}

// PrepareBundle prepares, forks, packs, and inspects a generic run bundle locally.
func (r *Runner) PrepareBundle(ctx context.Context, req PrepareBundleRequest) (fleet.PreparedBundle, error) {
	if r.client == nil {
		return fleet.PreparedBundle{}, ErrSporeClientNotConfigured
	}
	if r.workRoot == "" {
		return fleet.PreparedBundle{}, ErrWorkRootNotConfigured
	}
	if err := req.Run.Validate(); err != nil {
		return fleet.PreparedBundle{}, err
	}

	backend := req.Backend
	if backend == "" {
		backend = r.backend
	}
	info, err := r.client.HostInfo(ctx)
	if err != nil {
		return fleet.PreparedBundle{}, err
	}
	hostClass, err := info.FleetHostClass(backend)
	if err != nil {
		return fleet.PreparedBundle{}, err
	}

	root := r.prepareWorkDir(req.Run)
	parentDir := filepath.Join(root, "parent.spore")
	childrenDir := filepath.Join(root, "children")
	bundleDir := filepath.Join(root, "bundle")
	if err := os.RemoveAll(root); err != nil {
		return fleet.PreparedBundle{}, err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return fleet.PreparedBundle{}, err
	}

	events, err := r.client.RunCapture(ctx, RunCaptureRequest{
		Image:         req.Run.Source.Image,
		CaptureDir:    parentDir,
		CaptureSignal: req.Run.Prepare.CaptureSignal,
		ReadyMarker:   req.Run.Prepare.ReadyMarker,
		Backend:       backend,
		Command:       req.Run.Prepare.Command,
	})
	if err != nil {
		return fleet.PreparedBundle{}, err
	}
	terminal, err := TerminalEvent(events)
	if err != nil {
		return fleet.PreparedBundle{}, err
	}
	if !terminal.Captured || terminal.CapturePath == nil {
		return fleet.PreparedBundle{}, invalidMachineOutput("prepare did not capture parent")
	}
	if err := r.client.Fork(ctx, ForkRequest{
		ParentDir: parentDir,
		Count:     req.Run.Fork.Count,
		OutDir:    childrenDir,
	}); err != nil {
		return fleet.PreparedBundle{}, err
	}
	if err := r.client.Pack(ctx, PackRequest{
		ParentDir:   parentDir,
		ChildrenDir: childrenDir,
		OutDir:      bundleDir,
	}); err != nil {
		return fleet.PreparedBundle{}, err
	}

	bundleURI, err := fileURI(bundleDir)
	if err != nil {
		return fleet.PreparedBundle{}, err
	}
	inspection, err := r.client.InspectBundle(ctx, InspectBundleRequest{
		Source: bundleURI,
		ChildRange: &ChildRangeSelection{
			Start: req.Run.Children.Start,
			End:   req.Run.Children.ChildRange().End(),
		},
	})
	if err != nil {
		return fleet.PreparedBundle{}, err
	}
	prepared := fleet.PreparedBundle{
		Bundle: fleet.Bundle{
			URI:    bundleURI,
			Digest: inspection.BundleDigest.String(),
		},
		ChildCount: inspection.ChildCount,
		HostClass:  hostClass,
	}
	if _, err := req.Run.Compile(prepared); err != nil {
		return fleet.PreparedBundle{}, err
	}
	return prepared, nil
}

// RunChildRequest describes one child execution attempt inside a shard lease.
type RunChildRequest struct {
	Run                     fleet.Run
	Lease                   fleet.ShardLease
	ChildID                 int
	Attempt                 int
	Pressure                fleet.Pressure
	Region                  string
	AllowMetadataOnlyRootFS bool
	Backend                 string
}

// RunShardRequest describes execution of every child assigned by a shard lease.
type RunShardRequest struct {
	Run                     fleet.Run
	Lease                   fleet.ShardLease
	Attempt                 int
	Pressure                fleet.Pressure
	Region                  string
	AllowMetadataOnlyRootFS bool
	Backend                 string
}

// RunChild runs one child attempt with a single execution slot reservation.
func (r *Runner) RunChild(ctx context.Context, req RunChildRequest) (fleet.AttemptResult, error) {
	if err := r.validateRunChildRequest(req); err != nil {
		return fleet.AttemptResult{}, err
	}
	slotLease := req.Lease
	slotLease.ChildStart = req.ChildID
	slotLease.ChildCount = 1
	release, err := r.AdmitShard(slotLease, req.Pressure)
	if err != nil {
		return fleet.AttemptResult{}, err
	}
	defer release()
	return r.runChild(ctx, req)
}

// RunShard runs each child assigned by a lease with bounded local concurrency.
func (r *Runner) RunShard(ctx context.Context, req RunShardRequest) ([]fleet.AttemptResult, error) {
	if err := req.Lease.Validate(req.Run); err != nil {
		return nil, err
	}
	if req.Attempt < 1 || req.Attempt > req.Lease.AttemptBudget {
		return nil, ErrInvalidLease
	}
	release, err := r.AdmitShard(req.Lease, req.Pressure)
	if err != nil {
		return nil, err
	}
	defer release()

	results := make([]fleet.AttemptResult, req.Lease.ChildCount)
	jobs := make(chan int)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error

	workerCount := min(req.Lease.ChildCount, req.Run.Execution.MaxInFlightPerAgent)
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for childID := range jobs {
				childReq := RunChildRequest{
					Run:                     req.Run,
					Lease:                   req.Lease,
					ChildID:                 childID,
					Attempt:                 req.Attempt,
					Pressure:                req.Pressure,
					Region:                  req.Region,
					AllowMetadataOnlyRootFS: req.AllowMetadataOnlyRootFS,
					Backend:                 req.Backend,
				}
				result, err := r.runChild(ctx, childReq)
				results[childID-req.Lease.ChildStart] = result
				if err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}
		}()
	}

	for childID := req.Lease.ChildStart; childID < req.Lease.ChildRange().End(); childID++ {
		if err := ctx.Err(); err != nil {
			close(jobs)
			wg.Wait()
			return results, errors.Join(append(errs, err)...)
		}
		jobs <- childID
	}
	close(jobs)
	wg.Wait()
	return results, errors.Join(errs...)
}

func (r *Runner) runChild(ctx context.Context, req RunChildRequest) (fleet.AttemptResult, error) {
	if err := r.validateRunChildRequest(req); err != nil {
		return fleet.AttemptResult{}, err
	}
	startedAt := r.now()
	attemptID := fleet.FormatAttemptID(req.Run.RunID, req.ChildID, req.Attempt)
	result := fleet.AttemptResult{
		RunID:        req.Run.RunID,
		BundleDigest: req.Run.Bundle.Digest,
		ChildID:      req.ChildID,
		AttemptID:    attemptID,
		AgentID:      req.Lease.AgentID,
		ShardID:      req.Lease.ShardID,
		Status:       fleet.AttemptFailed,
		StartedAt:    startedAt,
		FinishedAt:   startedAt,
	}

	existing, ok, err := r.results.TerminalResult(ctx, req.Run, req.ChildID)
	result.TimingsMS.ResultCommit = elapsedMS(startedAt, r.now())
	if err != nil {
		return result, err
	}
	if ok && !req.Run.RetryPolicy.RerunCommittedChildren {
		result.Status = fleet.AttemptSkippedTerminalExists
		result.FinishedAt = r.now()
		result.Terminal = true
		result.ResultURI = existing.ResultURI
		if result.ResultURI == "" {
			result.ResultURI = TerminalResultURI(req.Run, req.ChildID)
		}
		if err := r.results.WriteAttemptResult(ctx, req.Run, result); err != nil {
			return result, err
		}
		r.observeAttempt(result, PullResult{}, false, false)
		return result, nil
	}

	outDir := r.childWorkDir(req.Run, req.Lease, req.ChildID, attemptID)
	if err := os.RemoveAll(outDir); err != nil {
		return r.failAttempt(ctx, req.Run, result, "agent.cleanup_failed", err.Error(), false)
	}
	defer os.RemoveAll(outDir)
	if err := os.MkdirAll(filepath.Dir(outDir), 0o755); err != nil {
		return r.failAttempt(ctx, req.Run, result, "agent.workdir_failed", err.Error(), false)
	}

	pullStart := r.now()
	pull, err := r.client.Pull(ctx, PullRequest{
		Source:                  bundleSource(req.Run),
		OutDir:                  outDir,
		ChildID:                 strconv.Itoa(req.ChildID),
		Region:                  req.Region,
		AllowMetadataOnlyRootFS: req.AllowMetadataOnlyRootFS,
	})
	result.TimingsMS.ArtifactPull = elapsedMS(pullStart, r.now())
	if err != nil {
		return r.failAttemptFromError(ctx, req.Run, result, err, false)
	}
	if pull.BundleDigest.String() != req.Run.Bundle.Digest {
		return r.failAttemptWithPull(ctx, req.Run, result, "object.invalid", "pull result bundle digest did not match run bundle digest", false, pull)
	}
	generationPath := filepath.Join(filepath.Dir(outDir), attemptID+".generation.json")
	defer os.Remove(generationPath)
	if err := writeGenerationFile(generationPath, req.Run, req.ChildID); err != nil {
		return r.failAttempt(ctx, req.Run, result, "agent.generation_failed", err.Error(), false)
	}

	backend := req.Backend
	if backend == "" {
		backend = r.backend
	}
	resumeStart := r.now()
	resumeCtx := ctx
	cancelResume := func() {}
	if r.childTimeout > 0 {
		resumeCtx, cancelResume = context.WithTimeout(ctx, r.childTimeout)
	}
	events, resumeErr := r.executeChild(resumeCtx, ctx, req, outDir, backend, generationPath, attemptID)
	cancelResume()
	result.TimingsMS.Resume = elapsedMS(resumeStart, r.now())

	terminal, terminalErr := TerminalEvent(events)
	if terminalErr != nil {
		if resumeErr != nil {
			return r.failAttemptFromError(ctx, req.Run, result, resumeErr, false)
		}
		return r.failAttemptFromError(ctx, req.Run, result, terminalErr, false)
	}
	if terminal.Timings != nil {
		result.TimingsMS.GuestReady = float64(terminal.Timings.ExecResponseMS)
	}
	output, outputErr := attemptOutputFromEvents(events)
	if outputErr != nil {
		return r.failAttemptFromError(ctx, req.Run, result, outputErr, false)
	}
	result.Output = output

	commitTerminal := false
	switch terminal.Event {
	case "exit":
		result.Terminal = true
		result.ResultURI = TerminalResultURI(req.Run, req.ChildID)
		commitTerminal = true
		if terminal.ExitCode != nil && *terminal.ExitCode == 0 {
			result.Status = fleet.AttemptSucceeded
		} else {
			result.Status = fleet.AttemptFailed
			exitCode := 0
			if terminal.ExitCode != nil {
				exitCode = *terminal.ExitCode
			}
			result.Error = &fleet.AttemptError{
				Code:    "runtime.execution_failed",
				Message: "guest exited with code " + strconv.Itoa(exitCode),
			}
		}
	case "failure":
		result.Status = fleet.AttemptFailed
		if terminal.Error != nil {
			result.Error = &fleet.AttemptError{
				Code:    terminal.Error.Code,
				Message: terminal.Error.Message,
			}
			if machineErrorIsPlatformMismatch(*terminal.Error) {
				result.Status = fleet.AttemptPlatformMismatch
			}
		}
	default:
		return r.failAttempt(ctx, req.Run, result, "runtime.invalid_terminal_event", "unsupported terminal event "+terminal.Event, false)
	}
	if resumeErr != nil && result.Error == nil {
		result.Status = fleet.AttemptFailed
		result.Terminal = false
		result.ResultURI = ""
		commitTerminal = false
		result.Error = attemptError(resumeErr)
	}
	result.FinishedAt = r.now()

	if commitTerminal {
		commitStart := r.now()
		committed, err := r.results.CommitTerminalResult(ctx, req.Run, result)
		result.TimingsMS.ResultCommit = elapsedMS(commitStart, r.now())
		if err != nil {
			return result, err
		}
		if !committed {
			result.Status = fleet.AttemptFailed
			result.Terminal = false
			result.ResultURI = ""
			result.Error = &fleet.AttemptError{
				Code:    "result.terminal_exists",
				Message: "terminal result already exists",
			}
			if err := r.results.WriteAttemptResult(ctx, req.Run, result); err != nil {
				return result, err
			}
			r.observeAttempt(result, pull, true, false)
			return result, ErrResultExists
		}
	}
	if err := r.results.WriteAttemptResult(ctx, req.Run, result); err != nil {
		return result, err
	}
	r.observeAttempt(result, pull, commitTerminal, commitTerminal)
	return result, nil
}

func (r *Runner) executeChild(ctx context.Context, cleanupParent context.Context, req RunChildRequest, sporeDir string, backend string, generationPath string, attemptID string) (events []RunEvent, err error) {
	resumeReq := ResumeRequest{
		SporeDir:       sporeDir,
		Backend:        backend,
		GenerationPath: generationPath,
	}
	if len(req.Run.ChildCommand) == 0 {
		return r.client.Resume(ctx, resumeReq)
	}

	name := childVMName(attemptID)
	resumeReq.Name = name
	defer func() {
		cleanupCtx := cleanupParent
		cleanupCancel := func() {}
		if cleanupCtx.Err() != nil {
			cleanupCtx, cleanupCancel = context.WithTimeout(context.Background(), 5*time.Second)
		}
		cleanupErr := r.client.RemoveVM(cleanupCtx, RemoveVMRequest{Name: name})
		cleanupCancel()
		if err == nil {
			err = cleanupErr
		}
	}()

	resumeEvents, err := r.client.Resume(ctx, resumeReq)
	if err != nil {
		return nil, err
	}
	if terminal, err := TerminalEvent(resumeEvents); err != nil {
		return nil, err
	} else if terminal.ExitCode != nil && *terminal.ExitCode != 0 {
		return nil, fmt.Errorf("named child resume exited with code %d before child command", *terminal.ExitCode)
	}
	events, err = r.client.Exec(ctx, ExecRequest{
		Name:    name,
		Command: req.Run.ChildCommand,
	})
	return events, err
}

func (r *Runner) validateRunChildRequest(req RunChildRequest) error {
	if r.client == nil {
		return ErrSporeClientNotConfigured
	}
	if r.results == nil {
		return ErrResultStoreNotConfigured
	}
	if r.workRoot == "" {
		return ErrWorkRootNotConfigured
	}
	if err := req.Lease.Validate(req.Run); err != nil {
		return err
	}
	if req.ChildID < req.Lease.ChildStart || req.ChildID >= req.Lease.ChildRange().End() {
		return ErrChildOutsideLease
	}
	if req.Attempt < 1 || req.Attempt > req.Lease.AttemptBudget {
		return ErrInvalidLease
	}
	return nil
}

func (r *Runner) failAttemptFromError(ctx context.Context, run fleet.Run, result fleet.AttemptResult, err error, terminal bool) (fleet.AttemptResult, error) {
	attemptErr := attemptError(err)
	status := fleet.AttemptFailed
	var machineErr *MachineError
	if errors.As(err, &machineErr) && machineErrorIsPlatformMismatch(machineErr.Envelope.Error) {
		status = fleet.AttemptPlatformMismatch
	}
	return r.failAttemptWithCause(ctx, run, result, status, attemptErr.Code, attemptErr.Message, terminal, PullResult{}, err)
}

func (r *Runner) failAttempt(ctx context.Context, run fleet.Run, result fleet.AttemptResult, code string, message string, terminal bool) (fleet.AttemptResult, error) {
	return r.failAttemptWithStatus(ctx, run, result, fleet.AttemptFailed, code, message, terminal, PullResult{})
}

func (r *Runner) failAttemptWithPull(ctx context.Context, run fleet.Run, result fleet.AttemptResult, code string, message string, terminal bool, pull PullResult) (fleet.AttemptResult, error) {
	return r.failAttemptWithStatus(ctx, run, result, fleet.AttemptFailed, code, message, terminal, pull)
}

func (r *Runner) failAttemptWithStatus(ctx context.Context, run fleet.Run, result fleet.AttemptResult, status fleet.AttemptStatus, code string, message string, terminal bool, pull PullResult) (fleet.AttemptResult, error) {
	return r.failAttemptWithCause(ctx, run, result, status, code, message, terminal, pull, errors.New(message))
}

func (r *Runner) failAttemptWithCause(ctx context.Context, run fleet.Run, result fleet.AttemptResult, status fleet.AttemptStatus, code string, message string, terminal bool, pull PullResult, cause error) (fleet.AttemptResult, error) {
	result.Status = status
	result.Terminal = terminal
	result.FinishedAt = r.now()
	result.Error = &fleet.AttemptError{
		Code:    code,
		Message: message,
	}
	if terminal {
		result.ResultURI = TerminalResultURI(run, result.ChildID)
	}
	if cause == nil {
		cause = errors.New(message)
	}
	writeCtx, cancel := resultWriteContext(ctx)
	defer cancel()
	if writeErr := r.results.WriteAttemptResult(writeCtx, run, result); writeErr != nil {
		return result, errors.Join(cause, writeErr)
	}
	r.observeAttempt(result, pull, false, false)
	return result, cause
}

func resultWriteContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Err() == nil {
		return ctx, func() {}
	}
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func (r *Runner) childWorkDir(run fleet.Run, lease fleet.ShardLease, childID int, attemptID string) string {
	return filepath.Join(r.workRoot, run.RunID, lease.ShardID, "child-"+strconv.Itoa(childID), attemptID+".spore")
}

func childVMName(attemptID string) string {
	var name strings.Builder
	name.Grow(len("sporevm-") + len(attemptID))
	name.WriteString("sporevm-")
	lastDash := true
	for _, r := range strings.ToLower(attemptID) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			name.WriteRune(r)
			lastDash = false
		case !lastDash:
			name.WriteByte('-')
			lastDash = true
		}
	}
	clean := strings.TrimRight(name.String(), "-")
	if clean == "sporevm" {
		return "sporevm-child"
	}
	return clean
}

func (r *Runner) prepareWorkDir(run fleet.GenericRun) string {
	return filepath.Join(r.workRoot, run.RunID, "prepare")
}

func fileURI(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return (&url.URL{Scheme: "file", Path: abs}).String(), nil
}

func bundleSource(run fleet.Run) string {
	if strings.HasPrefix(run.Bundle.URI, "file://") {
		return run.Bundle.URI
	}
	if strings.HasSuffix(run.Bundle.URI, "@"+run.Bundle.Digest) {
		return run.Bundle.URI
	}
	return run.Bundle.URI + "@" + run.Bundle.Digest
}

type generationPayload struct {
	RunID         string `json:"run_id"`
	ChildID       int    `json:"child_id"`
	ParallelIndex int    `json:"parallel_index"`
	ParallelCount int    `json:"parallel_count"`
	ForkIndex     int    `json:"fork_index"`
	ForkCount     int    `json:"fork_count"`
	ForkBatchID   string `json:"fork_batch_id"`
	VMID          string `json:"vm_id"`
}

func writeGenerationFile(path string, run fleet.Run, childID int) error {
	index := childID - run.Children.Start
	payload := generationPayload{
		RunID:         run.RunID,
		ChildID:       childID,
		ParallelIndex: index,
		ParallelCount: run.Children.Count,
		ForkIndex:     index,
		ForkCount:     run.Children.Count,
		ForkBatchID:   run.RunID,
		VMID:          run.RunID + "-child-" + strconv.Itoa(childID),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func elapsedMS(start, end time.Time) float64 {
	if end.Before(start) {
		return 0
	}
	return float64(end.Sub(start).Microseconds()) / 1000
}

func attemptOutputFromEvents(events []RunEvent) (*fleet.AttemptOutput, error) {
	var output fleet.AttemptOutput
	var stdoutPreview []byte
	var stderrPreview []byte
	for _, event := range events {
		stream := event.Event
		if stream == "output" {
			stream = "stdout"
		}
		if stream != "stdout" && stream != "stderr" {
			continue
		}
		data, err := base64.StdEncoding.DecodeString(event.DataBase64)
		if err != nil {
			return nil, invalidMachineOutput("%s event has invalid data_base64", event.Event)
		}
		if event.ByteCount != 0 && event.ByteCount != len(data) {
			return nil, invalidMachineOutput("%s event byte_count = %d, decoded %d", event.Event, event.ByteCount, len(data))
		}
		switch stream {
		case "stdout":
			output.StdoutBytes += int64(len(data))
			stdoutPreview = appendOutputPreview(stdoutPreview, data)
		case "stderr":
			output.StderrBytes += int64(len(data))
			stderrPreview = appendOutputPreview(stderrPreview, data)
		}
	}
	if output.StdoutBytes == 0 && output.StderrBytes == 0 {
		return nil, nil
	}
	if len(stdoutPreview) > 0 {
		output.StdoutPreviewBase64 = base64.StdEncoding.EncodeToString(stdoutPreview)
		output.StdoutTruncated = output.StdoutBytes > int64(len(stdoutPreview))
	}
	if len(stderrPreview) > 0 {
		output.StderrPreviewBase64 = base64.StdEncoding.EncodeToString(stderrPreview)
		output.StderrTruncated = output.StderrBytes > int64(len(stderrPreview))
	}
	return &output, nil
}

func appendOutputPreview(preview []byte, data []byte) []byte {
	if len(preview) >= attemptOutputPreviewLimit {
		return preview
	}
	remaining := attemptOutputPreviewLimit - len(preview)
	if len(data) > remaining {
		data = data[:remaining]
	}
	return append(preview, data...)
}

func attemptError(err error) *fleet.AttemptError {
	if err == nil {
		return &fleet.AttemptError{Code: "agent.error", Message: "unknown error"}
	}
	var machineErr *MachineError
	if errors.As(err, &machineErr) {
		body := machineErr.Envelope.Error
		code := body.Code
		if code == "" {
			code = "spore.error"
		}
		message := body.Message
		if message == "" {
			message = machineErr.Error()
		}
		return &fleet.AttemptError{Code: code, Message: message}
	}
	code := "agent.error"
	if errors.Is(err, context.Canceled) {
		code = "agent.canceled"
	} else if errors.Is(err, context.DeadlineExceeded) {
		code = "agent.deadline_exceeded"
	} else if errors.Is(err, ErrNoTerminalEvent) || errors.Is(err, ErrInvalidMachineOutput) {
		code = "runtime.invalid_output"
	}
	return &fleet.AttemptError{Code: code, Message: err.Error()}
}

func machineErrorIsPlatformMismatch(body MachineErrorBody) bool {
	return strings.HasPrefix(body.Code, "host.")
}

func (r *Runner) observeAttempt(result fleet.AttemptResult, pull PullResult, terminalCommitAttempted bool, terminalCommitSuccessful bool) {
	if r.metrics == nil {
		return
	}
	r.metrics.ObserveAttempt(AttemptMetrics{
		RunID:                    result.RunID,
		BundleDigest:             result.BundleDigest,
		ChildID:                  result.ChildID,
		AttemptID:                result.AttemptID,
		AgentID:                  result.AgentID,
		ShardID:                  result.ShardID,
		Status:                   result.Status,
		TimingsMS:                result.TimingsMS,
		OriginBytesRead:          pull.Remote.OriginBytesRead,
		PeerBytesRead:            pull.Remote.PeerBytesRead,
		ChunkCacheHits:           pull.Materialization.Cache.HitCount,
		ChunkCacheMisses:         pull.Materialization.Cache.MissCount,
		RootFSCacheHits:          pull.RootFS.Cache.HitCount,
		RootFSCacheMisses:        pull.RootFS.Cache.MissCount,
		MaterializedChunkCount:   pull.Materialization.MaterializedChunkCount,
		LinkedChunkCount:         pull.Materialization.LinkedChunkCount,
		CopiedChunkCount:         pull.Materialization.CopiedChunkCount,
		PlatformMismatch:         result.Status == fleet.AttemptPlatformMismatch,
		TerminalCommitAttempted:  terminalCommitAttempted,
		TerminalCommitSuccessful: terminalCommitSuccessful,
	})
}
