package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sporevm/k8s/internal/fleet"
)

// RunCheckpointInspector adapts a SporeVM client to fleet bundle admission.
type RunCheckpointInspector struct {
	Runner *Runner
}

// InspectRunCheckpoint verifies a materialized native parent before admission.
func (i RunCheckpointInspector) InspectRunCheckpoint(ctx context.Context, run fleet.CheckpointRun) (fleet.CheckpointInspection, error) {
	if i.Runner == nil {
		return fleet.CheckpointInspection{}, ErrSporeClientNotConfigured
	}
	parent, _, err := i.Runner.materializeCheckpoint(ctx, run)
	if err != nil {
		return fleet.CheckpointInspection{}, err
	}
	return fleet.CheckpointInspection{
		CheckpointDigest: parent.Checkpoint.Digest,
		HostClass:        parent.HostClass,
	}, nil
}

func (r *Runner) inspectCheckpoint(ctx context.Context, path string, attach bool) (fleet.HostClass, uint64, error) {
	checkpoint, err := r.client.InspectCheckpoint(ctx, InspectCheckpointRequest{Source: path})
	if err != nil {
		return fleet.HostClass{}, 0, err
	}
	if err := checkpoint.Validate(); err != nil {
		return fleet.HostClass{}, 0, err
	}
	if attach && !checkpoint.CanAttach {
		return fleet.HostClass{}, 0, invalidMachineOutput("checkpoint has no saved session; childCommand is required")
	}
	info, err := r.client.HostInfo(ctx)
	if err != nil {
		return fleet.HostClass{}, 0, err
	}
	hostClass, err := info.FleetHostClass(r.backend)
	if err != nil {
		return fleet.HostClass{}, 0, err
	}
	p, h := checkpoint.Platform, info.Platform
	if p.Arch != h.Arch || p.CPUProfile != h.CPUProfile ||
		p.DeviceModelVersion != h.DeviceModelVersion || p.RAMBase != h.RAMBase ||
		p.GICDistBase != h.GICDistBase || p.GICRedistBase != h.GICRedistBase ||
		p.CounterFrequencyHz != h.CounterFrequencyHz {
		return fleet.HostClass{}, 0, fmt.Errorf("%w: checkpoint platform does not match this host", fleet.ErrPlatformMismatch)
	}
	return hostClass, checkpoint.Generation, nil
}

// materializeCheckpoint returns the verified parent for run.Checkpoint,
// pulling it unless this agent already holds that digest. Runs with the same
// digest share one parent; ReleaseCheckpoint drops the run's use of it.
func (r *Runner) materializeCheckpoint(ctx context.Context, run fleet.CheckpointRun) (materializedCheckpoint, PullResult, error) {
	if err := run.Validate(); err != nil {
		return materializedCheckpoint{}, PullResult{}, err
	}
	if r.client == nil {
		return materializedCheckpoint{}, PullResult{}, ErrSporeClientNotConfigured
	}
	if r.workRoot == "" {
		return materializedCheckpoint{}, PullResult{}, ErrWorkRootNotConfigured
	}
	unlock, err := r.lockPrepareOperation(ctx, run.RunID)
	if err != nil {
		return materializedCheckpoint{}, PullResult{}, err
	}
	defer unlock()
	if entry, ok := r.checkpoints.Load(run.RunID); ok {
		parent := entry.(materializedCheckpoint)
		if parent.Checkpoint != run.Checkpoint || !parent.HostClass.Equal(run.HostClass) {
			return materializedCheckpoint{}, PullResult{}, fmt.Errorf("%w: run ID already owns another checkpoint or host class", fleet.ErrBundleMetadataMismatch)
		}
		return parent, PullResult{}, nil
	}
	digest := run.Checkpoint.Digest
	unlockDigest, err := r.lockPrepareOperation(ctx, checkpointGateKey(digest))
	if err != nil {
		return materializedCheckpoint{}, PullResult{}, err
	}
	defer unlockDigest()
	root := r.checkpointRoot(digest)
	if err := validateNoSymlinkPath(r.workRoot, root); err != nil {
		return materializedCheckpoint{}, PullResult{}, err
	}
	parentDir := filepath.Join(root, "parent.spore")
	var pull PullResult
	retained := r.checkpointRetained(digest)
	// A parent that fails its first admission is not retained.
	cleanup := func(cause error) (materializedCheckpoint, PullResult, error) {
		if retained {
			return materializedCheckpoint{}, pull, cause
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), lifecycleCleanupTimeout)
		defer cancel()
		return materializedCheckpoint{}, pull, errors.Join(cause, r.removePreparedSpore(cleanupCtx, parentDir))
	}
	if !retained {
		// Only a pull by this process proves the directory holds this digest,
		// so replace whatever an interrupted pull or an earlier agent left.
		if err := r.removePreparedSpore(ctx, parentDir); err != nil {
			return materializedCheckpoint{}, PullResult{}, err
		}
		if err := os.MkdirAll(root, 0o755); err != nil {
			return materializedCheckpoint{}, PullResult{}, err
		}
		pull, err = r.client.Pull(ctx, PullRequest{Source: bundleSource(run), OutDir: parentDir})
		if err != nil {
			return cleanup(err)
		}
		if pull.SporeDigest != digest || pull.OutDir != parentDir || pull.Ref != bundleSource(run) {
			return cleanup(invalidMachineOutput("pulled checkpoint identity or output directory does not match request"))
		}
	}
	// Runs differ in host class and in whether they attach, so inspect for
	// each run. A retained parent that this run cannot use stays for the others.
	hostClass, generation, err := r.inspectCheckpoint(ctx, parentDir, len(run.ChildCommand) == 0)
	if err != nil {
		return cleanup(err)
	}
	if !hostClass.Equal(run.HostClass) {
		return cleanup(fmt.Errorf("%w: requested host class does not match inspected checkpoint", fleet.ErrPlatformMismatch))
	}
	parent := materializedCheckpoint{Checkpoint: run.Checkpoint, ParentDir: parentDir, HostClass: hostClass, Generation: generation}
	r.markCheckpointUsed(digest)
	r.checkpoints.Store(run.RunID, parent)
	return parent, pull, nil
}

// ReleaseCheckpoint ends a run's use of its pulled parent after all aggregate
// run attempts, then evicts unused parents beyond the retain count.
func (r *Runner) ReleaseCheckpoint(ctx context.Context, run fleet.CheckpointRun) error {
	if err := run.Validate(); err != nil {
		return err
	}
	unlock, err := r.lockPrepareOperation(ctx, run.RunID)
	if err != nil {
		return err
	}
	defer unlock()
	digest := run.Checkpoint.Digest
	if entry, ok := r.checkpoints.Load(run.RunID); ok {
		if entry.(materializedCheckpoint).ParentDir != filepath.Join(r.checkpointRoot(digest), "parent.spore") {
			// A source run owns its parent and store under prepare/; ReleasePreparedRun
			// removes both with the persistent cleanup ledger.
			return nil
		}
		r.checkpoints.Delete(run.RunID)
		r.markCheckpointUsed(digest)
	}
	// Evict even when this run holds nothing, so a retried release finishes
	// an eviction that failed.
	return r.evictCheckpoints(ctx)
}

// RemoveStaleCheckpoints removes pulled parents this process did not pull.
// An earlier agent's parents are not reused, because nothing local proves
// which digest such a directory holds, so remove them before serving.
func (r *Runner) RemoveStaleCheckpoints(ctx context.Context) error {
	if r.client == nil {
		return ErrSporeClientNotConfigured
	}
	if r.workRoot == "" {
		return ErrWorkRootNotConfigured
	}
	entries, err := os.ReadDir(filepath.Join(r.workRoot, checkpointsDirName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var errs []error
	for _, entry := range entries {
		digest := checkpointDigestPrefix + entry.Name()
		if !r.checkpointRetained(digest) {
			errs = append(errs, r.removeCheckpoint(ctx, digest))
		}
	}
	return errors.Join(errs...)
}

// evictCheckpoints removes retained parents no run uses, least recently used
// first, until at most the retain count remain.
func (r *Runner) evictCheckpoints(ctx context.Context) error {
	for {
		digest, ok := r.nextCheckpointEviction()
		if !ok {
			return nil
		}
		if err := r.removeCheckpoint(ctx, digest); err != nil {
			return err
		}
	}
}

// nextCheckpointEviction picks the least recently used retained parent that
// no run uses, when more than the retain count are unused.
func (r *Runner) nextCheckpointEviction() (string, bool) {
	r.retainedMu.Lock()
	defer r.retainedMu.Unlock()
	oldest, unused := "", 0
	for digest, used := range r.retained {
		if r.checkpointInUse(digest) {
			continue
		}
		unused++
		if oldest == "" || used < r.retained[oldest] {
			oldest = digest
		}
	}
	return oldest, unused > r.checkpointRetain
}

// removeCheckpoint removes one digest's parent unless a run uses it. A run
// records its use while holding the digest gate, so the check cannot race a
// concurrent materialization.
func (r *Runner) removeCheckpoint(ctx context.Context, digest string) error {
	unlock, err := r.lockPrepareOperation(ctx, checkpointGateKey(digest))
	if err != nil {
		return err
	}
	defer unlock()
	if r.checkpointInUse(digest) {
		return nil
	}
	root := r.checkpointRoot(digest)
	if err := validateNoSymlinkPath(r.workRoot, root); err != nil {
		return err
	}
	if err := r.removePreparedSpore(ctx, filepath.Join(root, "parent.spore")); err != nil {
		return err
	}
	if err := os.RemoveAll(root); err != nil {
		return err
	}
	r.retainedMu.Lock()
	delete(r.retained, digest)
	r.retainedMu.Unlock()
	return nil
}

func (r *Runner) checkpointInUse(digest string) bool {
	parentDir := filepath.Join(r.checkpointRoot(digest), "parent.spore")
	inUse := false
	r.checkpoints.Range(func(_, entry any) bool {
		inUse = entry.(materializedCheckpoint).ParentDir == parentDir
		return !inUse
	})
	return inUse
}

func (r *Runner) checkpointRetained(digest string) bool {
	r.retainedMu.Lock()
	defer r.retainedMu.Unlock()
	_, ok := r.retained[digest]
	return ok
}

// markCheckpointUsed retains digest and makes it the most recently used.
func (r *Runner) markCheckpointUsed(digest string) {
	r.retainedMu.Lock()
	defer r.retainedMu.Unlock()
	r.retainedUses++
	r.retained[digest] = r.retainedUses
}

func (r *Runner) checkpointRoot(digest string) string {
	return filepath.Join(r.workRoot, checkpointsDirName, strings.TrimPrefix(digest, checkpointDigestPrefix))
}

// checkpointGateKey names a digest's gate in prepareOps. Run IDs cannot
// contain a colon, so it never collides with a run's gate.
func checkpointGateKey(digest string) string {
	return "checkpoint:" + digest
}

// RunnerShardExecutor adapts a node-local Runner to fleet shard execution.
type RunnerShardExecutor struct {
	Runner   *Runner
	Pressure fleet.Pressure
	Backend  string
}

// RunShard executes a coordinator lease on the configured Runner.
func (e RunnerShardExecutor) RunShard(ctx context.Context, req fleet.ShardExecutionRequest) ([]fleet.AttemptResult, error) {
	if e.Runner == nil {
		return nil, ErrSporeClientNotConfigured
	}
	return e.Runner.RunShard(ctx, RunShardRequest{
		Run:      req.Run,
		Lease:    req.Lease,
		Attempt:  req.Attempt,
		Pressure: e.Pressure,
		Backend:  e.Backend,
	})
}
