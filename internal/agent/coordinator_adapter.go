package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

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

func (r *Runner) inspectCheckpoint(ctx context.Context, path string, attach bool) (fleet.HostClass, error) {
	checkpoint, err := r.client.InspectCheckpoint(ctx, InspectCheckpointRequest{Source: path})
	if err != nil {
		return fleet.HostClass{}, err
	}
	if err := checkpoint.Validate(); err != nil {
		return fleet.HostClass{}, err
	}
	if attach && !checkpoint.CanAttach {
		return fleet.HostClass{}, invalidMachineOutput("checkpoint has no saved session; childCommand is required")
	}
	info, err := r.client.HostInfo(ctx)
	if err != nil {
		return fleet.HostClass{}, err
	}
	hostClass, err := info.FleetHostClass(r.backend)
	if err != nil {
		return fleet.HostClass{}, err
	}
	p, h := checkpoint.Platform, info.Platform
	if p.Arch != h.Arch || p.CPUProfile != h.CPUProfile ||
		p.DeviceModelVersion != h.DeviceModelVersion || p.RAMBase != h.RAMBase ||
		p.GICDistBase != h.GICDistBase || p.GICRedistBase != h.GICRedistBase ||
		p.CounterFrequencyHz != h.CounterFrequencyHz {
		return fleet.HostClass{}, fmt.Errorf("%w: checkpoint platform does not match this host", fleet.ErrPlatformMismatch)
	}
	return hostClass, nil
}

// materializeCheckpoint retains one verified parent across children and retries.
// The coordinator releases it after the aggregate run finishes.
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
	root := filepath.Join(r.workRoot, run.RunID, "checkpoint")
	if err := validateNoSymlinkPath(r.workRoot, root); err != nil {
		return materializedCheckpoint{}, PullResult{}, err
	}
	parentDir := filepath.Join(root, "parent.spore")
	// Recover an interrupted run before replacing its pinned save.
	if err := r.removePreparedSpore(ctx, parentDir); err != nil {
		return materializedCheckpoint{}, PullResult{}, err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return materializedCheckpoint{}, PullResult{}, err
	}
	pull, err := r.client.Pull(ctx, PullRequest{Source: bundleSource(run), OutDir: parentDir})
	cleanup := func(cause error) (materializedCheckpoint, PullResult, error) {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), lifecycleCleanupTimeout)
		defer cancel()
		return materializedCheckpoint{}, pull, errors.Join(cause, r.removePreparedSpore(cleanupCtx, parentDir))
	}
	if err != nil {
		return cleanup(err)
	}
	if pull.SporeDigest != run.Checkpoint.Digest || pull.OutDir != parentDir || pull.Ref != bundleSource(run) {
		return cleanup(invalidMachineOutput("pulled checkpoint identity or output directory does not match request"))
	}
	hostClass, err := r.inspectCheckpoint(ctx, parentDir, len(run.ChildCommand) == 0)
	if err != nil {
		return cleanup(err)
	}
	if !hostClass.Equal(run.HostClass) {
		return cleanup(fmt.Errorf("%w: requested host class does not match inspected checkpoint", fleet.ErrPlatformMismatch))
	}
	parent := materializedCheckpoint{Checkpoint: run.Checkpoint, ParentDir: parentDir, HostClass: hostClass}
	r.checkpoints.Store(run.RunID, parent)
	return parent, pull, nil
}

// ReleaseCheckpoint releases a remote parent after all aggregate run attempts.
func (r *Runner) ReleaseCheckpoint(ctx context.Context, run fleet.CheckpointRun) error {
	if err := run.Validate(); err != nil {
		return err
	}
	unlock, err := r.lockPrepareOperation(ctx, run.RunID)
	if err != nil {
		return err
	}
	defer unlock()
	root := filepath.Join(r.workRoot, run.RunID, "checkpoint")
	if entry, ok := r.checkpoints.Load(run.RunID); ok && entry.(materializedCheckpoint).ParentDir != filepath.Join(root, "parent.spore") {
		// A source run owns its parent and store under prepare/; ReleasePreparedRun
		// removes both with the persistent cleanup ledger.
		return nil
	}
	if err := validateNoSymlinkPath(r.workRoot, root); err != nil {
		return err
	}
	if err := r.removePreparedSpore(ctx, filepath.Join(root, "parent.spore")); err != nil {
		return err
	}
	if err := os.RemoveAll(root); err != nil {
		return err
	}
	r.checkpoints.Delete(run.RunID)
	return nil
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
