package agent

import (
	"context"

	"github.com/sporevm/k8s/internal/fleet"
)

// RunBundleInspector adapts a SporeVM client to fleet bundle admission.
type RunBundleInspector struct {
	Client SporeClient
}

// InspectRunBundle inspects immutable bundle metadata before coordinator admission.
func (i RunBundleInspector) InspectRunBundle(ctx context.Context, run fleet.Run) (fleet.BundleInspection, error) {
	if i.Client == nil {
		return fleet.BundleInspection{}, ErrSporeClientNotConfigured
	}
	result, err := i.Client.InspectBundle(ctx, InspectBundleRequest{
		Source: bundleSource(run),
		ChildRange: &ChildRangeSelection{
			Start: run.Children.Start,
			End:   run.Children.End(),
		},
	})
	if err != nil {
		return fleet.BundleInspection{}, err
	}
	return fleet.BundleInspection{
		BundleDigest: result.BundleDigest.String(),
		ChildCount:   result.ChildCount,
	}, nil
}

// RunnerShardExecutor adapts a node-local Runner to fleet shard execution.
type RunnerShardExecutor struct {
	Runner                  *Runner
	Pressure                fleet.Pressure
	Region                  string
	AllowMetadataOnlyRootFS bool
	Backend                 string
}

// RunShard executes a coordinator lease on the configured Runner.
func (e RunnerShardExecutor) RunShard(ctx context.Context, req fleet.ShardExecutionRequest) ([]fleet.AttemptResult, error) {
	if e.Runner == nil {
		return nil, ErrSporeClientNotConfigured
	}
	return e.Runner.RunShard(ctx, RunShardRequest{
		Run:                     req.Run,
		Lease:                   req.Lease,
		Attempt:                 req.Attempt,
		Pressure:                e.Pressure,
		Region:                  e.Region,
		AllowMetadataOnlyRootFS: e.AllowMetadataOnlyRootFS,
		Backend:                 e.Backend,
	})
}
