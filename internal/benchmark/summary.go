package benchmark

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/sporevm/k8s/internal/fleet"
)

var (
	// ErrInvalidBenchmark means benchmark inputs cannot produce a valid summary.
	ErrInvalidBenchmark = errors.New("invalid benchmark")
	// ErrTargetConcurrencyNotReached means too few children became ready.
	ErrTargetConcurrencyNotReached = errors.New("target concurrency not reached")
)

// SummaryOptions controls benchmark summary construction.
type SummaryOptions struct {
	TargetConcurrency int
	CachePosture      CachePosture
	SubmittedAt       time.Time
	AdmittedAt        time.Time
}

// BuildSummary builds benchmark output from a coordinator report and child observations.
func BuildSummary(report fleet.RuntimeReport, observations []Observation, opts SummaryOptions) (Summary, error) {
	childCount := report.Summary.ChildCount
	if childCount < 1 {
		return Summary{}, fmt.Errorf("%w: child count must be >= 1", ErrInvalidBenchmark)
	}
	target := opts.TargetConcurrency
	if target == 0 {
		target = childCount
	}
	if target < 1 || target > childCount {
		return Summary{}, fmt.Errorf("%w: target concurrency must be between 1 and child count", ErrInvalidBenchmark)
	}
	if !validCachePosture(opts.CachePosture) {
		return Summary{}, fmt.Errorf("%w: unsupported cache posture %q", ErrInvalidBenchmark, opts.CachePosture)
	}
	if opts.SubmittedAt.IsZero() {
		opts.SubmittedAt = report.Summary.StartedAt
	}
	if opts.AdmittedAt.IsZero() {
		opts.AdmittedAt = report.Summary.StartedAt
	}
	if opts.AdmittedAt.Before(opts.SubmittedAt) {
		return Summary{}, fmt.Errorf("%w: admitted time is before submitted time", ErrInvalidBenchmark)
	}
	if len(observations) == 0 {
		return Summary{}, fmt.Errorf("%w: no child observations", ErrInvalidBenchmark)
	}

	readyByChild := make(map[int]float64, childCount)
	var artifactPull []float64
	var materialization []float64
	var resume []float64
	var guestReady []float64
	var resultCommit []float64
	var originBytes int64
	var cacheHits int
	var cacheMisses int
	for _, observation := range observations {
		if err := validateObservation(report, observation); err != nil {
			return Summary{}, err
		}
		if observation.RunID != "" && observation.RunID != report.Summary.RunID {
			return Summary{}, fmt.Errorf("%w: observation run id %q does not match report %q", ErrInvalidBenchmark, observation.RunID, report.Summary.RunID)
		}
		if observation.Succeeded {
			readyOffset := elapsedMS(opts.SubmittedAt, observation.ReadyAt())
			if existing, ok := readyByChild[observation.ChildID]; !ok || readyOffset < existing {
				readyByChild[observation.ChildID] = readyOffset
			}
			artifactPull = append(artifactPull, observation.TimingsMS.ArtifactPull)
			materialization = append(materialization, observation.TimingsMS.Materialization)
			resume = append(resume, observation.TimingsMS.Resume)
			guestReady = append(guestReady, observation.TimingsMS.GuestReady)
			resultCommit = append(resultCommit, observation.TimingsMS.ResultCommit)
		}
		originBytes += observation.OriginBytesRead
		cacheHits += observation.CacheHitCount
		cacheMisses += observation.CacheMissCount
	}
	successes := len(readyByChild)
	if successes < target {
		return Summary{}, fmt.Errorf("%w: need %d ready children, have %d", ErrTargetConcurrencyNotReached, target, successes)
	}
	readyOffsets := make([]float64, 0, len(readyByChild))
	for _, readyOffset := range readyByChild {
		readyOffsets = append(readyOffsets, readyOffset)
	}
	slices.Sort(readyOffsets)

	return Summary{
		RunID:                     report.Summary.RunID,
		ChildCount:                childCount,
		TargetConcurrency:         target,
		CachePosture:              opts.CachePosture,
		SuccessRate:               float64(successes) / float64(childCount),
		AdmissionLatencyMS:        elapsedMS(opts.SubmittedAt, opts.AdmittedAt),
		TimeToFirstChildReadyMS:   readyOffsets[0],
		TimeToTargetConcurrencyMS: readyOffsets[target-1],
		StagePercentilesMS: StagePercentiles{
			ArtifactPull:    computePercentiles(artifactPull),
			Materialization: computePercentiles(materialization),
			Resume:          computePercentiles(resume),
			GuestReady:      computePercentiles(guestReady),
			ResultCommit:    computePercentiles(resultCommit),
		},
		OriginBytes:  originBytes,
		CacheHitRate: cacheHitRate(cacheHits, cacheMisses),
	}, nil
}

func validateObservation(report fleet.RuntimeReport, observation Observation) error {
	if observation.ChildID < 0 {
		return fmt.Errorf("%w: observation child id %d must be >= 0", ErrInvalidBenchmark, observation.ChildID)
	}
	if observation.StartedAt.IsZero() {
		return fmt.Errorf("%w: observation started time is zero", ErrInvalidBenchmark)
	}
	if observation.TimingsMS.ArtifactPull < 0 ||
		observation.TimingsMS.Materialization < 0 ||
		observation.TimingsMS.Resume < 0 ||
		observation.TimingsMS.GuestReady < 0 ||
		observation.TimingsMS.ResultCommit < 0 {
		return fmt.Errorf("%w: observation timings must be >= 0", ErrInvalidBenchmark)
	}
	if observation.OriginBytesRead < 0 || observation.CacheHitCount < 0 || observation.CacheMissCount < 0 {
		return fmt.Errorf("%w: observation cache metrics must be >= 0", ErrInvalidBenchmark)
	}
	return nil
}

func computePercentiles(values []float64) Percentiles {
	if len(values) == 0 {
		return Percentiles{}
	}
	sorted := append([]float64(nil), values...)
	slices.Sort(sorted)
	return Percentiles{
		P50: percentile(sorted, 0.50),
		P95: percentile(sorted, 0.95),
		P99: percentile(sorted, 0.99),
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 1 {
		return sorted[0]
	}
	index := int(p*float64(len(sorted))+0.999999999) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func cacheHitRate(hits, misses int) float64 {
	total := hits + misses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total)
}

func elapsedMS(start, end time.Time) float64 {
	if end.Before(start) {
		return 0
	}
	return float64(end.Sub(start).Microseconds()) / 1000
}

func validCachePosture(posture CachePosture) bool {
	switch posture {
	case CachePostureColdOriginColdCache, CachePostureWarmBundleColdMaterialization, CachePostureWarmNodeLocalCache:
		return true
	default:
		return false
	}
}
