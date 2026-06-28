package benchmark

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/sporevm/k8s/internal/fleet"
)

func TestBuildSummaryComputesPercentilesAndTargetConcurrency(t *testing.T) {
	start := time.Date(2026, 6, 20, 4, 0, 0, 0, time.UTC)
	report := fleet.RuntimeReport{
		Summary: fleet.RuntimeSummary{
			RunID:      "ruby-counter-20260620",
			ChildCount: 3,
			StartedAt:  start.Add(50 * time.Millisecond),
		},
	}
	observations := []Observation{
		observation("ruby-counter-20260620", 0, start.Add(50*time.Millisecond), true, StageTimings{ArtifactPull: 10, Materialization: 20, Resume: 30, GuestReady: 40, ResultCommit: 50}, 100, 8, 2),
		observation("ruby-counter-20260620", 1, start.Add(60*time.Millisecond), true, StageTimings{ArtifactPull: 20, Materialization: 30, Resume: 40, GuestReady: 50, ResultCommit: 60}, 200, 6, 4),
		observation("ruby-counter-20260620", 2, start.Add(70*time.Millisecond), false, StageTimings{ArtifactPull: 30, Materialization: 40, Resume: 50, GuestReady: 60, ResultCommit: 70}, 300, 4, 6),
	}

	summary, err := BuildSummary(report, observations, SummaryOptions{
		TargetConcurrency: 2,
		CachePosture:      CachePostureWarmBundleColdMaterialization,
		SubmittedAt:       start,
		AdmittedAt:        start.Add(50 * time.Millisecond),
	})
	if err != nil {
		t.Fatalf("BuildSummary: %v", err)
	}
	if summary.SuccessRate != float64(2)/float64(3) {
		t.Fatalf("success rate = %f", summary.SuccessRate)
	}
	if summary.TimeToFirstChildReadyMS != 150 {
		t.Fatalf("time to first ready = %f", summary.TimeToFirstChildReadyMS)
	}
	if summary.TimeToTargetConcurrencyMS != 200 {
		t.Fatalf("time to target = %f", summary.TimeToTargetConcurrencyMS)
	}
	if summary.StagePercentilesMS.ArtifactPull.P50 != 10 ||
		summary.StagePercentilesMS.ArtifactPull.P95 != 20 ||
		summary.StagePercentilesMS.ArtifactPull.P99 != 20 {
		t.Fatalf("artifact pull percentiles = %+v", summary.StagePercentilesMS.ArtifactPull)
	}
	if summary.OriginBytes != 600 {
		t.Fatalf("origin bytes = %d", summary.OriginBytes)
	}
	if summary.CacheHitRate != 0.6 {
		t.Fatalf("cache hit rate = %f", summary.CacheHitRate)
	}
}

func TestBuildSummaryRejectsUnreachedTargetConcurrency(t *testing.T) {
	start := time.Date(2026, 6, 20, 4, 0, 0, 0, time.UTC)
	_, err := BuildSummary(fleet.RuntimeReport{
		Summary: fleet.RuntimeSummary{RunID: "ruby-counter-20260620", ChildCount: 2, StartedAt: start},
	}, []Observation{
		observation("ruby-counter-20260620", 0, start, true, StageTimings{GuestReady: 1}, 0, 1, 0),
	}, SummaryOptions{
		TargetConcurrency: 2,
		CachePosture:      CachePostureWarmBundleColdMaterialization,
		SubmittedAt:       start,
		AdmittedAt:        start,
	})
	if !errors.Is(err, ErrTargetConcurrencyNotReached) {
		t.Fatalf("BuildSummary error = %v, want ErrTargetConcurrencyNotReached", err)
	}
}

func TestBuildSummaryCountsUniqueSuccessfulChildren(t *testing.T) {
	start := time.Date(2026, 6, 20, 4, 0, 0, 0, time.UTC)
	_, err := BuildSummary(fleet.RuntimeReport{
		Summary: fleet.RuntimeSummary{RunID: "ruby-counter-20260620", ChildCount: 2, StartedAt: start},
	}, []Observation{
		observation("ruby-counter-20260620", 0, start, true, StageTimings{GuestReady: 1}, 0, 1, 0),
		observation("ruby-counter-20260620", 0, start.Add(time.Millisecond), true, StageTimings{GuestReady: 1}, 0, 1, 0),
	}, SummaryOptions{
		TargetConcurrency: 2,
		CachePosture:      CachePostureWarmBundleColdMaterialization,
		SubmittedAt:       start,
		AdmittedAt:        start,
	})
	if !errors.Is(err, ErrTargetConcurrencyNotReached) {
		t.Fatalf("BuildSummary error = %v, want ErrTargetConcurrencyNotReached", err)
	}
}

func TestSummaryJSONShape(t *testing.T) {
	summary := Summary{
		RunID:                     "ruby-counter-20260620",
		ChildCount:                1000,
		TargetConcurrency:         1000,
		CachePosture:              CachePostureWarmBundleColdMaterialization,
		SuccessRate:               1,
		AdmissionLatencyMS:        50,
		TimeToFirstChildReadyMS:   100,
		TimeToTargetConcurrencyMS: 200,
		StagePercentilesMS: StagePercentiles{
			ArtifactPull:    Percentiles{P50: 1, P95: 2, P99: 3},
			Materialization: Percentiles{P50: 1, P95: 2, P99: 3},
			Resume:          Percentiles{P50: 1, P95: 2, P99: 3},
			GuestReady:      Percentiles{P50: 1, P95: 2, P99: 3},
			ResultCommit:    Percentiles{P50: 1, P95: 2, P99: 3},
		},
		OriginBytes:  1,
		CacheHitRate: 1,
	}
	payload, err := json.Marshal(summary)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if _, ok := decoded["stagePercentilesMs"]; !ok {
		t.Fatalf("summary JSON missing stagePercentilesMs: %s", payload)
	}
	if _, ok := decoded["timeToTargetConcurrencyMs"]; !ok {
		t.Fatalf("summary JSON missing timeToTargetConcurrencyMs: %s", payload)
	}
}

func observation(runID string, childID int, startedAt time.Time, succeeded bool, timings StageTimings, originBytes int64, hits int, misses int) Observation {
	return Observation{
		RunID:           runID,
		ChildID:         childID,
		Succeeded:       succeeded,
		StartedAt:       startedAt,
		TimingsMS:       timings,
		OriginBytesRead: originBytes,
		CacheHitCount:   hits,
		CacheMissCount:  misses,
	}
}
