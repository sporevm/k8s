package benchmark

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/sporevm/k8s/internal/fleet"
)

func TestRunSyntheticBenchmarks1000Children(t *testing.T) {
	run := loadRunExample(t)

	result, err := RunSynthetic(context.Background(), run, SyntheticOptions{})
	if err != nil {
		t.Fatalf("RunSynthetic: %v", err)
	}
	if result.Summary.RunID != run.RunID {
		t.Fatalf("run id = %q", result.Summary.RunID)
	}
	if result.Summary.ChildCount != 1000 || result.Summary.TargetConcurrency != 1000 {
		t.Fatalf("summary = %+v", result.Summary)
	}
	if result.Summary.SuccessRate != 1 {
		t.Fatalf("success rate = %f", result.Summary.SuccessRate)
	}
	if result.Summary.TimeToFirstChildReadyMS <= result.Summary.AdmissionLatencyMS {
		t.Fatalf("time to first ready should include post-admission work: %+v", result.Summary)
	}
	if result.Summary.TimeToTargetConcurrencyMS < result.Summary.TimeToFirstChildReadyMS {
		t.Fatalf("time to target < first ready: %+v", result.Summary)
	}
	if result.Summary.StagePercentilesMS.GuestReady.P99 <= result.Summary.StagePercentilesMS.GuestReady.P50 {
		t.Fatalf("guest-ready percentiles = %+v", result.Summary.StagePercentilesMS.GuestReady)
	}
	if result.Summary.OriginBytes <= 0 {
		t.Fatalf("origin bytes = %d", result.Summary.OriginBytes)
	}
	if result.Summary.CacheHitRate <= 0 || result.Summary.CacheHitRate > 1 {
		t.Fatalf("cache hit rate = %f", result.Summary.CacheHitRate)
	}
	if result.Report.Summary.AssignedChildren != 1000 || result.Report.Summary.AssignedShards != 10 {
		t.Fatalf("coordinator summary = %+v", result.Report.Summary)
	}
	if len(result.Observations) != 1000 {
		t.Fatalf("observations = %d", len(result.Observations))
	}
}

func TestRunSyntheticMatchesBenchmarkExample(t *testing.T) {
	run := loadRunExample(t)
	result, err := RunSynthetic(context.Background(), run, SyntheticOptions{})
	if err != nil {
		t.Fatalf("RunSynthetic: %v", err)
	}

	var example Summary
	payload, err := os.ReadFile(filepath.Join("..", "..", "examples", "fleet", "benchmark-summary-1000.json"))
	if err != nil {
		t.Fatalf("read benchmark example: %v", err)
	}
	if err := json.Unmarshal(payload, &example); err != nil {
		t.Fatalf("decode benchmark example: %v", err)
	}
	if !reflect.DeepEqual(result.Summary, example) {
		t.Fatalf("summary does not match example\n got: %+v\nwant: %+v", result.Summary, example)
	}
}

func TestRunSyntheticRejectsInsufficientCapacity(t *testing.T) {
	run := loadRunExample(t)

	_, err := RunSynthetic(context.Background(), run, SyntheticOptions{
		AgentCount:    1,
		SlotsPerAgent: 100,
	})
	if !errors.Is(err, fleet.ErrInsufficientCapacity) {
		t.Fatalf("RunSynthetic error = %v, want ErrInsufficientCapacity", err)
	}
}

func TestRunSyntheticRejectsInvalidOptions(t *testing.T) {
	run := loadRunExample(t)

	_, err := RunSynthetic(context.Background(), run, SyntheticOptions{
		AgentCount:    -1,
		SlotsPerAgent: 100,
	})
	if !errors.Is(err, ErrInvalidBenchmark) {
		t.Fatalf("RunSynthetic error = %v, want ErrInvalidBenchmark", err)
	}
}

func TestRunSyntheticWarmNodeLocalCacheReportsNoOriginBytes(t *testing.T) {
	run := loadRunExample(t)

	result, err := RunSynthetic(context.Background(), run, SyntheticOptions{
		CachePosture: CachePostureWarmNodeLocalCache,
	})
	if err != nil {
		t.Fatalf("RunSynthetic: %v", err)
	}
	if result.Summary.OriginBytes != 0 {
		t.Fatalf("origin bytes = %d, want 0", result.Summary.OriginBytes)
	}
	if result.Summary.CacheHitRate != 1 {
		t.Fatalf("cache hit rate = %f, want 1", result.Summary.CacheHitRate)
	}
}

func loadRunExample(t *testing.T) fleet.Run {
	t.Helper()
	file, err := os.Open(filepath.Join("..", "..", "examples", "fleet", "run-1000.json"))
	if err != nil {
		t.Fatalf("open run example: %v", err)
	}
	defer file.Close()
	run, err := fleet.DecodeRun(file)
	if err != nil {
		t.Fatalf("DecodeRun: %v", err)
	}
	return run
}
