// Package benchmark builds SporeVM fleet benchmark summaries.
package benchmark

import "time"

// CachePosture names the cache state for a benchmark run.
type CachePosture string

const (
	// CachePostureColdOriginColdCache means both origin and node-local caches start cold.
	CachePostureColdOriginColdCache CachePosture = "cold-origin-cold-cache"
	// CachePostureWarmBundleColdMaterialization means bundle metadata is warm but child materialization is cold.
	CachePostureWarmBundleColdMaterialization CachePosture = "warm-bundle-cold-materialization"
	// CachePostureWarmNodeLocalCache means bundle and child materialization data are node-local.
	CachePostureWarmNodeLocalCache CachePosture = "warm-node-local-cache"
)

// Summary is the benchmark output contract validated by schemas/fleet.
type Summary struct {
	RunID                     string           `json:"runID"`
	ChildCount                int              `json:"childCount"`
	TargetConcurrency         int              `json:"targetConcurrency"`
	CachePosture              CachePosture     `json:"cachePosture"`
	SuccessRate               float64          `json:"successRate"`
	AdmissionLatencyMS        float64          `json:"admissionLatencyMs"`
	TimeToFirstChildReadyMS   float64          `json:"timeToFirstChildReadyMs"`
	TimeToTargetConcurrencyMS float64          `json:"timeToTargetConcurrencyMs"`
	StagePercentilesMS        StagePercentiles `json:"stagePercentilesMs"`
	OriginBytes               int64            `json:"originBytes"`
	CacheHitRate              float64          `json:"cacheHitRate"`
}

// StagePercentiles groups benchmark phase percentiles.
type StagePercentiles struct {
	ArtifactPull    Percentiles `json:"artifactPull"`
	Materialization Percentiles `json:"materialization"`
	Resume          Percentiles `json:"resume"`
	GuestReady      Percentiles `json:"guestReady"`
	ResultCommit    Percentiles `json:"resultCommit"`
}

// Percentiles reports p50/p95/p99 latency in milliseconds.
type Percentiles struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

// Observation is one child attempt observation used to build a benchmark summary.
type Observation struct {
	RunID           string
	ChildID         int
	Succeeded       bool
	StartedAt       time.Time
	TimingsMS       StageTimings
	OriginBytesRead int64
	CacheHitCount   int
	CacheMissCount  int
}

// StageTimings records benchmark phase latencies in milliseconds.
type StageTimings struct {
	ArtifactPull    float64
	Materialization float64
	Resume          float64
	GuestReady      float64
	ResultCommit    float64
}

// ReadyAt returns the time the child became ready.
func (o Observation) ReadyAt() time.Time {
	readyMS := o.TimingsMS.ArtifactPull + o.TimingsMS.Materialization + o.TimingsMS.Resume + o.TimingsMS.GuestReady
	return o.StartedAt.Add(durationMS(readyMS))
}

func durationMS(ms float64) time.Duration {
	return time.Duration(ms * float64(time.Millisecond))
}
