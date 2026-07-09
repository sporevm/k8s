// Package fleet contains the first SporeVM fleet control-plane contracts.
package fleet

import "time"

// Bundle identifies an immutable SporeVM bundle input.
type Bundle struct {
	URI    string `json:"uri"`
	Digest string `json:"digest"`
}

// ChildRange identifies a global child id range.
type ChildRange struct {
	Start int `json:"start"`
	Count int `json:"count"`
}

// End returns the exclusive end child id.
func (r ChildRange) End() int {
	return r.Start + r.Count
}

// HostClass is the SporeVM restore compatibility class reported by SporeVM.
type HostClass struct {
	ID                   string `json:"id"`
	SporePlatformVersion string `json:"sporePlatformVersion"`
	Architecture         string `json:"architecture"`
	Backend              string `json:"backend"`
	CPUProfile           string `json:"cpuProfile"`
	DeviceModel          string `json:"deviceModel"`
}

// Equal reports whether two host classes are exact placement matches.
func (h HostClass) Equal(other HostClass) bool {
	return h == other
}

// Execution describes shard sizing and per-agent concurrency.
type Execution struct {
	ChildrenPerShard    int `json:"childrenPerShard"`
	MaxInFlightPerAgent int `json:"maxInFlightPerAgent"`
}

// RetryPolicy describes attempt limits for a child.
type RetryPolicy struct {
	MaxAttemptsPerChild    int  `json:"maxAttemptsPerChild"`
	RerunCommittedChildren bool `json:"rerunCommittedChildren"`
}

// SideEffects captures the first retry safety policy.
type SideEffects struct {
	IdempotencyRequired bool `json:"idempotencyRequired"`
}

// RunSource identifies the source used to prepare a parent VM.
type RunSource struct {
	Image    string `json:"image"`
	Platform string `json:"platform"`
}

// CommandSpec is an argv command run inside a SporeVM guest.
type CommandSpec struct {
	Command []string `json:"command"`
}

// PrepareSpec describes the warm command and optional capture trigger.
type PrepareSpec struct {
	Command       []string `json:"command"`
	CaptureSignal string   `json:"captureSignal,omitempty"`
	ReadyMarker   string   `json:"readyMarker,omitempty"`
	Memory        string   `json:"memory,omitempty"`
}

// ForkSpec describes how many children are forked from the prepared parent.
type ForkSpec struct {
	Count int `json:"count"`
}

// RunChildren describes the child range and command for a source run.
type RunChildren struct {
	Start   int      `json:"start"`
	Count   int      `json:"count"`
	Command []string `json:"command"`
}

// ChildRange returns the source child range.
func (c RunChildren) ChildRange() ChildRange {
	return ChildRange{Start: c.Start, Count: c.Count}
}

// Run is the source/prepare/fork contract users submit before a bundle exists.
type Run struct {
	RunID       string      `json:"runID"`
	Source      RunSource   `json:"source"`
	Prepare     PrepareSpec `json:"prepare"`
	Fork        ForkSpec    `json:"fork"`
	Children    RunChildren `json:"children"`
	Execution   Execution   `json:"execution"`
	RetryPolicy RetryPolicy `json:"retryPolicy"`
	SideEffects SideEffects `json:"sideEffects"`
	ResultStore string      `json:"resultStore"`
}

// PreparedBundle is the prepared, forked bundle used to compile a Run.
type PreparedBundle struct {
	Bundle     Bundle               `json:"bundle"`
	ChildCount int                  `json:"childCount"`
	HostClass  HostClass            `json:"hostClass"`
	Local      *PreparedLocalBundle `json:"local,omitempty"`
	TimingsMS  PrepareTimings       `json:"timingsMs,omitempty"`
}

// PreparedLocalBundle identifies same-agent prepared files that are safe to
// reference only inside the preparing agent.
type PreparedLocalBundle struct {
	ParentDir   string `json:"parentDir,omitempty"`
	ChildrenDir string `json:"childrenDir,omitempty"`
	BundleDir   string `json:"bundleDir,omitempty"`
}

// PrepareTimings records source-run preparation phase latencies in milliseconds.
type PrepareTimings struct {
	RunSave       float64 `json:"runSave,omitempty"`
	Fork          float64 `json:"fork,omitempty"`
	Pack          float64 `json:"pack,omitempty"`
	InspectBundle float64 `json:"inspectBundle,omitempty"`
}

// BundleRun is the admitted fleet run contract.
type BundleRun struct {
	RunID        string      `json:"runID"`
	Bundle       Bundle      `json:"bundle"`
	Children     ChildRange  `json:"children"`
	ChildCommand []string    `json:"childCommand,omitempty"`
	HostClass    HostClass   `json:"hostClass"`
	Execution    Execution   `json:"execution"`
	RetryPolicy  RetryPolicy `json:"retryPolicy"`
	SideEffects  SideEffects `json:"sideEffects"`
	ResultStore  string      `json:"resultStore"`
}

// ExecutionSlots reports an agent's current execution capacity.
type ExecutionSlots struct {
	Total     int `json:"total"`
	Available int `json:"available"`
}

// CacheStatus reports node-local cache size and entry counts.
type CacheStatus struct {
	BundleCacheBytes   int64 `json:"bundleCacheBytes"`
	RootFSCacheBytes   int64 `json:"rootfsCacheBytes"`
	BundleCacheEntries int   `json:"bundleCacheEntries"`
	RootFSCacheEntries int   `json:"rootfsCacheEntries"`
}

// PressureLevel is a coarse pressure signal for admission decisions.
type PressureLevel string

const (
	// PressureNormal allows new work.
	PressureNormal PressureLevel = "normal"
	// PressureWarning allows work but should affect placement preference later.
	PressureWarning PressureLevel = "warning"
	// PressureCritical refuses new work.
	PressureCritical PressureLevel = "critical"
)

// Pressure reports coarse resource pressure for an agent.
type Pressure struct {
	Disk   PressureLevel `json:"disk"`
	Memory PressureLevel `json:"memory"`
}

// Critical reports whether either pressure dimension is critical.
func (p Pressure) Critical() bool {
	return p.Disk == PressureCritical || p.Memory == PressureCritical
}

// AgentStatus is the compact status document the coordinator consumes.
type AgentStatus struct {
	AgentID        string         `json:"agentID"`
	CellID         string         `json:"cellID"`
	ObservedAt     time.Time      `json:"observedAt"`
	HostClass      HostClass      `json:"hostClass"`
	ExecutionSlots ExecutionSlots `json:"executionSlots"`
	Cache          CacheStatus    `json:"cache"`
	Pressure       Pressure       `json:"pressure"`
	Healthy        bool           `json:"healthy"`
}

// ShardLease assigns a global child id range to one agent.
type ShardLease struct {
	RunID         string    `json:"runID"`
	BundleDigest  string    `json:"bundleDigest"`
	ShardID       string    `json:"shardID"`
	ChildStart    int       `json:"childStart"`
	ChildCount    int       `json:"childCount"`
	AttemptBudget int       `json:"attemptBudget"`
	HostClassID   string    `json:"hostClassID"`
	AgentID       string    `json:"agentID"`
	LeaseDeadline time.Time `json:"leaseDeadline"`
}

// ChildRange returns the lease's global child id range.
func (l ShardLease) ChildRange() ChildRange {
	return ChildRange{Start: l.ChildStart, Count: l.ChildCount}
}

// AttemptStatus is the terminal or skipped state for one child attempt.
type AttemptStatus string

const (
	// AttemptSucceeded means the child reached a successful terminal event.
	AttemptSucceeded AttemptStatus = "succeeded"
	// AttemptFailed means the child attempt failed or did not reach terminal state.
	AttemptFailed AttemptStatus = "failed"
	// AttemptSkippedTerminalExists means an existing terminal result short-circuited execution.
	AttemptSkippedTerminalExists AttemptStatus = "skipped-terminal-exists"
	// AttemptPlatformMismatch means SporeVM rejected the host/platform contract.
	AttemptPlatformMismatch AttemptStatus = "platform-mismatch"
)

// AttemptTimings records the benchmark-relevant phases for a child attempt.
type AttemptTimings struct {
	ArtifactPull       float64 `json:"artifactPull"`
	Materialization    float64 `json:"materialization"`
	Resume             float64 `json:"resume"`
	GuestReady         float64 `json:"guestReady"`
	ResultCommit       float64 `json:"resultCommit"`
	LocalChild         float64 `json:"localChild,omitempty"`
	PullVerify         float64 `json:"pullVerify,omitempty"`
	PullInstallIndexes float64 `json:"pullInstallIndexes,omitempty"`
	PullInstallChunks  float64 `json:"pullInstallChunks,omitempty"`
}

// AttemptError is the compact error body stored with failed attempts.
type AttemptError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// AttemptOutput records a bounded inline preview plus complete output byte counts.
type AttemptOutput struct {
	StdoutBytes         int64  `json:"stdoutBytes,omitempty"`
	StderrBytes         int64  `json:"stderrBytes,omitempty"`
	StdoutPreviewBase64 string `json:"stdoutPreviewBase64,omitempty"`
	StderrPreviewBase64 string `json:"stderrPreviewBase64,omitempty"`
	StdoutTruncated     bool   `json:"stdoutTruncated,omitempty"`
	StderrTruncated     bool   `json:"stderrTruncated,omitempty"`
}

// AttemptResult is the per-child result document stored outside the coordinator.
type AttemptResult struct {
	RunID        string         `json:"runID"`
	BundleDigest string         `json:"bundleDigest"`
	ChildID      int            `json:"childID"`
	AttemptID    string         `json:"attemptID"`
	AgentID      string         `json:"agentID"`
	ShardID      string         `json:"shardID"`
	Status       AttemptStatus  `json:"status"`
	StartedAt    time.Time      `json:"startedAt"`
	FinishedAt   time.Time      `json:"finishedAt"`
	TimingsMS    AttemptTimings `json:"timingsMs"`
	Terminal     bool           `json:"terminal"`
	ResultURI    string         `json:"resultURI,omitempty"`
	Output       *AttemptOutput `json:"output,omitempty"`
	Error        *AttemptError  `json:"error,omitempty"`
}

// PlanSummary is aggregate coordinator state for a dry-run plan.
type PlanSummary struct {
	RunID               string `json:"runID"`
	State               string `json:"state"`
	ChildCount          int    `json:"childCount"`
	ShardCount          int    `json:"shardCount"`
	AssignedChildren    int    `json:"assignedChildren"`
	AssignedShards      int    `json:"assignedShards"`
	AgentCount          int    `json:"agentCount"`
	HealthyAgents       int    `json:"healthyAgents"`
	CompatibleAgents    int    `json:"compatibleAgents"`
	AvailableChildSlots int    `json:"availableChildSlots"`
}

// Plan is the dry-run output for one coordinator admission decision.
type Plan struct {
	Summary PlanSummary  `json:"summary"`
	Leases  []ShardLease `json:"leases"`
}

// BundleInspection is the coordinator's admission-time view of bundle metadata.
type BundleInspection struct {
	BundleDigest string    `json:"bundleDigest"`
	ChildCount   int       `json:"childCount"`
	HostClass    HostClass `json:"hostClass"`
}

// ShardExecutionRequest is the coordinator-to-agent runtime lease request.
type ShardExecutionRequest struct {
	Run     BundleRun            `json:"run"`
	Lease   ShardLease           `json:"lease"`
	Attempt int                  `json:"attempt"`
	Local   *PreparedLocalBundle `json:"local,omitempty"`
}

// RuntimePrepareSummary reports the source-run prepare work that happened
// before a compiled bundle run was admitted.
type RuntimePrepareSummary struct {
	AgentID      string         `json:"agentID"`
	BundleDigest string         `json:"bundleDigest"`
	ChildCount   int            `json:"childCount"`
	TimingsMS    PrepareTimings `json:"timingsMs"`
}

// RuntimeSummary is compact aggregate status for one coordinator run.
type RuntimeSummary struct {
	RunID                   string    `json:"runID"`
	State                   string    `json:"state"`
	ChildCount              int       `json:"childCount"`
	ShardCount              int       `json:"shardCount"`
	AssignedChildren        int       `json:"assignedChildren"`
	AssignedShards          int       `json:"assignedShards"`
	AttemptCount            int       `json:"attemptCount"`
	CompletedChildren       int       `json:"completedChildren"`
	SucceededChildren       int       `json:"succeededChildren"`
	FailedChildren          int       `json:"failedChildren"`
	SkippedTerminalChildren int       `json:"skippedTerminalChildren"`
	PlatformMismatches      int       `json:"platformMismatches"`
	NonTerminalFailures     int       `json:"nonTerminalFailures"`
	LeaseErrors             int       `json:"leaseErrors"`
	StartedAt               time.Time `json:"startedAt"`
	FinishedAt              time.Time `json:"finishedAt"`
}

// RuntimeTimingSummary reports compact timing distributions for terminal child attempts.
type RuntimeTimingSummary struct {
	ChildReadyMS       RuntimePercentiles      `json:"childReadyMs"`
	StagePercentilesMS RuntimeStagePercentiles `json:"stagePercentilesMs"`
}

// RuntimeStagePercentiles groups runtime phase percentiles.
type RuntimeStagePercentiles struct {
	ArtifactPull    RuntimePercentiles `json:"artifactPull"`
	Materialization RuntimePercentiles `json:"materialization"`
	Resume          RuntimePercentiles `json:"resume"`
	GuestReady      RuntimePercentiles `json:"guestReady"`
	ResultCommit    RuntimePercentiles `json:"resultCommit"`
}

// RuntimePercentiles reports p50/p95/p99 latency in milliseconds.
type RuntimePercentiles struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

// RuntimeReport is the coordinator result for one admitted run.
type RuntimeReport struct {
	Plan    Plan                   `json:"plan"`
	Summary RuntimeSummary         `json:"summary"`
	Timings *RuntimeTimingSummary  `json:"timings,omitempty"`
	Prepare *RuntimePrepareSummary `json:"prepare,omitempty"`
}
