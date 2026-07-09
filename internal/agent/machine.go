package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/sporevm/k8s/internal/fleet"
)

const (
	hostInfoSchema      = "spore.host-info.v1"
	inspectBundleSchema = "spore.bundle.inspect.v1"
	pullResultSchema    = "spore.pull.result.v1"
	errorSchema         = "spore.error.v1"
	runEventsSchema     = "spore.run-events.v1"
	schemaVersion       = 1
)

var (
	// ErrInvalidMachineOutput means SporeVM returned malformed machine output.
	ErrInvalidMachineOutput = errors.New("invalid sporevm machine output")
	// ErrNoTerminalEvent means a run/resume JSONL stream did not end cleanly.
	ErrNoTerminalEvent = errors.New("missing terminal event")
	// ErrInvalidSporeRequest means the agent built an invalid SporeVM request.
	ErrInvalidSporeRequest = errors.New("invalid sporevm request")
)

// HostInfo is the `spore --json host-info` result.
type HostInfo struct {
	Schema        string                `json:"schema"`
	SchemaVersion int                   `json:"schema_version"`
	HostClass     string                `json:"host_class"`
	Platform      PlatformFacts         `json:"platform"`
	Backends      []BackendAvailability `json:"backends"`
	CacheRoots    CacheRoots            `json:"cache_roots"`
}

// PlatformFacts describes exact restore compatibility facts for a host.
type PlatformFacts struct {
	OS                     string `json:"os"`
	Arch                   string `json:"arch"`
	CPUProfile             string `json:"cpu_profile"`
	DeviceModelVersion     uint32 `json:"device_model_version"`
	RAMBase                uint64 `json:"ram_base"`
	GICDistBase            uint64 `json:"gic_dist_base"`
	GICRedistBase          uint64 `json:"gic_redist_base"`
	CounterFrequencySource string `json:"counter_frequency_source"`
	CounterFrequencyHz     uint64 `json:"counter_frequency_hz"`
}

// BackendAvailability reports whether a SporeVM backend can run on this host.
type BackendAvailability struct {
	Name      string `json:"name"`
	Supported bool   `json:"supported"`
	Available bool   `json:"available"`
	Reason    string `json:"reason"`
}

// CacheRoots reports SporeVM cache and runtime root resolution.
type CacheRoots struct {
	Kernels PathFact `json:"kernels"`
	RootFS  PathFact `json:"rootfs"`
	Bundles PathFact `json:"bundles"`
	Runtime PathFact `json:"runtime"`
}

// PathFact describes one resolved or unresolved local path.
type PathFact struct {
	Path     *string `json:"path"`
	Resolved bool    `json:"resolved"`
	Source   string  `json:"source"`
}

// AvailableBackend returns a supported and available backend by name.
func (h HostInfo) AvailableBackend(name string) (BackendAvailability, bool) {
	for _, backend := range h.Backends {
		if backend.Name == name && backend.Supported && backend.Available {
			return backend, true
		}
	}
	return BackendAvailability{}, false
}

// FleetHostClass maps SporeVM host-info into the fleet placement contract.
func (h HostInfo) FleetHostClass(backend string) (fleet.HostClass, error) {
	if err := h.Validate(); err != nil {
		return fleet.HostClass{}, err
	}
	if _, ok := h.AvailableBackend(backend); !ok {
		return fleet.HostClass{}, fmt.Errorf("%w: backend %q is not available", ErrInvalidMachineOutput, backend)
	}
	return fleet.HostClass{
		ID:                   fmt.Sprintf("%s-%s-v%d", h.HostClass, h.Platform.CPUProfile, h.Platform.DeviceModelVersion),
		SporePlatformVersion: "v0",
		Architecture:         h.Platform.Arch,
		Backend:              backend,
		CPUProfile:           h.Platform.CPUProfile,
		DeviceModel:          fmt.Sprintf("sporevm-%s-v%d", h.Platform.Arch, h.Platform.DeviceModelVersion),
	}, nil
}

// Validate checks the host-info schema identity.
func (h HostInfo) Validate() error {
	if h.Schema != hostInfoSchema || h.SchemaVersion != schemaVersion {
		return invalidMachineOutput("host-info schema = %q v%d", h.Schema, h.SchemaVersion)
	}
	if h.HostClass == "" {
		return invalidMachineOutput("host-info host_class is empty")
	}
	if h.Platform.Arch == "" || h.Platform.CPUProfile == "" {
		return invalidMachineOutput("host-info platform is incomplete")
	}
	return nil
}

// DigestRef is SporeVM's shared digest reference.
type DigestRef struct {
	Algorithm string `json:"algorithm"`
	Hex       string `json:"hex"`
}

// String returns the digest in algorithm:hex form.
func (d DigestRef) String() string {
	if d.Algorithm == "" || d.Hex == "" {
		return ""
	}
	return d.Algorithm + ":" + d.Hex
}

// CacheState reports cache hits, misses, and fetched bytes.
type CacheState struct {
	HitCount     int   `json:"hit_count"`
	MissCount    int   `json:"miss_count"`
	BytesFetched int64 `json:"bytes_fetched"`
}

// ChunkMaterializationSummary reports spore chunk materialization.
type ChunkMaterializationSummary struct {
	ChunkCount             int        `json:"chunk_count"`
	MaterializedChunkCount int        `json:"materialized_chunk_count"`
	PayloadBytes           int64      `json:"payload_bytes"`
	LinkedChunkCount       int        `json:"linked_chunk_count"`
	CopiedChunkCount       int        `json:"copied_chunk_count"`
	Cache                  CacheState `json:"cache"`
}

// RootFSMaterializationSummary reports rootfs materialization.
type RootFSMaterializationSummary struct {
	ArtifactCount int        `json:"artifact_count"`
	PayloadBytes  int64      `json:"payload_bytes"`
	Cache         CacheState `json:"cache"`
}

// InspectBundleRequest is the agent-owned bundle inspection request.
type InspectBundleRequest struct {
	Source     string
	ChildID    string
	ChildRange *ChildRangeSelection
}

func (r InspectBundleRequest) validate() error {
	if r.Source == "" {
		return invalidSporeRequest("inspect-bundle source is required")
	}
	if r.ChildID != "" && r.ChildRange != nil {
		return invalidSporeRequest("inspect-bundle cannot select both child id and child range")
	}
	if r.ChildRange != nil && (r.ChildRange.Start < 0 || r.ChildRange.Start >= r.ChildRange.End) {
		return invalidSporeRequest("inspect-bundle child range must be non-empty and non-negative")
	}
	return nil
}

// ChildRangeSelection mirrors `spore --json inspect-bundle --child-range`.
type ChildRangeSelection struct {
	Start int
	End   int
}

// InspectBundleResult is the `spore --json inspect-bundle` result.
type InspectBundleResult struct {
	Schema         string                 `json:"schema"`
	SchemaVersion  int                    `json:"schema_version"`
	Source         string                 `json:"source"`
	BundleDir      string                 `json:"bundle_dir"`
	BundleDigest   DigestRef              `json:"bundle_digest"`
	Indexed        bool                   `json:"indexed"`
	ParentManifest string                 `json:"parent_manifest"`
	ChunkpackIndex string                 `json:"chunkpack_index"`
	Chunkpack      ChunkpackSummary       `json:"chunkpack"`
	ChildCount     int                    `json:"child_count"`
	Children       []BundleChildSummary   `json:"children"`
	Selection      BundleSelectionSummary `json:"selection"`
	RootFS         RootFSBundleSummary    `json:"rootfs"`
}

// ChunkpackSummary reports bundle chunkpack size.
type ChunkpackSummary struct {
	ChunkCount   int   `json:"chunk_count"`
	PackCount    int   `json:"pack_count"`
	PayloadBytes int64 `json:"payload_bytes"`
}

// BundleChildSummary identifies a child manifest inside an indexed bundle.
type BundleChildSummary struct {
	ID       string `json:"id"`
	Manifest string `json:"manifest"`
}

// BundleSelectionSummary reports an inspect-bundle child selection.
type BundleSelectionSummary struct {
	Kind          string               `json:"kind"`
	SelectedCount int                  `json:"selected_count"`
	Children      []BundleChildSummary `json:"children"`
}

// RootFSBundleSummary reports rootfs artifacts referenced by a bundle.
type RootFSBundleSummary struct {
	ArtifactCount     int   `json:"artifact_count"`
	ExactBytesCount   int   `json:"exact_bytes_count"`
	MetadataOnlyCount int   `json:"metadata_only_count"`
	PayloadBytes      int64 `json:"payload_bytes"`
}

// Validate checks the inspect-bundle schema identity.
func (r InspectBundleResult) Validate() error {
	if r.Schema != inspectBundleSchema || r.SchemaVersion != schemaVersion {
		return invalidMachineOutput("inspect-bundle schema = %q v%d", r.Schema, r.SchemaVersion)
	}
	if r.BundleDigest.String() == "" {
		return invalidMachineOutput("inspect-bundle bundle_digest is empty")
	}
	return nil
}

// PullRequest is the agent-owned materialization request.
type PullRequest struct {
	Source                  string
	OutDir                  string
	ChildID                 string
	Region                  string
	AllowMetadataOnlyRootFS bool
}

func (r PullRequest) validate() error {
	if r.Source == "" {
		return invalidSporeRequest("pull source is required")
	}
	if r.OutDir == "" {
		return invalidSporeRequest("pull out dir is required")
	}
	return nil
}

// PullResult is the `spore --json pull` result.
type PullResult struct {
	Schema          string                       `json:"schema"`
	SchemaVersion   int                          `json:"schema_version"`
	Source          string                       `json:"source"`
	BundleDir       string                       `json:"bundle_dir"`
	OutDir          string                       `json:"out_dir"`
	BundleDigest    DigestRef                    `json:"bundle_digest"`
	Materialization ChunkMaterializationSummary  `json:"materialization"`
	RootFS          RootFSMaterializationSummary `json:"rootfs"`
	Remote          RemoteBundleCache            `json:"remote"`
	Children        BundleChildrenSummary        `json:"children"`
	Timings         *PullTimings                 `json:"timings,omitempty"`
}

// PullTimings carries optional SporeVM pull sub-phase timings when the CLI
// exposes them.
type PullTimings struct {
	Verify         float64 `json:"verify,omitempty"`
	InstallIndexes float64 `json:"install_indexes,omitempty"`
	InstallChunks  float64 `json:"install_chunks,omitempty"`
}

// RemoteBundleCache reports remote bundle source bytes.
type RemoteBundleCache struct {
	CacheHit        bool  `json:"cache_hit"`
	OriginBytesRead int64 `json:"origin_bytes_read"`
	PeerBytesRead   int64 `json:"peer_bytes_read"`
}

// BundleChildrenSummary reports selected child materialization.
type BundleChildrenSummary struct {
	Count         int     `json:"count"`
	SelectedChild *string `json:"selected_child"`
}

// Validate checks the pull-result schema identity.
func (r PullResult) Validate() error {
	if r.Schema != pullResultSchema || r.SchemaVersion != schemaVersion {
		return invalidMachineOutput("pull schema = %q v%d", r.Schema, r.SchemaVersion)
	}
	if r.BundleDigest.String() == "" {
		return invalidMachineOutput("pull bundle_digest is empty")
	}
	if r.OutDir == "" {
		return invalidMachineOutput("pull out_dir is empty")
	}
	return nil
}

// RunCaptureRequest is the agent-owned parent preparation request.
type RunCaptureRequest struct {
	Image         string
	CaptureDir    string
	CaptureSignal string
	ReadyMarker   string
	Backend       string
	Memory        string
	Command       []string
}

func (r RunCaptureRequest) validate() error {
	if r.Image == "" {
		return invalidSporeRequest("run capture image is required")
	}
	if r.CaptureDir == "" {
		return invalidSporeRequest("run capture dir is required")
	}
	if len(r.Command) == 0 {
		return invalidSporeRequest("run capture command is required")
	}
	for i, arg := range r.Command {
		if arg == "" {
			return invalidSporeRequest("run capture command[%d] must not be empty", i)
		}
	}
	if r.CaptureSignal == "" && r.ReadyMarker == "" {
		return nil
	}
	if r.CaptureSignal == "" {
		return invalidSporeRequest("run capture signal is required when ready marker is set")
	}
	if r.ReadyMarker == "" {
		return invalidSporeRequest("run capture ready marker is required when capture signal is set")
	}
	return nil
}

// CreateVMRequest is the agent-owned named VM creation request.
type CreateVMRequest struct {
	Name    string   `json:"name"`
	Image   string   `json:"image"`
	Backend string   `json:"backend,omitempty"`
	Memory  string   `json:"memory,omitempty"`
	Command []string `json:"command"`
}

func (r CreateVMRequest) validate() error {
	if r.Name == "" {
		return invalidSporeRequest("create VM name is required")
	}
	if r.Image == "" {
		return invalidSporeRequest("create VM image is required")
	}
	if len(r.Command) == 0 {
		return invalidSporeRequest("create VM command is required")
	}
	for i, arg := range r.Command {
		if arg == "" {
			return invalidSporeRequest("create VM command[%d] must not be empty", i)
		}
	}
	return nil
}

// ForkRequest is the agent-owned child fork request.
type ForkRequest struct {
	ParentDir string
	Count     int
	OutDir    string
}

func (r ForkRequest) validate() error {
	if r.ParentDir == "" {
		return invalidSporeRequest("fork parent dir is required")
	}
	if r.Count < 1 {
		return invalidSporeRequest("fork count must be >= 1")
	}
	if r.OutDir == "" {
		return invalidSporeRequest("fork out dir is required")
	}
	return nil
}

// PackRequest is the agent-owned bundle pack request.
type PackRequest struct {
	ParentDir   string
	ChildrenDir string
	OutDir      string
}

func (r PackRequest) validate() error {
	if r.ParentDir == "" {
		return invalidSporeRequest("pack parent dir is required")
	}
	if r.ChildrenDir == "" {
		return invalidSporeRequest("pack children dir is required")
	}
	if r.OutDir == "" {
		return invalidSporeRequest("pack out dir is required")
	}
	return nil
}

// ResumeRequest is the agent-owned resume request.
type ResumeRequest struct {
	SporeDir       string
	Backend        string
	GenerationPath string
	Name           string
}

func (r ResumeRequest) validate() error {
	if r.SporeDir == "" {
		return invalidSporeRequest("resume spore dir is required")
	}
	return nil
}

// ExecRequest is one command execution inside a named resumed VM.
type ExecRequest struct {
	Name    string
	Command []string
}

func (r ExecRequest) validate() error {
	if r.Name == "" {
		return invalidSporeRequest("exec VM name is required")
	}
	if len(r.Command) == 0 {
		return invalidSporeRequest("exec command is required")
	}
	for i, arg := range r.Command {
		if arg == "" {
			return invalidSporeRequest("exec command[%d] must not be empty", i)
		}
	}
	return nil
}

// RemoveVMRequest removes one named SporeVM lifecycle target.
type RemoveVMRequest struct {
	Name string
}

func (r RemoveVMRequest) validate() error {
	if r.Name == "" {
		return invalidSporeRequest("remove VM name is required")
	}
	return nil
}

// RunEvent is one SporeVM JSONL lifecycle event.
type RunEvent struct {
	Schema           string            `json:"schema"`
	SchemaVersion    int               `json:"schema_version"`
	Event            string            `json:"event"`
	Command          string            `json:"command"`
	RequestedBackend string            `json:"requested_backend,omitempty"`
	Backend          *string           `json:"backend,omitempty"`
	Offset           uint64            `json:"offset,omitempty"`
	ByteCount        int               `json:"byte_count,omitempty"`
	DataBase64       string            `json:"data_base64,omitempty"`
	ExitCode         *int              `json:"exit_code,omitempty"`
	VCPUs            uint32            `json:"vcpus,omitempty"`
	MemoryBytes      uint64            `json:"memory_bytes,omitempty"`
	Captured         bool              `json:"captured,omitempty"`
	CapturePath      *string           `json:"capture_path,omitempty"`
	Timings          *RunEventTimings  `json:"timings,omitempty"`
	Error            *MachineErrorBody `json:"error,omitempty"`
}

// RunEventTimings reports event-mode runtime timings.
type RunEventTimings struct {
	StartMS         uint64 `json:"start_ms"`
	VSockConnectMS  uint64 `json:"vsock_connect_ms"`
	ExecResponseMS  uint64 `json:"exec_response_ms"`
	ProbeDurationMS uint64 `json:"probe_duration_ms"`
}

// Terminal reports whether the event ends a JSONL stream.
func (e RunEvent) Terminal() bool {
	return e.Event == "exit" || e.Event == "failure"
}

// Validate checks one run/resume event.
func (e RunEvent) Validate() error {
	if e.Schema != runEventsSchema || e.SchemaVersion != schemaVersion {
		return invalidMachineOutput("run event schema = %q v%d", e.Schema, e.SchemaVersion)
	}
	if e.Event == "" || e.Command == "" {
		return invalidMachineOutput("run event is missing event or command")
	}
	if e.Event == "failure" && e.Error == nil {
		return invalidMachineOutput("failure event is missing error")
	}
	if e.Event == "exit" && e.ExitCode == nil {
		return invalidMachineOutput("exit event is missing exit_code")
	}
	return nil
}

// TerminalEvent returns the single terminal event from an event stream.
func TerminalEvent(events []RunEvent) (RunEvent, error) {
	var terminal *RunEvent
	for i := range events {
		if !events[i].Terminal() {
			continue
		}
		if terminal != nil {
			return RunEvent{}, invalidMachineOutput("run event stream has multiple terminal events")
		}
		terminal = &events[i]
	}
	if terminal == nil {
		return RunEvent{}, ErrNoTerminalEvent
	}
	return *terminal, nil
}

// DecodeRunEvents decodes newline-delimited run/resume events.
func DecodeRunEvents(r io.Reader) ([]RunEvent, error) {
	decoder := json.NewDecoder(r)
	var events []RunEvent
	for {
		var event RunEvent
		if err := decoder.Decode(&event); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("%w: decode run event: %v", ErrInvalidMachineOutput, err)
		}
		if err := event.Validate(); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

// MachineErrorEnvelope is SporeVM's shared machine error envelope.
type MachineErrorEnvelope struct {
	Schema        string           `json:"schema"`
	SchemaVersion int              `json:"schema_version"`
	Error         MachineErrorBody `json:"error"`
}

// MachineErrorBody is the stable caller-facing error body.
type MachineErrorBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
	Scope     string `json:"scope"`
	ExitCode  int    `json:"exit_code"`
	Source    string `json:"source"`
}

// MachineError wraps a SporeVM-originated machine error.
type MachineError struct {
	Envelope MachineErrorEnvelope
	Stderr   string
}

// Error returns a concise machine error string.
func (e *MachineError) Error() string {
	if e == nil {
		return ""
	}
	body := e.Envelope.Error
	message := body.Message
	if message == "" {
		message = strings.TrimSpace(e.Stderr)
	}
	if message == "" {
		message = "spore command failed"
	}
	if body.Code == "" {
		return message
	}
	return body.Code + ": " + message
}

// Validate checks the shared error envelope.
func (e MachineErrorEnvelope) Validate() error {
	if e.Schema != errorSchema || e.SchemaVersion != schemaVersion {
		return invalidMachineOutput("error schema = %q v%d", e.Schema, e.SchemaVersion)
	}
	if e.Error.Code == "" || e.Error.Scope == "" {
		return invalidMachineOutput("machine error is missing code or scope")
	}
	return nil
}

func invalidMachineOutput(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidMachineOutput}, args...)...)
}

func invalidSporeRequest(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidSporeRequest}, args...)...)
}
