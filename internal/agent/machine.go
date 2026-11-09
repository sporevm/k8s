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
	versionSchema       = "spore.version.result.v1"
	hostInfoV3Schema    = "spore.host-info.v3"
	lifecycleSchema     = "spore.lifecycle.v1"
	inspectBundleSchema = "spore.inspect.result.v1"
	pullResultSchema    = "spore.pull.result.v2"
	removeSavedV2Schema = "spore.saved.remove.result.v2"
	cacheGCV2Schema     = "spore.cache.gc.result.v2"
	errorSchema         = "spore.error.v1"
	runEventsSchema     = "spore.automation.event.v1"
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

// VersionResult is the `spore --json version` result.
type VersionResult struct {
	Schema        string `json:"schema"`
	SchemaVersion int    `json:"schema_version"`
	Version       string `json:"version"`
	BuildMode     string `json:"build_mode"`
}

// Validate checks the version schema identity.
func (r VersionResult) Validate() error {
	if r.Schema != versionSchema || r.SchemaVersion != schemaVersion {
		return invalidMachineOutput("version schema = %q v%d", r.Schema, r.SchemaVersion)
	}
	if r.Version == "" {
		return invalidMachineOutput("version is empty")
	}
	return nil
}

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

// UnmarshalJSON normalizes SporeVM's v3 system-info
// architecture-specific platform facts into the fleet adapter's shared view.
func (h *HostInfo) UnmarshalJSON(data []byte) error {
	var header struct {
		Schema        string `json:"schema"`
		SchemaVersion int    `json:"schema_version"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return err
	}

	switch {
	case header.Schema == hostInfoV3Schema && header.SchemaVersion == 3:
		var decoded struct {
			Schema        string                `json:"schema"`
			SchemaVersion int                   `json:"schema_version"`
			HostClass     string                `json:"host_class"`
			Architecture  string                `json:"architecture"`
			Platform      hostInfoV3Platform    `json:"platform"`
			Backends      []BackendAvailability `json:"backends"`
			CacheRoots    CacheRoots            `json:"cache_roots"`
		}
		if err := json.Unmarshal(data, &decoded); err != nil {
			return err
		}
		platform, err := decoded.Platform.normalize(decoded.Architecture)
		if err != nil {
			return err
		}
		*h = HostInfo{
			Schema:        decoded.Schema,
			SchemaVersion: decoded.SchemaVersion,
			HostClass:     decoded.HostClass,
			Platform:      platform,
			Backends:      decoded.Backends,
			CacheRoots:    decoded.CacheRoots,
		}
		return nil
	default:
		return invalidMachineOutput("host-info schema = %q v%d", header.Schema, header.SchemaVersion)
	}
}

type hostInfoV3Platform struct {
	ARM64 *struct {
		OS                  string `json:"os"`
		CPUProfile          string `json:"cpu_profile"`
		DeviceModelVersion  uint32 `json:"device_model_version"`
		RAMBase             uint64 `json:"ram_base"`
		InterruptController struct {
			DistributorBase   uint64 `json:"distributor_base"`
			RedistributorBase uint64 `json:"redistributor_base"`
		} `json:"interrupt_controller"`
		Counter struct {
			Source      string `json:"source"`
			FrequencyHz uint64 `json:"frequency_hz"`
		} `json:"counter"`
	} `json:"arm64"`
	AMD64 *struct {
		OS                 string `json:"os"`
		CPUProfile         string `json:"cpu_profile"`
		DeviceModelVersion uint32 `json:"device_model_version"`
		RAM                struct {
			Base uint64 `json:"base"`
		} `json:"ram"`
	} `json:"amd64"`
}

func (p hostInfoV3Platform) normalize(architecture string) (PlatformFacts, error) {
	switch architecture {
	case "arm64":
		if p.ARM64 == nil || p.AMD64 != nil {
			return PlatformFacts{}, invalidMachineOutput("host-info v3 arm64 platform variant is invalid")
		}
		return PlatformFacts{
			OS:                     p.ARM64.OS,
			Arch:                   "aarch64",
			CPUProfile:             p.ARM64.CPUProfile,
			DeviceModelVersion:     p.ARM64.DeviceModelVersion,
			RAMBase:                p.ARM64.RAMBase,
			GICDistBase:            p.ARM64.InterruptController.DistributorBase,
			GICRedistBase:          p.ARM64.InterruptController.RedistributorBase,
			CounterFrequencySource: p.ARM64.Counter.Source,
			CounterFrequencyHz:     p.ARM64.Counter.FrequencyHz,
		}, nil
	case "amd64":
		if p.AMD64 == nil || p.ARM64 != nil {
			return PlatformFacts{}, invalidMachineOutput("host-info v3 amd64 platform variant is invalid")
		}
		return PlatformFacts{
			OS:                 p.AMD64.OS,
			Arch:               "x86_64",
			CPUProfile:         p.AMD64.CPUProfile,
			DeviceModelVersion: p.AMD64.DeviceModelVersion,
			RAMBase:            p.AMD64.RAM.Base,
		}, nil
	default:
		return PlatformFacts{}, invalidMachineOutput("host-info v3 architecture = %q", architecture)
	}
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
	hostClass := fleet.HostClass{
		ID:                   fmt.Sprintf("%s-%s-v%d", h.HostClass, h.Platform.CPUProfile, h.Platform.DeviceModelVersion),
		SporePlatformVersion: "v0",
		Architecture:         h.Platform.Arch,
		Backend:              backend,
		CPUProfile:           h.Platform.CPUProfile,
		DeviceModel:          fmt.Sprintf("sporevm-%s-v%d", h.Platform.Arch, h.Platform.DeviceModelVersion),
	}
	if err := hostClass.Validate("hostClass"); err != nil {
		return fleet.HostClass{}, fmt.Errorf("%w: unsupported fleet host class: %v", ErrInvalidMachineOutput, err)
	}
	return hostClass, nil
}

// Validate checks the host-info schema identity.
func (h HostInfo) Validate() error {
	if h.Schema != hostInfoV3Schema || h.SchemaVersion != 3 {
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

// InspectCheckpointRequest inspects a locally materialized native checkpoint.
type InspectCheckpointRequest struct {
	Source string
}

func (r InspectCheckpointRequest) validate() error {
	if r.Source == "" {
		return invalidSporeRequest("checkpoint inspect source is required")
	}
	return nil
}

// InspectCheckpointResult is the native checkpoint inspect result.
type InspectCheckpointResult struct {
	Schema         string        `json:"schema"`
	SchemaVersion  int           `json:"schema_version"`
	Version        int           `json:"version"`
	VMStatePresent bool          `json:"vm_state_present"`
	CanAttach      bool          `json:"can_attach"`
	CanRunFrom     bool          `json:"can_run_from"`
	Platform       PlatformFacts `json:"platform"`
	Generation     uint64        `json:"-"` // Verified local manifest; v0.18 inspect omits this field.
}

// Validate requires the released checkpoint format and executable VM state.
func (r InspectCheckpointResult) Validate() error {
	if r.Schema != inspectBundleSchema || r.SchemaVersion != schemaVersion {
		return invalidMachineOutput("checkpoint inspect schema = %q v%d", r.Schema, r.SchemaVersion)
	}
	if r.Version != 8 || !r.VMStatePresent || !r.CanRunFrom {
		return invalidMachineOutput("checkpoint must contain runnable format-v8 VM state")
	}
	return nil
}

// PullRequest is the agent-owned materialization request.
type PullRequest struct {
	Source string
	OutDir string
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

// PullResult is the `spore --json checkpoint pull` v2 result.
type PullResult struct {
	Schema        string      `json:"schema"`
	SchemaVersion int         `json:"schema_version"`
	Ref           string      `json:"ref"`
	SporeDigest   string      `json:"spore_digest"`
	OutDir        string      `json:"out_dir"`
	RAM           PullObjects `json:"ram"`
	Disk          PullObjects `json:"disk"`
}

// PullObjects records verified native-store object transfer.
type PullObjects struct {
	Objects        int   `json:"objects"`
	ObjectsFetched int   `json:"objects_fetched"`
	BytesFetched   int64 `json:"bytes_fetched"`
}

// Validate checks the pull-result schema identity.
func (r PullResult) Validate() error {
	if r.Schema != pullResultSchema || r.SchemaVersion != 2 {
		return invalidMachineOutput("pull schema = %q v%d", r.Schema, r.SchemaVersion)
	}
	if !validCheckpointDigest(r.SporeDigest) || !strings.HasSuffix(r.Ref, "/"+r.SporeDigest) {
		return invalidMachineOutput("pull checkpoint identity is invalid")
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

// PushRequest publishes one parent checkpoint to a native store.
type PushRequest struct {
	ParentDir string
	To        string
}

func (r PushRequest) validate() error {
	if r.ParentDir == "" {
		return invalidSporeRequest("checkpoint push parent dir is required")
	}
	if r.To == "" {
		return invalidSporeRequest("checkpoint push destination is required")
	}
	return nil
}

// PushResult identifies the immutable native checkpoint published by SporeVM.
type PushResult struct {
	Schema        string `json:"schema"`
	SchemaVersion int    `json:"schema_version"`
	DigestRef     string `json:"digest_ref"`
	SporeDigest   string `json:"spore_digest"`
}

func (r PushResult) Validate() error {
	if r.Schema != "spore.push.result.v2" || r.SchemaVersion != 2 ||
		!validCheckpointDigest(r.SporeDigest) || !strings.HasSuffix(r.DigestRef, "/"+r.SporeDigest) {
		return invalidMachineOutput("checkpoint push identity or schema is invalid")
	}
	return nil
}

func validCheckpointDigest(value string) bool {
	if !strings.HasPrefix(value, "blake3:") || len(value) != 71 {
		return false
	}
	for _, c := range value[7:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// ResumeRequest is the agent-owned resume request.
type ResumeRequest struct {
	SporeDir       string
	Backend        string
	GenerationPath string
}

func (r ResumeRequest) validate() error {
	if r.SporeDir == "" {
		return invalidSporeRequest("resume spore dir is required")
	}
	return nil
}

// RestoreNamedRequest starts one persistent named VM from a saved spore.
type RestoreNamedRequest struct {
	SporeDir       string
	Backend        string
	GenerationPath string
	Name           string
}

func (r RestoreNamedRequest) validate() error {
	if r.SporeDir == "" {
		return invalidSporeRequest("named restore spore dir is required")
	}
	if r.Name == "" {
		return invalidSporeRequest("named restore VM name is required")
	}
	return nil
}

// NamedLifecycleResult is the SporeVM named lifecycle machine contract.
type NamedLifecycleResult struct {
	Schema        string                `json:"schema"`
	SchemaVersion uint32                `json:"schema_version"`
	Action        string                `json:"action"`
	Name          string                `json:"name"`
	State         string                `json:"state"`
	Timing        *NamedLifecycleTiming `json:"timing"`
}

// NamedLifecycleTiming reports named restore startup phases in milliseconds.
type NamedLifecycleTiming struct {
	PrepareMS       uint64 `json:"prepare_ms"`
	SpawnMonitorMS  uint64 `json:"spawn_monitor_ms"`
	WaitExecReadyMS uint64 `json:"wait_exec_ready_ms"`
	TotalMS         uint64 `json:"total_ms"`
}

func (r NamedLifecycleResult) validateRestored(name string) error {
	if r.Schema != lifecycleSchema || r.SchemaVersion != uint32(schemaVersion) {
		return invalidMachineOutput("named lifecycle schema = %q v%d", r.Schema, r.SchemaVersion)
	}
	if r.Action != "restored" || r.State != "ready" {
		return invalidMachineOutput("named lifecycle action = %q state = %q", r.Action, r.State)
	}
	if r.Name != name {
		return invalidMachineOutput("named lifecycle name = %q, want %q", r.Name, name)
	}
	if r.Timing == nil {
		return invalidMachineOutput("named lifecycle timing is missing")
	}
	return nil
}

// RunFromRequest is one command execution from a saved spore.
type RunFromRequest struct {
	SporeDir       string
	Backend        string
	GenerationPath string
	Command        []string
}

func (r RunFromRequest) validate() error {
	if r.SporeDir == "" {
		return invalidSporeRequest("run-from spore dir is required")
	}
	if len(r.Command) == 0 {
		return invalidSporeRequest("run-from command is required")
	}
	for i, arg := range r.Command {
		if arg == "" {
			return invalidSporeRequest("run-from command[%d] must not be empty", i)
		}
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

// RemoveSavedSporeRequest removes a machine-local saved spore and its durable pin.
type RemoveSavedSporeRequest struct {
	SporeDir string
}

func (r RemoveSavedSporeRequest) validate() error {
	if r.SporeDir == "" {
		return invalidSporeRequest("remove saved spore dir is required")
	}
	return nil
}

type removedSavedSporeResult struct {
	Schema        string `json:"schema"`
	SchemaVersion int    `json:"schema_version"`
	Action        string `json:"action"`
}

// Validate checks the native checkpoint removal result.
func (r removedSavedSporeResult) Validate() error {
	if r.Schema != removeSavedV2Schema || r.SchemaVersion != 2 {
		return invalidMachineOutput("remove saved spore schema = %q v%d", r.Schema, r.SchemaVersion)
	}
	if r.Action != "removed_spore" {
		return invalidMachineOutput("remove saved spore action = %q", r.Action)
	}
	return nil
}

// CacheGCResult is the part of the `spore --json cache gc` result the agent logs.
type CacheGCResult struct {
	Schema        string `json:"schema"`
	SchemaVersion int    `json:"schema_version"`
	DryRun        bool   `json:"dry_run"`
	DeletedCount  int    `json:"deleted_count"`
	DeletedBytes  uint64 `json:"deleted_bytes"`
}

// Validate checks the native GC result and rejects a dry run.
func (r CacheGCResult) Validate() error {
	if r.Schema != cacheGCV2Schema || r.SchemaVersion != 2 {
		return invalidMachineOutput("cache gc schema = %q v%d", r.Schema, r.SchemaVersion)
	}
	if r.DryRun {
		return invalidMachineOutput("cache gc reported a dry run")
	}
	return nil
}

// RunEvent is one SporeVM JSONL lifecycle event.
type RunEvent struct {
	Schema           string            `json:"schema"`
	SchemaVersion    int               `json:"schema_version"`
	Event            string            `json:"event"`
	Outcome          string            `json:"outcome,omitempty"`
	Command          string            `json:"command"`
	RequestedBackend string            `json:"requested_backend,omitempty"`
	Backend          *string           `json:"backend,omitempty"`
	Offset           uint64            `json:"offset,omitempty"`
	ByteCount        int               `json:"byte_count,omitempty"`
	DataBase64       string            `json:"data_base64,omitempty"`
	ExitCode         *int              `json:"exit_code,omitempty"`
	VCPUs            uint32            `json:"vcpus,omitempty"`
	MemoryBytes      uint64            `json:"memory_bytes,omitempty"`
	MaxMemoryBytes   uint64            `json:"max_memory_bytes,omitempty"`
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
	return e.Event == "completion"
}

// Validate checks one run/resume event.
func (e RunEvent) Validate() error {
	if e.Schema != runEventsSchema || e.SchemaVersion != schemaVersion {
		return invalidMachineOutput("run event schema = %q v%d", e.Schema, e.SchemaVersion)
	}
	if e.Event == "" || e.Command == "" {
		return invalidMachineOutput("run event is missing event or command")
	}
	if !e.Terminal() {
		if e.Outcome != "" || e.Error != nil {
			return invalidMachineOutput("non-terminal run event contains terminal outcome fields")
		}
		return nil
	}
	if e.ExitCode == nil {
		return invalidMachineOutput("completion event is missing exit_code")
	}
	switch e.Outcome {
	case "completed":
		if e.Error != nil {
			return invalidMachineOutput("completed event contains error")
		}
	case "failed", "canceled":
		if e.Error == nil {
			return invalidMachineOutput("%s completion event is missing error", e.Outcome)
		}
		if err := e.Error.Validate(); err != nil {
			return err
		}
		if e.Error.ExitCode != *e.ExitCode {
			return invalidMachineOutput("completion exit_code does not match error exit_code")
		}
	case "":
		return invalidMachineOutput("completion event is missing outcome")
	default:
		return invalidMachineOutput("completion event outcome = %q", e.Outcome)
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
		if i != len(events)-1 {
			return RunEvent{}, invalidMachineOutput("run event stream continues after completion")
		}
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
	Retry     string `json:"retry"`
	Retryable bool   `json:"retryable"`
	Scope     string `json:"scope"`
	ExitCode  int    `json:"exit_code"`
	Source    string `json:"source"`
}

// Validate checks the stable machine error classification.
func (e MachineErrorBody) Validate() error {
	if e.Code == "" || e.Scope == "" || e.Retry == "" {
		return invalidMachineOutput("machine error is missing code, scope, or retry classification")
	}
	if e.Retry != "after_fix" && e.Retry != "transient" && e.Retry != "unknown" {
		return invalidMachineOutput("machine error retry = %q", e.Retry)
	}
	if e.Retryable != (e.Retry == "transient") {
		return invalidMachineOutput("machine error retryable does not match retry classification")
	}
	return nil
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
	if body.Source == "" {
		return body.Code + ": " + message
	}
	return fmt.Sprintf("%s: %s (source=%q)", body.Code, message, body.Source)
}

// Validate checks the shared error envelope.
func (e MachineErrorEnvelope) Validate() error {
	if e.Schema != errorSchema || e.SchemaVersion != schemaVersion {
		return invalidMachineOutput("error schema = %q v%d", e.Schema, e.SchemaVersion)
	}
	return e.Error.Validate()
}

func invalidMachineOutput(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidMachineOutput}, args...)...)
}

func invalidSporeRequest(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidSporeRequest}, args...)...)
}
