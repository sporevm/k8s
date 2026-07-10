package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sporevm/k8s/internal/fleet"
)

const bootTemplateSchema = "sporevm-k8s.boot-template.v1"

const sandboxCleanupTimeout = 30 * time.Second

var (
	// ErrMutableImage means an interactive request did not select immutable image content.
	ErrMutableImage = errors.New("interactive image must be digest-pinned")
	// ErrSandboxExists means a named sandbox is already owned by this agent.
	ErrSandboxExists = errors.New("sandbox already exists")
	// ErrSandboxBusy means sandbox creation or deletion is already in progress.
	ErrSandboxBusy = errors.New("sandbox lifecycle operation in progress")
)

// RunRequest describes one ephemeral command execution from an immutable image.
type RunRequest struct {
	RunID   string   `json:"runID,omitempty"`
	Image   string   `json:"image"`
	Memory  string   `json:"memory,omitempty"`
	Command []string `json:"command"`
}

func (r RunRequest) validate() error {
	if err := validatePinnedImage(r.Image); err != nil {
		return err
	}
	return validateCommand("run", r.Command)
}

// SandboxCreateRequest describes one persistent named sandbox.
type SandboxCreateRequest struct {
	Name   string `json:"name"`
	Image  string `json:"image"`
	Memory string `json:"memory,omitempty"`
}

func (r SandboxCreateRequest) validate() error {
	if r.Name == "" {
		return invalidSporeRequest("sandbox name is required")
	}
	return validatePinnedImage(r.Image)
}

// TemplateStatus identifies the immutable boot template used by a request.
type TemplateStatus struct {
	ID       string `json:"id"`
	CacheHit bool   `json:"cacheHit"`
}

// RunTimings reports the node-local work in one ephemeral run.
type RunTimings struct {
	Template  float64 `json:"templateMs"`
	Execution float64 `json:"executionMs"`
	Total     float64 `json:"totalMs"`
}

// RunResponse is the result of one ephemeral run.
type RunResponse struct {
	RunID    string         `json:"runID,omitempty"`
	Template TemplateStatus `json:"template"`
	Timings  RunTimings     `json:"timingsMs"`
	Events   []RunEvent     `json:"events"`
}

// SandboxTimings reports the node-local work in sandbox creation.
type SandboxTimings struct {
	Template float64 `json:"templateMs"`
	Restore  float64 `json:"restoreMs"`
	Ready    float64 `json:"readyMs"`
	Total    float64 `json:"totalMs"`
}

// SandboxCreateResponse reports the template used to create a sandbox.
type SandboxCreateResponse struct {
	Name     string         `json:"name"`
	Template TemplateStatus `json:"template"`
	Timings  SandboxTimings `json:"timingsMs"`
}

type bootTemplateRuntimeIdentity struct {
	Version   string
	HostClass fleet.HostClass
}

type bootTemplateKey struct {
	Schema    string          `json:"schema"`
	Image     string          `json:"image"`
	Memory    string          `json:"memory,omitempty"`
	Backend   string          `json:"backend"`
	Version   string          `json:"sporeVersion"`
	HostClass fleet.HostClass `json:"hostClass"`
}

// Run executes one command in a fresh child restored from an automatic boot template.
func (r *Runner) Run(ctx context.Context, req RunRequest, pressure fleet.Pressure) (RunResponse, error) {
	started := r.now()
	response := RunResponse{RunID: req.RunID}
	if err := req.validate(); err != nil {
		return response, err
	}
	release, err := r.admitOne(pressure)
	if err != nil {
		return response, err
	}
	defer release()

	templateStarted := r.now()
	template, templateDir, err := r.ensureBootTemplate(ctx, req.Image, req.Memory)
	response.Timings.Template = elapsedMS(templateStarted, r.now())
	if err != nil {
		response.Timings.Total = elapsedMS(started, r.now())
		return response, err
	}
	response.Template = template

	executionStarted := r.now()
	response.Events, err = r.client.RunFrom(ctx, RunFromRequest{
		SporeDir: templateDir,
		Backend:  r.backend,
		Command:  req.Command,
	})
	response.Timings.Execution = elapsedMS(executionStarted, r.now())
	response.Timings.Total = elapsedMS(started, r.now())
	if err != nil {
		return response, err
	}
	if _, err := TerminalEvent(response.Events); err != nil {
		return response, err
	}
	return response, nil
}

// CreateSandbox restores a persistent named child from an automatic boot template.
func (r *Runner) CreateSandbox(ctx context.Context, req SandboxCreateRequest, pressure fleet.Pressure) (SandboxCreateResponse, error) {
	started := r.now()
	response := SandboxCreateResponse{Name: req.Name}
	if err := req.validate(); err != nil {
		return response, err
	}
	release, err := r.admitOne(pressure)
	if err != nil {
		return response, err
	}
	if err := r.reserveSandbox(req.Name); err != nil {
		release()
		return response, err
	}
	keepReservation := false
	defer func() {
		if !keepReservation {
			r.clearSandboxReservation(req.Name)
			release()
		}
	}()

	templateStarted := r.now()
	template, templateDir, err := r.ensureBootTemplate(ctx, req.Image, req.Memory)
	response.Timings.Template = elapsedMS(templateStarted, r.now())
	if err != nil {
		response.Timings.Total = elapsedMS(started, r.now())
		return response, err
	}
	response.Template = template

	restoreStarted := r.now()
	events, err := r.client.Resume(ctx, ResumeRequest{
		SporeDir: templateDir,
		Backend:  r.backend,
		Name:     req.Name,
	})
	response.Timings.Restore = elapsedMS(restoreStarted, r.now())
	response.Timings.Total = elapsedMS(started, r.now())
	if err != nil {
		return response, r.failSandboxCreation(ctx, req.Name, release, &keepReservation, err)
	}
	terminal, err := TerminalEvent(events)
	if err != nil {
		return response, r.failSandboxCreation(ctx, req.Name, release, &keepReservation, err)
	}
	if terminal.Event != "exit" || terminal.ExitCode == nil || *terminal.ExitCode != 0 {
		return response, r.failSandboxCreation(ctx, req.Name, release, &keepReservation,
			invalidMachineOutput("sandbox restore did not exit successfully"))
	}
	readyStarted := r.now()
	readyEvents, err := r.client.Exec(ctx, ExecRequest{Name: req.Name, Command: []string{"/bin/true"}})
	response.Timings.Ready = elapsedMS(readyStarted, r.now())
	response.Timings.Total = elapsedMS(started, r.now())
	if err != nil {
		return response, r.failSandboxCreation(ctx, req.Name, release, &keepReservation, err)
	}
	readyTerminal, err := TerminalEvent(readyEvents)
	if err != nil || readyTerminal.Event != "exit" || readyTerminal.ExitCode == nil || *readyTerminal.ExitCode != 0 {
		if err != nil {
			return response, r.failSandboxCreation(ctx, req.Name, release, &keepReservation, err)
		}
		return response, r.failSandboxCreation(ctx, req.Name, release, &keepReservation,
			invalidMachineOutput("sandbox readiness command did not exit successfully"))
	}

	r.activateSandbox(req.Name, release)
	keepReservation = true
	return response, nil
}

// ExecSandbox runs one command in a persistent named sandbox.
func (r *Runner) ExecSandbox(ctx context.Context, req ExecRequest) ([]RunEvent, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	if err := r.sandboxReady(req.Name); err != nil {
		return nil, err
	}
	return r.client.Exec(ctx, req)
}

// RemoveSandbox deletes a persistent sandbox and releases its execution slot.
func (r *Runner) RemoveSandbox(ctx context.Context, req RemoveVMRequest) error {
	if err := req.validate(); err != nil {
		return err
	}
	release, tracked, err := r.sandboxRelease(req.Name)
	if err != nil {
		return err
	}
	if err := r.client.RemoveVM(ctx, req); err != nil {
		if tracked {
			r.activateSandbox(req.Name, release)
		}
		return err
	}
	if !tracked {
		return nil
	}
	r.sandboxMu.Lock()
	delete(r.sandboxes, req.Name)
	r.sandboxMu.Unlock()
	release()
	return nil
}

func (r *Runner) ensureBootTemplate(ctx context.Context, image string, memory string) (TemplateStatus, string, error) {
	// ponytail: one lock keeps publication atomic; use per-key locks only if parallel cold builds become necessary.
	r.templateMu.Lock()
	defer r.templateMu.Unlock()

	identity, err := r.bootTemplateIdentity(ctx)
	if err != nil {
		return TemplateStatus{}, "", err
	}
	key := bootTemplateKey{
		Schema:    bootTemplateSchema,
		Image:     image,
		Memory:    memory,
		Backend:   r.backend,
		Version:   identity.Version,
		HostClass: identity.HostClass,
	}
	payload, err := json.Marshal(key)
	if err != nil {
		return TemplateStatus{}, "", err
	}
	digest := sha256.Sum256(payload)
	id := "sha256:" + hex.EncodeToString(digest[:])
	templateRoot := filepath.Join(r.workRoot, "templates")
	entryDir := filepath.Join(templateRoot, hex.EncodeToString(digest[:]))
	templateDir := filepath.Join(entryDir, "parent.spore")
	if bootTemplateReady(templateDir) {
		return TemplateStatus{ID: id, CacheHit: true}, templateDir, nil
	}
	if err := os.RemoveAll(entryDir); err != nil {
		return TemplateStatus{}, "", err
	}
	if err := os.MkdirAll(templateRoot, 0o755); err != nil {
		return TemplateStatus{}, "", err
	}
	tmpDir, err := os.MkdirTemp(templateRoot, ".build-")
	if err != nil {
		return TemplateStatus{}, "", err
	}
	defer os.RemoveAll(tmpDir)
	tmpTemplateDir := filepath.Join(tmpDir, "parent.spore")
	events, err := r.client.RunCapture(ctx, RunCaptureRequest{
		Image:      image,
		CaptureDir: tmpTemplateDir,
		Backend:    r.backend,
		Memory:     memory,
		Command:    []string{"/bin/true"},
	})
	if err != nil {
		return TemplateStatus{}, "", err
	}
	terminal, err := TerminalEvent(events)
	if err != nil {
		return TemplateStatus{}, "", err
	}
	if terminal.Event != "exit" || terminal.ExitCode == nil || *terminal.ExitCode != 0 || !terminal.Captured {
		return TemplateStatus{}, "", invalidMachineOutput("boot template capture did not exit successfully")
	}
	if !bootTemplateReady(tmpTemplateDir) {
		return TemplateStatus{}, "", invalidMachineOutput("boot template capture did not produce manifest.json")
	}
	metadata, err := json.MarshalIndent(key, "", "  ")
	if err != nil {
		return TemplateStatus{}, "", err
	}
	metadata = append(metadata, '\n')
	if err := os.WriteFile(filepath.Join(tmpDir, "template.json"), metadata, 0o644); err != nil {
		return TemplateStatus{}, "", err
	}
	if err := os.Rename(tmpDir, entryDir); err != nil {
		return TemplateStatus{}, "", err
	}
	return TemplateStatus{ID: id, CacheHit: false}, templateDir, nil
}

func (r *Runner) bootTemplateIdentity(ctx context.Context) (bootTemplateRuntimeIdentity, error) {
	if r.templateIdentity != nil {
		return *r.templateIdentity, nil
	}
	if r.client == nil {
		return bootTemplateRuntimeIdentity{}, ErrSporeClientNotConfigured
	}
	if r.workRoot == "" {
		return bootTemplateRuntimeIdentity{}, ErrWorkRootNotConfigured
	}
	version, err := r.client.Version(ctx)
	if err != nil {
		return bootTemplateRuntimeIdentity{}, err
	}
	info, err := r.client.HostInfo(ctx)
	if err != nil {
		return bootTemplateRuntimeIdentity{}, err
	}
	hostClass, err := info.FleetHostClass(r.backend)
	if err != nil {
		return bootTemplateRuntimeIdentity{}, err
	}
	identity := bootTemplateRuntimeIdentity{Version: version, HostClass: hostClass}
	r.templateIdentity = &identity
	return identity, nil
}

func (r *Runner) admitOne(pressure fleet.Pressure) (func(), error) {
	if err := pressure.Validate("pressure"); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidPressure, err)
	}
	if pressure.Critical() {
		return nil, ErrUnsafePressure
	}
	release, ok := r.slots.TryAcquire(1)
	if !ok {
		return nil, ErrOversubscribed
	}
	return release, nil
}

func (r *Runner) reserveSandbox(name string) error {
	r.sandboxMu.Lock()
	defer r.sandboxMu.Unlock()
	if _, ok := r.sandboxes[name]; ok {
		return fmt.Errorf("%w: %s", ErrSandboxExists, name)
	}
	r.sandboxes[name] = nil
	return nil
}

func (r *Runner) clearSandboxReservation(name string) {
	r.sandboxMu.Lock()
	defer r.sandboxMu.Unlock()
	delete(r.sandboxes, name)
}

func (r *Runner) activateSandbox(name string, release func()) {
	r.sandboxMu.Lock()
	defer r.sandboxMu.Unlock()
	r.sandboxes[name] = release
}

func (r *Runner) sandboxReady(name string) error {
	r.sandboxMu.Lock()
	defer r.sandboxMu.Unlock()
	release, ok := r.sandboxes[name]
	if !ok {
		return invalidSporeRequest("sandbox %q is not owned by this agent", name)
	}
	if release == nil {
		return fmt.Errorf("%w: %s", ErrSandboxBusy, name)
	}
	return nil
}

func (r *Runner) sandboxRelease(name string) (func(), bool, error) {
	r.sandboxMu.Lock()
	defer r.sandboxMu.Unlock()
	release, ok := r.sandboxes[name]
	if !ok {
		return nil, false, nil
	}
	if release == nil {
		return nil, true, fmt.Errorf("%w: %s", ErrSandboxBusy, name)
	}
	r.sandboxes[name] = nil
	return release, true, nil
}

func (r *Runner) failSandboxCreation(ctx context.Context, name string, release func(), keepReservation *bool, cause error) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sandboxCleanupTimeout)
	defer cancel()
	if cleanupErr := r.client.RemoveVM(cleanupCtx, RemoveVMRequest{Name: name}); cleanupErr != nil {
		r.activateSandbox(name, release)
		*keepReservation = true
		return errors.Join(cause, fmt.Errorf("cleanup sandbox %q: %w", name, cleanupErr))
	}
	return cause
}

func bootTemplateReady(path string) bool {
	info, err := os.Stat(filepath.Join(path, "manifest.json"))
	return err == nil && !info.IsDir()
}

func validatePinnedImage(image string) error {
	prefix, digest, ok := strings.Cut(image, "@sha256:")
	if !ok || prefix == "" || strings.Contains(prefix, "@") || len(digest) != sha256.Size*2 || strings.Contains(digest, "@") {
		return fmt.Errorf("%w: use registry/repository@sha256:<digest>", ErrMutableImage)
	}
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != sha256.Size {
		return fmt.Errorf("%w: use registry/repository@sha256:<digest>", ErrMutableImage)
	}
	return nil
}

func validateCommand(name string, command []string) error {
	if len(command) == 0 {
		return invalidSporeRequest("%s command is required", name)
	}
	for i, arg := range command {
		if arg == "" {
			return invalidSporeRequest("%s command[%d] must not be empty", name, i)
		}
	}
	return nil
}
