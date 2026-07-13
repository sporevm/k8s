package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const interactiveTestImage = "docker.io/library/node@sha256:6db9be2ebb4bafb687a078ef5ba1b1dd256e8004d246a31fd210b6b848ab6be2"

func TestRunnerRunReusesImmutableBootTemplate(t *testing.T) {
	client := &fakeSporeClient{hostInfo: validHostInfo()}
	runner := newInteractiveTestRunner(t, 1, client)
	req := RunRequest{
		RunID:   "node-version-0001",
		Image:   interactiveTestImage,
		Memory:  "512mb",
		Command: []string{"/bin/sh", "-lc", "node -v"},
	}

	first, err := runner.Run(context.Background(), req, normalPressure())
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if first.Template.ID == "" || first.Template.CacheHit {
		t.Fatalf("first template = %+v", first.Template)
	}
	second, err := runner.Run(context.Background(), req, normalPressure())
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if second.Template.ID != first.Template.ID || !second.Template.CacheHit {
		t.Fatalf("second template = %+v, want cached %q", second.Template, first.Template.ID)
	}
	if captures := client.runCaptureRequests(); len(captures) != 1 {
		t.Fatalf("capture requests = %d, want 1", len(captures))
	}
	runs := client.runFromRequests()
	if len(runs) != 2 {
		t.Fatalf("run-from requests = %d, want 2", len(runs))
	}
	if runs[0].GenerationPath != "" || runs[0].SporeDir != runs[1].SporeDir {
		t.Fatalf("run-from requests = %+v", runs)
	}
	if filepath.Base(runs[0].SporeDir) != "parent.spore" {
		t.Fatalf("run-from template dir = %q", runs[0].SporeDir)
	}
}

func TestRunnerRunTemplateKeyIncludesMemory(t *testing.T) {
	client := &fakeSporeClient{hostInfo: validHostInfo()}
	runner := newInteractiveTestRunner(t, 1, client)
	req := RunRequest{Image: interactiveTestImage, Memory: "512mb", Command: []string{"/bin/true"}}

	first, err := runner.Run(context.Background(), req, normalPressure())
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}
	req.Memory = "1024mb"
	second, err := runner.Run(context.Background(), req, normalPressure())
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if first.Template.ID == second.Template.ID || second.Template.CacheHit {
		t.Fatalf("templates = %+v and %+v", first.Template, second.Template)
	}
	if captures := client.runCaptureRequests(); len(captures) != 2 {
		t.Fatalf("capture requests = %d, want 2", len(captures))
	}
}

func TestRunnerRunDefaultsInteractiveMemory(t *testing.T) {
	client := &fakeSporeClient{hostInfo: validHostInfo()}
	runner := newInteractiveTestRunner(t, 1, client)
	req := RunRequest{Image: interactiveTestImage, Command: []string{"/bin/true"}}

	first, err := runner.Run(context.Background(), req, normalPressure())
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}
	req.Memory = interactiveDefaultMemory
	second, err := runner.Run(context.Background(), req, normalPressure())
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if !second.Template.CacheHit || second.Template.ID != first.Template.ID {
		t.Fatalf("templates = %+v and %+v", first.Template, second.Template)
	}
	captures := client.runCaptureRequests()
	if len(captures) != 1 || captures[0].Memory != interactiveDefaultMemory {
		t.Fatalf("capture requests = %+v", captures)
	}
}

func TestRunnerRunRejectsMutableImage(t *testing.T) {
	client := &fakeSporeClient{hostInfo: validHostInfo()}
	runner := newInteractiveTestRunner(t, 1, client)
	_, err := runner.Run(context.Background(), RunRequest{
		Image:   "docker.io/library/node:22-bookworm-slim",
		Command: []string{"/bin/true"},
	}, normalPressure())
	if !errors.Is(err, ErrMutableImage) {
		t.Fatalf("Run error = %v, want ErrMutableImage", err)
	}
	if captures := client.runCaptureRequests(); len(captures) != 0 {
		t.Fatalf("capture requests = %d, want 0", len(captures))
	}
}

func TestRunnerRunDoesNotCacheIncompleteTemplate(t *testing.T) {
	captures := 0
	client := &fakeSporeClient{
		hostInfo: validHostInfo(),
		runFunc: func(_ context.Context, req RunCaptureRequest) ([]RunEvent, error) {
			captures++
			if captures > 1 {
				if err := os.MkdirAll(req.CaptureDir, 0o755); err != nil {
					return nil, err
				}
				if err := os.WriteFile(filepath.Join(req.CaptureDir, "manifest.json"), []byte("{}\n"), 0o644); err != nil {
					return nil, err
				}
			}
			return []RunEvent{captureExitEvent(req.CaptureDir)}, nil
		},
	}
	runner := newInteractiveTestRunner(t, 1, client)
	req := RunRequest{Image: interactiveTestImage, Command: []string{"/bin/true"}}

	if _, err := runner.Run(context.Background(), req, normalPressure()); err == nil {
		t.Fatal("first Run succeeded with incomplete template")
	}
	response, err := runner.Run(context.Background(), req, normalPressure())
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if response.Template.CacheHit {
		t.Fatalf("template = %+v, want rebuilt miss", response.Template)
	}
	if captures != 2 {
		t.Fatalf("captures = %d, want 2", captures)
	}
}

func TestRunnerRunReleasesCapturedTemplateOnPublicationFailure(t *testing.T) {
	client := &fakeSporeClient{
		hostInfo: validHostInfo(),
		runFunc: func(_ context.Context, req RunCaptureRequest) ([]RunEvent, error) {
			if err := os.MkdirAll(req.CaptureDir, 0o755); err != nil {
				return nil, err
			}
			if err := os.WriteFile(filepath.Join(req.CaptureDir, "manifest.json"), []byte("{}\n"), 0o644); err != nil {
				return nil, err
			}
			return []RunEvent{exitEvent(0)}, nil
		},
	}
	runner := newInteractiveTestRunner(t, 1, client)

	_, err := runner.Run(context.Background(), RunRequest{
		Image:   interactiveTestImage,
		Command: []string{"/bin/true"},
	}, normalPressure())
	if err == nil {
		t.Fatal("Run succeeded without a captured terminal event")
	}
	removes := client.removeSavedSporeRequests()
	if len(removes) != 1 || filepath.Base(removes[0].SporeDir) != "parent.spore" {
		t.Fatalf("saved spore removals = %+v", removes)
	}
	entries, readErr := os.ReadDir(filepath.Join(runner.workRoot, "templates"))
	if readErr != nil {
		t.Fatalf("read template root: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("temporary template entries = %d, want 0", len(entries))
	}
}

func TestRunnerRunKeepsCapturedTemplateWhenDurableRemovalFails(t *testing.T) {
	client := &fakeSporeClient{
		hostInfo: validHostInfo(),
		runFunc: func(_ context.Context, req RunCaptureRequest) ([]RunEvent, error) {
			if err := os.MkdirAll(req.CaptureDir, 0o755); err != nil {
				return nil, err
			}
			if err := os.WriteFile(filepath.Join(req.CaptureDir, "manifest.json"), []byte("{}\n"), 0o644); err != nil {
				return nil, err
			}
			return []RunEvent{exitEvent(0)}, nil
		},
		removeSavedFunc: func(context.Context, RemoveSavedSporeRequest) error {
			return errors.New("cache lock unavailable")
		},
	}
	runner := newInteractiveTestRunner(t, 1, client)

	_, err := runner.Run(context.Background(), RunRequest{
		Image:   interactiveTestImage,
		Command: []string{"/bin/true"},
	}, normalPressure())
	if err == nil {
		t.Fatal("Run succeeded without a captured terminal event")
	}
	entries, readErr := os.ReadDir(filepath.Join(runner.workRoot, "templates"))
	if readErr != nil {
		t.Fatalf("read template root: %v", readErr)
	}
	if len(entries) != 1 || !strings.HasPrefix(entries[0].Name(), ".build-") {
		t.Fatalf("temporary template entries = %+v, want one recoverable build", entries)
	}
}

func TestRunnerSandboxUsesTemplateAndOwnsSlot(t *testing.T) {
	client := &fakeSporeClient{hostInfo: validHostInfo()}
	runner := newInteractiveTestRunner(t, 1, client)
	req := SandboxCreateRequest{Name: "node-sandbox", Image: interactiveTestImage, Memory: "512mb"}

	created, err := runner.CreateSandbox(context.Background(), req, normalPressure())
	if err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	if created.Name != req.Name || created.Template.ID == "" {
		t.Fatalf("created = %+v", created)
	}
	if runner.AvailableSlots() != 0 {
		t.Fatalf("available slots = %d, want 0", runner.AvailableSlots())
	}
	restores := client.restoreNamedRequests()
	if len(restores) != 1 || restores[0].Name != req.Name || restores[0].GenerationPath != "" {
		t.Fatalf("named restore requests = %+v", restores)
	}
	if created.Timings.RestorePrepare != 3 || created.Timings.RestoreSpawnMonitor != 4 ||
		created.Timings.RestoreWaitExecReady != 17 || created.Timings.RestoreSporeTotal != 24 {
		t.Fatalf("restore timings = %+v", created.Timings)
	}
	if execs := client.execRequests(); len(execs) != 0 {
		t.Fatalf("sandbox readiness execs = %+v, want none", execs)
	}
	if _, err := runner.ExecSandbox(context.Background(), ExecRequest{
		Name:    req.Name,
		Command: []string{"/bin/sh", "-lc", "echo state"},
	}); err != nil {
		t.Fatalf("ExecSandbox: %v", err)
	}
	if execs := client.execRequests(); len(execs) != 1 {
		t.Fatalf("sandbox exec requests = %d, want 1", len(execs))
	}
	if err := runner.RemoveSandbox(context.Background(), RemoveVMRequest{Name: req.Name}); err != nil {
		t.Fatalf("RemoveSandbox: %v", err)
	}
	if runner.AvailableSlots() != 1 {
		t.Fatalf("available slots = %d, want 1", runner.AvailableSlots())
	}
}

func TestRunnerSandboxCleanupFailureKeepsSlotUntilDelete(t *testing.T) {
	cleanupCalls := 0
	client := &fakeSporeClient{
		hostInfo: validHostInfo(),
		restoreNamedFunc: func(context.Context, RestoreNamedRequest) (NamedLifecycleResult, error) {
			return NamedLifecycleResult{}, errors.New("restore interrupted")
		},
		removeFunc: func(context.Context, RemoveVMRequest) error {
			cleanupCalls++
			if cleanupCalls == 1 {
				return errors.New("cleanup interrupted")
			}
			return nil
		},
	}
	runner := newInteractiveTestRunner(t, 1, client)
	req := SandboxCreateRequest{Name: "node-sandbox", Image: interactiveTestImage}

	if _, err := runner.CreateSandbox(context.Background(), req, normalPressure()); err == nil {
		t.Fatal("CreateSandbox succeeded")
	}
	if runner.AvailableSlots() != 0 {
		t.Fatalf("available slots = %d, want 0 after failed cleanup", runner.AvailableSlots())
	}
	if err := runner.RemoveSandbox(context.Background(), RemoveVMRequest{Name: req.Name}); err != nil {
		t.Fatalf("RemoveSandbox retry: %v", err)
	}
	if runner.AvailableSlots() != 1 {
		t.Fatalf("available slots = %d, want 1 after delete", runner.AvailableSlots())
	}
}

func TestRunnerRemoveSandboxReconcilesUnknownName(t *testing.T) {
	client := &fakeSporeClient{hostInfo: validHostInfo()}
	runner := newInteractiveTestRunner(t, 1, client)

	if err := runner.RemoveSandbox(context.Background(), RemoveVMRequest{Name: "leftover-sandbox"}); err != nil {
		t.Fatalf("RemoveSandbox: %v", err)
	}
	removes := client.removeRequests()
	if len(removes) != 1 || removes[0].Name != "leftover-sandbox" {
		t.Fatalf("remove requests = %+v", removes)
	}
	if runner.AvailableSlots() != 1 {
		t.Fatalf("available slots = %d, want 1", runner.AvailableSlots())
	}
}

func newInteractiveTestRunner(t *testing.T, slots int, client SporeClient) *Runner {
	t.Helper()
	runner, err := NewRunner(slots, WithSporeClient(client), WithWorkRoot(t.TempDir()))
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	return runner
}
