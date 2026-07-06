package agent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/sporevm/k8s/internal/fleet"
)

func TestSlotLimiterRefusesOversubscription(t *testing.T) {
	limiter, err := NewSlotLimiter(100)
	if err != nil {
		t.Fatalf("NewSlotLimiter: %v", err)
	}

	release, ok := limiter.TryAcquire(100)
	if !ok {
		t.Fatal("TryAcquire(100) failed")
	}
	if _, ok := limiter.TryAcquire(1); ok {
		t.Fatal("TryAcquire(1) succeeded while full")
	}

	release()
	release()
	if limiter.Available() != 100 {
		t.Fatalf("available slots = %d, want 100", limiter.Available())
	}
}

func TestRunnerAdmitShardReservesAndReleasesSlots(t *testing.T) {
	runner, err := NewRunner(100)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	release, err := runner.AdmitShard(fleet.ShardLease{ChildCount: 75}, fleet.Pressure{
		Disk:   fleet.PressureNormal,
		Memory: fleet.PressureWarning,
	})
	if err != nil {
		t.Fatalf("AdmitShard: %v", err)
	}
	if runner.AvailableSlots() != 25 {
		t.Fatalf("available slots = %d, want 25", runner.AvailableSlots())
	}

	release()
	if runner.AvailableSlots() != 100 {
		t.Fatalf("available slots = %d, want 100", runner.AvailableSlots())
	}
}

func TestRunnerAdmitShardRefusesOversubscription(t *testing.T) {
	runner, err := NewRunner(50)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	_, err = runner.AdmitShard(fleet.ShardLease{ChildCount: 100}, fleet.Pressure{
		Disk:   fleet.PressureNormal,
		Memory: fleet.PressureNormal,
	})
	if !errors.Is(err, ErrOversubscribed) {
		t.Fatalf("AdmitShard error = %v, want ErrOversubscribed", err)
	}
}

func TestRunnerAdmitShardRejectsEmptyLease(t *testing.T) {
	runner, err := NewRunner(50)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	_, err = runner.AdmitShard(fleet.ShardLease{}, fleet.Pressure{
		Disk:   fleet.PressureNormal,
		Memory: fleet.PressureNormal,
	})
	if !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("AdmitShard error = %v, want ErrInvalidLease", err)
	}
}

func TestRunnerAdmitShardRejectsMissingPressure(t *testing.T) {
	runner, err := NewRunner(50)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	_, err = runner.AdmitShard(fleet.ShardLease{ChildCount: 1}, fleet.Pressure{})
	if !errors.Is(err, ErrInvalidPressure) {
		t.Fatalf("AdmitShard error = %v, want ErrInvalidPressure", err)
	}
	if runner.AvailableSlots() != 50 {
		t.Fatalf("available slots = %d, want 50", runner.AvailableSlots())
	}
}

func TestRunnerAdmitShardRefusesCriticalPressure(t *testing.T) {
	runner, err := NewRunner(100)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	_, err = runner.AdmitShard(fleet.ShardLease{ChildCount: 1}, fleet.Pressure{
		Disk:   fleet.PressureCritical,
		Memory: fleet.PressureNormal,
	})
	if !errors.Is(err, ErrUnsafePressure) {
		t.Fatalf("AdmitShard error = %v, want ErrUnsafePressure", err)
	}
	if runner.AvailableSlots() != 100 {
		t.Fatalf("available slots = %d, want 100", runner.AvailableSlots())
	}
}

func TestRunnerStatusReportsHostClassAndSlots(t *testing.T) {
	client := &fakeSporeClient{hostInfo: validHostInfo()}
	runner, err := NewRunner(100, WithSporeClient(client))
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	status, err := runner.Status(context.Background(), StatusRequest{
		AgentID: "spore-agent-us-east-1a-0001",
		CellID:  "cell-us-east-1a",
		Pressure: fleet.Pressure{
			Disk:   fleet.PressureNormal,
			Memory: fleet.PressureWarning,
		},
		ObservedAt: time.Date(2026, 6, 20, 4, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.HostClass.ID != "linux-aarch64-kvm-graviton1-v0" {
		t.Fatalf("host class = %q", status.HostClass.ID)
	}
	if status.ExecutionSlots.Total != 100 || status.ExecutionSlots.Available != 100 {
		t.Fatalf("slots = %+v", status.ExecutionSlots)
	}
	if !status.Healthy {
		t.Fatal("status should be healthy")
	}
}

func TestRunnerPrepareBundleCapturesForksPacksAndInspects(t *testing.T) {
	source := testRun()
	source.Prepare.Memory = "512mb"
	workRoot := t.TempDir()
	client := &fakeSporeClient{
		hostInfo: validHostInfo(),
		inspectFunc: func(_ context.Context, req InspectBundleRequest) (InspectBundleResult, error) {
			if req.ChildRange == nil || req.ChildRange.Start != 0 || req.ChildRange.End != 1000 {
				t.Fatalf("inspect child range = %+v", req.ChildRange)
			}
			wantURI, err := fileURI(filepath.Join(workRoot, source.RunID, "prepare", "bundle"))
			if err != nil {
				t.Fatalf("fileURI: %v", err)
			}
			if req.Source != wantURI {
				t.Fatalf("inspect source = %q, want %q", req.Source, wantURI)
			}
			return InspectBundleResult{
				Schema:        inspectBundleSchema,
				SchemaVersion: schemaVersion,
				BundleDigest: DigestRef{
					Algorithm: "sha256",
					Hex:       "2222222222222222222222222222222222222222222222222222222222222222",
				},
				ChildCount: 1000,
			}, nil
		},
	}
	runner, err := NewRunner(100,
		WithSporeClient(client),
		WithWorkRoot(workRoot),
	)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	prepared, err := runner.PrepareBundle(context.Background(), PrepareBundleRequest{Run: source})
	if err != nil {
		t.Fatalf("PrepareBundle: %v", err)
	}
	if prepared.ChildCount != 1000 {
		t.Fatalf("child count = %d", prepared.ChildCount)
	}
	if prepared.Bundle.Digest != "sha256:2222222222222222222222222222222222222222222222222222222222222222" {
		t.Fatalf("bundle digest = %q", prepared.Bundle.Digest)
	}
	if _, err := source.Compile(prepared); err != nil {
		t.Fatalf("Compile prepared bundle: %v", err)
	}

	runCaptures := client.runCaptureRequests()
	if len(runCaptures) != 1 {
		t.Fatalf("run capture count = %d", len(runCaptures))
	}
	capture := runCaptures[0]
	wantParent := filepath.Join(workRoot, source.RunID, "prepare", "parent.spore")
	if capture.Image != source.Source.Image || capture.CaptureDir != wantParent || capture.CaptureSignal != "USR1" || capture.ReadyMarker != "SPOREVM_RAILS_READY" || capture.Memory != "512mb" {
		t.Fatalf("run capture request = %+v", capture)
	}
	if got, want := capture.Command, source.Prepare.Command; !equalStrings(got, want) {
		t.Fatalf("capture command = %v, want %v", got, want)
	}

	forks := client.forkRequests()
	if len(forks) != 1 || forks[0].ParentDir != wantParent || forks[0].Count != 1000 {
		t.Fatalf("fork requests = %+v", forks)
	}
	packs := client.packRequests()
	wantChildren := filepath.Join(workRoot, source.RunID, "prepare", "children")
	wantBundle := filepath.Join(workRoot, source.RunID, "prepare", "bundle")
	if len(packs) != 1 || packs[0].ParentDir != wantParent || packs[0].ChildrenDir != wantChildren || packs[0].OutDir != wantBundle {
		t.Fatalf("pack requests = %+v", packs)
	}
}

func TestRunnerRunChildSuccessCommitsTerminalAndAttempt(t *testing.T) {
	store := newTestResultStore(t)
	metrics := &recordingMetricsSink{}
	var generationPath string
	client := &fakeSporeClient{
		pullFunc: func(_ context.Context, req PullRequest) (PullResult, error) {
			if err := os.MkdirAll(req.OutDir, 0o755); err != nil {
				return PullResult{}, err
			}
			return pullResult(req.OutDir, "42"), nil
		},
		resumeFunc: func(_ context.Context, req ResumeRequest) ([]RunEvent, error) {
			if req.GenerationPath == "" {
				t.Fatal("resume generation path is empty")
			}
			generationPath = req.GenerationPath
			data, err := os.ReadFile(req.GenerationPath)
			if err != nil {
				t.Fatalf("read generation file: %v", err)
			}
			var generation generationPayload
			if err := json.Unmarshal(data, &generation); err != nil {
				t.Fatalf("decode generation file: %v", err)
			}
			if generation.RunID != "ruby-counter-20260620" ||
				generation.ChildID != 42 ||
				generation.ParallelIndex != 42 ||
				generation.ParallelCount != 100 ||
				generation.ForkIndex != 42 ||
				generation.ForkCount != 100 ||
				generation.ForkBatchID == "" ||
				generation.VMID == "" {
				t.Fatalf("generation = %+v", generation)
			}
			return []RunEvent{exitEvent(0)}, nil
		},
	}
	runner := newConfiguredRunnerWithMetrics(t, client, store, metrics)
	run := testBundleRun()
	lease := testLease(run)

	result, err := runner.RunChild(context.Background(), RunChildRequest{
		Run:      run,
		Lease:    lease,
		ChildID:  42,
		Attempt:  1,
		Pressure: normalPressure(),
	})
	if err != nil {
		t.Fatalf("RunChild: %v", err)
	}
	if result.Status != fleet.AttemptSucceeded || !result.Terminal {
		t.Fatalf("result = %+v", result)
	}
	if result.ResultURI != TerminalResultURI(run, 42) {
		t.Fatalf("result uri = %q", result.ResultURI)
	}
	if result.TimingsMS.ArtifactPull < 0 || result.TimingsMS.Resume < 0 {
		t.Fatalf("timings = %+v", result.TimingsMS)
	}
	if runner.AvailableSlots() != 100 {
		t.Fatalf("available slots = %d, want 100", runner.AvailableSlots())
	}

	terminal, ok, err := store.TerminalResult(context.Background(), run, 42)
	if err != nil {
		t.Fatalf("TerminalResult: %v", err)
	}
	if !ok || terminal.AttemptID != result.AttemptID {
		t.Fatalf("terminal = %+v ok=%v", terminal, ok)
	}
	if _, err := os.Stat(client.lastOutDir()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("work dir still exists or stat failed differently: %v", err)
	}
	if _, err := os.Stat(generationPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("generation file still exists or stat failed differently: %v", err)
	}

	pulls := client.pullRequests()
	if len(pulls) != 1 {
		t.Fatalf("pull count = %d", len(pulls))
	}
	if pulls[0].Source != run.Bundle.URI+"@"+run.Bundle.Digest {
		t.Fatalf("pull source = %q", pulls[0].Source)
	}
	if pulls[0].ChildID != "42" {
		t.Fatalf("pull child = %q", pulls[0].ChildID)
	}

	observed := metrics.all()
	if len(observed) != 1 {
		t.Fatalf("metrics count = %d", len(observed))
	}
	if observed[0].OriginBytesRead != 8192 || observed[0].ChunkCacheHits != 8 || observed[0].RootFSCacheMisses != 1 {
		t.Fatalf("metrics = %+v", observed[0])
	}
	if !observed[0].TerminalCommitAttempted || !observed[0].TerminalCommitSuccessful {
		t.Fatalf("terminal commit metrics = %+v", observed[0])
	}
}

func TestRunnerRunChildGuestFailureCommitsTerminalResult(t *testing.T) {
	store := newTestResultStore(t)
	client := &fakeSporeClient{
		pullFunc: func(_ context.Context, req PullRequest) (PullResult, error) {
			return pullResult(req.OutDir, "43"), nil
		},
		resumeFunc: func(_ context.Context, req ResumeRequest) ([]RunEvent, error) {
			return []RunEvent{exitEvent(7)}, nil
		},
	}
	runner := newConfiguredRunner(t, client, store)
	run := testBundleRun()

	result, err := runner.RunChild(context.Background(), RunChildRequest{
		Run:      run,
		Lease:    testLease(run),
		ChildID:  43,
		Attempt:  1,
		Pressure: normalPressure(),
	})
	if err != nil {
		t.Fatalf("RunChild: %v", err)
	}
	if result.Status != fleet.AttemptFailed || !result.Terminal {
		t.Fatalf("result = %+v", result)
	}
	if result.Error == nil || result.Error.Code != "runtime.execution_failed" {
		t.Fatalf("error = %+v", result.Error)
	}
	if _, ok, err := store.TerminalResult(context.Background(), run, 43); err != nil || !ok {
		t.Fatalf("terminal ok=%v err=%v", ok, err)
	}
}

func TestRunnerRunChildRecordsGuestOutput(t *testing.T) {
	store := newTestResultStore(t)
	client := &fakeSporeClient{
		pullFunc: func(_ context.Context, req PullRequest) (PullResult, error) {
			return pullResult(req.OutDir, "42"), nil
		},
		resumeFunc: func(_ context.Context, req ResumeRequest) ([]RunEvent, error) {
			return []RunEvent{
				outputEvent("stdout", "hello\n"),
				outputEvent("stderr", "warn\n"),
				exitEvent(0),
			}, nil
		},
	}
	runner := newConfiguredRunner(t, client, store)
	run := testBundleRun()

	result, err := runner.RunChild(context.Background(), RunChildRequest{
		Run:      run,
		Lease:    testLease(run),
		ChildID:  42,
		Attempt:  1,
		Pressure: normalPressure(),
	})
	if err != nil {
		t.Fatalf("RunChild: %v", err)
	}
	if result.Output == nil {
		t.Fatal("result output is nil")
	}
	if result.Output.StdoutBytes != 6 || result.Output.StderrBytes != 5 {
		t.Fatalf("output bytes = %+v", result.Output)
	}
	if result.Output.StdoutPreviewBase64 != base64.StdEncoding.EncodeToString([]byte("hello\n")) {
		t.Fatalf("stdout preview = %q", result.Output.StdoutPreviewBase64)
	}
	terminal, ok, err := store.TerminalResult(context.Background(), run, 42)
	if err != nil || !ok {
		t.Fatalf("TerminalResult ok=%v err=%v", ok, err)
	}
	if terminal.Output == nil || terminal.Output.StderrPreviewBase64 != base64.StdEncoding.EncodeToString([]byte("warn\n")) {
		t.Fatalf("terminal output = %+v", terminal.Output)
	}
}

func TestRunnerRunChildExecutesChildCommand(t *testing.T) {
	store := newTestResultStore(t)
	client := &fakeSporeClient{
		pullFunc: func(_ context.Context, req PullRequest) (PullResult, error) {
			return pullResult(req.OutDir, "42"), nil
		},
		resumeFunc: func(_ context.Context, req ResumeRequest) ([]RunEvent, error) {
			if req.Name == "" {
				t.Fatal("resume name is empty")
			}
			if req.GenerationPath == "" {
				t.Fatal("resume generation path is empty")
			}
			return []RunEvent{exitEvent(0)}, nil
		},
		execFunc: func(_ context.Context, req ExecRequest) ([]RunEvent, error) {
			if req.Name == "" {
				t.Fatal("exec name is empty")
			}
			return []RunEvent{
				outputEvent("stdout", "ok\n"),
				exitEvent(0),
			}, nil
		},
	}
	runner := newConfiguredRunner(t, client, store)
	run := testBundleRun()
	run.ChildCommand = []string{"/usr/local/bin/sporevm-rspec-shard", "--seed", "1"}

	result, err := runner.RunChild(context.Background(), RunChildRequest{
		Run:      run,
		Lease:    testLease(run),
		ChildID:  42,
		Attempt:  1,
		Pressure: normalPressure(),
	})
	if err != nil {
		t.Fatalf("RunChild: %v", err)
	}
	if result.Status != fleet.AttemptSucceeded || !result.Terminal {
		t.Fatalf("result = %+v", result)
	}
	if result.Output == nil || result.Output.StdoutBytes != 3 {
		t.Fatalf("output = %+v", result.Output)
	}

	resumes := client.resumeRequests()
	execs := client.execRequests()
	removes := client.removeRequests()
	if len(resumes) != 1 || len(execs) != 1 || len(removes) != 1 {
		t.Fatalf("resumes=%+v execs=%+v removes=%+v", resumes, execs, removes)
	}
	if resumes[0].Name == "" || execs[0].Name != resumes[0].Name || removes[0].Name != resumes[0].Name {
		t.Fatalf("names resumes=%+v execs=%+v removes=%+v", resumes, execs, removes)
	}
	if !equalStrings(execs[0].Command, run.ChildCommand) {
		t.Fatalf("exec command = %+v, want %+v", execs[0].Command, run.ChildCommand)
	}
}

func TestRunnerRunChildChildCommandResumeExitFailsBeforeExec(t *testing.T) {
	store := newTestResultStore(t)
	client := &fakeSporeClient{
		pullFunc: func(_ context.Context, req PullRequest) (PullResult, error) {
			return pullResult(req.OutDir, "42"), nil
		},
		resumeFunc: func(_ context.Context, req ResumeRequest) ([]RunEvent, error) {
			return []RunEvent{exitEvent(7)}, nil
		},
		execFunc: func(context.Context, ExecRequest) ([]RunEvent, error) {
			t.Fatal("exec called after failed named resume")
			return nil, nil
		},
	}
	runner := newConfiguredRunner(t, client, store)
	run := testBundleRun()
	run.ChildCommand = []string{"/usr/local/bin/sporevm-rspec-shard"}

	result, err := runner.RunChild(context.Background(), RunChildRequest{
		Run:      run,
		Lease:    testLease(run),
		ChildID:  42,
		Attempt:  1,
		Pressure: normalPressure(),
	})
	if err == nil {
		t.Fatal("RunChild succeeded")
	}
	if result.Status != fleet.AttemptFailed || result.Terminal {
		t.Fatalf("result = %+v", result)
	}
	if len(client.execRequests()) != 0 || len(client.removeRequests()) != 1 {
		t.Fatalf("execs=%+v removes=%+v", client.execRequests(), client.removeRequests())
	}
	if _, ok, err := store.TerminalResult(context.Background(), run, 42); err != nil || ok {
		t.Fatalf("terminal ok=%v err=%v", ok, err)
	}
}

func TestRunnerRunChildWritesAttemptAfterContextCancellation(t *testing.T) {
	store := newTestResultStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	client := &fakeSporeClient{
		pullFunc: func(_ context.Context, req PullRequest) (PullResult, error) {
			return pullResult(req.OutDir, "42"), nil
		},
		resumeFunc: func(ctx context.Context, _ ResumeRequest) ([]RunEvent, error) {
			cancel()
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	runner := newConfiguredRunner(t, client, store)
	run := testBundleRun()
	result, err := runner.RunChild(ctx, RunChildRequest{
		Run:      run,
		Lease:    testLease(run),
		ChildID:  42,
		Attempt:  1,
		Pressure: normalPressure(),
	})
	if err == nil {
		t.Fatal("RunChild succeeded")
	}
	if result.Status != fleet.AttemptFailed || result.AttemptID == "" {
		t.Fatalf("result = %+v", result)
	}
	path, err := store.pathForURI(AttemptResultURI(run, 42, result.AttemptID))
	if err != nil {
		t.Fatalf("attempt path: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("attempt result was not written after cancellation: %v", err)
	}
}

func TestRunnerRunChildResumeTimeoutWritesAttempt(t *testing.T) {
	store := newTestResultStore(t)
	client := &fakeSporeClient{
		pullFunc: func(_ context.Context, req PullRequest) (PullResult, error) {
			return pullResult(req.OutDir, "42"), nil
		},
		resumeFunc: func(ctx context.Context, _ ResumeRequest) ([]RunEvent, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	runner, err := NewRunner(100,
		WithSporeClient(client),
		WithResultStore(store),
		WithWorkRoot(t.TempDir()),
		WithChildTimeout(5*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	run := testBundleRun()
	result, err := runner.RunChild(context.Background(), RunChildRequest{
		Run:      run,
		Lease:    testLease(run),
		ChildID:  42,
		Attempt:  1,
		Pressure: normalPressure(),
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunChild error = %v, want context deadline exceeded", err)
	}
	if result.Error == nil || result.Error.Code != "agent.deadline_exceeded" {
		t.Fatalf("result error = %+v", result.Error)
	}
	path, err := store.pathForURI(AttemptResultURI(run, 42, result.AttemptID))
	if err != nil {
		t.Fatalf("attempt path: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("attempt result was not written after resume timeout: %v", err)
	}
}

func TestRunnerRunChildPlatformMismatchIsNonTerminalAttempt(t *testing.T) {
	store := newTestResultStore(t)
	client := &fakeSporeClient{
		pullFunc: func(_ context.Context, req PullRequest) (PullResult, error) {
			return pullResult(req.OutDir, "44"), nil
		},
		resumeFunc: func(_ context.Context, req ResumeRequest) ([]RunEvent, error) {
			body := MachineErrorBody{
				Code:      "host.unsupported",
				Message:   "host backend is unavailable",
				Retryable: false,
				Scope:     "host",
				ExitCode:  69,
				Source:    "UnsupportedHost",
			}
			return []RunEvent{failureEvent(body)}, &MachineError{
				Envelope: MachineErrorEnvelope{
					Schema:        errorSchema,
					SchemaVersion: schemaVersion,
					Error:         body,
				},
			}
		},
	}
	runner := newConfiguredRunner(t, client, store)
	run := testBundleRun()

	result, err := runner.RunChild(context.Background(), RunChildRequest{
		Run:      run,
		Lease:    testLease(run),
		ChildID:  44,
		Attempt:  1,
		Pressure: normalPressure(),
	})
	if err != nil {
		t.Fatalf("RunChild: %v", err)
	}
	if result.Status != fleet.AttemptPlatformMismatch || result.Terminal {
		t.Fatalf("result = %+v", result)
	}
	if _, ok, err := store.TerminalResult(context.Background(), run, 44); err != nil || ok {
		t.Fatalf("terminal ok=%v err=%v", ok, err)
	}
}

func TestRunnerRunChildPullHostErrorIsPlatformMismatch(t *testing.T) {
	store := newTestResultStore(t)
	client := &fakeSporeClient{
		pullFunc: func(context.Context, PullRequest) (PullResult, error) {
			return PullResult{}, machineError("host.unavailable", "host backend is unavailable")
		},
	}
	runner := newConfiguredRunner(t, client, store)
	run := testBundleRun()

	result, err := runner.RunChild(context.Background(), RunChildRequest{
		Run:      run,
		Lease:    testLease(run),
		ChildID:  47,
		Attempt:  1,
		Pressure: normalPressure(),
	})
	if err == nil {
		t.Fatal("RunChild succeeded")
	}
	if result.Status != fleet.AttemptPlatformMismatch || result.Terminal {
		t.Fatalf("result = %+v", result)
	}
	if _, ok, err := store.TerminalResult(context.Background(), run, 47); err != nil || ok {
		t.Fatalf("terminal ok=%v err=%v", ok, err)
	}
}

func TestRunnerRunChildSkipsExistingTerminalResult(t *testing.T) {
	store := newTestResultStore(t)
	client := &fakeSporeClient{
		pullFunc: func(_ context.Context, req PullRequest) (PullResult, error) {
			return pullResult(req.OutDir, "45"), nil
		},
		resumeFunc: func(_ context.Context, req ResumeRequest) ([]RunEvent, error) {
			return []RunEvent{exitEvent(0)}, nil
		},
	}
	runner := newConfiguredRunner(t, client, store)
	run := testBundleRun()
	lease := testLease(run)

	if _, err := runner.RunChild(context.Background(), RunChildRequest{
		Run:      run,
		Lease:    lease,
		ChildID:  45,
		Attempt:  1,
		Pressure: normalPressure(),
	}); err != nil {
		t.Fatalf("first RunChild: %v", err)
	}
	result, err := runner.RunChild(context.Background(), RunChildRequest{
		Run:      run,
		Lease:    lease,
		ChildID:  45,
		Attempt:  2,
		Pressure: normalPressure(),
	})
	if err != nil {
		t.Fatalf("second RunChild: %v", err)
	}
	if result.Status != fleet.AttemptSkippedTerminalExists || !result.Terminal {
		t.Fatalf("result = %+v", result)
	}
	if got := len(client.pullRequests()); got != 1 {
		t.Fatalf("pull count = %d, want 1", got)
	}
}

func TestRunnerRunChildCancellationCleansWorkDir(t *testing.T) {
	store := newTestResultStore(t)
	pullStarted := make(chan string, 1)
	client := &fakeSporeClient{
		pullFunc: func(ctx context.Context, req PullRequest) (PullResult, error) {
			if err := os.MkdirAll(req.OutDir, 0o755); err != nil {
				return PullResult{}, err
			}
			pullStarted <- req.OutDir
			<-ctx.Done()
			return PullResult{}, ctx.Err()
		},
	}
	runner := newConfiguredRunner(t, client, store)
	run := testBundleRun()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := runner.RunChild(ctx, RunChildRequest{
			Run:      run,
			Lease:    testLease(run),
			ChildID:  46,
			Attempt:  1,
			Pressure: normalPressure(),
		})
		done <- err
	}()

	outDir := <-pullStarted
	cancel()
	err := <-done
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("RunChild error = %v, want context.Canceled", err)
	}
	if _, statErr := os.Stat(outDir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("work dir still exists or stat failed differently: %v", statErr)
	}
}

func TestRunnerRunShardExecutesEveryChildAndReleasesSlots(t *testing.T) {
	store := newTestResultStore(t)
	client := &fakeSporeClient{
		pullFunc: func(_ context.Context, req PullRequest) (PullResult, error) {
			return pullResult(req.OutDir, req.ChildID), nil
		},
		resumeFunc: func(_ context.Context, req ResumeRequest) ([]RunEvent, error) {
			return []RunEvent{exitEvent(0)}, nil
		},
	}
	runner := newConfiguredRunner(t, client, store)
	run := testBundleRun()
	run.Children = fleet.ChildRange{Start: 10, Count: 3}
	run.Execution.ChildrenPerShard = 3
	run.Execution.MaxInFlightPerAgent = 3
	lease := testLease(run)
	lease.ChildStart = 10
	lease.ChildCount = 3

	results, err := runner.RunShard(context.Background(), RunShardRequest{
		Run:      run,
		Lease:    lease,
		Attempt:  1,
		Pressure: normalPressure(),
	})
	if err != nil {
		t.Fatalf("RunShard: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("results len = %d", len(results))
	}
	var children []int
	for _, result := range results {
		children = append(children, result.ChildID)
		if result.Status != fleet.AttemptSucceeded {
			t.Fatalf("result = %+v", result)
		}
	}
	sort.Ints(children)
	if got, want := children, []int{10, 11, 12}; !equalInts(got, want) {
		t.Fatalf("children = %v, want %v", got, want)
	}
	if runner.AvailableSlots() != 100 {
		t.Fatalf("available slots = %d, want 100", runner.AvailableSlots())
	}
}

func TestRunnerRunShardReservesOnlyInFlightSlots(t *testing.T) {
	store := newTestResultStore(t)
	client := &fakeSporeClient{
		pullFunc: func(_ context.Context, req PullRequest) (PullResult, error) {
			return pullResult(req.OutDir, req.ChildID), nil
		},
		resumeFunc: func(_ context.Context, req ResumeRequest) ([]RunEvent, error) {
			return []RunEvent{exitEvent(0)}, nil
		},
	}
	runner, err := NewRunner(
		2,
		WithSporeClient(client),
		WithResultStore(store),
		WithWorkRoot(t.TempDir()),
	)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	run := testBundleRun()
	run.Children = fleet.ChildRange{Start: 10, Count: 5}
	run.Execution.ChildrenPerShard = 2
	run.Execution.MaxInFlightPerAgent = 2
	lease := testLease(run)
	lease.ChildStart = 10
	lease.ChildCount = 5

	results, err := runner.RunShard(context.Background(), RunShardRequest{
		Run:      run,
		Lease:    lease,
		Attempt:  1,
		Pressure: normalPressure(),
	})
	if err != nil {
		t.Fatalf("RunShard: %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("results len = %d, want 5", len(results))
	}
	if got := len(client.pullRequests()); got != 5 {
		t.Fatalf("pull requests = %d, want 5", got)
	}
	if runner.AvailableSlots() != 2 {
		t.Fatalf("available slots = %d, want 2", runner.AvailableSlots())
	}
}

func TestBundleSourceRequiresExactDigestSuffix(t *testing.T) {
	run := testBundleRun()
	run.Bundle.URI = "s3://example-sporevm-artifacts/runs/ruby@base.bundle"
	if got, want := bundleSource(run), run.Bundle.URI+"@"+run.Bundle.Digest; got != want {
		t.Fatalf("bundleSource = %q, want %q", got, want)
	}

	run.Bundle.URI = run.Bundle.URI + "@" + run.Bundle.Digest
	if got, want := bundleSource(run), run.Bundle.URI; got != want {
		t.Fatalf("bundleSource = %q, want %q", got, want)
	}
}

func TestBundleSourceKeepsLocalFileURIUnpinned(t *testing.T) {
	run := testBundleRun()
	run.Bundle.URI = "file:///var/lib/sporevm/k8s-smoke/bundle"
	if got, want := bundleSource(run), run.Bundle.URI; got != want {
		t.Fatalf("bundleSource = %q, want %q", got, want)
	}
}

func TestRunBundleInspectorUsesDigestPinnedSourceAndRange(t *testing.T) {
	run := testBundleRun()
	client := &fakeSporeClient{
		inspectFunc: func(_ context.Context, req InspectBundleRequest) (InspectBundleResult, error) {
			if req.Source != run.Bundle.URI+"@"+run.Bundle.Digest {
				t.Fatalf("inspect source = %q", req.Source)
			}
			if req.ChildRange == nil || req.ChildRange.Start != run.Children.Start || req.ChildRange.End != run.Children.End() {
				t.Fatalf("child range = %+v", req.ChildRange)
			}
			return InspectBundleResult{
				Schema:        inspectBundleSchema,
				SchemaVersion: schemaVersion,
				BundleDigest: DigestRef{
					Algorithm: "sha256",
					Hex:       "1111111111111111111111111111111111111111111111111111111111111111",
				},
				ChildCount: run.Children.Count,
			}, nil
		},
	}

	inspection, err := (RunBundleInspector{Client: client}).InspectRunBundle(context.Background(), run)
	if err != nil {
		t.Fatalf("InspectRunBundle: %v", err)
	}
	if inspection.BundleDigest != run.Bundle.Digest || inspection.ChildCount != run.Children.Count {
		t.Fatalf("inspection = %+v", inspection)
	}
}

func TestLocalResultStoreRejectsUnsafeBucketSegment(t *testing.T) {
	store := newTestResultStore(t)
	run := testBundleRun()
	run.ResultStore = "s3://../ruby-counter-20260620/"

	_, _, err := store.TerminalResult(context.Background(), run, 0)
	if err == nil {
		t.Fatal("TerminalResult succeeded with unsafe bucket segment")
	}
}

type fakeSporeClient struct {
	mu          sync.Mutex
	hostInfo    HostInfo
	inspectFunc func(context.Context, InspectBundleRequest) (InspectBundleResult, error)
	runFunc     func(context.Context, RunCaptureRequest) ([]RunEvent, error)
	createFunc  func(context.Context, CreateVMRequest) error
	forkFunc    func(context.Context, ForkRequest) error
	packFunc    func(context.Context, PackRequest) error
	pullFunc    func(context.Context, PullRequest) (PullResult, error)
	resumeFunc  func(context.Context, ResumeRequest) ([]RunEvent, error)
	execFunc    func(context.Context, ExecRequest) ([]RunEvent, error)
	removeFunc  func(context.Context, RemoveVMRequest) error
	runCaptures []RunCaptureRequest
	creates     []CreateVMRequest
	forks       []ForkRequest
	packs       []PackRequest
	pulls       []PullRequest
	resumes     []ResumeRequest
	execs       []ExecRequest
	removes     []RemoveVMRequest
	outDir      string
}

func (c *fakeSporeClient) HostInfo(context.Context) (HostInfo, error) {
	return c.hostInfo, nil
}

func (c *fakeSporeClient) InspectBundle(ctx context.Context, req InspectBundleRequest) (InspectBundleResult, error) {
	if c.inspectFunc == nil {
		return InspectBundleResult{}, nil
	}
	return c.inspectFunc(ctx, req)
}

func (c *fakeSporeClient) RunCapture(ctx context.Context, req RunCaptureRequest) ([]RunEvent, error) {
	c.mu.Lock()
	c.runCaptures = append(c.runCaptures, req)
	c.mu.Unlock()
	if c.runFunc == nil {
		return []RunEvent{captureExitEvent(req.CaptureDir)}, nil
	}
	return c.runFunc(ctx, req)
}

func (c *fakeSporeClient) CreateVM(ctx context.Context, req CreateVMRequest) error {
	c.mu.Lock()
	c.creates = append(c.creates, req)
	c.mu.Unlock()
	if c.createFunc == nil {
		return nil
	}
	return c.createFunc(ctx, req)
}

func (c *fakeSporeClient) Fork(ctx context.Context, req ForkRequest) error {
	c.mu.Lock()
	c.forks = append(c.forks, req)
	c.mu.Unlock()
	if c.forkFunc == nil {
		return nil
	}
	return c.forkFunc(ctx, req)
}

func (c *fakeSporeClient) Pack(ctx context.Context, req PackRequest) error {
	c.mu.Lock()
	c.packs = append(c.packs, req)
	c.mu.Unlock()
	if c.packFunc == nil {
		return nil
	}
	return c.packFunc(ctx, req)
}

func (c *fakeSporeClient) Pull(ctx context.Context, req PullRequest) (PullResult, error) {
	c.mu.Lock()
	c.pulls = append(c.pulls, req)
	c.outDir = req.OutDir
	c.mu.Unlock()
	if c.pullFunc == nil {
		return pullResult(req.OutDir, req.ChildID), nil
	}
	return c.pullFunc(ctx, req)
}

func (c *fakeSporeClient) Resume(ctx context.Context, req ResumeRequest) ([]RunEvent, error) {
	c.mu.Lock()
	c.resumes = append(c.resumes, req)
	c.mu.Unlock()
	if c.resumeFunc == nil {
		return []RunEvent{exitEvent(0)}, nil
	}
	return c.resumeFunc(ctx, req)
}

func (c *fakeSporeClient) Exec(ctx context.Context, req ExecRequest) ([]RunEvent, error) {
	c.mu.Lock()
	c.execs = append(c.execs, req)
	c.mu.Unlock()
	if c.execFunc == nil {
		return []RunEvent{exitEvent(0)}, nil
	}
	return c.execFunc(ctx, req)
}

func (c *fakeSporeClient) RemoveVM(ctx context.Context, req RemoveVMRequest) error {
	c.mu.Lock()
	c.removes = append(c.removes, req)
	c.mu.Unlock()
	if c.removeFunc == nil {
		return nil
	}
	return c.removeFunc(ctx, req)
}

func (c *fakeSporeClient) runCaptureRequests() []RunCaptureRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]RunCaptureRequest(nil), c.runCaptures...)
}

func (c *fakeSporeClient) forkRequests() []ForkRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]ForkRequest(nil), c.forks...)
}

func (c *fakeSporeClient) packRequests() []PackRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]PackRequest(nil), c.packs...)
}

func (c *fakeSporeClient) pullRequests() []PullRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]PullRequest(nil), c.pulls...)
}

func (c *fakeSporeClient) resumeRequests() []ResumeRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]ResumeRequest(nil), c.resumes...)
}

func (c *fakeSporeClient) execRequests() []ExecRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]ExecRequest(nil), c.execs...)
}

func (c *fakeSporeClient) removeRequests() []RemoveVMRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]RemoveVMRequest(nil), c.removes...)
}

func (c *fakeSporeClient) lastOutDir() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.outDir
}

func newConfiguredRunner(t *testing.T, client *fakeSporeClient, store *LocalResultStore) *Runner {
	t.Helper()
	return newConfiguredRunnerWithMetrics(t, client, store, nil)
}

func newConfiguredRunnerWithMetrics(t *testing.T, client *fakeSporeClient, store *LocalResultStore, metrics MetricsSink) *Runner {
	t.Helper()
	runner, err := NewRunner(100,
		WithSporeClient(client),
		WithResultStore(store),
		WithWorkRoot(t.TempDir()),
		WithMetricsSink(metrics),
	)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	return runner
}

func newTestResultStore(t *testing.T) *LocalResultStore {
	t.Helper()
	store, err := NewLocalResultStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalResultStore: %v", err)
	}
	return store
}

func testBundleRun() fleet.BundleRun {
	return fleet.BundleRun{
		RunID: "ruby-counter-20260620",
		Bundle: fleet.Bundle{
			URI:    "s3://example-sporevm-artifacts/runs/ruby.bundle",
			Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		},
		Children: fleet.ChildRange{Start: 0, Count: 100},
		HostClass: fleet.HostClass{
			ID:                   "linux-aarch64-kvm-graviton1-v0",
			SporePlatformVersion: "v0",
			Architecture:         "aarch64",
			Backend:              "kvm",
			CPUProfile:           "graviton1",
			DeviceModel:          "sporevm-aarch64-v0",
		},
		Execution: fleet.Execution{
			ChildrenPerShard:    100,
			MaxInFlightPerAgent: 100,
		},
		RetryPolicy: fleet.RetryPolicy{
			MaxAttemptsPerChild:    2,
			RerunCommittedChildren: false,
		},
		SideEffects: fleet.SideEffects{
			IdempotencyRequired: true,
		},
		ResultStore: "s3://example-sporevm-results/ruby-counter-20260620/",
	}
}

func testRun() fleet.Run {
	return fleet.Run{
		RunID: "rails-rspec-20260624",
		Source: fleet.RunSource{
			Image:    "example.com/sporevm/rails-rspec:sha-1111111",
			Platform: "linux/arm64",
		},
		Prepare: fleet.PrepareSpec{
			Command:       []string{"/bin/bash", "/usr/local/bin/sporevm-rails-coordinator", "--capture-delay", "2"},
			CaptureSignal: "USR1",
			ReadyMarker:   "SPOREVM_RAILS_READY",
		},
		Fork: fleet.ForkSpec{Count: 1000},
		Children: fleet.RunChildren{
			Start:   0,
			Count:   1000,
			Command: []string{"/usr/local/bin/sporevm-rspec-shard"},
		},
		Execution: fleet.Execution{
			ChildrenPerShard:    100,
			MaxInFlightPerAgent: 100,
		},
		RetryPolicy: fleet.RetryPolicy{
			MaxAttemptsPerChild:    2,
			RerunCommittedChildren: false,
		},
		SideEffects: fleet.SideEffects{
			IdempotencyRequired: true,
		},
		ResultStore: "s3://example-sporevm-results/rails-rspec-20260624/",
	}
}

func testLease(run fleet.BundleRun) fleet.ShardLease {
	return fleet.ShardLease{
		RunID:         run.RunID,
		BundleDigest:  run.Bundle.Digest,
		ShardID:       run.RunID + "-shard-0000",
		ChildStart:    run.Children.Start,
		ChildCount:    run.Children.Count,
		AttemptBudget: run.RetryPolicy.MaxAttemptsPerChild,
		HostClassID:   run.HostClass.ID,
		AgentID:       "spore-agent-us-east-1a-0001",
		LeaseDeadline: time.Date(2026, 6, 20, 4, 10, 0, 0, time.UTC),
	}
}

func normalPressure() fleet.Pressure {
	return fleet.Pressure{
		Disk:   fleet.PressureNormal,
		Memory: fleet.PressureNormal,
	}
}

func validHostInfo() HostInfo {
	kernelPath := "/cache/kernels"
	rootFSPath := "/cache/rootfs"
	bundlePath := "/cache/bundles"
	runtimePath := "/run/sporevm"
	return HostInfo{
		Schema:        hostInfoSchema,
		SchemaVersion: schemaVersion,
		HostClass:     "linux-aarch64-kvm",
		Platform: PlatformFacts{
			OS:                 "linux",
			Arch:               "aarch64",
			CPUProfile:         "graviton1",
			DeviceModelVersion: 0,
		},
		Backends: []BackendAvailability{
			{Name: "kvm", Supported: true, Available: true, Reason: "available"},
		},
		CacheRoots: CacheRoots{
			Kernels: PathFact{Path: &kernelPath, Resolved: true, Source: "environment"},
			RootFS:  PathFact{Path: &rootFSPath, Resolved: true, Source: "environment"},
			Bundles: PathFact{Path: &bundlePath, Resolved: true, Source: "environment"},
			Runtime: PathFact{Path: &runtimePath, Resolved: true, Source: "environment"},
		},
	}
}

func pullResult(outDir, childID string) PullResult {
	return PullResult{
		Schema:        pullResultSchema,
		SchemaVersion: schemaVersion,
		OutDir:        outDir,
		BundleDigest: DigestRef{
			Algorithm: "sha256",
			Hex:       "1111111111111111111111111111111111111111111111111111111111111111",
		},
		Materialization: ChunkMaterializationSummary{
			ChunkCount:             10,
			MaterializedChunkCount: 10,
			PayloadBytes:           40960,
			LinkedChunkCount:       8,
			CopiedChunkCount:       2,
			Cache: CacheState{
				HitCount:     8,
				MissCount:    2,
				BytesFetched: 8192,
			},
		},
		RootFS: RootFSMaterializationSummary{
			ArtifactCount: 1,
			PayloadBytes:  1048576,
			Cache: CacheState{
				HitCount:     0,
				MissCount:    1,
				BytesFetched: 1048576,
			},
		},
		Remote: RemoteBundleCache{
			CacheHit:        false,
			OriginBytesRead: 8192,
			PeerBytesRead:   0,
		},
		Children: BundleChildrenSummary{
			Count:         100,
			SelectedChild: &childID,
		},
	}
}

type recordingMetricsSink struct {
	mu      sync.Mutex
	metrics []AttemptMetrics
}

func (s *recordingMetricsSink) ObserveAttempt(metric AttemptMetrics) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metrics = append(s.metrics, metric)
}

func (s *recordingMetricsSink) all() []AttemptMetrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]AttemptMetrics(nil), s.metrics...)
}

func exitEvent(code int) RunEvent {
	backend := "kvm"
	return RunEvent{
		Schema:        runEventsSchema,
		SchemaVersion: schemaVersion,
		Event:         "exit",
		Command:       "resume",
		Backend:       &backend,
		ExitCode:      &code,
		Timings: &RunEventTimings{
			ExecResponseMS: uint64(10 + code),
		},
	}
}

func outputEvent(event string, data string) RunEvent {
	backend := "kvm"
	return RunEvent{
		Schema:        runEventsSchema,
		SchemaVersion: schemaVersion,
		Event:         event,
		Command:       "resume",
		Backend:       &backend,
		ByteCount:     len(data),
		DataBase64:    base64.StdEncoding.EncodeToString([]byte(data)),
	}
}

func captureExitEvent(path string) RunEvent {
	backend := "kvm"
	exitCode := 0
	return RunEvent{
		Schema:        runEventsSchema,
		SchemaVersion: schemaVersion,
		Event:         "exit",
		Command:       "run",
		Backend:       &backend,
		ExitCode:      &exitCode,
		Captured:      true,
		CapturePath:   &path,
	}
}

func failureEvent(body MachineErrorBody) RunEvent {
	backend := "kvm"
	return RunEvent{
		Schema:        runEventsSchema,
		SchemaVersion: schemaVersion,
		Event:         "failure",
		Command:       "resume",
		Backend:       &backend,
		Error:         &body,
	}
}

func machineError(code, message string) error {
	return &MachineError{
		Envelope: MachineErrorEnvelope{
			Schema:        errorSchema,
			SchemaVersion: schemaVersion,
			Error: MachineErrorBody{
				Code:      code,
				Message:   message,
				Retryable: false,
				Scope:     "host",
				ExitCode:  69,
				Source:    "test",
			},
		},
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
