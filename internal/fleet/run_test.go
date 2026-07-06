package fleet

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"testing"
)

func TestDecodeRunRejectsUnknownFields(t *testing.T) {
	var raw map[string]any
	decodeExample(t, "run-rails-rspec.json", &raw)
	raw["unexpected"] = true

	payload, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal source run: %v", err)
	}

	_, err = DecodeRun(bytes.NewReader(payload))
	if err == nil {
		t.Fatal("DecodeRun succeeded with unknown field")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("DecodeRun error = %v, want ErrInvalidContract", err)
	}
}

func TestRunCompilesToBundleRun(t *testing.T) {
	source := loadRunExample(t)
	prepared := PreparedBundle{
		Bundle: Bundle{
			URI:    "s3://example-sporevm-artifacts/runs/rails-rspec-20260624.bundle",
			Digest: "sha256:2222222222222222222222222222222222222222222222222222222222222222",
		},
		ChildCount: 1000,
		HostClass:  loadExampleRun(t).HostClass,
	}

	run, err := source.Compile(prepared)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	if run.RunID != source.RunID {
		t.Fatalf("runID = %q, want %q", run.RunID, source.RunID)
	}
	if run.Bundle != prepared.Bundle {
		t.Fatalf("bundle = %#v, want %#v", run.Bundle, prepared.Bundle)
	}
	if run.Children != (ChildRange{Start: 0, Count: 1000}) {
		t.Fatalf("children = %#v, want 0/1000", run.Children)
	}
	if !slices.Equal(run.ChildCommand, source.Children.Command) {
		t.Fatalf("childCommand = %#v, want %#v", run.ChildCommand, source.Children.Command)
	}
	if run.ResultStore != source.ResultStore {
		t.Fatalf("resultStore = %q, want %q", run.ResultStore, source.ResultStore)
	}
}

func TestRunRejectsMissingSourceImage(t *testing.T) {
	run := loadRunExample(t)
	run.Source.Image = ""

	err := run.Validate()
	if err == nil {
		t.Fatal("Validate succeeded with missing source image")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("Validate error = %v, want ErrInvalidContract", err)
	}
}

func TestRunRejectsMissingChildCount(t *testing.T) {
	run := loadRunExample(t)
	run.Children.Count = 0

	err := run.Validate()
	if err == nil {
		t.Fatal("Validate succeeded with missing child count")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("Validate error = %v, want ErrInvalidContract", err)
	}
}

func TestRunRejectsIncompleteCaptureTrigger(t *testing.T) {
	run := loadRunExample(t)
	run.Prepare.ReadyMarker = ""

	err := run.Validate()
	if err == nil {
		t.Fatal("Validate succeeded with capture signal but no ready marker")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("Validate error = %v, want ErrInvalidContract", err)
	}
}

func TestRunRejectsUnsupportedCaptureSignal(t *testing.T) {
	run := loadRunExample(t)
	run.Prepare.CaptureSignal = "TERM"

	err := run.Validate()
	if err == nil {
		t.Fatal("Validate succeeded with unsupported capture signal")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("Validate error = %v, want ErrInvalidContract", err)
	}
}

func TestRunRejectsInvalidPrepareMemory(t *testing.T) {
	run := loadRunExample(t)
	run.Prepare.Memory = "512 mb"

	err := run.Validate()
	if err == nil {
		t.Fatal("Validate succeeded with invalid prepare memory")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("Validate error = %v, want ErrInvalidContract", err)
	}
}

func TestRunRejectsMissingResultStore(t *testing.T) {
	run := loadRunExample(t)
	run.ResultStore = ""

	err := run.Validate()
	if err == nil {
		t.Fatal("Validate succeeded with missing result store")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("Validate error = %v, want ErrInvalidContract", err)
	}
}

func TestRunRejectsUnsafeRetrySettings(t *testing.T) {
	run := loadRunExample(t)
	run.RetryPolicy.RerunCommittedChildren = true

	err := run.Validate()
	if err == nil {
		t.Fatal("Validate succeeded with unsafe retry settings")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("Validate error = %v, want ErrInvalidContract", err)
	}
}

func TestRunChildrenMustFitForkCount(t *testing.T) {
	run := loadRunExample(t)
	run.Children.Start = 1

	err := run.Validate()
	if err == nil {
		t.Fatal("Validate succeeded with child range outside fork count")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("Validate error = %v, want ErrInvalidContract", err)
	}
}

func TestRunCompileRequiresPreparedBundleCoverage(t *testing.T) {
	run := loadRunExample(t)
	_, err := run.Compile(PreparedBundle{
		Bundle: Bundle{
			URI:    "s3://example-sporevm-artifacts/runs/rails-rspec-20260624.bundle",
			Digest: "sha256:2222222222222222222222222222222222222222222222222222222222222222",
		},
		ChildCount: 999,
		HostClass:  loadExampleRun(t).HostClass,
	})

	if err == nil {
		t.Fatal("Compile succeeded with too few prepared children")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("Compile error = %v, want ErrInvalidContract", err)
	}
}

func loadRunExample(t *testing.T) Run {
	t.Helper()

	var run Run
	decodeExample(t, "run-rails-rspec.json", &run)
	if err := run.Validate(); err != nil {
		t.Fatalf("source run Validate: %v", err)
	}
	return run
}
