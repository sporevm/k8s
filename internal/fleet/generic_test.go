package fleet

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"testing"
)

func TestDecodeGenericRunRejectsUnknownFields(t *testing.T) {
	var raw map[string]any
	decodeExample(t, "generic-run-rails-rspec.json", &raw)
	raw["unexpected"] = true

	payload, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal generic run: %v", err)
	}

	_, err = DecodeGenericRun(bytes.NewReader(payload))
	if err == nil {
		t.Fatal("DecodeGenericRun succeeded with unknown field")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("DecodeGenericRun error = %v, want ErrInvalidContract", err)
	}
}

func TestGenericRunCompilesToBundleRun(t *testing.T) {
	generic := loadGenericRunExample(t)
	prepared := PreparedBundle{
		Bundle: Bundle{
			URI:    "s3://example-sporevm-artifacts/runs/rails-rspec-20260624.bundle",
			Digest: "sha256:2222222222222222222222222222222222222222222222222222222222222222",
		},
		ChildCount: 1000,
		HostClass:  loadExampleRun(t).HostClass,
	}

	run, err := generic.Compile(prepared)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	if run.RunID != generic.RunID {
		t.Fatalf("runID = %q, want %q", run.RunID, generic.RunID)
	}
	if run.Bundle != prepared.Bundle {
		t.Fatalf("bundle = %#v, want %#v", run.Bundle, prepared.Bundle)
	}
	if run.Children != (ChildRange{Start: 0, Count: 1000}) {
		t.Fatalf("children = %#v, want 0/1000", run.Children)
	}
	if !slices.Equal(run.ChildCommand, generic.Children.Command) {
		t.Fatalf("childCommand = %#v, want %#v", run.ChildCommand, generic.Children.Command)
	}
	if run.ResultStore != generic.ResultStore {
		t.Fatalf("resultStore = %q, want %q", run.ResultStore, generic.ResultStore)
	}
}

func TestGenericRunRejectsMissingSourceImage(t *testing.T) {
	run := loadGenericRunExample(t)
	run.Source.Image = ""

	err := run.Validate()
	if err == nil {
		t.Fatal("Validate succeeded with missing source image")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("Validate error = %v, want ErrInvalidContract", err)
	}
}

func TestGenericRunRejectsMissingChildCount(t *testing.T) {
	run := loadGenericRunExample(t)
	run.Children.Count = 0

	err := run.Validate()
	if err == nil {
		t.Fatal("Validate succeeded with missing child count")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("Validate error = %v, want ErrInvalidContract", err)
	}
}

func TestGenericRunRejectsIncompleteCaptureTrigger(t *testing.T) {
	run := loadGenericRunExample(t)
	run.Prepare.ReadyMarker = ""

	err := run.Validate()
	if err == nil {
		t.Fatal("Validate succeeded with capture signal but no ready marker")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("Validate error = %v, want ErrInvalidContract", err)
	}
}

func TestGenericRunRejectsUnsupportedCaptureSignal(t *testing.T) {
	run := loadGenericRunExample(t)
	run.Prepare.CaptureSignal = "TERM"

	err := run.Validate()
	if err == nil {
		t.Fatal("Validate succeeded with unsupported capture signal")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("Validate error = %v, want ErrInvalidContract", err)
	}
}

func TestGenericRunRejectsInvalidPrepareMemory(t *testing.T) {
	run := loadGenericRunExample(t)
	run.Prepare.Memory = "512 mb"

	err := run.Validate()
	if err == nil {
		t.Fatal("Validate succeeded with invalid prepare memory")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("Validate error = %v, want ErrInvalidContract", err)
	}
}

func TestGenericRunRejectsMissingResultStore(t *testing.T) {
	run := loadGenericRunExample(t)
	run.ResultStore = ""

	err := run.Validate()
	if err == nil {
		t.Fatal("Validate succeeded with missing result store")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("Validate error = %v, want ErrInvalidContract", err)
	}
}

func TestGenericRunRejectsUnsafeRetrySettings(t *testing.T) {
	run := loadGenericRunExample(t)
	run.RetryPolicy.RerunCommittedChildren = true

	err := run.Validate()
	if err == nil {
		t.Fatal("Validate succeeded with unsafe retry settings")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("Validate error = %v, want ErrInvalidContract", err)
	}
}

func TestGenericRunChildrenMustFitForkCount(t *testing.T) {
	run := loadGenericRunExample(t)
	run.Children.Start = 1

	err := run.Validate()
	if err == nil {
		t.Fatal("Validate succeeded with child range outside fork count")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("Validate error = %v, want ErrInvalidContract", err)
	}
}

func TestGenericRunCompileRequiresPreparedBundleCoverage(t *testing.T) {
	run := loadGenericRunExample(t)
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

func loadGenericRunExample(t *testing.T) GenericRun {
	t.Helper()

	var run GenericRun
	decodeExample(t, "generic-run-rails-rspec.json", &run)
	if err := run.Validate(); err != nil {
		t.Fatalf("generic run Validate: %v", err)
	}
	return run
}
