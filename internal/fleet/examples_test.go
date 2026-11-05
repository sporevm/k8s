package fleet

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestExamplesValidate(t *testing.T) {
	run := loadExampleRun(t)
	validators := map[string]func(io.Reader) error{
		"agent-status.json":      validateExample(AgentStatus.Validate),
		"attempt-result.json":    validateExample(func(r AttemptResult) error { return r.Validate(run) }),
		"bundle-run-1000.json":   validateExample(BundleRun.Validate),
		"run-busybox-smoke.json": validateExample(Run.Validate),
		"run-rails-rspec.json":   validateExample(Run.Validate),
		"shard-lease.json":       validateExample(func(l ShardLease) error { return l.Validate(run) }),
	}

	paths, err := filepath.Glob(filepath.Join("..", "..", "examples", "fleet", "*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != len(validators) {
		t.Errorf("examples = %d files, validators = %d", len(paths), len(validators))
	}
	for _, path := range paths {
		validate, ok := validators[filepath.Base(path)]
		if !ok {
			t.Errorf("%s has no validator", path)
			continue
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := validate(f); err != nil {
			t.Errorf("%s: %v", path, err)
		}
		f.Close()
	}
}

func validateExample[T any](validate func(T) error) func(io.Reader) error {
	return func(r io.Reader) error {
		var doc T
		if err := decodeOne(r, "example", &doc); err != nil {
			return err
		}
		return validate(doc)
	}
}
