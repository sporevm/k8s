package agent

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type blockingJSON struct {
	started chan<- struct{}
	resume  <-chan struct{}
}

func (value blockingJSON) MarshalJSON() ([]byte, error) {
	close(value.started)
	<-value.resume
	return []byte(`{"complete":true}`), nil
}

func TestWriteJSONCreateOnlyPublishesCompleteDocument(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "terminal.json")
	started := make(chan struct{})
	resume := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		created, err := writeJSONCreateOnly(path, blockingJSON{started: started, resume: resume})
		if err == nil && !created {
			err = errors.New("result was not created")
		}
		done <- err
	}()
	<-started
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("result became visible before encoding completed: %v", err)
	}
	close(resume)
	if err := <-done; err != nil {
		t.Fatalf("writeJSONCreateOnly: %v", err)
	}
	var result struct {
		Complete bool `json:"complete"`
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &result); err != nil || !result.Complete {
		t.Fatalf("published result = %q, error = %v", data, err)
	}
}

func TestWriteJSONCreateOnlyDoesNotReplaceExistingResult(t *testing.T) {
	path := filepath.Join(t.TempDir(), "terminal.json")
	if created, err := writeJSONCreateOnly(path, map[string]int{"attempt": 1}); err != nil || !created {
		t.Fatalf("first write = created %v, error %v", created, err)
	}
	if created, err := writeJSONCreateOnly(path, map[string]int{"attempt": 2}); err != nil || created {
		t.Fatalf("second write = created %v, error %v", created, err)
	}
	var result map[string]int
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &result); err != nil || result["attempt"] != 1 {
		t.Fatalf("result = %q, error = %v", data, err)
	}
	temps, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".sporevm-result-*.tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if len(temps) != 0 {
		t.Fatalf("temporary result files remain: %v", temps)
	}
}
