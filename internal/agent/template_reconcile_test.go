package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReconcileBootTemplatesCleansBuildsAndRetainsNewest(t *testing.T) {
	client := &fakeSporeClient{hostInfo: validHostInfo()}
	runner := newInteractiveTestRunner(t, 1, client)
	root := filepath.Join(runner.workRoot, "templates")
	old := writeBootTemplateEntry(t, root, strings.Repeat("1", 64), time.Unix(1, 0))
	newest := writeBootTemplateEntry(t, root, strings.Repeat("2", 64), time.Unix(2, 0))
	build := writeBootTemplateEntry(t, root, ".build-abandoned", time.Unix(3, 0))
	incomplete := filepath.Join(root, strings.Repeat("3", 64))
	if err := os.MkdirAll(incomplete, 0o755); err != nil {
		t.Fatalf("create incomplete entry: %v", err)
	}
	unknown := filepath.Join(root, "operator-notes")
	if err := os.MkdirAll(unknown, 0o755); err != nil {
		t.Fatalf("create unknown entry: %v", err)
	}

	result, err := runner.ReconcileBootTemplates(context.Background(), 1)
	if err != nil {
		t.Fatalf("ReconcileBootTemplates: %v", err)
	}
	if result.RemovedBuilds != 1 || result.RemovedTemplates != 2 || result.RetainedTemplates != 1 {
		t.Fatalf("result = %+v", result)
	}
	for _, removed := range []string{old, build, incomplete} {
		if _, err := os.Stat(removed); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("removed entry %s still exists: %v", removed, err)
		}
	}
	for _, retained := range []string{newest, unknown} {
		if _, err := os.Stat(retained); err != nil {
			t.Fatalf("retained entry %s: %v", retained, err)
		}
	}
	removes := client.removeSavedSporeRequests()
	if len(removes) != 2 {
		t.Fatalf("saved spore removals = %+v", removes)
	}
}

func TestReconcileBootTemplatesPreservesPinWhenRemovalFails(t *testing.T) {
	client := &fakeSporeClient{
		hostInfo: validHostInfo(),
		removeSavedFunc: func(context.Context, RemoveSavedSporeRequest) error {
			return errors.New("cache lock unavailable")
		},
	}
	runner := newInteractiveTestRunner(t, 1, client)
	entry := writeBootTemplateEntry(t, filepath.Join(runner.workRoot, "templates"), strings.Repeat("1", 64), time.Unix(1, 0))

	result, err := runner.ReconcileBootTemplates(context.Background(), 0)
	if err == nil {
		t.Fatal("ReconcileBootTemplates succeeded when durable pin removal failed")
	}
	if result.RemovedTemplates != 0 {
		t.Fatalf("result = %+v", result)
	}
	if _, err := os.Stat(entry); err != nil {
		t.Fatalf("recoverable template was removed: %v", err)
	}
}

func TestReconcileBootTemplatesSkipsActiveTemplate(t *testing.T) {
	runner := newInteractiveTestRunner(t, 1, &fakeSporeClient{hostInfo: validHostInfo()})
	entry := writeBootTemplateEntry(t, filepath.Join(runner.workRoot, "templates"), strings.Repeat("1", 64), time.Unix(1, 0))
	templateDir := filepath.Join(entry, "parent.spore")
	runner.templateMu.Lock()
	release := runner.leaseBootTemplate(templateDir)
	runner.templateMu.Unlock()
	defer release()

	result, err := runner.ReconcileBootTemplates(context.Background(), 0)
	if err != nil {
		t.Fatalf("ReconcileBootTemplates: %v", err)
	}
	if result.RetainedTemplates != 1 || result.RemovedTemplates != 0 {
		t.Fatalf("result = %+v", result)
	}
	if _, err := os.Stat(entry); err != nil {
		t.Fatalf("active template was removed: %v", err)
	}
}

func writeBootTemplateEntry(t *testing.T, root, name string, modified time.Time) string {
	t.Helper()
	entry := filepath.Join(root, name)
	templateDir := filepath.Join(entry, "parent.spore")
	if err := os.MkdirAll(templateDir, 0o755); err != nil {
		t.Fatalf("create template entry: %v", err)
	}
	if err := os.WriteFile(filepath.Join(templateDir, "manifest.json"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	metadata := filepath.Join(entry, "template.json")
	if err := os.WriteFile(metadata, []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write metadata: %v", err)
	}
	if err := os.Chtimes(metadata, modified, modified); err != nil {
		t.Fatalf("set metadata time: %v", err)
	}
	return entry
}
