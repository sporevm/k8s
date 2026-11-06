package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	old := writeBootTemplateEntry(t, runner, strings.Repeat("1", 64), time.Unix(1, 0))
	newest := writeBootTemplateEntry(t, runner, strings.Repeat("2", 64), time.Unix(2, 0))
	build := writeBootTemplateEntry(t, runner, ".build-abandoned", time.Unix(3, 0))
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
	entry := writeBootTemplateEntry(t, runner, strings.Repeat("1", 64), time.Unix(1, 0))

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
	entry := writeBootTemplateEntry(t, runner, strings.Repeat("1", 64), time.Unix(1, 0))
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

func TestReconcileBootTemplatesEvictsTemplatesFromAnotherSporeBinary(t *testing.T) {
	spore := writeFakeSporeBinary(t, "spore build a", time.Unix(1, 0))
	client := &fakeSporeClient{hostInfo: validHostInfo(), version: "0.17.1", executable: spore}
	runner := newInteractiveTestRunner(t, 1, client)
	req := RunRequest{Image: interactiveTestImage, Memory: "512mb", Command: []string{"/bin/true"}}
	old, err := runner.Run(context.Background(), req, normalPressure())
	if err != nil {
		t.Fatalf("Run with the old binary: %v", err)
	}
	writeFakeSporeBinary(t, "spore build b", time.Unix(2, 0), spore)
	current, err := runner.Run(context.Background(), req, normalPressure())
	if err != nil {
		t.Fatalf("Run with the new binary: %v", err)
	}
	templateEntry := func(status TemplateStatus) string {
		return filepath.Join(runner.workRoot, "templates", strings.TrimPrefix(status.ID, "sha256:"))
	}
	oldEntry, currentEntry := templateEntry(old.Template), templateEntry(current.Template)
	oldTemplate := filepath.Join(oldEntry, "parent.spore")

	// A stale template that is still restoring a child stays until released.
	runner.templateMu.Lock()
	release := runner.leaseBootTemplate(oldTemplate)
	runner.templateMu.Unlock()
	result, err := runner.ReconcileBootTemplates(context.Background(), 16)
	if err != nil {
		t.Fatalf("ReconcileBootTemplates while leased: %v", err)
	}
	if result.RemovedTemplates != 0 {
		t.Fatalf("result while leased = %+v", result)
	}
	if _, err := os.Stat(oldEntry); err != nil {
		t.Fatalf("leased stale template was removed: %v", err)
	}
	release()

	result, err = runner.ReconcileBootTemplates(context.Background(), 16)
	if err != nil {
		t.Fatalf("ReconcileBootTemplates: %v", err)
	}
	if result.RemovedTemplates != 1 || result.RetainedTemplates != 1 {
		t.Fatalf("result = %+v, want the old binary's template removed", result)
	}
	if _, err := os.Stat(oldEntry); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale template still exists: %v", err)
	}
	if _, err := os.Stat(currentEntry); err != nil {
		t.Fatalf("current template was removed: %v", err)
	}
	removes := client.removeSavedSporeRequests()
	if len(removes) != 1 || removes[0].SporeDir != oldTemplate {
		t.Fatalf("saved spore removals = %+v, want %s", removes, oldTemplate)
	}
}

func TestReconcileBootTemplatesEvictsNothingAsStaleWithoutSporeIdentity(t *testing.T) {
	spore := writeFakeSporeBinary(t, "spore build a", time.Unix(1, 0))
	client := &fakeSporeClient{hostInfo: validHostInfo(), executable: spore}
	runner := newInteractiveTestRunner(t, 1, client)
	entry := writeBootTemplateEntry(t, runner, strings.Repeat("1", 64), time.Unix(1, 0))
	if err := os.Remove(spore); err != nil {
		t.Fatalf("remove spore binary: %v", err)
	}

	result, err := runner.ReconcileBootTemplates(context.Background(), 16)
	if err == nil || !strings.Contains(err.Error(), "boot template identity") {
		t.Fatalf("ReconcileBootTemplates error = %v, want the identity error", err)
	}
	if result.RemovedTemplates != 0 || result.RetainedTemplates != 1 {
		t.Fatalf("result = %+v, want the template retained", result)
	}
	if _, err := os.Stat(entry); err != nil {
		t.Fatalf("template removed without a known spore identity: %v", err)
	}
	if removes := client.removeSavedSporeRequests(); len(removes) != 0 {
		t.Fatalf("saved spore removals = %+v, want none", removes)
	}
}

func TestCollectCacheRunsOnlyOnTheCacheOwnerAfterRemovals(t *testing.T) {
	for _, owner := range []bool{false, true} {
		t.Run(fmt.Sprintf("owner=%t", owner), func(t *testing.T) {
			client := &fakeSporeClient{hostInfo: validHostInfo()}
			runner, err := NewRunner(1, WithSporeClient(client), WithWorkRoot(t.TempDir()), WithCacheGC(owner))
			if err != nil {
				t.Fatalf("NewRunner: %v", err)
			}
			collect := func() {
				t.Helper()
				if _, err := runner.CollectCache(context.Background()); err != nil {
					t.Fatalf("CollectCache: %v", err)
				}
			}

			collect() // startup
			collect() // nothing removed since
			writeBootTemplateEntry(t, runner, strings.Repeat("1", 64), time.Unix(1, 0))
			if result, err := runner.ReconcileBootTemplates(context.Background(), 0); err != nil || result.RemovedTemplates != 1 {
				t.Fatalf("ReconcileBootTemplates = %+v, %v; want one template removed", result, err)
			}
			collect()

			want := 0
			if owner {
				want = 2
			}
			if got := client.cacheGCCallCount(); got != want {
				t.Fatalf("spore cache gc ran %d times, want %d", got, want)
			}
		})
	}
}

// writeBootTemplateEntry writes a completed template whose key matches the
// runner's current spore binary and host.
func writeBootTemplateEntry(t *testing.T, runner *Runner, name string, modified time.Time) string {
	t.Helper()
	entry := filepath.Join(runner.workRoot, "templates", name)
	templateDir := filepath.Join(entry, "parent.spore")
	if err := os.MkdirAll(templateDir, 0o755); err != nil {
		t.Fatalf("create template entry: %v", err)
	}
	if err := os.WriteFile(filepath.Join(templateDir, "manifest.json"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	runner.templateMu.Lock()
	identity, err := runner.bootTemplateIdentity(context.Background())
	runner.templateMu.Unlock()
	if err != nil {
		t.Fatalf("boot template identity: %v", err)
	}
	key, err := json.Marshal(identity.key(runner.backend, interactiveTestImage, "512mb"))
	if err != nil {
		t.Fatalf("encode template key: %v", err)
	}
	metadata := filepath.Join(entry, "template.json")
	if err := os.WriteFile(metadata, key, 0o644); err != nil {
		t.Fatalf("write metadata: %v", err)
	}
	if err := os.Chtimes(metadata, modified, modified); err != nil {
		t.Fatalf("set metadata time: %v", err)
	}
	return entry
}
