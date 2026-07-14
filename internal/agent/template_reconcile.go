package agent

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// TemplateReconcileResult summarizes one off-request boot-template cleanup.
type TemplateReconcileResult struct {
	RemovedBuilds     int
	RemovedTemplates  int
	RetainedTemplates int
}

type bootTemplateEntry struct {
	path        string
	templateDir string
	modifiedNS  int64
}

// ReconcileBootTemplates removes abandoned builds and retains the newest completed templates.
func (r *Runner) ReconcileBootTemplates(ctx context.Context, retain int) (TemplateReconcileResult, error) {
	var result TemplateReconcileResult
	if retain < 0 {
		return result, fmt.Errorf("template retain count must be >= 0")
	}

	r.templateMu.Lock()
	defer r.templateMu.Unlock()

	templateRoot := filepath.Join(r.workRoot, "templates")
	entries, err := os.ReadDir(templateRoot)
	if errors.Is(err, os.ErrNotExist) {
		return result, nil
	}
	if err != nil {
		return result, err
	}

	var complete []bootTemplateEntry
	var cleanupErrors []error
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		entryDir := filepath.Join(templateRoot, entry.Name())
		templateDir := filepath.Join(entryDir, "parent.spore")
		if strings.HasPrefix(entry.Name(), ".build-") {
			if err := r.removeBootTemplateEntry(ctx, entryDir, templateDir); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("remove abandoned template build %s: %w", entry.Name(), err))
			} else {
				result.RemovedBuilds++
			}
			continue
		}
		if !isBootTemplateEntryName(entry.Name()) {
			continue
		}
		metadata, metadataErr := os.Stat(filepath.Join(entryDir, "template.json"))
		if !bootTemplateReady(templateDir) || metadataErr != nil || metadata.IsDir() {
			if err := r.removeBootTemplateEntry(ctx, entryDir, templateDir); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("remove incomplete template %s: %w", entry.Name(), err))
			} else {
				result.RemovedTemplates++
			}
			continue
		}
		complete = append(complete, bootTemplateEntry{
			path:        entryDir,
			templateDir: templateDir,
			modifiedNS:  metadata.ModTime().UnixNano(),
		})
	}

	sort.Slice(complete, func(i, j int) bool {
		if complete[i].modifiedNS == complete[j].modifiedNS {
			return complete[i].path > complete[j].path
		}
		return complete[i].modifiedNS > complete[j].modifiedNS
	})
	for _, entry := range complete {
		if result.RetainedTemplates < retain || r.templateUses[entry.templateDir] > 0 {
			result.RetainedTemplates++
			continue
		}
		if err := r.removeBootTemplateEntry(ctx, entry.path, entry.templateDir); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("remove expired template %s: %w", filepath.Base(entry.path), err))
			continue
		}
		result.RemovedTemplates++
	}
	return result, errors.Join(cleanupErrors...)
}

func (r *Runner) removeBootTemplateEntry(ctx context.Context, entryDir, templateDir string) error {
	if bootTemplateReady(templateDir) {
		if err := r.client.RemoveSavedSpore(ctx, RemoveSavedSporeRequest{SporeDir: templateDir}); err != nil {
			return err
		}
	}
	return os.RemoveAll(entryDir)
}

func isBootTemplateEntryName(name string) bool {
	decoded, err := hex.DecodeString(name)
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == name
}
