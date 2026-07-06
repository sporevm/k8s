package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/sporevm/k8s/internal/fleet"
)

var (
	// ErrResultStoreNotConfigured means the runner has no result store.
	ErrResultStoreNotConfigured = errors.New("result store not configured")
	// ErrResultExists means a create-only result write found an existing object.
	ErrResultExists = errors.New("result object already exists")
)

// ResultStore persists per-child attempt documents and terminal commits.
type ResultStore interface {
	TerminalResult(context.Context, fleet.BundleRun, int) (fleet.AttemptResult, bool, error)
	WriteAttemptResult(context.Context, fleet.BundleRun, fleet.AttemptResult) error
	CommitTerminalResult(context.Context, fleet.BundleRun, fleet.AttemptResult) (bool, error)
}

// TerminalResultURI returns the create-if-absent terminal object URI for a child.
func TerminalResultURI(run fleet.BundleRun, childID int) string {
	return run.ResultStore + "children/" + strconv.Itoa(childID) + "/terminal.json"
}

// AttemptResultURI returns the append-only attempt object URI for a child attempt.
func AttemptResultURI(run fleet.BundleRun, childID int, attemptID string) string {
	return run.ResultStore + "children/" + strconv.Itoa(childID) + "/attempts/" + attemptID + ".json"
}

// LocalResultStore maps fleet S3 result URIs into a local directory.
//
// It is intended for deterministic local tests and smokes while preserving the
// same URI layout and create-only semantics expected from the S3 backend.
type LocalResultStore struct {
	Root string
}

// NewLocalResultStore creates a local result store rooted at root.
func NewLocalResultStore(root string) (*LocalResultStore, error) {
	if root == "" {
		return nil, errors.New("local result store root must not be empty")
	}
	return &LocalResultStore{Root: root}, nil
}

// TerminalResult reads a previously committed terminal result if present.
func (s *LocalResultStore) TerminalResult(ctx context.Context, run fleet.BundleRun, childID int) (fleet.AttemptResult, bool, error) {
	if err := ctx.Err(); err != nil {
		return fleet.AttemptResult{}, false, err
	}
	if err := run.Validate(); err != nil {
		return fleet.AttemptResult{}, false, err
	}
	if childID < run.Children.Start || childID >= run.Children.End() {
		return fleet.AttemptResult{}, false, fmt.Errorf("%w: terminal child id is outside the run", fleet.ErrInvalidContract)
	}

	path, err := s.pathForURI(TerminalResultURI(run, childID))
	if err != nil {
		return fleet.AttemptResult{}, false, err
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return fleet.AttemptResult{}, false, nil
	}
	if err != nil {
		return fleet.AttemptResult{}, false, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var result fleet.AttemptResult
	if err := decoder.Decode(&result); err != nil {
		return fleet.AttemptResult{}, false, fmt.Errorf("%w: decode terminal result: %v", ErrInvalidMachineOutput, err)
	}
	var extra struct{}
	if err := decoder.Decode(&extra); err == nil {
		return fleet.AttemptResult{}, false, fmt.Errorf("%w: terminal result has multiple JSON documents", ErrInvalidMachineOutput)
	} else if !errors.Is(err, io.EOF) {
		return fleet.AttemptResult{}, false, fmt.Errorf("%w: decode trailing terminal result: %v", ErrInvalidMachineOutput, err)
	}
	if err := result.Validate(run); err != nil {
		return fleet.AttemptResult{}, false, err
	}
	if !result.Terminal {
		return fleet.AttemptResult{}, false, fmt.Errorf("%w: committed terminal result is not terminal", ErrInvalidMachineOutput)
	}
	return result, true, nil
}

// WriteAttemptResult writes an append-only attempt result document.
func (s *LocalResultStore) WriteAttemptResult(ctx context.Context, run fleet.BundleRun, result fleet.AttemptResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := result.Validate(run); err != nil {
		return err
	}
	path, err := s.pathForURI(AttemptResultURI(run, result.ChildID, result.AttemptID))
	if err != nil {
		return err
	}
	created, err := writeJSONCreateOnly(path, result)
	if err != nil {
		return err
	}
	if !created {
		return ErrResultExists
	}
	return nil
}

// CommitTerminalResult writes the terminal result only when no terminal object exists.
func (s *LocalResultStore) CommitTerminalResult(ctx context.Context, run fleet.BundleRun, result fleet.AttemptResult) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := result.Validate(run); err != nil {
		return false, err
	}
	if !result.Terminal {
		return false, errors.New("cannot commit non-terminal result")
	}
	terminalURI := TerminalResultURI(run, result.ChildID)
	if result.ResultURI != terminalURI {
		return false, fmt.Errorf("terminal result URI = %q, want %q", result.ResultURI, terminalURI)
	}
	path, err := s.pathForURI(terminalURI)
	if err != nil {
		return false, err
	}
	return writeJSONCreateOnly(path, result)
}

func (s *LocalResultStore) pathForURI(raw string) (string, error) {
	if s == nil || s.Root == "" {
		return "", ErrResultStoreNotConfigured
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if parsed.Scheme != "s3" || parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("unsupported result URI %q", raw)
	}
	if unsafeLocalPathSegment(parsed.Host) {
		return "", fmt.Errorf("result URI %q contains an unsafe bucket segment", raw)
	}
	key := strings.TrimPrefix(parsed.Path, "/")
	if key == "" {
		return "", fmt.Errorf("result URI %q has empty object key", raw)
	}
	parts := []string{s.Root, parsed.Host}
	for _, part := range strings.Split(key, "/") {
		if unsafeLocalPathSegment(part) {
			return "", fmt.Errorf("result URI %q contains an unsafe path segment", raw)
		}
		parts = append(parts, part)
	}
	return filepath.Join(parts...), nil
}

func unsafeLocalPathSegment(part string) bool {
	return part == "" || part == "." || part == ".." || strings.ContainsRune(part, os.PathSeparator)
}

func writeJSONCreateOnly(path string, value any) (bool, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if errors.Is(err, os.ErrExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return false, err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return false, err
	}
	return true, nil
}
