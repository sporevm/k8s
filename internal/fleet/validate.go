package fleet

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

var (
	idPattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{2,127}$`)
	digestPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

	// ErrInvalidContract identifies invalid fleet contract documents.
	ErrInvalidContract = errors.New("invalid fleet contract")
)

// DecodeRun decodes a run document and rejects unknown fields.
func DecodeRun(r io.Reader) (Run, error) {
	var run Run
	if err := decodeOne(r, "run", &run); err != nil {
		return Run{}, err
	}
	if err := run.Validate(); err != nil {
		return Run{}, err
	}
	return run, nil
}

// DecodeGenericRun decodes a generic source/prepare/fork run document.
func DecodeGenericRun(r io.Reader) (GenericRun, error) {
	var run GenericRun
	if err := decodeOne(r, "generic run", &run); err != nil {
		return GenericRun{}, err
	}
	if err := run.Validate(); err != nil {
		return GenericRun{}, err
	}
	return run, nil
}

func decodeOne(r io.Reader, name string, out any) error {
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(out); err != nil {
		return fmt.Errorf("%w: decode %s: %v", ErrInvalidContract, name, err)
	}
	var extra struct{}
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return contractError("%s must contain exactly one JSON document", name)
		}
		return fmt.Errorf("%w: decode trailing %s data: %v", ErrInvalidContract, name, err)
	}
	return nil
}

// Validate checks the generic run-level contract before parent preparation.
func (r GenericRun) Validate() error {
	if err := validateID(r.RunID, "runID"); err != nil {
		return err
	}
	if r.Source.Image == "" {
		return contractError("source.image must not be empty")
	}
	if r.Source.Platform != "linux/arm64" {
		return contractError("source.platform must be linux/arm64")
	}
	if err := r.Prepare.Validate("prepare"); err != nil {
		return err
	}
	if r.Fork.Count < 1 {
		return contractError("fork.count must be >= 1")
	}
	if err := r.Children.ChildRange().Validate("children"); err != nil {
		return err
	}
	if r.Children.ChildRange().End() > r.Fork.Count {
		return contractError("children range must fit fork.count")
	}
	if err := (CommandSpec{Command: r.Children.Command}).Validate("children"); err != nil {
		return err
	}
	if r.Execution.ChildrenPerShard < 1 {
		return contractError("execution.childrenPerShard must be >= 1")
	}
	if r.Execution.ChildrenPerShard > r.Children.Count {
		return contractError("execution.childrenPerShard cannot exceed child count")
	}
	if r.Execution.MaxInFlightPerAgent < 1 {
		return contractError("execution.maxInFlightPerAgent must be >= 1")
	}
	if r.RetryPolicy.MaxAttemptsPerChild < 1 {
		return contractError("retryPolicy.maxAttemptsPerChild must be >= 1")
	}
	if r.RetryPolicy.RerunCommittedChildren {
		return contractError("retryPolicy.rerunCommittedChildren must be false")
	}
	if !r.SideEffects.IdempotencyRequired {
		return contractError("sideEffects.idempotencyRequired must be true")
	}
	if err := validateS3Prefix(r.ResultStore, "resultStore"); err != nil {
		return err
	}
	return nil
}

// Validate checks an argv command spec.
func (c CommandSpec) Validate(path string) error {
	if len(c.Command) == 0 {
		return contractError("%s.command must not be empty", path)
	}
	for i, arg := range c.Command {
		if arg == "" {
			return contractError("%s.command[%d] must not be empty", path, i)
		}
	}
	return nil
}

// Validate checks a warm parent preparation command.
func (p PrepareSpec) Validate(path string) error {
	if err := (CommandSpec{Command: p.Command}).Validate(path); err != nil {
		return err
	}
	if p.CaptureSignal == "" && p.ReadyMarker == "" {
		return nil
	}
	if p.CaptureSignal == "" {
		return contractError("%s.captureSignal is required when readyMarker is set", path)
	}
	if p.ReadyMarker == "" {
		return contractError("%s.readyMarker is required when captureSignal is set", path)
	}
	if p.CaptureSignal != "USR1" {
		return contractError("%s.captureSignal must be USR1", path)
	}
	return nil
}

// Compile turns a prepared generic run into the existing immutable bundle run.
func (r GenericRun) Compile(prepared PreparedBundle) (Run, error) {
	if err := r.Validate(); err != nil {
		return Run{}, err
	}
	if prepared.Bundle.URI == "" {
		return Run{}, contractError("prepared.bundle.uri must not be empty")
	}
	if err := validateDigest(prepared.Bundle.Digest, "prepared.bundle.digest"); err != nil {
		return Run{}, err
	}
	if prepared.ChildCount < r.Children.ChildRange().End() {
		return Run{}, contractError("prepared.childCount must cover children range")
	}
	if err := prepared.HostClass.Validate("prepared.hostClass"); err != nil {
		return Run{}, err
	}
	run := Run{
		RunID:       r.RunID,
		Bundle:      prepared.Bundle,
		Children:    r.Children.ChildRange(),
		HostClass:   prepared.HostClass,
		Execution:   r.Execution,
		RetryPolicy: r.RetryPolicy,
		SideEffects: r.SideEffects,
		ResultStore: r.ResultStore,
	}
	if err := run.Validate(); err != nil {
		return Run{}, err
	}
	return run, nil
}

// Validate checks the run-level contract invariants needed for admission.
func (r Run) Validate() error {
	if err := validateID(r.RunID, "runID"); err != nil {
		return err
	}
	if r.Bundle.URI == "" {
		return contractError("bundle.uri must not be empty")
	}
	if err := validateDigest(r.Bundle.Digest, "bundle.digest"); err != nil {
		return err
	}
	if err := r.Children.Validate("children"); err != nil {
		return err
	}
	if err := r.HostClass.Validate("hostClass"); err != nil {
		return err
	}
	if r.Execution.ChildrenPerShard < 1 {
		return contractError("execution.childrenPerShard must be >= 1")
	}
	if r.Execution.ChildrenPerShard > r.Children.Count {
		return contractError("execution.childrenPerShard cannot exceed child count")
	}
	if r.Execution.MaxInFlightPerAgent < 1 {
		return contractError("execution.maxInFlightPerAgent must be >= 1")
	}
	if r.RetryPolicy.MaxAttemptsPerChild < 1 {
		return contractError("retryPolicy.maxAttemptsPerChild must be >= 1")
	}
	if r.RetryPolicy.RerunCommittedChildren {
		return contractError("retryPolicy.rerunCommittedChildren must be false")
	}
	if !r.SideEffects.IdempotencyRequired {
		return contractError("sideEffects.idempotencyRequired must be true")
	}
	if err := validateS3Prefix(r.ResultStore, "resultStore"); err != nil {
		return err
	}
	return nil
}

// Validate checks the child range.
func (r ChildRange) Validate(path string) error {
	if r.Start < 0 {
		return contractError("%s.start must be >= 0", path)
	}
	if r.Count < 1 {
		return contractError("%s.count must be >= 1", path)
	}
	return nil
}

// Validate checks host-class fields that are exact restore inputs today.
func (h HostClass) Validate(path string) error {
	if err := validateID(h.ID, path+".id"); err != nil {
		return err
	}
	if h.SporePlatformVersion != "v0" {
		return contractError("%s.sporePlatformVersion must be v0", path)
	}
	if h.Architecture != "aarch64" {
		return contractError("%s.architecture must be aarch64", path)
	}
	if h.Backend != "kvm" {
		return contractError("%s.backend must be kvm", path)
	}
	if h.CPUProfile == "" {
		return contractError("%s.cpuProfile must not be empty", path)
	}
	if h.DeviceModel == "" {
		return contractError("%s.deviceModel must not be empty", path)
	}
	return nil
}

// Validate checks agent status before it is considered for placement.
func (s AgentStatus) Validate() error {
	if err := validateID(s.AgentID, "agentID"); err != nil {
		return err
	}
	if err := validateID(s.CellID, "cellID"); err != nil {
		return err
	}
	if s.ObservedAt.IsZero() {
		return contractError("observedAt must not be zero")
	}
	if err := s.HostClass.Validate("hostClass"); err != nil {
		return err
	}
	if s.ExecutionSlots.Total < 1 {
		return contractError("executionSlots.total must be >= 1")
	}
	if s.ExecutionSlots.Available < 0 {
		return contractError("executionSlots.available must be >= 0")
	}
	if s.ExecutionSlots.Available > s.ExecutionSlots.Total {
		return contractError("executionSlots.available cannot exceed total")
	}
	if s.Cache.BundleCacheBytes < 0 || s.Cache.RootFSCacheBytes < 0 ||
		s.Cache.BundleCacheEntries < 0 || s.Cache.RootFSCacheEntries < 0 {
		return contractError("cache values must be >= 0")
	}
	if err := s.Pressure.Validate("pressure"); err != nil {
		return err
	}
	if s.Healthy && s.Pressure.Critical() {
		return contractError("agent cannot be healthy under critical pressure")
	}
	return nil
}

// Validate checks pressure enum values.
func (p Pressure) Validate(path string) error {
	if !validPressure(p.Disk) {
		return contractError("%s.disk has unsupported value", path)
	}
	if !validPressure(p.Memory) {
		return contractError("%s.memory has unsupported value", path)
	}
	return nil
}

// Validate checks a shard lease against a run.
func (l ShardLease) Validate(run Run) error {
	if err := run.Validate(); err != nil {
		return err
	}
	if l.RunID != run.RunID {
		return contractError("shardLease.runID does not match run.runID")
	}
	if l.BundleDigest != run.Bundle.Digest {
		return contractError("shardLease.bundleDigest does not match run bundle digest")
	}
	if err := validateID(l.ShardID, "shardLease.shardID"); err != nil {
		return err
	}
	if err := l.ChildRange().Validate("shardLease.children"); err != nil {
		return err
	}
	if l.ChildStart < run.Children.Start || l.ChildRange().End() > run.Children.End() {
		return contractError("shardLease child range is outside the run")
	}
	if l.AttemptBudget < 1 {
		return contractError("shardLease.attemptBudget must be >= 1")
	}
	if l.AttemptBudget > run.RetryPolicy.MaxAttemptsPerChild {
		return contractError("shardLease.attemptBudget exceeds run retry budget")
	}
	if l.HostClassID != run.HostClass.ID {
		return contractError("shardLease.hostClassID does not match run host class")
	}
	if err := validateID(l.AgentID, "shardLease.agentID"); err != nil {
		return err
	}
	if l.LeaseDeadline.IsZero() {
		return contractError("shardLease.leaseDeadline must not be zero")
	}
	return nil
}

// FormatAttemptID returns the stable attempt id for a child attempt.
func FormatAttemptID(runID string, childID int, attempt int) string {
	return runID + "-child-" + strconv.Itoa(childID) + "-attempt-" + fmt.Sprintf("%02d", attempt)
}

// Validate checks an attempt result against the admitted run contract.
func (r AttemptResult) Validate(run Run) error {
	if err := run.Validate(); err != nil {
		return err
	}
	if r.RunID != run.RunID {
		return contractError("attemptResult.runID does not match run.runID")
	}
	if r.BundleDigest != run.Bundle.Digest {
		return contractError("attemptResult.bundleDigest does not match run bundle digest")
	}
	if r.ChildID < run.Children.Start || r.ChildID >= run.Children.End() {
		return contractError("attemptResult.childID is outside the run")
	}
	if err := validateID(r.AttemptID, "attemptResult.attemptID"); err != nil {
		return err
	}
	if err := validateID(r.AgentID, "attemptResult.agentID"); err != nil {
		return err
	}
	if err := validateID(r.ShardID, "attemptResult.shardID"); err != nil {
		return err
	}
	if !validAttemptStatus(r.Status) {
		return contractError("attemptResult.status has unsupported value")
	}
	if r.StartedAt.IsZero() {
		return contractError("attemptResult.startedAt must not be zero")
	}
	if r.FinishedAt.IsZero() {
		return contractError("attemptResult.finishedAt must not be zero")
	}
	if r.FinishedAt.Before(r.StartedAt) {
		return contractError("attemptResult.finishedAt must not be before startedAt")
	}
	if err := r.TimingsMS.Validate("attemptResult.timingsMs"); err != nil {
		return err
	}
	if r.Terminal && r.ResultURI == "" {
		return contractError("attemptResult.resultURI must be set for terminal results")
	}
	if r.ResultURI != "" {
		if err := validateS3JSONURI(r.ResultURI, "attemptResult.resultURI"); err != nil {
			return err
		}
	}
	if r.Output != nil {
		if err := r.Output.Validate("attemptResult.output"); err != nil {
			return err
		}
	}
	if (r.Status == AttemptSucceeded || r.Status == AttemptSkippedTerminalExists) && !r.Terminal {
		return contractError("attemptResult.status requires terminal=true")
	}
	if r.Status == AttemptFailed || r.Status == AttemptPlatformMismatch {
		if r.Error == nil || r.Error.Code == "" || r.Error.Message == "" {
			return contractError("attemptResult.error requires code and message")
		}
	}
	return nil
}

// Validate checks output summary fields.
func (o AttemptOutput) Validate(path string) error {
	if o.StdoutBytes < 0 {
		return contractError("%s.stdoutBytes must be >= 0", path)
	}
	if o.StderrBytes < 0 {
		return contractError("%s.stderrBytes must be >= 0", path)
	}
	if o.StdoutPreviewBase64 == "" && o.StdoutTruncated {
		return contractError("%s.stdoutTruncated requires stdoutPreviewBase64", path)
	}
	if o.StderrPreviewBase64 == "" && o.StderrTruncated {
		return contractError("%s.stderrTruncated requires stderrPreviewBase64", path)
	}
	if err := validateBase64Preview(o.StdoutPreviewBase64, o.StdoutBytes, path+".stdoutPreviewBase64"); err != nil {
		return err
	}
	if err := validateBase64Preview(o.StderrPreviewBase64, o.StderrBytes, path+".stderrPreviewBase64"); err != nil {
		return err
	}
	return nil
}

func validateBase64Preview(value string, totalBytes int64, path string) error {
	if value == "" {
		return nil
	}
	data, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return contractError("%s must be base64", path)
	}
	if int64(len(data)) > totalBytes {
		return contractError("%s must not exceed total output bytes", path)
	}
	return nil
}

// Validate checks attempt timing fields.
func (t AttemptTimings) Validate(path string) error {
	if t.ArtifactPull < 0 {
		return contractError("%s.artifactPull must be >= 0", path)
	}
	if t.Materialization < 0 {
		return contractError("%s.materialization must be >= 0", path)
	}
	if t.Resume < 0 {
		return contractError("%s.resume must be >= 0", path)
	}
	if t.GuestReady < 0 {
		return contractError("%s.guestReady must be >= 0", path)
	}
	if t.ResultCommit < 0 {
		return contractError("%s.resultCommit must be >= 0", path)
	}
	return nil
}

func validateID(value, path string) error {
	if !idPattern.MatchString(value) {
		return contractError("%s must be a stable lowercase id", path)
	}
	return nil
}

func validateDigest(value, path string) error {
	if !digestPattern.MatchString(value) {
		return contractError("%s must be a sha256 digest", path)
	}
	return nil
}

func validateS3Prefix(value, path string) error {
	if !strings.HasPrefix(value, "s3://") || !strings.HasSuffix(value, "/") {
		return contractError("%s must be an s3 prefix URI ending in /", path)
	}
	parts := strings.SplitN(strings.TrimPrefix(value, "s3://"), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return contractError("%s must be an s3 prefix URI ending in /", path)
	}
	return nil
}

func validateS3JSONURI(value, path string) error {
	if !strings.HasPrefix(value, "s3://") || !strings.HasSuffix(value, ".json") {
		return contractError("%s must be an s3 JSON URI", path)
	}
	parts := strings.SplitN(strings.TrimPrefix(value, "s3://"), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return contractError("%s must be an s3 JSON URI", path)
	}
	return nil
}

func validPressure(level PressureLevel) bool {
	switch level {
	case PressureNormal, PressureWarning, PressureCritical:
		return true
	default:
		return false
	}
}

func validAttemptStatus(status AttemptStatus) bool {
	switch status {
	case AttemptSucceeded, AttemptFailed, AttemptSkippedTerminalExists, AttemptPlatformMismatch:
		return true
	default:
		return false
	}
}

func contractError(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidContract}, args...)...)
}
