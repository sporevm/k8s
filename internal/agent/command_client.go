package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
)

// CommandClient invokes the `spore` CLI machine interface.
type CommandClient struct {
	Path string
	Dir  string
	Env  []string
	// Region is the AWS region of s3:// image stores; empty uses the region
	// of the AWS default config chain.
	Region string
}

// Executable resolves the spore binary this client runs, the way exec does:
// a bare name through PATH and a relative path against Dir.
func (c CommandClient) Executable() (string, error) {
	path := c.Path
	if path == "" {
		path = "spore"
	}
	if !strings.Contains(path, string(filepath.Separator)) {
		return exec.LookPath(path)
	}
	if !filepath.IsAbs(path) && c.Dir != "" {
		return filepath.Join(c.Dir, path), nil
	}
	return path, nil
}

// Version runs `spore --json version` and returns the release version.
func (c CommandClient) Version(ctx context.Context) (string, error) {
	var result VersionResult
	if err := c.runJSON(ctx, &result, "--json", "version"); err != nil {
		return "", err
	}
	if err := result.Validate(); err != nil {
		return "", err
	}
	return result.Version, nil
}

// HostInfo runs `spore --json host-info`.
func (c CommandClient) HostInfo(ctx context.Context) (HostInfo, error) {
	var result HostInfo
	if err := c.runJSON(ctx, &result, "--json", "host-info"); err != nil {
		return HostInfo{}, err
	}
	return result, result.Validate()
}

// InspectBundle runs `spore --json inspect-bundle`.
func (c CommandClient) InspectBundle(ctx context.Context, req InspectBundleRequest) (InspectBundleResult, error) {
	if err := req.validate(); err != nil {
		return InspectBundleResult{}, err
	}
	args := []string{"--json", "inspect-bundle", req.Source}
	if req.ChildID != "" {
		args = append(args, "--child", req.ChildID)
	}
	if req.ChildRange != nil {
		args = append(args, "--child-range", strconv.Itoa(req.ChildRange.Start)+".."+strconv.Itoa(req.ChildRange.End))
	}

	var result InspectBundleResult
	if err := c.runJSON(ctx, &result, args...); err != nil {
		return InspectBundleResult{}, err
	}
	return result, result.Validate()
}

// Pull runs `spore --json pull`.
func (c CommandClient) Pull(ctx context.Context, req PullRequest) (PullResult, error) {
	if err := req.validate(); err != nil {
		return PullResult{}, err
	}
	args := []string{"--json", "pull", req.Source}
	if req.ChildID != "" {
		args = append(args, "--child", req.ChildID)
	}
	if req.AllowMetadataOnlyRootFS {
		args = append(args, "--allow-metadata-only-rootfs")
	}
	args = append(args, "--out", req.OutDir)
	if req.Region != "" {
		args = append(args, "--region", req.Region)
	}

	var result PullResult
	if err := c.runJSON(ctx, &result, args...); err != nil {
		return PullResult{}, err
	}
	return result, result.Validate()
}

// RunCapture runs `spore run --save` to prepare a warm parent.
func (c CommandClient) RunCapture(ctx context.Context, req RunCaptureRequest) ([]RunEvent, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	args := []string{"run", "--events=jsonl"}
	if req.Backend != "" {
		args = append(args, "--backend", req.Backend)
	}
	if req.Memory != "" {
		args = append(args, "--memory", req.Memory)
	}
	args = append(args, "--image", req.Image, "--save", req.CaptureDir)
	if req.CaptureSignal != "" {
		args = append(args, "--save-on", req.CaptureSignal)
	}
	args = append(args, "--")
	args = append(args, req.Command...)

	// c is this call's copy, so the credentials reach only this command.
	env, err := c.imageEnv(ctx, req.Image)
	if err != nil {
		return nil, err
	}
	c.Env = env

	var stdout []byte
	var stderr []byte
	if req.ReadyMarker != "" {
		stdout, stderr, err = c.runSignalingOnMarker(ctx, req.ReadyMarker, req.CaptureSignal, args...)
	} else {
		stdout, stderr, err = c.run(ctx, args...)
	}
	events, terminal, err := decodeRunEventsAfterCommand("spore run", stdout, stderr, err, true)
	if err != nil {
		return events, err
	}
	if !terminal.Captured || terminal.CapturePath == nil {
		return events, invalidMachineOutput("spore run capture did not report a captured parent")
	}
	return events, nil
}

// Fork runs `spore fork`.
func (c CommandClient) Fork(ctx context.Context, req ForkRequest) error {
	if err := req.validate(); err != nil {
		return err
	}
	_, stderr, err := c.run(ctx, "fork", req.ParentDir, "--count", strconv.Itoa(req.Count), "--out", req.OutDir)
	if err != nil {
		return commandError(err, stderr)
	}
	return nil
}

// Pack runs `spore pack`.
func (c CommandClient) Pack(ctx context.Context, req PackRequest) error {
	if err := req.validate(); err != nil {
		return err
	}
	_, stderr, err := c.run(ctx, "pack", req.ParentDir, "--children", req.ChildrenDir, "--out", req.OutDir)
	if err != nil {
		return commandError(err, stderr)
	}
	return nil
}

// Resume attaches or restores a saved spore session.
func (c CommandClient) Resume(ctx context.Context, req ResumeRequest) ([]RunEvent, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	commandName := "spore attach"
	args := []string{"attach", "--events=jsonl"}
	if req.Backend != "" {
		args = append(args, "--backend", req.Backend)
	}
	if req.GenerationPath != "" {
		args = append(args, "--generation", req.GenerationPath)
	}
	args = append(args, req.SporeDir)

	stdout, stderr, err := c.run(ctx, args...)
	events, _, decodeErr := decodeRunEventsAfterCommand(commandName, stdout, stderr, err, false)
	if decodeErr != nil {
		return events, decodeErr
	}
	return events, nil
}

// RestoreNamed runs `spore --json restore` and returns only after the VM is exec-ready.
func (c CommandClient) RestoreNamed(ctx context.Context, req RestoreNamedRequest) (NamedLifecycleResult, error) {
	if err := req.validate(); err != nil {
		return NamedLifecycleResult{}, err
	}
	args := []string{"--json", "restore", req.SporeDir, "--name", req.Name}
	if req.Backend != "" {
		args = append(args, "--backend", req.Backend)
	}
	if req.GenerationPath != "" {
		args = append(args, "--generation", req.GenerationPath)
	}
	var result NamedLifecycleResult
	if err := c.runJSON(ctx, &result, args...); err != nil {
		return NamedLifecycleResult{}, err
	}
	return result, result.validateRestored(req.Name)
}

// RunFrom runs one command from a saved spore.
func (c CommandClient) RunFrom(ctx context.Context, req RunFromRequest) ([]RunEvent, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	args := []string{"run", "--events=jsonl"}
	if req.Backend != "" {
		args = append(args, "--backend", req.Backend)
	}
	args = append(args, "--from", req.SporeDir)
	if req.GenerationPath != "" {
		args = append(args, "--generation", req.GenerationPath)
	}
	args = append(args, "--")
	args = append(args, req.Command...)

	stdout, stderr, err := c.run(ctx, args...)
	events, _, decodeErr := decodeRunEventsAfterCommand("spore run --from", stdout, stderr, err, false)
	if decodeErr != nil {
		return events, decodeErr
	}
	return events, nil
}

// Exec runs `spore exec` against a named resumed VM.
func (c CommandClient) Exec(ctx context.Context, req ExecRequest) ([]RunEvent, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	args := []string{"exec", "--events=jsonl", req.Name, "--"}
	args = append(args, req.Command...)

	stdout, stderr, err := c.run(ctx, args...)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	events, _, decodeErr := decodeRunEventsAfterCommand("spore exec", stdout, stderr, err, false)
	if decodeErr != nil {
		return events, decodeErr
	}
	return events, nil
}

// RemoveVM runs `spore rm` against a named VM.
func (c CommandClient) RemoveVM(ctx context.Context, req RemoveVMRequest) error {
	if err := req.validate(); err != nil {
		return err
	}
	_, stderr, err := c.run(ctx, "--json", "rm", req.Name)
	if err != nil {
		var machineErr *MachineError
		if errors.As(machineErrorFromStderr(stderr), &machineErr) && machineErr.Envelope.Error.Code == "object.not_found" {
			return nil
		}
		return commandError(err, stderr)
	}
	return nil
}

// RemoveSavedSpore runs `spore rm --spore`. It refuses only a spore a running
// VM restored with a saved-spore lease (pulled or unpacked spores); it removes
// a cache-pinned save such as a boot template even while a VM restored from it
// runs, since that VM's rootfs-cache lease roots the storage. SporeVM 0.17 also
// releases the durable cache pin; newer SporeVM leaves the pin and its storage
// to `spore cache gc --force`.
func (c CommandClient) RemoveSavedSpore(ctx context.Context, req RemoveSavedSporeRequest) error {
	if err := req.validate(); err != nil {
		return err
	}
	var result removedSavedSporeResult
	if err := c.runJSON(ctx, &result, "--json", "rm", "--spore", req.SporeDir); err != nil {
		return err
	}
	return result.Validate()
}

// CollectCache runs `spore --json cache gc --force`, which deletes cache
// storage and orphaned saved-spore pins that nothing references. It roots only
// the VM leases under this process's runtime directory, and holds the rootfs
// cache lock exclusively for its whole run.
func (c CommandClient) CollectCache(ctx context.Context) (CacheGCResult, error) {
	var result CacheGCResult
	if err := c.runJSON(ctx, &result, "--json", "cache", "gc", "--force"); err != nil {
		return CacheGCResult{}, err
	}
	return result, result.Validate()
}

func (c CommandClient) runJSON(ctx context.Context, out any, args ...string) error {
	stdout, stderr, err := c.run(ctx, args...)
	if err != nil {
		return commandError(err, stderr)
	}

	decoder := json.NewDecoder(bytes.NewReader(stdout))
	if err := decoder.Decode(out); err != nil {
		return fmt.Errorf("%w: decode JSON output: %v", ErrInvalidMachineOutput, err)
	}
	var extra struct{}
	if err := decoder.Decode(&extra); err == nil {
		return fmt.Errorf("%w: command emitted more than one JSON document", ErrInvalidMachineOutput)
	} else if !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: decode trailing JSON output: %v", ErrInvalidMachineOutput, err)
	}
	return nil
}

func (c CommandClient) run(ctx context.Context, args ...string) ([]byte, []byte, error) {
	cmd := c.command(ctx, args...)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = errors.Join(err, ctxErr)
		}
	}
	return stdout.Bytes(), stderr.Bytes(), err
}

func (c CommandClient) command(ctx context.Context, args ...string) *exec.Cmd {
	path := c.Path
	if path == "" {
		path = "spore"
	}
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Dir = c.Dir
	cmd.Env = c.Env
	return cmd
}

// s3ImageEnvKeys are the variables SporeVM reads to sign s3:// store reads.
var s3ImageEnvKeys = []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_REGION"}

// imageEnv returns the environment for a spore command that reads image.
// SporeVM signs s3:// store reads only with credentials and a region from its
// environment, so for those images it resolves them from the AWS default
// chain on every call, which picks up refreshed role credentials, and
// replaces any inherited values.
func (c CommandClient) imageEnv(ctx context.Context, image string) ([]string, error) {
	if !strings.HasPrefix(image, "s3://") {
		return c.Env, nil
	}
	var options []func(*config.LoadOptions) error
	if c.Region != "" {
		options = append(options, config.WithRegion(c.Region))
	}
	awsConfig, err := config.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration for s3:// image: %w", err)
	}
	if awsConfig.Region == "" {
		return nil, errors.New("s3:// image needs an AWS region: set --region or AWS_REGION")
	}
	credentials, err := awsConfig.Credentials.Retrieve(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve AWS credentials for s3:// image: %w", err)
	}

	env := c.Env
	if env == nil {
		env = os.Environ()
	}
	out := make([]string, 0, len(env)+len(s3ImageEnvKeys))
	for _, entry := range env {
		name, _, _ := strings.Cut(entry, "=")
		if !slices.Contains(s3ImageEnvKeys, name) {
			out = append(out, entry)
		}
	}
	out = append(out,
		"AWS_ACCESS_KEY_ID="+credentials.AccessKeyID,
		"AWS_SECRET_ACCESS_KEY="+credentials.SecretAccessKey,
		"AWS_REGION="+awsConfig.Region,
	)
	if credentials.SessionToken != "" {
		out = append(out, "AWS_SESSION_TOKEN="+credentials.SessionToken)
	}
	return out, nil
}

func (c CommandClient) runSignalingOnMarker(ctx context.Context, marker string, signalName string, args ...string) ([]byte, []byte, error) {
	sig, err := signalByName(signalName)
	if err != nil {
		return nil, nil, err
	}
	cmd := c.command(ctx, args...)
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, err
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, nil, err
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := cmd.Start(); err != nil {
		return nil, nil, err
	}

	var signalOnce sync.Once
	var signalErr error
	var signaled bool
	var signalMu sync.Mutex
	sendSignal := func() {
		signalOnce.Do(func() {
			signalMu.Lock()
			defer signalMu.Unlock()
			signaled = true
			signalErr = cmd.Process.Signal(sig)
		})
	}

	stdoutErr := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(stdoutPipe)
		for {
			line, err := reader.ReadBytes('\n')
			if len(line) > 0 {
				line = append([]byte(nil), line...)
				eventLine := bytes.TrimRight(line, "\r\n")
				stdout.Write(line)
				if runEventLineContains(eventLine, marker) {
					sendSignal()
				}
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					err = nil
				}
				stdoutErr <- err
				return
			}
		}
	}()

	stderrErr := make(chan error, 1)
	go func() {
		_, copyErr := io.Copy(&stderr, stderrPipe)
		stderrErr <- copyErr
	}()

	readErr := errors.Join(<-stdoutErr, <-stderrErr)
	waitErr := cmd.Wait()
	signalMu.Lock()
	wasSignaled := signaled
	err = signalErr
	signalMu.Unlock()
	if readErr != nil {
		waitErr = errors.Join(waitErr, readErr)
	}
	if err != nil {
		waitErr = errors.Join(waitErr, err)
	}
	if !wasSignaled {
		waitErr = errors.Join(waitErr, invalidMachineOutput("spore run ready marker %q was not observed", marker))
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		waitErr = errors.Join(waitErr, ctxErr)
	}
	return stdout.Bytes(), stderr.Bytes(), waitErr
}

func signalByName(name string) (os.Signal, error) {
	switch strings.TrimPrefix(name, "SIG") {
	case "USR1":
		return syscall.SIGUSR1, nil
	default:
		return nil, invalidSporeRequest("unsupported run capture signal %q", name)
	}
}

func runEventLineContains(line []byte, marker string) bool {
	if marker == "" {
		return false
	}
	if bytes.Contains(line, []byte(marker)) {
		return true
	}
	var event RunEvent
	if err := json.Unmarshal(line, &event); err != nil {
		return false
	}
	if event.DataBase64 == "" {
		return false
	}
	data, err := base64.StdEncoding.DecodeString(event.DataBase64)
	if err != nil {
		return false
	}
	return bytes.Contains(data, []byte(marker))
}

func commandError(err error, stderr []byte) error {
	if machineErr := machineErrorFromStderr(stderr); machineErr != nil {
		return machineErr
	}
	if len(stderr) == 0 {
		return err
	}
	return fmt.Errorf("%w: %s", err, bytes.TrimSpace(stderr))
}

func machineErrorFromStderr(stderr []byte) error {
	var envelope MachineErrorEnvelope
	if jsonErr := json.Unmarshal(bytes.TrimSpace(stderr), &envelope); jsonErr == nil {
		if validateErr := envelope.Validate(); validateErr == nil {
			return &MachineError{Envelope: envelope, Stderr: string(stderr)}
		}
	}
	return nil
}

func decodeRunEventsAfterCommand(name string, stdout []byte, stderr []byte, runErr error, guestExitIsError bool) ([]RunEvent, RunEvent, error) {
	events, decodeErr := DecodeRunEvents(bytes.NewReader(stdout))
	if decodeErr != nil {
		if runErr != nil {
			return nil, RunEvent{}, fmt.Errorf("%s failed before valid events: %w: %s", name, runErr, bytes.TrimSpace(stderr))
		}
		return nil, RunEvent{}, decodeErr
	}

	terminal, terminalErr := TerminalEvent(events)
	if terminalErr != nil {
		if runErr != nil {
			return nil, RunEvent{}, fmt.Errorf("%s failed without terminal event: %w: %s", name, runErr, bytes.TrimSpace(stderr))
		}
		return nil, RunEvent{}, terminalErr
	}
	if err := validateRunEventExitStatus(name, runErr, terminal); err != nil {
		return events, terminal, err
	}
	if terminal.Outcome == "failed" || terminal.Outcome == "canceled" {
		envelope := MachineErrorEnvelope{
			Schema:        errorSchema,
			SchemaVersion: schemaVersion,
			Error:         *terminal.Error,
		}
		if err := envelope.Validate(); err != nil {
			return events, terminal, err
		}
		return events, terminal, &MachineError{Envelope: envelope, Stderr: string(stderr)}
	}
	if guestExitIsError && terminal.ExitCode != nil && *terminal.ExitCode != 0 {
		return events, terminal, fmt.Errorf("%s guest exited with code %d", name, *terminal.ExitCode)
	}
	return events, terminal, nil
}

func validateRunEventExitStatus(name string, runErr error, terminal RunEvent) error {
	if terminal.Event != "completion" || terminal.ExitCode == nil {
		return invalidMachineOutput("%s terminal event is not a completion", name)
	}
	if runErr == nil {
		if *terminal.ExitCode == 0 {
			return nil
		}
		return invalidMachineOutput("%s exited 0 but completion reported exit %d", name, *terminal.ExitCode)
	}
	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		if exitErr.ExitCode() == *terminal.ExitCode {
			return nil
		}
		return invalidMachineOutput("%s exit status %d does not match completion %d", name, exitErr.ExitCode(), *terminal.ExitCode)
	}
	return runErr
}
