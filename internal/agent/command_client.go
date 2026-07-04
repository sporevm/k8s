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
	"strconv"
	"strings"
	"sync"
	"syscall"
)

// CommandClient invokes the `spore` CLI machine interface.
type CommandClient struct {
	Path string
	Dir  string
	Env  []string
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

// RunCapture runs `spore run --capture` to prepare a warm parent.
func (c CommandClient) RunCapture(ctx context.Context, req RunCaptureRequest) ([]RunEvent, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	args := []string{"run", "--events=jsonl"}
	if req.Backend != "" {
		args = append(args, "--backend", req.Backend)
	}
	args = append(args, "--image", req.Image, "--capture", req.CaptureDir)
	if req.CaptureSignal != "" {
		args = append(args, "--capture-on", req.CaptureSignal)
	}
	args = append(args, "--")
	args = append(args, req.Command...)

	var stdout []byte
	var stderr []byte
	var err error
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

// Resume runs `spore resume --events=jsonl`.
func (c CommandClient) Resume(ctx context.Context, req ResumeRequest) ([]RunEvent, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	args := []string{"resume", "--events=jsonl"}
	if req.Backend != "" {
		args = append(args, "--backend", req.Backend)
	}
	if req.GenerationPath != "" {
		args = append(args, "--generation", req.GenerationPath)
	}
	args = append(args, req.SporeDir)
	if req.Name != "" {
		args = append(args, "--name", req.Name)
	}

	stdout, stderr, err := c.run(ctx, args...)
	events, _, decodeErr := decodeRunEventsAfterCommand("spore resume", stdout, stderr, err, false)
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
	args := []string{"exec", req.Name, "--"}
	args = append(args, req.Command...)

	stdout, stderr, err := c.run(ctx, args...)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if err != nil {
		if machineErr := machineErrorFromStderr(stderr); machineErr != nil {
			return nil, machineErr
		}
		if strings.HasPrefix(string(bytes.TrimSpace(stderr)), "spore exec:") {
			return nil, commandError(err, stderr)
		}
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			return nil, commandError(err, stderr)
		}
		return commandRunEvents("exec", stdout, stderr, exitErr.ExitCode()), nil
	}
	return commandRunEvents("exec", stdout, stderr, 0), nil
}

// RemoveVM runs `spore rm` against a named VM.
func (c CommandClient) RemoveVM(ctx context.Context, req RemoveVMRequest) error {
	if err := req.validate(); err != nil {
		return err
	}
	_, stderr, err := c.run(ctx, "rm", req.Name)
	if err != nil {
		return commandError(err, stderr)
	}
	return nil
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

func commandRunEvents(command string, stdout []byte, stderr []byte, exitCode int) []RunEvent {
	events := make([]RunEvent, 0, 3)
	if len(stdout) > 0 {
		events = append(events, commandOutputEvent(command, "stdout", stdout))
	}
	if len(stderr) > 0 {
		events = append(events, commandOutputEvent(command, "stderr", stderr))
	}
	events = append(events, RunEvent{
		Schema:        runEventsSchema,
		SchemaVersion: schemaVersion,
		Event:         "exit",
		Command:       command,
		ExitCode:      &exitCode,
	})
	return events
}

func commandOutputEvent(command string, event string, data []byte) RunEvent {
	return RunEvent{
		Schema:        runEventsSchema,
		SchemaVersion: schemaVersion,
		Event:         event,
		Command:       command,
		ByteCount:     len(data),
		DataBase64:    base64.StdEncoding.EncodeToString(data),
	}
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
	if terminal.Event == "failure" {
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
	if err := validateRunEventExitStatus(name, runErr, terminal); err != nil {
		return events, terminal, err
	}
	if guestExitIsError && terminal.ExitCode != nil && *terminal.ExitCode != 0 {
		return events, terminal, fmt.Errorf("%s guest exited with code %d", name, *terminal.ExitCode)
	}
	return events, terminal, nil
}

func validateRunEventExitStatus(name string, runErr error, terminal RunEvent) error {
	if terminal.Event != "exit" || terminal.ExitCode == nil {
		if runErr != nil {
			return runErr
		}
		return nil
	}
	if runErr == nil {
		if *terminal.ExitCode == 0 {
			return nil
		}
		return invalidMachineOutput("%s exited 0 but terminal event reported guest exit %d", name, *terminal.ExitCode)
	}
	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		if exitErr.ExitCode() == *terminal.ExitCode {
			return nil
		}
		return invalidMachineOutput("%s exit status %d does not match terminal event %d", name, exitErr.ExitCode(), *terminal.ExitCode)
	}
	return runErr
}
