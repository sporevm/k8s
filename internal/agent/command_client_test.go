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

func TestCommandClientHostInfo(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "--json" ] && [ "$2" = "host-info" ]; then
  cat <<'JSON'
{
  "schema": "spore.host-info.v1",
  "schema_version": 1,
  "host_class": "linux-aarch64-kvm",
  "platform": {
    "os": "linux",
    "arch": "aarch64",
    "cpu_profile": "graviton1",
    "device_model_version": 0,
    "ram_base": 1073741824,
    "gic_dist_base": 134217728,
    "gic_redist_base": 134348800,
    "counter_frequency_source": "cntfrq_el0",
    "counter_frequency_hz": 24000000
  },
  "backends": [
    {"name": "kvm", "supported": true, "available": true, "reason": "available"},
    {"name": "hvf", "supported": false, "available": false, "reason": "unsupported_os_or_arch"}
  ],
  "cache_roots": {
    "kernels": {"path": "/cache/kernels", "resolved": true, "source": "environment"},
    "rootfs": {"path": "/cache/rootfs", "resolved": true, "source": "environment"},
    "bundles": {"path": "/cache/bundles", "resolved": true, "source": "environment"},
    "runtime": {"path": "/run/sporevm", "resolved": true, "source": "environment"}
  }
}
JSON
  exit 0
fi
echo unexpected "$*" >&2
exit 2
`, argsFile)

	client := CommandClient{Path: spore, Env: append(os.Environ(), "SPORE_ARGS_FILE="+argsFile)}
	info, err := client.HostInfo(context.Background())
	if err != nil {
		t.Fatalf("HostInfo: %v", err)
	}
	if info.HostClass != "linux-aarch64-kvm" {
		t.Fatalf("host class = %q", info.HostClass)
	}

	hostClass, err := info.FleetHostClass("kvm")
	if err != nil {
		t.Fatalf("FleetHostClass: %v", err)
	}
	if hostClass.ID != "linux-aarch64-kvm-graviton1-v0" || hostClass.Backend != "kvm" {
		t.Fatalf("fleet host class = %+v", hostClass)
	}

	args := readFile(t, argsFile)
	if strings.TrimSpace(args) != "--json host-info" {
		t.Fatalf("args = %q", args)
	}
}

func TestCommandClientPull(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "--json" ] && [ "$2" = "pull" ]; then
  cat <<'JSON'
{
  "schema": "spore.pull.result.v1",
  "schema_version": 1,
  "source": "s3://example-sporevm-artifacts/runs/ruby.bundle@sha256:1111111111111111111111111111111111111111111111111111111111111111",
  "bundle_dir": "/cache/bundles/ruby",
  "out_dir": "/work/child-42.spore",
  "bundle_digest": {"algorithm": "sha256", "hex": "1111111111111111111111111111111111111111111111111111111111111111"},
  "materialization": {
    "chunk_count": 10,
    "materialized_chunk_count": 10,
    "payload_bytes": 40960,
    "linked_chunk_count": 8,
    "copied_chunk_count": 2,
    "cache": {"hit_count": 8, "miss_count": 2, "bytes_fetched": 8192}
  },
  "rootfs": {
    "artifact_count": 1,
    "payload_bytes": 1048576,
    "cache": {"hit_count": 0, "miss_count": 1, "bytes_fetched": 1048576}
  },
  "remote": {"cache_hit": false, "origin_bytes_read": 8192, "peer_bytes_read": 0},
  "children": {"count": 1000, "selected_child": "42"}
}
JSON
  exit 0
fi
echo unexpected "$*" >&2
exit 2
`, argsFile)

	client := CommandClient{Path: spore, Env: append(os.Environ(), "SPORE_ARGS_FILE="+argsFile)}
	result, err := client.Pull(context.Background(), PullRequest{
		Source:                  "s3://example-sporevm-artifacts/runs/ruby.bundle@sha256:1111111111111111111111111111111111111111111111111111111111111111",
		ChildID:                 "42",
		OutDir:                  "/work/child-42.spore",
		Region:                  "us-east-1",
		AllowMetadataOnlyRootFS: true,
	})
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if result.BundleDigest.String() != "sha256:1111111111111111111111111111111111111111111111111111111111111111" {
		t.Fatalf("bundle digest = %q", result.BundleDigest.String())
	}
	if result.Children.SelectedChild == nil || *result.Children.SelectedChild != "42" {
		t.Fatalf("selected child = %v", result.Children.SelectedChild)
	}

	args := strings.TrimSpace(readFile(t, argsFile))
	want := "--json pull s3://example-sporevm-artifacts/runs/ruby.bundle@sha256:1111111111111111111111111111111111111111111111111111111111111111 --child 42 --allow-metadata-only-rootfs --out /work/child-42.spore --region us-east-1"
	if args != want {
		t.Fatalf("args = %q, want %q", args, want)
	}
}

func TestCommandClientInspectBundle(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "--json" ] && [ "$2" = "inspect-bundle" ]; then
  cat <<'JSON'
{
  "schema": "spore.bundle.inspect.v1",
  "schema_version": 1,
  "source": "file:///bundles/ruby",
  "bundle_dir": "/bundles/ruby",
  "bundle_digest": {"algorithm": "sha256", "hex": "1111111111111111111111111111111111111111111111111111111111111111"},
  "indexed": true,
  "parent_manifest": "manifests/parent.json",
  "chunkpack_index": "chunkpack.index.json",
  "chunkpack": {"chunk_count": 10, "pack_count": 1, "payload_bytes": 40960},
  "child_count": 1000,
  "children": [],
  "selection": {"kind": "range", "selected_count": 10, "children": []},
  "rootfs": {"artifact_count": 1, "exact_bytes_count": 1, "metadata_only_count": 0, "payload_bytes": 1048576}
}
JSON
  exit 0
fi
echo unexpected "$*" >&2
exit 2
`, argsFile)

	client := CommandClient{Path: spore, Env: append(os.Environ(), "SPORE_ARGS_FILE="+argsFile)}
	result, err := client.InspectBundle(context.Background(), InspectBundleRequest{
		Source:     "file:///bundles/ruby",
		ChildRange: &ChildRangeSelection{Start: 10, End: 20},
	})
	if err != nil {
		t.Fatalf("InspectBundle: %v", err)
	}
	if result.Selection.SelectedCount != 10 {
		t.Fatalf("selected count = %d", result.Selection.SelectedCount)
	}

	args := strings.TrimSpace(readFile(t, argsFile))
	if args != "--json inspect-bundle file:///bundles/ruby --child-range 10..20" {
		t.Fatalf("args = %q", args)
	}
}

func TestCommandClientRunCaptureSignalsOnReadyMarker(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "run" ]; then
  trap 'printf "%s\n" "{\"schema\":\"spore.run-events.v1\",\"schema_version\":1,\"event\":\"exit\",\"command\":\"run\",\"backend\":\"kvm\",\"exit_code\":0,\"vcpus\":1,\"memory_bytes\":536870912,\"captured\":true,\"capture_path\":\"/work/parent.spore\",\"timings\":{\"start_ms\":1,\"vsock_connect_ms\":2,\"exec_response_ms\":3,\"probe_duration_ms\":4}}"; exit 0' USR1
  cat <<'JSONL'
{"schema":"spore.run-events.v1","schema_version":1,"event":"start","command":"run","requested_backend":"kvm"}
{"schema":"spore.run-events.v1","schema_version":1,"event":"output","command":"run","backend":"kvm","data_base64":"U1BPUkVWTV9SQUlMU19SRUFEWQ=="}
JSONL
  while :; do sleep 1; done
fi
echo unexpected "$*" >&2
exit 2
`, argsFile)

	client := CommandClient{Path: spore, Env: append(os.Environ(), "SPORE_ARGS_FILE="+argsFile)}
	events, err := client.RunCapture(context.Background(), RunCaptureRequest{
		Image:         "example.com/sporevm/rails-rspec:sha-1111111",
		CaptureDir:    "/work/parent.spore",
		CaptureSignal: "USR1",
		ReadyMarker:   "SPOREVM_RAILS_READY",
		Backend:       "kvm",
		Memory:        "512mb",
		Command:       []string{"/bin/bash", "/usr/local/bin/sporevm-rails-coordinator"},
	})
	if err != nil {
		t.Fatalf("RunCapture: %v", err)
	}
	terminal, err := TerminalEvent(events)
	if err != nil {
		t.Fatalf("TerminalEvent: %v", err)
	}
	if !terminal.Captured || terminal.CapturePath == nil || *terminal.CapturePath != "/work/parent.spore" {
		t.Fatalf("terminal capture = %+v", terminal)
	}

	args := strings.TrimSpace(readFile(t, argsFile))
	want := "run --events=jsonl --backend kvm --memory 512mb --image example.com/sporevm/rails-rspec:sha-1111111 --capture /work/parent.spore --capture-on USR1 -- /bin/bash /usr/local/bin/sporevm-rails-coordinator"
	if args != want {
		t.Fatalf("args = %q, want %q", args, want)
	}
}

func TestCommandClientForkAndPack(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" >> "$SPORE_ARGS_FILE"
case "$1" in
  fork|pack) exit 0 ;;
esac
echo unexpected "$*" >&2
exit 2
`, argsFile)

	client := CommandClient{Path: spore, Env: append(os.Environ(), "SPORE_ARGS_FILE="+argsFile)}
	if err := client.Fork(context.Background(), ForkRequest{
		ParentDir: "/work/parent.spore",
		Count:     1000,
		OutDir:    "/work/children",
	}); err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if err := client.Pack(context.Background(), PackRequest{
		ParentDir:   "/work/parent.spore",
		ChildrenDir: "/work/children",
		OutDir:      "/work/bundle",
	}); err != nil {
		t.Fatalf("Pack: %v", err)
	}

	args := strings.TrimSpace(readFile(t, argsFile))
	want := "fork /work/parent.spore --count 1000 --out /work/children\npack /work/parent.spore --children /work/children --out /work/bundle"
	if args != want {
		t.Fatalf("args = %q, want %q", args, want)
	}
}

func TestCommandClientCreateVM(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "create" ]; then
  exit 0
fi
echo unexpected "$*" >&2
exit 2
	`, argsFile)

	client := CommandClient{Path: spore, Env: append(os.Environ(), "SPORE_ARGS_FILE="+argsFile)}
	if err := client.CreateVM(context.Background(), CreateVMRequest{
		Name:    "sporevm-hot-node",
		Image:   "docker.io/library/node:22-bookworm-slim",
		Backend: "kvm",
		Memory:  "512mb",
		Command: []string{"/bin/sh", "-lc", "node -v >/dev/null"},
	}); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	args := strings.TrimSpace(readFile(t, argsFile))
	want := "create sporevm-hot-node --backend kvm --memory 512mb --image docker.io/library/node:22-bookworm-slim -- /bin/sh -lc node -v >/dev/null"
	if args != want {
		t.Fatalf("args = %q, want %q", args, want)
	}
}

func TestCommandClientResumeTreatsGuestExitAsResult(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "resume" ]; then
  cat <<'JSONL'
{"schema":"spore.run-events.v1","schema_version":1,"event":"start","command":"resume","requested_backend":"kvm"}
{"schema":"spore.run-events.v1","schema_version":1,"event":"ready","command":"resume","backend":"kvm"}
{"schema":"spore.run-events.v1","schema_version":1,"event":"exit","command":"resume","backend":"kvm","exit_code":7,"vcpus":1,"memory_bytes":536870912,"captured":false,"capture_path":null,"timings":{"start_ms":1,"vsock_connect_ms":2,"exec_response_ms":3,"probe_duration_ms":4}}
JSONL
  exit 7
fi
echo unexpected "$*" >&2
exit 2
`, argsFile)

	client := CommandClient{Path: spore, Env: append(os.Environ(), "SPORE_ARGS_FILE="+argsFile)}
	events, err := client.Resume(context.Background(), ResumeRequest{
		SporeDir:       "/work/child-42.spore",
		Backend:        "kvm",
		GenerationPath: "/work/child-42.generation.json",
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	terminal, err := TerminalEvent(events)
	if err != nil {
		t.Fatalf("TerminalEvent: %v", err)
	}
	if terminal.ExitCode == nil || *terminal.ExitCode != 7 {
		t.Fatalf("exit code = %v", terminal.ExitCode)
	}

	args := strings.TrimSpace(readFile(t, argsFile))
	if args != "resume --events=jsonl --backend kvm --generation /work/child-42.generation.json /work/child-42.spore" {
		t.Fatalf("args = %q", args)
	}
}

func TestCommandClientResumeSupportsName(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "resume" ]; then
  printf '%s\n' '{"schema":"spore.run-events.v1","schema_version":1,"event":"exit","command":"resume","backend":"kvm","exit_code":0}'
  exit 0
fi
echo unexpected "$*" >&2
exit 2
`, argsFile)

	client := CommandClient{Path: spore, Env: append(os.Environ(), "SPORE_ARGS_FILE="+argsFile)}
	if _, err := client.Resume(context.Background(), ResumeRequest{
		SporeDir:       "/work/child-42.spore",
		Backend:        "kvm",
		GenerationPath: "/work/child-42.generation.json",
		Name:           "sporevm-child-42",
	}); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	args := strings.TrimSpace(readFile(t, argsFile))
	if args != "resume --events=jsonl --backend kvm --generation /work/child-42.generation.json /work/child-42.spore --name sporevm-child-42" {
		t.Fatalf("args = %q", args)
	}
}

func TestCommandClientExecTreatsGuestExitAsResult(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "exec" ]; then
  printf 'hello\n'
  printf 'warn\n' >&2
  exit 7
fi
echo unexpected "$*" >&2
exit 2
`, argsFile)

	client := CommandClient{Path: spore, Env: append(os.Environ(), "SPORE_ARGS_FILE="+argsFile)}
	events, err := client.Exec(context.Background(), ExecRequest{
		Name:    "sporevm-child-42",
		Command: []string{"/bin/sh", "-c", "exit 7"},
	})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	terminal, err := TerminalEvent(events)
	if err != nil {
		t.Fatalf("TerminalEvent: %v", err)
	}
	if terminal.ExitCode == nil || *terminal.ExitCode != 7 {
		t.Fatalf("exit code = %v", terminal.ExitCode)
	}
	if len(events) != 3 || events[0].Event != "stdout" || events[1].Event != "stderr" || events[2].Command != "exec" {
		t.Fatalf("events = %+v", events)
	}

	args := strings.TrimSpace(readFile(t, argsFile))
	if args != "exec sporevm-child-42 -- /bin/sh -c exit 7" {
		t.Fatalf("args = %q", args)
	}
}

func TestCommandClientExecReturnsSporeCommandError(t *testing.T) {
	spore := fakeSpore(t, `
if [ "$1" = "exec" ]; then
  echo "spore exec: VM is not ready: $2" >&2
  exit 2
fi
echo unexpected "$*" >&2
exit 2
`, "")

	client := CommandClient{Path: spore}
	events, err := client.Exec(context.Background(), ExecRequest{
		Name:    "sporevm-child-42",
		Command: []string{"/bin/true"},
	})
	if err == nil {
		t.Fatal("Exec succeeded")
	}
	if len(events) != 0 {
		t.Fatalf("events = %+v, want none", events)
	}
	if !strings.Contains(err.Error(), "VM is not ready") {
		t.Fatalf("Exec error = %v, want VM readiness error", err)
	}
}

func TestCommandClientRemoveVM(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "rm" ]; then
  exit 0
fi
echo unexpected "$*" >&2
exit 2
`, argsFile)

	client := CommandClient{Path: spore, Env: append(os.Environ(), "SPORE_ARGS_FILE="+argsFile)}
	if err := client.RemoveVM(context.Background(), RemoveVMRequest{Name: "sporevm-child-42"}); err != nil {
		t.Fatalf("RemoveVM: %v", err)
	}

	args := strings.TrimSpace(readFile(t, argsFile))
	if args != "rm sporevm-child-42" {
		t.Fatalf("args = %q", args)
	}
}

func TestCommandClientResumeRejectsExitMismatch(t *testing.T) {
	spore := fakeSpore(t, `
if [ "$1" = "resume" ]; then
  cat <<'JSONL'
{"schema":"spore.run-events.v1","schema_version":1,"event":"exit","command":"resume","backend":"kvm","exit_code":7,"vcpus":1,"memory_bytes":536870912,"captured":false,"capture_path":null,"timings":{"start_ms":1,"vsock_connect_ms":2,"exec_response_ms":3,"probe_duration_ms":4}}
JSONL
  exit 0
fi
exit 2
`, "")

	client := CommandClient{Path: spore}
	_, err := client.Resume(context.Background(), ResumeRequest{SporeDir: "/work/child-42.spore"})
	if !errors.Is(err, ErrInvalidMachineOutput) {
		t.Fatalf("Resume error = %v, want ErrInvalidMachineOutput", err)
	}
}

func TestCommandClientPreservesContextDeadline(t *testing.T) {
	spore := fakeSpore(t, `
if [ "$1" = "resume" ]; then
  sleep 30
fi
exit 2
`, "")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	client := CommandClient{Path: spore}
	_, err := client.Resume(ctx, ResumeRequest{SporeDir: "/work/child-42.spore"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Resume error = %v, want context deadline exceeded", err)
	}
}

func TestCommandClientReturnsMachineError(t *testing.T) {
	spore := fakeSpore(t, `
cat >&2 <<'JSON'
{
  "schema": "spore.error.v1",
  "schema_version": 1,
  "error": {
    "code": "host.unsupported",
    "message": "The host lacks a required capability.",
    "retryable": false,
    "scope": "host",
    "exit_code": 69,
    "source": "UnsupportedHost"
  }
}
JSON
exit 69
`, "")

	client := CommandClient{Path: spore}
	_, err := client.HostInfo(context.Background())
	var machineErr *MachineError
	if !errors.As(err, &machineErr) {
		t.Fatalf("HostInfo error = %T %v, want MachineError", err, err)
	}
	if machineErr.Envelope.Error.Code != "host.unsupported" {
		t.Fatalf("error code = %q", machineErr.Envelope.Error.Code)
	}
}

func TestCommandClientRejectsInvalidRequestsBeforeExec(t *testing.T) {
	client := CommandClient{Path: "/path/that/should/not/run"}

	if _, err := client.Pull(context.Background(), PullRequest{}); !errors.Is(err, ErrInvalidSporeRequest) {
		t.Fatalf("Pull error = %v, want ErrInvalidSporeRequest", err)
	}
	if _, err := client.InspectBundle(context.Background(), InspectBundleRequest{
		Source:     "file:///bundle",
		ChildID:    "1",
		ChildRange: &ChildRangeSelection{Start: 1, End: 2},
	}); !errors.Is(err, ErrInvalidSporeRequest) {
		t.Fatalf("InspectBundle error = %v, want ErrInvalidSporeRequest", err)
	}
	if _, err := client.InspectBundle(context.Background(), InspectBundleRequest{
		Source:     "file:///bundle",
		ChildRange: &ChildRangeSelection{Start: -1, End: 2},
	}); !errors.Is(err, ErrInvalidSporeRequest) {
		t.Fatalf("InspectBundle negative range error = %v, want ErrInvalidSporeRequest", err)
	}
	if _, err := client.Resume(context.Background(), ResumeRequest{}); !errors.Is(err, ErrInvalidSporeRequest) {
		t.Fatalf("Resume error = %v, want ErrInvalidSporeRequest", err)
	}
	if _, err := client.Exec(context.Background(), ExecRequest{}); !errors.Is(err, ErrInvalidSporeRequest) {
		t.Fatalf("Exec error = %v, want ErrInvalidSporeRequest", err)
	}
	if err := client.CreateVM(context.Background(), CreateVMRequest{}); !errors.Is(err, ErrInvalidSporeRequest) {
		t.Fatalf("CreateVM error = %v, want ErrInvalidSporeRequest", err)
	}
	if err := client.RemoveVM(context.Background(), RemoveVMRequest{}); !errors.Is(err, ErrInvalidSporeRequest) {
		t.Fatalf("RemoveVM error = %v, want ErrInvalidSporeRequest", err)
	}
	if _, err := client.RunCapture(context.Background(), RunCaptureRequest{}); !errors.Is(err, ErrInvalidSporeRequest) {
		t.Fatalf("RunCapture error = %v, want ErrInvalidSporeRequest", err)
	}
	if err := client.Fork(context.Background(), ForkRequest{}); !errors.Is(err, ErrInvalidSporeRequest) {
		t.Fatalf("Fork error = %v, want ErrInvalidSporeRequest", err)
	}
	if err := client.Pack(context.Background(), PackRequest{}); !errors.Is(err, ErrInvalidSporeRequest) {
		t.Fatalf("Pack error = %v, want ErrInvalidSporeRequest", err)
	}
}

func fakeSpore(t *testing.T, body string, argsFile string) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "spore")
	script := "#!/bin/sh\nset -eu\n"
	if argsFile == "" {
		script += "SPORE_ARGS_FILE=${SPORE_ARGS_FILE:-/dev/null}\n"
	}
	script += body
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake spore: %v", err)
	}
	return path
}

func readFile(t *testing.T, path string) string {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}
