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

func TestCommandClientHostInfo(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "--json" ] && [ "$2" = "system" ] && [ "$3" = "info" ]; then
  cat <<'JSON'
{
  "schema": "spore.host-info.v3",
  "schema_version": 3,
  "host_class": "linux-arm64-kvm",
  "architecture": "arm64",
  "platform": {"arm64": {
    "os": "linux",
    "cpu_profile": "sporevm-aarch64-v0",
    "device_model_version": 4,
    "ram_base": 1073741824,
    "interrupt_controller": {"kind":"gicv3","distributor_base":134217728,"redistributor_base":134348800},
    "counter": {"source":"cntfrq_el0","frequency_hz":24000000}
  }},
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
	if info.HostClass != "linux-arm64-kvm" || info.Platform.Arch != "aarch64" {
		t.Fatalf("host class = %q", info.HostClass)
	}

	hostClass, err := info.FleetHostClass("kvm")
	if err != nil {
		t.Fatalf("FleetHostClass: %v", err)
	}
	if hostClass.ID != "linux-arm64-kvm-sporevm-aarch64-v0-v4" || hostClass.Backend != "kvm" {
		t.Fatalf("fleet host class = %+v", hostClass)
	}

	args := readFile(t, argsFile)
	if strings.TrimSpace(args) != "--json system info" {
		t.Fatalf("args = %q", args)
	}
}

func TestHostInfoV3NormalizesARM64(t *testing.T) {
	data := []byte(`{
  "schema":"spore.host-info.v3",
  "schema_version":3,
  "host_class":"linux-arm64-kvm",
  "architecture":"arm64",
  "platform":{"arm64":{
    "os":"linux",
    "cpu_profile":"sporevm-aarch64-v0",
    "device_model_version":4,
    "ram_base":2147483648,
    "interrupt_controller":{"kind":"gicv3","distributor_base":134217728,"redistributor_base":134348800},
    "counter":{"source":"cntfrq_el0","frequency_hz":24000000}
  }},
  "backends":[{"name":"kvm","supported":true,"available":true,"reason":"available"}],
  "cache_roots":{}
}`)

	var info HostInfo
	if err := json.Unmarshal(data, &info); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if err := info.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if info.Platform.Arch != "aarch64" || info.Platform.GICDistBase != 134217728 || info.Platform.CounterFrequencyHz != 24000000 {
		t.Fatalf("normalized platform = %+v", info.Platform)
	}
	hostClass, err := info.FleetHostClass("kvm")
	if err != nil {
		t.Fatalf("FleetHostClass: %v", err)
	}
	if hostClass.Architecture != "aarch64" || hostClass.DeviceModel != "sporevm-aarch64-v4" {
		t.Fatalf("fleet host class = %+v", hostClass)
	}
}

func TestHostInfoV3RejectsUnsupportedAMD64FleetClass(t *testing.T) {
	data := []byte(`{
  "schema":"spore.host-info.v3",
  "schema_version":3,
  "host_class":"linux-amd64-kvm",
  "architecture":"amd64",
  "platform":{"amd64":{
    "os":"linux",
    "board_profile":"sporevm-x86_64-board-v0",
    "cpu_profile":"sporevm-x86_64-v0",
    "cpu_profile_status":"approved_same_host",
    "device_model_version":1,
    "ram":{"base":0}
  }},
  "backends":[{"name":"kvm","supported":true,"available":true,"reason":"available"}],
  "cache_roots":{}
}`)

	var info HostInfo
	if err := json.Unmarshal(data, &info); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if info.Platform.Arch != "x86_64" {
		t.Fatalf("normalized architecture = %q", info.Platform.Arch)
	}
	if _, err := info.FleetHostClass("kvm"); !errors.Is(err, ErrInvalidMachineOutput) {
		t.Fatalf("FleetHostClass error = %v, want ErrInvalidMachineOutput", err)
	}
}

func TestCommandClientPull(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "--json" ] && [ "$2" = "checkpoint" ] && [ "$3" = "pull" ]; then
  cat <<'JSON'
{
  "schema": "spore.pull.result.v2",
  "schema_version": 2,
  "ref": "s3://example-sporevm-artifacts/runs/blake3:1111111111111111111111111111111111111111111111111111111111111111",
  "out_dir": "/work/child-42.spore",
  "spore_digest": "blake3:1111111111111111111111111111111111111111111111111111111111111111",
  "ram": {"objects": 10, "objects_fetched": 2, "bytes_fetched": 8192},
  "disk": {"objects": 3, "objects_fetched": 1, "bytes_fetched": 1048576}
}
JSON
  exit 0
fi
echo unexpected "$*" >&2
exit 2
`, argsFile)

	client := CommandClient{Path: spore, Env: append(os.Environ(), "SPORE_ARGS_FILE="+argsFile)}
	result, err := client.Pull(context.Background(), PullRequest{
		Source: "s3://example-sporevm-artifacts/runs/blake3:1111111111111111111111111111111111111111111111111111111111111111",
		OutDir: "/work/child-42.spore",
	})
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if result.SporeDigest != "blake3:1111111111111111111111111111111111111111111111111111111111111111" {
		t.Fatalf("checkpoint digest = %q", result.SporeDigest)
	}
	if result.RAM.ObjectsFetched != 2 || result.Disk.BytesFetched != 1048576 {
		t.Fatalf("transfer = %+v", result)
	}

	args := strings.TrimSpace(readFile(t, argsFile))
	want := "--json checkpoint pull s3://example-sporevm-artifacts/runs/blake3:1111111111111111111111111111111111111111111111111111111111111111 --output /work/child-42.spore"
	if args != want {
		t.Fatalf("args = %q, want %q", args, want)
	}
}

func TestCommandClientInspectCheckpoint(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	parent := t.TempDir()
	if err := os.WriteFile(filepath.Join(parent, "manifest.json"), []byte(`{"generation":{"generation":17}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "--json" ] && [ "$2" = "checkpoint" ] && [ "$3" = "inspect" ]; then
  cat <<'JSON'
{
  "schema": "spore.inspect.result.v1",
  "schema_version": 1,
  "version": 8,
  "vm_state_present": true,
  "can_run_from": true,
  "can_attach": false,
  "platform": {"arch": "aarch64", "cpu_profile": "graviton1", "counter_frequency_hz": 24000000}
}
JSON
  exit 0
fi
echo unexpected "$*" >&2
exit 2
`, argsFile)

	client := CommandClient{Path: spore, Env: append(os.Environ(), "SPORE_ARGS_FILE="+argsFile)}
	result, err := client.InspectCheckpoint(context.Background(), InspectCheckpointRequest{
		Source: parent,
	})
	if err != nil {
		t.Fatalf("InspectBundle: %v", err)
	}
	if result.CanAttach || result.Platform.CounterFrequencyHz != 24000000 || result.Generation != 17 {
		t.Fatalf("checkpoint = %+v", result)
	}

	args := strings.TrimSpace(readFile(t, argsFile))
	if args != "--json checkpoint inspect "+parent {
		t.Fatalf("args = %q", args)
	}
}

func TestCommandClientRunCaptureSignalsOnReadyMarker(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "--json" ] && [ "$2" = "run" ]; then
  trap 'printf "%s\n" "{\"schema\":\"spore.automation.event.v1\",\"schema_version\":1,\"event\":\"completion\",\"outcome\":\"completed\",\"command\":\"run\",\"backend\":\"kvm\",\"exit_code\":0,\"vcpus\":1,\"memory_bytes\":536870912,\"max_memory_bytes\":536870912,\"captured\":true,\"capture_path\":\"/work/parent.spore\",\"timings\":{\"start_ms\":1,\"vsock_connect_ms\":2,\"exec_response_ms\":3,\"probe_duration_ms\":4}}"; exit 0' USR1
  cat <<'JSONL'
{"schema":"spore.automation.event.v1","schema_version":1,"event":"start","command":"run","requested_backend":"kvm"}
{"schema":"spore.automation.event.v1","schema_version":1,"event":"stdout","command":"run","backend":"kvm","data_base64":"U1BPUkVWTV9SQUlMU19SRUFEWQ=="}
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
	want := "--json run --backend kvm --memory 512mb --exec-timeout none --image example.com/sporevm/rails-rspec:sha-1111111 --save-checkpoint /work/parent.spore --save-on USR1 -- /bin/bash /usr/local/bin/sporevm-rails-coordinator"
	if args != want {
		t.Fatalf("args = %q, want %q", args, want)
	}
}

func TestCommandClientVersion(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "--json" ] && [ "$2" = "version" ]; then
  cat <<'JSON'
{
  "schema": "spore.version.result.v1",
  "schema_version": 1,
  "version": "0.17.1",
  "build_mode": "ReleaseSafe"
}
JSON
  exit 0
fi
echo unexpected "$*" >&2
exit 2
`, argsFile)

	client := CommandClient{Path: spore, Env: append(os.Environ(), "SPORE_ARGS_FILE="+argsFile)}
	version, err := client.Version(context.Background())
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if version != "0.17.1" {
		t.Fatalf("version = %q", version)
	}
	if args := strings.TrimSpace(readFile(t, argsFile)); args != "--json version" {
		t.Fatalf("args = %q", args)
	}
}

func TestCommandClientRunCaptureOnGuestExit(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "--json" ] && [ "$2" = "run" ]; then
  printf '%s\n' '{"schema":"spore.automation.event.v1","schema_version":1,"event":"completion","outcome":"completed","command":"run","backend":"kvm","exit_code":0,"captured":true,"capture_path":"/work/template.spore"}'
  exit 0
fi
echo unexpected "$*" >&2
exit 2
`, argsFile)

	client := CommandClient{Path: spore, Env: append(os.Environ(), "SPORE_ARGS_FILE="+argsFile)}
	_, err := client.RunCapture(context.Background(), RunCaptureRequest{
		Image:      interactiveTestImage,
		CaptureDir: "/work/template.spore",
		Backend:    "kvm",
		Memory:     "512mb",
		Command:    []string{"/bin/true"},
	})
	if err != nil {
		t.Fatalf("RunCapture: %v", err)
	}
	want := "--json run --backend kvm --memory 512mb --exec-timeout none --image " + interactiveTestImage + " --save-checkpoint /work/template.spore -- /bin/true"
	if args := strings.TrimSpace(readFile(t, argsFile)); args != want {
		t.Fatalf("args = %q, want %q", args, want)
	}
}

func TestCommandClientForkAndPack(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" >> "$SPORE_ARGS_FILE"
case "$*" in
  '--json checkpoint fork '*) exit 0 ;;
  '--json checkpoint push '*) printf '%s\n' '{"schema":"spore.push.result.v2","schema_version":2,"digest_ref":"file:///work/store/blake3:1111111111111111111111111111111111111111111111111111111111111111","spore_digest":"blake3:1111111111111111111111111111111111111111111111111111111111111111"}'; exit 0 ;;
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
	if _, err := client.Push(context.Background(), PushRequest{
		ParentDir: "/work/parent.spore",
		To:        "file:///work/store/parent",
	}); err != nil {
		t.Fatalf("Pack: %v", err)
	}

	args := strings.TrimSpace(readFile(t, argsFile))
	want := "--json checkpoint fork /work/parent.spore --count 1000 --output /work/children\n--json checkpoint push /work/parent.spore --to file:///work/store/parent"
	if args != want {
		t.Fatalf("args = %q, want %q", args, want)
	}
}

func TestCommandClientResumeTreatsGuestExitAsResult(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "--json" ] && [ "$2" = "checkpoint" ] && [ "$3" = "resume" ]; then
  cat <<'JSONL'
{"schema":"spore.automation.event.v1","schema_version":1,"event":"start","command":"attach","requested_backend":"kvm"}
{"schema":"spore.automation.event.v1","schema_version":1,"event":"ready","command":"attach","backend":"kvm"}
{"schema":"spore.automation.event.v1","schema_version":1,"event":"completion","outcome":"completed","command":"attach","backend":"kvm","exit_code":7,"vcpus":1,"memory_bytes":536870912,"max_memory_bytes":536870912,"captured":false,"capture_path":null,"timings":{"start_ms":1,"vsock_connect_ms":2,"exec_response_ms":3,"probe_duration_ms":4}}
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
	if args != "--json checkpoint resume --backend kvm --generation /work/child-42.generation.json /work/child-42.spore" {
		t.Fatalf("args = %q", args)
	}
}

func TestCommandClientRestoreNamedWaitsForReadyLifecycle(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "--json" ] && [ "$2" = "vm" ] && [ "$3" = "restore" ]; then
  printf '%s\n' '{"schema":"spore.lifecycle.v1","schema_version":1,"action":"restored","name":"sporevm-child-42","state":"ready","timing":{"prepare_ms":3,"spawn_monitor_ms":4,"wait_exec_ready_ms":17,"total_ms":24}}'
  exit 0
fi
echo unexpected "$*" >&2
exit 2
`, argsFile)

	client := CommandClient{Path: spore, Env: append(os.Environ(), "SPORE_ARGS_FILE="+argsFile)}
	result, err := client.RestoreNamed(context.Background(), RestoreNamedRequest{
		SporeDir:       "/work/child-42.spore",
		Backend:        "kvm",
		GenerationPath: "/work/child-42.generation.json",
		Name:           "sporevm-child-42",
	})
	if err != nil {
		t.Fatalf("RestoreNamed: %v", err)
	}
	if result.Timing == nil || result.Timing.WaitExecReadyMS != 17 || result.Timing.TotalMS != 24 {
		t.Fatalf("result = %+v", result)
	}

	args := strings.TrimSpace(readFile(t, argsFile))
	if args != "--json vm restore sporevm-child-42 --checkpoint /work/child-42.spore --backend kvm --generation /work/child-42.generation.json" {
		t.Fatalf("args = %q", args)
	}
}

func TestCommandClientRestoreNamedRejectsNonReadyLifecycle(t *testing.T) {
	spore := fakeSpore(t, `
if [ "$1" = "--json" ] && [ "$2" = "vm" ] && [ "$3" = "restore" ]; then
  printf '%s\n' '{"schema":"spore.lifecycle.v1","schema_version":1,"action":"restored","name":"sporevm-child-42","state":"starting","timing":{"prepare_ms":3,"spawn_monitor_ms":4,"wait_exec_ready_ms":0,"total_ms":7}}'
  exit 0
fi
exit 2
`, "")

	client := CommandClient{Path: spore}
	_, err := client.RestoreNamed(context.Background(), RestoreNamedRequest{
		SporeDir: "/work/child-42.spore",
		Name:     "sporevm-child-42",
	})
	if !errors.Is(err, ErrInvalidMachineOutput) {
		t.Fatalf("RestoreNamed error = %v, want ErrInvalidMachineOutput", err)
	}
}

func TestCommandClientRunFromTreatsGuestExitAsResult(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "--json" ] && [ "$2" = "run" ]; then
  cat <<'JSONL'
{"schema":"spore.automation.event.v1","schema_version":1,"event":"start","command":"run","requested_backend":"kvm"}
{"schema":"spore.automation.event.v1","schema_version":1,"event":"ready","command":"run","backend":"kvm"}
{"schema":"spore.automation.event.v1","schema_version":1,"event":"completion","outcome":"completed","command":"run","backend":"kvm","exit_code":7,"vcpus":1,"memory_bytes":536870912,"max_memory_bytes":536870912,"captured":false,"capture_path":null,"timings":{"start_ms":1,"vsock_connect_ms":2,"exec_response_ms":3,"probe_duration_ms":4}}
JSONL
  exit 7
fi
echo unexpected "$*" >&2
exit 2
`, argsFile)

	client := CommandClient{Path: spore, Env: append(os.Environ(), "SPORE_ARGS_FILE="+argsFile)}
	events, err := client.RunFrom(context.Background(), RunFromRequest{
		SporeDir:       "/work/child-42.spore",
		Backend:        "kvm",
		GenerationPath: "/work/child-42.generation.json",
		Command:        []string{"/bin/sh", "-c", "exit 7"},
	})
	if err != nil {
		t.Fatalf("RunFrom: %v", err)
	}
	terminal, err := TerminalEvent(events)
	if err != nil {
		t.Fatalf("TerminalEvent: %v", err)
	}
	if terminal.ExitCode == nil || *terminal.ExitCode != 7 {
		t.Fatalf("exit code = %v", terminal.ExitCode)
	}

	args := strings.TrimSpace(readFile(t, argsFile))
	if args != "--json run --backend kvm --exec-timeout none --checkpoint /work/child-42.spore --generation /work/child-42.generation.json -- /bin/sh -c exit 7" {
		t.Fatalf("args = %q", args)
	}
}

func TestCommandClientRunFromWithoutGeneration(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "--json" ] && [ "$2" = "run" ]; then
  printf '%s\n' '{"schema":"spore.automation.event.v1","schema_version":1,"event":"completion","outcome":"completed","command":"run","backend":"kvm","exit_code":0}'
  exit 0
fi
echo unexpected "$*" >&2
exit 2
`, argsFile)

	client := CommandClient{Path: spore, Env: append(os.Environ(), "SPORE_ARGS_FILE="+argsFile)}
	_, err := client.RunFrom(context.Background(), RunFromRequest{
		SporeDir: "/work/template.spore",
		Backend:  "kvm",
		Command:  []string{"/bin/sh", "-lc", "node -v"},
	})
	if err != nil {
		t.Fatalf("RunFrom: %v", err)
	}
	want := "--json run --backend kvm --exec-timeout none --checkpoint /work/template.spore -- /bin/sh -lc node -v"
	if args := strings.TrimSpace(readFile(t, argsFile)); args != want {
		t.Fatalf("args = %q, want %q", args, want)
	}
}

func TestCommandClientExecTreatsGuestExitAsResult(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "--json" ] && [ "$2" = "vm" ] && [ "$3" = "exec" ]; then
  cat <<'JSONL'
{"schema":"spore.automation.event.v1","schema_version":1,"event":"stdout","command":"exec","backend":"kvm","offset":0,"byte_count":6,"data_base64":"aGVsbG8K"}
{"schema":"spore.automation.event.v1","schema_version":1,"event":"stderr","command":"exec","backend":"kvm","offset":0,"byte_count":5,"data_base64":"d2Fybgo="}
{"schema":"spore.automation.event.v1","schema_version":1,"event":"completion","outcome":"completed","command":"exec","backend":"kvm","exit_code":7}
JSONL
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
	if args != "--json vm exec sporevm-child-42 --exec-timeout none -- /bin/sh -c exit 7" {
		t.Fatalf("args = %q", args)
	}
}

func TestCommandClientExecReturnsSporeCommandError(t *testing.T) {
	spore := fakeSpore(t, `
if [ "$1" = "--json" ] && [ "$2" = "vm" ] && [ "$3" = "exec" ]; then
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
if [ "$1" = "--json" ] && [ "$2" = "vm" ] && [ "$3" = "rm" ]; then
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
	if args != "--json vm rm sporevm-child-42" {
		t.Fatalf("args = %q", args)
	}
}

func TestCommandClientRemoveVMIgnoresMissingVM(t *testing.T) {
	spore := fakeSpore(t, `
if [ "$1" = "--json" ] && [ "$2" = "vm" ] && [ "$3" = "rm" ]; then
  printf '%s\n' '{"schema":"spore.error.v1","schema_version":1,"error":{"code":"object.not_found","message":"VM not found","retry":"after_fix","retryable":false,"scope":"object","exit_code":22,"source":"rm"}}' >&2
  exit 1
fi
exit 2
`, "")

	client := CommandClient{Path: spore}
	if err := client.RemoveVM(context.Background(), RemoveVMRequest{Name: "missing-vm"}); err != nil {
		t.Fatalf("RemoveVM: %v", err)
	}
}

func TestCommandClientRemoveSavedSpore(t *testing.T) {
	for _, tc := range []struct {
		name    string
		output  string
		wantErr bool
	}{
		{
			name:    "v0.17 result",
			output:  `{"schema":"spore.saved.remove.result.v1","schema_version":1,"resource_type":"checkpoint","action":"removed_spore","spore_dir":"/work/template.spore","ownership":"machine-local-pinned","pin_id":"pin-1","pin_removed":true}`,
			wantErr: true,
		},
		{
			name:   "result without pin fields",
			output: `{"schema":"spore.saved.remove.result.v2","schema_version":2,"resource_type":"checkpoint","action":"removed_spore","spore_dir":"/work/template.spore"}`,
		},
		{
			name:    "mismatched schema version",
			output:  `{"schema":"spore.saved.remove.result.v2","schema_version":1,"action":"removed_spore","spore_dir":"/work/template.spore"}`,
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			argsFile := filepath.Join(t.TempDir(), "args")
			spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
if [ "$1" = "--json" ] && [ "$2" = "checkpoint" ] && [ "$3" = "rm" ]; then
  printf '%s\n' "$SPORE_RM_OUTPUT"
  exit 0
fi
echo unexpected "$*" >&2
exit 2
`, argsFile)

			client := CommandClient{Path: spore, Env: append(os.Environ(), "SPORE_ARGS_FILE="+argsFile, "SPORE_RM_OUTPUT="+tc.output)}
			err := client.RemoveSavedSpore(context.Background(), RemoveSavedSporeRequest{SporeDir: "/work/template.spore"})
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidMachineOutput) {
					t.Fatalf("RemoveSavedSpore error = %v, want ErrInvalidMachineOutput", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("RemoveSavedSpore: %v", err)
			}

			args := strings.TrimSpace(readFile(t, argsFile))
			if args != "--json checkpoint rm /work/template.spore" {
				t.Fatalf("args = %q", args)
			}
		})
	}
}

func TestCommandClientCollectCache(t *testing.T) {
	for _, tc := range []struct {
		name    string
		output  string
		wantErr bool
	}{
		{
			name:    "v0.17 result",
			output:  `{"schema":"spore.cache.gc.result.v1","schema_version":1,"cache_root":"/var/lib/sporevm/rootfs-cache","dry_run":false,"unparseable_ref_count":0,"legacy_step_record_count":0,"candidate_count":3,"candidate_bytes":4096,"deleted_count":3,"deleted_bytes":4096,"entries":[]}`,
			wantErr: true,
		},
		{
			name:   "v2 result",
			output: `{"schema":"spore.cache.gc.result.v2","schema_version":2,"cache_root":"/var/lib/sporevm/rootfs-cache","dry_run":false,"candidate_count":3,"candidate_bytes":4096,"deleted_count":3,"deleted_bytes":4096,"entries":[]}`,
		},
		{
			name:    "dry run",
			output:  `{"schema":"spore.cache.gc.result.v2","schema_version":2,"cache_root":"/var/lib/sporevm/rootfs-cache","dry_run":true,"deleted_count":0,"deleted_bytes":0,"entries":[]}`,
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			argsFile := filepath.Join(t.TempDir(), "args")
			spore := fakeSpore(t, `
printf '%s\n' "$*" > "$SPORE_ARGS_FILE"
printf '%s\n' "$SPORE_GC_OUTPUT"
`, argsFile)

			client := CommandClient{Path: spore, Env: append(os.Environ(), "SPORE_ARGS_FILE="+argsFile, "SPORE_GC_OUTPUT="+tc.output)}
			result, err := client.CollectCache(context.Background())
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidMachineOutput) {
					t.Fatalf("CollectCache error = %v, want ErrInvalidMachineOutput", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("CollectCache: %v", err)
			}
			if result.DeletedCount != 3 || result.DeletedBytes != 4096 {
				t.Fatalf("result = %+v", result)
			}
			if args := strings.TrimSpace(readFile(t, argsFile)); args != "--json system gc --force" {
				t.Fatalf("args = %q", args)
			}
		})
	}
}

func TestCommandClientExecutableResolvesLikeExec(t *testing.T) {
	bin := t.TempDir()
	spore := filepath.Join(bin, "spore")
	if err := os.WriteFile(spore, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write spore: %v", err)
	}
	t.Setenv("PATH", bin)

	if path, err := (CommandClient{}).Executable(); err != nil || path != spore {
		t.Fatalf("Executable() = %q, %v; want %q from PATH", path, err, spore)
	}
	if path, err := (CommandClient{Path: "bin/spore", Dir: "/work"}).Executable(); err != nil || path != "/work/bin/spore" {
		t.Fatalf("Executable() = %q, %v; want the relative path under Dir", path, err)
	}
}

func TestCommandClientResumeRejectsExitMismatch(t *testing.T) {
	spore := fakeSpore(t, `
if [ "$1" = "--json" ] && [ "$2" = "checkpoint" ] && [ "$3" = "resume" ]; then
  cat <<'JSONL'
{"schema":"spore.automation.event.v1","schema_version":1,"event":"completion","outcome":"completed","command":"attach","backend":"kvm","exit_code":7,"vcpus":1,"memory_bytes":536870912,"max_memory_bytes":536870912,"captured":false,"capture_path":null,"timings":{"start_ms":1,"vsock_connect_ms":2,"exec_response_ms":3,"probe_duration_ms":4}}
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

func TestCommandClientPreservesFailedAndCanceledCompletion(t *testing.T) {
	tests := []struct {
		name     string
		outcome  string
		code     string
		retry    string
		exitCode int
	}{
		{name: "failed", outcome: "failed", code: "runtime.execution_failed", retry: "unknown", exitCode: 1},
		{name: "canceled", outcome: "canceled", code: "operation.canceled", retry: "unknown", exitCode: 130},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			completion := `{"schema":"spore.automation.event.v1","schema_version":1,"event":"completion","outcome":"` + tt.outcome + `","command":"run","backend":"kvm","exit_code":` + fmt.Sprint(tt.exitCode) + `,"error":{"code":"` + tt.code + `","message":"classified failure","retry":"` + tt.retry + `","retryable":false,"scope":"runtime","exit_code":` + fmt.Sprint(tt.exitCode) + `,"source":"test"}}`
			spore := fakeSpore(t, "printf '%s\\n' '"+completion+"'\nexit "+fmt.Sprint(tt.exitCode)+"\n", "")
			client := CommandClient{Path: spore}
			events, err := client.RunFrom(context.Background(), RunFromRequest{
				SporeDir: "/work/child.spore",
				Command:  []string{"/bin/true"},
			})
			terminal, terminalErr := TerminalEvent(events)
			if terminalErr != nil {
				t.Fatalf("TerminalEvent: %v", terminalErr)
			}
			if len(events) != 1 || terminal.Outcome != tt.outcome {
				t.Fatalf("events = %+v, terminal = %+v", events, terminal)
			}
			var machineErr *MachineError
			if !errors.As(err, &machineErr) {
				t.Fatalf("error = %T %v, want MachineError", err, err)
			}
			if machineErr.Envelope.Error.Code != tt.code || machineErr.Envelope.Error.Retry != tt.retry {
				t.Fatalf("machine error = %+v", machineErr.Envelope.Error)
			}
		})
	}
}

func TestTerminalEventRejectsInterruptedAndTrailingStreams(t *testing.T) {
	start := RunEvent{Schema: runEventsSchema, SchemaVersion: schemaVersion, Event: "start", Command: "run"}
	if _, err := TerminalEvent([]RunEvent{start}); !errors.Is(err, ErrNoTerminalEvent) {
		t.Fatalf("interrupted stream error = %v, want ErrNoTerminalEvent", err)
	}
	completion := exitEvent(0)
	if _, err := TerminalEvent([]RunEvent{completion, start}); !errors.Is(err, ErrInvalidMachineOutput) {
		t.Fatalf("trailing stream error = %v, want ErrInvalidMachineOutput", err)
	}
}

func TestCommandClientPreservesContextDeadline(t *testing.T) {
	spore := fakeSpore(t, `
if [ "$1" = "--json" ] && [ "$2" = "checkpoint" ] && [ "$3" = "resume" ]; then
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
    "retry": "after_fix",
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
	if got, want := err.Error(), `host.unsupported: The host lacks a required capability. (source="UnsupportedHost")`; got != want {
		t.Fatalf("HostInfo error = %q, want %q", got, want)
	}
}

func TestCommandClientRejectsInvalidRequestsBeforeExec(t *testing.T) {
	client := CommandClient{Path: "/path/that/should/not/run"}

	if _, err := client.Pull(context.Background(), PullRequest{}); !errors.Is(err, ErrInvalidSporeRequest) {
		t.Fatalf("Pull error = %v, want ErrInvalidSporeRequest", err)
	}
	if _, err := client.InspectCheckpoint(context.Background(), InspectCheckpointRequest{}); !errors.Is(err, ErrInvalidSporeRequest) {
		t.Fatalf("InspectBundle error = %v, want ErrInvalidSporeRequest", err)
	}
	if _, err := client.Resume(context.Background(), ResumeRequest{}); !errors.Is(err, ErrInvalidSporeRequest) {
		t.Fatalf("Resume error = %v, want ErrInvalidSporeRequest", err)
	}
	if _, err := client.RestoreNamed(context.Background(), RestoreNamedRequest{}); !errors.Is(err, ErrInvalidSporeRequest) {
		t.Fatalf("RestoreNamed error = %v, want ErrInvalidSporeRequest", err)
	}
	if _, err := client.RunFrom(context.Background(), RunFromRequest{}); !errors.Is(err, ErrInvalidSporeRequest) {
		t.Fatalf("RunFrom error = %v, want ErrInvalidSporeRequest", err)
	}
	if _, err := client.Exec(context.Background(), ExecRequest{}); !errors.Is(err, ErrInvalidSporeRequest) {
		t.Fatalf("Exec error = %v, want ErrInvalidSporeRequest", err)
	}
	if err := client.RemoveVM(context.Background(), RemoveVMRequest{}); !errors.Is(err, ErrInvalidSporeRequest) {
		t.Fatalf("RemoveVM error = %v, want ErrInvalidSporeRequest", err)
	}
	if err := client.RemoveSavedSpore(context.Background(), RemoveSavedSporeRequest{}); !errors.Is(err, ErrInvalidSporeRequest) {
		t.Fatalf("RemoveSavedSpore error = %v, want ErrInvalidSporeRequest", err)
	}
	if _, err := client.RunCapture(context.Background(), RunCaptureRequest{}); !errors.Is(err, ErrInvalidSporeRequest) {
		t.Fatalf("RunCapture error = %v, want ErrInvalidSporeRequest", err)
	}
	if err := client.Fork(context.Background(), ForkRequest{}); !errors.Is(err, ErrInvalidSporeRequest) {
		t.Fatalf("Fork error = %v, want ErrInvalidSporeRequest", err)
	}
	if _, err := client.Push(context.Background(), PushRequest{}); !errors.Is(err, ErrInvalidSporeRequest) {
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
