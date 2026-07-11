#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

kubectl_bin="${KUBECTL:-kubectl}"
context="${SPORE_CONTEXT:-}"
namespace="${SPORE_NAMESPACE:-sporevm-system}"
pod_name="${SPORE_DEV_POD_NAME:-spore-dev-runtime-probe}"
client_pod_name="${SPORE_DEV_CLIENT_POD_NAME:-${pod_name}-client}"
benchmark_image="${SPORE_DEV_BENCHMARK_IMAGE:-python:3.13-slim}"
iterations="${SPORE_DEV_ITERATIONS:-3}"
timeout="${SPORE_DEV_TIMEOUT:-10m}"
run_prefix="${SPORE_DEV_RUN_PREFIX:-computesdk-node-dev-runtime}"
out_dir="${SPORE_DEV_OUT_DIR:-results/sequential_tti-dev-runtime}"
raw_report_dir="${SPORE_DEV_RAW_REPORT_DIR:-results/raw-dev-runtime}"
agent_id="${SPORE_DEV_AGENT_ID:-spore-dev-runtime-probe}"
cell_id="${SPORE_DEV_CELL_ID:-dev-runtime-probe}"
agent_port=18080
api_port=18081
spore_root="/var/lib/sporevm"
work_root="${spore_root}/work-dev-runtime-probe"
agent_result_root="${spore_root}/results-dev-runtime-probe"
api_result_root="${spore_root}/coordinator-results-dev-runtime-probe"
tmpdir="$(mktemp -d)"

cleanup() {
  if [[ "${SPORE_DEV_KEEP_POD:-}" != "1" ]]; then
    kube -n "${namespace}" delete pod "${client_pod_name}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    kube -n "${namespace}" delete pod "${pod_name}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi
  rm -rf "${tmpdir}"
}
trap cleanup EXIT INT TERM

die() {
  echo "dev_runtime_probe: $*" >&2
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

need "${kubectl_bin}"
need go
need python3

kubectl_args=()
if [[ -n "${context}" ]]; then
  kubectl_args+=(--context "${context}")
fi

kube() {
  "${kubectl_bin}" "${kubectl_args[@]}" "$@"
}

agent_pod="$(kube -n "${namespace}" get pod -l app.kubernetes.io/name=spore-agent -o jsonpath='{.items[0].metadata.name}')"
[[ -n "${agent_pod}" ]] || die "no spore-agent pod found in namespace ${namespace}"

node_name="$(kube -n "${namespace}" get pod "${agent_pod}" -o jsonpath='{.spec.nodeName}')"
[[ -n "${node_name}" ]] || die "could not resolve node for pod ${agent_pod}"

runtime_image="${SPORE_DEV_RUNTIME_IMAGE:-}"
spore_archive="${SPORE_DEV_SPORE_ARCHIVE:-}"
spore_path="/usr/local/bin/spore"
if [[ -z "${runtime_image}" ]]; then
  runtime_image="$(kube -n "${namespace}" get ds spore-agent -o jsonpath='{.spec.template.spec.containers[0].image}')"
fi
[[ -n "${runtime_image}" ]] || die "could not resolve runtime image"

if [[ -n "${spore_archive}" ]]; then
  [[ -f "${spore_archive}" ]] || die "SporeVM archive does not exist: ${spore_archive}"
  mkdir -p "${tmpdir}/spore-release"
  tar -xzf "${spore_archive}" -C "${tmpdir}/spore-release"
  if [[ -x "${tmpdir}/spore-release/spore_Linux_arm64/bin/spore" ]]; then
    spore_release_root="${tmpdir}/spore-release/spore_Linux_arm64"
    spore_path="/tmp/spore-release/bin/spore"
  elif [[ -x "${tmpdir}/spore-release/spore_Linux_arm64/spore" ]]; then
    spore_release_root="${tmpdir}/spore-release/spore_Linux_arm64"
    spore_path="/tmp/spore-release/spore"
  else
    die "SporeVM archive does not contain spore_Linux_arm64/bin/spore or spore_Linux_arm64/spore"
  fi
fi

echo "dev_runtime_probe: building linux/arm64 binaries" >&2
(
  cd "${repo_root}"
  CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o "${tmpdir}/spore-agent" ./cmd/spore-agent
  CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o "${tmpdir}/spore-coordinator" ./cmd/spore-coordinator
)

echo "dev_runtime_probe: creating ${namespace}/${pod_name} from ${runtime_image}" >&2
kube -n "${namespace}" delete pod "${pod_name}" --ignore-not-found --wait=true --timeout=60s >/dev/null 2>&1 || true
kube apply -f - <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: ${pod_name}
  namespace: ${namespace}
  labels:
    app.kubernetes.io/name: spore-dev-runtime-probe
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  nodeName: ${node_name}
  tolerations:
    - key: sporevm.io/agent
      operator: Equal
      value: "true"
      effect: NoSchedule
  containers:
    - name: runtime
      image: ${runtime_image}
      imagePullPolicy: IfNotPresent
      command: ["/bin/sh", "-c", "sleep 36000"]
      ports:
        - name: agent
          containerPort: ${agent_port}
        - name: api
          containerPort: ${api_port}
      securityContext:
        privileged: true
        allowPrivilegeEscalation: true
      env:
        - name: SPOREVM_KERNEL_CACHE_DIR
          value: ${spore_root}/kernels
        - name: SPOREVM_BUNDLE_CACHE_DIR
          value: ${spore_root}/bundle-cache
        - name: SPOREVM_ROOTFS_CACHE_DIR
          value: ${spore_root}/rootfs-cache
      volumeMounts:
        - name: kvm
          mountPath: /dev/kvm
        - name: sporevm-var
          mountPath: ${spore_root}
  volumes:
    - name: kvm
      hostPath:
        path: /dev/kvm
        type: CharDevice
    - name: sporevm-var
      hostPath:
        path: ${spore_root}
        type: DirectoryOrCreate
YAML

kube -n "${namespace}" wait --for=condition=Ready "pod/${pod_name}" --timeout=5m

echo "dev_runtime_probe: copying local binaries" >&2
kube -n "${namespace}" cp "${tmpdir}/spore-agent" "${pod_name}:/tmp/spore-agent" -c runtime
kube -n "${namespace}" cp "${tmpdir}/spore-coordinator" "${pod_name}:/tmp/spore-coordinator" -c runtime
if [[ -n "${spore_archive}" ]]; then
  echo "dev_runtime_probe: copying SporeVM archive contents" >&2
  kube -n "${namespace}" exec "${pod_name}" -c runtime -- mkdir -p /tmp/spore-release
  kube -n "${namespace}" cp "${spore_release_root}/." "${pod_name}:/tmp/spore-release" -c runtime
fi
kube -n "${namespace}" exec "${pod_name}" -c runtime -- "${spore_path}" version

region_arg=()
if [[ -n "${SPORE_DEV_REGION:-}" ]]; then
  region_arg=("--region=${SPORE_DEV_REGION}")
fi

echo "dev_runtime_probe: starting temporary agent and API" >&2
kube -n "${namespace}" exec "${pod_name}" -c runtime -- /bin/sh -ec "
chmod +x /tmp/spore-agent /tmp/spore-coordinator
rm -rf '${work_root}' '${agent_result_root}' '${api_result_root}'
mkdir -p '${work_root}' '${agent_result_root}' '${api_result_root}'
/tmp/spore-agent \
  --listen=:${agent_port} \
  --agent-id='${agent_id}' \
  --cell-id='${cell_id}' \
  --slots=1 \
  --spore-path='${spore_path}' \
  --result-store-root='${agent_result_root}' \
  --work-root='${work_root}' \
  --bundle-cache-root='${spore_root}/bundle-cache' \
  --rootfs-cache-root='${spore_root}/rootfs-cache' \
  ${region_arg[*]} \
  --backend=kvm \
  --child-timeout=90s >/tmp/spore-agent-dev-runtime-probe.log 2>&1 &
echo \$! >/tmp/spore-agent-dev-runtime-probe.pid
sleep 2
/tmp/spore-coordinator \
  --listen=:${api_port} \
  --agent-url=http://127.0.0.1:${agent_port} \
  --result-store-root='${api_result_root}' \
  --timeout=30m >/tmp/spore-api-dev-runtime-probe.log 2>&1 &
echo \$! >/tmp/spore-api-dev-runtime-probe.pid
sleep 2
kill -0 \$(cat /tmp/spore-agent-dev-runtime-probe.pid)
kill -0 \$(cat /tmp/spore-api-dev-runtime-probe.pid)
"

pod_ip="$(kube -n "${namespace}" get pod "${pod_name}" -o jsonpath='{.status.podIP}')"
[[ -n "${pod_ip}" ]] || die "could not resolve pod IP for ${pod_name}"

echo "dev_runtime_probe: creating in-cluster benchmark client ${namespace}/${client_pod_name}" >&2
kube -n "${namespace}" delete pod "${client_pod_name}" --ignore-not-found --wait=true --timeout=60s >/dev/null 2>&1 || true
kube apply -f - <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: ${client_pod_name}
  namespace: ${namespace}
  labels:
    app.kubernetes.io/name: spore-dev-runtime-probe-client
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  nodeName: ${node_name}
  tolerations:
    - key: sporevm.io/agent
      operator: Equal
      value: "true"
      effect: NoSchedule
  containers:
    - name: benchmark
      image: ${benchmark_image}
      imagePullPolicy: IfNotPresent
      command: ["/bin/sh", "-c", "sleep 36000"]
YAML
kube -n "${namespace}" wait --for=condition=Ready "pod/${client_pod_name}" --timeout=5m
kube -n "${namespace}" cp "${repo_root}/scripts/computesdk_sporevm_benchmark.py" "${client_pod_name}:/tmp/computesdk_sporevm_benchmark.py" -c benchmark

echo "dev_runtime_probe: running ComputeSDK-shaped benchmark inside the cluster" >&2
kube -n "${namespace}" exec "${client_pod_name}" -c benchmark -- \
  python3 /tmp/computesdk_sporevm_benchmark.py \
    --api-url "http://${pod_ip}:${api_port}" \
    --iterations "${iterations}" \
    --raw-report-dir /tmp/sporevm-raw \
    --out-dir /tmp/sporevm-results \
    --run-prefix "${run_prefix}" \
    --timeout "${timeout}"

resolve_output_dir() {
  if [[ "$1" = /* ]]; then
    printf '%s\n' "$1"
  else
    printf '%s/%s\n' "${repo_root}" "$1"
  fi
}

local_out_dir="$(resolve_output_dir "${out_dir}")"
local_raw_report_dir="$(resolve_output_dir "${raw_report_dir}")"
probe_raw_report_dir="${tmpdir}/raw-reports"
mkdir -p "${local_out_dir}" "${local_raw_report_dir}" "${probe_raw_report_dir}"
kube -n "${namespace}" cp "${client_pod_name}:/tmp/sporevm-results/." "${local_out_dir}" -c benchmark
kube -n "${namespace}" cp "${client_pod_name}:/tmp/sporevm-raw/." "${probe_raw_report_dir}" -c benchmark

python3 - "${repo_root}" "${probe_raw_report_dir}" "${run_prefix}" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

repo = Path(sys.argv[1])
raw = Path(sys.argv[2])
run_prefix = sys.argv[3]
raw_dir = raw if raw.is_absolute() else repo / raw
reports = sorted(raw_dir.glob(f"{run_prefix}-*.run-response.json"))
if not reports:
    raise SystemExit(f"no run responses found in {raw_dir} for prefix {run_prefix!r}")

cache_hits = []
for path in reports:
    report = json.loads(path.read_text(encoding="utf-8"))
    terminal = next(
        (event for event in reversed(report.get("events", [])) if event.get("event") in {"exit", "failure"}),
        None,
    )
    if terminal is None or terminal.get("event") != "exit" or terminal.get("exit_code") != 0:
        raise SystemExit(f"{path.name}: terminal={terminal!r}")
    template = report.get("template", {})
    if not template.get("id"):
        raise SystemExit(f"{path.name}: missing template id")
    cache_hits.append(template.get("cacheHit"))
    timings = report.get("timingsMs", {})
    for key in ("templateMs", "executionMs", "totalMs"):
        if key not in timings:
            raise SystemExit(f"{path.name}: missing timingsMs.{key}")

if cache_hits[0] is not False:
    raise SystemExit(f"first request cacheHit={cache_hits[0]!r}, want false")
if any(hit is not True for hit in cache_hits[1:]):
    raise SystemExit(f"later request cache hits={cache_hits[1:]!r}, want all true")

print(f"dev_runtime_probe: verified one cold-parent request and {len(reports) - 1} template hit(s)")
PY

cp -R "${probe_raw_report_dir}/." "${local_raw_report_dir}"
