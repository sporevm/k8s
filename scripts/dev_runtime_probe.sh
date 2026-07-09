#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

kubectl_bin="${KUBECTL:-kubectl}"
namespace="${SPORE_NAMESPACE:-sporevm-system}"
pod_name="${SPORE_DEV_POD_NAME:-spore-dev-runtime-probe}"
local_port="${SPORE_DEV_LOCAL_PORT:-18081}"
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
port_forward_pid=""

cleanup() {
  if [[ -n "${port_forward_pid}" ]] && kill -0 "${port_forward_pid}" >/dev/null 2>&1; then
    kill "${port_forward_pid}" >/dev/null 2>&1 || true
    wait "${port_forward_pid}" >/dev/null 2>&1 || true
  fi
  if [[ "${SPORE_DEV_KEEP_POD:-}" != "1" ]]; then
    "${kubectl_bin}" -n "${namespace}" delete pod "${pod_name}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
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

agent_pod="$("${kubectl_bin}" -n "${namespace}" get pod -l app.kubernetes.io/name=spore-agent -o jsonpath='{.items[0].metadata.name}')"
[[ -n "${agent_pod}" ]] || die "no spore-agent pod found in namespace ${namespace}"

node_name="$("${kubectl_bin}" -n "${namespace}" get pod "${agent_pod}" -o jsonpath='{.spec.nodeName}')"
[[ -n "${node_name}" ]] || die "could not resolve node for pod ${agent_pod}"

runtime_image="${SPORE_DEV_RUNTIME_IMAGE:-}"
if [[ -z "${runtime_image}" ]]; then
  runtime_image="$("${kubectl_bin}" -n "${namespace}" get ds spore-agent -o jsonpath='{.spec.template.spec.containers[0].image}')"
fi
[[ -n "${runtime_image}" ]] || die "could not resolve runtime image"

echo "dev_runtime_probe: building linux/arm64 binaries" >&2
(
  cd "${repo_root}"
  CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o "${tmpdir}/spore-agent" ./cmd/spore-agent
  CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o "${tmpdir}/spore-coordinator" ./cmd/spore-coordinator
)

echo "dev_runtime_probe: creating ${namespace}/${pod_name} from ${runtime_image}" >&2
"${kubectl_bin}" -n "${namespace}" delete pod "${pod_name}" --ignore-not-found --wait=true --timeout=60s >/dev/null 2>&1 || true
"${kubectl_bin}" apply -f - <<YAML
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

"${kubectl_bin}" -n "${namespace}" wait --for=condition=Ready "pod/${pod_name}" --timeout=5m

echo "dev_runtime_probe: copying local binaries" >&2
"${kubectl_bin}" -n "${namespace}" cp "${tmpdir}/spore-agent" "${pod_name}:/tmp/spore-agent" -c runtime
"${kubectl_bin}" -n "${namespace}" cp "${tmpdir}/spore-coordinator" "${pod_name}:/tmp/spore-coordinator" -c runtime

region_arg=()
if [[ -n "${SPORE_DEV_REGION:-}" ]]; then
  region_arg=("--region=${SPORE_DEV_REGION}")
fi

echo "dev_runtime_probe: starting temporary agent and API" >&2
"${kubectl_bin}" -n "${namespace}" exec "${pod_name}" -c runtime -- /bin/sh -ec "
chmod +x /tmp/spore-agent /tmp/spore-coordinator
rm -rf '${work_root}' '${agent_result_root}' '${api_result_root}'
mkdir -p '${work_root}' '${agent_result_root}' '${api_result_root}'
/tmp/spore-agent \
  --listen=:${agent_port} \
  --agent-id='${agent_id}' \
  --cell-id='${cell_id}' \
  --slots=1 \
  --spore-path=/usr/local/bin/spore \
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

port_log="${tmpdir}/port-forward.log"
echo "dev_runtime_probe: port-forwarding API on 127.0.0.1:${local_port}" >&2
"${kubectl_bin}" -n "${namespace}" port-forward "pod/${pod_name}" "${local_port}:${api_port}" >"${port_log}" 2>&1 &
port_forward_pid="$!"
sleep 2
if ! kill -0 "${port_forward_pid}" >/dev/null 2>&1; then
  cat "${port_log}" >&2 || true
  die "port-forward failed"
fi

echo "dev_runtime_probe: running ComputeSDK-shaped benchmark" >&2
(
  cd "${repo_root}"
  python3 scripts/computesdk_sporevm_benchmark.py \
    --api-url "http://127.0.0.1:${local_port}" \
    --iterations "${iterations}" \
    --raw-report-dir "${raw_report_dir}" \
    --out-dir "${out_dir}" \
    --run-prefix "${run_prefix}" \
    --timeout "${timeout}"
)

python3 - "${repo_root}" "${raw_report_dir}" "${run_prefix}" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

repo = Path(sys.argv[1])
raw = Path(sys.argv[2])
run_prefix = sys.argv[3]
raw_dir = raw if raw.is_absolute() else repo / raw
reports = sorted(raw_dir.glob(f"{run_prefix}-*.runtime-report.json"))
if not reports:
    raise SystemExit(f"no runtime reports found in {raw_dir} for prefix {run_prefix!r}")

for path in reports:
    report = json.loads(path.read_text(encoding="utf-8"))
    state = report.get("summary", {}).get("state")
    if state != "succeeded":
        raise SystemExit(f"{path.name}: state={state!r}, want 'succeeded'")
    prepare = report.get("prepare", {}).get("timingsMs", {})
    for key in ("runSave", "fork"):
        if key not in prepare:
            raise SystemExit(f"{path.name}: missing prepare.timingsMs.{key}")
    for key in ("pack", "inspectBundle"):
        if prepare.get(key, 0) != 0:
            raise SystemExit(f"{path.name}: prepare.timingsMs.{key}={prepare.get(key)}, want zero for direct same-agent runs")
    artifact = report.get("timings", {}).get("stagePercentilesMs", {}).get("artifactPull", {})
    if any(artifact.get(key) != 0 for key in ("p50", "p95", "p99")):
        raise SystemExit(f"{path.name}: artifactPull={artifact}, want all zero")

print(f"dev_runtime_probe: verified {len(reports)} runtime report(s)")
PY
