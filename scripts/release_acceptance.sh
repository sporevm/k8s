#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
kubectl_bin="${KUBECTL:-kubectl}"
context="${SPORE_CONTEXT:-}"
namespace="${SPORE_NAMESPACE:-sporevm-system}"
benchmark_image="${SPORE_ACCEPTANCE_CLIENT_IMAGE:-python:3.13-slim}"
workload_image="${SPORE_ACCEPTANCE_WORKLOAD_IMAGE:-docker.io/library/node@sha256:6db9be2ebb4bafb687a078ef5ba1b1dd256e8004d246a31fd210b6b848ab6be2}"
memory="${SPORE_ACCEPTANCE_MEMORY:-1024mb}"
timeout_seconds="${SPORE_ACCEPTANCE_TIMEOUT_SECONDS:-180}"
report_path="${SPORE_ACCEPTANCE_REPORT:-results/runtime-acceptance/latest.json}"
probe_id="$(date -u +%Y%m%d%H%M%S)-$$"
runtime_pod="spore-release-acceptance-${probe_id}"
client_pod="${runtime_pod}-client"
sandbox_name="acceptance-${probe_id}"
agent_port=18080
api_port=18081
tmpdir="$(mktemp -d)"

die() {
  echo "release_acceptance: $*" >&2
  exit 1
}

command -v "${kubectl_bin}" >/dev/null 2>&1 || die "missing required command: ${kubectl_bin}"
command -v python3 >/dev/null 2>&1 || die "missing required command: python3"

kubectl_args=()
if [[ -n "${context}" ]]; then
  kubectl_args+=(--context "${context}")
fi

kube() {
  if ((${#kubectl_args[@]})); then
    "${kubectl_bin}" "${kubectl_args[@]}" "$@"
  else
    "${kubectl_bin}" "$@"
  fi
}

release_templates() {
  kube -n "${namespace}" exec "${runtime_pod}" -c runtime -- /bin/sh -ec '
    for template in /acceptance/work/templates/*/parent.spore /acceptance/work/templates/.build-*/parent.spore; do
      [ -d "${template}" ] || continue
      /usr/local/bin/spore --json rm --spore "${template}"
    done
    rm -rf /acceptance/work /acceptance/agent-results /acceptance/coordinator-results
  '
}

cleanup() {
  status=$?
  trap - EXIT INT TERM
  if kube -n "${namespace}" get pod "${runtime_pod}" >/dev/null 2>&1; then
    release_templates >/dev/null 2>&1 || true
  fi
  kube -n "${namespace}" delete pod "${client_pod}" "${runtime_pod}" --ignore-not-found --wait=true --timeout=2m >/dev/null 2>&1 || true
  rm -rf "${tmpdir}"
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

agent_pod="$(kube -n "${namespace}" get pod -l app.kubernetes.io/name=spore-agent -o jsonpath='{.items[0].metadata.name}')"
[[ -n "${agent_pod}" ]] || die "no spore-agent pod found in namespace ${namespace}"
node_name="$(kube -n "${namespace}" get pod "${agent_pod}" -o jsonpath='{.spec.nodeName}')"
runtime_image="${SPORE_ACCEPTANCE_RUNTIME_IMAGE:-$(kube -n "${namespace}" get ds spore-agent -o jsonpath='{.spec.template.spec.containers[0].image}')}"
[[ -n "${node_name}" && -n "${runtime_image}" ]] || die "could not resolve the deployed runtime placement"

echo "release_acceptance: starting ${runtime_image} on node ${node_name}" >&2
kube apply -f - <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: ${runtime_pod}
  namespace: ${namespace}
  labels:
    app.kubernetes.io/name: spore-release-acceptance
    sporevm.io/acceptance-id: ${probe_id}
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
      securityContext:
        privileged: true
        allowPrivilegeEscalation: true
      command: ["/bin/sh", "-ec"]
      args:
        - |
          mkdir -p /acceptance/work /acceptance/agent-results /acceptance/coordinator-results
          /usr/local/bin/spore-agent \
            --listen=:${agent_port} \
            --agent-id=release-acceptance \
            --cell-id=release-acceptance \
            --slots=1 \
            --spore-path=/usr/local/bin/spore \
            --result-store-root=/acceptance/agent-results \
            --work-root=/acceptance/work \
            --bundle-cache-root=/var/lib/sporevm/bundle-cache \
            --rootfs-cache-root=/var/lib/sporevm/rootfs-cache \
            --backend=kvm \
            --child-timeout=90s >/tmp/spore-agent-acceptance.log 2>&1 &
          /usr/local/bin/spore-coordinator \
            --listen=:${api_port} \
            --agent-url=http://127.0.0.1:${agent_port} \
            --result-store-root=/acceptance/coordinator-results \
            --timeout=5m >/tmp/spore-coordinator-acceptance.log 2>&1 &
          wait
      env:
        - name: SPOREVM_KERNEL_CACHE_DIR
          value: /var/lib/sporevm/kernels
        - name: SPOREVM_BUNDLE_CACHE_DIR
          value: /var/lib/sporevm/bundle-cache
        - name: SPOREVM_ROOTFS_CACHE_DIR
          value: /var/lib/sporevm/rootfs-cache
      volumeMounts:
        - name: kvm
          mountPath: /dev/kvm
        - name: sporevm-var
          mountPath: /var/lib/sporevm
        - name: acceptance
          mountPath: /acceptance
  volumes:
    - name: kvm
      hostPath:
        path: /dev/kvm
        type: CharDevice
    - name: sporevm-var
      hostPath:
        path: /var/lib/sporevm
        type: DirectoryOrCreate
    - name: acceptance
      emptyDir: {}
YAML
kube -n "${namespace}" wait --for=condition=Ready "pod/${runtime_pod}" --timeout=5m

runtime_image_id="$(kube -n "${namespace}" get pod "${runtime_pod}" -o jsonpath='{.status.containerStatuses[0].imageID}')"
spore_version="$(kube -n "${namespace}" exec "${runtime_pod}" -c runtime -- /usr/local/bin/spore version | tr -d '\r\n')"
[[ -n "${runtime_image_id}" && -n "${spore_version}" ]] || die "could not read runtime provenance"
if [[ -n "${SPORE_ACCEPTANCE_EXPECT_RUNTIME_IMAGE:-}" && "${runtime_image}" != "${SPORE_ACCEPTANCE_EXPECT_RUNTIME_IMAGE}" ]]; then
  die "runtime image ${runtime_image} did not match ${SPORE_ACCEPTANCE_EXPECT_RUNTIME_IMAGE}"
fi
if [[ -n "${SPORE_ACCEPTANCE_EXPECT_SPORE_VERSION:-}" && "${spore_version}" != *"${SPORE_ACCEPTANCE_EXPECT_SPORE_VERSION}"* ]]; then
  die "SporeVM version ${spore_version} did not contain ${SPORE_ACCEPTANCE_EXPECT_SPORE_VERSION}"
fi

pod_ip="$(kube -n "${namespace}" get pod "${runtime_pod}" -o jsonpath='{.status.podIP}')"
[[ -n "${pod_ip}" ]] || die "could not resolve acceptance runtime pod IP"

kube apply -f - <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: ${client_pod}
  namespace: ${namespace}
  labels:
    app.kubernetes.io/name: spore-release-acceptance-client
    sporevm.io/acceptance-id: ${probe_id}
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
    - name: client
      image: ${benchmark_image}
      imagePullPolicy: IfNotPresent
      command: ["/bin/sh", "-c", "sleep 3600"]
YAML
kube -n "${namespace}" wait --for=condition=Ready "pod/${client_pod}" --timeout=5m
kube -n "${namespace}" cp "${repo_root}/scripts/runtime_acceptance.py" "${client_pod}:/tmp/runtime_acceptance.py" -c client

echo "release_acceptance: exercising cold run, template hit, and named sandbox lifecycle" >&2
report="$(kube -n "${namespace}" exec "${client_pod}" -c client -- \
  python3 /tmp/runtime_acceptance.py \
    --api-url "http://${pod_ip}:${api_port}" \
    --agent-url "http://${pod_ip}:${agent_port}" \
    --image "${workload_image}" \
    --memory "${memory}" \
    --sandbox-name "${sandbox_name}" \
    --timeout-seconds "${timeout_seconds}" \
    --runtime-image "${runtime_image}" \
    --runtime-image-id "${runtime_image_id}" \
    --spore-version "${spore_version}")"
printf '%s\n' "${report}"

echo "release_acceptance: releasing durable template pins" >&2
release_templates

if [[ "${report_path}" != /* ]]; then
  report_path="${repo_root}/${report_path}"
fi
mkdir -p "$(dirname "${report_path}")"
printf '%s\n' "${report}" >"${report_path}"

kube -n "${namespace}" delete pod "${client_pod}" "${runtime_pod}" --wait=true --timeout=2m
if kube -n "${namespace}" get pod "${client_pod}" >/dev/null 2>&1 || kube -n "${namespace}" get pod "${runtime_pod}" >/dev/null 2>&1; then
  die "temporary acceptance pods were not removed"
fi

echo "release_acceptance: passed; report=${report_path}" >&2
