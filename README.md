# SporeVM Kubernetes

Kubernetes adapter for running SporeVM fan-out cells.

This repo owns the public product surface:

- `spore-agent`, `spore-coordinator`, and `sporectl`
- fleet schemas and examples
- reusable Kubernetes bases
- the `sporevm-k8s` Helm chart

Private infrastructure belongs in an ops repo. Keep cloud accounts, live
cluster roots, state backends, private image repositories, queues, and operator
runbooks out of this repository.

## Chart

Install the reusable cell components with Helm:

```bash
helm install sporevm-k8s ./charts/sporevm-k8s \
  --namespace sporevm-system \
  --create-namespace
```

Private environments should keep their values file in ops:

```bash
helm upgrade --install sporevm-k8s oci://ghcr.io/sporevm/charts/sporevm-k8s \
  --version 0.1.3 \
  --namespace sporevm-system \
  --create-namespace \
  -f values/sporevm-k8s.yaml
```

The chart installs long-lived cell pieces only. Per-run coordinator jobs are
created by `sporectl submit`.

Publishing notes live in [docs/publishing.md](docs/publishing.md).

## Benchmarks

The live ComputeSDK-shaped TTI harness can call the resident coordinator API
and measure wall-clock time until `node -v` succeeds through the coordinator
report:

```bash
kubectl -n sporevm-system port-forward svc/spore-agent 18081:8080
go run ./cmd/spore-coordinator --listen=127.0.0.1:18080 --agent-url=http://127.0.0.1:18081 --result-store-root="$(mktemp -d)"
python3 scripts/computesdk_sporevm_benchmark.py \
  --api-url http://127.0.0.1:18080 \
  --iterations 10
```

It writes ComputeSDK-style JSON under `results/sequential_tti/`. See
[docs/benchmarks.md](docs/benchmarks.md) for the live cluster command shape and
scope.

## Development

```bash
mise run fleet:contracts:test
mise run fleet:go:test
mise run fleet:test
mise run public:leak-scan
```

Build the runtime image:

```bash
mise run runtime:image:build
```

Use `SPOREVM_DOWNLOAD_URL` and `SPOREVM_LINUX_ARM64_SHA256` when testing an
unreleased SporeVM Linux ARM64 archive.
