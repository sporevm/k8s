# SporeVM Kubernetes

Kubernetes adapter for running SporeVM fan-out cells.

This repo owns the public product surface:

- `spore-agent`, `spore-coordinator`, and `sporectl`
- the `@sporevm/computesdk-k8s` sandbox adapter
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
  --version 0.1.16 \
  --namespace sporevm-system \
  --create-namespace \
  -f values/sporevm-k8s.yaml
```

The chart installs long-lived cell pieces only. Per-run coordinator jobs are
created by `sporectl submit`.

Buildkite steps use the same command with `--buildkite` to wait for aggregate
completion and post the runtime summary. See [docs/ci.md](docs/ci.md).

Production batch results use conditional S3 writes; local directory mapping is
kept for tests and smokes. See [docs/results.md](docs/results.md).

Publishing notes live in [docs/publishing.md](docs/publishing.md).

## Benchmarks

The live ComputeSDK-shaped TTI harness measures a fresh ephemeral child through
the resident coordinator API. The development probe runs its client inside the
cluster so port-forward latency is excluded:

```bash
SPORE_DEV_ITERATIONS=10 mise run dev:runtime-probe
```

It labels the first automatic template capture separately from later immutable
template hits and writes ComputeSDK-style JSON under `results/sequential_tti/`. See
[docs/benchmarks.md](docs/benchmarks.md) for the live cluster command shape and
scope.

The actual upstream sequential benchmark can use the adapter under
`integrations/computesdk`; `docs/benchmarks.md` records the checkout and runner
command.

To test a published SporeVM archive before cutting a Kubernetes runtime release,
pass its local path to the same probe:

```bash
SPORE_DEV_SPORE_ARCHIVE=/tmp/spore_Linux_arm64.tar.gz \
SPORE_DEV_ITERATIONS=10 \
mise run dev:runtime-probe
```

After deploying a runtime image, run the isolated in-cluster acceptance path:

```bash
mise run release:accept
```

It records the pulled image digest and SporeVM version, verifies one cold parent
followed by a template hit, exercises first and warm execs in a named sandbox,
then releases the template pins and fails if the sandbox slot or temporary pods
remain allocated.

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
