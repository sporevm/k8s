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

## SporeVM 0.18 migration

The source runtime image now builds with SporeVM 0.18.0 and AWS CLI v2. This
does not change already-published image tags. Deploy a newly built image
explicitly; the chart's existing published default is not a v0.18 upgrade.

This is a clean break. Stop old named VMs before replacing the runtime and
recreate old checkpoints and boot templates as format v8. Old bundles cannot
be converted by editing their manifest version. Keep the old binary and state
separate if rollback is required.

Batch runs now reuse one immutable native checkpoint, with per-child generation
JSON and independent writable state. Prebuilt submissions use `checkpoint`
and a `blake3:` digest, not `bundle` or an OCI `sha256:` digest. The coordinator
accepts `--checkpoint-run` / `SPORE_CHECKPOINT_RUN_PATH`; lease, inspection and
result documents use `checkpointDigest`. See
[the checkpoint example](examples/fleet/checkpoint-run-1000.json).
The agent's preparation and inspection endpoints are `/prepare-checkpoint`
and `/inspect-run-checkpoint`. Legacy bundle, metadata-only rootfs and bundle
cache options are removed.

Native S3 image and checkpoint access uses SporeVM's renewable AWS CLI provider
chain. Configure its AWS region and workload identity in the environment;
do not freeze role credentials into an agent process. Result storage keeps its
separate AWS SDK configuration.

The current coordinator still executes a run on one compatible agent. Warming
the application, database and cache inside the guest provides forkable service
state; external services are not checkpointed. Cross-node scheduling is the
next delivery slice, not a feature enabled by this upgrade. The acceptance
milestones and networking limits are in [the design plan](docs/plans/design.md#sporevm-0180-migration).

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
  --version 0.1.19 \
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

For published releases, `mise run release:benchmark` runs an isolated
in-cluster sample, records p50/p95/p99 for template-hit requests and warm
sandbox execs, and binds the evidence to the live Helm revision, pulled chart
digest, runtime image ID, SporeVM version, and benchmark source revision.

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
