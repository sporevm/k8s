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

The source runtime image builds with SporeVM 0.18.5 and AWS CLI v2. This
does not change already-published image tags. Deploy runtime/chart 0.1.20 or a
newly built image explicitly; older chart defaults do not provide this upgrade.

Remote-parent preparation requires `checkpoint pull --local-ram-backing`.
SporeVM verifies and reconstructs a read-only local RAM backing once per pulled
parent so its children share clean RAM pages; child writes remain private.
Preparation fails rather than falling back to a private RAM copy per child.
Allow local storage for the sparse backing in addition to the native object
cache, and include its construction in cold-parent preparation measurements.
Parents remain scoped to one run on each agent, not shared across agents or
independent runs.

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

Prebuilt checkpoint runs divide their global child range across explicitly
configured compatible agents and execute the ranges concurrently. Supply one
stable, agent-specific `--agent-url` per destination (or comma-separated
`SPORE_AGENT_URLS`); do not use a load-balanced Service URL for multiple agents.
Every selected destination materializes and checks the same checkpoint before
any child starts. Multi-agent runs require a shared S3 checkpoint store, and
the report exposes `plan.leases`, not a single `plan.lease`. Per-lease
`maxInFlight` caps execution at the capacity observed during planning, bounded
by the run's `execution.maxInFlightPerAgent`.

Source runs still prepare and execute on one agent because their generated
checkpoint store is local. Automatic publication of that parent to a shared
store and agent discovery remain follow-ups. Warming the application, database
and cache inside the guest provides forkable service state; external services
are not checkpointed. Multi-node acceptance and large-suite speedup remain
unproven. See the [design plan](docs/plans/design.md#sporevm-0180-migration)
for the acceptance milestones and networking limits.

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
  --version 0.1.20 \
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
