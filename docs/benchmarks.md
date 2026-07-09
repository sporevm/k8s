# Benchmarks

This repo has two benchmark paths:

- `mise run fleet:benchmark:synthetic` exercises the coordinator with
  deterministic local agents.
- `scripts/computesdk_sporevm_benchmark.py` runs a live Kubernetes benchmark
  shaped like the ComputeSDK sandbox TTI benchmark.

The live ComputeSDK-shaped path is intentionally narrow. Each iteration creates
one SporeVM run, prepares `docker.io/library/node:22-bookworm-slim`,
forks one child, runs `node -v`, waits for the coordinator report, and records
wall-clock TTI. This matches the public benchmark's create-plus-first-command
shape closely enough for first cluster numbers, without adding a ComputeSDK
provider or SDK shim before the measurements are useful.

Use the resident API path for TTI measurements. The `sporectl submit` fallback
is only a smoke path because it creates a Kubernetes Job per iteration.

The `--sandbox` mode is a lower-level diagnostic. It creates one named sandbox before
the measured loop, runs `node -v` in that VM for each iteration, and deletes the
VM at the end. Use it to separate resident API plus `spore exec` overhead from
prepare, fork, pull, resume, and result-reporting costs. It is not a
per-request isolation benchmark. The coordinator only enables this path when it
sees exactly one compatible agent, because named VM state is local to one agent.

The `--sandbox-pool` mode pre-creates one named sandbox per iteration, executes each
VM exactly once in the measured loop, and deletes the pool at the end. This is
the warmed-pool shape: it measures request TTI for a unique already-warmed VM,
not pool refill or parent prepare time.

The shortest development loop does not need a runtime image release. Run the
coordinator API locally and port-forward to the in-cluster agent:

```bash
kubectl -n sporevm-system port-forward svc/spore-agent 18081:8080

go run ./cmd/spore-coordinator \
  --listen=127.0.0.1:18080 \
  --agent-url=http://127.0.0.1:18081 \
  --result-store-root="$(mktemp -d)"
```

```bash
python3 scripts/computesdk_sporevm_benchmark.py \
  --api-url http://127.0.0.1:18080 \
  --iterations 10 \
  --out-dir results/sequential_tti
```

To measure the sandbox exec floor:

```bash
python3 scripts/computesdk_sporevm_benchmark.py \
  --api-url http://127.0.0.1:18080 \
  --sandbox \
  --iterations 10 \
  --out-dir results/sequential_tti
```

To measure warmed-pool request TTI with one VM per iteration:

```bash
python3 scripts/computesdk_sporevm_benchmark.py \
  --api-url http://127.0.0.1:18080 \
  --sandbox-pool \
  --iterations 10 \
  --out-dir results/sequential_tti
```

For a durable in-cluster API, publish a runtime image containing the resident API
mode and enable the chart:

```bash
helm upgrade --install sporevm-k8s ./charts/sporevm-k8s \
  --namespace sporevm-system \
  --set api.enabled=true \
  --set api.image.tag=<runtime-image-tag-with-api>
```

The output file uses the ComputeSDK result envelope:

```text
results/sequential_tti/YYYY-MM-DD.json
results/sequential_tti/latest.json
```

Useful overrides:

```bash
SPORE_API_URL=http://127.0.0.1:8081 \
python3 scripts/computesdk_sporevm_benchmark.py \
  --iterations 100
```

`--result-store-prefix` only needs to be an S3-shaped prefix because the current
agent maps result documents into its local result-store root. Use a private
prefix from ops when a live environment has a real object-store backend.

This first harness measures external sequential TTI through the resident
coordinator API. It includes parent preparation, child resume, and result
reporting, but not Kubernetes Job creation. SporeVM warm-fork capacity should
be reported separately: one prepare, many children, aggregate success, and
runtime timing percentiles from the coordinator report.

## Current Live Baseline

On 2026-07-08 UTC, a compatible Kubernetes cell running public runtime
`0.1.7` with SporeVM 0.9.1 completed an in-cluster one-child Node run through
`POST /runs`:

```text
transport=api-incluster wall=10.415s success=100%
prepareBundle=5.200s runShard=3.710s artifactPull=3.235s resume=474ms resultCommit=0.291ms
```

That is the cached-rootfs, per-request isolation path. It avoids Kubernetes Job
startup and the old `spore rm` cleanup floor, but still pays SporeVM prepare,
fork, pack, bundle inspection, pull/materialization, restore, guest command,
and result reporting for every request.

A direct named-VM diagnostic on the same runtime measured:

```text
create=68ms exec=42ms rm=22ms
```

The remaining `/runs` cost is therefore not named-VM cleanup. The next
benchmark work is to separate `pack`, `inspect-bundle`, and `pull` into
storage-aware buckets, then compare the current portable-bundle path against a
same-agent fast path and the latest SporeVM chunked rootfs / writable disk
storage model.
