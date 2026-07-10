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

For agent or coordinator runtime changes, the shortest useful live loop does
not need a runtime image release:

```bash
mise run dev:runtime-probe
```

This dev-only probe builds local `linux/arm64` `spore-agent` and
`spore-coordinator` binaries, creates a temporary privileged pod from the
currently deployed runtime image, copies the local binaries into that pod,
starts a private agent/API pair with isolated work and result directories,
runs the ComputeSDK-shaped benchmark, and asserts that same-agent source runs
skip artifact pull. It discovers the current `spore-agent` pod, node, and
runtime image from the active Kubernetes context; do not hard-code private
cluster details in this repository.

Useful overrides:

```bash
SPORE_NAMESPACE=sporevm-system \
SPORE_DEV_ITERATIONS=3 \
SPORE_DEV_LOCAL_PORT=18081 \
mise run dev:runtime-probe
```

Set `SPORE_DEV_KEEP_POD=1` only while debugging; the default cleans up the
temporary pod and port-forward.

For coordinator-only changes, another short loop is to run the coordinator API
locally and port-forward to the in-cluster agent:

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

On 2026-07-09 UTC, public runtime `0.1.9` completed the resident API matrix:

```text
/runs api median=8039.98ms p95=8186.43ms success=100%
sandbox exec floor median=244.50ms p95=255.41ms success=100%
warmed sandbox pool median=254.75ms p95=271.60ms success=100%
```

The gap confirmed that the remaining `/runs` cost was source-run preparation
and same-agent handoff, not resident API overhead.

An unreleased dev-runtime probe then copied local binaries into the running
runtime image and exercised the direct same-agent source-run path:

```text
/runs api median=1271.96ms p95=1320.37ms success=100%
prepare.runSave=164.608ms prepare.fork=1.405ms
prepare.pack=0ms prepare.inspectBundle=0ms artifactPull=0ms materialization=0ms
resume=868.054ms resultCommit=0.288ms
```

That path prepares and forks on the selected agent, executes the prepared child
directory directly for single-attempt source runs, and leaves portable bundle
packing/inspection for retry-enabled runs, bundle runs, or future multi-agent
handoff.

Public runtime `0.1.10` then shipped that path:

```text
/runs api median=1299.49ms p95=1321.83ms success=100%
prepare.runSave=~158ms prepare.fork=~1.4ms
prepare.pack=0ms prepare.inspectBundle=0ms artifactPull=0ms materialization=0ms
resume=~860ms resultCommit=~0.3ms
```

Public runtime `0.1.11` shipped the child-command fast path and was measured
from an in-cluster benchmark client, without port-forward latency:

```text
/runs api steady median=~2031ms success=100%
prepare.runSave=~1963ms prepare.fork=~1.5ms
prepare.pack=0ms prepare.inspectBundle=0ms artifactPull=0ms materialization=0ms
resume=~33ms guestReady=~18ms resultCommit=~0.2ms
```

The first request after the SporeVM storage upgrade took about 18.1s because
the new rootfs cache identity rebuilt the image. The following nine requests
were stable around 2.03s. The child-command branch now uses:

```bash
spore run --from CHILD --generation FILE -- COMMAND
```

That reduced the old `resume` bucket from about 860ms to about 33ms. The new
limiter was SporeVM's hot parent capture, which regressed from about 158ms to
about 1.96s by rescanning the full logical rootfs on every save. SporeVM 0.11.1
fixes that scan while retaining writable-rootfs capture and the fast run-from
path.

On 2026-07-10 UTC, public runtime `0.1.12` with SporeVM 0.11.1 completed ten
requests from the same in-cluster benchmark client:

```text
/runs api median=319.61ms p95=348.80ms success=100%
prepare.runSave=~248.6ms prepare.fork=~1.5ms
prepare.pack=0ms prepare.inspectBundle=0ms artifactPull=0ms materialization=0ms
resume=~35.1ms guestReady=~18ms resultCommit=~0.2ms
```

That is about 6.4 times faster than runtime `0.1.11` at the same request shape.
The remaining cold-parent floor is now parent boot and capture, not child
resume or Kubernetes scheduling.
