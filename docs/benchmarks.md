# Benchmarks

This repo has two benchmark paths:

- `mise run fleet:benchmark:synthetic` exercises the coordinator with
  deterministic local agents.
- `scripts/computesdk_sporevm_benchmark.py` runs a live Kubernetes benchmark
  shaped like the ComputeSDK sandbox TTI benchmark.

The live ComputeSDK-shaped path is intentionally narrow. Each iteration creates
one generic SporeVM run, prepares `docker.io/library/node:22-bookworm-slim`,
forks one child, runs `node -v`, waits for the coordinator report, and records
wall-clock TTI. This matches the public benchmark's create-plus-first-command
shape closely enough for first cluster numbers, without adding a ComputeSDK
provider or SDK shim before the measurements are useful.

Use the resident API path for TTI measurements. The `sporectl submit` fallback
is only a smoke path because it creates a Kubernetes Job per iteration.

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
