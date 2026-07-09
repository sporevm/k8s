---
status: active
last_reviewed: 2026-07-06
spec_refs:
  - https://github.com/sporevm/sporevm
  - https://www.computesdk.com/blog/scale-invitational-2026/
  - https://kubernetes.io/docs/setup/best-practices/cluster-large/
  - https://kueue.sigs.k8s.io/docs/concepts/workload/
---

# SporeVM Fleet Design

## Summary

SporeVM is a general VMM built around fast capture, fork, materialize, and
resume. The fleet layer should mirror those primitives directly instead of
turning every child VM into a Kubernetes object or a CI parallel job.

The product shape is a SporeVM run:

```text
source -> prepare parent -> fork children -> execute child ranges -> publish results
```

Kubernetes is an adapter cell for deployment, host placement, access control,
and coarse admission. CI fan-out is the first useful workload and the
first proof path, but it should sit on the run contract rather than
define it.

## Problem

The easy implementation is to map each child VM to a Pod, Job, CRD, or
CI job. That is the wrong abstraction. It makes Kubernetes or CI
the hot-path scheduler and hides the thing SporeVM should prove: many children
restored from one verified warm parent with node-local cache reuse.

The fleet has to avoid these failure modes:

- one Kubernetes object per child;
- one CI job per child when SporeVM can fan out internally;
- Kubernetes placement that ignores SporeVM's exact restore platform contract;
- static slot counts that overstate CPU, memory, KVM, or cache capacity;
- retries that rerun a child after a terminal result was already committed;
- benchmark or CI output that flattens prepare, fork, resume, guest work, and
  result commit into one opaque duration.

## Goals

- Preserve SporeVM's core contract: verify bytes, materialize a selected child,
  and let SporeVM remain the final restore gate.
- Model the fleet around SporeVM primitives: source, prepare, parent, fork,
  child, shard, agent, and result.
- Run CI test fan-out for CI as the first concrete workload.
- Keep Kubernetes useful as an adapter for compatible hosts, DaemonSets,
  one-shot coordinator Jobs for batch submissions, resident fast APIs, private
  access, and later coarse admission.
- Avoid one Kubernetes object, custom resource, or CI job per child.
- Assign global child ids explicitly across runs, shards, agents, and cells.
- Store detailed per-child results outside Kubernetes; keep control-plane status
  aggregate.
- Report timings that separate prepare, fork, pull/materialization, resume,
  guest-ready or command output, child command duration, and result commit.

## Non-Goals

- No Kubernetes dependency in the SporeVM core runtime.
- No CRI or `RuntimeClass` implementation in the first version.
- No custom Kubernetes scheduler plugin in the first version.
- No CRD/operator requirement for the first useful CI path.
- No public multi-tenant security claim.
- No exactly-once execution claim for arbitrary non-idempotent workloads.
- No general workflow engine. The first API should describe one warm parent and
  many child executions.

## Target Model

### Run Contract

The public contract should be general enough for CI, simulations, fuzzing,
browser swarms, and agent workloads:

```yaml
runID: rails-rspec-20260624
source:
  image: example.com/sporevm/rails-rspec:sha-...
  platform: linux/arm64
prepare:
  command:
    - /bin/bash
    - /usr/local/bin/sporevm-rails-coordinator
    - --capture-delay
    - "2"
  captureSignal: USR1
  readyMarker: SPOREVM_RAILS_READY
  memory: 2gb
fork:
  count: 1000
children:
  command:
    - /usr/local/bin/sporevm-rspec-shard
  start: 0
  count: 1000
execution:
  childrenPerShard: 100
  maxInFlightPerAgent: 100
retryPolicy:
  maxAttemptsPerChild: 2
  rerunCommittedChildren: false
sideEffects:
  idempotencyRequired: true
resultStore: s3://example-sporevm-results/rails-rspec-20260624/
```

That contract compiles down to the existing lower-level bundle run once the
parent has been prepared, forked, packed, and published:

```yaml
bundle:
  uri: s3://example-sporevm-artifacts/runs/rails-rspec-20260624.bundle
  digest: sha256:...
children:
  start: 0
  count: 1000
childCommand:
  - /usr/local/bin/sporevm-rspec-shard
```

The lower-level bundle run remains useful for prebuilt bundles and benchmark
work. The higher-level run is the normal user and CI entrypoint; its
`children.command` compiles to the lower-level `childCommand`.

When the prepared bundle is still a local `file://` bundle, the preparing agent
also executes the run through one sequential lease. That lease may cover the
full child range, but local execution still reserves only the configured
in-flight child slots. Multi-agent runs require publishing the prepared
bundle first so other agents can materialize the same immutable input.

### Fleet Components

`sporectl` is the user-facing submitter. CI should use the same run submit
path as every other caller: render or provide a run document, then call
`sporectl submit RUN.json`. The submitter infers whether the document is a
source/prepare/fork run or a lower-level prebuilt bundle run. A
separate CI subcommand can wait until the same CI-only defaults are repeated
enough to justify it.

`spore-coordinator` owns one run when invoked as a batch command. It validates
the run, chooses compatible agents, prepares or references the bundle, leases
child ranges, tracks compact aggregate state, and exits with a clear run
result.

For latency-sensitive callers, the same coordinator binary can run as a
resident API. The first useful API is deliberately small: `POST /runs`
accepts the same run document as `sporectl submit`, talks directly to
the agent service, and returns the same aggregate runtime report. It removes
Kubernetes Job startup from benchmark and sandbox request paths without adding
CRDs, an operator, or per-child Kubernetes objects.

`spore-agent` runs on compatible hosts. It owns `/dev/kvm`, SporeVM caches,
object-store credentials, local work directories, slot admission, cache GC, and
the actual `spore` commands.

Kubernetes owns deployment and coarse lifecycle for a cell:

| SporeVM primitive | Kubernetes adapter |
| --- | --- |
| compatible host | `Node` in a labeled or tainted node group |
| node-local executor | `spore-agent` DaemonSet |
| run coordinator | one `Job` per batch submission or resident API process |
| run spec | `ConfigMap` today, optional `SporeRun` later |
| child VM | no Kubernetes object |
| child range lease | coordinator-to-agent lease |
| execution slot | agent-reported capacity |
| detailed result | object-store key |
| aggregate status | Job output today, optional CRD status later |

### Run Lifecycle

1. Resolve the source image, rootfs, existing spore, or prebuilt bundle.
2. Prepare a parent VM by running the warm command until the capture point.
3. Capture and fork the parent into `N` children.
4. Pack the child bundle under immutable digest identity, publishing it before
   multi-agent handoff or reuse.
5. Admit the child range against compatible agents with honest slots.
6. Lease non-overlapping child ranges to agents.
7. Agents materialize and resume selected children.
8. Children run the requested command using SporeVM generation identity.
9. Agents commit per-child terminal results and logs to object storage.
10. The coordinator reports aggregate status and timing summaries.

### CI Profile

CI should submit one logical SporeVM run, not schedule every test shard
itself:

```yaml
steps:
  - label: ":spore: RSpec fan-out"
    command: |
      ./scripts/render-sporevm-run > sporevm-run.json
      sporectl submit sporevm-run.json
```

The CI step should:

- derive `runID` from CI pipeline, run, job, and commit metadata;
- use a deterministic result-store prefix;
- annotate aggregate failures and link to child logs;
- exit non-zero when any child has a failed terminal result;
- leave the run contract visible for debugging.

### Host Compatibility And Slots

Host compatibility is an admission invariant. An architecture label alone is
not enough. Agents must report SporeVM host facts, and the coordinator should
only lease work to agents whose host class matches the prepared parent or
bundle.

Slots must be honest. The first implementation can clamp configured slots by
container cgroup memory, CPU policy, and fixed per-child memory budget. Later it
should use bundle-specific memory once SporeVM exposes it in machine-readable
inspection output.

For local runs, admission compares the preparing agent against in-flight
slot demand, not total child count. The prebuilt bundle planner remains stricter
and still requires enough compatible fleet slots to cover every child
concurrently.

### Results And Retries

Every child attempt is identified by:

```text
run_id
bundle_digest
child_id
attempt_id
```

Before running a child, the agent checks for:

```text
<result-store>/children/<child_id>/terminal.json
```

If `rerunCommittedChildren=false`, an existing terminal result short-circuits
the attempt. Attempt records are append-only; terminal records use
create-if-absent semantics.

### Cache And Artifact Pressure

Cold source and bundle pulls are scheduling resources. The coordinator should
surface:

- source/rootfs bytes fetched;
- bundle bytes fetched;
- cache hits and misses;
- prepare duration;
- fork and pack duration;
- materialization and resume duration;
- child command duration;
- result commit duration.

Kubernetes CPU and memory quota are not enough to model this. Kueue or CRDs can
help later with coarse admission, but cache posture belongs to SporeVM agents.

## Current State

- The public repository now contains the design plan plus the Kubernetes
  adapter cell, runtime image, `spore-agent`, `spore-coordinator`,
  `sporectl submit`, schemas, examples, chart, and fleet contract code.
- The public repository validation path is wired: CI runs `mise run fleet:test`
  and `mise run public:leak-scan`, and tag builds publish the runtime image and
  Helm chart to GHCR.
- Public release `v0.1.9` has been cut and published. Anonymous GHCR reads now
  verify `ghcr.io/sporevm/k8s-runtime:0.1.9` and
  `oci://ghcr.io/sporevm/charts/sporevm-k8s --version 0.1.9`.
- The public `main` branch requires the `buildkite/sporevm-k8s` status check.
- The thin Kubernetes adapter shape has been proved live: `spore-agent` as a
  DaemonSet, `spore-coordinator` as a one-shot Job, private ClusterIP agent
  access, and finite SporeVM/KVM runs on compatible Kubernetes nodes.
- Live pressure-testing has reached 1,000 successful children on one compatible
  KVM node using public runtime `v0.1.1`: one sequential local lease, 100
  in-flight slots, 1,000 attempts, 1,000 completed, no failed children, with
  prepare taking 18.6s and shard execution taking 2m45.9s.
- The source/prepare/fork run contract now carries the warm-command
  capture trigger needed by the Rails/RSpec example and compiles to the
  existing immutable bundle run once a prepared bundle is available.
- The agent can now run the local `prepare -> capture -> fork -> pack ->
  inspect` sequence behind the SporeVM CLI boundary and return a digest-addressed
  file bundle.
- The agent HTTP API exposes preparation, and `spore-coordinator --run`
  can prepare a run on one agent, compile it to a bundle run, and execute
  the shards on that same agent while the bundle remains a local `file://`
  artifact.
- `sporectl submit RUN.json` can render the Kubernetes ConfigMap and one-shot
  coordinator Job for either a run or a prebuilt bundle run. The
  submitter infers the run shape and passes run contracts through to
  `spore-coordinator --run`.
- A one-child public busybox run now completes in the Kubernetes adapter
  cell through `sporectl submit`, `spore-coordinator`, private ClusterIP agent
  access, agent-side prepare/fork/pack, local file-bundle handoff, shard
  execution, and create-only terminal result commit.
- The dev cell now has a checked-in cluster-local OCI registry component for
  app-level images. It is a private ClusterIP `registry:2` deployment with
  persistent storage and a cluster-local TLS certificate; CI can push through
  `kubectl port-forward`, and `spore-agent` trusts the registry CA before
  SporeVM resolves `source.image`.
- A Rails/RSpec image from `sporevm-examples` was built as a linux/arm64 OCI
  archive, pushed into the cluster-local registry with `skopeo`, resolved by
  `spore-agent` through the private service DNS name, and run through the
  Kubernetes run path.
- SporeVM exposes single-child restore identity with
  `spore restore --generation FILE`, and PR #432 adds the same generation file
  contract to `spore run --from`. The adapter writes one generation JSON per
  child and now uses `spore run --from CHILD --generation FILE -- COMMAND` for
  child-command runs. The runtime image is pinned to SporeVM 0.11.0, which
  contains that contract.
- The runtime image can be pinned to a SporeVM release tarball. The latest live
  child-command smoke used SporeVM 0.9.1 with `spore restore --generation`,
  named restore, `spore exec`, and fast `spore rm`.
- The real Rails/RSpec sharded smoke now succeeds in Kubernetes for one child
  without the unsharded fallback. The run prepared/forked/packed in 23.5s,
  completed its shard in 36.8s, and wrote a succeeded terminal result with
  artifact pull 13.5s, resume 23.3s, and guest-ready 2.5s.
- The run path preserves `children.command` as the lower-level `childCommand`.
  A live one-child busybox smoke verified the earlier named-resume path through
  `sporectl submit`; the implementation now maps that same contract to one
  `spore run --from --generation` invocation instead of named resume, exec, and
  cleanup.
- Runs can set `prepare.memory`, which is passed to `spore run --memory`
  before capture so small fan-out smokes do not inherit an oversized default
  guest memory budget.
- A 10-child busybox run first proved the `prepare.memory: 512mb`
  fix. The follow-up 100-child run used the same memory setting, 100 advertised
  slots, one shard, 100 terminal results, and generation-aware child command
  output for every sampled child.
- Per-child terminal results now include bounded stdout/stderr previews and
  complete output byte counts from SporeVM JSONL output events.
- The coordinator now maps aggregate report state to process exit status, so a
  failed child result fails the coordinator process instead of producing a
  successful Kubernetes Job.
- The live resident API now runs in-cluster through the public chart. With
  runtime `0.1.7` and SporeVM 0.9.1, direct named VM cleanup dropped from about
  5s to 22ms, and a one-child Node `POST /runs` completed in about 10.4s.
  The remaining measured cost is prepare/pack plus local bundle pull, not
  restore cleanup.
- SporeVM's landed storage model changes the next optimization target:
  image-created spores can carry chunked rootfs storage and sealed writable disk
  indexes. The Kubernetes adapter should stop treating pack/pull as an opaque
  exact-rootfs byte copy and measure whether bundle work is index installation,
  chunk transfer, or local materialization.
- Public runtime `0.1.9` measured one-child Node `POST /runs` at about 8.04s
  median while sandbox exec and warmed-pool request TTI were about 245-255ms.
  An unreleased dev-runtime probe of the direct same-agent source-run path
  dropped `/runs` to about 1.27s median by skipping `pack`, `inspect-bundle`,
  `pull`, and materialization for single-attempt local source runs.
- Public runtime `0.1.10` shipped that direct same-agent path and measured
  one-child Node `POST /runs` at about 1.30s median. The remaining hot bucket
  was child resume/command execution at about 860ms, which is why the next
  adapter change targets `spore run --from --generation`.

## Delivery Strategy

### Slice 1: Run Contract

Status: implemented locally.

Define the source, prepare, fork, children, execution, retry, and result-store
schema.

Done when:

- examples validate for a CI-shaped Rails/RSpec run;
- the run can compile to the existing bundle run once a bundle exists;
- invalid source, missing child count, missing result store, and unsafe retry
  settings are rejected.

### Slice 2: Agent Prepare And Pack

Status: implemented locally and live-proved for Rails prepare/fork/pack.

Teach the agent to prepare a parent, capture it, fork children, and pack a
bundle using SporeVM commands.

The local implementation supports `spore run --save`, watches JSONL output
for a configured `readyMarker`, sends `USR1`, runs `spore fork`, runs
`spore pack`, and inspects the local file bundle. The real
Rails/Postgres/RSpec warm command from `sporevm-examples` now prepares,
captures, forks, packs, and inspects successfully on a compatible KVM agent.

Done when:

- one agent can run the Rails/Postgres/RSpec warm command from
  `sporevm-examples`;
- it captures, forks, packs, and inspects a bundle;
- the output is digest-addressed and usable by the existing shard executor.

### Slice 3: Coordinator End-To-End Run

Status: implemented locally and live-proved for single-agent file bundles;
Rails control and one-child sharded smokes pass. Explicit post-resume child
command execution is implemented locally through named resume plus `spore exec`.
Published-bundle handoff and a live Kubernetes smoke for child-command execution
remain pending. Per-child terminal results now capture bounded guest output
previews and total output byte counts.

Wire `spore-coordinator` so one run performs prepare, fork, bundle
publication or local file-bundle handoff, shard execution, and aggregate
reporting.

Done when:

- a local or single-agent run completes without Kubernetes in the hot path;
- per-child terminal results are written outside the coordinator;
- timings distinguish prepare, fork, resume, child command, and result commit.

### Slice 4: Kubernetes Adapter Cell

Status: implemented for one-child public busybox and Rails/RSpec sharded run
smokes in a compatible Kubernetes cell. Resident fast API wiring is implemented
in code and live-smoked through a port-forwarded agent; in-cluster deployment
requires publishing a runtime image containing the API mode. Multi-agent bundle
handoff remains pending.

Keep the existing adapter shape: DaemonSet agents, one coordinator Job per
batch run, and an optional resident API for low-latency submit paths.

Done when:

- `sporectl submit` creates the run ConfigMap and coordinator Job for either a
  prebuilt bundle run or a run from one positional run document;
- the resident coordinator API can accept a run without creating a
  Kubernetes Job;
- the coordinator talks to agents through private cluster networking;
- the same run completes in a compatible Kubernetes cell;
- no per-child Kubernetes objects are created.

### Slice 5: CI Submit Profile

Status: not implemented. The current CI pipeline validates and publishes this
repository; it does not yet submit a SporeVM fan-out run. The intended CLI path
is `sporectl submit sporevm-run.json`, not a separate `sporectl ci` command.

Add the smallest CI-specific submit behavior on top of the run.

Done when:

- a CI step can submit a Rails/RSpec fan-out run;
- the step waits for aggregate completion;
- failures produce an annotation and links to child logs;
- the step exits with the aggregate run result.

### Slice 6: Honest 1,000-Child Scale

Status: implemented for one compatible single-agent cell. Synthetic planning
covers 1,000 children, and a live 1,000-child busybox run has completed with
100 in-flight child slots. The next benchmark work is to turn this into
repeatable cluster measurements instead of one-off smoke evidence.

Scale by adding real capacity or reducing verified per-child requirements, not
by overstating slots.

Done when:

- dry-run planning proves unique coverage for 1,000 children;
- live capacity can honestly advertise enough slots across one or more agents;
- a 1,000-child run reports success rate and timing percentiles.
- a ComputeSDK-shaped sequential TTI benchmark can run against the live cell.
- the same benchmark can target the resident API path without creating a
  Kubernetes Job per iteration.
- a sandbox diagnostic path can measure resident API plus `spore exec` overhead
  separately from prepare, fork, pull, resume, and result-reporting costs.

Current live finding: the resident API removes Kubernetes Job startup from the
hot path, and SporeVM 0.9.1 removed the hidden `spore rm` cleanup floor. A
direct named-VM probe measured create at 68ms, exec at 42ms, and cleanup at
22ms. The same runtime completed a one-child Node `POST /runs` in about 10.4s:
prepare-bundle was about 5.2s, shard execution was about 3.7s, artifact pull
was about 3.2s, and the restore/exec/cleanup bucket was about 474ms.

The next isolated TTI work is now resume/command overhead, not restore cleanup
or local bundle pull. SporeVM's new block-storage contract records
image-created rootfs state as chunked rootfs storage and writable rootfs state
as sealed disk indexes. That should still improve portable multi-agent bundles,
but the single-agent `POST /runs` adapter now has a local prepare path that
performs `prepare -> fork -> run-from` without `pack -> inspect-bundle -> pull`
when the same agent prepares and executes a single-attempt child. Retry-enabled
runs keep the portable bundle path so later attempts can materialize a clean
child.

The optimization order is:

- release and deploy the `spore run --from --generation` child-command path;
- remeasure the remaining `resume` bucket, currently about 860ms for the Node
  smoke after the direct same-agent path;
- split portable bundle timings for `pack`, `inspectBundle`, `pullVerify`,
  `pullInstallIndexes`, `pullInstallChunks`, and child manifest writes so
  multi-agent pack/pull stops being one opaque bucket;
- for real multi-agent fan-out, keep the portable bundle contract but push
  SporeVM toward index-aware or lazy `pull` so a selected child can restore
  from verified indexes and node-local chunks without full eager
  materialization;
- teach placement and preheat decisions to prefer agents that already have the
  needed rootfs/disk indexes and chunks on node-local cache.

### Slice 7: Optional Kubernetes UX

Add CRDs, Kueue, or an operator only after the run and CI paths work.

Done when:

- one `SporeRun` maps to one coordinator run;
- status remains aggregate;
- Kueue gates coarse admission without becoming the child scheduler;
- existing `sporectl` flows still work without CRDs.

## Verification

- Schema tests for run and compiled bundle run examples.
- Unit tests for child id derivation, shard overlap rejection, attempt keys, and
  terminal-result short-circuiting.
- Agent tests for prepare, fork, pack, cache locking, slot admission,
  cancellation, cleanup, and platform mismatch.
- Coordinator tests for admission, lease assignment, retry behavior, aggregate
  status, and failed-child reporting.
- A real Rails/RSpec fan-out smoke using `sporevm-examples`.
- Kubernetes render checks for the agent DaemonSet and coordinator Job.
- Kubernetes render checks for the cluster-local OCI registry and the dev
  agent CA trust patch.
- Live Kubernetes smoke has reached 1,000 children on one compatible node.
- CI smoke that submits one logical run and fails the step on aggregate
  child failure.
- Live cluster-local registry smoke: build the Rails OCI archive with buildx,
  push it into `spore-registry.sporevm-system.svc.cluster.local:5000`, and
  resolve it from `spore-agent` with `spore rootfs resolve`.
- Live Rails/RSpec run control smoke through `sporectl submit` against the
  cluster-local registry image.
- Live Rails/RSpec sharded run smoke through `sporectl submit` against the
  cluster-local registry image and the pinned SporeVM release runtime.
- Public repository CI smoke for `mise run fleet:test`, leak scan, chart lint,
  and tag-gated GHCR image/chart publishing is done for `v0.1.1`.

## Resolved Decisions

- SporeVM fleet primitives are the product model.
- Kubernetes is an adapter cell, not the inner-loop scheduler.
- CI fan-out is the first workload, not the exclusive contract.
- Do not create one Kubernetes object per child.
- Keep per-child results outside Kubernetes.
- Keep KVM, credentials, caches, and SporeVM execution inside agents.
- In the first coordinator path, the agent that prepares a local
  `file://` bundle also executes a single sequential lease bounded by in-flight
  slots. Multi-agent runs require publishing the prepared bundle first.
- Public runtime images publish to GHCR; private environments can override the
  image repository from their ops values.
- The public `main` branch requires the `buildkite/sporevm-k8s` status check.
- `spore-coordinator` treats an aggregate runtime report with
  `state != succeeded` as a failed run even when the container reached the end
  of its process.
- Do not add `sporectl ci` yet. CI uses `sporectl submit RUN.json`; helper
  scripts or flags can render CI metadata until a separate subcommand earns its
  keep.
- Add CRDs, Kueue, and operator UX later.

## Deferred Work

- Registry auth, garbage collection, pull-through mirroring, and multi-replica
  registry operation.
- CRI / `RuntimeClass` integration.
- Custom Kubernetes scheduler plugins.
- DRA-backed execution slots.
- Published prepared-bundle handoff for multi-agent runs.
- Interactive terminal support.
- Public multi-tenant hardening.
- Non-idempotent side-effect protocols beyond terminal result commits.
- Richer cache peer selection and preheat scheduling.

## Open Questions

- What is the first durable result backend: S3 conditional writes, a small API,
  or the existing bundle store?
- How much CI UI polish is needed for the first useful demo: annotation
  only, artifact links, or a richer summary document?

## Key Learnings From Pressure-Testing

- Kubernetes buys deployment, isolation around access, and coarse admission. It
  should not schedule every child.
- The abstraction should be SporeVM-shaped, not CI-shaped. CI is just the first
  high-value profile.
- Static slots are dangerous. Capacity has to reflect cgroup memory and, later,
  bundle-specific memory.
- Guest memory is part of the fleet contract in practice: 10 children inherited
  the default guest memory and OOM-killed the agent until the prepared parent
  used an explicit smaller memory budget.
- Signal-based parent capture is part of the run contract for
  long-lived warm commands. The agent owns that host-side trigger; Kubernetes
  should only see the resulting run and aggregate status.
- Rails/RSpec proves that plain child `spore resume` is not equivalent to
  `spore fanout`: sharded workloads need stable `/run/sporevm/env` or
  `/run/sporevm/generation.json` identity on every resumed child.
- The 100-child run held on one compatible KVM node with explicit 512 MB guest
  memory, which moves the next pressure point to honest 1,000-child capacity.
- The next useful implementation work is scaling honest live capacity beyond
  the current single-agent proof. CRDs and Kueue can wait.
