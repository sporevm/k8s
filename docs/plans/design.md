---
status: active
last_reviewed: 2026-08-03
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

The product has two execution shapes built on the same node-local SporeVM
primitives:

```text
interactive: immutable image -> cached boot template -> fresh child -> command -> discard
batch:       source -> prepare parent -> fork children -> execute child ranges -> publish results
```

Kubernetes is an adapter cell for deployment, host placement, access control,
and coarse admission. CI fan-out is the first useful workload and the
first proof path, but it should sit on the run contract rather than
define it.

## SporeVM 0.18.0 migration

The released runtime is [v0.18.0](https://github.com/sporevm/sporevm/releases/tag/v0.18.0),
source `1197f65baec9bf4618334d113e145a6833763fa8`. Its Linux ARM64 archive
SHA-256 is `184ff27b58e8994ed61b94b01ca78e5533fa0ef81b3111d8e8f45925a8fd15df`.
This is a breaking migration, not a runtime-pin update. The older design
sections below retain historical bundle-based design context; this section
describes the native checkpoint replacement and its remaining acceptance gates.

### Target workload and acceptance boundary

The target is a complex test environment such as the Buildkite application:
start the application and its databases, caches and other test services once,
wait for readiness, checkpoint the warm guest, and execute disjoint test-suite
shards in isolated children. The Rails example is the pattern, not the final
workload. Put forkable service state inside the guest; an external database,
cache or queue is not captured or isolated by a VM checkpoint.

The intended fleet shape is one immutable parent transferred through a native
checkpoint store to compatible node agents, then bounded child execution per
agent with globally assigned shard identities and aggregate results. Pods own
node agents or shard groups, not individual test children. Do not make child
identity depend on a node-local fork directory name or a per-agent child count.

Acceptance must distinguish these milestones rather than call them equivalent:

1. Warm the multi-service environment once in a single guest. Record the exact
   image, runtime version, readiness checks and service placement. Checkpoint
   only after services and test fixtures are ready.
2. Run at least two different suite shards from that same parent on ARM64 KVM.
   Assert the expected shard identities, disjoint test selection, complete
   aggregate coverage, exit codes and per-child database/disk isolation. A
   failed shard must not rerun already committed shards on retry. Measure warm
   preparation separately from checkpoint transfer, restore and test execution.
3. Transfer the checkpoint through a native store into an isolated cache in
   another agent pod, verify its platform before admission, and execute a
   different shard there. Two pods on one node prove pod handoff, not cross-node
   scheduling or checkpoint portability.
4. Execute disjoint shards concurrently on at least two compatible physical
   nodes. Record node identities, one shared parent digest, global shard
   coverage and output, and verify cleanup on every participating agent. Do not
   report the current single-node test as completion of this milestone.

Today's limits affect the delivery boundary:

- External services are not forked. Shared external state requires an explicit
  isolation/reset contract; the first checkpoint proof should use guest-local
  services rather than assume external transactions are snapshotted.
- Networked named live fork is unsupported in v0.18.0. Do not promise networked
  named children from `vm fork`. Saved checkpoint execution is different:
  `run --checkpoint` and `checkpoint resume` can attach a fresh gateway under
  the saved policy, without preserving TCP flows. This needs workload-level
  testing and fleet policy/binding support before relying on external services.
- Native `checkpoint fanout` is local orchestration. The current Kubernetes
  coordinator also deliberately selects one agent. Multi-node execution needs
  parent publication/materialization per agent, global range assignment,
  compatibility admission on each destination, concurrent lease execution,
  retry/terminal-result ownership, and per-agent cleanup. Merely changing CLI
  names or raising agent replicas does not provide it.
- A Mac/HVF workload run can establish service and test requirements, but it
  does not prove Linux/KVM checkpoint compatibility. ARM64 architecture alone
  is insufficient: compare the full saved platform facts on the target hosts.
- The v0.18 gateway has an eight-flow TCP pool. The Buildkite workload trial
  reached this limit while fetching dependencies, before running its browser
  suite. Serialize build stages and cap dependency-fetch concurrency for the
  first proof; keep runtime services guest-local. This is a setup constraint,
  not evidence that the warm application or its test shards have passed.
- The separate Buildkite workload trial reports a guest PID 1 cleanup panic
  after timed-out Docker commands on both HVF and KVM. Its browser suite has
  not passed. Ordinary-shell cancellation and Rails/PostgreSQL acceptance do
  not validate Docker/cgroup cleanup; that runtime issue remains separate.

These limits are grounded in the v0.18.0
[fan-out](https://github.com/sporevm/sporevm/blob/v0.18.0/docs/fanout.md),
[named lifecycle](https://github.com/sporevm/sporevm/blob/v0.18.0/docs/lifecycle.md)
and [networking](https://github.com/sporevm/sporevm/blob/v0.18.0/docs/networking.md)
contracts and `internal/fleet/coordinator.go`'s single-agent planner.

### Delivery slices

1. Adapt the agent's capture, local fork, saved-process resume, new-command
   execution, named VM lifecycle and GC calls to the released CLI. Use global
   `--json`, preserve the stable `attach`/`exec` wire identifiers, and retain
   exactly-one-completion and guest-exit validation. Validate execution-budget
   ownership separately from the spelling change. Do not change the deployed
   runtime pin in the first slice.
2. Replace checkpoint bundles with one immutable parent checkpoint and
   aggregate run-owned child generation metadata. Replace the old bundle
   machine schemas, `inspect-bundle`, `pack`, child-selecting pull and
   metadata-only rootfs behavior, rather than translating their names. A
   checkpoint's identity is BLAKE3; OCI image identity remains SHA-256.
   Explicitly migrate the fleet JSON schemas, examples, admission and metrics.
   Old bundle inputs must fail clearly, not be misinterpreted as checkpoints.
3. Materialize a remote parent through `checkpoint pull REF --output DIR`
   once on the selected agent, on the rootfs cache filesystem; inspect the
   local parent and enforce host compatibility before scheduling children.
   Child count/range belongs to the aggregate run, not the checkpoint.
   Supply generation JSON on each child execution and retry. Retain the
   parent until all attempts finish, then remove it through SporeVM before GC.
   Local source runs should reuse their captured parent without downloading
   RAM or constructing a portable artifact per child. Preserve the existing
   single-agent execution boundary for the migration unless the scope decision
   below explicitly includes distributed scheduling; use a transferable parent
   and global generation metadata so that boundary can be lifted later.
4. Provide AWS CLI v2 in the runtime image and use SporeVM 0.18.0's renewable
   provider chain for all native S3 image/checkpoint reads and writes. Do not
   export one-time role credentials into a lazy reader. Keep result-store AWS
   SDK behavior separate. Update the runtime version and verified archive
   checksum only with the complete migration, including charts, smoke scripts
   and upgrade documentation.

### Acceptance and rollout gates

- Run Go tests/vet, Python script tests, Helm lint and the public leak scan for
  each slice. Fake CLI tests prove argv and parser behavior only.
- On real ARM64 KVM, use the published archive and isolated cache/runtime/work
  directories. Exercise cold and warm `/runs`, named sandbox exec/removal,
  signal capture and saved-process generation-aware resume, nonzero guest
  exits, bounded concurrency, retry/terminal-result behavior, and cleanup/GC.
- Exercise a directory store first, then private S3 image and checkpoint
  transfer into a fresh cache, followed by offline execution of the fully
  materialized checkpoint. Record exact revisions, command outcomes and any
  skipped path; do not claim fake-spore or macOS tests validate Kubernetes.
- Cluster probes must not share runtime state or GC ownership with an old
  agent. Review the existing dev probe's shared-cache mounts before using it.
- Stop old named VMs before upgrading. Recreate every old checkpoint and boot
  template under format v8; changing manifest version fields is not migration.
  Keep old deployments pinned until acceptance passes. Rollback requires the
  old binary and its old state; do not restore v8 state with the old runtime.
- Human approval is required before merge, release, deployment, dev-node
  scaling or other infrastructure changes. Operational approvals and bounded
  hardware test windows are tracked outside this public plan; this document
  itself grants no permission. Keep private infrastructure details out of this
  public repo.

### Settled decisions

- No backward compatibility is required. Replace old bundle inputs and legacy
  machine-schema fallbacks rather than add aliases. Source runs retain their
  aggregate child/fork counts; old prebuilt bundles must be recreated as native
  checkpoint submissions.

- Keep [#50](https://github.com/sporevm/k8s/pull/50) separate rather
  than folding its capture-only credential exporter into this migration.
  Its claim that SporeVM cannot use profiles/roles is obsolete in 0.18.0;
  importing it would also leave checkpoint pulls and renewable reads uncovered.
  Leave #50 untouched; its credential bridge is not a prerequisite.
- Complete the native v0.18 migration with a guest-local multi-service
  checkpoint/isolation proof first, then deliver distributed scheduling as a
  separate PR. Networking for named live forks is upstream runtime work, not
  something the Kubernetes adapter can supply by renaming commands.

### Current implementation state

The native checkpoint adapter and v0.18.0 runtime pin are implemented in
[#51](https://github.com/sporevm/k8s/pull/51).
Source preparation captures and publishes one parent, and all child attempts
reuse that parent with distinct generation metadata. Remote submissions pull
once and compare the checkpoint's saved platform facts with the selected
host before admission. Cleanup retains the parent through retries and removes
it through SporeVM when aggregate execution finishes. Legacy bundle inputs and
machine-schema fallbacks are removed; AWS CLI v2 is included for native stores.
Saved-process resumes use the checkpoint's saved generation and its successor
alongside global shard identity; without that barrier a warm process can remain
asleep instead of running its shard. Source runs may omit `children.command`
to resume the captured process rather than replace it with a new command.

Warm capture, new-command checkpoint execution and named exec disable the
CLI's default 30-second execution limit: caller contexts and the configured
per-child timeout own their execution budgets. Startup readiness remains
bounded by SporeVM. A failed capture can leave chunks without a manifest;
cleanup discards that incomplete agent-owned output and makes GC pending,
but retains a corrupt save whose manifest exists for investigation.

Local validation passes: Go tests with the race detector, vet, Python script
tests, Helm lint, the public leak scan, example schema checks and whitespace
checks. CI passes for runtime commit
[`8c38fb1`](https://github.com/sporevm/k8s/commit/8c38fb1e38419725125472066d69d560d7a892a5).
The ARM64 Docker image builds with the published archive's checksum verified;
the included AWS CLI executable runs inside that image.

Real Linux ARM64/KVM acceptance at that runtime commit establishes:

- Cold and warm interactive runs, named sandbox repeated exec/removal and
  recovery of both execution slots.
- One warm Rails/PostgreSQL guest from
  [`sporevm/examples@7bfe0e3`](https://github.com/sporevm/examples/commit/7bfe0e3f62d55ed5e0bf2544e449aab21d22b591),
  captured once and resumed as three disjoint RSpec shards at concurrency two.
  Each child reports its distinct index, count three, HTTP 200 and one passing
  example; all three examples are covered. Additional children independently
  create the same PostgreSQL table and disk marker and read only their own
  values.
- Replay of the completed run produces zero new attempts. Interrupting a
  different shard after two children commit leaves only the unfinished child
  to execute on retry. A separate child exiting seven becomes a terminal
  failure while its siblings succeed.
- A directory-store handoff into a second container with a fresh cache and
  runtime, with networking disabled, passes all three Rails shards using the
  same parent digest. Parent release and execution-slot recovery succeed.
  This is same-host isolated-container evidence, not a Kubernetes pod test or
  cross-node portability proof.

The sample separates 9.315 seconds of warm/capture from 57.545 seconds of
native local publication and 0.408 seconds of already-prepared shard execution.
The fresh-container handoff takes 13.420 seconds including admission-time pull
and all three shards. These are single samples, not benchmark percentiles or
evidence of large-suite speedup. Child `artifactPull`/`materialization` timings
do not include the initial admission pull; retain overall elapsed time when
evaluating cold handoff and add explicit admission-phase transfer accounting
before using child timings for distributed performance comparisons.

Additional Kubernetes/private-S3 acceptance establishes:

- Two isolated privileged, host-network pods on one real ARM64 KVM node use
  separate `emptyDir` cache/runtime/work state, without touching the existing
  deployment or node cache. The published v0.18 binary, AWS CLI and current
  agent binaries are overlaid into existing base images for this probe; this
  is not acceptance of a newly published runtime image or a Helm rollout.
- A native Rails image is built and published privately. Pod B starts with an
  empty image cache, warms/captures once, and passes three RSpec shards at
  concurrency two, with terminal outputs verified independently from S3.
  Warm/capture takes 89.411 seconds, local checkpoint publication 56.175 seconds
  and already-prepared aggregate execution 0.651 seconds in this single sample.
- The same immutable parent is pushed to private S3. Pod A, restarted with an
  empty cache and runtime, pulls it, passes compatibility admission and executes
  all three shards. Its aggregate report spans 56.482 seconds including
  admission-time transfer, versus 1.294 seconds in the agent's shard handler.
  Child timing percentiles are not a substitute for that overall elapsed time.
- Database/disk isolation passes for three children. A child exiting seven
  remains a terminal failure while its siblings succeed. Completed replay
  produces zero attempts. After cancellation, two committed terminal objects
  remain unchanged and the unfinished third child produces a successful result
  on resubmission. Existing retry accounting counts skipped results as attempts
  and logs append-only attempt-record collisions; do not treat that summary
  count as a count of newly executed VMs. This pre-existing issue is deferred.
- Three native forks also pass with the invoking process in a fresh network
  namespace and AWS credential access disabled. Unauthenticated S3 access is
  denied (anonymous object HEAD returns 403; native image pull cannot resolve
  credentials). Node-role provider-chain integration passes; actual credential
  expiry/rotation is not exercised during the short test window.
- The cold warm-up initially fails at the CLI's 30-second execution limit and
  leaves incomplete checkpoint chunks. After the deadline/cleanup fixes, the
  agent removes that same incomplete output and the cold 89-second capture
  succeeds. Identical 31-second interactive run and named-exec commands fail
  before the fix and complete with their expected output afterward.

These remain small-fixture, single-sample results, not large-suite speedup,
cluster-DNS, production isolation or cross-node portability evidence. The full
Buildkite browser workload remains unproven, including its Docker cleanup
blocker. Distributed scheduling remains a separate PR as agreed; external
services and named-fork networking retain the limits above. Published runtime
image tags and chart defaults remain unchanged; merge and release still require
approval.

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
- Model interactive runs and persistent sandboxes around one immutable template
  and child-execution engine.
- Keep source, prepare, fork, shard, and result primitives for batch fan-out.
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
- No general workflow engine.
- No caller-defined prepare command in the ordinary interactive API. Advanced
  application templates can be added only after they have a clean lifecycle
  contract.

## Target Model

### Interactive Run And Sandbox APIs

The latency-sensitive API is intentionally smaller than the batch contract.
Callers select immutable image content and a command; the agent owns template
creation and child lifecycle:

```yaml
runID: node-version-0001
image: docker.io/library/node@sha256:...
memory: 1gb
command:
  - /bin/sh
  - -lc
  - node -v
```

`POST /runs` ensures a boot template for the digest-pinned image, restores a
fresh ephemeral child with `spore run --from`, executes the command, and
discards the child. The response includes SporeVM events, node-local timings,
and whether template creation was a cache hit.

The response separates agent wall time from SporeVM runtime phases. Alongside
`templateMs`, `executionMs`, and `totalMs`, `timingsMs` reports
`runtimeStartMs`, `runtimeVSockConnectMs`, `runtimeExecResponseMs`, and
`runtimeProbeMs` from the terminal event. This keeps VM start, guest connection,
command completion, and compatibility probing independently measurable.

Published runtime acceptance uses an isolated agent and coordinator from the
deployed image on a compatible node. A same-node client verifies image and
SporeVM provenance, cold-parent and template-hit `/runs`, named sandbox create,
first and warm exec, slot recovery, durable template-pin release, and cleanup
of the temporary Kubernetes resources. This keeps release proof reproducible
without making a Kubernetes object part of an ordinary run request.

`POST /sandboxes` accepts `name`, `image`, and optional `memory`; interactive
requests default to `1024mb`. It ensures the
same boot template, restores a named child, treats successful SporeVM restore
completion as exec-ready, and keeps that child alive for
`POST /sandboxes/{name}/exec` until `DELETE /sandboxes/{name}`. A sandbox owns
one execution slot for its lifetime; its filesystem state survives multiple
exec calls.

The public ComputeSDK adapter maps its sandbox contract directly onto this
lifecycle: create restores a named child, `runCommand` decodes exec events, and
destroy deletes the child and releases its slot. The upstream sequential
benchmark therefore measures sandbox create plus first exec with its own
identity and cleanup checks unchanged; `/runs` remains the distinct one-shot
fresh-child contract.

Boot templates are automatic, immutable cache entries. Their identity includes
the image digest, memory, backend, SporeVM version, the SHA-256 of the spore
binary, host class, and template schema version. The binary digest keeps two
builds that report the same version from sharing templates; the agent hashes
the binary again only when its file identity, size, mtime, or ctime changes.
Mutable image tags are rejected so a cache hit cannot silently select different
bytes. The first request may pay image/rootfs resolution, boot, and capture.
Later requests use the published template without forking, packing, pulling, or
writing generation metadata.
SporeVM 0.13.0 disk-backed saves own durable host-cache pins, so failed
temporary captures are removed through `spore rm --spore`; raw directory
deletion would leak the pin.
The agent reconciles this cache before serving and periodically thereafter.
Each pass removes abandoned builds, incomplete entries, and templates saved by
another spore binary, host class, or backend, keeps the newest 16 completed
templates by default, skips templates currently restoring a child, and removes
every evicted template through `spore rm --spore` before deleting its metadata.
Newer SporeVM releases a removed save's pin in `spore cache gc --force` rather
than in `rm --spore`. The chart's agent owns the node's SporeVM caches, so it
runs GC (`--cache-gc`): once at startup, reporting not ready until GC
finishes, and after a periodic pass if it has removed a saved spore since the
last collection. GC roots only VMs under the agent's own runtime directory and
pins its own SporeVM release reads, so agents that share the node cache
without owning it, such as those the acceptance and probe scripts start, must
not run it. Operators can set the count and interval with the chart's
`agent.cache.templateRetain` and `agent.cache.templateReconcileInterval`
values; a zero interval keeps startup reconciliation but disables the periodic
pass.
The agent also keeps SporeVM's local RAM-backing authority under the persistent
work root so retained template proofs survive pod replacement. Before serving,
it removes every other entry from that runtime directory: named VM metadata,
PIDs, sockets, fork batches, and active leases are process-local and must not
cross pod PID namespaces.
The current boot-template capture requires `/bin/true` in the selected image; a
commandless capture primitive is deferred.

### Batch Fan-Out Contract

The submitted batch contract remains general enough for CI, simulations,
fuzzing, browser swarms, and agent workloads that need an application-specific
capture point and many child identities:

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

The lower-level bundle run remains useful for prebuilt bundles. The
higher-level batch run is the normal CI entrypoint; its `children.command`
compiles to the lower-level `childCommand`.

A cell runs one agent until multi-agent bundle handoff exists. That agent gets
one sequential lease for the full child range and runs at most
`maxInFlightPerAgent` children at once.

### Fleet Components

`sporectl` is the user-facing submitter. CI should use the same run submit
path as every other caller: render or provide a run document, then call
`sporectl submit RUN.json`. The submitter infers whether the document is a
source/prepare/fork run or a lower-level prebuilt bundle run. A
separate CI subcommand can wait until the same CI-only defaults are repeated
enough to justify it.

`spore-coordinator` owns one run when invoked as a batch command. It validates
the run, chooses one compatible agent, prepares or references the bundle, leases
the child range, tracks compact aggregate state, and exits with a clear run
result.

For latency-sensitive callers, the same coordinator binary runs as a resident
API. `POST /runs` accepts the compact ephemeral run request and proxies it to an
available agent. `/sandboxes` exposes the persistent named-child lifecycle.
Neither path creates a Kubernetes Job or enters the batch prepare/fork planner.

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

### Interactive Lifecycle

1. Require a digest-pinned OCI image and validate slot and pressure admission.
2. Derive the boot-template identity from image, memory, backend, SporeVM
   version and binary digest, template schema, and exact host class.
3. On a cache miss, run `/bin/true` with `spore run --save` and atomically
   publish the captured template. On a hit, reuse the immutable template.
4. For `/runs`, execute the command with `spore run --from` and discard the
   fresh child.
5. For `/sandboxes`, restore a named child, preserve it across exec calls, and
   release its slot only when deleted.

### Batch Lifecycle

1. Resolve the source image, rootfs, existing spore, or prebuilt bundle.
2. Prepare a parent VM by running the warm command until the capture point.
3. Capture and fork the parent into `N` children.
4. Pack the child bundle under immutable digest identity.
5. Admit the run against one compatible agent with honest slots.
6. Lease the child range to that agent.
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

Admission compares one compatible agent against in-flight slot demand, not
total child count. Prebuilt bundle runs use the same single-agent lease until
multi-agent handoff exists.

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
- boot-template cache identity and hit or miss;
- boot-template capture duration;
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
- Public release `v0.1.14` pins SporeVM 0.13.0 and consumes its proof-gated
  named restore, truthful readiness timing, and durable saved-spore storage
  contract.
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
- `sporectl submit --buildkite RUN.json` preserves that execution path, waits
  for the aggregate result, parses the coordinator report, and posts a compact
  Buildkite annotation. An optional result-browser URL turns the result object
  prefix into a link without putting environment-specific locations here.
- Agents and batch coordinators can now use a durable S3 result store through
  the same `ResultStore` contract as local smokes. Attempts and terminals use
  conditional `PutObject` with `If-None-Match: *`, so retries cannot overwrite
  an existing attempt or committed terminal object.
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
  child-command runs. Public runtime `0.1.11` shipped that contract with
  SporeVM 0.11.0; runtime `0.1.12` updates the pin to SporeVM 0.11.1.
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
- Source runs release every fully published prepared child and parent through
  SporeVM's saved checkpoint removal contract after successful or failed
  execution. A preparation root that fails before publishing a valid supported
  checkpoint is retained for diagnosis/operator recovery rather than removed
  raw. One-shot coordinators propagate interruption into the run, then perform
  detached, bounded cleanup. Agent
  prepare and release operations are serialized per unique active run ID, so a
  release following an ambiguous prepare response waits for preparation to
  unwind. Cancellation retries only the structured
  `SavedSporeInUse` race, and raw work-directory removal happens only after pin
  release succeeds; the batch-owned `shared-chunks` directory is not treated as
  a checkpoint. Cleanup first persists a pending-path ledger and only advances
  it after confirmed removals. Missing pending paths are sent through SporeVM
  and succeed only when its exact durable removal journal authorizes resumption;
  arbitrary missing paths and unsafe filesystem types fail closed and retain
  the ledger and prepare root for diagnosis.
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
- Public runtime `0.1.11` shipped `spore run --from --generation`. An in-cluster
  benchmark client measured steady one-child Node `POST /runs` at about 2.03s:
  child resume fell to about 33ms, while hot parent capture regressed to about
  1.96s. The first request after the storage upgrade rebuilt the rootfs cache
  and took about 18.1s.
- SporeVM 0.11.1 fixes that parent-capture regression by reusing the canonical
  rootfs index and publishing unchanged CAS objects without a full logical
  rootfs scan. ReleaseSafe Linux ARM64/KVM measured hot capture at 312ms while
  preserving 34-35ms run-from. Public runtime `0.1.12` carries that fix and
  measured one-child Node `POST /runs` at 319.61ms median and 348.80ms p95 from
  an in-cluster benchmark client. Parent capture was about 248.6ms, fork about
  1.5ms, and child resume about 35.1ms, with 100% success across ten requests.
- The resident API implementation now separates interactive execution from the
  batch fan-out contract. `/runs` accepts only `runID`, a digest-pinned image,
  optional memory, and a command. `/sandboxes` uses the same automatic boot
  template but restores a persistent named child. The first request captures
  and atomically publishes a versioned host-compatible template; subsequent
  requests execute directly from it. An unreleased in-cluster probe measured a
  cold-parent Node run at 388.41ms wall and five template hits at 149.34ms
  median, with about 119ms inside `spore run --from`. A template-hit sandbox
  create took 793.69ms wall because named restore readiness took 748.60ms; its
  first `node -v` exec took 120.70ms and the next nine had a 33.59ms median.
- A durable public `0.1.13` run measured template-hit Node `/runs` at 153.68ms
  median. An unreleased adapter probe with SporeVM 0.12.0 then cached immutable
  host class and five-second cache-root metrics, reducing template-hit median
  to 119.22ms while node execution remained about 118ms. SporeVM's truthful
  named restore took 754.71ms on the same node, after which a no-op exec took
  4.19ms; first and repeated Node execs measured 93.92ms and 6.56ms median. A
  request that refreshes the five-second cache metrics can still pay the
  recursive directory walk; move that refresh off-request only if measured
  tail latency warrants the added lifecycle machinery.
- An unreleased SporeVM 0.13.0 adapter probe reduced template-hit named sandbox
  restore to 51.64ms on the same KVM node: 19ms prepare and 31ms exec readiness.
  End-to-end create was 91.98ms, the first Node exec was 105.18ms, and the next
  nine Node execs had a 6.71ms median. Template-hit `/runs` measured 136.13ms
  median. The first request after the disk-format upgrade took 2.12s; with the
  rootfs cache warm, a fresh parent capture took 381.00ms wall, so the first
  post-upgrade cache-rebuild sample must remain separate from steady-state
  cold-parent measurements.

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
smokes in a compatible Kubernetes cell. The resident API is deployed and has
been measured from an in-cluster client. Multi-agent bundle handoff remains
pending.

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

Status: the public `sporectl submit --buildkite` profile is implemented. It
waits for aggregate completion, exits with the run result, and posts aggregate
counts plus an optional result-browser link. Wiring a private Rails/RSpec run
into a cluster-authenticated pipeline remains an ops concern. The intended CLI
path remains `sporectl submit sporevm-run.json`, not a separate `sporectl ci`
command.

Add the smallest CI-specific submit behavior on top of the run.

Done when:

- a CI step can submit a Rails/RSpec fan-out run;
- the step waits for aggregate completion;
- failures produce an annotation and links to child logs;
- the step exits with the aggregate run result.

### Slice 6: Honest 1,000-Child Scale

Status: implemented for one compatible single-agent cell. A live 1,000-child
busybox run has completed with 100 in-flight child slots. The next benchmark
work is to turn this into repeatable cluster measurements instead of one-off
smoke evidence.

Scale by adding real capacity or reducing verified per-child requirements, not
by overstating slots.

Done when:

- one agent can honestly advertise enough in-flight slots;
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

The direct same-agent path isolated resume/command overhead from restore cleanup
and local bundle pull. SporeVM's new block-storage contract records
image-created rootfs state as chunked rootfs storage and writable rootfs state
as sealed disk indexes. That should still improve portable multi-agent bundles,
but the single-agent `POST /runs` adapter now has a local prepare path that
performs `prepare -> fork -> run-from` without `pack -> inspect-bundle -> pull`
when the same agent prepares and executes a single-attempt child. Retry-enabled
runs keep the portable bundle path so later attempts can materialize a clean
child.

The remaining optimization order is:

- split the remaining template-hit `spore run --from` floor into VM resume and
  application-cold command work without adding caller-defined prepare steps;
- split portable bundle timings for `pack`, `inspectBundle`, `pullVerify`,
  `pullInstallIndexes`, `pullInstallChunks`, and child manifest writes so
  multi-agent pack/pull stops being one opaque bucket;
- for real multi-agent fan-out, keep the portable bundle contract but push
  SporeVM toward index-aware or lazy `pull` so a selected child can restore
  from verified indexes and node-local chunks without full eager
  materialization;
- teach placement and preheat decisions to prefer agents that already have the
  needed rootfs/disk indexes and chunks on node-local cache.

### Slice 7: Shared Interactive Execution Engine

Status: implemented and released in public runtime `v0.1.14`; private rollout
is tracked separately. This runtime consumes SporeVM 0.13.0's proof-gated named
restore, removes the adapter's synthetic sandbox readiness exec, consumes named
lifecycle timing directly, and releases durable saved-spore pins during failed
publication.

Use one automatic immutable boot-template cache for ephemeral `/runs` and
persistent `/sandboxes`. Keep the batch source/prepare/fork planner behind
`sporectl submit` rather than exposing it as the ordinary sandbox API.

Done when:

- `/runs` executes one command in a fresh child without pack, pull, fork, or a
  Kubernetes Job;
- `/sandboxes` restores from the same template and preserves state across execs;
- mutable image tags are rejected and template identity includes runtime and
  host compatibility inputs;
- sandbox lifetime reserves an honest agent slot;
- the dev probe measures from inside the cluster and labels cold-parent and
  template-hit samples separately.

### Slice 8: Optional Kubernetes UX

Add CRDs, Kueue, or an operator only after the run and CI paths work.

Done when:

- one `SporeRun` maps to one coordinator run;
- status remains aggregate;
- Kueue gates coarse admission without becoming the child scheduler;
- existing `sporectl` flows still work without CRDs.

## Verification

- Go contract validation for every fleet example.
- Unit tests for create-only result commits and terminal-result
  short-circuiting.
- Agent tests for prepare, fork, pack, automatic boot-template reuse, sandbox
  slot ownership, slot admission, cancellation, cleanup, and platform mismatch.
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
- Use S3 conditional writes as the first durable result backend; keep the local
  URI mapper for deterministic tests and smokes.
- Keep KVM, credentials, caches, and SporeVM execution inside agents.
- `/runs` means one fresh ephemeral child and one command. `/sandboxes` means a
  caller-owned persistent named child with repeated execs.
- Interactive runs and sandboxes share automatic immutable boot templates.
  Caller-defined prepare commands remain a batch fan-out concern.
- Require digest-pinned images for interactive template reuse.
- A cell runs one agent until multi-agent bundle handoff exists; the
  coordinator gives it one sequential lease bounded by `maxInFlightPerAgent`.
- Public runtime images publish to GHCR; private environments can override the
  image repository from their ops values.
- Runtime `v0.1.17` pins SporeVM 0.16.0, normalizes ARM host-info v2/v3 into the
  saved-state `aarch64` fleet contract, and consumes
  `spore.automation.event.v1` completion outcomes for every streamed command.
- Keep the released fleet on ARM64 saved-state hosts. SporeVM host-info v3 can
  describe AMD64, but the adapter rejects that architecture until saved-state
  restore support reaches parity. Guest memory is an explicit fixed size;
  removed `memory: auto` requests are rejected rather than silently translated.
- The public `main` branch requires the `buildkite/sporevm-k8s` status check.
- `spore-coordinator` treats an aggregate runtime report with
  `state != succeeded` as a failed run even when the container reached the end
  of its process.
- Prepared source-run state is agent-owned. Coordinators request idempotent
  release even when the prepare response is uncertain, and cleanup failures
  fail the run rather than hiding durable cache-pin leaks.
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
- Template cache byte-size limits and pressure-aware eviction.
- Durable sandbox ownership routing and restart reconciliation across multiple
  agents.
- Explicit application-template creation and versioning, if automatic boot
  templates prove insufficient.
- A commandless SporeVM boot-capture primitive so automatic templates do not
  depend on `/bin/true` existing in the selected image.

## Open Questions

- How much CI UI polish is needed beyond the aggregate annotation and result
  browser link?

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
- Signal-based parent capture is an advanced batch fan-out mechanism for
  long-lived application preparation. It is not part of ordinary `/runs` or
  sandbox creation.
- Rails/RSpec proves that plain child `spore resume` is not equivalent to
  `spore fanout`: sharded workloads need stable `/run/sporevm/env` or
  `/run/sporevm/generation.json` identity on every resumed child.
- The 100-child run held on one compatible KVM node with explicit 512 MB guest
  memory, which moves the next pressure point to honest 1,000-child capacity.
- The interactive floor must distinguish cold template creation, immutable
  template hits, and already-running sandbox pools. CRDs and Kueue can wait.
