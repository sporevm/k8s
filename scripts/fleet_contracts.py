from __future__ import annotations

import base64
import json
import re
from pathlib import Path
from typing import Any


class ContractError(ValueError):
    pass


DIGEST_RE = re.compile(r"^sha256:[a-f0-9]{64}$")
ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{2,127}$")
HOST_CLASS_FIELDS = {
    "id",
    "sporePlatformVersion",
    "architecture",
    "backend",
    "cpuProfile",
    "deviceModel",
}
TIMING_FIELDS = {
    "artifactPull",
    "materialization",
    "resume",
    "guestReady",
    "resultCommit",
}


def load_json(path: Path) -> Any:
    with path.open() as f:
        return json.load(f)


def validate_id(value: Any, path: str) -> None:
    if not isinstance(value, str) or ID_RE.match(value) is None:
        raise ContractError(f"{path} must be a stable lowercase id")


def validate_digest(value: Any, path: str) -> None:
    if not isinstance(value, str) or DIGEST_RE.match(value) is None:
        raise ContractError(f"{path} must be a sha256 digest")


def validate_s3_json_uri(value: Any, path: str) -> None:
    if not isinstance(value, str) or not value.startswith("s3://") or not value.endswith(".json"):
        raise ContractError(f"{path} must be an s3 JSON object URI")
    bucket_and_key = value.removeprefix("s3://").split("/", 1)
    if len(bucket_and_key) != 2 or not bucket_and_key[0] or not bucket_and_key[1]:
        raise ContractError(f"{path} must be an s3 JSON object URI")


def validate_s3_prefix_uri(value: Any, path: str) -> None:
    if not isinstance(value, str) or not value.startswith("s3://") or not value.endswith("/"):
        raise ContractError(f"{path} must be an s3 prefix URI ending in /")
    bucket_and_key = value.removeprefix("s3://").split("/", 1)
    if len(bucket_and_key) != 2 or not bucket_and_key[0] or not bucket_and_key[1]:
        raise ContractError(f"{path} must be an s3 prefix URI ending in /")


def require_object(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ContractError(f"{path} must be an object")
    return value


def require_keys(value: dict[str, Any], keys: set[str], path: str) -> None:
    missing = sorted(keys - value.keys())
    if missing:
        raise ContractError(f"{path} missing required keys: {', '.join(missing)}")


def require_int(value: Any, path: str, minimum: int = 0) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise ContractError(f"{path} must be an integer >= {minimum}")
    return value


def require_number(value: Any, path: str, minimum: float = 0.0) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value < minimum:
        raise ContractError(f"{path} must be a number >= {minimum}")
    return float(value)


def validate_host_class(host_class: Any, path: str = "hostClass") -> None:
    host = require_object(host_class, path)
    require_keys(host, HOST_CLASS_FIELDS, path)
    if set(host.keys()) != HOST_CLASS_FIELDS:
        extra = sorted(set(host.keys()) - HOST_CLASS_FIELDS)
        raise ContractError(f"{path} has unsupported keys: {', '.join(extra)}")
    validate_id(host["id"], f"{path}.id")
    if host["sporePlatformVersion"] != "v0":
        raise ContractError(f"{path}.sporePlatformVersion must be v0")
    if host["architecture"] != "aarch64":
        raise ContractError(f"{path}.architecture must be aarch64")
    if host["backend"] != "kvm":
        raise ContractError(f"{path}.backend must be kvm")
    for key in ("cpuProfile", "deviceModel"):
        if not isinstance(host[key], str) or not host[key]:
            raise ContractError(f"{path}.{key} must not be empty")


def validate_run(run: Any) -> None:
    run = require_object(run, "run")
    require_keys(
        run,
        {
            "runID",
            "bundle",
            "children",
            "hostClass",
            "execution",
            "retryPolicy",
            "sideEffects",
            "resultStore",
        },
        "run",
    )
    validate_id(run["runID"], "run.runID")

    bundle = require_object(run["bundle"], "run.bundle")
    require_keys(bundle, {"uri", "digest"}, "run.bundle")
    if not isinstance(bundle["uri"], str) or not bundle["uri"]:
        raise ContractError("run.bundle.uri must not be empty")
    validate_digest(bundle["digest"], "run.bundle.digest")

    children = require_object(run["children"], "run.children")
    require_keys(children, {"start", "count"}, "run.children")
    require_int(children["start"], "run.children.start", 0)
    require_int(children["count"], "run.children.count", 1)

    validate_host_class(run["hostClass"], "run.hostClass")

    execution = require_object(run["execution"], "run.execution")
    require_keys(execution, {"childrenPerShard", "maxInFlightPerAgent"}, "run.execution")
    children_per_shard = require_int(
        execution["childrenPerShard"], "run.execution.childrenPerShard", 1
    )
    require_int(execution["maxInFlightPerAgent"], "run.execution.maxInFlightPerAgent", 1)
    if children_per_shard > children["count"]:
        raise ContractError("run.execution.childrenPerShard cannot exceed child count")

    retry_policy = require_object(run["retryPolicy"], "run.retryPolicy")
    require_keys(
        retry_policy,
        {"maxAttemptsPerChild", "rerunCommittedChildren"},
        "run.retryPolicy",
    )
    require_int(retry_policy["maxAttemptsPerChild"], "run.retryPolicy.maxAttemptsPerChild", 1)
    if retry_policy["rerunCommittedChildren"] is not False:
        raise ContractError("run.retryPolicy.rerunCommittedChildren must be false")

    side_effects = require_object(run["sideEffects"], "run.sideEffects")
    require_keys(side_effects, {"idempotencyRequired"}, "run.sideEffects")
    if side_effects["idempotencyRequired"] is not True:
        raise ContractError("run.sideEffects.idempotencyRequired must be true")

    validate_s3_prefix_uri(run["resultStore"], "run.resultStore")


def validate_command_spec(command_spec: Any, path: str, allow_capture: bool = False) -> None:
    spec = require_object(command_spec, path)
    require_keys(spec, {"command"}, path)
    allowed = {"command"}
    if allow_capture:
        allowed |= {"captureSignal", "readyMarker"}
    extra = sorted(set(spec.keys()) - allowed)
    if extra:
        raise ContractError(f"{path} has unsupported keys: {', '.join(extra)}")
    command = spec["command"]
    if not isinstance(command, list) or not command:
        raise ContractError(f"{path}.command must be a non-empty array")
    for i, arg in enumerate(command):
        if not isinstance(arg, str) or not arg:
            raise ContractError(f"{path}.command[{i}] must not be empty")
    if not allow_capture:
        return
    capture_signal = spec.get("captureSignal")
    ready_marker = spec.get("readyMarker")
    if capture_signal is None and ready_marker is None:
        return
    if capture_signal != "USR1":
        raise ContractError(f"{path}.captureSignal must be USR1")
    if not isinstance(ready_marker, str) or not ready_marker:
        raise ContractError(f"{path}.readyMarker must not be empty")


def validate_generic_run(run: Any) -> None:
    run = require_object(run, "genericRun")
    require_keys(
        run,
        {
            "runID",
            "source",
            "prepare",
            "fork",
            "children",
            "execution",
            "retryPolicy",
            "sideEffects",
            "resultStore",
        },
        "genericRun",
    )
    validate_id(run["runID"], "genericRun.runID")

    source = require_object(run["source"], "genericRun.source")
    require_keys(source, {"image", "platform"}, "genericRun.source")
    if not isinstance(source["image"], str) or not source["image"]:
        raise ContractError("genericRun.source.image must not be empty")
    if source["platform"] != "linux/arm64":
        raise ContractError("genericRun.source.platform must be linux/arm64")

    validate_command_spec(run["prepare"], "genericRun.prepare", allow_capture=True)

    fork = require_object(run["fork"], "genericRun.fork")
    require_keys(fork, {"count"}, "genericRun.fork")
    fork_count = require_int(fork["count"], "genericRun.fork.count", 1)

    children = require_object(run["children"], "genericRun.children")
    require_keys(children, {"start", "count", "command"}, "genericRun.children")
    child_start = require_int(children["start"], "genericRun.children.start", 0)
    child_count = require_int(children["count"], "genericRun.children.count", 1)
    if child_start + child_count > fork_count:
        raise ContractError("genericRun.children range must fit fork.count")
    validate_command_spec({"command": children["command"]}, "genericRun.children")

    execution = require_object(run["execution"], "genericRun.execution")
    require_keys(execution, {"childrenPerShard", "maxInFlightPerAgent"}, "genericRun.execution")
    children_per_shard = require_int(
        execution["childrenPerShard"], "genericRun.execution.childrenPerShard", 1
    )
    require_int(execution["maxInFlightPerAgent"], "genericRun.execution.maxInFlightPerAgent", 1)
    if children_per_shard > child_count:
        raise ContractError("genericRun.execution.childrenPerShard cannot exceed child count")

    retry_policy = require_object(run["retryPolicy"], "genericRun.retryPolicy")
    require_keys(
        retry_policy,
        {"maxAttemptsPerChild", "rerunCommittedChildren"},
        "genericRun.retryPolicy",
    )
    require_int(
        retry_policy["maxAttemptsPerChild"], "genericRun.retryPolicy.maxAttemptsPerChild", 1
    )
    if retry_policy["rerunCommittedChildren"] is not False:
        raise ContractError("genericRun.retryPolicy.rerunCommittedChildren must be false")

    side_effects = require_object(run["sideEffects"], "genericRun.sideEffects")
    require_keys(side_effects, {"idempotencyRequired"}, "genericRun.sideEffects")
    if side_effects["idempotencyRequired"] is not True:
        raise ContractError("genericRun.sideEffects.idempotencyRequired must be true")

    validate_s3_prefix_uri(run["resultStore"], "genericRun.resultStore")


def child_range(document: dict[str, Any], start_key: str, count_key: str) -> tuple[int, int]:
    start = require_int(document[start_key], start_key, 0)
    count = require_int(document[count_key], count_key, 1)
    return start, start + count


def derive_shard_ranges(run: dict[str, Any]) -> list[tuple[int, int]]:
    validate_run(run)
    start = run["children"]["start"]
    count = run["children"]["count"]
    size = run["execution"]["childrenPerShard"]
    ranges = []
    child = start
    end = start + count
    while child < end:
        next_child = min(child + size, end)
        ranges.append((child, next_child))
        child = next_child
    return ranges


def reject_overlapping_ranges(ranges: list[tuple[int, int]]) -> None:
    previous_end = None
    for start, end in sorted(ranges):
        if start >= end:
            raise ContractError("child ranges must be non-empty")
        if previous_end is not None and start < previous_end:
            raise ContractError("child ranges must not overlap")
        previous_end = end


def validate_complete_coverage(
    ranges: list[tuple[int, int]], expected_start: int, expected_count: int
) -> None:
    reject_overlapping_ranges(ranges)
    expected_end = expected_start + expected_count
    if not ranges:
        raise ContractError("no shard ranges provided")
    sorted_ranges = sorted(ranges)
    if sorted_ranges[0][0] != expected_start or sorted_ranges[-1][1] != expected_end:
        raise ContractError("shard ranges do not cover the run child range")
    cursor = expected_start
    for start, end in sorted_ranges:
        if start != cursor:
            raise ContractError("shard ranges have a gap")
        cursor = end


def validate_shard_lease(lease: Any, run: dict[str, Any] | None = None) -> None:
    lease = require_object(lease, "shardLease")
    require_keys(
        lease,
        {
            "runID",
            "bundleDigest",
            "shardID",
            "childStart",
            "childCount",
            "attemptBudget",
            "hostClassID",
            "agentID",
            "leaseDeadline",
        },
        "shardLease",
    )
    validate_id(lease["runID"], "shardLease.runID")
    validate_digest(lease["bundleDigest"], "shardLease.bundleDigest")
    validate_id(lease["shardID"], "shardLease.shardID")
    child_range(lease, "childStart", "childCount")
    require_int(lease["attemptBudget"], "shardLease.attemptBudget", 1)
    validate_id(lease["hostClassID"], "shardLease.hostClassID")
    validate_id(lease["agentID"], "shardLease.agentID")

    if run is not None:
        validate_run(run)
        if lease["runID"] != run["runID"]:
            raise ContractError("shardLease.runID does not match run.runID")
        if lease["bundleDigest"] != run["bundle"]["digest"]:
            raise ContractError("shardLease.bundleDigest does not match run bundle digest")
        if lease["hostClassID"] != run["hostClass"]["id"]:
            raise ContractError("shardLease.hostClassID does not match run host class")
        if lease["attemptBudget"] > run["retryPolicy"]["maxAttemptsPerChild"]:
            raise ContractError("shardLease.attemptBudget exceeds run retry budget")
        run_start = run["children"]["start"]
        run_end = run_start + run["children"]["count"]
        lease_start, lease_end = child_range(lease, "childStart", "childCount")
        if lease_start < run_start or lease_end > run_end:
            raise ContractError("shardLease child range is outside the run")


def attempt_key(run_id: str, bundle_digest: str, child_id: int, attempt_number: int) -> str:
    validate_id(run_id, "runID")
    validate_digest(bundle_digest, "bundleDigest")
    require_int(child_id, "childID", 0)
    require_int(attempt_number, "attemptNumber", 1)
    return f"{run_id}/{bundle_digest}/children/{child_id}/attempts/{attempt_number}"


def terminal_result_key(run_id: str, child_id: int) -> str:
    validate_id(run_id, "runID")
    require_int(child_id, "childID", 0)
    return f"{run_id}/children/{child_id}/terminal.json"


def validate_agent_status(status: Any) -> None:
    status = require_object(status, "agentStatus")
    require_keys(
        status,
        {
            "agentID",
            "cellID",
            "observedAt",
            "hostClass",
            "executionSlots",
            "cache",
            "pressure",
            "healthy",
        },
        "agentStatus",
    )
    validate_id(status["agentID"], "agentStatus.agentID")
    validate_id(status["cellID"], "agentStatus.cellID")
    validate_host_class(status["hostClass"], "agentStatus.hostClass")

    slots = require_object(status["executionSlots"], "agentStatus.executionSlots")
    require_keys(slots, {"total", "available"}, "agentStatus.executionSlots")
    total = require_int(slots["total"], "agentStatus.executionSlots.total", 1)
    available = require_int(slots["available"], "agentStatus.executionSlots.available", 0)
    if available > total:
        raise ContractError("agentStatus.executionSlots.available cannot exceed total")

    cache = require_object(status["cache"], "agentStatus.cache")
    require_keys(
        cache,
        {
            "bundleCacheBytes",
            "rootfsCacheBytes",
            "bundleCacheEntries",
            "rootfsCacheEntries",
        },
        "agentStatus.cache",
    )
    for key in cache:
        require_int(cache[key], f"agentStatus.cache.{key}", 0)

    pressure = require_object(status["pressure"], "agentStatus.pressure")
    require_keys(pressure, {"disk", "memory"}, "agentStatus.pressure")
    for key in ("disk", "memory"):
        if pressure[key] not in {"normal", "warning", "critical"}:
            raise ContractError(f"agentStatus.pressure.{key} has unsupported value")
    if not isinstance(status["healthy"], bool):
        raise ContractError("agentStatus.healthy must be boolean")
    if status["healthy"] and "critical" in pressure.values():
        raise ContractError("agentStatus cannot be healthy under critical pressure")


def validate_attempt_result(result: Any, lease: dict[str, Any] | None = None) -> None:
    result = require_object(result, "attemptResult")
    require_keys(
        result,
        {
            "runID",
            "bundleDigest",
            "childID",
            "attemptID",
            "agentID",
            "shardID",
            "status",
            "startedAt",
            "finishedAt",
            "timingsMs",
            "terminal",
        },
        "attemptResult",
    )
    validate_id(result["runID"], "attemptResult.runID")
    validate_digest(result["bundleDigest"], "attemptResult.bundleDigest")
    child_id = require_int(result["childID"], "attemptResult.childID", 0)
    validate_id(result["attemptID"], "attemptResult.attemptID")
    validate_id(result["agentID"], "attemptResult.agentID")
    validate_id(result["shardID"], "attemptResult.shardID")
    if result["status"] not in {
        "succeeded",
        "failed",
        "skipped-terminal-exists",
        "platform-mismatch",
    }:
        raise ContractError("attemptResult.status has unsupported value")
    if not isinstance(result["terminal"], bool):
        raise ContractError("attemptResult.terminal must be boolean")
    if result["status"] in {"succeeded", "skipped-terminal-exists"}:
        if result["terminal"] is not True:
            raise ContractError("successful or skipped attemptResult must be terminal")
        if "resultURI" not in result:
            raise ContractError("successful or skipped attemptResult requires resultURI")
        validate_s3_json_uri(result["resultURI"], "attemptResult.resultURI")
    if result["status"] in {"failed", "platform-mismatch"} and "error" not in result:
        raise ContractError("failed attemptResult requires error")

    timings = require_object(result["timingsMs"], "attemptResult.timingsMs")
    require_keys(timings, TIMING_FIELDS, "attemptResult.timingsMs")
    for key in TIMING_FIELDS:
        require_number(timings[key], f"attemptResult.timingsMs.{key}", 0.0)

    if "output" in result:
        validate_attempt_output(result["output"])

    if lease is not None:
        validate_shard_lease(lease)
        if result["runID"] != lease["runID"]:
            raise ContractError("attemptResult.runID does not match shard lease")
        if result["bundleDigest"] != lease["bundleDigest"]:
            raise ContractError("attemptResult.bundleDigest does not match shard lease")
        if result["agentID"] != lease["agentID"]:
            raise ContractError("attemptResult.agentID does not match shard lease")
        if result["shardID"] != lease["shardID"]:
            raise ContractError("attemptResult.shardID does not match shard lease")
        start, end = child_range(lease, "childStart", "childCount")
        if child_id < start or child_id >= end:
            raise ContractError("attemptResult.childID is outside the shard lease")


def validate_attempt_output(output: Any) -> None:
    output = require_object(output, "attemptResult.output")
    allowed = {
        "stdoutBytes",
        "stderrBytes",
        "stdoutPreviewBase64",
        "stderrPreviewBase64",
        "stdoutTruncated",
        "stderrTruncated",
    }
    for key in output:
        if key not in allowed:
            raise ContractError(f"attemptResult.output has unknown field {key}")
    stdout_bytes = require_int(output.get("stdoutBytes", 0), "attemptResult.output.stdoutBytes", 0)
    stderr_bytes = require_int(output.get("stderrBytes", 0), "attemptResult.output.stderrBytes", 0)
    validate_output_preview(
        output.get("stdoutPreviewBase64", ""),
        stdout_bytes,
        "attemptResult.output.stdoutPreviewBase64",
    )
    validate_output_preview(
        output.get("stderrPreviewBase64", ""),
        stderr_bytes,
        "attemptResult.output.stderrPreviewBase64",
    )
    for key in ("stdoutTruncated", "stderrTruncated"):
        if key in output and not isinstance(output[key], bool):
            raise ContractError(f"attemptResult.output.{key} must be boolean")


def validate_output_preview(value: Any, total_bytes: int, path: str) -> None:
    if value == "":
        return
    if not isinstance(value, str):
        raise ContractError(f"{path} must be base64")
    try:
        decoded = base64.b64decode(value, validate=True)
    except ValueError as err:
        raise ContractError(f"{path} must be base64") from err
    if len(decoded) > total_bytes:
        raise ContractError(f"{path} must not exceed total output bytes")


def validate_benchmark_summary(summary: Any, run: dict[str, Any] | None = None) -> None:
    summary = require_object(summary, "benchmarkSummary")
    require_keys(
        summary,
        {
            "runID",
            "childCount",
            "targetConcurrency",
            "cachePosture",
            "successRate",
            "admissionLatencyMs",
            "timeToFirstChildReadyMs",
            "timeToTargetConcurrencyMs",
            "stagePercentilesMs",
            "originBytes",
            "cacheHitRate",
        },
        "benchmarkSummary",
    )
    validate_id(summary["runID"], "benchmarkSummary.runID")
    child_count = require_int(summary["childCount"], "benchmarkSummary.childCount", 1)
    target = require_int(summary["targetConcurrency"], "benchmarkSummary.targetConcurrency", 1)
    if target > child_count:
        raise ContractError("benchmarkSummary.targetConcurrency cannot exceed childCount")
    if summary["cachePosture"] not in {
        "cold-origin-cold-cache",
        "warm-bundle-cold-materialization",
        "warm-node-local-cache",
    }:
        raise ContractError("benchmarkSummary.cachePosture has unsupported value")
    success_rate = require_number(summary["successRate"], "benchmarkSummary.successRate", 0.0)
    cache_hit_rate = require_number(summary["cacheHitRate"], "benchmarkSummary.cacheHitRate", 0.0)
    if success_rate > 1 or cache_hit_rate > 1:
        raise ContractError("rates cannot exceed 1")
    for key in (
        "admissionLatencyMs",
        "timeToFirstChildReadyMs",
        "timeToTargetConcurrencyMs",
    ):
        require_number(summary[key], f"benchmarkSummary.{key}", 0.0)
    require_int(summary["originBytes"], "benchmarkSummary.originBytes", 0)

    stages = require_object(summary["stagePercentilesMs"], "benchmarkSummary.stagePercentilesMs")
    require_keys(stages, TIMING_FIELDS, "benchmarkSummary.stagePercentilesMs")
    for stage in TIMING_FIELDS:
        percentiles = require_object(
            stages[stage], f"benchmarkSummary.stagePercentilesMs.{stage}"
        )
        require_keys(percentiles, {"p50", "p95", "p99"}, f"benchmarkSummary.{stage}")
        p50 = require_number(percentiles["p50"], f"benchmarkSummary.{stage}.p50", 0.0)
        p95 = require_number(percentiles["p95"], f"benchmarkSummary.{stage}.p95", 0.0)
        p99 = require_number(percentiles["p99"], f"benchmarkSummary.{stage}.p99", 0.0)
        if not p50 <= p95 <= p99:
            raise ContractError(f"benchmarkSummary.{stage} percentiles must be ordered")

    if run is not None:
        validate_run(run)
        if summary["runID"] != run["runID"]:
            raise ContractError("benchmarkSummary.runID does not match run.runID")
        if child_count != run["children"]["count"]:
            raise ContractError("benchmarkSummary.childCount does not match run")


def validate_schema_documents(root: Path) -> None:
    for path in sorted((root / "schemas" / "fleet").glob("*.schema.json")):
        document = load_json(path)
        if document.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
            raise ContractError(f"{path} is not a draft 2020-12 JSON schema")
        if "$id" not in document or "title" not in document:
            raise ContractError(f"{path} missing $id or title")


def validate_examples(root: Path) -> None:
    examples = root / "examples" / "fleet"
    generic_run = load_json(examples / "generic-run-rails-rspec.json")
    busybox_generic_run = load_json(examples / "generic-run-busybox-smoke.json")
    run = load_json(examples / "run-1000.json")
    lease = load_json(examples / "shard-lease.json")
    validate_schema_documents(root)
    validate_generic_run(generic_run)
    validate_generic_run(busybox_generic_run)
    validate_run(run)
    validate_shard_lease(lease, run)
    validate_agent_status(load_json(examples / "agent-status.json"))
    validate_attempt_result(load_json(examples / "attempt-result.json"), lease)
    validate_benchmark_summary(load_json(examples / "benchmark-summary-1000.json"), run)


def repo_root_from_script() -> Path:
    return Path(__file__).resolve().parents[1]
