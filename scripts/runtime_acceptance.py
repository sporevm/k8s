#!/usr/bin/env python3
"""Exercise the deployed SporeVM runtime API from inside its Kubernetes cell."""

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any


DEFAULT_IMAGE = "docker.io/library/node@sha256:6db9be2ebb4bafb687a078ef5ba1b1dd256e8004d246a31fd210b6b848ab6be2"


def request_json(base_url: str, method: str, path: str, payload: Any, timeout: float) -> Any:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        base_url.rstrip("/") + path,
        data=data,
        headers={"content-type": "application/json"} if data is not None else {},
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as err:
        body = err.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path}: HTTP {err.code}: {body}") from err
    except (OSError, json.JSONDecodeError) as err:
        raise RuntimeError(f"{method} {path}: {err}") from err


def wait_ready(api_url: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(api_url.rstrip("/") + "/readyz", timeout=5) as response:
                if response.status == 200:
                    return
        except OSError as err:
            last_error = err
        time.sleep(0.5)
    raise RuntimeError(f"coordinator did not become ready: {last_error}")


def require_successful_events(label: str, events: Any) -> None:
    if not isinstance(events, list):
        raise RuntimeError(f"{label}: response did not contain an event list")
    terminal = next(
        (event for event in reversed(events) if isinstance(event, dict) and event.get("event") == "completion"),
        None,
    )
    if (
        terminal is None
        or terminal.get("schema") != "spore.automation.event.v1"
        or terminal.get("schema_version") != 1
        or terminal.get("outcome") != "completed"
        or terminal.get("exit_code") != 0
    ):
        raise RuntimeError(f"{label}: terminal event was {terminal!r}")


def require_run(label: str, response: Any) -> dict[str, Any]:
    if not isinstance(response, dict):
        raise RuntimeError(f"{label}: response was not an object")
    require_successful_events(label, response.get("events"))
    template = response.get("template")
    if not isinstance(template, dict) or not template.get("id"):
        raise RuntimeError(f"{label}: response did not identify a template")
    timings = response.get("timingsMs")
    if not isinstance(timings, dict):
        raise RuntimeError(f"{label}: response did not contain timingsMs")
    for key in ("templateMs", "executionMs", "totalMs"):
        if not isinstance(timings.get(key), (int, float)):
            raise RuntimeError(f"{label}: timingsMs.{key} was missing")
    return response


def execution_slots(status: Any) -> tuple[int, int]:
    if not isinstance(status, dict) or not isinstance(status.get("executionSlots"), dict):
        raise RuntimeError("agent status did not contain executionSlots")
    slots = status["executionSlots"]
    total = slots.get("total")
    available = slots.get("available")
    if not isinstance(total, int) or not isinstance(available, int):
        raise RuntimeError("agent execution slots were not integers")
    return total, available


def percentile_summary(values: list[float]) -> dict[str, float]:
    if not values:
        raise ValueError("percentile sample must not be empty")
    ordered = sorted(values)

    def nearest_rank(percentile: float) -> float:
        index = max(0, int(percentile * len(ordered) + 0.999999999) - 1)
        return round(ordered[min(index, len(ordered) - 1)], 3)

    return {
        "p50": nearest_rank(0.50),
        "p95": nearest_rank(0.95),
        "p99": nearest_rank(0.99),
    }


def timing_percentiles(samples: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    if not samples:
        raise ValueError("timing sample must not be empty")
    keys = set(samples[0])
    for sample in samples[1:]:
        keys.intersection_update(sample)
    return {
        key: percentile_summary([float(sample[key]) for sample in samples])
        for key in sorted(keys)
        if all(isinstance(sample[key], (int, float)) for sample in samples)
    }


def run_acceptance(
    api_url: str,
    agent_url: str,
    image: str,
    memory: str,
    sandbox_name: str,
    timeout: float,
    provenance: dict[str, str],
    warm_run_iterations: int = 1,
    sandbox_exec_iterations: int = 2,
) -> dict[str, Any]:
    if warm_run_iterations < 1:
        raise ValueError("warm_run_iterations must be >= 1")
    if sandbox_exec_iterations < 2:
        raise ValueError("sandbox_exec_iterations must be >= 2")

    wait_ready(api_url, timeout)
    total, available = execution_slots(request_json(agent_url, "GET", "/status", None, timeout))
    if total < 1 or available != total:
        raise RuntimeError(f"agent did not start clean: available slots {available}/{total}")

    run_request = {
        "image": image,
        "memory": memory,
        "command": ["/bin/sh", "-lc", "node -v"],
    }
    started = time.perf_counter()
    cold = require_run("cold run", request_json(api_url, "POST", "/runs", run_request, timeout))
    cold_wall_ms = round((time.perf_counter() - started) * 1000, 3)
    if cold["template"].get("cacheHit") is not False:
        raise RuntimeError(f"first run did not capture a cold parent: {cold['template']!r}")

    warm_runs = []
    warm_run_wall_ms = []
    for index in range(warm_run_iterations):
        label = f"template-hit run {index + 1}"
        started = time.perf_counter()
        warm = require_run(label, request_json(api_url, "POST", "/runs", run_request, timeout))
        warm_run_wall_ms.append(round((time.perf_counter() - started) * 1000, 3))
        if warm["template"].get("cacheHit") is not True:
            raise RuntimeError(f"{label} did not hit the template cache: {warm['template']!r}")
        if warm["template"]["id"] != cold["template"]["id"]:
            raise RuntimeError(f"{label} selected a different template")
        warm_runs.append(warm)

    encoded_name = urllib.parse.quote(sandbox_name, safe="")
    created = False
    try:
        sandbox = request_json(
            api_url,
            "POST",
            "/sandboxes",
            {"name": sandbox_name, "image": image, "memory": memory},
            timeout,
        )
        created = True
        sandbox_template = sandbox.get("template") if isinstance(sandbox, dict) else None
        if (
            not isinstance(sandbox_template, dict)
            or sandbox_template.get("id") != cold["template"]["id"]
            or sandbox_template.get("cacheHit") is not True
        ):
            raise RuntimeError(f"sandbox did not reuse the run template: {sandbox!r}")

        exec_wall_ms = []
        for index in range(sandbox_exec_iterations):
            label = "first sandbox exec" if index == 0 else f"warm sandbox exec {index}"
            started = time.perf_counter()
            events = request_json(
                api_url,
                "POST",
                f"/sandboxes/{encoded_name}/exec",
                {"command": ["/bin/sh", "-lc", "node -v"]},
                timeout,
            )
            exec_wall_ms.append(round((time.perf_counter() - started) * 1000, 3))
            require_successful_events(label, events)
    finally:
        if created:
            request_json(api_url, "DELETE", f"/sandboxes/{encoded_name}", None, timeout)

    deadline = time.monotonic() + timeout
    while True:
        _, available = execution_slots(request_json(agent_url, "GET", "/status", None, timeout))
        if available == total:
            break
        if time.monotonic() >= deadline:
            raise RuntimeError(f"sandbox cleanup leaked an execution slot: available {available}/{total}")
        time.sleep(0.25)

    return {
        "schema": "sporevm-k8s.runtime-acceptance.v1",
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "provenance": provenance,
        "config": {
            "workloadImage": image,
            "memory": memory,
            "command": run_request["command"],
        },
        "templateID": cold["template"]["id"],
        "runs": {
            "coldParentTimingsMs": cold["timingsMs"],
            "coldParentWallMs": cold_wall_ms,
            "templateHitTimingsMs": warm_runs[0]["timingsMs"],
        },
        "sandbox": {
            "createTimingsMs": sandbox.get("timingsMs", {}),
            "firstExecWallMs": exec_wall_ms[0],
            "warmExecWallMs": exec_wall_ms[1],
        },
        "benchmark": {
            "sampleCounts": {
                "templateHits": len(warm_runs),
                "warmSandboxExecs": len(exec_wall_ms) - 1,
            },
            "samples": {
                "templateHitWallMs": warm_run_wall_ms,
                "templateHitNodeTimingsMs": [warm["timingsMs"] for warm in warm_runs],
                "warmSandboxExecWallMs": exec_wall_ms[1:],
            },
            "percentilesMs": {
                "templateHitWall": percentile_summary(warm_run_wall_ms),
                "templateHitNode": timing_percentiles([warm["timingsMs"] for warm in warm_runs]),
                "warmSandboxExecWall": percentile_summary(exec_wall_ms[1:]),
            },
        },
        "cleanup": {"availableSlots": available, "totalSlots": total},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--agent-url", required=True)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--memory", default="1024mb")
    parser.add_argument("--sandbox-name", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=180)
    parser.add_argument("--runtime-image", required=True)
    parser.add_argument("--runtime-image-id", required=True)
    parser.add_argument("--spore-version", required=True)
    parser.add_argument("--warm-run-iterations", type=int, default=1)
    parser.add_argument("--sandbox-exec-iterations", type=int, default=2)
    parser.add_argument("--source-revision", default="")
    parser.add_argument("--helm-release", default="")
    parser.add_argument("--helm-revision", default="")
    parser.add_argument("--chart-ref", default="")
    parser.add_argument("--chart-version", default="")
    parser.add_argument("--chart-app-version", default="")
    parser.add_argument("--chart-digest", default="")
    parser.add_argument("--chart-package-sha256", default="")
    args = parser.parse_args()
    if args.warm_run_iterations < 1:
        parser.error("--warm-run-iterations must be >= 1")
    if args.sandbox_exec_iterations < 2:
        parser.error("--sandbox-exec-iterations must be >= 2")
    return args


def main() -> None:
    args = parse_args()
    provenance = {
        "runtimeImage": args.runtime_image,
        "runtimeImageID": args.runtime_image_id,
        "sporeVersion": args.spore_version,
    }
    optional_provenance = {
        "sourceRevision": args.source_revision,
        "helmRelease": args.helm_release,
        "helmRevision": args.helm_revision,
        "chartRef": args.chart_ref,
        "chartVersion": args.chart_version,
        "chartAppVersion": args.chart_app_version,
        "chartDigest": args.chart_digest,
        "chartPackageSHA256": args.chart_package_sha256,
    }
    provenance.update({key: value for key, value in optional_provenance.items() if value})
    report = run_acceptance(
        args.api_url,
        args.agent_url,
        args.image,
        args.memory,
        args.sandbox_name,
        args.timeout_seconds,
        provenance,
        args.warm_run_iterations,
        args.sandbox_exec_iterations,
    )
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
