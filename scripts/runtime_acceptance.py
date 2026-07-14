#!/usr/bin/env python3
"""Exercise the deployed SporeVM runtime API from inside its Kubernetes cell."""

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
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
        (event for event in reversed(events) if isinstance(event, dict) and event.get("event") in {"exit", "failure"}),
        None,
    )
    if terminal is None or terminal.get("event") != "exit" or terminal.get("exit_code") != 0:
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


def run_acceptance(
    api_url: str,
    agent_url: str,
    image: str,
    memory: str,
    sandbox_name: str,
    timeout: float,
    provenance: dict[str, str],
) -> dict[str, Any]:
    wait_ready(api_url, timeout)
    total, available = execution_slots(request_json(agent_url, "GET", "/status", None, timeout))
    if total < 1 or available != total:
        raise RuntimeError(f"agent did not start clean: available slots {available}/{total}")

    run_request = {
        "image": image,
        "memory": memory,
        "command": ["/bin/sh", "-lc", "node -v"],
    }
    cold = require_run("cold run", request_json(api_url, "POST", "/runs", run_request, timeout))
    warm = require_run("template-hit run", request_json(api_url, "POST", "/runs", run_request, timeout))
    if cold["template"].get("cacheHit") is not False:
        raise RuntimeError(f"first run did not capture a cold parent: {cold['template']!r}")
    if warm["template"].get("cacheHit") is not True:
        raise RuntimeError(f"second run did not hit the template cache: {warm['template']!r}")
    if warm["template"]["id"] != cold["template"]["id"]:
        raise RuntimeError("cold and warm runs selected different templates")

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
        for label in ("first sandbox exec", "warm sandbox exec"):
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
        "provenance": provenance,
        "templateID": cold["template"]["id"],
        "runs": {
            "coldParentTimingsMs": cold["timingsMs"],
            "templateHitTimingsMs": warm["timingsMs"],
        },
        "sandbox": {
            "createTimingsMs": sandbox.get("timingsMs", {}),
            "firstExecWallMs": exec_wall_ms[0],
            "warmExecWallMs": exec_wall_ms[1],
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = run_acceptance(
        args.api_url,
        args.agent_url,
        args.image,
        args.memory,
        args.sandbox_name,
        args.timeout_seconds,
        {
            "runtimeImage": args.runtime_image,
            "runtimeImageID": args.runtime_image_id,
            "sporeVersion": args.spore_version,
        },
    )
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
