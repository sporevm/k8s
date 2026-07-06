#!/usr/bin/env python3
"""Run a ComputeSDK-shaped SporeVM Kubernetes TTI benchmark.

This is a thin live harness around the resident coordinator API. It measures
the public ComputeSDK sandbox TTI shape: submit one fresh one-child run, execute
`node -v` in the child, and record wall-clock time until the coordinator report
is available. Without --api-url it falls back to `sporectl submit` for smoke
testing, which includes Kubernetes Job startup.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROVIDER = "sporevm-k8s"
MODE = "sequential"


def main() -> int:
    args = parse_args()
    repo = Path(__file__).resolve().parents[1]
    timeout_ms = duration_ms(args.timeout)
    out_dir = output_dir(repo, args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    iterations: list[dict[str, Any]] = []
    raw_reports: list[dict[str, Any]] = []

    with tempfile.TemporaryDirectory(prefix="sporevm-computesdk-") as tmp:
        tmpdir = Path(tmp)
        for index in range(args.iterations):
            run_id = f"{args.run_prefix}-{run_stamp}-{index + 1:04d}"
            run_doc = build_generic_run(args, run_id)
            run_path = tmpdir / f"{run_id}.json"
            run_path.write_text(
                json.dumps(run_doc, indent=2) + "\n",
                encoding="utf-8",
            )

            print(f"[{index + 1}/{args.iterations}] {run_id}", file=sys.stderr)
            started = time.perf_counter()
            if args.api_url:
                report, error = run_api(args, run_doc)
                tti_ms = (time.perf_counter() - started) * 1000
                if error:
                    iterations.append({"ttiMs": 0, "error": error})
                    print(f"  failed in {tti_ms / 1000:.2f}s", file=sys.stderr)
                    continue
                raw_reports.append(report)
                state = report.get("summary", {}).get("state")
                if state != "succeeded":
                    iterations.append({"ttiMs": 0, "error": f"runtime report state={state!r}"})
                    print(f"  state={state!r} in {tti_ms / 1000:.2f}s", file=sys.stderr)
                    continue
                iterations.append({"ttiMs": round_ms(tti_ms)})
                print(f"  tti={tti_ms / 1000:.2f}s", file=sys.stderr)
                continue

            completed = run_sporectl(repo, args, run_path)
            tti_ms = (time.perf_counter() - started) * 1000

            if completed.returncode != 0:
                iterations.append({"ttiMs": 0, "error": trim_error(completed.stdout)})
                print(f"  failed in {tti_ms / 1000:.2f}s", file=sys.stderr)
                continue

            try:
                report = extract_runtime_report(completed.stdout)
                raw_reports.append(report)
                state = report.get("summary", {}).get("state")
                if state != "succeeded":
                    iterations.append({"ttiMs": 0, "error": f"runtime report state={state!r}"})
                    print(f"  state={state!r} in {tti_ms / 1000:.2f}s", file=sys.stderr)
                    continue
            except ValueError as err:
                iterations.append({"ttiMs": 0, "error": str(err)})
                print(f"  no report in {tti_ms / 1000:.2f}s", file=sys.stderr)
                continue

            iterations.append({"ttiMs": round_ms(tti_ms)})
            print(f"  tti={tti_ms / 1000:.2f}s", file=sys.stderr)

    successful = [item["ttiMs"] for item in iterations if "error" not in item]
    result = {
        "provider": PROVIDER,
        "mode": MODE,
        "iterations": iterations,
        "summary": {"ttiMs": compute_stats(successful)},
        "successRate": round(len(successful) / len(iterations), 4) if iterations else 0,
    }
    output = {
        "version": "1.1",
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "environment": {
            "python": platform.python_version(),
            "platform": sys.platform,
            "arch": platform.machine(),
        },
        "config": {
            "iterations": args.iterations,
            "timeoutMs": timeout_ms,
            "transport": "api" if args.api_url else "sporectl",
            "workloadImage": args.workload_image,
            "workloadCommand": "node -v",
        },
        "results": [result],
    }

    date_name = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dated_path = out_dir / f"{date_name}.json"
    latest_path = out_dir / "latest.json"
    dated_path.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
    shutil.copyfile(dated_path, latest_path)
    print(f"wrote {dated_path}", file=sys.stderr)
    print(f"wrote {latest_path}", file=sys.stderr)

    if args.raw_report_dir:
        raw_dir = output_dir(repo, args.raw_report_dir)
        raw_dir.mkdir(parents=True, exist_ok=True)
        for report in raw_reports:
            run_id = report.get("summary", {}).get("runID", "unknown-run")
            (raw_dir / f"{run_id}.runtime-report.json").write_text(
                json.dumps(report, indent=2) + "\n",
                encoding="utf-8",
            )

    print(json.dumps(output, indent=2))
    return 0 if len(successful) == len(iterations) else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--namespace", default=os.environ.get("SPORE_NAMESPACE", "sporevm-system"))
    parser.add_argument("--kubectl", default=os.environ.get("KUBECTL", "kubectl"))
    parser.add_argument("--sporectl", default=os.environ.get("SPORECTL", "go run ./cmd/sporectl"))
    parser.add_argument("--api-url", default=os.environ.get("SPORE_API_URL", ""))
    parser.add_argument("--coordinator-image", default=os.environ.get("SPORE_RUNTIME_IMAGE"))
    parser.add_argument("--image-pull-policy", default=os.environ.get("SPORE_IMAGE_PULL_POLICY", "IfNotPresent"))
    parser.add_argument("--timeout", default="30m")
    parser.add_argument("--result-store-prefix", default="s3://example-sporevm-results/computesdk/")
    parser.add_argument("--result-store-root", default="/var/lib/sporevm/coordinator-results")
    parser.add_argument("--workload-image", default="docker.io/library/node:22-bookworm-slim")
    parser.add_argument("--memory", default="1024mb")
    parser.add_argument("--prepare-sleep-seconds", type=int, default=300)
    parser.add_argument("--run-prefix", default="computesdk-node-seq")
    parser.add_argument("--out-dir", default="results/sequential_tti")
    parser.add_argument("--raw-report-dir", default="")
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args()
    if args.iterations < 1:
        parser.error("--iterations must be >= 1")
    if args.prepare_sleep_seconds < 1:
        parser.error("--prepare-sleep-seconds must be >= 1")
    if not args.result_store_prefix.endswith("/"):
        args.result_store_prefix += "/"
    if not args.coordinator_image:
        args.coordinator_image = default_runtime_image(Path(__file__).resolve().parents[1])
    return args


def build_generic_run(args: argparse.Namespace, run_id: str) -> dict[str, Any]:
    ready_marker = "SPOREVM_NODE_READY"
    return {
        "runID": run_id,
        "source": {
            "image": args.workload_image,
            "platform": "linux/arm64",
        },
        "prepare": {
            "command": [
                "/bin/sh",
                "-lc",
                f"trap '' USR1; node -v >/dev/null; echo {ready_marker}; sleep {args.prepare_sleep_seconds}",
            ],
            "captureSignal": "USR1",
            "readyMarker": ready_marker,
            "memory": args.memory,
        },
        "fork": {"count": 1},
        "children": {
            "start": 0,
            "count": 1,
            "command": ["/bin/sh", "-lc", "node -v"],
        },
        "execution": {
            "childrenPerShard": 1,
            "maxInFlightPerAgent": 1,
        },
        "retryPolicy": {
            "maxAttemptsPerChild": 1,
            "rerunCommittedChildren": False,
        },
        "sideEffects": {
            "idempotencyRequired": True,
        },
        "resultStore": f"{args.result_store_prefix}{run_id}/",
    }


def run_sporectl(repo: Path, args: argparse.Namespace, run_path: Path) -> subprocess.CompletedProcess[str]:
    command = shlex.split(args.sporectl) + [
        "submit",
        "--namespace",
        args.namespace,
        "--kubectl",
        args.kubectl,
        "--image",
        args.coordinator_image,
        "--image-pull-policy",
        args.image_pull_policy,
        "--timeout",
        args.timeout,
        "--result-store-root",
        args.result_store_root,
    ]
    if args.replace:
        command.append("--replace")
    command.append(str(run_path))
    return subprocess.run(
        command,
        cwd=repo,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=duration_ms(args.timeout) / 1000 + 90,
        check=False,
    )


def run_api(args: argparse.Namespace, run_doc: dict[str, Any]) -> tuple[dict[str, Any], str]:
    endpoint = args.api_url.rstrip("/") + "/generic-runs"
    payload = json.dumps(run_doc).encode("utf-8")
    request = urllib.request.Request(
        endpoint,
        data=payload,
        headers={"content-type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=duration_ms(args.timeout) / 1000) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as err:
        body = err.read().decode("utf-8", errors="replace")
        return {}, trim_error(body)
    except OSError as err:
        return {}, str(err)
    try:
        report = json.loads(body)
    except json.JSONDecodeError as err:
        return {}, f"API response was not JSON: {err}"
    if not isinstance(report, dict) or "plan" not in report or "summary" not in report:
        return {}, "API response did not contain a RuntimeReport"
    return report, ""


def extract_runtime_report(output: str) -> dict[str, Any]:
    decoder = json.JSONDecoder()
    for index, char in enumerate(output):
        if char != "{":
            continue
        try:
            value, _ = decoder.raw_decode(output[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and "plan" in value and "summary" in value:
            return value
    raise ValueError("sporectl output did not contain a RuntimeReport JSON object")


def compute_stats(values: list[float]) -> dict[str, float]:
    if not values:
        return {"median": 0, "p95": 0, "p99": 0}
    sorted_values = sorted(values)
    trim = int(len(sorted_values) * 0.05)
    if trim > 0 and len(sorted_values) - (2 * trim) > 0:
        sorted_values = sorted_values[trim : len(sorted_values) - trim]
    mid = len(sorted_values) // 2
    if len(sorted_values) % 2 == 0:
        median = (sorted_values[mid - 1] + sorted_values[mid]) / 2
    else:
        median = sorted_values[mid]
    return {
        "median": round_ms(median),
        "p95": round_ms(percentile(sorted_values, 95)),
        "p99": round_ms(percentile(sorted_values, 99)),
    }


def percentile(sorted_values: list[float], p: int) -> float:
    if not sorted_values:
        return 0
    index = max(0, int_ceil((p / 100) * len(sorted_values)) - 1)
    return sorted_values[min(index, len(sorted_values) - 1)]


def int_ceil(value: float) -> int:
    rounded = int(value)
    if value == rounded:
        return rounded
    return rounded + 1


def round_ms(value: float) -> float:
    return round(value, 2)


def duration_ms(value: str) -> int:
    value = value.strip().lower()
    if not value:
        raise ValueError("empty duration")
    units = {
        "ms": 1,
        "s": 1000,
        "m": 60 * 1000,
        "h": 60 * 60 * 1000,
    }
    for suffix, scale in units.items():
        if value.endswith(suffix):
            return int(float(value[: -len(suffix)]) * scale)
    return int(float(value) * 1000)


def output_dir(repo: Path, raw: str) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path
    return repo / path


def default_runtime_image(repo: Path) -> str:
    chart = repo / "charts" / "sporevm-k8s" / "Chart.yaml"
    version = "0.1.1"
    for line in chart.read_text(encoding="utf-8").splitlines():
        if line.startswith("appVersion:"):
            version = line.split(":", 1)[1].strip().strip('"')
            break
    return f"ghcr.io/sporevm/k8s-runtime:{version}"


def trim_error(output: str) -> str:
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if not lines:
        return "sporectl submit failed without output"
    joined = " | ".join(lines[-8:])
    if len(joined) > 1000:
        return joined[-1000:]
    return joined


if __name__ == "__main__":
    raise SystemExit(main())
