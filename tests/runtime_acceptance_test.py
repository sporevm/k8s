from __future__ import annotations

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from scripts.computesdk_sporevm_benchmark import run_response_error
from scripts.runtime_acceptance import percentile_summary, require_successful_events, run_acceptance


class AcceptanceHandler(BaseHTTPRequestHandler):
    run_count = 0
    exec_count = 0
    sandbox_created = False
    sandbox_deleted = False

    def do_GET(self) -> None:
        if self.path == "/readyz":
            self.send_response(200)
            self.end_headers()
            return
        if self.path == "/status":
            available = 0 if self.sandbox_created and not self.sandbox_deleted else 1
            self.reply({"executionSlots": {"total": 1, "available": available}})
            return
        self.send_error(404)

    def do_POST(self) -> None:
        self.read_json()
        if self.path == "/runs":
            type(self).run_count += 1
            count = self.run_count
            self.reply(
                {
                    "template": {"id": "sha256:template", "cacheHit": count > 1},
                    "timingsMs": {"templateMs": count, "executionMs": count * 2, "totalMs": count * 3},
                    "events": [terminal_event()],
                }
            )
            return
        if self.path == "/sandboxes":
            type(self).sandbox_created = True
            self.reply(
                {
                    "name": "acceptance",
                    "template": {"id": "sha256:template", "cacheHit": True},
                    "timingsMs": {"templateMs": 1, "restoreMs": 2, "totalMs": 3},
                }
            )
            return
        if self.path.endswith("/exec"):
            type(self).exec_count += 1
            self.reply([terminal_event()])
            return
        self.send_error(404)

    def do_DELETE(self) -> None:
        if self.path.startswith("/sandboxes/"):
            type(self).sandbox_deleted = True
            self.reply({"name": "acceptance"})
            return
        self.send_error(404)

    def read_json(self) -> object:
        length = int(self.headers.get("content-length", "0"))
        return json.loads(self.rfile.read(length)) if length else None

    def reply(self, value: object) -> None:
        body = json.dumps(value).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: object) -> None:
        return


def terminal_event() -> dict[str, object]:
    return {
        "schema": "spore.automation.event.v1",
        "schema_version": 1,
        "event": "completion",
        "outcome": "completed",
        "exit_code": 0,
    }


class RuntimeAcceptanceTest(unittest.TestCase):
    def setUp(self) -> None:
        AcceptanceHandler.run_count = 0
        AcceptanceHandler.exec_count = 0
        AcceptanceHandler.sandbox_created = False
        AcceptanceHandler.sandbox_deleted = False
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), AcceptanceHandler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_acceptance_covers_runs_sandbox_and_cleanup(self) -> None:
        url = f"http://127.0.0.1:{self.server.server_port}"
        report = run_acceptance(
            url,
            url,
            "example.com/node@sha256:abc",
            "1024mb",
            "acceptance",
            2,
            {"runtimeImage": "example.com/runtime:1"},
            warm_run_iterations=3,
            sandbox_exec_iterations=4,
        )

        self.assertEqual(report["templateID"], "sha256:template")
        self.assertEqual(report["cleanup"], {"availableSlots": 1, "totalSlots": 1})
        self.assertEqual(report["benchmark"]["sampleCounts"], {"templateHits": 3, "warmSandboxExecs": 3})
        self.assertEqual(
            report["benchmark"]["percentilesMs"]["templateHitNode"]["totalMs"],
            {"p50": 9.0, "p95": 12.0, "p99": 12.0},
        )
        self.assertEqual(len(report["benchmark"]["samples"]["templateHitWallMs"]), 3)
        self.assertEqual(len(report["benchmark"]["samples"]["warmSandboxExecWallMs"]), 3)
        self.assertEqual(AcceptanceHandler.run_count, 4)
        self.assertEqual(AcceptanceHandler.exec_count, 4)
        self.assertTrue(AcceptanceHandler.sandbox_deleted)

    def test_percentiles_use_nearest_rank(self) -> None:
        self.assertEqual(percentile_summary([4, 1, 3, 2]), {"p50": 2, "p95": 4, "p99": 4})

    def test_event_consumers_require_successful_completion(self) -> None:
        self.assertEqual("", run_response_error({"events": [terminal_event()]}))

        failed = terminal_event()
        failed["outcome"] = "failed"
        failed["exit_code"] = 1
        with self.assertRaisesRegex(RuntimeError, "terminal event"):
            require_successful_events("test", [failed])
        self.assertIn("run terminal", run_response_error({"events": [failed]}))

        legacy = {"event": "exit", "exit_code": 0}
        with self.assertRaisesRegex(RuntimeError, "terminal event"):
            require_successful_events("test", [legacy])
        self.assertIn("no terminal event", run_response_error({"events": [legacy]}))


if __name__ == "__main__":
    unittest.main()
