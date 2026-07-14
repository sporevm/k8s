from __future__ import annotations

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from scripts.runtime_acceptance import run_acceptance


class AcceptanceHandler(BaseHTTPRequestHandler):
    run_count = 0
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
            self.reply(
                {
                    "template": {"id": "sha256:template", "cacheHit": self.run_count > 1},
                    "timingsMs": {"templateMs": 1, "executionMs": 2, "totalMs": 3},
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
    return {"event": "exit", "exit_code": 0}


class RuntimeAcceptanceTest(unittest.TestCase):
    def setUp(self) -> None:
        AcceptanceHandler.run_count = 0
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
        )

        self.assertEqual(report["templateID"], "sha256:template")
        self.assertEqual(report["cleanup"], {"availableSlots": 1, "totalSlots": 1})
        self.assertTrue(AcceptanceHandler.sandbox_deleted)


if __name__ == "__main__":
    unittest.main()
