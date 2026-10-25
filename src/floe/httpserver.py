"""Tiny threaded HTTP server for /metrics, /healthz and /readyz (no web framework needed)."""

from __future__ import annotations

import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest


def serve(port: int, live: Callable[[], bool], ready: Callable[[], bool]) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/metrics":
                body, ctype, code = generate_latest(), CONTENT_TYPE_LATEST, 200
            elif self.path == "/healthz":
                ok = live()
                body, ctype, code = (b"ok" if ok else b"stalled"), "text/plain", 200 if ok else 503
            elif self.path == "/readyz":
                ok = ready()
                body, ctype, code = (b"ready" if ok else b"not ready"), "text/plain", 200 if ok else 503
            else:
                body, ctype, code = b"not found", "text/plain", 404
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: object) -> None:
            pass

    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv
