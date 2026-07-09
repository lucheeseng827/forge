#!/usr/bin/env python3
# A deliberately SLOW OpenAI-compatible mock: fixed per-request delay so a forge run
# stays in flight long enough to kill it mid-batch. Chat completions only.
import json, sys, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DELAY = float(sys.argv[1]) if len(sys.argv) > 1 else 0.25
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 8099


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        # health probe / metrics — always ready.
        self.send_response(200)
        self.send_header("content-length", "2")
        self.end_headers()
        self._write(b"ok")

    def do_POST(self):
        n = int(self.headers.get("content-length", 0))
        _ = self.rfile.read(n)
        time.sleep(DELAY)
        body = json.dumps({
            "id": "chatcmpl-mock",
            "object": "chat.completion",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12},
        }).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self._write(body)

    def _write(self, data):
        # A client killed mid-request (exactly the kill -9 scenario this script
        # exists to support) disconnects before we finish writing — harmless, but
        # would otherwise log a BrokenPipeError traceback per thread.
        try:
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass


if __name__ == "__main__":
    print(f"slow-mock on :{PORT} delay={DELAY}s", flush=True)
    ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
