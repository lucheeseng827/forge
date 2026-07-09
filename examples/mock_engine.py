#!/usr/bin/env python3
"""A zero-dependency mock OpenAI-compatible engine for the forge quickstart.

Stands in for a real vLLM / SGLang / llama.cpp endpoint so you can watch forge's
fan-out -> checkpoint -> resume loop work with **no GPU**. It answers the three
endpoints forge drives, plus the health probe:

  GET  /health                 -> 200 (the readiness gate forge waits on)
  POST /v1/chat/completions    -> a canned assistant message + a `usage` object
  POST /v1/completions         -> a canned text completion + `usage`
  POST /v1/embeddings          -> a tiny deterministic embedding + `usage`

The `usage` object is what forge captures for its tokens-per-dollar accounting, so
`forge status` reports real (mock) token totals.

To demo the dead-letter path: any request whose body contains the token
`FORCE_500` is answered with HTTP 500, so you can watch forge retry it and then
quarantine it to `<out>.dead.jsonl` without wedging the rest of the run.

Usage:
    python3 examples/mock_engine.py [--host 127.0.0.1] [--port 8000]
"""

import argparse
import hashlib
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def _embedding(text: str, dims: int = 8) -> list:
    # A deterministic pseudo-embedding from a hash so re-runs (resume) are stable.
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return [round((digest[i % len(digest)] / 255.0) * 2.0 - 1.0, 6) for i in range(dims)]


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args):  # keep the quickstart output clean
        pass

    def _send(self, code: int, obj: dict) -> None:
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.rstrip("/") in ("/health", "/health_generate"):
            self._send(200, {"status": "ok"})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0) or 0)
        try:
            req = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            return self._send(400, {"error": "invalid json"})

        # Poison trigger: any body mentioning FORCE_500 fails, to show dead-lettering.
        if "FORCE_500" in json.dumps(req):
            return self._send(500, {"error": "forced failure for the dead-letter demo"})

        model = req.get("model", "mock-model")

        if self.path.startswith("/v1/embeddings"):
            raw = req.get("input", "")
            inputs = raw if isinstance(raw, list) else [raw]
            data = [
                {"object": "embedding", "index": i, "embedding": _embedding(str(x))}
                for i, x in enumerate(inputs)
            ]
            ptoks = sum(len(str(x).split()) for x in inputs)
            return self._send(
                200,
                {
                    "object": "list",
                    "data": data,
                    "model": model,
                    "usage": {"prompt_tokens": ptoks, "completion_tokens": 0, "total_tokens": ptoks},
                },
            )

        if self.path.startswith("/v1/completions"):
            return self._send(
                200,
                {
                    "object": "text_completion",
                    "model": model,
                    "choices": [{"index": 0, "text": " (mock completion)", "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": 8, "completion_tokens": 3, "total_tokens": 11},
                },
            )

        # Only the chat path remains; an unknown POST path is a 404 (a mistyped URL
        # should fail fast, not silently look healthy as a chat completion).
        if not self.path.startswith("/v1/chat/completions"):
            return self._send(404, {"error": f"unknown route: POST {self.path}"})

        # Chat completions. If the prompt asks for JSON, reply with a JSON object
        # string (so `forge run --require json` accepts it); otherwise reply in prose
        # (which `--require json` would quarantine — that is the demo).
        messages = req.get("messages", [])
        last = str(messages[-1].get("content", "")) if messages else ""
        if "json" in last.lower():
            reply = json.dumps({"summary": last[:40], "ok": True})
        else:
            reply = f"mock reply to: {last[:60]}"
        return self._send(
            200,
            {
                "object": "chat.completion",
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": reply},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 12, "completion_tokens": 5, "total_tokens": 17},
            },
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(
        f"mock OpenAI-compatible engine on http://{args.host}:{args.port} "
        f"(GET /health · POST /v1/chat/completions|completions|embeddings)"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
