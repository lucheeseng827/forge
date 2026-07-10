#!/usr/bin/env python3
"""A realistic OpenAI-compatible engine simulator for benching forge's scheduling.

Models what a real vLLM/SGLang box does under load, without a GPU:
  - a hard concurrency cap C (like --max-num-seqs): C requests decode "in parallel"
  - requests beyond C queue; time-in-system grows with queue depth
  - a bounded admission queue: beyond C * QUEUE_FACTOR waiting, reply 429 + Retry-After
  - per-request base latency with jitter, plus a usage object with real-ish token counts

Usage: realistic_engine.py PORT CAP BASE_LATENCY_MS [QUEUE_FACTOR]
"""
import json
import os
import random
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(sys.argv[1])
CAP = int(sys.argv[2])
BASE_MS = float(sys.argv[3])
QUEUE_FACTOR = float(sys.argv[4]) if len(sys.argv) > 4 else 3.0
# Bind loopback by default; multi-node benches set ENGINE_SIM_HOST=0.0.0.0.
HOST = os.environ.get("ENGINE_SIM_HOST", "127.0.0.1")

slots = threading.Semaphore(CAP)
lock = threading.Lock()
stats = {"served": 0, "rejected_429": 0, "waiting": 0, "peak_wait": 0}


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _reply(self, code, body: bytes, extra=None):
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/stats":
            with lock:
                body = json.dumps(stats).encode()
            self._reply(200, body)
            return
        # /health (+ pseudo /metrics with a vLLM-style waiting gauge)
        if self.path == "/metrics":
            with lock:
                w = stats["waiting"]
            body = f"vllm:num_requests_waiting {w}\n".encode()
            self.send_response(200)
            self.send_header("content-type", "text/plain")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self._reply(200, b'{"status":"ok"}')

    def do_POST(self):
        n = int(self.headers.get("content-length", 0))
        _ = self.rfile.read(n)

        with lock:
            if stats["waiting"] >= CAP * QUEUE_FACTOR:
                stats["rejected_429"] += 1
                body = json.dumps({"error": {"message": "engine overloaded", "type": "rate_limit_error"}}).encode()
                self._reply(429, body, {"retry-after": "1"})
                return
            stats["waiting"] += 1
            stats["peak_wait"] = max(stats["peak_wait"], stats["waiting"])

        try:
            slots.acquire()
            with lock:
                stats["waiting"] -= 1
            # decode time: base +- 30% jitter
            time.sleep((BASE_MS / 1000.0) * random.uniform(0.7, 1.3))
        finally:
            slots.release()

        prompt_toks = random.randint(180, 420)
        completion_toks = random.randint(40, 160)
        body = json.dumps({
            "id": "chatcmpl-sim",
            "object": "chat.completion",
            "model": "sim-7b",
            "choices": [{"index": 0,
                         "message": {"role": "assistant", "content": "simulated answer"},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": prompt_toks,
                      "completion_tokens": completion_toks,
                      "total_tokens": prompt_toks + completion_toks},
        }).encode()
        with lock:
            stats["served"] += 1
        self._reply(200, body)


print(f"engine sim {HOST}:{PORT} cap={CAP} base={BASE_MS}ms queue_factor={QUEUE_FACTOR}", flush=True)
srv = ThreadingHTTPServer((HOST, PORT), H)
try:
    srv.serve_forever()
finally:
    with lock:
        print(json.dumps(stats), flush=True)
