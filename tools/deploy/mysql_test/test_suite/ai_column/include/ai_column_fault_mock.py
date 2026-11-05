#!/usr/bin/env python3
"""Fault-injection OpenAI-compatible service for AI column reliability tests."""

import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def directory(port):
    return "/tmp/seekdb_ai_column_fault_%d" % port


def path(port, name):
    return os.path.join(directory(port), name)


def stop(port):
    try:
        with open(path(port, "pid"), encoding="ascii") as stream:
            pid = int(stream.read())
        os.kill(pid, signal.SIGTERM)
    except (FileNotFoundError, ProcessLookupError, ValueError):
        pass


def start(port):
    os.makedirs(directory(port), exist_ok=True)
    stop(port)
    open(path(port, "events.jsonl"), "w", encoding="utf-8").close()
    output = open(path(port, "server.log"), "w", encoding="utf-8")
    process = subprocess.Popen(
        [sys.executable, os.path.abspath(__file__), "serve", str(port)],
        stdin=subprocess.DEVNULL,
        stdout=output,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    with open(path(port, "pid"), "w", encoding="ascii") as stream:
        stream.write(str(process.pid))
    for _ in range(100):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.05):
                return
        except OSError:
            time.sleep(0.02)
    raise RuntimeError("fault mock failed to start")


def serve(port):
    attempts = {}
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def do_POST(self):
            size = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(size).decode("utf-8"))
            user = ""
            for message in body.get("messages", []):
                if message.get("role") == "user":
                    user = str(message.get("content", ""))
            scenario = self.path.split("/")[2]
            with lock:
                attempts[scenario] = attempts.get(scenario, 0) + 1
                attempt = attempts[scenario]

            status = 200
            action = "response"
            if scenario == "429-once" and attempt == 1:
                status = 429
            elif scenario == "500-always":
                status = 500
            elif scenario == "503-once" and attempt == 1:
                status = 503
            elif scenario == "disconnect-once" and attempt == 1:
                action = "disconnect"
            elif scenario == "response-lost-once" and attempt == 1:
                action = "response-lost"
            elif scenario == "timeout-once" and attempt == 1:
                action = "timeout"

            event = {"scenario": scenario, "attempt": attempt,
                     "status": status, "action": action, "user": user}
            with lock:
                with open(path(port, "events.jsonl"), "a", encoding="utf-8") as stream:
                    stream.write(json.dumps(event) + "\n")
            if action == "disconnect" or action == "response-lost":
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            if action == "timeout":
                time.sleep(65)
            payload = json.dumps(
                {"error": {"message": "injected"}} if status != 200 else
                {"choices": [{"message": {"role": "assistant", "content": "mock:" + user}}]}
            ).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            try:
                self.wfile.write(payload)
            except BrokenPipeError:
                pass

    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


def assert_scenario(port, scenario):
    with open(path(port, "events.jsonl"), encoding="utf-8") as stream:
        events = [json.loads(line) for line in stream if line.strip()]
    selected = [event for event in events if event["scenario"] == scenario]
    expected = {
        "429-once": [(429, "response"), (200, "response")],
        # The shared AI client retries 5xx four times inside each of two task attempts.
        "500-always": [(500, "response")] * 8,
        "503-once": [(503, "response"), (200, "response")],
        "disconnect-once": [(200, "disconnect"), (200, "response")],
        "response-lost-once": [(200, "response-lost"), (200, "response")],
        "timeout-once": [(200, "timeout"), (200, "response")],
    }[scenario]
    actual = [(event["status"], event["action"]) for event in selected]
    if actual != expected:
        raise AssertionError("%s: expected %r, got %r" % (scenario, expected, actual))
    print("fault_%s=ok" % scenario.replace("-", "_"))


if __name__ == "__main__":
    mode, port = sys.argv[1], int(sys.argv[2])
    if mode == "start":
        start(port)
    elif mode == "stop":
        stop(port)
    elif mode == "serve":
        serve(port)
    elif mode == "assert":
        assert_scenario(port, sys.argv[3])
