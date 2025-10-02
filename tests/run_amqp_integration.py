#!/usr/bin/env python3
"""Minimal end-to-end AMQP integration test.

This test boots a RabbitMQ instance (via podman), the TCP proxy, the
tracker_parser_daemon, and the cat_location_daemon. It then sends a single raw
tracker packet through the proxy and asserts that a parsed tracker event and a
location event appear on the expected queues.

The focus is verifying the data flow, so only one consumer per queue is used
and the test exits as soon as the expected payloads are observed.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import List

import pika

REPO_ROOT = Path(__file__).resolve().parents[1]
LOG_ROOT = Path(os.environ.get("TCPPROXY_TEST_LOG_ROOT", REPO_ROOT / "build" / "logs"))
LOG_ROOT.mkdir(parents=True, exist_ok=True)

PROXY_BIN = Path(os.environ.get("TCPPROXY_BIN", REPO_ROOT / "build" / "tcpproxy"))
TRACKER_PARSER_BIN = Path(os.environ.get("TCPPROXY_TRACKER_PARSER", REPO_ROOT / "build" / "tracker_parser_daemon"))
LOCATION_BIN = Path(os.environ.get("TCPPROXY_CAT_LOCATION", REPO_ROOT / "build" / "cat_location_daemon"))
KML_PATH = REPO_ROOT / "Locations.kml"

TRACKER_EXCHANGE = "tcpproxy.events"
TRACKER_ROUTING_KEY = "tracker.raw"


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def start_podman_rabbitmq(port: int, verbose: bool) -> subprocess.Popen:
    cmd = [
        "podman",
        "run",
        "--rm",
        "-p",
        f"{port}:5672",
        "docker.io/library/rabbitmq:3.12-management",
    ]
    if verbose:
        print("[podman]", " ".join(cmd))
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def stop_podman(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def wait_for_port(port: int, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect(("127.0.0.1", port))
            except OSError:
                time.sleep(0.2)
                continue
        return
    raise RuntimeError(f"port {port} did not open")


def sanitize_uri(uri: str) -> tuple[str, str | None]:
    base, _, query = uri.partition('?')
    clean = base
    if "@" in base:
        _, tail = base.split('@', 1)
        clean = f"amqp://guest:guest@{tail}"
    return clean, query or None


def ensure_queue(uri: str, queue: str, *, exchange: str | None = None, routing_key: str | None = None, durable: bool = False, auto_delete: bool = False, verbose: bool = False) -> None:
    params = pika.URLParameters(uri)
    with pika.BlockingConnection(params) as connection:
        channel = connection.channel()
        channel.queue_declare(queue=queue, durable=durable, auto_delete=auto_delete)
        if exchange:
            channel.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)
            channel.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key or "#")
        if verbose:
            print(f"[ensure_queue] queue={queue} exchange={exchange} rk={routing_key}")


def send_tracker_message(port: int, payload: bytes, verbose: bool) -> bytes:
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        if verbose:
            print("[client] -> proxy", payload)
        sock.sendall(payload)
        sock.shutdown(socket.SHUT_WR)
        return sock.recv(65536)


def debug_fetch(uri: str, queue: str) -> dict | None:
    params = pika.URLParameters(uri)
    with pika.BlockingConnection(params) as connection:
        channel = connection.channel()
        method, properties, body = channel.basic_get(queue, auto_ack=False)
        if not method:
            return None
        channel.basic_nack(method.delivery_tag, requeue=True)
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {"_raw": body.decode("utf-8", errors="replace")}


def start_proxy(local_port: int, remote_port: int, logdir: Path, observer: str | None, verbose: bool) -> subprocess.Popen:
    cmd = [
        str(PROXY_BIN),
        "-l",
        f"0.0.0.0:{local_port}",
        "-r",
        f"127.0.0.1:{remote_port}",
        "-o",
        str(logdir / "proxy"),
    ]
    if observer:
        cmd.extend(["-O", observer])
    if verbose:
        print("[proxy]", " ".join(cmd))
    (logdir / "proxy").mkdir(exist_ok=True)
    stdout = open(logdir / "proxy" / "stdout.log", "wb")
    stderr = open(logdir / "proxy" / "stderr.log", "wb")
    return subprocess.Popen(cmd, stdout=stdout, stderr=stderr)


def start_server(port: int, logdir: Path, verbose: bool) -> subprocess.Popen:
    cmd = [
        sys.executable,
        str(REPO_ROOT / "tests" / "echo_server.py"),
        str(port),
        "--echo",
    ]
    if verbose:
        print("[server]", " ".join(cmd))
    log_file = open(logdir / "server.log", "wb")
    return subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)


def start_tracker_parser(input_uri: str,
                         input_queue: str,
                         output_uri: str,
                         output_exchange: str,
                         output_routing_key: str,
                         logdir: Path,
                         verbose: bool) -> subprocess.Popen:
    cmd = [
        str(TRACKER_PARSER_BIN),
        "--input-uri",
        input_uri,
        "--input-queue",
        input_queue,
        "--input-exchange",
        TRACKER_EXCHANGE,
        "--input-routing-key",
        TRACKER_ROUTING_KEY,
        "--output-uri",
        output_uri,
        "--output-exchange",
        output_exchange,
        "--output-routing-key",
        output_routing_key,
        "--log-level",
        "DEBUG" if verbose else "INFO",
    ]
    if verbose:
        print("[tracker_parser]", " ".join(cmd))
    log_file = open(logdir / "tracker_parser.log", "wb")
    return subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)


def start_consumer(uri: str, queue: str, sink: List[dict], stop_event: threading.Event, verbose: bool, *, exchange: str | None = None, routing_key: str | None = None) -> threading.Thread:
    def _consume() -> None:
        params = pika.URLParameters(uri)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue=queue, durable=False, auto_delete=False)
        if exchange:
            channel.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)
            channel.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key or "#")
        if verbose:
            print(f"[consumer] waiting queue={queue} exchange={exchange} rk={routing_key}")
        for method, _, body in channel.consume(queue=queue, inactivity_timeout=1.0):
            if stop_event.is_set():
                break
            if method is None:
                continue
            channel.basic_ack(method.delivery_tag)
            try:
                sink.append(json.loads(body))
            except json.JSONDecodeError:
                sink.append({"_raw": body.decode("utf-8", errors="replace")})
            break
        channel.cancel()
        connection.close()

    thread = threading.Thread(target=_consume, daemon=True)
    thread.start()
    return thread


def run_test(keep_logs: bool = False, verbose: bool = False) -> None:
    logdir = Path(tempfile.mkdtemp(prefix="amqp-chain-", dir=str(LOG_ROOT)))
    container = None
    proxy = server = tracker_proc = location_proc = None
    location_results: list[dict] = []
    location_thread = None
    stop_event = threading.Event()

    try:
        rabbit_port = get_free_port()
        container = start_podman_rabbitmq(rabbit_port, verbose)
        wait_for_port(rabbit_port)

        raw_uri = f"amqp://guest:guest@127.0.0.1:{rabbit_port}/%2F?exchange={TRACKER_EXCHANGE}&routing_key={TRACKER_ROUTING_KEY}"
        clean_uri, _ = sanitize_uri(raw_uri)
        wait_for_rabbitmq(clean_uri)

        remote_port = get_free_port()
        server = start_server(remote_port, logdir, verbose)

        local_port = get_free_port()
        proxy = start_proxy(local_port, remote_port, logdir, f"amqp={raw_uri}", verbose)

        parsed_exchange = "tracker.events.parsed"
        location_queue = "cat.location.test"
        location_input_queue = "cat.location.source"
        output_uri = f"amqp://guest:guest@127.0.0.1:{rabbit_port}/%2F"

        ensure_queue(clean_uri, "tracker.events.parser", exchange=TRACKER_EXCHANGE, routing_key=TRACKER_ROUTING_KEY, durable=True, verbose=verbose)
        ensure_queue(clean_uri, location_input_queue, exchange=parsed_exchange, routing_key="tracker.parsed", verbose=verbose)
        ensure_queue(clean_uri, location_queue, exchange="cat.location", routing_key=location_queue, verbose=verbose)

        tracker_proc = start_tracker_parser(clean_uri, "tracker.events.parser", output_uri, parsed_exchange, "tracker.parsed", logdir, verbose)

        if not LOCATION_BIN.exists():
            raise RuntimeError("cat_location_daemon missing; build before running tests")

        location_cmd = [
            str(LOCATION_BIN),
            "--input-uri",
            clean_uri,
            "--input-queue",
            location_input_queue,
            "--input-exchange",
            parsed_exchange,
            "--input-routing-key",
            "tracker.parsed",
            "--output-uri",
            output_uri,
            "--output-exchange",
            "cat.location",
            "--output-routing-key",
            location_queue,
            "--kml",
            str(KML_PATH),
            "--log-level",
            "DEBUG" if verbose else "WARNING",
        ]
        if verbose:
            print("[cat_location]", " ".join(location_cmd))
        location_log = open(logdir / "cat_location.log", "wb")
        location_proc = subprocess.Popen(location_cmd, stdout=location_log, stderr=subprocess.STDOUT)

        time.sleep(1.0)

        location_thread = start_consumer(clean_uri, location_queue, location_results, stop_event, verbose, exchange="cat.location", routing_key=location_queue)

        payload = b"[SG*1234567890*0066*UD,270524,061232,V,9.999851,N,30.000207,W,0.0,176,11,00,80,99,0,50,00000000,1,1,240,1,36,57745184,22,,00]"
        _ = send_tracker_message(local_port, payload, verbose)

        deadline = time.time() + 10
        while time.time() < deadline and not location_results:
            time.sleep(0.2)
        if not location_results:
            if verbose:
                parsed_debug = debug_fetch(clean_uri, location_input_queue)
                if parsed_debug is not None:
                    print("[debug] parsed queue payload", parsed_debug)
                else:
                    print("[debug] parsed queue empty")
                loc_debug = debug_fetch(clean_uri, location_queue)
                if loc_debug is not None:
                    print("[debug] location queue payload", loc_debug)
                else:
                    print("[debug] location queue empty")
                parser_debug = debug_fetch(clean_uri, "tracker.events.parser")
                if parser_debug is not None:
                    print("[debug] tracker parser queue payload", parser_debug)
                else:
                    print("[debug] tracker parser queue empty")
            raise AssertionError("No location event observed")

        location_event = location_results[0]
        assert location_event.get("tracker_id") == "1234567890", "Location tracker id mismatch"
        assert location_event.get("position") == "at the back of the house", "Location position mismatch"

        if verbose:
            print("[ok] location event", location_event)

    finally:
        stop_event.set()
        for handle in (location_thread,):
            if handle:
                handle.join(timeout=5)
        for proc in (proxy, server, tracker_proc, location_proc):
            if proc:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
        if container:
            stop_podman(container)
        if not keep_logs:
            shutil.rmtree(logdir, ignore_errors=True)


def wait_for_rabbitmq(uri: str, timeout: float = 50.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            params = pika.URLParameters(uri)
            with pika.BlockingConnection(params):
                return
        except Exception:
            time.sleep(0.5)
    raise RuntimeError("RabbitMQ did not become ready")


def main() -> None:
    parser = argparse.ArgumentParser(description="AMQP chain integration test")
    parser.add_argument("--keep-logs", action="store_true", help="Preserve log directory on success")
    parser.add_argument("--verbose", action="store_true", help="Print diagnostic output")
    args = parser.parse_args()
    try:
        run_test(keep_logs=args.keep_logs, verbose=args.verbose)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Integration test failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
