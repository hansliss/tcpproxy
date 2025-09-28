#!/usr/bin/env python3
"""Spin up RabbitMQ in Podman and verify AMQP observer end-to-end."""

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
from pathlib import Path

try:
    import pika
except ImportError as exc:  # pragma: no cover
    print(f"pika module required for AMQP integration test: {exc}", file=sys.stderr)
    sys.exit(2)

BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent
HELPER_SCRIPT = REPO_ROOT / "scripts" / "observer_amqp_publisher.py"
LOG_ROOT = BASE_DIR / "logs"
LOG_ROOT.mkdir(parents=True, exist_ok=True)

SPECIAL_KEYS = {"exchange", "routing_key", "queue"}


def sanitize_uri(uri: str):
    parsed = urllib.parse.urlparse(uri)
    query = urllib.parse.parse_qs(parsed.query)
    clean_pairs = [(k, v) for k, vs in query.items() for v in vs if k not in SPECIAL_KEYS]
    clean_query = urllib.parse.urlencode(clean_pairs, doseq=True)
    sanitized = parsed._replace(query=clean_query)
    return urllib.parse.urlunparse(sanitized), query

def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_port(port: int, timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.25)
            try:
                sock.connect(("127.0.0.1", port))
                return
            except (ConnectionRefusedError, socket.timeout, OSError):
                time.sleep(0.1)
    raise RuntimeError(f"Timed out waiting for localhost:{port}")


def run(command, *, verbose=False, **kwargs):
    if verbose:
        print("[run]", " ".join(command))
    return subprocess.run(command, check=True, **kwargs)


def wait_for_rabbitmq(uri: str, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        clean_uri, _ = sanitize_uri(uri)
        try:
            params = pika.URLParameters(clean_uri)
            connection = pika.BlockingConnection(params)
            connection.close()
            return
        except (pika.exceptions.AMQPConnectionError, OSError):
            time.sleep(0.5)
    raise RuntimeError("RabbitMQ not ready in time")


def start_podman_rabbitmq(host_port: int, verbose: bool) -> str:
    if shutil.which("podman") is None:
        raise RuntimeError("podman is required for this test")
    container_name = f"tcpproxy-rmq-{os.getpid()}-{int(time.time())}"
    image = os.environ.get("TCPPROXY_RABBITMQ_IMAGE", "docker.io/library/rabbitmq:3")
    run([
        "podman",
        "run",
        "--rm",
        "-d",
        "-p",
        f"{host_port}:5672",
        "--name",
        container_name,
        image
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, verbose=verbose)
    return container_name


def stop_podman(container: str) -> None:
    if shutil.which("podman") is None:
        return
    subprocess.run(["podman", "stop", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def start_server(port: int, logdir: Path, verbose: bool) -> subprocess.Popen:
    cmd = [
        sys.executable,
        str(BASE_DIR / "echo_server.py"),
        str(port),
        "--echo",
        "--max-connections",
        "4"
    ]
    server_log = open(logdir / "server.log", "wb")
    if verbose:
        print("[start_server]", " ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=server_log, stderr=subprocess.STDOUT)
    wait_for_port(port)
    return proc


def start_proxy(local_port: int, remote_port: int, logdir: Path, observer: str, verbose: bool) -> subprocess.Popen:
    proxy_bin = REPO_ROOT / "build" / "tcpproxy"
    if not proxy_bin.exists():
        raise RuntimeError("Build the proxy before running this test")
    cmd = [
        str(proxy_bin),
        "-l",
        f"127.0.0.1:{local_port}",
        "-r",
        f"127.0.0.1:{remote_port}",
        "-o",
        str(logdir),
        "-O",
        observer
    ]
    env = os.environ.copy()
    env.setdefault("TCPPROXY_AMQP_HELPER", str(HELPER_SCRIPT))
    proxy_log = open(logdir / "proxy.log", "wb")
    if verbose:
        print("[start_proxy]", " ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=proxy_log, stderr=subprocess.STDOUT, env=env)
    wait_for_port(local_port)
    return proc


def start_consumer(uri: str, queue: str, sink: list, stop_event: threading.Event, verbose: bool) -> threading.Thread:
    def _consume():
        clean_uri, _ = sanitize_uri(uri)
        params = pika.URLParameters(clean_uri)
        if verbose:
            print("[consumer] connecting to", clean_uri)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue=queue, durable=False, auto_delete=True)
        for method, properties, body in channel.consume(queue=queue, inactivity_timeout=1.0):
            if stop_event.is_set():
                break
            if method is None:
                continue
            channel.basic_ack(method.delivery_tag)
            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                payload = body.decode("utf-8", errors="replace")
            sink.append(payload)
            if verbose:
                print("[consumer] received", payload)
            break
        channel.cancel()
        connection.close()
    thread = threading.Thread(target=_consume, daemon=True)
    thread.start()
    return thread


def send_tracker_message(port: int, payload: bytes, verbose: bool) -> bytes:
    with socket.create_connection(("127.0.0.1", port), timeout=5.0) as sock:
        if verbose:
            print("[client] connect 127.0.0.1", port)
        sock.sendall(payload)
        sock.shutdown(socket.SHUT_WR)
        data = sock.recv(65536)
        return data


def run_test(keep_logs: bool = False, verbose: bool = False) -> None:
    logdir = Path(tempfile.mkdtemp(prefix="amqp-integration-", dir=str(LOG_ROOT)))
    container = None
    server = proxy = None
    consumer_thread = None
    observer_results = []
    stop_event = threading.Event()

    try:
        rabbit_port = get_free_port()
        container = start_podman_rabbitmq(rabbit_port, verbose)
        wait_for_port(rabbit_port)

        uri = f"amqp://guest:guest@127.0.0.1:{rabbit_port}/%2F?queue=tcpproxy.integration&routing_key=tcpproxy.integration"

        wait_for_rabbitmq(uri)

        remote_port = get_free_port()
        server = start_server(remote_port, logdir, verbose)

        local_port = get_free_port()
        proxy = start_proxy(local_port, remote_port, logdir, f"amqp={uri}", verbose=verbose)

        consumer_thread = start_consumer(uri, "tcpproxy.integration", observer_results, stop_event, verbose)

        payload = b"[SG*1234567890*0005*LK]"
        reply = send_tracker_message(local_port, payload, verbose)
        if reply != payload:
            raise AssertionError("Proxy failed to echo tracker payload")

        deadline = time.time() + 10
        while time.time() < deadline and not observer_results:
            time.sleep(0.2)
        if not observer_results:
            raise AssertionError("No observer event received from RabbitMQ")

        event = observer_results[0]
        if isinstance(event, dict):
            assert event.get("payload") == payload.decode("ascii"), "Payload mismatch"
            assert event.get("direction") == "client", "Unexpected direction"
        else:
            raise AssertionError("Observer event not JSON")
        if verbose:
            print("[success] received event", event)

    finally:
        stop_event.set()
        if proxy:
            proxy.terminate()
            proxy.wait(timeout=5)
        if server:
            server.terminate()
            server.wait(timeout=5)
        if consumer_thread:
            consumer_thread.join(timeout=5)
        if container:
            stop_podman(container)
        if not keep_logs:
            shutil.rmtree(logdir, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(description="End-to-end AMQP integration test")
    parser.add_argument("--keep-logs", action="store_true", help="Preserve log directory")
    parser.add_argument("--verbose", action="store_true", help="Print progress messages")
    args = parser.parse_args()

    try:
        run_test(keep_logs=args.keep_logs, verbose=args.verbose)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"AMQP integration test failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
