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
LOCATION_DAEMON = REPO_ROOT / "scripts" / "cat_location_daemon.py"
TRACKER_PARSER_BIN = Path(os.environ.get("TCPPROXY_TRACKER_PARSER", REPO_ROOT / "build" / "tracker_parser_daemon"))
KML_PATH = REPO_ROOT / "Locations.kml"
TRACKER_EXCHANGE = "tcpproxy.events"
TRACKER_ROUTING_KEY = "tracker.raw"
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


def ensure_queue(
    uri: str,
    queue: str,
    *,
    exchange: str | None = None,
    routing_key: str | None = None,
    verbose: bool = False,
    durable: bool = False,
    auto_delete: bool = True,
) -> None:
    clean_uri, _ = sanitize_uri(uri)
    params = pika.URLParameters(clean_uri)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=durable, auto_delete=auto_delete)
    if exchange:
        if verbose:
            print(f"[ensure_queue] binding {queue} to {exchange} ({routing_key or '#'} )")
        channel.exchange_declare(exchange=exchange, exchange_type='topic', durable=True, auto_delete=False)
        channel.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key or "#")
    elif verbose:
        print(f"[ensure_queue] declared queue {queue}")
    connection.close()

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


def wait_for_rabbitmq(uri: str, timeout: float = 50.0) -> None:
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


def start_tracker_parser(
    input_uri: str,
    input_queue: str,
    output_uri: str,
    output_exchange: str,
    output_routing_key: str,
    logdir: Path,
    verbose: bool,
) -> subprocess.Popen:
    if not TRACKER_PARSER_BIN.exists():
        raise RuntimeError("Build the tracker_parser_daemon before running this test")

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
        "INFO",
    ]
    parser_log = open(logdir / "tracker_parser.log", "wb")
    if verbose:
        print("[start_tracker_parser]", " ".join(cmd))
    return subprocess.Popen(cmd, stdout=parser_log, stderr=subprocess.STDOUT)


def start_consumer(uri: str, queue: str, sink: list, stop_event: threading.Event, verbose: bool, *, exchange: str | None = None, routing_key: str | None = None) -> threading.Thread:
    def _consume():
        clean_uri, _ = sanitize_uri(uri)
        params = pika.URLParameters(clean_uri)
        if verbose:
            print("[consumer] connecting to", clean_uri)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue=queue, durable=False, auto_delete=True)
        if exchange:
            channel.exchange_declare(exchange=exchange, exchange_type='topic', durable=True, auto_delete=False)
            channel.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key or "#")
        if verbose:
            if exchange:
                print(f"[consumer] ready queue={queue} exchange={exchange} rk={routing_key or '#'}")
            else:
                print(f"[consumer] ready queue={queue}")
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
    server = proxy = location_proc = tracker_parser_proc = None
    tracker_results: list = []
    tracker_thread = None
    location_results_primary: list = []
    location_results_secondary: list = []
    location_thread_primary = None
    location_thread_secondary = None
    parsed_results: list = []
    parsed_thread = None
    stop_event = threading.Event()

    try:
        rabbit_port = get_free_port()
        container = start_podman_rabbitmq(rabbit_port, verbose)
        wait_for_port(rabbit_port)

        uri = f"amqp://guest:guest@127.0.0.1:{rabbit_port}/%2F?exchange={TRACKER_EXCHANGE}&routing_key={TRACKER_ROUTING_KEY}"

        wait_for_rabbitmq(uri)

        remote_port = get_free_port()
        server = start_server(remote_port, logdir, verbose)

        local_port = get_free_port()
        proxy = start_proxy(local_port, remote_port, logdir, f"amqp={uri}", verbose=verbose)

        location_exchange = "cat.location"
        location_output_queue = "cat.location.test"
        tracker_input_queue = "tracker.events.test"
        tracker_tap_queue = "tracker.events.tap"
        tracker_parser_input_queue = "tracker.events.parser"
        tracker_parsed_exchange = "tracker.events.parsed"
        tracker_parsed_routing_key = "tracker.parsed"
        tracker_parsed_queue = "tracker.parsed.test"
        input_uri_clean, _ = sanitize_uri(uri)
        output_uri = f"amqp://guest:guest@127.0.0.1:{rabbit_port}/%2F"
        ensure_queue(
            input_uri_clean,
            tracker_input_queue,
            exchange=TRACKER_EXCHANGE,
            routing_key=TRACKER_ROUTING_KEY,
            verbose=verbose,
        )
        ensure_queue(
            input_uri_clean,
            tracker_parser_input_queue,
            exchange=TRACKER_EXCHANGE,
            routing_key=TRACKER_ROUTING_KEY,
            verbose=verbose,
            durable=True,
            auto_delete=False,
        )
        location_cmd = [
            sys.executable,
            str(LOCATION_DAEMON),
            "--input-uri",
            input_uri_clean,
            "--input-queue",
            tracker_input_queue,
            "--input-exchange",
            TRACKER_EXCHANGE,
            "--input-routing-key",
            TRACKER_ROUTING_KEY,
            "--output-uri",
            output_uri,
            "--output-exchange",
            location_exchange,
            "--output-routing-key",
            location_output_queue,
            "--kml",
            str(KML_PATH),
            "--log-level",
            "WARNING",
        ]
        if verbose:
            print("[start_location]", " ".join(location_cmd))
        location_proc = subprocess.Popen(location_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        tracker_parser_proc = start_tracker_parser(
            input_uri_clean,
            tracker_parser_input_queue,
            output_uri,
            tracker_parsed_exchange,
            tracker_parsed_routing_key,
            logdir,
            verbose,
        )
        time.sleep(1.0)

        consumer_uri = f"amqp://guest:guest@127.0.0.1:{rabbit_port}/%2F"
        location_thread_primary = start_consumer(
            consumer_uri,
            location_output_queue,
            location_results_primary,
            stop_event,
            verbose,
            exchange=location_exchange,
            routing_key=location_output_queue,
        )
        location_thread_secondary = start_consumer(
            consumer_uri,
            f"{location_output_queue}.tap",
            location_results_secondary,
            stop_event,
            verbose,
            exchange=location_exchange,
            routing_key=location_output_queue,
        )
        ensure_queue(
            consumer_uri,
            tracker_tap_queue,
            exchange=TRACKER_EXCHANGE,
            routing_key=TRACKER_ROUTING_KEY,
            verbose=verbose,
        )
        tracker_thread = start_consumer(
            consumer_uri,
            tracker_tap_queue,
            tracker_results,
            stop_event,
            verbose,
            exchange=TRACKER_EXCHANGE,
            routing_key=TRACKER_ROUTING_KEY,
        )
        ensure_queue(
            consumer_uri,
            tracker_parsed_queue,
            exchange=tracker_parsed_exchange,
            routing_key=tracker_parsed_routing_key,
            verbose=verbose,
        )
        parsed_thread = start_consumer(
            consumer_uri,
            tracker_parsed_queue,
            parsed_results,
            stop_event,
            verbose,
            exchange=tracker_parsed_exchange,
            routing_key=tracker_parsed_routing_key,
        )

        payload = b"[SG*1234567890*0066*UD,270524,061232,V,9.999851,N,30.000207,W,0.0,176,11,00,80,99,0,50,00000000,1,1,240,1,36,57745184,22,,00]"
        reply = send_tracker_message(local_port, payload, verbose)
        if reply != payload:
            raise AssertionError("Proxy failed to echo tracker payload")

        deadline = time.time() + 10
        while time.time() < deadline and (
            not tracker_results
            or not location_results_primary
            or not location_results_secondary
            or not parsed_results
        ):
            time.sleep(0.2)
        if not tracker_results:
            raise AssertionError("No tracker event received from RabbitMQ")
        if not location_results_primary or not location_results_secondary:
            raise AssertionError("No location event received from RabbitMQ")
        if not parsed_results:
            raise AssertionError("No parsed tracker event received")

        tracker_event = tracker_results[0]
        if isinstance(tracker_event, dict):
            if tracker_event.get("payload") != payload.decode("ascii"):
                raise AssertionError("Tracker payload mismatch")
        else:
            raise AssertionError("Tracker event not JSON")

        expected_position = "at the back of the house"
        expected_tracker = "1234567890"
        for idx, event in enumerate((location_results_primary[0], location_results_secondary[0]), start=1):
            if isinstance(event, dict):
                if event.get("position") != expected_position:
                    raise AssertionError(f"Location mismatch (consumer {idx})")
                if event.get("tracker_id") != expected_tracker:
                    raise AssertionError(f"Tracker ID mismatch (consumer {idx})")
            else:
                raise AssertionError(f"Location event {idx} not JSON")
        parsed_event = parsed_results[0]
        if isinstance(parsed_event, dict):
            if parsed_event.get("tracker_id") != expected_tracker:
                raise AssertionError("Parsed tracker ID mismatch")
            if abs(parsed_event.get("latitude", 0.0) - 9.999851) > 1e-6:
                raise AssertionError("Parsed latitude mismatch")
            if abs(parsed_event.get("longitude", 0.0) + 30.000207) > 1e-6:
                raise AssertionError("Parsed longitude mismatch")
        else:
            raise AssertionError("Parsed event not JSON")

        if verbose:
            print("[success] location consumer 1", location_results_primary[0])
            print("[success] location consumer 2", location_results_secondary[0])
            print("[success] parsed consumer", parsed_event)

    finally:
        stop_event.set()
        if proxy:
            proxy.terminate()
            proxy.wait(timeout=5)
        if server:
            server.terminate()
            server.wait(timeout=5)
        if tracker_parser_proc:
            tracker_parser_proc.terminate()
            try:
                tracker_parser_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                tracker_parser_proc.kill()
                tracker_parser_proc.wait(timeout=5)
        if location_proc:
            location_proc.terminate()
            try:
                stdout, stderr = location_proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                location_proc.kill()
                stdout, stderr = location_proc.communicate(timeout=5)
            if verbose:
                if stdout:
                    print("[location stdout]", stdout.decode("utf-8", errors="ignore"))
                if stderr:
                    print("[location stderr]", stderr.decode("utf-8", errors="ignore"))
        if location_thread_primary:
            location_thread_primary.join(timeout=5)
        if location_thread_secondary:
            location_thread_secondary.join(timeout=5)
        if tracker_thread:
            tracker_thread.join(timeout=5)
        if parsed_thread:
            parsed_thread.join(timeout=5)
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
