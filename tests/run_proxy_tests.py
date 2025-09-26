#!/usr/bin/env python3
import argparse
import os
import socket
import subprocess
import sys
import tempfile
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Optional

BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent
SERVER_SCRIPT = BASE_DIR / "echo_server.py"
LOG_ROOT = BASE_DIR / "logs"
LOG_ROOT.mkdir(parents=True, exist_ok=True)


class ManagedProcess:
    def __init__(self, popen: subprocess.Popen, log_file):
        self._popen = popen
        self._log_file = log_file

    def stop(self):
        if self._popen.poll() is None:
            self._popen.terminate()
            try:
                self._popen.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._popen.kill()
        if self._log_file:
            self._log_file.close()

    def returncode(self):
        return self._popen.returncode


@dataclass
class TestCase:
    name: str
    description: str
    server_options: Dict[str, object]
    client_action: Callable[[int], None]
    event_check: Optional[Callable[[Path], None]] = None


def get_free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_port(host: str, port: int, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            sock.settimeout(0.25)
            try:
                sock.connect((host, port))
                return
            except (ConnectionRefusedError, socket.timeout, OSError):
                time.sleep(0.05)
    raise RuntimeError(f"Timed out waiting for {host}:{port} to become ready")


def start_server(port: int, options: Dict[str, object], logdir: Path) -> ManagedProcess:
    cmd = [sys.executable, str(SERVER_SCRIPT), str(port)]
    response = options.get("response")
    if response:
        cmd.append(response)
    if options.get("echo"):
        cmd.append("--echo")
    disconnect_after = options.get("disconnect_after")
    if disconnect_after and disconnect_after != "never":
        cmd.extend(["--disconnect-after", disconnect_after])
    max_conn = options.get("max_connections")
    if max_conn is None:
        max_conn = 4
    cmd.extend(["--max-connections", str(max_conn)])
    host = options.get("host") or "127.0.0.1"
    cmd.extend(["--host", host])
    server_log = open(logdir / "server.log", "wb")
    proc = subprocess.Popen(cmd, stdout=server_log, stderr=subprocess.STDOUT)
    wait_for_port(host, port)
    return ManagedProcess(proc, server_log)


def resolve_proxy_binary() -> Path:
    env = os.environ.get("TCPPROXY_BIN")
    candidates = []
    if env:
        candidates.append(Path(env))
    candidates.extend([
        REPO_ROOT / "tcpproxy",
        REPO_ROOT / "build" / "tcpproxy",
    ])
    for candidate in candidates:
        if candidate and candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Proxy binary not found. Set TCPPROXY_BIN or build the project first."
    )


def start_proxy(local_port: int,
                remote_port: int,
                logdir: Path,
                observer_config: Optional[str]) -> ManagedProcess:
    proxy_bin = resolve_proxy_binary()
    local = f"127.0.0.1:{local_port}"
    remote = f"127.0.0.1:{remote_port}"
    cmd = [str(proxy_bin), "-l", local, "-r", remote, "-o", str(logdir)]
    if observer_config:
        cmd.extend(["-O", observer_config])
    proxy_log = open(logdir / "proxy.log", "wb")
    proc = subprocess.Popen(cmd, stdout=proxy_log, stderr=subprocess.STDOUT)
    wait_for_port("127.0.0.1", local_port)
    return ManagedProcess(proc, proxy_log)


def send_and_receive(port: int, payload: bytes, expect_response: bool = True) -> bytes:
    data = bytearray()
    with closing(socket.create_connection(("127.0.0.1", port), timeout=5.0)) as sock:
        sock.sendall(payload)
        sock.shutdown(socket.SHUT_WR)
        if not expect_response:
            return b""
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
            data.extend(chunk)
    return bytes(data)


def client_basic_echo(port: int) -> None:
    payload = b"hello through proxy"
    reply = send_and_receive(port, payload)
    if reply != payload:
        raise AssertionError("Echo reply mismatch")


def client_large_payload(port: int) -> None:
    payload = b"X" * (131072 * 2 + 137)
    reply = send_and_receive(port, payload)
    if reply != payload:
        raise AssertionError("Large payload corrupted")


def client_server_drop(port: int) -> None:
    payload = b"trigger drop"
    with closing(socket.create_connection(("127.0.0.1", port), timeout=5.0)) as sock:
        sock.sendall(payload)
        sock.shutdown(socket.SHUT_WR)
        try:
            chunk = sock.recv(1024)
            if chunk:
                raise AssertionError("Expected connection drop, but received data")
        except ConnectionResetError:
            pass


def client_client_drop(port: int) -> None:
    payload = b"client drop test"
    with closing(socket.create_connection(("127.0.0.1", port), timeout=5.0)) as sock:
        sock.sendall(payload)
    time.sleep(0.2)


def client_server_push(port: int) -> None:
    with closing(socket.create_connection(("127.0.0.1", port), timeout=5.0)) as sock:
        data = sock.recv(1024)
        if data != b"server says hi":
            raise AssertionError("Did not receive expected server greeting")


def client_tracker_message(port: int) -> None:
    payload = b"[SG*1234567890*0005*LK]"
    reply = send_and_receive(port, payload)
    if reply != payload:
        raise AssertionError("Tracker message mismatch")


def expect_tracker_event(log_path: Path) -> None:
    if not log_path.exists():
        raise AssertionError("Observer log not created")
    contents = log_path.read_text(encoding="utf-8")
    if "[SG*" not in contents:
        raise AssertionError("Expected bracketed tracker message in observer log")


TESTS = [
    TestCase(
        name="basic_echo",
        description="Simple echo round-trip",
        server_options={"echo": True},
        client_action=client_basic_echo,
    ),
    TestCase(
        name="large_payload",
        description="Echo large payload to exercise repeated send() handling",
        server_options={"echo": True},
        client_action=client_large_payload,
    ),
    TestCase(
        name="server_drop",
        description="Remote closes after first recv; client should observe EOF",
        server_options={"disconnect_after": "after-first-recv"},
        client_action=client_server_drop,
    ),
    TestCase(
        name="client_drop",
        description="Client closes without reading; proxy and server stay healthy",
        server_options={"echo": True},
        client_action=client_client_drop,
    ),
    TestCase(
        name="server_push",
        description="Server sends data first and closes cleanly",
        server_options={"response": "server says hi"},
        client_action=client_server_push,
    ),
    TestCase(
        name="tracker_message",
        description="Bracketed tracker-style payloads are parsed by the observer",
        server_options={"echo": True},
        client_action=client_tracker_message,
        event_check=expect_tracker_event,
    ),
]


def run_test_case(test: TestCase, keep_logs: bool = False) -> bool:
    remote_port = get_free_port()
    local_port = get_free_port()
    test_logdir = Path(tempfile.mkdtemp(prefix=f"{test.name}-", dir=str(LOG_ROOT)))
    observer_log = test_logdir / "observer_events.log"
    server = proxy = None
    try:
        server = start_server(remote_port, test.server_options, test_logdir)
        proxy = start_proxy(local_port,
                            remote_port,
                            test_logdir,
                            f"file={observer_log}")
        test.client_action(local_port)
        if test.event_check:
            test.event_check(observer_log)
        return True
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Test '{test.name}' failed: {exc}")
        return False
    finally:
        if proxy:
            proxy.stop()
        if server:
            server.stop()
        if not keep_logs:
            for child in test_logdir.iterdir():
                child.unlink()
            test_logdir.rmdir()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run end-to-end tests against tcpproxy")
    parser.add_argument("--keep-logs", action="store_true", help="Preserve per-test log files")
    parser.add_argument("--test", choices=[t.name for t in TESTS], help="Run a single test case")
    args = parser.parse_args()

    tests = [t for t in TESTS if not args.test or t.name == args.test]

    results = []
    for test in tests:
        print(f"Running {test.name}: {test.description}...", end=" ")
        success = run_test_case(test, keep_logs=args.keep_logs)
        results.append((test, success))
        print("OK" if success else "FAIL")

    failures = [t for t, success in results if not success]
    if failures:
        print("Failures: " + ", ".join(t.name for t in failures))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
