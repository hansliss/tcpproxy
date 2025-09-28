#!/usr/bin/env python3
"""Tiny configurable TCP server used by the integration tests."""

import argparse
import socket
import sys
from contextlib import closing


def parse_args():
    parser = argparse.ArgumentParser(description="Simple TCP test server for tcpproxy")
    parser.add_argument("port", type=int, help="Port to listen on")
    parser.add_argument("response", nargs="?", default=None,
                        help="Optional static response to send once the first request arrives")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address")
    parser.add_argument("--max-connections", type=int, default=1,
                        help="Number of client connections to handle before exiting")
    parser.add_argument("--disconnect-after", choices=["never", "accept", "after-first-recv"],
                        default="never",
                        help="Control when the server drops the client connection for negative tests")
    parser.add_argument("--echo", action="store_true", help="Echo received bytes back to the client")
    return parser.parse_args()


def handle_client(conn, addr, args):
    with closing(conn):
        if args.disconnect_after == "accept":
            return
        if args.response is not None:
            conn.sendall(args.response.encode("utf-8"))
        first = True
        while True:
            data = conn.recv(65536)
            if not data:
                return
            if args.echo:
                conn.sendall(data)
            if first and args.disconnect_after == "after-first-recv":
                return
            first = False


def main():
    args = parse_args()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((args.host, args.port))
        srv.listen()
        handled = 0
        while handled < args.max_connections:
            conn, addr = srv.accept()
            handle_client(conn, addr, args)
            handled += 1


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
