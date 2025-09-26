#!/bin/sh

if [ $# -lt 3 ]; then
  echo "Usage: $0 <local addr:port> <remote addr:port> <logdir> [proxy_binary]" >&2
  exit 1
fi

LOCAL=$1
REMOTE=$2
LOGDIR=$3
PROXY=${4:-$(dirname "$0")/../tcpproxy}

if [ ! -x "$PROXY" ]; then
  echo "Proxy binary not found or not executable at $PROXY" >&2
  exit 2
fi

mkdir -p "$LOGDIR"
exec "$PROXY" -l "$LOCAL" -r "$REMOTE" -o "$LOGDIR"
