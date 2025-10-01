# tcpproxy
Accept TCP connections and mirror them to a remote server, while logging

## Description
This was made to log traffic between GPS trackers and their servers. In order to use it,
run this proxy on a suitable Linux box (like a Raspberry Pi) that's reachable from the entire
Internet, and then use an SMS command to point your GPS tracker to this proxy instead of the
real server.

Log files will be created automatically for each connection, with a filename built
from a timestamp and the client IP address. Any errors will be (badly) logged in
the file "errors.log".

The two servers I'm aware of are TKSTAR's at 47.88.85.196:7700 for TK909 and TK911 and probably
many more similar models, and another one (like the "FA29" from Shenzhen i365-Tech Co., Limited),
popular with tracking watches and personal trackers, at 52.28.132.157:8001. Curiously, the
latter uses a very similar protocol as TKSTAR.

For the TKSTAR trackers, you can use the "adminip" SMS command to point the tracker to your public
IP address:
```
adminip123456 10.20.30.40 7700
```

Then run the proxy on that server using
```
tcpproxy -l 0.0.0.0:7700 -r 47.88.85.196:7700 -o /var/log/tcpproxy -p /run/tcpproxy/tcpproxy.pid
```
Create `/run/tcpproxy` first when running manually; the provided systemd unit
uses `RuntimeDirectory` to handle this automatically.

For the other tracker, the SMS command is different:
```
pw,123456,ip,10.20.30.40,8001#
```

A start script and systemd service definition is included for reference.

Additions and fixes are welcome.

## Building
This project now uses CMake. A typical build from the repository root looks like:

```
mkdir -p build
cmake -S . -B build
cmake --build build
```

Install to a prefix (optional) with `cmake --install build --prefix /desired/path`.

To run the end-to-end test harness after building, you can invoke either

```
cmake --build build --target proxy-tests
```

or run the Python script directly while pointing it at the freshly built
binary:

```
TCPPROXY_BIN=$(pwd)/build/tcpproxy tests/run_proxy_tests.py
```

## Observer Events
An optional observer can mirror the proxied traffic and emit parsed tracker
messages to a separate logfile without altering the live TCP stream. Enable it
with the `-O` flag and point it at an event file:

```
tcpproxy -l 0.0.0.0:7700 -r 47.88.85.196:7700 -o /var/log/tcpproxy -O file=/var/log/tcpproxy/events.log
```

Each observer line records the chunk timestamp, direction (`client` or
`server`), the source IP address, the connection identifier, and the bracketed
payload detected by the parser. The parsing logic lives in dedicated observer
modules so future processing—such as raising AMQP events—can evolve without
touching the core proxy loop.

### RabbitMQ Observer

The observer can also publish events to RabbitMQ by passing an AMQP URI:

```
tcpproxy -l 0.0.0.0:7700 -r 47.88.85.196:7700 -o /var/log/tcpproxy \
  -O amqp=amqp://guest:guest@127.0.0.1:5672/%2F?exchange=tcpproxy.events&routing_key=tracker.raw
```

Events are published as JSON documents containing the same fields as the file
logger. When AMQP mode is selected (`-O amqp=...`) the proxy now connects to
RabbitMQ directly via librabbitmq; no external helper process or Python
dependencies are required. The observer supports exactly one target at a time,
so `-O file=...` and `-O amqp=...` cannot be combined.

The proxy and both helper daemons accept a simple configuration file via
`--config /path/to.conf`. Each file is composed of `key=value` pairs (blank
lines and `#` comments are ignored), and any value supplied on the command line
overrides the file. For `tcpproxy`, recognised keys include `local`, `remote`,
`log-dir`, `pidfile`, and `observer`. For `cat_location_daemon`, keys include
`input-uri`, `input-queue`, `input-exchange`, `input-routing-key`, `output-uri`,
`output-exchange`, `output-routing-key`, `kml`, `pidfile`, and `log-level`. For
`tracker_parser_daemon`, recognised keys include `input-uri`, `input-queue`,
`input-exchange`, `input-routing-key`, `output-uri`, `output-exchange`,
`output-routing-key`, `pidfile`, and `log-level`.

When using the AMQP mode, specify the target exchange and routing key in the
connection URI query (for example
`amqp://.../?exchange=tcpproxy.events&routing_key=tracker.raw`). Consumers can
bind their own queues to that exchange to receive near-raw tracker payloads.

An example consumer lives at `tests/amqp_consumer_example.py`:

```
python3 tests/amqp_consumer_example.py \
  amqp://guest:guest@127.0.0.1:5672/%2F?queue=tcpproxy.events
```

### Location Daemon

`cat_location_daemon` (installed under `libexec/tcpproxy`) consumes observer
events from RabbitMQ, matches them against the placemarks in `Locations.kml`,
and republishes a location update whenever the nearest named position changes.
Each message contains the tracker ID, timestamp, and position string.

Copy `/usr/local/etc/Locations.kml.sample` to `/usr/local/etc/Locations.kml`
and edit it to match your real-world placemarks before launching the daemon.

```
/usr/local/libexec/tcpproxy/cat_location_daemon \
  --input-uri=amqp://guest:guest@127.0.0.1:5672/%2F \
  --input-queue=tracker.events.cli \
  --input-exchange=tcpproxy.events \
  --input-routing-key=tracker.raw \
  --output-uri=amqp://guest:guest@127.0.0.1:5672/%2F \
  --output-exchange=cat.location \
  --output-routing-key=cat.position \
  --kml=/usr/local/etc/Locations.kml
```

Example configuration entries for the location daemon:

```
input-uri=amqp://guest:guest@127.0.0.1:5672/%2F
input-queue=tracker.events.cli
input-exchange=tcpproxy.events
output-exchange=cat.location
output-routing-key=cat.position
kml=/usr/local/etc/Locations.kml
log-level=INFO
```

Example configuration entries for the proxy daemon:

```
local=0.0.0.0:7000
remote=tracker.example.net:5000
log-dir=/var/log/tcpproxy
pidfile=/run/tcpproxy.pid
observer=file=/var/log/tcpproxy/events.log
```

Run it as a foreground process for local testing, or install it as a systemd
service that points at the same AMQP broker as the proxy. A sample unit file
is installed as `cat_location_daemon.service.sample`. Bind a queue to the `cat.location`
exchange (topic) with the `cat.position` routing key to receive updates.

## Test Harness
An end-to-end test harness lives under `tests/` and exercises the proxy with a
local client and server pair. Build `tcpproxy` and then run

```
tests/run_proxy_tests.py
```

The script starts a disposable echo server, brings up the proxy via
`tests/start_proxy.sh`, and runs several scenarios:
- basic request/response echo
- large payload forwarding (covers partial `send` writes)
- remote host dropping the connection mid-stream
- client closing without reading responses
- server-initiated data before any client payload

Use `--test <name>` to run a single case or `--keep-logs` to preserve the
generated per-test logs under `tests/logs/` for inspection.

### RabbitMQ Integration Test (requires Podman)

To exercise the AMQP observer end-to-end, run the integration script that
launches RabbitMQ inside Podman, proxies a tracker payload, and confirms the
event arrives on the broker:

```
tests/run_amqp_integration.py
```

By default the script removes containers and logs once it finishes. Pass
`--keep-logs` to retain the generated artifacts for inspection.

## Internals

The repository is intentionally small and is organised as follows:

- `tcpproxy.c` – main entry point and listening loop. Accepts connections,
  forks workers, proxies bytes with `select(2)`, writes per-connection logs,
  and invokes the observer hook. The binary installs to `${prefix}/sbin`
  (default `/usr/local/sbin`). IPv4 and IPv6 endpoints are supported via
  `getaddrinfo`, so the proxy can bind or forward to whichever family the
  system resolves.
- `observer.c` / `observer.h` – pluggable observer back-end. It parses the
  `-O` option, maintains shared configuration, and creates per-connection
  instances that either append to a logfile or publish JSON events directly to
  RabbitMQ.
- `tracker_parser.c` / `tracker_parser.h` – streaming parser used by the
  observer to reassemble tracker packets from arbitrary chunk boundaries and
  emit bracketed messages only when they pass basic validation.
- `tracker_parser_daemon.c` – consumes raw tracker events from the
  shared `tcpproxy.events` exchange, parses the payload, and republishes the
  structured JSON to a second exchange for downstream consumers.
- `cat_location_daemon.c` / `cat_location_daemon` – consumes tracker events from
  the shared exchange, maps coordinates to placemarks from `Locations.kml`, and
  republishes location updates on its own exchange.
- `tests/` – Python utilities for exercising the proxy. `run_proxy_tests.py`
  spins up a client/server pair for various scenarios, `run_amqp_integration.py`
  orchestrates the Podman-backed RabbitMQ flow, and `echo_server.py` provides a
  configurable dummy backend. Logs from the tests land under `tests/logs/`.

Key behaviours:

- Every connection spawned by `handle_connection()` maintains a logging
  context so error messages in `errors.log` can be traced back to the specific
  session and endpoints involved.
- `copy_message()` retries short writes, mirrors traffic to disk, and notifies
  the observer before forwarding bytes to the peer.
- Supplying `-p` records the daemon PID to a file that is automatically
  removed on graceful exit and when systemd sends termination signals.
- The observer buffers per-direction fragments until complete bracketed
  payloads are reconstructed. Events contain the timestamp, direction, source
  IP, connection identifier, and payload, and they can be routed to multiple
  back-ends without touching the hot-path forwarding code.
- The optional AMQP integration is now handled entirely inside the proxy via
  librabbitmq, so no external helper needs to be shipped or configured.
- The overall install prefix defaults to `/usr/local`; adjust it via
  `-DCMAKE_INSTALL_PREFIX=` when configuring if you need a different layout.

## Authors

- Hans Liss
- ChatGPT (observer integration and tooling)
