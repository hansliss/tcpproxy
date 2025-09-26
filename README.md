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
tcpproxy -l 0.0.0.0:7700 -r 47.88.85.196:7700 -o /opt/tcpproxy/log
```

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
