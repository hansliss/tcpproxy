#!/usr/bin/env python3
"""Consume tracker events from RabbitMQ, parse them, and publish JSON updates."""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from typing import Callable, Dict, Optional
import pika
from pika.exceptions import (
    AMQPConnectionError,
    ChannelClosedByBroker,
    NackError,
    UnroutableError,
    StreamLostError,
    ConnectionClosed,
    AMQPChannelError,
)

LOGGER = logging.getLogger("tracker_parser_daemon")

LOGGER.info("pika version: %s", getattr(pika, "__version__", "unknown"))

import re


class _PikaConnectionResetFilter(logging.Filter):
    """Suppress verbose stack traces for expected connection resets."""

    patterns = ("Connection reset by peer", "StreamLostError")

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return not any(pattern in msg for pattern in self.patterns)


def _install_pika_filters() -> None:
    flt = _PikaConnectionResetFilter()
    targets = (
        "",
        "pika",
        "pika.connection",
        "pika.channel",
        "pika.adapters",
        "pika.adapters.base_connection",
        "pika.adapters.blocking_connection",
        "pika.adapters.utils",
        "pika.adapters.utils.connection_workflow",
        "pika.adapters.utils.io_services_utils",
    )
    for name in targets:
        logging.getLogger(name).addFilter(flt)


_install_pika_filters()

TK_POS_PATTERN = re.compile(
    r"^\[([A-Z0-9]*)\*(?P<device>[0-9]+)\*[0-9A-Fa-f]+\*UD[^,]*,"  # header with device id
    r"(?P<day>..)(?P<month>..)(?P<year>..),(?P<hour>..)(?P<minute>..)(?P<second>..),(?P<status>[AV]),"  # date/time/status
    r"(?P<lat>[-0-9.]*),(?P<ns>[NS]),"  # latitude + hemisphere
    r"(?P<lon>[-0-9.]*),(?P<ew>[EW]),"  # longitude + hemisphere
    r"(?P<speed>[.0-9]*),(?P<direction>[-.0-9]*),(?P<tkunk01>[^,]*),"
    r"(?P<nsats>[0-9]*),(?P<tkunk02>[^,]*),"
    r"(?P<bat>[.0-9]*),(?P<tkunk03>[^,]*),(?P<tkunk04>[^,]*),(?P<tkunk05>[^,]*),"
    r"(?P<ntowers>[0-9]*),(?P<mnc>[0-9]*),(?P<mcc>[0-9]*),(?P<tkunk06>[0-9]*)"
    r"(?P<celltowers>.*),,00\]$"
)

FA_POS_PATTERN = re.compile(
    r"^\[([A-Z0-9]*)\*(?P<device>[0-9]+)\*[0-9A-Fa-f]+\*UD[^,]*,"  # header with device id
    r"(?P<day>..)(?P<month>..)(?P<year>..),(?P<hour>..)(?P<minute>..)(?P<second>..),(?P<status>[AV]),"  # date/time/status
    r"(?P<lat>[-0-9.]*),(?P<ns>[NS]),"  # latitude + hemisphere
    r"(?P<lon>[-0-9.]*),(?P<ew>[EW]),"  # longitude + hemisphere
    r"(?P<speed>[.0-9]*),(?P<direction>[-.0-9]*),(?P<faunk01>[^,]*),"
    r"(?P<nsats>[0-9]*),(?P<faunk02>[^,]*),"
    r"(?P<bat>[.0-9]*),"
    r"(?P<faunk03>[^,]*),(?P<faunk04>[^,]*),(?P<faunk05>[^,]*),"
    r"(?P<faunk06>[^,]*),(?P<faunk07>[^,]*),(?P<faunk08>[^,]*).*\]$"
)


def _parse_float(value: str, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_tracker_payload(payload: str) -> Optional[Dict[str, object]]:
    match = TK_POS_PATTERN.match(payload) or FA_POS_PATTERN.match(payload)
    if not match:
        LOGGER.warning("Unparsed tracker event: %s", payload)
        return None

    year = int(match.group('year'))
    year += 1900 if year >= 90 else 2000
    date = f"{year:04d}-{match.group('month')}-{match.group('day')}"
    timestamp = f"{match.group('hour')}:{match.group('minute')}:{match.group('second')}"

    latitude = _parse_float(match.group('lat'))
    if latitude > 0 and match.group('ns') == 'S':
        latitude = -latitude
    longitude = _parse_float(match.group('lon'))
    if longitude > 0 and match.group('ew') == 'W':
        longitude = -longitude

    parsed: Dict[str, object] = {
        "tracker_id": match.group('device'),
        "date": date,
        "time": timestamp,
        "status": match.group('status'),
        "latitude": latitude,
        "longitude": longitude,
        "speed": _parse_float(match.group('speed')),
        "direction": _parse_float(match.group('direction')),
        "battery": _parse_float(match.group('bat')),
    }

    return parsed


class TrackerDataPublisher:
    """Publish parsed tracker data when a payload is available."""

    def __init__(self, publish_func: Callable[[Dict[str, object]], None]) -> None:
        self.publish_func = publish_func

    def process_event(self, event: Dict[str, object]) -> bool:
        payload = event.get('payload')
        if not payload:
            return False

        direction = event.get('direction')
        if direction and direction != 'client':
            return True  # acknowledge silently

        message = parse_tracker_payload(payload)
        if not message:
            return True

        if event.get('timestamp'):
            message['timestamp'] = event['timestamp']

        self.publish_func(message)
        return True


class TrackerParserDaemon:
    MAX_PUBLISH_RETRIES = 5

    def __init__(
        self,
        input_uri: str,
        input_queue: str,
        output_uri: str,
        output_exchange: str,
        output_routing_key: str,
        *,
        input_exchange: Optional[str] = None,
        input_routing_key: Optional[str] = None,
    ) -> None:
        self.input_uri = input_uri
        self.input_queue = input_queue
        self.input_exchange = input_exchange
        self.input_routing_key = input_routing_key or input_queue or "#"
        self.output_uri = output_uri
        self.output_exchange = output_exchange
        self.output_routing_key = output_routing_key

        self._stop = False
        self._input_connection: Optional[pika.BlockingConnection] = None
        self._output_connection: Optional[pika.BlockingConnection] = None
        self._input_channel: Optional[pika.channel.Channel] = None
        self._output_channel: Optional[pika.channel.Channel] = None
        self._publisher: Optional[TrackerDataPublisher] = None

    def stop(self, *_: object) -> None:
        LOGGER.info("Stopping daemon")
        self._stop = True
        if self._input_channel and self._input_channel.is_open:
            self._input_channel.stop_consuming()

    def _close_connections(self) -> None:
        self._reset_input_connection()
        self._reset_output_connection()
        self._publisher = None

    def _reset_input_connection(self) -> None:
        if self._input_channel:
            try:
                if self._input_channel.is_open:
                    self._input_channel.close()
            except Exception:
                pass
        if self._input_connection:
            try:
                if self._input_connection.is_open:
                    self._input_connection.close()
            except Exception:
                pass
        self._input_channel = None
        self._input_connection = None

    def _reset_output_connection(self) -> None:
        if self._output_channel:
            try:
                if self._output_channel.is_open:
                    self._output_channel.close()
            except Exception:
                pass
        if self._output_connection:
            try:
                if self._output_connection.is_open:
                    self._output_connection.close()
            except Exception:
                pass
        self._output_channel = None
        self._output_connection = None

    def _ensure_connections(self) -> None:
        self._ensure_input_connection()
        self._ensure_output_connection()

    def _ensure_input_connection(self) -> None:
        if self._input_connection and self._input_connection.is_open:
            return
        LOGGER.info("Connecting to RabbitMQ (input)")
        params = pika.URLParameters(self.input_uri)
        # Keep the consumer connection alive
        params.heartbeat = 30
        params.blocked_connection_timeout = 300
        params.socket_timeout = 10
        self._input_connection = pika.BlockingConnection(params)
        self._input_channel = self._input_connection.channel()
        self._input_channel.basic_qos(prefetch_count=1)
        self._input_channel.queue_declare(queue=self.input_queue, durable=True, auto_delete=False)
        if self.input_exchange:
            self._input_channel.exchange_declare(exchange=self.input_exchange, exchange_type='topic', durable=True, auto_delete=False)
            self._input_channel.queue_bind(queue=self.input_queue, exchange=self.input_exchange, routing_key=self.input_routing_key)

    def _ensure_output_connection(self) -> None:
        if self._output_connection and self._output_connection.is_open:
            if not self._output_channel or self._output_channel.is_closed:
                self._output_channel = self._output_connection.channel()
                self._declare_output_topology()
            return
        LOGGER.info("Connecting to RabbitMQ (output)")
        out_params = pika.URLParameters(self.output_uri)
        # Keep the producer connection alive
        out_params.heartbeat = 30
        out_params.blocked_connection_timeout = 300
        out_params.socket_timeout = 10
        self._output_connection = pika.BlockingConnection(out_params)
        self._output_channel = self._output_connection.channel()
        self._declare_output_topology()

    def _declare_output_topology(self) -> None:
        if self._output_channel is None:
            return
        if self.output_exchange:
            self._output_channel.exchange_declare(exchange=self.output_exchange, exchange_type='topic', durable=True)
        else:
            self._output_channel.queue_declare(queue=self.output_routing_key, durable=True, auto_delete=False)
        # Enable confirms & unroutable returns
        self._output_channel.confirm_delivery()
        self._output_channel.add_on_return_callback(self._on_message_returned)

        self.CONFIRM_TIMEOUT = 1.0

        if not self._publisher:
            def publisher(payload: Dict[str, object]) -> None:
                self._publish_with_retry(payload)

            self._publisher = TrackerDataPublisher(publisher)

    def _on_message_returned(self, ch, method, props, body):
        msg = body.decode("utf-8", "replace")
        LOGGER.error("UNROUTABLE basic.return exch=%r rk=%r code=%s text=%r body=%s",
                     method.exchange, method.routing_key, method.reply_code, method.reply_text, msg[:256])
        raise UnroutableError(f"Unroutable to {method.exchange}:{method.routing_key} "
                              f"({method.reply_code} {method.reply_text})")

    def _pump_output_connection(self) -> None:
        if self._output_connection and self._output_connection.is_open:
            # services I/O and heartbeats once, without real sleeping
            self._output_connection.sleep(0)

    def _publish_with_retry(self, payload: Dict[str, object]) -> None:
        """
        Pika 1.2-safe publish with retries.
        - Uses legacy confirms: basic_publish returns True (ACK) / False (NACK).
        - Raises UnroutableError immediately on basic.return (mandatory=True).
        - Retries on connection/channel errors or NACKs, with backoff.
        """
        body = json.dumps(payload).encode("utf-8")
        attempt = 0
        last_error: Optional[Exception] = None
        
        while attempt < self.MAX_PUBLISH_RETRIES and not self._stop:
            attempt += 1
            try:
                # (Re)open connection/channel and RE-ENABLE confirms every time
                self._ensure_output_connection()  # must call confirm_delivery() inside
                
                published = self._output_channel.basic_publish(  # type: ignore[union-attr]
                    exchange=self.output_exchange,
                    routing_key=self.output_routing_key,
                    body=body,
                    mandatory=True,  # so unroutables surface via return callback
                    properties=pika.BasicProperties(
                        content_type="application/json",
                        delivery_mode=2,  # persistent
                    ),
                )
                
                # Service I/O/heartbeats so return/confirm frames are processed
                if self._output_connection and self._output_connection.is_open:
                    self._output_connection.sleep(0)
                    
                # Legacy confirm path (pika 1.2): True = ACK, False = NACK
                if not published:
                    raise NackError("Publish not confirmed by broker (legacy confirm path)")
                
                LOGGER.debug("Published parsed tracker event: %s", payload)
                return  # success

            except UnroutableError as exc:
                # Not retryable unless you change topology/routing key
                LOGGER.error("Unroutable publish exch=%r rk=%r: %s",
                             self.output_exchange, self.output_routing_key, exc)
                raise

            except (AMQPChannelError,
                    AMQPConnectionError,
                    StreamLostError,
                    ChannelClosedByBroker,
                    ConnectionClosed,
                    NackError,
                    OSError) as exc:
                last_error = exc
                LOGGER.warning("Publish attempt %d/%d failed: %s",
                               attempt, self.MAX_PUBLISH_RETRIES, exc)
                # Tear down and back off before retry
                self._reset_output_connection()
                backoff = min(2 ** (attempt - 1), 30)  # 1,2,4,8,16,30...
                time.sleep(backoff)

        # Exhausted attempts or asked to stop
        if last_error:
            raise last_error
        raise RuntimeError("Publish aborted.")

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)

        while not self._stop:
            try:
                self._ensure_connections()
                assert self._input_channel is not None

                for method, properties, body in self._input_channel.consume(queue=self.input_queue, inactivity_timeout=1.0):
                    if self._stop:
                        break
                    try:
                        self._pump_output_connection()
                    except Exception:
                        raise
                    if method is None:
                        continue

                    acknowledged = False
                    try:
                        event = json.loads(body)
                        if isinstance(event, dict) and self._publisher.process_event(event):
                            self._input_channel.basic_ack(method.delivery_tag)
                            acknowledged = True
                        else:
                            self._input_channel.basic_ack(method.delivery_tag)
                            acknowledged = True
                    except json.JSONDecodeError:
                        LOGGER.warning("Invalid JSON payload: %r", body)
                        self._input_channel.basic_ack(method.delivery_tag)
                        acknowledged = True
                    except Exception:
                        if not acknowledged:
                            try:
                                self._input_channel.basic_nack(method.delivery_tag, requeue=True)
                            except Exception:
                                pass
                        raise
            except (pika.exceptions.AMQPConnectionError, OSError) as exc:
                msg = str(exc)
                if "Connection reset by peer" in msg:
                    LOGGER.info("RabbitMQ connection reset by peer; retrying in 5s")
                else:
                    LOGGER.warning("AMQP connection failed: %s", exc)
                self._close_connections()
                time.sleep(5)
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.exception("Unhandled exception in daemon: %s", exc)
                self._close_connections()
                time.sleep(5)

        self._close_connections()


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish parsed tracker updates")
    parser.add_argument("--input-uri", required=True, help="AMQP URI to consume tracker events")
    parser.add_argument("--input-queue", default="tracker.events", help="Queue name with tracker events")
    parser.add_argument("--input-exchange", help="Optional exchange to bind the input queue to")
    parser.add_argument("--input-routing-key", help="Routing key for binding the input queue (defaults to queue name)")
    parser.add_argument("--output-uri", help="AMQP URI for publishing parsed events (defaults to input URI)")
    parser.add_argument("--output-exchange", default="", help="Exchange for parsed tracker events")
    parser.add_argument("--output-routing-key", default="tracker.parsed", help="Routing key for parsed tracker events")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    output_uri = args.output_uri or args.input_uri
    daemon = TrackerParserDaemon(
        input_uri=args.input_uri,
        input_queue=args.input_queue,
        output_uri=output_uri,
        output_exchange=args.output_exchange,
        output_routing_key=args.output_routing_key,
        input_exchange=args.input_exchange,
        input_routing_key=args.input_routing_key,
    )
    daemon.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
