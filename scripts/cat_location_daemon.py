#!/usr/bin/env python3
"""Consume tracker events from RabbitMQ and publish location updates."""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET

import pika

LOGGER = logging.getLogger("cat_location_daemon")

# Regular expression to extract tracker coordinates from payloads.
import re
UD_PATTERN = re.compile(
    r"^\[SG\*(?P<device>[0-9]+)\*[0-9A-Fa-f]+\*UD[^,]*,"  # header
    r"(?P<date>[0-9]+),(?P<time>[0-9]+),(?P<status>[AV]),"    # date/time/status
    r"(?P<lat>[-0-9.]+),(?P<ns>[NS]),"                       # latitude + hemisphere
    r"(?P<lon>[-0-9.]+),(?P<ew>[EW]),"                       # longitude + hemisphere
    r".*\]$"
)


@dataclass
class Location:
    name: str
    longitude: float
    latitude: float


class LocationResolver:
    """Resolve the nearest named location based on KML coordinates."""

    def __init__(self, kml_path: Path) -> None:
        self.locations: List[Location] = self._load_locations(kml_path)
        if not self.locations:
            raise ValueError(f"No placemarks found in {kml_path}")

    @staticmethod
    def _load_locations(kml_path: Path) -> List[Location]:
        tree = ET.parse(kml_path)
        root = tree.getroot()
        ns = {'kml': 'http://www.opengis.net/kml/2.2'}
        locations: List[Location] = []
        for placemark in root.findall('.//kml:Placemark', ns):
            name_el = placemark.find('kml:name', ns)
            coord_el = placemark.find('kml:Point/kml:coordinates', ns)
            if name_el is None or coord_el is None:
                continue
            coords = coord_el.text.strip().split(',')
            if len(coords) < 2:
                continue
            lon = float(coords[0])
            lat = float(coords[1])
            locations.append(Location(name=name_el.text.strip(), longitude=lon, latitude=lat))
        return locations

    def resolve(self, latitude: float, longitude: float) -> str:
        best_name = self.locations[0].name
        best_distance = float('inf')
        for loc in self.locations:
            distance = ((loc.latitude - latitude) ** 2 + (loc.longitude - longitude) ** 2) ** 0.5
            if distance < best_distance:
                best_distance = distance
                best_name = loc.name
        return best_name


def parse_tracker_payload(payload: str) -> Optional[Tuple[float, float]]:
    match = UD_PATTERN.match(payload)
    if not match:
        return None
    lat = float(match.group('lat'))
    if match.group('ns') == 'S':
        lat = -lat
    lon = float(match.group('lon'))
    if match.group('ew') == 'W':
        lon = -lon
    return lat, lon


class LocationPublisher:
    """Helper that publishes JSON events when the location changes."""

    def __init__(self, resolver: LocationResolver, publish_func: Callable[[Dict[str, str]], None], initial_position: Optional[str] = None) -> None:
        self.resolver = resolver
        self.publish_func = publish_func
        self._last_position: Optional[str] = initial_position

    def process_event(self, event: Dict[str, str]) -> None:
        payload = event.get('payload')
        if not payload:
            return
        coords = parse_tracker_payload(payload)
        if not coords:
            return
        latitude, longitude = coords
        location = self.resolver.resolve(latitude, longitude)
        if location == self._last_position:
            return
        self._last_position = location
        timestamp = event.get('timestamp') or time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
        self.publish_func({'timestamp': timestamp, 'position': location})

    @property
    def last_position(self) -> Optional[str]:
        return self._last_position


class CatLocationDaemon:
    def __init__(
        self,
        input_uri: str,
        input_queue: str,
        output_uri: str,
        output_exchange: str,
        output_routing_key: str,
        kml_path: Path,
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
        self.resolver = LocationResolver(kml_path)
        self._stop = False
        self._connection: Optional[pika.BlockingConnection] = None
        self._output_connection: Optional[pika.BlockingConnection] = None
        self._channel = None
        self._output_channel = None
        self._publisher: Optional[LocationPublisher] = None
        self._last_position: Optional[str] = None

    def stop(self, *_: object) -> None:
        LOGGER.info("Stopping daemon")
        self._stop = True
        if self._channel and self._channel.is_open:
            self._channel.stop_consuming()

    def _ensure_connections(self) -> None:
        if not self._connection or self._connection.is_closed:
            LOGGER.info("Connecting to RabbitMQ (input)")
            params = pika.URLParameters(self.input_uri)
            self._connection = pika.BlockingConnection(params)
            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=self.input_queue, durable=False, auto_delete=True)
            if self.input_exchange:
                self._channel.exchange_declare(exchange=self.input_exchange, exchange_type='topic', durable=True, auto_delete=False)
                self._channel.queue_bind(queue=self.input_queue, exchange=self.input_exchange, routing_key=self.input_routing_key)
        if not self._output_connection or self._output_connection.is_closed:
            LOGGER.info("Connecting to RabbitMQ (output)")
            out_params = pika.URLParameters(self.output_uri)
            self._output_connection = pika.BlockingConnection(out_params)
            self._output_channel = self._output_connection.channel()
            if self.output_exchange:
                self._output_channel.exchange_declare(exchange=self.output_exchange, exchange_type='topic', durable=True)
            else:
                self._output_channel.queue_declare(queue=self.output_routing_key, durable=False, auto_delete=False)
        if not self._publisher:
            def publisher(payload: Dict[str, str]) -> None:
                body = json.dumps(payload).encode('utf-8')
                self._output_channel.basic_publish(  # type: ignore[union-attr]
                    exchange=self.output_exchange,
                    routing_key=self.output_routing_key,
                    body=body,
                )
                LOGGER.info("Published new location: %s", payload)
            self._publisher = LocationPublisher(self.resolver, publisher, self._last_position)

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)
        while not self._stop:
            try:
                self._ensure_connections()
                for method, properties, body in self._channel.consume(queue=self.input_queue, inactivity_timeout=1.0):
                    if self._stop:
                        break
                    if method is None:
                        continue
                    try:
                        event = json.loads(body)
                        if isinstance(event, dict):
                            self._publisher.process_event(event)  # type: ignore
                            self._last_position = self._publisher.last_position
                    except json.JSONDecodeError:
                        LOGGER.warning("Invalid JSON payload: %r", body)
                    finally:
                        self._channel.basic_ack(method.delivery_tag)
            except (pika.exceptions.AMQPConnectionError, OSError) as exc:
                LOGGER.warning("AMQP connection failed: %s", exc)
                time.sleep(5)
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.exception("Unhandled exception in daemon: %s", exc)
                time.sleep(5)
            finally:
                if self._connection and self._connection.is_open:
                    self._connection.close()
                if self._output_connection and self._output_connection.is_open:
                    self._output_connection.close()
                self._connection = None
                self._output_connection = None
                self._channel = None
                self._publisher = None


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish cat location updates based on tracker events")
    parser.add_argument("--input-uri", required=True, help="AMQP URI to consume tracker events")
    parser.add_argument("--input-queue", default="tcpproxy.integration", help="Queue name with tracker events")
    parser.add_argument("--input-exchange", help="Optional exchange to bind the input queue to")
    parser.add_argument("--input-routing-key", help="Routing key for binding the input queue (defaults to queue name)")
    parser.add_argument("--output-uri", help="AMQP URI for publishing location updates (defaults to input URI)")
    parser.add_argument("--output-exchange", default="", help="Exchange for location updates")
    parser.add_argument("--output-routing-key", default="cat.location", help="Routing key for location updates")
    parser.add_argument("--kml", default="/usr/local/etc/Locations.kml", help="Path to KML file with named locations")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    output_uri = args.output_uri or args.input_uri
    daemon = CatLocationDaemon(
        input_uri=args.input_uri,
        input_queue=args.input_queue,
        output_uri=output_uri,
        output_exchange=args.output_exchange,
        output_routing_key=args.output_routing_key,
        kml_path=Path(args.kml),
        input_exchange=args.input_exchange,
        input_routing_key=args.input_routing_key,
    )
    daemon.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
