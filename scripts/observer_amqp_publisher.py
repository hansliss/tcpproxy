#!/usr/bin/env python3
"""Helper process that forwards observer events to RabbitMQ using pika."""

import argparse
import json
import sys
import urllib.parse

try:
    import pika
except ImportError as exc:  # pragma: no cover
    sys.stderr.write(f"pika module is required for AMQP observer: {exc}\n")
    sys.exit(2)


SPECIAL_KEYS = {"exchange", "routing_key", "queue"}


def sanitize_uri(uri: str):
    parsed = urllib.parse.urlparse(uri)
    query = urllib.parse.parse_qs(parsed.query)
    clean_pairs = [(k, v) for k, vs in query.items() for v in vs if k not in SPECIAL_KEYS]
    clean_query = urllib.parse.urlencode(clean_pairs, doseq=True)
    sanitized = parsed._replace(query=clean_query)
    return urllib.parse.urlunparse(sanitized), query


def main():
    parser = argparse.ArgumentParser(description="Publish observer events to RabbitMQ")
    parser.add_argument("--config", required=True, help="AMQP URI with optional query params")
    args = parser.parse_args()

    clean_uri, query = sanitize_uri(args.config)
    exchange = query.get("exchange", [""])[0]
    routing_key = query.get("routing_key", [""])[0]
    queue = query.get("queue", [""])[0]

    params = pika.URLParameters(clean_uri)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    if queue:
        channel.queue_declare(queue=queue, durable=False, auto_delete=True)
        if exchange:
            channel.exchange_declare(exchange=exchange, exchange_type="topic", durable=False)
            channel.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key or queue)
    elif exchange:
        channel.exchange_declare(exchange=exchange, exchange_type="topic", durable=False)

    publish_exchange = exchange
    publish_routing_key = routing_key or queue
    if not publish_exchange and queue:
        publish_routing_key = queue

    if not publish_routing_key:
        publish_routing_key = queue or "observer.event"

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        payload = json.loads(line)
        body = json.dumps(payload).encode("utf-8")
        channel.basic_publish(exchange=publish_exchange,
                              routing_key=publish_routing_key,
                              body=body)

    connection.close()


if __name__ == "__main__":
    main()
