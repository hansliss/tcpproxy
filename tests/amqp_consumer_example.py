#!/usr/bin/env python3
"""Simple example consumer for proxy observer events."""

import argparse
import json
import sys
import urllib.parse

import pika


def parse_args():
    parser = argparse.ArgumentParser(description="Consume observer events from RabbitMQ")
    parser.add_argument("uri", help="AMQP URI with queue or routing parameters")
    parser.add_argument("--count", type=int, default=1, help="Number of messages to consume before exiting")
    return parser.parse_args()

SPECIAL_KEYS = {"exchange", "routing_key", "queue"}


def sanitize_uri(uri: str):
    parsed = urllib.parse.urlparse(uri)
    query = urllib.parse.parse_qs(parsed.query)
    clean_pairs = [(k, v) for k, vs in query.items() for v in vs if k not in SPECIAL_KEYS]
    clean_query = urllib.parse.urlencode(clean_pairs, doseq=True)
    sanitized = parsed._replace(query=clean_query)
    return urllib.parse.urlunparse(sanitized), query


def main():
    args = parse_args()
    clean_uri, query = sanitize_uri(args.uri)
    params = pika.URLParameters(clean_uri)
    exchange = query.get("exchange", [None])[0]
    routing_key = query.get("routing_key", [None])[0]
    queue = query.get("queue", [""])[0]

    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    if queue:
        channel.queue_declare(queue=queue, durable=False, auto_delete=False)
    else:
        result = channel.queue_declare(queue="", exclusive=True, auto_delete=True)
        queue = result.method.queue
        print(f"[*] Using temporary queue '{queue}'")

    if exchange:
        channel.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)
        channel.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key or "#")

    consumed = 0
    for method, properties, body in channel.consume(queue=queue, inactivity_timeout=9999):
        if method is None:
            break
        channel.basic_ack(method.delivery_tag)
        consumed += 1
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = body.decode("utf-8", errors="replace")
        print(json.dumps(payload, indent=2))
        if consumed >= args.count:
            break

    channel.cancel()
    connection.close()


if __name__ == "__main__":
    main()
