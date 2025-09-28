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


def main():
    args = parse_args()
    params = pika.URLParameters(args.uri)
    parsed = urllib.parse.urlparse(args.uri)
    query = urllib.parse.parse_qs(parsed.query)
    queue = query.get("queue", [""])[0] or "observer.events"

    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=False, auto_delete=True)

    consumed = 0
    for method, properties, body in channel.consume(queue=queue, inactivity_timeout=5):
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
