"""Replay measurements in cycle order as JSON or send them to Kafka."""

import argparse
from datetime import datetime, timezone
from itertools import islice
import json
from pathlib import Path
import time

from telemetry import read_fleet


def sensor_events(fleet):
    for cycle_index in range(max(map(len, fleet.values()))):
        for engine, history in fleet.items():
            if cycle_index < len(history):
                measurement = history[cycle_index]
                yield {
                    "engine_id": engine,
                    "cycle": cycle_index + 1,
                    "settings": measurement[1:4].tolist(),
                    "sensors": measurement[4:].tolist(),
                }


def replay(args):
    fleet = read_fleet(args.input)
    producer = None
    if args.broker:
        from kafka import KafkaProducer

        producer = KafkaProducer(bootstrap_servers=args.broker, acks="all")

    count = 0
    try:
        for event in islice(sensor_events(fleet), args.limit):
            if count and args.rate:
                time.sleep(1 / args.rate)
            event["timestamp"] = datetime.now(timezone.utc).isoformat()
            payload = json.dumps(event, allow_nan=False)
            if producer is None:
                print(payload)
            else:
                key = str(event["engine_id"]).encode()
                producer.send(args.topic, key=key, value=payload.encode()).get(timeout=10)
            count += 1
    finally:
        if producer is not None:
            producer.close()
    print(f"{count} events {'acknowledged by Kafka' if args.broker else 'printed locally'}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("data/test_FD001.txt"))
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--rate", type=float, default=0, help="events per second, zero for no delay")
    parser.add_argument("--broker", help="Kafka host:port. Omit to print the events.")
    parser.add_argument("--topic", default="smartpredict.telemetry")
    arguments = parser.parse_args()
    if arguments.limit < 0 or arguments.rate < 0:
        parser.error("limit and rate must be nonnegative")
    replay(arguments)
