"""Rate-controlled generator of random Nexmark events, written to Kafka as JSON.

Usage: python generator.py [--rate N] [--count N] [--topic T] [--bootstrap HOST:PORT]
Every flag can also be set by env var (RATE, COUNT, TOPIC, BOOTSTRAP).
"""

import argparse
import itertools
import os
import random
import time
from datetime import UTC, datetime, timedelta

from confluent_kafka import Producer

from schema import Auction, Bid, Event, EventType, Person

# Nexmark's default mix: in every 50 events, 1 person, 3 auctions and 46 bids.
MIX = [EventType.PERSON] + [EventType.AUCTION] * 3 + [EventType.BID] * 46

NAMES = ["Peter", "Paul", "Luke", "John", "Saul", "Vicky", "Kate", "Julie", "Sarah", "Deiter", "Walter"]
SURNAMES = ["Shultz", "Abrams", "Spencer", "White", "Bartels", "Walton", "Smith", "Jones", "Noris"]
CITIES = ["Phoenix", "Los Angeles", "San Francisco", "Boise", "Portland", "Bend", "Redmond", "Seattle", "Kent", "Cheyenne"]
STATES = ["AZ", "CA", "ID", "OR", "WA", "WY"]
CHANNELS = ["Apple", "Google", "Facebook", "Baidu"]
WORDS = ["vintage", "lamp", "chair", "brass", "oak", "rare", "mint", "boxed", "signed", "classic"]


def random_id():
    return random.randint(1000, 9999)


def random_event(kind, now):
    if kind == EventType.PERSON:
        name = f"{random.choice(NAMES)} {random.choice(SURNAMES)}"
        return Event(event_type=kind, person=Person(
            id=random_id(),
            name=name,
            emailAddress=f"{name.replace(' ', '.').lower()}@example.com",
            creditCard=" ".join(f"{random.randint(0, 9999):04d}" for _ in range(4)),
            city=random.choice(CITIES),
            state=random.choice(STATES),
            dateTime=now,
            extra="",
        ))
    if kind == EventType.AUCTION:
        initial_bid = random.randint(1, 1000)
        return Event(event_type=kind, auction=Auction(
            id=random_id(),
            itemName=" ".join(random.sample(WORDS, 2)),
            description=" ".join(random.sample(WORDS, 5)),
            initialBid=initial_bid,
            reserve=initial_bid + random.randint(1, 1000),
            dateTime=now,
            expires=now + timedelta(seconds=random.randint(1, 60)),
            seller=random_id(),
            category=random.randint(10, 14),
            extra="",
        ))
    return Event(event_type=kind, bid=Bid(
        auction=random_id(),
        bidder=random_id(),
        price=random.randint(1, 10_000),
        channel=random.choice(CHANNELS),
        url=f"https://www.nexmark.com/{random.choice(WORDS)}/item.htm?query=1",
        dateTime=now,
        extra="",
    ))


def generate():
    """Endless stream of random events in Nexmark's mix, timestamped when taken."""
    for kind in itertools.cycle(MIX):
        yield random_event(kind, datetime.now(UTC).replace(tzinfo=None))


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rate", type=int, default=int(os.environ.get("RATE", 1000)), help="events/s, 0 = unlimited")
    parser.add_argument("--count", type=int, default=int(os.environ.get("COUNT", 0)), help="stop after N events, 0 = never")
    parser.add_argument("--topic", default=os.environ.get("TOPIC", "nexmark"))
    parser.add_argument("--bootstrap", default=os.environ.get("BOOTSTRAP", "kafka:9092"))
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 5})
    print(f"writing to {args.topic} at {args.rate or 'unlimited'} events/s", flush=True)

    events = generate()
    start = last_report = time.perf_counter()
    sent = 0
    while not args.count or sent < args.count:
        if args.rate:
            delay = start + sent / args.rate - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
        message = next(events).model_dump_json()
        while True:
            try:
                producer.produce(args.topic, message)
                break
            except BufferError:  # local queue full: let it drain
                producer.poll(0.1)
        producer.poll(0)
        sent += 1

        now = time.perf_counter()
        if now - last_report >= 5:
            print(f"{sent} events, {sent / (now - start):.0f} events/s", flush=True)
            last_report = now

    producer.flush()
    print(f"done: {sent} events in {time.perf_counter() - start:.1f}s", flush=True)


if __name__ == "__main__":
    main()
