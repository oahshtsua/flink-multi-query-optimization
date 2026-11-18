#!/usr/bin/env python3
"""Usage: python produce.py <file> <topic> [bootstrap_servers]"""
import sys
from kafka import KafkaProducer  # pip install kafka-python

path, topic = sys.argv[1], sys.argv[2]
servers = sys.argv[3] if len(sys.argv) > 3 else "localhost:9092"

producer = KafkaProducer(bootstrap_servers=servers, linger_ms=50, batch_size=64 * 1024)

with open(path, "rb") as f:
    for i, line in enumerate(f, 1):
        line = line.rstrip(b"\r\n")
        if line:
            producer.send(topic, value=line)
        if i % 100_000 == 0:
            print(f"sent {i} lines")

producer.flush()
print(f"done, sent {i} lines to '{topic}'")
