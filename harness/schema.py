"""Nexmark event schema, mirroring nexmark/.../queries/ddl_kafka.sql.

Field names match the DDL exactly, so `Event.model_dump_json()` produces the
JSON that Flink's Kafka table expects.
"""

from datetime import datetime
from enum import IntEnum
from typing import Annotated

from pydantic import BaseModel, PlainSerializer

# TIMESTAMP(3) in Flink's JSON format: "yyyy-MM-dd HH:mm:ss.SSS"
Timestamp = Annotated[
    datetime,
    PlainSerializer(lambda t: t.strftime("%Y-%m-%d %H:%M:%S.") + f"{t.microsecond // 1000:03d}"),
]


class EventType(IntEnum):
    PERSON = 0
    AUCTION = 1
    BID = 2


class Person(BaseModel):
    id: int
    name: str
    emailAddress: str
    creditCard: str
    city: str
    state: str
    dateTime: Timestamp
    extra: str


class Auction(BaseModel):
    id: int
    itemName: str
    description: str
    initialBid: int
    reserve: int
    dateTime: Timestamp
    expires: Timestamp
    seller: int
    category: int
    extra: str


class Bid(BaseModel):
    auction: int
    bidder: int
    price: int
    channel: str
    url: str
    dateTime: Timestamp
    extra: str


class Event(BaseModel):
    """One Kafka message: only the field matching `event_type` is set."""

    event_type: EventType
    person: Person | None = None
    auction: Auction | None = None
    bid: Bid | None = None
