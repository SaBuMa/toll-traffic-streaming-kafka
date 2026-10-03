"""
Transform step of the streaming pipeline.

Kept in its own module (no Kafka or MySQL imports) so it can be
unit-tested without a running broker or database.

Input message format (produced by producer.py):
    "Fri Oct  2 19:26:20 2026,9820660,truck,4004"
Output row (matches the livetolldata table):
    ("2026-10-02 19:26:20", 9820660, "truck", 4004)
"""
from datetime import datetime

SOURCE_TIME_FORMAT = "%a %b %d %H:%M:%S %Y"   # what time.ctime() produces
TARGET_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"      # MySQL DATETIME
VALID_VEHICLE_TYPES = {"car", "truck", "van"}


class InvalidMessage(ValueError):
    """Raised when a Kafka message cannot be turned into a valid row."""


def parse_message(raw):
    """Decode, validate and transform one Kafka message into a DB row."""
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise InvalidMessage("message is not valid UTF-8") from exc

    parts = [p.strip() for p in raw.strip().split(",")]
    if len(parts) != 4:
        raise InvalidMessage(f"expected 4 fields, got {len(parts)}: {raw!r}")

    timestamp, vehicle_id, vehicle_type, plaza_id = parts

    try:
        # A space in the format matches one or more spaces, so ctime's
        # double space before single-digit days ("Oct  2") is handled.
        timestamp = datetime.strptime(timestamp, SOURCE_TIME_FORMAT)
    except ValueError as exc:
        raise InvalidMessage(f"bad timestamp: {timestamp!r}") from exc

    if vehicle_type not in VALID_VEHICLE_TYPES:
        raise InvalidMessage(f"unknown vehicle type: {vehicle_type!r}")

    try:
        vehicle_id = int(vehicle_id)
        plaza_id = int(plaza_id)
    except ValueError as exc:
        raise InvalidMessage(f"non-numeric id in: {raw!r}") from exc

    return (timestamp.strftime(TARGET_TIME_FORMAT), vehicle_id, vehicle_type, plaza_id)
