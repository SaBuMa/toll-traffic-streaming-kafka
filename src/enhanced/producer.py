"""
Toll traffic simulator (enhanced Kafka producer).

Improvements over the course version:
  * Command-line options (topic, broker, number of events, delay)
  * Plaza id used as the message key -> all events of one plaza land in
    the same partition, so their order is preserved per plaza
  * acks="all" + retries for safer delivery
  * Clean shutdown on Ctrl+C: buffered messages are flushed, not lost
  * Messages encoded to bytes before sending (works on kafka-python 2.x
    and 3.x, where plain-function serializers raise a DeprecationWarning)
"""
import argparse
import logging
from random import choice, randint, random
from time import ctime, sleep, time

from kafka import KafkaProducer

# Weighted list: ~65% cars, ~24% trucks, ~12% vans (same mix as the course)
VEHICLE_TYPES = ("car",) * 11 + ("truck",) * 4 + ("van",) * 2
PLAZA_IDS = range(4000, 4011)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [producer] %(message)s")
logging.getLogger("kafka").setLevel(logging.WARNING)  # hide kafka-python internals
log = logging.getLogger(__name__)


def build_event():
    """Simulate one vehicle passing a toll plaza. Returns (key, message)."""
    vehicle_id = randint(10000, 10000000)
    vehicle_type = choice(VEHICLE_TYPES)
    plaza_id = choice(PLAZA_IDS)
    now = ctime(time())
    return str(plaza_id), f"{now},{vehicle_id},{vehicle_type},{plaza_id}"


def parse_args():
    parser = argparse.ArgumentParser(description="Stream simulated toll traffic to Kafka")
    parser.add_argument("--topic", default="toll")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--count", type=int, default=100000, help="events to send")
    parser.add_argument("--max-delay", type=float, default=2.0,
                        help="max seconds between events (random 0..max)")
    return parser.parse_args()


def main():
    args = parse_args()
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        acks="all",
        retries=5,
    )
    log.info("Streaming %d events to topic '%s' on %s",
             args.count, args.topic, args.bootstrap_servers)

    sent = 0
    try:
        for _ in range(args.count):
            key, message = build_event()
            producer.send(args.topic, key=key.encode("utf-8"), value=message.encode("utf-8"))
            sent += 1
            log.info("sent #%d -> %s", sent, message)
            sleep(random() * args.max_delay)
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        producer.flush()   # deliver anything still in the buffer
        producer.close()
        log.info("Producer closed. Events sent: %d", sent)


if __name__ == "__main__":
    main()
