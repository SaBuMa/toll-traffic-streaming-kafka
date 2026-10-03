"""
Streaming toll data consumer (enhanced): Kafka -> transform -> MySQL.

Improvements over the course version:
  * Credentials read from environment variables (nothing secret in code)
  * Fixes the course bug where a failed DB connection crashed later with
    NameError: the script now exits cleanly with a clear message
  * Consumer group + manual offset commits AFTER the DB commit
    -> at-least-once delivery: a crash never silently loses rows
  * auto_offset_reset="earliest": messages produced before the consumer
    started are still loaded
  * Micro-batches with executemany() instead of one INSERT + COMMIT per row
  * Malformed messages are logged and skipped instead of crashing the loop
  * Clean shutdown on Ctrl+C
"""
import logging
import os
import sys

import mysql.connector
from kafka import KafkaConsumer

from transform import InvalidMessage, parse_message

logging.basicConfig(level=logging.INFO, format="%(asctime)s [consumer] %(message)s")
logging.getLogger("kafka").setLevel(logging.WARNING)  # hide kafka-python internals
log = logging.getLogger(__name__)

INSERT_SQL = (
    "INSERT INTO livetolldata (`timestamp`, vehicle_id, vehicle_type, toll_plaza_id) "
    "VALUES (%s, %s, %s, %s)"
)


def load_config():
    cfg = {
        "topic": os.getenv("KAFKA_TOPIC", "toll"),
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group_id": os.getenv("KAFKA_GROUP_ID", "toll-mysql-loader"),
        "db_host": os.getenv("MYSQL_HOST", "mysql"),
        "db_port": int(os.getenv("MYSQL_PORT", "3306")),
        "db_name": os.getenv("MYSQL_DATABASE", "tolldata"),
        "db_user": os.getenv("MYSQL_USER", "root"),
        "db_password": os.getenv("MYSQL_PASSWORD"),
    }
    if not cfg["db_password"]:
        log.error("MYSQL_PASSWORD is not set. See .env.example")
        sys.exit(1)
    return cfg


def connect_db(cfg):
    try:
        connection = mysql.connector.connect(
            host=cfg["db_host"], port=cfg["db_port"], database=cfg["db_name"],
            user=cfg["db_user"], password=cfg["db_password"],
        )
    except mysql.connector.Error as exc:
        log.error("Could not connect to MySQL: %s", exc)
        sys.exit(1)
    log.info("Connected to MySQL database '%s'", cfg["db_name"])
    return connection


def main():
    cfg = load_config()
    connection = connect_db(cfg)
    cursor = connection.cursor()

    consumer = KafkaConsumer(
        cfg["topic"],
        bootstrap_servers=cfg["bootstrap_servers"],
        group_id=cfg["group_id"],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    log.info("Reading topic '%s' as consumer group '%s'", cfg["topic"], cfg["group_id"])

    loaded = skipped = 0
    try:
        while True:
            # Wait up to 1 s, return whatever arrived (a micro-batch)
            records = consumer.poll(timeout_ms=1000, max_records=500)
            if not records:
                continue

            rows = []
            for messages in records.values():
                for msg in messages:
                    try:
                        rows.append(parse_message(msg.value))
                    except InvalidMessage as exc:
                        skipped += 1
                        log.warning("Skipped offset %d: %s", msg.offset, exc)

            if rows:
                cursor.executemany(INSERT_SQL, rows)
                connection.commit()          # 1) rows are safe in MySQL...
            consumer.commit()                # 2) ...then move the bookmark
            loaded += len(rows)
            log.info("Batch of %d rows loaded (total %d, skipped %d)",
                     len(rows), loaded, skipped)
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        consumer.close()
        cursor.close()
        connection.close()
        log.info("Shut down. Rows loaded: %d, messages skipped: %d", loaded, skipped)


if __name__ == "__main__":
    main()
