#!/usr/bin/env bash
# ------------------------------------------------------------
# Create the 'toll' topic (idempotent) and show its layout.
# Usage:  ./scripts/create_topic.sh            (1 partition, as in the lab)
#         PARTITIONS=3 ./scripts/create_topic.sh
# ------------------------------------------------------------
set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-kafka_2.12-3.7.0}"
TOPIC="${TOPIC:-toll}"
PARTITIONS="${PARTITIONS:-1}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"

"$KAFKA_HOME/bin/kafka-topics.sh" --create --if-not-exists \
    --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 1 \
    --bootstrap-server "$BOOTSTRAP"

"$KAFKA_HOME/bin/kafka-topics.sh" --describe --topic "$TOPIC" \
    --bootstrap-server "$BOOTSTRAP"
