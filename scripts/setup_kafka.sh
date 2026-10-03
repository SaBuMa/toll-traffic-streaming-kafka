#!/usr/bin/env bash
# ------------------------------------------------------------
# Download Kafka 3.7.0, format KRaft storage and start the broker.
# Run from the repository root. Leave this terminal open:
# the broker runs in the foreground.
# ------------------------------------------------------------
set -euo pipefail

KAFKA_VERSION="3.7.0"
SCALA_VERSION="2.12"
KAFKA_DIR="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
ARCHIVE="${KAFKA_DIR}.tgz"
URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${ARCHIVE}"

if [ ! -d "$KAFKA_DIR" ]; then
    echo "Downloading Kafka ${KAFKA_VERSION}..."
    wget -q "$URL"
    tar -xzf "$ARCHIVE"
fi

cd "$KAFKA_DIR"

# KRaft mode: no ZooKeeper. The cluster ID identifies this cluster.
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
echo "Cluster ID: ${KAFKA_CLUSTER_ID}"

# --ignore-formatted makes the script safe to run twice
bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" \
    -c config/kraft/server.properties --ignore-formatted

echo "Starting Kafka broker on localhost:9092 ..."
exec bin/kafka-server-start.sh config/kraft/server.properties
