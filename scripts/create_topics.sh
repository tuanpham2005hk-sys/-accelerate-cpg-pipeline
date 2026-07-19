#!/usr/bin/env bash
# Phần 1 — Việc 3: Tạo 4 topic THẬT trên Kafka (chạy kafka-topics trong container)
# Dùng: bash scripts/create_topics.sh
set -euo pipefail

CONTAINER="cpg-kafka"
BOOTSTRAP="localhost:9092"

# topic|partitions|replication|retention.ms
TOPICS=(
  "cpg.node.events|3|1|604800000"
  "cpg.edge.events|3|1|604800000"
  "cpg.source.metadata.events|3|1|1209600000"
  "cpg.parser.error.events|3|1|2592000000"
)

echo "=== Tạo topic trên Kafka (broker=$BOOTSTRAP) ==="
for entry in "${TOPICS[@]}"; do
  IFS='|' read -r name parts rf retention <<< "$entry"
  echo "--> $name (partitions=$parts, rf=$rf, retention.ms=$retention)"
  docker exec "$CONTAINER" kafka-topics \
    --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$parts" \
    --replication-factor "$rf" \
    --config "retention.ms=$retention" \
    --config "cleanup.policy=delete"
done

echo "=== Xong. Danh sách topic hiện có: ==="
docker exec "$CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP" --list
