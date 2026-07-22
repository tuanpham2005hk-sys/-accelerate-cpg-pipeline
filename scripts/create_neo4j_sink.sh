#!/usr/bin/env bash
# Phần 3 — Việc 4: Đăng ký Neo4j Kafka Connector Sink qua REST API của
# Kafka Connect worker. Chạy sau khi `docker compose up -d` và sau khi đã
# copy jar plugin vào kafka-connect/plugins/ (xem README trong đó).
set -euo pipefail

CONNECT_URL="http://localhost:8083"
CONFIG_FILE="$(dirname "$0")/../kafka-connect/neo4j-sink-connector.json"

echo "=== Chờ Kafka Connect REST API sẵn sàng ($CONNECT_URL) ==="
for _ in $(seq 1 30); do
  if curl -s -o /dev/null -w "%{http_code}" "$CONNECT_URL/connectors" | grep -q "200"; then
    echo "--> Connect REST API đã sẵn sàng."
    break
  fi
  sleep 3
done

echo "=== Kiểm tra plugin Neo4j đã được nhận chưa ==="
if ! curl -s "$CONNECT_URL/connector-plugins" | grep -q "org.neo4j.connectors.kafka.sink.Neo4jConnector"; then
  echo "[ERROR] Không thấy plugin Neo4j trong Kafka Connect."
  echo "        Kiểm tra lại: đã copy jar vào kafka-connect/plugins/ chưa?"
  echo "        Xem: kafka-connect/plugins/README.md"
  exit 1
fi
echo "--> Plugin org.neo4j.connectors.kafka.sink.Neo4jConnector đã sẵn sàng."

echo "=== Tạo constraint uniqueness trên Neo4j (idempotent, chạy IF NOT EXISTS) ==="
docker exec -i cpg-neo4j cypher-shell -u neo4j -p password \
  < "$(dirname "$0")/../kafka-connect/neo4j-constraints.cypher"

echo "=== Đăng ký (hoặc cập nhật) connector cpg-neo4j-sink ==="
NAME=$(python3 -c "import json;print(json.load(open('$CONFIG_FILE'))['name'])")
CONFIG_JSON=$(python3 -c "import json;print(json.dumps(json.load(open('$CONFIG_FILE'))['config']))")

curl -s -X PUT "$CONNECT_URL/connectors/$NAME/config" \
  -H "Content-Type: application/json" \
  -d "$CONFIG_JSON" | python3 -m json.tool

echo ""
echo "=== Trạng thái connector ==="
sleep 3
curl -s "$CONNECT_URL/connectors/$NAME/status" | python3 -m json.tool
