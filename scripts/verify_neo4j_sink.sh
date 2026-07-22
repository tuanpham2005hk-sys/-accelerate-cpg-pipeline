#!/usr/bin/env bash
# Phần 3 — Việc 4: Verify connector RUNNING (không FAILED) + in số liệu
# node/relationship hiện có trong Neo4j để đối chiếu với output Parser
# Service (Việc 2).
set -euo pipefail

CONNECT_URL="http://localhost:8083"
NAME="cpg-neo4j-sink"

echo "=== 1. Trạng thái connector + task ==="
STATUS_JSON=$(curl -s "$CONNECT_URL/connectors/$NAME/status")
echo "$STATUS_JSON" | python3 -m json.tool

CONNECTOR_STATE=$(echo "$STATUS_JSON" | python3 -c "import json,sys;print(json.load(sys.stdin)['connector']['state'])")
TASK_STATES=$(echo "$STATUS_JSON" | python3 -c "import json,sys;d=json.load(sys.stdin);print(','.join(t['state'] for t in d['tasks']))")

if [[ "$CONNECTOR_STATE" != "RUNNING" || "$TASK_STATES" == *"FAILED"* ]]; then
  echo "[FAIL] Connector hoặc task đang không RUNNING. Xem log:"
  echo "       docker compose logs -f kafka-connect"
  exit 1
fi
echo "[PASS] Connector + toàn bộ task đều RUNNING."

echo ""
echo "=== 2. Số liệu trong Neo4j (CPGNode / CPG_EDGE) ==="
docker exec -i cpg-neo4j cypher-shell -u neo4j -p password <<'CYPHER'
MATCH (n:CPGNode) RETURN count(n) AS total_nodes;
MATCH ()-[r:CPG_EDGE]->() RETURN count(r) AS total_edges;
MATCH ()-[r:CPG_EDGE]->() RETURN r.edge_type AS edge_type, count(*) AS cnt ORDER BY edge_type;
CYPHER

echo ""
echo "=== 3. Dead-letter queue (message lỗi, nếu có) ==="
docker exec cpg-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic cpg.neo4j.sink.dlq \
  --from-beginning --max-messages 5 --timeout-ms 3000 2>/dev/null \
  || echo "--> DLQ trống hoặc chưa có message lỗi (bình thường)."
