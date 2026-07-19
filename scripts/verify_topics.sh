#!/usr/bin/env bash
# Phần 1 — Việc 3: VERIFY 4 topic bằng kafka-topics --list (không mock)
# Exit code 0 = 4/4 PASS, 1 = thiếu topic. Dùng: bash scripts/verify_topics.sh
set -uo pipefail

CONTAINER="cpg-kafka"
BOOTSTRAP="localhost:9092"

EXPECTED=(
  "cpg.node.events"
  "cpg.edge.events"
  "cpg.source.metadata.events"
  "cpg.parser.error.events"
)

echo "=== VERIFY: kafka-topics --list ==="
ACTUAL="$(docker exec "$CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP" --list)"
echo "$ACTUAL"
echo "-----------------------------------"

pass=0
for t in "${EXPECTED[@]}"; do
  if echo "$ACTUAL" | grep -qx "$t"; then
    echo "  [PASS] $t"
    pass=$((pass+1))
  else
    echo "  [FAIL] $t  (không tìm thấy)"
  fi
done

echo "-----------------------------------"
echo "  KẾT QUẢ: $pass/${#EXPECTED[@]} topic PASS"

echo ""
echo "=== Chi tiết describe từng topic ==="
for t in "${EXPECTED[@]}"; do
  docker exec "$CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$t" 2>/dev/null | head -1 || true
done

if [ "$pass" -eq "${#EXPECTED[@]}" ]; then
  echo ""
  echo ">>> ALL TOPICS PASS <<<"
  exit 0
else
  echo ""
  echo ">>> VERIFY FAILED <<<"
  exit 1
fi
