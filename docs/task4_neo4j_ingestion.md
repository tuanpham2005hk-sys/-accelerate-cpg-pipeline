# Task 4 - Graph Topology Ingestion vào Neo4j (Neo4j Kafka Connector Sink)

## 1. Mục tiêu của Task

Task 4 thuộc **Phần 3 – Việc 4** của đồ án (2/10 điểm). Yêu cầu gốc của đề:

> Wire the Neo4j Kafka Connector Sink to the topics carrying node and edge
> events so that the graph topology is written into Neo4j directly from
> Kafka **without an intermediate Spark layer**. The ingestion logic must be
> **idempotent** so that reprocessing the same node or edge does not create
> duplicates.

Ba ràng buộc bắt buộc: (1) dùng đúng **Neo4j Connector for Kafka** (Kafka
Connect plugin chính thức của Neo4j, không tự viết consumer Python), (2)
không đi qua Spark, (3) idempotent.

Luồng dữ liệu:

```text
Kafka: cpg.node.events, cpg.edge.events
              │
              ▼
   Kafka Connect worker (service "kafka-connect")
   plugin: org.neo4j.connectors.kafka.sink.Neo4jConnector
              │
              │  Cypher Strategy — MERGE theo node_id / edge_id
              ▼
            Neo4j
   (:CPGNode)-[:CPG_EDGE]->(:CPGNode)
```

Khác với nhánh MongoDB (Kafka → Spark Structured Streaming → MongoDB), nhánh
này đi **thẳng** từ Kafka Connect vào Neo4j qua Bolt protocol, không có Spark
ở giữa — đúng yêu cầu đề.

---

## 2. Vì sao dùng "Neo4j Connector for Kafka" chứ không tự viết consumer?

Đề bài chỉ định rõ **"Neo4j Kafka Connector Sink"** — đây là tên một sản phẩm
cụ thể của Neo4j (không phải "một cách nào đó đẩy dữ liệu vào Neo4j"). Sản
phẩm này chạy như một **plugin trong Kafka Connect** (một framework riêng để
chạy connector, tách biệt khỏi cả Kafka broker lẫn Neo4j), cấu hình hoàn toàn
bằng JSON + Cypher template, không cần viết code Python/Java nào.

Lưu ý: bản mới nhất (5.1+) đã đổi `connector.class` từ
`streams.kafka.connect.sink.Neo4jSinkConnector` (bản cũ, nhiều tutorial trên
mạng vẫn ghi vậy) sang `org.neo4j.connectors.kafka.sink.Neo4jConnector`, kèm
đổi tên một số config (`neo4j.uri` thay vì `neo4j.server.uri`,
`neo4j.cypher.topic.<topic>` thay vì `neo4j.topic.cypher.<topic>`). File cấu
hình trong repo này (`kafka-connect/neo4j-sink-connector.json`) viết theo
đúng bản mới nhất.

---

## 3. Thiết kế Cypher — vì sao idempotent

### 3.1. Vấn đề: label/relationship-type phải tĩnh, nhưng `node_type`/`edge_type` của mình lại động

Cypher không cho `MERGE (n:$dynamicLabel)` trực tiếp. Có 2 lựa chọn:
`apoc.merge.node`/`apoc.merge.relationship` (label động qua APOC), hoặc dùng
**1 label/type cố định + lưu type thật dưới dạng property**. Nhóm chọn cách
thứ hai (`:CPGNode {node_type: ...}`, `:CPG_EDGE {edge_type: ...}`) vì đơn
giản, dễ verify, và vẫn truy vấn được theo loại qua `WHERE n.node_type = ...`.

### 3.2. Vấn đề: 1 topic mang CẢ 2 loại event (`_upsert` và `_delete`)

Nhìn lại `parser_service/service.py` (Việc 2): `node_upsert`/`node_delete`
cùng đi vào `cpg.node.events`; `edge_upsert`/`edge_delete` cùng đi vào
`cpg.edge.events`. Cypher Strategy chỉ có **1 template cố định mỗi topic**,
nên phải tự rẽ nhánh theo `event.event_type` ngay trong Cypher. Nhóm dùng
`apoc.do.when(condition, ifQuery, elseQuery, params)` (APOC Core, có sẵn khi
bật `NEO4J_PLUGINS=["apoc"]`) — đúng theo tài liệu chính thức là procedure
được thiết kế cho việc này.

```cypher
-- topic cpg.node.events
WITH __value AS event
CALL apoc.do.when(
  event.event_type = 'node_upsert',
  'MERGE (n:CPGNode {node_id: $event.node_id})
   SET n.node_type = $event.node_type, n.name = $event.name,
       n.file_path = $event.file_path, n.scope = $event.scope,
       n.start_line = $event.start_line, n.end_line = $event.end_line,
       n.start_col = $event.start_col, n.end_col = $event.end_col,
       n.updated_at = $event.event_time
   RETURN count(n) AS affected',
  'OPTIONAL MATCH (n:CPGNode {node_id: $event.node_id})
   DETACH DELETE n
   RETURN count(n) AS affected',
  {event: event}
) YIELD value RETURN value
```

```cypher
-- topic cpg.edge.events
WITH __value AS event
CALL apoc.do.when(
  event.event_type = 'edge_upsert',
  'MERGE (src:CPGNode {node_id: $event.src_node_id})
   MERGE (dst:CPGNode {node_id: $event.dst_node_id})
   MERGE (src)-[r:CPG_EDGE {edge_id: $event.edge_id}]->(dst)
   SET r.edge_type = $event.edge_type, r.file_path = $event.file_path,
       r.updated_at = $event.event_time
   RETURN count(r) AS affected',
  'OPTIONAL MATCH ()-[r:CPG_EDGE {edge_id: $event.edge_id}]-()
   DELETE r
   RETURN count(r) AS affected',
  {event: event}
) YIELD value RETURN value
```

### 3.3. Vì sao đây là idempotent

- `node_id`/`edge_id` là **SHA-256 ổn định** do Parser Service tính từ nội
  dung + vị trí (không dùng timestamp — xem `parser_service/identifiers.py`).
  Reprocess cùng 1 file luôn ra đúng cùng `node_id`/`edge_id`.
- Mọi nhánh upsert dùng **`MERGE` theo đúng ID đó**, không dùng `CREATE`. Gửi
  lại cùng message N lần → chỉ 1 node/relationship tồn tại, các lần sau chỉ
  `SET` đè lên property cũ (kể cả nếu property có thay đổi nhỏ, ví dụ
  `end_line` do sửa file).
- `neo4j-constraints.cypher` tạo **uniqueness constraint thật ở tầng DB**
  trên `node_id` và `edge_id` — không chỉ "đúng nhờ" logic Cypher mà còn được
  Neo4j enforce, đồng thời tăng tốc `MERGE` (index-backed thay vì full scan).

### 3.4. Xử lý out-of-order giữa 2 topic (edge case đáng chú ý)

`cpg.node.events` và `cpg.edge.events` là 2 topic **độc lập**, Kafka Connect
không đảm bảo thứ tự xử lý message giữa 2 topic với nhau. Có thể một edge
event tới trước node event của node đầu/cuối cạnh đó (đặc biệt với
`ExternalSymbol` hoặc cạnh trỏ sang node ở batch khác). Nhánh `edge_upsert`
dùng `MERGE (src:CPGNode {node_id: ...})` — nếu node chưa tồn tại, Neo4j sẽ
tạo một "node rỗng" tạm thời (chỉ có `node_id`), và khi `node_upsert` event
thật đến sau, nó sẽ `MERGE` trúng đúng node đó rồi `SET` đầy đủ property.
Không có race condition nào tạo ra node trùng.

Tương tự, nếu `node_delete` đến trước `edge_delete` của cùng cạnh (ngược thứ
tự publish của Parser Service), `DETACH DELETE n` ở nhánh xoá node đã tự dọn
sạch mọi relationship dính vào node đó — không có relationship "mồ côi" còn
sót lại dù thứ tự xử lý thế nào.

---

## 4. Cài đặt & chạy

### 4.1. Tải plugin (làm 1 lần)

Xem `kafka-connect/plugins/README.md` — tải file `.jar` từ
https://github.com/neo4j/neo4j-kafka-connector/releases, copy vào
`kafka-connect/plugins/`.

### 4.2. Dựng hạ tầng

```bash
docker compose up -d
docker compose ps    # neo4j, kafka-connect phải "healthy"
```

### 4.3. Đăng ký connector

```bash
bash scripts/create_neo4j_sink.sh
# Windows: powershell -File scripts/create_neo4j_sink.ps1
```

Script này tự: chờ Connect REST API sẵn sàng → kiểm tra plugin đã nhận →
chạy constraint trên Neo4j → `PUT` config connector → in trạng thái.

### 4.4. Bơm dữ liệu thật (Parser Service — Việc 2)

```bash
python -m parser_service --repo accelerate \
  --manifest output/file_discovery.json \
  --bootstrap-servers localhost:9092
```

### 4.5. Verify

```bash
bash scripts/verify_neo4j_sink.sh
```

---

## 5. Hướng dẫn test chi tiết

### Test 1 — Connector đang chạy đúng, không lỗi

```bash
curl -s http://localhost:8083/connectors/cpg-neo4j-sink/status | python3 -m json.tool
```
Kỳ vọng: `"connector": {"state": "RUNNING"}` và mọi phần tử trong `"tasks"`
đều `"state": "RUNNING"` (không có `FAILED`). Nếu FAILED, xem
`docker compose logs -f kafka-connect` — lỗi thường gặp: sai mật khẩu Neo4j,
plugin chưa được mount đúng, hoặc `apoc.do.when` bị chặn do thiếu
`NEO4J_dbms_security_procedures_unrestricted=apoc.*`.

### Test 2 — Đối chiếu số lượng node/edge với Parser Service

Parser Service in ra console tổng số node/edge đã publish (`--discard-events`
hoặc log mặc định). Mở Neo4j Browser (`http://localhost:7474`) hoặc chạy:

```bash
docker exec -i cpg-neo4j cypher-shell -u neo4j -p password \
  "MATCH (n:CPGNode) RETURN count(n);
   MATCH ()-[r:CPG_EDGE]->() RETURN count(r);
   MATCH ()-[r:CPG_EDGE]->() RETURN r.edge_type, count(*) ORDER BY r.edge_type;"
```
Kỳ vọng: tổng số node/edge trong Neo4j **khớp** (hoặc rất sát — chênh lệch có
thể do độ trễ consume) với số liệu Parser Service đã publish, và breakdown
theo `edge_type` (AST_CHILD/CFG/DFG/CALL) khớp với `counts` trong metadata
event (đối chiếu chéo với Task 5 — cùng 1 nguồn dữ liệu).

### Test 3 — Idempotent: publish lại KHÔNG tạo trùng (yêu cầu cốt lõi của đề)

```bash
# Đếm trước
docker exec -i cpg-neo4j cypher-shell -u neo4j -p password \
  "MATCH (n:CPGNode) RETURN count(n) AS before"

# Publish lại đúng 1 file, KHÔNG sửa nội dung
python -m parser_service --repo accelerate \
  --file src/accelerate/accelerator.py \
  --bootstrap-servers localhost:9092

sleep 5   # chờ connector xử lý xong batch

# Đếm sau
docker exec -i cpg-neo4j cypher-shell -u neo4j -p password \
  "MATCH (n:CPGNode) RETURN count(n) AS after"
```
Kỳ vọng: `after == before` (không tăng), vì mọi node/edge của file đó có
đúng cùng `node_id`/`edge_id` như lần trước → `MERGE` chỉ update, không tạo
mới. Đây chính là bằng chứng "idempotent" đề yêu cầu.

### Test 4 — Sửa file → graph cập nhật đúng, không để lại rác (tiền đề Task 6)

```bash
# giả sử sửa/xoá bớt vài dòng trong accelerator.py rồi lưu lại
python -m parser_service --repo accelerate \
  --file src/accelerate/accelerator.py \
  --bootstrap-servers localhost:9092
```
Kiểm tra:
```bash
docker exec -i cpg-neo4j cypher-shell -u neo4j -p password \
  "MATCH (n:CPGNode {file_path: 'src/accelerate/accelerator.py'})
   RETURN n.updated_at ORDER BY n.updated_at DESC LIMIT 3;"
```
`updated_at` phải là thời điểm vừa chạy lại. Vì Parser Service (Việc 2) tự
tính diff và phát `node_delete`/`edge_delete` cho phần tử không còn tồn tại
sau khi sửa (xem `service.py::_publish_result`), số node/edge của file đó
trong Neo4j phải giảm đúng bằng số phần tử đã bị xoá khỏi source — không có
node/edge "rác" của phiên bản code cũ còn sót lại.

### Test 5 — Dead-letter queue bắt lỗi đúng cách (không âm thầm mất dữ liệu)

```bash
docker exec cpg-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic cpg.neo4j.sink.dlq --from-beginning --max-messages 5 --timeout-ms 3000
```
Bình thường phải **trống** (timeout không có message). Nếu có message ở đây
nghĩa là có event nào đó connector không ghi được vào Neo4j (ví dụ Cypher
lỗi) — cần xem log để sửa, không được bỏ qua.

---

## 6. Reflection

Khó nhất là việc 1 topic mang 2 loại event (`_upsert`/`_delete`) trong khi
Cypher Strategy chỉ cho 1 template tĩnh mỗi topic — giải quyết bằng
`apoc.do.when` để rẽ nhánh ngay trong Cypher thay vì tách thêm topic (tách
topic sẽ đúng hơn về mặt thiết kế nhưng phá vỡ layout 4-topic đã chốt ở Việc
3). Điểm cần lưu ý khi trình bày: dùng `MERGE` theo ID ổn định (không phải
theo nội dung/thời gian) là yếu tố quyết định tính idempotent, constraint ở
tầng DB chỉ là lớp bảo vệ + tối ưu hiệu năng thêm, không phải cơ chế chính.
