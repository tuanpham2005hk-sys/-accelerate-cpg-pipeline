# Task 3 - Kafka Topic Design

## 1. Mục tiêu

Thiết kế layout Kafka mà Parser Service (Task 2) dùng để phát 4 loại sự kiện
CPG, và mà Neo4j Kafka Connector (Task 4) + Spark Structured Streaming (Task
5) sẽ tiêu thụ. Yêu cầu gốc của đề: đủ 4 topic riêng biệt (node, edge, source
metadata, parser error), mỗi message có `schema_version` và `event_time`.

## 2. Bốn topic

| Topic | Mục đích | Partitions | RF | retention.ms | Partition key |
|---|---|---|---|---|---|
| `cpg.node.events` | Sự kiện Node (function/class/module…) | 3 | 1 | 604800000 (7d) | `file_path` |
| `cpg.edge.events` | Sự kiện Edge (AST_CHILD/CFG/DFG/CALL) | 3 | 1 | 604800000 (7d) | `src_node_id` |
| `cpg.source.metadata.events` | Metadata cấp file nguồn | 3 | 1 | 1209600000 (14d) | `file_path` |
| `cpg.parser.error.events` | Lỗi khi parse (dead-letter) | 3 | 1 | 2592000000 (30d) | `file_path` |

**Field bắt buộc trong mọi message:** `schema_version` (SemVer, vd `1.0.0`) và
`event_time` (ISO-8601 UTC).

**Lý do chọn partition key:** gom các event liên quan tới cùng một file / cùng
một node nguồn vào cùng partition → giữ đúng thứ tự (ordering) khi consumer xử
lý, đồng thời phân tán tải đều giữa các partition.

**Cấu hình cho môi trường lab:** chỉ 1 broker nên `replication_factor = 1` cho
tất cả (kể cả `__consumer_offsets`); `cleanup.policy = delete`; retention tăng
dần theo mức độ cần điều tra khi có sự cố (node/edge 7 ngày → metadata 14
ngày → error 30 ngày, vì lỗi cần giữ lâu hơn để debug).

## 3. Ví dụ message thật của từng topic

**`cpg.node.events`:**
```json
{
  "schema_version": "1.0.0",
  "event_time": "2026-07-19T10:30:00Z",
  "event_type": "node_upsert",
  "node_id": "n:663f9dced11735709df3dc6ab7d1e760204baa6b26bc679505d9b581a63e7963",
  "node_type": "ClassDef",
  "name": "Accelerator",
  "file_path": "src/accelerate/accelerator.py",
  "scope": "<module>",
  "start_line": 120,
  "end_line": 450
}
```

**`cpg.source.metadata.events`:**
```json
{
  "schema_version": "1.0.0",
  "event_time": "2026-07-19T10:29:50Z",
  "event_type": "source_upsert",
  "file_path": "src/accelerate/accelerator.py",
  "repository": "huggingface/accelerate",
  "language": "python",
  "loc": 4359,
  "counts": {
    "nodes": 16139, "edges": 20448,
    "ast_child_edges": 15836, "cfg_edges": 1877,
    "dfg_edges": 1882, "call_edges": 853
  }
}
```

**`cpg.parser.error.events`** (dead-letter riêng cho lỗi *parse*, khác với
`cpg.neo4j.sink.dlq` ở Task 4 — xem phân biệt ở Chương 2):
```json
{
  "schema_version": "1.0.0",
  "event_time": "2026-07-19T10:30:10Z",
  "event_type": "parse_error",
  "repository": "huggingface/accelerate",
  "file_path": "examples/legacy/old_script.py",
  "error_type": "SyntaxError",
  "error_message": "invalid syntax (line 42)",
  "stage": "ast_parse",
  "severity": "error"
}
```

Schema đầy đủ (cả 4 topic): [`kafka/schemas/kafka_topics.json`](../kafka/schemas/kafka_topics.json).

## 4. Dựng & thao tác

```bash
docker compose up -d              # dựng Kafka + Zookeeper
bash scripts/create_topics.sh     # tạo 4 topic
bash scripts/verify_topics.sh     # verify: 4/4 PASS + describe
docker compose down -v            # tắt & xoá data khi xong
```

### Output verify thật

```
  [PASS] cpg.node.events
  [PASS] cpg.edge.events
  [PASS] cpg.source.metadata.events
  [PASS] cpg.parser.error.events
  KẾT QUẢ: 4/4 topic PASS

Topic: cpg.node.events            PartitionCount: 3  ReplicationFactor: 1  Configs: cleanup.policy=delete,retention.ms=604800000
Topic: cpg.edge.events            PartitionCount: 3  ReplicationFactor: 1  Configs: cleanup.policy=delete,retention.ms=604800000
Topic: cpg.source.metadata.events PartitionCount: 3  ReplicationFactor: 1  Configs: cleanup.policy=delete,retention.ms=1209600000
Topic: cpg.parser.error.events    PartitionCount: 3  ReplicationFactor: 1  Configs: cleanup.policy=delete,retention.ms=2592000000
>>> ALL TOPICS PASS <<<
```

## 5. Reflection

**Cái gì work:** tách riêng `cpg.parser.error.events` khỏi 2 topic chính
(node/edge) hoá ra rất hữu ích về sau ở Task 4 — khi Kafka Connect có DLQ
riêng của chính nó (`cpg.neo4j.sink.dlq`), nhóm không bị nhầm giữa "lỗi lúc
parse" và "lỗi lúc ghi vào Neo4j" vì hai luồng lỗi tách bạch ngay từ thiết kế
topic. Chọn `file_path`/`src_node_id` làm partition key cũng chứng minh đúng
khi debug Task 6: có thể lần theo đúng thứ tự event của 1 file cụ thể.

**Cái gì cần lưu ý:** vì môi trường lab chỉ có 1 broker Kafka,
`replication_factor = 1` không có failover — nếu broker chết, dữ liệu trong
topic mất hoàn toàn. Nhóm chấp nhận đánh đổi này vì đúng phạm vi bài lab
(single-broker), không phải môi trường production; nếu triển khai thật sẽ cần
tối thiểu RF = 3.
