# Architecture Diagram

## 1. Sơ đồ tổng thể

```mermaid
flowchart LR
    SRC["huggingface/accelerate\n142 file .py"] --> PARSER["Parser Service\n(Python, ast module)"]

    subgraph KAFKA_CLUSTER["Apache Kafka 7.6.1 (cp-kafka), 1 broker, 3 partitions/topic"]
        NODE[["cpg.node.events"]]
        EDGE[["cpg.edge.events"]]
        META[["cpg.source.metadata.events"]]
        ERR[["cpg.parser.error.events"]]
    end

    PARSER --> NODE
    PARSER --> EDGE
    PARSER --> META
    PARSER --> ERR

    subgraph SINK["Kafka Connect 7.6.1 (cp-kafka-connect)"]
        NEO4JC["Neo4j Kafka Connector Sink 5.5.0\n(APOC MERGE, idempotent upsert/delete)"]
    end

    NODE --> NEO4JC
    EDGE --> NEO4JC
    NEO4JC --> NEO4J[("Neo4j 5.15.0\nCPGNode / CPG_EDGE")]
    NEO4JC -.lỗi ghi.-> DLQ[["cpg.neo4j.sink.dlq"]]

    META --> SPARKJOB["Spark Structured Streaming\napache/spark:3.5.1-python3\n+ spark-sql-kafka-0-10_2.12:3.5.1"]
    SPARKJOB <--> CKPT[("Checkpoint location\n(local volume, offsets + commits)")]
    SPARKJOB --> MONGOC["MongoDB Spark Connector\nmongo-spark-connector_2.12:10.4.1\n(replace theo _id = file_path)"]
    MONGOC --> MONGO[("MongoDB 8.0\ncpg_database.source_metadata")]

    ERR --> ERRLOG["Error log / monitoring\n(dead-letter cho lỗi parse)"]
```

### 📸 Ảnh render sẵn (dùng khi môi trường build không bật extension Mermaid)

![Kien truc pipeline CPG](../images/task7/architecture-diagram.png)

**Điểm mấu chốt của kiến trúc** (bám sát yêu cầu đề "ingest graph topology vào Neo4j
*không qua Spark*"): nhánh `cpg.node.events`/`cpg.edge.events` → Neo4j đi thẳng qua Kafka
Connect, hoàn toàn tách biệt khỏi nhánh `cpg.source.metadata.events` → Spark → MongoDB.
Hai nhánh chạy song song, độc lập, không phụ thuộc lẫn nhau.

Hệ thống có **2 đường lỗi tách biệt**, dễ nhầm nên ghi rõ:
- `cpg.parser.error.events` — lỗi phát sinh ngay tại **Parser Service** khi parse 1 file
  (ví dụ syntax error), được phát trước khi có dữ liệu gửi tới Neo4j/MongoDB.
- `cpg.neo4j.sink.dlq` — lỗi phát sinh **sau đó**, ngay tại Kafka Connect khi ghi vào Neo4j
  thất bại (ví dụ constraint vi phạm, mất kết nối) — cơ chế dead-letter-queue riêng của
  Kafka Connect, không liên quan tới lỗi parse.

## 2. Bảng công cụ và version cụ thể

| Thành phần | Image / package | Version |
|---|---|---|
| Message broker | `confluentinc/cp-kafka` | 7.6.1 |
| Coordination | `confluentinc/cp-zookeeper` | 7.6.1 |
| Kafka Connect worker | `confluentinc/cp-kafka-connect` | 7.6.1 |
| Neo4j Sink plugin | Neo4j Kafka Connector | 5.5.0 |
| Graph database | `neo4j` | 5.15.0 |
| Document database | `mongo` | 8.0 |
| Compute engine | `apache/spark` (python3) | 3.5.1 |
| Spark ↔ Kafka connector | `spark-sql-kafka-0-10_2.12` | 3.5.1 |
| Spark ↔ MongoDB connector | `mongo-spark-connector_2.12` | 10.4.1 |
| Parser | Python `ast` (standard library) | Python 3.x |

## 3. Luồng dữ liệu tóm tắt

1. Parser Service đọc từng file `.py`, xây CPG (AST/CFG/DFG/Call), so sánh với
   `parser_state` của lần chạy trước để tính đúng phần cần thêm/xoá (idempotent).
2. Phát 4 loại event lên 4 topic Kafka riêng biệt, mỗi message có `schema_version` và
   `event_time`.
3. Nhánh đồ thị: Kafka Connect (Neo4j Sink) tiêu thụ `cpg.node.events`/`cpg.edge.events`,
   dùng Cypher `MERGE` theo `node_id`/`edge_id` ổn định (SHA-256) để upsert/delete đúng,
   không qua Spark.
4. Nhánh metadata: Spark Structured Streaming tiêu thụ `cpg.source.metadata.events`, ghi
   vào MongoDB bằng chế độ `replace` theo `_id = file_path`, dùng checkpoint để resume
   đúng offset khi job bị restart (đã kiểm chứng thực nghiệm ở Việc 6).
