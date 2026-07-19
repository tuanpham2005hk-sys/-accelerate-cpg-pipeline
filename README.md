# Phần 1 — File Discovery & Kafka Topic Design

Đồ án Big Data (Spark Streaming) · Repo nguồn: **huggingface/accelerate** · Môi trường: **lab (single-broker Kafka)**

> Phạm vi Phần 1 = **Việc 1** (Repository Cloning & File Discovery) + **Việc 3** (Kafka Topic Design).
> Không bao gồm: Parser Service, Neo4j, Spark+MongoDB, Idempotent verification, Architecture diagram (phần của thành viên khác).

---

## 0. Môi trường thực thi (đã verify)

| Thành phần | Phiên bản |
|---|---|
| Docker | 29.3.1 |
| Docker Compose | v5.1.1 |
| git | 2.54.0 |
| Python | 3.13.0 |
| Kafka / Zookeeper image | confluentinc/cp-kafka:7.6.1 · cp-zookeeper:7.6.1 |

Kafka được dựng **thật** bằng Docker, 4 topic được tạo **thật** và verify bằng `kafka-topics --list` (không mock).

---

## Việc 1 — Repository Cloning & File Discovery

**Clone (shallow):**
```bash
git clone --depth 1 https://github.com/huggingface/accelerate.git
```

**Chạy discovery:**
```bash
python scripts/discover_files.py --repo accelerate --out output/file_discovery.json
```

### Tiêu chí loại bỏ file

| Loại | Tiêu chí |
|---|---|
| **test** | Nằm trong thư mục `tests/` HOẶC tên khớp `test_*.py` / `*_test.py` / `conftest.py` |
| **setup** | File packaging/build: `setup.py` |
| **auto-generated** | Tên `_version.py` / `version.py` / `*_pb2.py`; HOẶC 40 dòng header đầu chứa marker như `DO NOT EDIT`, `@generated`, `auto-generated` |

### Kết quả

| Chỉ số | Giá trị |
|---|---|
| **Tổng số file `.py` tìm được** | **197** |
| Giữ lại (source thật) | **142** |
| Loại bỏ | **55** |
| — test | 54 |
| — setup | 1 |
| — auto-generated | 0 |

> Repo `accelerate` không có file auto-generated theo tiêu chí trên → count = 0 (đúng như kỳ vọng).

Kết quả đầy đủ (danh sách file kept/excluded + lý do): [`output/file_discovery.json`](output/file_discovery.json).

---

## Việc 3 — Kafka Topic Design

### 4 topic

| Topic | Mục đích | Partitions | RF | retention.ms | Partition key |
|---|---|---|---|---|---|
| `cpg.node.events` | Sự kiện Node (function/class/module…) | 3 | 1 | 604800000 (7d) | `file_path` |
| `cpg.edge.events` | Sự kiện Edge (calls/imports/inherits…) | 3 | 1 | 604800000 (7d) | `src_node_id` |
| `cpg.source.metadata.events` | Metadata cấp file nguồn | 3 | 1 | 1209600000 (14d) | `file_path` |
| `cpg.parser.error.events` | Lỗi khi parse (dead-letter) | 3 | 1 | 2592000000 (30d) | `file_path` |

**Field bắt buộc trong mọi message:** `schema_version` (SemVer, vd `1.0.0`) và `event_time` (ISO-8601 UTC).

**Lý do chọn partition key:** gom các event liên quan tới cùng một file / cùng một node nguồn vào cùng partition → **giữ đúng thứ tự (ordering)** khi consumer xử lý, đồng thời phân tán tải đều giữa các partition.

**Cấu hình cho môi trường lab:** chỉ 1 broker nên `replication_factor = 1` cho tất cả (kể cả `__consumer_offsets`); `cleanup.policy = delete`; retention tăng dần theo mức độ cần điều tra (node/edge 7d → metadata 14d → error 30d).

Schema chi tiết + ví dụ message: [`kafka/schemas/kafka_topics.json`](kafka/schemas/kafka_topics.json).

### Dựng & thao tác

```bash
docker compose up -d              # dựng Kafka + Zookeeper
bash scripts/create_topics.sh     # tạo 4 topic
bash scripts/verify_topics.sh     # verify: 4/4 PASS + describe
docker compose down -v            # tắt & xoá data khi xong
```

### Output verify (thật)

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

---

## Cấu trúc thư mục

```
accelerate-cpg-lab4/
├── accelerate/                     # repo đã clone (--depth 1)
├── docker-compose.yml              # Kafka + Zookeeper (lab)
├── scripts/
│   ├── discover_files.py           # liệt kê + đếm .py -> JSON
│   ├── create_topics.sh            # tạo 4 topic
│   └── verify_topics.sh            # verify 4/4 PASS
├── kafka/schemas/kafka_topics.json # schema 4 topic
├── output/file_discovery.json      # kết quả Việc 1
└── README.md                       # (file này)
```

## Tóm tắt nghiệm thu

- ✅ Tổng số file `.py`: **197** (giữ 142 / loại 55) — lưu JSON.
- ✅ 4 topic thiết kế đầy đủ, mỗi message có `schema_version` + `event_time`, partition key hợp lý.
- ✅ Kafka **thật** dựng bằng Docker → tạo topic thật → verify **4/4 PASS**.
