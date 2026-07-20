# Accelerate CPG Streaming Pipeline - Lab 04

## Phần 1 - File Discovery & Kafka Topic Design

Đồ án Big Data (Spark Streaming) · Repo nguồn: **huggingface/accelerate** · Môi trường: **lab (single-broker Kafka)**

> Phạm vi Phần 1 = **Việc 1** (Repository Cloning & File Discovery) + **Việc 3** (Kafka Topic Design).
> Không bao gồm: Parser Service, Neo4j, Spark+MongoDB, Idempotent verification, Architecture diagram (phần của thành viên khác).

---

## 0. Môi trường thực thi (đã verify)

| Thành phần | Phiên bản |
|---|---|
| Docker | 29.4.0 |
| Docker Compose | v5.1.2 |
| git | 2.47.0.windows.2 |
| Python | 3.12.10 |
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

---

## Phần 2 - Incremental CPG Parser Service

Parser dùng thư viện chuẩn Python `ast`, xử lý từng file một và phát bốn nhóm
dữ liệu: AST nodes/child edges, CFG edges, DFG edges và Call edges. Mỗi phần tử
có ID SHA-256 ổn định; state riêng theo file cho phép replay/upsert và phát
`node_delete`/`edge_delete` khi source thay đổi.

### Cài đặt

```bash
python -m pip install -r requirements.txt
```

### Chạy không cần Kafka

```bash
# Một file, lưu toàn bộ event thành JSONL
python -m parser_service \
  --repo accelerate \
  --file src/accelerate/accelerator.py \
  --dry-run \
  --output-jsonl output/parser_events_smoke.jsonl

# Toàn bộ 142 file, kiểm tra parser nhưng không giữ payload lớn
python -m parser_service \
  --repo accelerate \
  --manifest output/file_discovery.json \
  --discard-events

# Replay incremental: bỏ qua mọi file không đổi
python -m parser_service \
  --repo accelerate \
  --manifest output/file_discovery.json \
  --discard-events \
  --skip-unchanged
```

### Chạy với Kafka thật

```bash
docker compose up -d
bash scripts/create_topics.sh

python -m parser_service \
  --repo accelerate \
  --manifest output/file_discovery.json \
  --bootstrap-servers localhost:9092
```

Trên Windows PowerShell, dùng `scripts/create_topics.ps1` và
`scripts/verify_topics.ps1` thay cho hai script `.sh`.

Nếu cần thực hiện Task 6, sửa một file rồi chỉ parse lại file đó:

```bash
python -m parser_service \
  --repo accelerate \
  --file src/accelerate/accelerator.py \
  --bootstrap-servers localhost:9092
```

### Kiểm thử

```bash
python -m pytest -q
```

Kết quả đã nghiệm thu cục bộ:

- `12 passed`.
- `142/142` file parse thành công.
- `193087` node, `240673` edge.
- Edge: `187538 AST_CHILD`, `20540 CFG`, `21210 DFG`, `11385 CALL`.
- Replay với `--skip-unchanged`: `142/142` file được skip.
- File lớn `src/accelerate/accelerator.py`: peak Python allocation khoảng
  `31.17 MiB`, thể hiện bộ nhớ bị giới hạn theo một file thay vì cả repo.

Chi tiết phục vụ báo cáo: [`docs/task2_parser_service.md`](docs/task2_parser_service.md).
