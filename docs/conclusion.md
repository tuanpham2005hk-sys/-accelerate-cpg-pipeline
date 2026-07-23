# Task 8 - Kết luận & Reflection tổng thể

## 1. Tóm tắt những gì đã làm được

Nhóm đã hoàn thành đầy đủ pipeline streaming CPG cho repo
`huggingface/accelerate` theo đúng yêu cầu đề bài:

| # | Hạng mục | Điểm | Trạng thái |
|---|---|---:|---|
| 1 | Repository Cloning & File Discovery | 1 | ✅ 197 file `.py` tìm được, giữ lại 142 |
| 2 | Incremental CPG Parser Service | 1.5 | ✅ 142/142 file parse thành công, bounded memory theo từng file |
| 3 | Kafka Topic Design | 1.5 | ✅ 4/4 topic tạo & verify PASS |
| 4 | Graph Topology Ingestion vào Neo4j | 2 | ✅ Neo4j Kafka Connector Sink, không qua Spark, idempotent bằng `MERGE` |
| 5 | Source Metadata Ingestion vào MongoDB | 2 | ✅ Spark Structured Streaming + checkpoint, ghi `replace` theo `_id` |
| 6 | Idempotent Replay Verification | 1 | ✅ Kiểm chứng cả 3 vế (Neo4j / MongoDB / Spark checkpoint) bằng dữ liệu thật |
| — | Architecture Diagram | 1 | ✅ Sơ đồ + bảng version đầy đủ (Chương 2) |
| **Tổng** | | **10** | |

Số liệu cuối cùng của toàn bộ đồ thị CPG (142 file, sau khi test Task 6 sửa
`memory.py`): **193,415 node / 241,082 edge** trong Neo4j, **142 document**
trong MongoDB `cpg_database.source_metadata`.

## 2. Reflection tổng hợp theo từng phần

**Parser Service (Task 2)** là điểm nghẽn quan trọng nhất của cả pipeline —
mọi phần sau đều phụ thuộc vào nó. Chọn thư viện chuẩn `ast` (thay vì
tree-sitter/Joern) giúp không cần cài đặt công cụ ngoài, đủ để minh hoạ 4
loại thành phần CPG (AST/CFG/DFG/Call) trong phạm vi lab, dù CFG/DFG chỉ dừng
ở mức intraprocedural — giới hạn này được nêu rõ thay vì tuyên bố phân tích
semantic hoàn chỉnh.

**Nhánh Neo4j (Task 4)** khó nhất ở chỗ 1 topic Kafka phải mang cả 2 loại sự
kiện (`_upsert`/`_delete`) trong khi Cypher Strategy của Kafka Connect chỉ
cho 1 template tĩnh mỗi topic — giải quyết bằng `apoc.do.when` để rẽ nhánh
ngay trong Cypher, thay vì phá vỡ layout 4-topic đã chốt ở Task 3.

**Nhánh MongoDB (Task 5)** đơn giản hơn về mặt logic (dùng `_id = file_path`
+ ghi `replace`), nhưng phụ thuộc chặt vào thứ tự khởi động service — Spark
job cần Kafka & dữ liệu đã sẵn sàng mới chạy ổn định.

**Idempotent Replay Verification (Task 6)** là nơi phát hiện ra bài học quan
trọng nhất của cả nhóm: **không nên chỉ tin vào "số liệu cuối cùng đúng"**,
vì cơ chế ghi đè (upsert/replace) có thể che giấu việc phần "resume" phía sau
không hoạt động đúng thiết kế. Khi đối chiếu offset Spark checkpoint, nhóm
phát hiện checkpoint đang dùng không phải checkpoint nguyên bản của lần
full-run đầu tiên (nhiều khả năng mất do một lần container restart trong quá
trình dựng hạ tầng) — nên chủ động làm thêm bài test dừng/khởi động lại tiến
trình thật để có bằng chứng độc lập, thay vì dừng lại ở suy luận gián tiếp.
Tương tự, trong lúc chuẩn bị Task 6 cũng phát hiện connector `cpg-neo4j-sink`
từng rơi vào trạng thái `FAILED` sau một lần restart hạ tầng (do Kafka Connect
worker khởi động trước khi Neo4j sẵn sàng hẳn) — khắc phục bằng cách gọi lại
API restart của Kafka Connect, và từ đó rút ra thói quen luôn kiểm tra trạng
thái `RUNNING` của connector trước khi tin vào số liệu Neo4j.

## 3. Bài học chung của cả nhóm

- **Xác minh bằng dữ liệu thật, không suy luận từ log gián tiếp hay lời kể**
  — thói quen này lặp lại xuyên suốt từ Task 4 đến Task 6 (đối chiếu số liệu
  Neo4j bằng công thức "tổng cũ − xoá + thêm", đối chiếu offset Kafka bằng
  cách đọc trực tiếp file `offsets/N`, restart thật tiến trình Spark thay vì
  chỉ tin log) và là điểm nhóm tự tin nhất khi trình bày báo cáo.
- **Thiết kế ID ổn định (SHA-256, không phụ thuộc thời gian) là nền tảng của
  toàn bộ tính idempotent** — mọi cơ chế `MERGE`/`replace` phía sau chỉ hoạt
  động đúng nhờ Parser Service tính đúng ID ngay từ đầu.
- **Pipeline tuần tự có điểm nghẽn rõ ràng** (Parser Service) — lịch làm việc
  6 ngày phải ưu tiên thời gian cho đúng phần này, các phần phụ thuộc sau nó
  (Neo4j, Spark+MongoDB) dựng khung sẵn bằng dữ liệu giả trong lúc chờ, để khi
  có dữ liệu thật thì chỉ cần cắm vào chạy.

## 4. Hạn chế còn tồn tại

- CFG/DFG chỉ phân tích ở mức intraprocedural (trong phạm vi 1 hàm/method),
  chưa mô phỏng aliasing hay dynamic attribute — đúng phạm vi lab nhưng chưa
  phải phân tích ngữ nghĩa đầy đủ như các công cụ CPG chuyên dụng (Joern).
- Môi trường lab chỉ dùng 1 Kafka broker (`replication_factor = 1`), không có
  failover — chấp nhận được cho mục đích demo nhưng không phản ánh cấu hình
  production thật.
- README hiện chưa có mục riêng tổng hợp cho Task 4 (nội dung đầy đủ nằm ở
  `docs/task4_neo4j_ingestion.md`, được dùng trực tiếp làm Chương 5 của sách
  này).

## 5. Lời kết

Toàn bộ pipeline — từ Parser Service, qua 4 topic Kafka, tới 2 nhánh ingest
độc lập (Neo4j Kafka Connector và Spark Structured Streaming + MongoDB
Connector) — đã chạy được end-to-end trên dữ liệu thật của repo
`huggingface/accelerate`, và tính idempotent đã được kiểm chứng bằng thực
nghiệm ở cả 3 điểm (Neo4j, MongoDB, Spark checkpoint) chứ không chỉ dựa trên
lý thuyết thiết kế.
