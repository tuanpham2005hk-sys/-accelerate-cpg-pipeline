# Lab 04 – Spark Streaming: Giới Thiệu & Phân Công Công Việc

**Môn:** CSC14118 – Nhập môn Dữ liệu lớn
**Repo đã chọn:** [huggingface/accelerate](https://github.com/huggingface/accelerate)
**Nộp bài:** 1 URL Jupyter Book (GitHub Pages) — không nộp zip/PDF/Word

---

## 1. Đồ án này thực chất là làm gì?

Nói đơn giản: nhóm sẽ xây một **hệ thống đọc source code Python của repo `accelerate`, "hiểu" cấu trúc logic bên trong từng file code, rồi lưu kết quả đó vào 2 loại database khác nhau** — tất cả chạy theo kiểu **streaming** (xử lý từng file một, real-time) chứ không phải chạy 1 lần rồi xong.

### 1.1. "Hiểu cấu trúc code" nghĩa là gì? → Code Property Graph (CPG)

Khi máy đọc 1 file `.py`, nó không chỉ đọc chữ như người mà có thể phân tích ra **4 loại thông tin** tạo thành 1 "đồ thị mô tả code" gọi là **CPG**:

- **AST node (Abstract Syntax Tree)**: cây cú pháp — mỗi hàm, biến, câu lệnh, class... là 1 "node".
- **CFG edge (Control Flow Graph)**: đường đi luồng điều khiển — code chạy theo thứ tự nào, nhánh `if/else`, vòng lặp nào dẫn tới đâu.
- **DFG edge (Data Flow Graph)**: dữ liệu (biến) được tạo ra ở đâu, dùng lại ở đâu.
- **Call edge**: hàm này gọi hàm kia ở đâu trong code.

Gộp 4 loại lại → ta có 1 "bản đồ" đầy đủ về cách 1 file code hoạt động, thay vì chỉ là văn bản thuần.

### 1.2. Luồng xử lý tổng thể (pipeline)

```
Repo accelerate (.py files)
        │
        ▼
  Parser Service  ──► đọc từng file MỘT (không đọc cả repo 1 lượt)
        │              trích ra AST / CFG / DFG / Call edges
        ▼
     Apache Kafka  ──► giống một "trạm trung chuyển" nhận các "sự kiện"
   (4 topic riêng:      (node, edge, metadata, error) rồi phát đi cho
    node/edge/           bên nào cần nhận
    metadata/error)
        │
        ├──────────────────────────────┐
        ▼                              ▼
  Neo4j Kafka Connector       Spark Structured Streaming
  (nhận thẳng node+edge        (đọc metadata từ Kafka)
   từ Kafka, KHÔNG qua Spark)          │
        │                              ▼
        ▼                        MongoDB Spark Connector
      Neo4j                             │
  (lưu đồ thị CPG)                      ▼
                                     MongoDB
                              (lưu metadata của từng file)
```

**Vì sao có 2 nhánh khác nhau?** Đây chính là điểm bài lab muốn kiểm tra — 2 cách "ingest" (đưa dữ liệu vào database) khác nhau:
- Nhánh Neo4j: dùng **Kafka Connector Sink** — 1 cấu hình có sẵn tự động đẩy dữ liệu từ Kafka thẳng vào Neo4j, không cần code Spark ở giữa.
- Nhánh MongoDB: dùng **Spark Structured Streaming** — phải tự viết job Spark đọc Kafka rồi ghi vào MongoDB, có **checkpoint** để nếu job bị crash/restart thì biết chỗ đã xử lý tới đâu, không xử lý lại từ đầu.

### 1.3. "Idempotent" là gì và vì sao quan trọng?

Idempotent = **xử lý lại nhiều lần vẫn ra kết quả giống như xử lý 1 lần**, không bị nhân đôi dữ liệu.

Ví dụ: nếu 1 file `.py` được parse lại (vì bị sửa), hệ thống phải **cập nhật** node/edge cũ, chứ không được **tạo thêm bản sao mới** bên cạnh bản cũ. Đây là lý do Parser Service phải gán **ID ổn định** cho từng node/edge, và bên Neo4j/MongoDB phải dùng cơ chế "cập nhật nếu đã tồn tại" (upsert) thay vì "luôn tạo mới".

### 1.4. Kết quả cuối cùng nhóm cần có

- Toàn bộ code (Parser Service, Kafka config, Neo4j sink config, Spark job) trên 1 repo GitHub.
- 1 trang **Jupyter Book** (dạng sách online) trình bày lại từng bước đã làm, kèm ảnh chụp thật (kết quả chạy, số liệu, giao diện database) — không phải chỉ code suông.
- Chỉ nộp **1 đường link** duy nhất trỏ tới trang Jupyter Book đó.

### 1.5. Vài lưu ý bắt buộc

- Chỉ làm trên **1 repo duy nhất** (`accelerate`) — làm nhiều repo không được cộng điểm thêm.
- Nên **học kỹ lý thuyết về pipeline** (Kafka, Neo4j Connector, Spark Structured Streaming) trước khi bắt tay cào/parse thật.
- Commit code rải rác theo tiến độ thật, không dồn 1 lần cuối kỳ.

---

## 2. Công cụ / kiến thức cần nắm trước khi code

| Mảng | Công cụ gợi ý |
|---|---|
| Parse code Python | module `ast` (built-in, nhẹ) hoặc `tree-sitter` / Joern |
| Message broker | Apache Kafka (producer/consumer bằng `kafka-python` hoặc `confluent-kafka`) |
| Graph database | Neo4j + Neo4j Kafka Connector Sink |
| Streaming job | Apache Spark Structured Streaming |
| Document database | MongoDB + MongoDB Spark Connector |
| Hạ tầng chạy chung | Docker Compose (dựng Kafka, Neo4j, MongoDB, Spark cùng lúc) |
| Báo cáo | Jupyter Book (publish qua GitHub Pages) |

---

## 3. Các mảng công việc (nhóm tự chia người phụ trách)

> Thang điểm bám sát đúng bảng chấm điểm của đề bài (tổng 10đ).

### 3.0. Lưu ý quan trọng: đây là pipeline tuần tự, không phải 4 phần độc lập

Vì dữ liệu chảy nối tiếp qua từng bước (Parser → Kafka → Neo4j/MongoDB), nên **không thể** kiểu "4 người mỗi người ôm 1 phần làm từ đầu tới cuối, không ai đụng ai". Có phần bắt buộc phải chờ phần trước xong mới có dữ liệu để chạy/test.

**Chia thành 4 phần (không tính Việc 0 — vì Việc 0 cả nhóm làm chung trước):**

| Phần | Gồm | Điểm |
|---|---|---|
| **Phần 1** | Việc 1 (File Discovery) + Việc 3 (Kafka Topic Design) | 2.5đ |
| **Phần 2** | Việc 2 (Parser Service) + Việc 7 (Architecture Diagram) | 2.5đ |
| **Phần 3** | Việc 4 (Neo4j Ingestion) | 2đ |
| **Phần 4** | Việc 5 (Spark + MongoDB Ingestion) | 2đ |
| *(chung, không thuộc phần nào)* | Việc 6 (Idempotent Replay Verification) | 1đ |

> Việc 6 để riêng vì bắt buộc phải phối hợp giữa người Phần 2, 3, 4 — không giao hẳn được cho 1 người.

**Thứ tự thực hiện:**

1. **Phần 1** làm trước tiên — không phụ thuộc gì cả, nên xong sớm nhất.
2. **Phần 2** phải **đợi Phần 1 xong**, vì Parser Service cần danh sách file (Việc 1) và schema/tên topic Kafka đã thiết kế (Việc 3) mới biết đẩy event đi đâu, định dạng gì.
   - Việc 7 (kiến trúc) có thể vẽ nháp song song từ đầu, nhưng nên chốt bản cuối sau khi pipeline chạy thật.
3. **Phần 3 và Phần 4** đều phải **đợi Phần 2 đẩy được dữ liệu thật lên Kafka** thì mới có gì để nhận/test. Nhưng Phần 3 và Phần 4 **độc lập với nhau** → khi Phần 2 xong, 2 phần này làm **song song** được.
4. **Việc 6** chỉ làm được sau khi Phần 2, 3, 4 đều đã chạy hoàn chỉnh end-to-end (vì phải sửa file → parse lại → kiểm tra cả Neo4j lẫn MongoDB cùng lúc).
5. Chốt bản cuối cùng của **Việc 7** sau khi mọi thứ đã chạy thật.

**Gợi ý đỡ phí thời gian chờ:** người phụ trách Phần 3/4 không cần ngồi không trong lúc chờ Phần 2 — có thể tranh thủ dựng sẵn Neo4j/Spark container, viết khung code (Cypher `MERGE` mẫu, khung Spark job), test bằng dữ liệu giả tự tạo, để khi Phần 2 xong là cắm vào chạy luôn.

### 3.1. Lịch trình 6 ngày

> Phần 2 (Parser Service) là **điểm nghẽn** của cả pipeline — mọi thứ sau nó đều phải chờ, nên cần ưu tiên thời gian nhiều nhất cho phần này. Lịch 6 ngày khá gấp, nên **Phần 3/4 bắt buộc phải dựng khung sẵn từ Ngày 2–3** (không đợi tới lúc có data thật mới bắt đầu) để Ngày 4 chỉ cần cắm data thật vào chạy.

| Ngày | Công việc chính |
|---|---|
| **Ngày 1** | Việc 0 (dựng Docker Compose, thống nhất schema) → Phần 1 (Việc 1: file discovery + Việc 3: thiết kế Kafka topic). Xong trong 1 ngày vì không phụ thuộc gì. |
| **Ngày 2–3** | **Trọng tâm: Phần 2** (viết Parser Service — trích AST/CFG/DFG/call edges, gán ID ổn định, đẩy event lên Kafka). Song song: Phần 3 và Phần 4 dựng sẵn khung (Neo4j connector config, khung Spark job) bằng dữ liệu giả — **bắt buộc xong khung trong 2 ngày này**, không chờ Phần 2. |
| **Ngày 4** | Phần 3 (Neo4j) và Phần 4 (Spark + MongoDB) cắm dữ liệu thật từ Kafka vào khung đã dựng sẵn, chạy và fix lỗi — làm **song song** vì độc lập nhau. Việc 7 (kiến trúc) vẽ bản nháp trong lúc này. |
| **Ngày 5** | Việc 6 (Idempotent Replay Verification) — bắt buộc người phụ trách Phần 2, 3, 4 ngồi lại kiểm tra cùng nhau. Chốt bản cuối Việc 7 (Architecture Diagram) sau khi pipeline chạy ổn. |
| **Ngày 6** | Hoàn thiện Jupyter Book (ảnh chụp, số liệu, reflection), publish lên GitHub Pages, review chéo lẫn nhau, nộp bài. |

**Lưu ý vì lịch gấp:** mỗi người nên **viết luôn chương Jupyter Book của phần mình ngay sau khi phần đó chạy xong** (ví dụ xong Phần 1 ở Ngày 1 thì viết chương 1 luôn), tuyệt đối không để dồn hết qua ngày cuối — Ngày 6 chỉ nên dùng để ghép lại, polish và publish. Nếu Ngày 2–3 Phần 2 bị trễ, cả nhóm cần họp nhanh để rút gọn phạm vi (ví dụ ưu tiên AST + Call edge trước, CFG/DFG làm sau nếu còn thời gian) thay vì để trễ dây chuyền cả Phần 3, 4.

---

### Việc 0 — Chuẩn bị chung (cả nhóm cùng làm trước)
- Shallow clone repo, thống nhất tiêu chí lọc file test/setup/auto-generated
- Dựng Docker Compose dùng chung: Kafka, Neo4j, MongoDB, Spark
- Thống nhất schema chung cho event: mỗi message phải có field `schema_version` và `event_time` (timestamp)
- Khởi tạo repo GitHub + khung sườn Jupyter Book (mục lục theo từng Task)

---

### Việc 1 — Repository Cloning & File Discovery *(1 điểm)*
- Shallow-clone repo bằng `git clone --depth 1`
- Liệt kê toàn bộ file `.py` trong repo
- Loại bỏ file test/setup/auto-generated (tùy chọn nhưng khuyến khích — nếu loại thì phải nêu rõ tiêu chí)
- Ghi lại **tổng số file `.py` tìm được** vào báo cáo

---

### Việc 2 — Incremental CPG Parser Service *(1.5 điểm)*
- Viết 1 service Python ("Parser Service") xử lý **từng file một**, không load cả repo vào bộ nhớ 1 lượt (bounded memory)
- Chọn công cụ parse: `ast` module / tree-sitter / Joern
- Trích xuất đủ 4 loại: AST nodes, CFG edges, DFG edges, Call edges
- Gán **ID ổn định** cho mỗi node/edge để đảm bảo reprocess không tạo trùng lặp
- Đẩy mỗi loại thành **event có cấu trúc** lên Kafka topic tương ứng

---

### Việc 3 — Kafka Topic Design *(1.5 điểm)*
- Thiết kế đủ **4 topic riêng biệt**: node events, edge events, source metadata events, parser error events
- Mỗi message phải có `schema_version` và `event_time`
- Định nghĩa partition key hợp lý (ví dụ theo file path hoặc node id)
- Cấu hình topic (replication factor, retention) phù hợp môi trường lab

---

### Việc 4 — Graph Topology Ingestion vào Neo4j *(2 điểm)*
- Cấu hình **Neo4j Kafka Connector Sink** để nhận trực tiếp từ Kafka — **không dùng Spark** ở nhánh này (đây là yêu cầu bắt buộc của đề, khác với nhánh MongoDB)
- Map node events → Neo4j node, edge events → Neo4j relationship
- Đảm bảo idempotent: dùng `MERGE` (Cypher) thay vì `CREATE` để tránh trùng khi dữ liệu bị đẩy lại
- Chụp ảnh Neo4j Browser minh họa đồ thị CPG cho báo cáo

---

### Việc 5 — Source Metadata Ingestion vào MongoDB *(2 điểm)*
- Viết job **Spark Structured Streaming** đọc topic metadata từ Kafka
- Ghi dữ liệu vào MongoDB qua **MongoDB Spark Connector**
- Bắt buộc cấu hình **checkpoint location** để job có thể resume đúng offset khi bị restart
- Chụp ảnh MongoDB Compass (collection + document mẫu) cho báo cáo

---

### Việc 6 — Idempotent Replay Verification *(1 điểm)*
> Việc này cần người phụ trách Việc 2, 4, 5 phối hợp cùng nhau, vì phải kiểm tra xuyên suốt cả pipeline.

- Sửa **1 file Python** bất kỳ trong repo đã clone
- Chạy lại đúng file đó qua Parser Service
- Kiểm chứng:
  - Neo4j: số node/edge cập nhật đúng, **không** tạo bản ghi trùng
  - MongoDB: có document metadata **mới nhất** cho đúng file đó
  - Spark checkpoint: **bỏ qua** các file không đổi, không xử lý lại từ đầu

---

### Việc 7 — Architecture Diagram *(1 điểm)*
- Vẽ sơ đồ kiến trúc tổng thể toàn bộ pipeline (giống sơ đồ ở mục 1.2 nhưng chi tiết hơn, có tên công cụ/version cụ thể nhóm dùng)
- Công cụ gợi ý: draw.io, Excalidraw, hoặc Mermaid ngay trong Jupyter Book

---

## 4. Cấu trúc Jupyter Book (mỗi chương ứng với 1 Việc ở trên)

1. Giới thiệu chung & Repository Cloning
2. Kiến trúc tổng thể (Architecture Diagram)
3. Incremental CPG Parser Service
4. Kafka Topic Design
5. Graph Ingestion vào Neo4j
6. Metadata Ingestion vào MongoDB
7. Idempotent Replay Verification
8. Kết luận & Reflection tổng thể

Mỗi chương cần có đủ 4 phần: **giải thích cách làm + lý do chọn** → **output thực tế** (số liệu, sample Kafka message, kết quả query DB) → **screenshot giao diện database** → **reflection** (cái gì work, cái gì fail, cách nhóm xử lý).

---

## 5. Lưu ý chung
- Chỉ làm trên **1 repo** (`huggingface/accelerate`) từ đầu đến cuối.
- Commit thường xuyên, message rõ ràng, phản ánh tiến độ thật theo thời gian.
- Không nộp zip/PDF/Word — chỉ nộp đúng 1 URL Jupyter Book.
