# Accelerate CPG Streaming Pipeline

**Môn:** CSC14118 – Nhập môn Dữ liệu lớn — Lab 04: Spark Streaming
**Repo nguồn:** [`huggingface/accelerate`](https://github.com/huggingface/accelerate)
**Repo bài làm:** [`tuanpham2005hk-sys/-accelerate-cpg-pipeline`](https://github.com/tuanpham2005hk-sys/-accelerate-cpg-pipeline)

Cuốn sách này trình bày lại toàn bộ quá trình nhóm xây dựng một pipeline
streaming: đọc source code Python của repo `accelerate`, phân tích cấu trúc
mỗi file thành **Code Property Graph (CPG)** (AST/CFG/DFG/Call edges), đẩy
qua **Apache Kafka**, rồi ghi song song vào hai hệ cơ sở dữ liệu qua hai cơ
chế ingest khác nhau:

- **Neo4j** — qua Neo4j Kafka Connector Sink (không qua Spark).
- **MongoDB** — qua Apache Spark Structured Streaming.

Mỗi chương tương ứng với 1 phần việc trong phân công, gồm: cách làm & lý do
chọn, output thực tế (số liệu, sample message, kết quả query DB), ảnh chụp
giao diện database, và reflection (cái gì work, cái gì không, cách nhóm xử
lý).

```{tableofcontents}
```
