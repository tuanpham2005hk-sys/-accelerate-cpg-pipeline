# Task 1 - Giới thiệu chung & Repository Cloning

## 1. Đồ án này làm gì

Môn **CSC14118 – Nhập môn Dữ liệu lớn**, Lab 04: xây dựng một pipeline
**streaming** đọc source code Python của repo
[`huggingface/accelerate`](https://github.com/huggingface/accelerate),
phân tích cấu trúc mỗi file thành **Code Property Graph (CPG)** — gồm 4 loại
thông tin: **AST node**, **CFG edge**, **DFG edge** và **Call edge** — rồi ghi
kết quả xuống hai hệ cơ sở dữ liệu khác nhau theo hai cơ chế ingest khác
nhau:

```text
Repo accelerate (.py files)
        │
        ▼
  Parser Service  ──► đọc từng file MỘT (không đọc cả repo 1 lượt)
        │              trích ra AST / CFG / DFG / Call edges
        ▼
     Apache Kafka  ──► 4 topic riêng: node / edge / metadata / error
        │
        ├──────────────────────────────┐
        ▼                              ▼
  Neo4j Kafka Connector       Spark Structured Streaming
  (nhận thẳng từ Kafka,        (đọc metadata từ Kafka)
   KHÔNG qua Spark)                    │
        │                              ▼
        ▼                        MongoDB Spark Connector
      Neo4j                             │
  (lưu đồ thị CPG)                      ▼
                                     MongoDB
                              (lưu metadata từng file)
```

Điểm mấu chốt đề bài muốn kiểm tra: **hai cách ingest dữ liệu khác nhau vào
hai database khác nhau** — nhánh Neo4j dùng Neo4j Kafka Connector Sink (không
cần code Spark), nhánh MongoDB dùng Spark Structured Streaming tự viết, có
checkpoint để resume đúng khi restart. Toàn bộ hệ thống phải **idempotent**:
xử lý lại một file không được tạo dữ liệu trùng.

Chi tiết đầy đủ phần đề bài và phân công công việc trong nhóm nằm trong
[`Lab04_PhanCongCongViec-3.md`](https://github.com/tuanpham2005hk-sys/-accelerate-cpg-pipeline/blob/main/Lab04_PhanCongCongViec-3.md)
ở repo GitHub của nhóm.

## 2. Repository Cloning

Repo được chọn: **`huggingface/accelerate`**, clone dạng shallow (chỉ lấy
commit mới nhất, không kéo toàn bộ lịch sử) để giảm dung lượng tải:

```bash
git clone --depth 1 https://github.com/huggingface/accelerate.git
```

## 3. File Discovery

Sau khi clone, chạy script liệt kê + lọc toàn bộ file `.py`:

```bash
python scripts/discover_files.py --repo accelerate --out output/file_discovery.json
```

### Tiêu chí loại bỏ file

| Loại | Tiêu chí |
|---|---|
| **test** | Nằm trong thư mục `tests/` HOẶC tên khớp `test_*.py` / `*_test.py` / `conftest.py` |
| **setup** | File packaging/build: `setup.py` |
| **auto-generated** | Tên `_version.py` / `version.py` / `*_pb2.py`; HOẶC 40 dòng header đầu chứa marker như `DO NOT EDIT`, `@generated`, `auto-generated` |

Nhóm chọn loại các nhóm file này vì chúng không phản ánh logic nghiệp vụ thật
của thư viện — file test chủ yếu gọi lại API đã có sẵn, `setup.py` chỉ khai
báo packaging, và file auto-generated không do người viết tay — nên đưa vào
CPG sẽ không có nhiều giá trị phân tích cấu trúc code thật.

### Kết quả thật (từ `output/file_discovery.json`)

| Chỉ số | Giá trị |
|---|---|
| **Tổng số file `.py` tìm được** | **197** |
| Giữ lại (source thật, đưa vào Parser Service) | **142** |
| Loại bỏ | **55** |
| — test | 54 |
| — setup | 1 |
| — auto-generated | 0 |

> Repo `accelerate` không có file nào khớp tiêu chí auto-generated → count =
> 0, đúng như kỳ vọng vì đây là thư viện Python thuần, không sinh code tự
> động kiểu protobuf.

Vài file mẫu nằm trong danh sách 142 file giữ lại:

```text
benchmarks/big_model_inference/big_model_inference.py
benchmarks/big_model_inference/measures_util.py
benchmarks/fp8/ms_amp/ddp.py
benchmarks/fp8/ms_amp/distrib_deepspeed.py
benchmarks/fp8/ms_amp/fp8_utils.py
...
src/accelerate/accelerator.py   (file lớn nhất, dùng làm file mẫu xuyên suốt sách)
```

Toàn bộ kết quả đầy đủ (danh sách 197 file kèm lý do giữ/loại) nằm trong
[`output/file_discovery.json`](../output/file_discovery.json).

## 4. Reflection

**Cái gì work:** shallow clone (`--depth 1`) đủ cho mục đích lab — chỉ cần
nội dung file mới nhất, không cần lịch sử commit của `accelerate`, nên tiết
kiệm đáng kể thời gian tải và dung lượng đĩa. Tiêu chí loại bỏ file dựa trên
đường dẫn/tên file (không cần đọc nội dung để phân loại test/setup) chạy
nhanh và dễ kiểm chứng lại bằng tay.

**Cái gì cần lưu ý:** vì lịch làm gấp (6 ngày), nhóm quyết định **không** đào
sâu thêm tiêu chí auto-generated (ví dụ file sinh bởi codegen nội bộ không
theo marker chuẩn) — chấp nhận rủi ro nhỏ là có thể sót một vài file không
thật sự "hand-written", đổi lại tiết kiệm thời gian để dồn cho Phần 2 (Parser
Service) — điểm nghẽn quan trọng nhất của cả pipeline.
