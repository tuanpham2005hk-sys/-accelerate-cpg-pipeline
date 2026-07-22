# Neo4j Kafka Connector — plugin jar

Thư mục này phải chứa file `.jar` của **Neo4j Connector for Kafka** (bản Kafka
Connect self-contained), để `docker-compose.yml` mount vào container
`kafka-connect`. File jar **không** được commit sẵn trong repo team (dung
lượng lớn, nên tải tại máy mỗi người).

## Tải về

1. Vào trang release chính thức:
   https://github.com/neo4j/neo4j-kafka-connector/releases
2. Tải file có đuôi dạng `neo4j-kafka-connect-<version>.jar` (bản mới nhất khi
   viết doc này là dòng 5.1.x/5.2.x — cứ lấy **Latest**, không cần đúng số cụ
   thể).
3. Copy file `.jar` vừa tải vào **đúng thư mục này**
   (`kafka-connect/plugins/`).
4. Chạy `docker compose up -d kafka-connect` (hoặc `docker compose up -d`)
   — Kafka Connect worker sẽ tự nhận plugin qua `CONNECT_PLUGIN_PATH`.

## Vì sao không cài qua `confluent-hub install` như nhiều tutorial cũ?

Gói trên Confluent Hub (`neo4j/kafka-connect-neo4j`) hiện dừng ở bản cũ, dùng
`connector.class = streams.kafka.connect.sink.Neo4jSinkConnector`. Bản hiện
hành (5.1+) đã đổi sang `connector.class = org.neo4j.connectors.kafka.sink.Neo4jConnector`
với nhiều tham số cấu hình khác (`neo4j.uri` thay vì `neo4j.server.uri`,
`neo4j.cypher.topic.<topic>` thay vì `neo4j.topic.cypher.<topic>`...). File
`kafka-connect/neo4j-sink-connector.json` trong repo này viết theo đúng bản
mới — nên phải tải jar bản mới từ GitHub, không dùng Confluent Hub.

## Kiểm tra đã nhận plugin chưa

```bash
docker compose up -d kafka-connect
curl -s http://localhost:8083/connector-plugins | grep -i neo4j
```
Phải thấy `"class":"org.neo4j.connectors.kafka.sink.Neo4jConnector"` trong
kết quả trả về.
