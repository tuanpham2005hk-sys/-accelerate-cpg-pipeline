// Phần 3 — Việc 4: chạy 1 LẦN DUY NHẤT trước khi bật sink connector.
// Không có constraint này, MERGE theo node_id vẫn đúng nhưng sẽ full label
// scan mỗi lần (chậm dần khi đồ thị lớn) và không có ràng buộc unique thật
// sự ở tầng DB (chỉ đúng "nhờ" logic Cypher, không được DB enforce).

CREATE CONSTRAINT cpg_node_id_unique IF NOT EXISTS
FOR (n:CPGNode) REQUIRE n.node_id IS UNIQUE;

CREATE CONSTRAINT cpg_edge_id_unique IF NOT EXISTS
FOR ()-[r:CPG_EDGE]-() REQUIRE r.edge_id IS UNIQUE;
