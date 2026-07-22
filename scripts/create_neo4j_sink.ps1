# Phần 3 — Việc 4: Đăng ký Neo4j Kafka Connector Sink qua REST API.
# Chạy sau khi `docker compose up -d` và sau khi đã copy jar plugin vào
# kafka-connect/plugins/ (xem README trong đó).

$ConnectUrl = "http://localhost:8083"
$ConfigFile = Join-Path $PSScriptRoot "..\kafka-connect\neo4j-sink-connector.json"
$ConstraintsFile = Join-Path $PSScriptRoot "..\kafka-connect\neo4j-constraints.cypher"

Write-Host "=== Cho Kafka Connect REST API san sang ($ConnectUrl) ==="
$ready = $false
for ($i = 0; $i -lt 30; $i++) {
    try {
        $resp = Invoke-WebRequest -Uri "$ConnectUrl/connectors" -UseBasicParsing -TimeoutSec 3
        if ($resp.StatusCode -eq 200) { $ready = $true; break }
    } catch {}
    Start-Sleep -Seconds 3
}
if (-not $ready) { Write-Error "Kafka Connect khong san sang."; exit 1 }
Write-Host "--> Connect REST API da san sang."

Write-Host "=== Kiem tra plugin Neo4j ==="
$plugins = Invoke-RestMethod -Uri "$ConnectUrl/connector-plugins"
if (-not ($plugins.class -contains "org.neo4j.connectors.kafka.sink.Neo4jConnector")) {
    Write-Error "Khong thay plugin Neo4j. Xem kafka-connect/plugins/README.md"
    exit 1
}
Write-Host "--> Plugin org.neo4j.connectors.kafka.sink.Neo4jConnector da san sang."

Write-Host "=== Tao constraint uniqueness tren Neo4j ==="
Get-Content $ConstraintsFile | docker exec -i cpg-neo4j cypher-shell -u neo4j -p password

Write-Host "=== Dang ky (hoac cap nhat) connector cpg-neo4j-sink ==="
$configObj = Get-Content $ConfigFile -Raw | ConvertFrom-Json
$name = $configObj.name
$configJson = $configObj.config | ConvertTo-Json -Depth 10 -Compress

Invoke-RestMethod -Method Put -Uri "$ConnectUrl/connectors/$name/config" `
    -ContentType "application/json" -Body $configJson | ConvertTo-Json -Depth 10

Start-Sleep -Seconds 3
Write-Host "`n=== Trang thai connector ==="
Invoke-RestMethod -Uri "$ConnectUrl/connectors/$name/status" | ConvertTo-Json -Depth 10
