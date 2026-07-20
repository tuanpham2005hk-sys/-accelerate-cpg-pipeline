$ErrorActionPreference = "Stop"

$container = "cpg-kafka"
$bootstrap = "localhost:9092"
$expected = @(
    "cpg.node.events",
    "cpg.edge.events",
    "cpg.source.metadata.events",
    "cpg.parser.error.events"
)

$actual = @(docker exec $container kafka-topics --bootstrap-server $bootstrap --list)
if ($LASTEXITCODE -ne 0) {
    throw "Failed to list Kafka topics"
}

$passed = 0
foreach ($topic in $expected) {
    if ($actual -contains $topic) {
        Write-Host "[PASS] $topic"
        $passed++
    }
    else {
        Write-Host "[FAIL] $topic"
    }
}

Write-Host "RESULT: $passed/$($expected.Count) topics PASS"
foreach ($topic in $expected) {
    docker exec $container kafka-topics `
        --bootstrap-server $bootstrap --describe --topic $topic `
        | Select-Object -First 1
}

if ($passed -ne $expected.Count) {
    throw "Kafka topic verification failed"
}
Write-Host "ALL TOPICS PASS"
