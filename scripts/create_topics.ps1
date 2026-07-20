$ErrorActionPreference = "Stop"

$container = "cpg-kafka"
$bootstrap = "localhost:9092"
$topics = @(
    @{ Name = "cpg.node.events"; Partitions = 3; Replication = 1; Retention = 604800000 },
    @{ Name = "cpg.edge.events"; Partitions = 3; Replication = 1; Retention = 604800000 },
    @{ Name = "cpg.source.metadata.events"; Partitions = 3; Replication = 1; Retention = 1209600000 },
    @{ Name = "cpg.parser.error.events"; Partitions = 3; Replication = 1; Retention = 2592000000 }
)

Write-Host "=== Create Kafka topics (broker=$bootstrap) ==="
foreach ($topic in $topics) {
    Write-Host "--> $($topic.Name)"
    docker exec $container kafka-topics `
        --bootstrap-server $bootstrap `
        --create --if-not-exists `
        --topic $topic.Name `
        --partitions $topic.Partitions `
        --replication-factor $topic.Replication `
        --config "retention.ms=$($topic.Retention)" `
        --config "cleanup.policy=delete"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create topic $($topic.Name)"
    }
}

docker exec $container kafka-topics --bootstrap-server $bootstrap --list
if ($LASTEXITCODE -ne 0) {
    throw "Failed to list Kafka topics"
}
