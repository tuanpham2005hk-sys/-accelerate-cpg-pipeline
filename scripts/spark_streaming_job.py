#!/usr/bin/env python
import os
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

def parse_args():
    parser = argparse.ArgumentParser(description="Spark Structured Streaming Job for CPG Source Metadata Ingestion")
    parser.add_argument(
        "--kafka-bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (default: localhost:9092)"
    )
    parser.add_argument(
        "--kafka-topic",
        default="cpg.source.metadata.events",
        help="Kafka topic to consume metadata from"
    )
    parser.add_argument(
        "--mongodb-uri",
        default=os.getenv("MONGODB_URI", "mongodb://localhost:27017"),
        help="MongoDB connection URI (default: mongodb://localhost:27017)"
    )
    parser.add_argument(
        "--mongodb-database",
        default="cpg_database",
        help="MongoDB database name"
    )
    parser.add_argument(
        "--mongodb-collection",
        default="source_metadata",
        help="MongoDB collection name"
    )
    parser.add_argument(
        "--checkpoint-location",
        default=os.getenv("SPARK_CHECKPOINT_LOCATION", "output/spark_checkpoint"),
        help="Directory for Spark structured streaming checkpoints"
    )
    parser.add_argument(
        "--starting-offsets",
        default="earliest",
        help="Kafka starting offsets (default: earliest)"
    )
    return parser.parse_args()

def main():
    args = parse_args()
    
    print("===============================================================")
    print("Starting Spark Structured Streaming Ingestion Job")
    print(f"Kafka Bootstrap Servers : {args.kafka_bootstrap_servers}")
    print(f"Kafka Topic             : {args.kafka_topic}")
    print(f"MongoDB URI             : {args.mongodb_uri}")
    print(f"MongoDB Database        : {args.mongodb_database}")
    print(f"MongoDB Collection      : {args.mongodb_collection}")
    print(f"Checkpoint Location     : {args.checkpoint_location}")
    print(f"Starting Offsets        : {args.starting_offsets}")
    print("===============================================================")

    # Define Schema for CPG source metadata events
    counts_schema = StructType([
        StructField("nodes", IntegerType(), True),
        StructField("edges", IntegerType(), True),
        StructField("ast_child_edges", IntegerType(), True),
        StructField("cfg_edges", IntegerType(), True),
        StructField("dfg_edges", IntegerType(), True),
        StructField("call_edges", IntegerType(), True)
    ])

    metadata_schema = StructType([
        StructField("schema_version", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("file_path", StringType(), True),
        StructField("repository", StringType(), True),
        StructField("loc", IntegerType(), True),
        StructField("size_bytes", LongType(), True),
        StructField("content_sha256", StringType(), True),
        StructField("counts", counts_schema, True)
    ])

    # Initialize SparkSession configured for Kafka and MongoDB Spark Connector
    spark = SparkSession.builder \
        .appName("CPGSourceMetadataIngestion") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Read stream from Kafka
    print(f"Subscribing to Kafka topic: {args.kafka_topic}")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka_bootstrap_servers) \
        .option("subscribe", args.kafka_topic) \
        .option("startingOffsets", args.starting_offsets) \
        .load()

    # Parse JSON payload from 'value' column
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), metadata_schema).alias("data")) \
        .select("data.*") \
        .filter(col("file_path").isNotNull())

    # Map file_path to _id to guarantee idempotent updates (replace/upsert by primary key in MongoDB)
    mongo_df = parsed_df.withColumn("_id", col("file_path"))

    print("Starting Streaming Query to MongoDB...")
    
    # Write stream to MongoDB
    query = mongo_df.writeStream \
        .format("mongodb") \
        .queryName("MetadataMongoDBIngestion") \
        .option("checkpointLocation", args.checkpoint_location) \
        .option("spark.mongodb.connection.uri", args.mongodb_uri) \
        .option("spark.mongodb.database", args.mongodb_database) \
        .option("spark.mongodb.collection", args.mongodb_collection) \
        .option("spark.mongodb.write.operationType", "replace") \
        .outputMode("append") \
        .start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping query...")
        query.stop()

if __name__ == "__main__":
    main()