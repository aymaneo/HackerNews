"""
Bronze Layer - Spark Batch Loader
Reads from Kafka using Spark and saves to Parquet
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    BooleanType, ArrayType, LongType
)
from datetime import datetime
import os
from delta import configure_spark_with_delta_pip

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkBronzeLoader:
    """Spark-based Bronze layer loader - ingests data from Kafka to Parquet"""

    def __init__(self, bronze_path: str = "data/bronze", kafka_servers: str = "kafka:9092"):
        """
        Initialize Spark Bronze Loader

        Args:
            bronze_path: Path to bronze data directory
            kafka_servers: Kafka bootstrap servers
        """
        self.bronze_path = bronze_path
        self.kafka_servers = kafka_servers
        self.spark = self._create_spark_session()
        logger.info(f"Initialized SparkBronzeLoader (bronze_path={bronze_path})")

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Kafka and Delta Lake support"""
        logger.info("Creating Spark session with Delta Lake...")

        # Set Java options for Java 23 compatibility
        os.environ['PYSPARK_SUBMIT_ARGS'] = (
            '--conf spark.driver.extraJavaOptions="-Djava.security.manager=allow" '
            '--conf spark.executor.extraJavaOptions="-Djava.security.manager=allow" '
            'pyspark-shell'
        )

        # Configure Spark with Kafka and Delta Lake
        # Note: We configure Delta first, then add Kafka package
        builder = SparkSession.builder \
            .appName("HN Bronze Loader - Delta Lake") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
            .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # Configure Delta Lake, then add Kafka package
        spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"]).getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        logger.info("✓ Spark session created")
        return spark

    def _get_hn_story_schema(self) -> StructType:
        """Define schema for HN stories"""
        return StructType([
            StructField("id", IntegerType(), True),
            StructField("by", StringType(), True),
            StructField("descendants", IntegerType(), True),
            StructField("kids", ArrayType(IntegerType()), True),
            StructField("score", IntegerType(), True),
            StructField("time", LongType(), True),
            StructField("title", StringType(), True),
            StructField("type", StringType(), True),
            StructField("url", StringType(), True),
            StructField("text", StringType(), True)
        ])

    def _get_hn_comment_schema(self) -> StructType:
        """Define schema for HN comments"""
        return StructType([
            StructField("id", IntegerType(), True),
            StructField("by", StringType(), True),
            StructField("kids", ArrayType(IntegerType()), True),
            StructField("parent", IntegerType(), True),
            StructField("story_id", IntegerType(), True),
            StructField("text", StringType(), True),
            StructField("time", LongType(), True),
            StructField("type", StringType(), True),
            StructField("deleted", BooleanType(), True),
            StructField("dead", BooleanType(), True)
        ])

    def load_from_kafka_batch(self, topic: str, schema: StructType, table_name: str):
        """
        Load data from Kafka topic in batch mode using Spark

        Args:
            topic: Kafka topic name
            schema: Schema for the data
            table_name: Name for output table (stories or comments)
        """
        logger.info(f"Loading from Kafka topic: {topic}")

        # Read from Kafka in batch mode (read all available data)
        df = self.spark \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()

        logger.info(f"Read {df.count()} messages from Kafka")

        # Parse JSON value
        df_parsed = df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("offset").alias("_kafka_offset"),
            col("partition").alias("_kafka_partition")
        ).select("data.*", "_kafka_offset", "_kafka_partition")

        # Add ingestion timestamp
        df_bronze = df_parsed.withColumn("_bronze_ingested_at", current_timestamp())

        # Save to Delta Lake
        output_path = f"{self.bronze_path}/{table_name}"
        os.makedirs(output_path, exist_ok=True)

        # Write to Delta Lake (append mode to preserve history)
        df_bronze.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(output_path)

        record_count = df_bronze.count()
        logger.info(f"✓ Saved {record_count} records to Delta table: {output_path}")
        logger.info(f"  Columns: {df_bronze.columns}")

        return record_count

    def load_stories(self):
        """Load stories from Kafka to Bronze layer using Spark"""
        logger.info("\n" + "="*60)
        logger.info("Loading STORIES with Spark")
        logger.info("="*60)

        schema = self._get_hn_story_schema()
        count = self.load_from_kafka_batch("hn-stories", schema, "stories")
        return count

    def load_comments(self):
        """Load comments from Kafka to Bronze layer using Spark"""
        logger.info("\n" + "="*60)
        logger.info("Loading COMMENTS with Spark")
        logger.info("="*60)

        schema = self._get_hn_comment_schema()
        count = self.load_from_kafka_batch("hn-comments", schema, "comments")
        return count

    def load_all(self):
        """
        Load all data from Kafka to Bronze layer using Spark

        Returns:
            Tuple of (stories_count, comments_count)
        """
        logger.info("Starting Spark Bronze Batch Loader")

        stories_count = self.load_stories()
        comments_count = self.load_comments()

        logger.info("\n" + "="*60)
        logger.info("Spark Bronze Batch Loader Complete!")
        logger.info("="*60)
        logger.info(f"Stories: {stories_count} records")
        logger.info(f"Comments: {comments_count} records")
        logger.info(f"\nData saved to:")
        logger.info(f"  - {self.bronze_path}/stories/")
        logger.info(f"  - {self.bronze_path}/comments/")

        return (stories_count, comments_count)

    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")
