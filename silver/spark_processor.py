"""
Silver Layer - Spark Processor
Clean and standardize Bronze data using Spark
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, when, length, size, coalesce, lit,
    from_unixtime, regexp_replace, split, current_timestamp
)
from pyspark.sql.types import StringType, BooleanType, IntegerType, FloatType
from datetime import datetime
import os
import re
from html import unescape as html_unescape
from delta import configure_spark_with_delta_pip

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_html_text(text: str) -> str:
    """
    Clean HTML from text (standalone UDF function)

    Args:
        text: Raw text with HTML

    Returns:
        Cleaned text
    """
    if not text or text == "":
        return ""

    try:
        # Unescape HTML entities (&#x27; -> ', &#x2F; -> /, etc.)
        text = html_unescape(text)

        # Replace <p> tags with newlines
        text = re.sub(r'<p>', '\n\n', text)

        # Remove HTML tags but keep the content
        text = re.sub(r'<[^>]+>', '', text)

        # Clean up multiple newlines
        text = re.sub(r'\n{3,}', '\n\n', text)

        # Remove leading/trailing whitespace
        text = text.strip()

        return text
    except:
        return ""


class SparkSilverProcessor:
    """Spark-based Silver layer processor - clean and transform Bronze data"""

    def __init__(self, bronze_path: str = "data/bronze", silver_path: str = "data/silver"):
        """
        Initialize Spark Silver Processor

        Args:
            bronze_path: Path to bronze data directory
            silver_path: Path to silver data directory
        """
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.spark = self._create_spark_session()
        logger.info(f"Initialized SparkSilverProcessor (bronze={bronze_path}, silver={silver_path})")

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Delta Lake support"""
        logger.info("Creating Spark session with Delta Lake...")

        # Set Java options for Java 23 compatibility
        os.environ['PYSPARK_SUBMIT_ARGS'] = (
            '--conf spark.driver.extraJavaOptions="-Djava.security.manager=allow" '
            '--conf spark.executor.extraJavaOptions="-Djava.security.manager=allow" '
            'pyspark-shell'
        )

        # Configure Spark with Delta Lake
        builder = SparkSession.builder \
            .appName("HN Silver Processor - Delta Lake") \
            .master("local[*]") \
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
            .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        logger.info("✓ Spark session created")
        return spark


    def process_stories(self):
        """Process stories from Bronze to Silver using Spark"""
        logger.info("\n" + "="*60)
        logger.info("Processing STORIES with Spark")
        logger.info("="*60)

        # Read Bronze stories from Delta Lake
        bronze_stories_path = f"{self.bronze_path}/stories"
        if not os.path.exists(bronze_stories_path):
            logger.warning(f"No Bronze stories found at {bronze_stories_path}")
            return

        df = self.spark.read.format("delta").load(bronze_stories_path)
        logger.info(f"Loaded {df.count()} stories from Bronze")

        # Register UDF for HTML cleaning
        clean_html_udf = udf(clean_html_text, StringType())

        # Parse timestamp (convert Unix timestamp to ISO format string)
        df_clean = df.withColumn(
            "timestamp",
            from_unixtime(col("time")).cast("timestamp")
        )

        # Clean text if exists (some stories have text field)
        df_clean = df_clean.withColumn(
            "text_raw",
            when(col("text").isNotNull(), col("text")).otherwise(lit(""))
        ).withColumn(
            "text_clean",
            when(col("text").isNotNull(), clean_html_udf(col("text"))).otherwise(lit(""))
        ).withColumn(
            "has_text",
            length(col("text_clean")) > 0
        )

        # Add metadata columns
        df_clean = df_clean \
            .withColumn("has_url", col("url").isNotNull()) \
            .withColumn("comment_count", coalesce(col("descendants"), lit(0)).cast(IntegerType())) \
            .withColumn("author", col("by"))

        # Select and order columns for Silver
        silver_columns = [
            "id", "author", "title", "url", "score", "comment_count",
            "timestamp", "has_url", "has_text", "type",
            "_bronze_ingested_at"
        ]

        # Add text columns conditionally
        df_silver = df_clean.select(
            "id", "author", "title", "url", "score", "comment_count",
            "text_raw", "text_clean",
            "timestamp", "has_url", "has_text", "type",
            "_bronze_ingested_at"
        )

        # Remove duplicates (by id, keep last)
        df_silver = df_silver.dropDuplicates(["id"])

        # Save to Delta Lake
        output_path = f"{self.silver_path}/stories"
        os.makedirs(output_path, exist_ok=True)

        df_silver.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(output_path)

        # Collect statistics
        record_count = df_silver.count()
        has_url_count = df_silver.filter(col("has_url") == True).count()
        has_text_count = df_silver.filter(col("has_text") == True).count()

        logger.info(f"✓ Saved {record_count} clean stories to Delta table: {output_path}")
        logger.info(f"  - Stories with URLs: {has_url_count}")
        logger.info(f"  - Stories with text: {has_text_count}")

        return record_count

    def process_comments(self):
        """Process comments from Bronze to Silver using Spark"""
        logger.info("\n" + "="*60)
        logger.info("Processing COMMENTS with Spark")
        logger.info("="*60)

        # Read Bronze comments from Delta Lake
        bronze_comments_path = f"{self.bronze_path}/comments"
        if not os.path.exists(bronze_comments_path):
            logger.warning(f"No Bronze comments found at {bronze_comments_path}")
            return

        df = self.spark.read.format("delta").load(bronze_comments_path)
        logger.info(f"Loaded {df.count()} comments from Bronze")

        # Register UDF for HTML cleaning
        clean_html_udf = udf(clean_html_text, StringType())

        # Parse timestamp
        df_clean = df.withColumn(
            "timestamp",
            from_unixtime(col("time")).cast("timestamp")
        )

        # Clean HTML from text
        df_clean = df_clean \
            .withColumn("text_raw", col("text")) \
            .withColumn("text_clean", clean_html_udf(col("text")))

        # Add metadata columns
        df_clean = df_clean \
            .withColumn("has_text", length(col("text_clean")) > 0) \
            .withColumn("word_count",
                when(col("text_clean").isNotNull(),
                     size(split(col("text_clean"), r"\s+"))).otherwise(lit(0))) \
            .withColumn("char_count",
                when(col("text_clean").isNotNull(),
                     length(col("text_clean"))).otherwise(lit(0))) \
            .withColumn("has_replies", col("kids").isNotNull()) \
            .withColumn("is_deleted", coalesce(col("deleted"), lit(False))) \
            .withColumn("is_dead", coalesce(col("dead"), lit(False)))

        # Calculate quality score (simple heuristic)
        df_clean = df_clean.withColumn(
            "quality_score",
            (when(col("word_count") > 5, lit(1.0)).otherwise(lit(0.0)) * 0.5) +
            (when(col("is_deleted") == False, lit(1.0)).otherwise(lit(0.0)) * 0.3) +
            (when(col("is_dead") == False, lit(1.0)).otherwise(lit(0.0)) * 0.2)
        )

        # Rename 'by' to 'author'
        df_clean = df_clean.withColumn("author", col("by"))

        # Select and order columns for Silver
        df_silver = df_clean.select(
            "id", "author", "story_id", "parent",
            "text_raw", "text_clean",
            "timestamp", "has_text", "word_count", "char_count",
            "has_replies", "is_deleted", "is_dead", "quality_score",
            "type", "_bronze_ingested_at"
        )

        # Remove duplicates (by id)
        df_silver = df_silver.dropDuplicates(["id"])

        # Filter out deleted/dead comments with no text
        df_silver_filtered = df_silver.filter(
            (col("has_text") == True) |
            (col("is_deleted") == False) |
            (col("is_dead") == False)
        )

        # Save to Delta Lake
        output_path = f"{self.silver_path}/comments"
        os.makedirs(output_path, exist_ok=True)

        df_silver_filtered.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(output_path)

        # Collect statistics
        total_before_filter = df_silver.count()
        record_count = df_silver_filtered.count()
        has_text_count = df_silver_filtered.filter(col("has_text") == True).count()

        # Calculate averages
        avg_stats = df_silver_filtered.agg(
            {"word_count": "avg", "quality_score": "avg"}
        ).collect()[0]
        avg_word_count = avg_stats["avg(word_count)"]
        avg_quality_score = avg_stats["avg(quality_score)"]

        logger.info(f"✓ Saved {record_count} clean comments to Delta table: {output_path}")
        logger.info(f"  - Comments with text: {has_text_count}")
        logger.info(f"  - Average word count: {avg_word_count:.1f}")
        logger.info(f"  - Average quality score: {avg_quality_score:.2f}")
        logger.info(f"  - Filtered out: {total_before_filter - record_count} low-quality comments")

        return record_count

    def process_all(self):
        """
        Process all data from Bronze to Silver using Spark

        Returns:
            Tuple of (stories_count, comments_count)
        """
        logger.info("Starting Spark Silver Processing")

        stories_count = self.process_stories()
        comments_count = self.process_comments()

        logger.info("\n" + "="*60)
        logger.info("Spark Silver Processing Complete!")
        logger.info("="*60)
        logger.info(f"Stories: {stories_count} records")
        logger.info(f"Comments: {comments_count} records")
        logger.info(f"\nClean data saved to:")
        logger.info(f"  - {self.silver_path}/stories/")
        logger.info(f"  - {self.silver_path}/comments/")

        return (stories_count, comments_count)

    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")
