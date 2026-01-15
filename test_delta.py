#!/usr/bin/env python3
"""
Quick test script to verify Delta Lake setup
Run: python3 test_delta.py
"""

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os

def main():
    print("=" * 60)
    print("Testing Delta Lake Setup")
    print("=" * 60)
    print()

    # Check if data exists
    bronze_stories = "data/bronze/stories"
    silver_stories = "data/silver/stories"

    if not os.path.exists(bronze_stories):
        print("⚠️  No Bronze data found. Run ./run_pipeline.sh first!")
        return

    # Create Spark session
    print("Creating Spark session with Delta Lake...")
    builder = SparkSession.builder \
        .appName("Test Delta Lake") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print("✓ Spark session created")
    print()

    # Read Bronze stories
    print("Reading Bronze stories...")
    df_bronze = spark.read.format("delta").load(bronze_stories)
    bronze_count = df_bronze.count()
    print(f"✓ Found {bronze_count} stories in Bronze layer")
    df_bronze.show(3, truncate=False)
    print()

    # Read Silver stories
    print("Reading Silver stories...")
    df_silver = spark.read.format("delta").load(silver_stories)
    silver_count = df_silver.count()
    print(f"✓ Found {silver_count} stories in Silver layer")
    df_silver.show(3, truncate=False)
    print()

    # Show Delta Lake features
    print("=" * 60)
    print("Delta Lake Features")
    print("=" * 60)
    print()

    print("1. Transaction History:")
    from delta.tables import DeltaTable
    dt = DeltaTable.forPath(spark, bronze_stories)
    dt.history().select("version", "timestamp", "operation", "operationMetrics").show(5, truncate=False)
    print()

    print("2. Schema:")
    df_silver.printSchema()
    print()

    print("=" * 60)
    print("✅ Delta Lake is working perfectly!")
    print("=" * 60)
    print()
    print("Your team can now:")
    print("  • Query data with Spark")
    print("  • Time travel to previous versions")
    print("  • Use ACID transactions")
    print("  • Auto-merge schema changes")
    print()

    spark.stop()

if __name__ == "__main__":
    main()
