#!/usr/bin/env python3
"""
Run Spark Bronze Loader
Loads data from Kafka to Bronze layer using Spark
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spark_loader import SparkBronzeLoader


def main():
    """Main execution function"""
    print("=" * 60)
    print("SPARK BRONZE LAYER LOADER")
    print("=" * 60)

    # Initialize Bronze loader
    loader = SparkBronzeLoader(
        bronze_path="data/bronze",
        kafka_servers="kafka:29092"
    )

    try:
        # Load all data from Kafka
        stories_count, comments_count = loader.load_all()

        print("\n" + "=" * 60)
        print("SUCCESS!")
        print("=" * 60)
        print(f"Total stories loaded: {stories_count}")
        print(f"Total comments loaded: {comments_count}")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        # Stop Spark session
        loader.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
