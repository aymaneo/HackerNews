#!/usr/bin/env python3
"""
Run Spark Silver Processor
Process Bronze data to Silver layer using Spark
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from silver.spark_processor import SparkSilverProcessor


def main():
    """Main execution function"""
    print("=" * 60)
    print("SPARK SILVER LAYER PROCESSOR")
    print("=" * 60)

    # Initialize Silver processor
    processor = SparkSilverProcessor(
        bronze_path="data/bronze",
        silver_path="data/silver"
    )

    try:
        # Process all Bronze data to Silver
        stories_count, comments_count = processor.process_all()

        print("\n" + "=" * 60)
        print("SUCCESS!")
        print("=" * 60)
        print(f"Total stories processed: {stories_count}")
        print(f"Total comments processed: {comments_count}")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        # Stop Spark session
        processor.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
