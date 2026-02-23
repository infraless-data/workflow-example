#!/usr/bin/env python3
"""
Extract Data Function (Demo Stub)
Demonstrates the extraction step of the ETL pipeline.

Functions receive a SparkSession as the first argument, followed by
any additional arguments from the pipeline YAML definition.
"""

import json
import logging
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main(spark, source_table: str = "raw_transactions", dataset: str = "sales_data"):
    """
    Main entry point for the extraction function.

    Args:
        spark: SparkSession with Iceberg catalog configured
        source_table: The source table to extract from
        dataset: The dataset name to use
    """
    logger.info("Starting data extraction (demo mode)")

    # Get config from Spark properties (set by the platform)
    conf = spark.sparkContext.getConf()
    execution_id = conf.get("spark.workflow.executionId", "local-run")
    pipeline_name = conf.get("spark.workflow.pipelineName", "daily_etl")
    task_name = conf.get("spark.workflow.taskName", "extract")
    environment = conf.get("spark.workflow.environment", "development")

    logger.info(f"Configuration:")
    logger.info(f"  Dataset: {dataset} test")
    logger.info(f"  Source Table: {source_table}")
    logger.info(f"  Execution ID: {execution_id}")
    logger.info(f"  Pipeline: {pipeline_name}")
    logger.info(f"  Task: {task_name}")
    logger.info(f"  Environment: {environment}")

    # Simulate extraction window
    end_time = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=1)

    logger.info(f"Extraction window: {start_time.isoformat()} to {end_time.isoformat()}")

    # Create a sample Iceberg table using PySpark
    logger.info("Step 1: Generating sample data...")

    data = [
        (1, "2025-01-15", "us-east", "electronics", 142, 28450.00),
        (2, "2025-01-15", "us-west", "electronics", 98, 19600.00),
        (3, "2025-01-15", "us-east", "clothing", 215, 10750.00),
        (4, "2025-01-15", "eu-west", "electronics", 76, 15200.00),
        (5, "2025-01-15", "eu-west", "clothing", 134, 6700.00),
    ]
    columns = ["id", "date", "region", "category", "orders", "revenue"]

    df = spark.createDataFrame(data, columns)

    logger.info("Step 2: Writing to Iceberg table...")
    df.writeTo(f"sales.{source_table}").createOrReplace()

    row_count = df.count()
    logger.info(f"Extraction completed: {row_count} records written to sales.{source_table}")

    result = {
        "status": "success",
        "records_extracted": row_count,
        "table": f"sales.{source_table}",
        "execution_id": execution_id,
    }

    print(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    # Use dev.py for local testing:
    #   python dev.py run functions/extract_data.py raw_transactions sales_data
    print("Use 'python dev.py run functions/extract_data.py raw_transactions sales_data' for local testing")
