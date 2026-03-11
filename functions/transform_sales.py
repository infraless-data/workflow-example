#!/usr/bin/env python3
"""
Transform Sales Function (Demo Stub)
Demonstrates the transformation step of the ETL pipeline.

Functions receive a SparkSession as the first argument, followed by
any additional arguments from the pipeline YAML definition.
"""

import json
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main(spark):
    """
    Main entry point for the transformation function.

    Args:
        spark: SparkSession with Iceberg catalog configured
    """
    logger.info("Starting sales transformation (demo mode)")

    # Get config from Spark properties (set by the platform)
    conf = spark.sparkContext.getConf()
    execution_id = conf.get("spark.workflow.executionId", "local-run")
    pipeline_name = conf.get("spark.workflow.pipelineName", "daily_etl")
    task_name = conf.get("spark.workflow.taskName", "transform")
    environment = conf.get("spark.workflow.environment", "development")

    logger.info(f"Configuration:")
    logger.info(f"  Execution ID: {execution_id}")
    logger.info(f"  Pipeline: {pipeline_name}")
    logger.info(f"  Task: {task_name}")
    logger.info(f"  Environment: {environment}")

    # Read from the Iceberg table created by the extract step
    logger.info("Step 1: Reading from analytics.raw_transactions...")
    df = spark.table("analytics.raw_transactions")
    input_count = df.count()
    logger.info(f"  Read {input_count} records")

    # Apply transformations using Spark SQL
    logger.info("Step 2: Applying transformations...")
    df.createOrReplaceTempView("raw_data")

    transformed = spark.sql("""
        SELECT
            region,
            category AS product_category,
            SUM(orders) AS total_orders,
            SUM(revenue) AS total_revenue,
            ROUND(SUM(revenue) / SUM(orders), 2) AS avg_order_value
        FROM raw_data
        GROUP BY region, category
    """)

    # Write transformed data to Iceberg table
    logger.info("Step 3: Writing to analytics.sales_summary...")
    transformed.writeTo("analytics.sales_summary").createOrReplace()

    output_count = transformed.count()
    logger.info(f"Transformation completed: {input_count} -> {output_count} records")

    # Compute summary metrics
    metrics = spark.sql("""
        SELECT
            SUM(total_revenue) as total_revenue,
            SUM(total_orders) as total_orders,
            COUNT(DISTINCT region) as unique_regions
        FROM analytics.sales_summary
    """).collect()[0]

    result = {
        "status": "success",
        "records_input": input_count,
        "records_output": output_count,
        "metrics": {
            "total_revenue": float(metrics.total_revenue or 0),
            "total_orders": int(metrics.total_orders or 0),
            "unique_regions": int(metrics.unique_regions or 0),
        },
        "execution_id": execution_id,
    }

    logger.info(f"Total revenue: ${result['metrics']['total_revenue']:,.2f}")

    print(json.dumps(result, indent=2, default=str))
    return result


if __name__ == "__main__":
    # Use dev.py for local testing:
    #   python dev.py run functions/transform_sales.py
    print("Use 'python dev.py run functions/transform_sales.py' for local testing")
