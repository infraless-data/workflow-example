"""
Extract Data Function
Generates sample sales data and writes it to BigQuery.
"""

import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def main(bq, gcs, source_table: str = "raw_transactions", **context):
    """
    Extract raw sales data into BigQuery.

    Args:
        bq: google.cloud.bigquery.Client — authenticated as this repo's service account
        gcs: google.cloud.storage.Client — authenticated as this repo's service account
        source_table: destination table name in BigQuery
        **context: dataset_id, environment, execution_id, pipeline_name, task_name, project_id
    """
    import pandas as pd
    from google.cloud import bigquery

    dataset_id = context["dataset_id"]
    table_ref = f"{dataset_id}.{source_table}"

    logger.info(f"Extracting data to {table_ref} (env: {context['environment']})")

    # Simulate extraction window
    end_time = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=1)
    logger.info(f"Window: {start_time.date()} → {end_time.date()}")

    # Generate sample data
    data = [
        {"id": 1, "date": "2025-01-15", "region": "us-east", "category": "electronics", "orders": 142, "revenue": 28450.00},
        {"id": 2, "date": "2025-01-15", "region": "us-west", "category": "electronics", "orders": 98,  "revenue": 19600.00},
        {"id": 3, "date": "2025-01-15", "region": "us-east", "category": "clothing",    "orders": 215, "revenue": 10750.00},
        {"id": 4, "date": "2025-01-15", "region": "eu-west", "category": "electronics", "orders": 76,  "revenue": 15200.00},
        {"id": 5, "date": "2025-01-15", "region": "eu-west", "category": "clothing",    "orders": 134, "revenue":  6700.00},
    ]
    df = pd.DataFrame(data)

    # Write to BigQuery
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = bq.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    logger.info(f"Wrote {len(df)} rows to {table_ref}")
    return {"status": "success", "rows_written": len(df), "table": table_ref}
