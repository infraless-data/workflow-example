"""
Transform Sales Function
Reads raw transactions from BigQuery and writes aggregated sales summaries.
"""

import logging

logger = logging.getLogger(__name__)


def main(bq, gcs, **context):
    """
    Aggregate raw_transactions into sales_summary.

    Args:
        bq: google.cloud.bigquery.Client
        gcs: google.cloud.storage.Client
        **context: dataset_id, environment, execution_id, pipeline_name, task_name, project_id
    """
    from google.cloud import bigquery

    dataset_id = context["dataset_id"]

    logger.info(f"Transforming sales data in dataset {dataset_id}")

    # Read from the table written by extract
    query = f"""
        SELECT
            region,
            category AS product_category,
            SUM(orders)  AS total_orders,
            SUM(revenue) AS total_revenue,
            ROUND(SUM(revenue) / SUM(orders), 2) AS avg_order_value
        FROM `{dataset_id}.raw_transactions`
        GROUP BY region, category
    """
    df = bq.query(query).to_dataframe()
    logger.info(f"Aggregated {len(df)} region/category rows")

    # Write transformed data
    table_ref = f"{dataset_id}.sales_summary"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = bq.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    metrics = {
        "total_revenue": float(df["total_revenue"].sum()),
        "total_orders":  int(df["total_orders"].sum()),
        "unique_regions": int(df["region"].nunique()),
    }
    logger.info(f"Total revenue: ${metrics['total_revenue']:,.2f}")

    return {"status": "success", "rows_output": len(df), "metrics": metrics}
