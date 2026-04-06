"""
Visualize Sales Function
Reads sales_summary from BigQuery and publishes a dashboard.
Returning a list publishes a multi-chart dashboard under the vizId in the pipeline YAML.
"""

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def main(bq, gcs, **context):
    """
    Read sales_summary and return viz specs to publish as a dashboard.

    Args:
        bq: google.cloud.bigquery.Client
        gcs: google.cloud.storage.Client
        **context: dataset_id, environment, execution_id, pipeline_name, task_name, project_id
    """
    dataset_id = context["dataset_id"]
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    df = bq.query(f"""
        SELECT region, SUM(total_revenue) AS revenue, SUM(total_orders) AS orders
        FROM `{dataset_id}.sales_summary`
        GROUP BY region
        ORDER BY revenue DESC
    """).to_dataframe()

    logger.info(f"Loaded {len(df)} regions from {dataset_id}.sales_summary")

    chart_data = [
        {"region": row.region, "revenue": round(float(row.revenue), 2), "orders": int(row.orders)}
        for row in df.itertuples()
    ]

    total_revenue = sum(r["revenue"] for r in chart_data)
    total_orders  = sum(r["orders"]  for r in chart_data)

    # Returning a list publishes a dashboard — each dict is one chart.
    return [
        {
            "type": "metric",
            "title": "Total Revenue",
            "value": total_revenue,
            "unit": "USD",
            "label": f"As of {today}",
        },
        {
            "type": "metric",
            "title": "Total Orders",
            "value": total_orders,
            "label": f"As of {today}",
        },
        {
            "type": "bar",
            "title": "Revenue by Region",
            "subtitle": f"As of {today} UTC",
            "data": chart_data,
            "xKey": "region",
            "series": [
                {"key": "revenue", "color": "#3b82f6", "label": "Revenue ($)"},
                {"key": "orders",  "color": "#10b981", "label": "Orders"},
            ],
        },
        {
            "type": "table",
            "title": "Region Breakdown",
            "columns": ["region", "revenue", "orders"],
            "data": chart_data,
        },
    ]
