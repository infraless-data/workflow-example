#!/usr/bin/env python3
"""
Visualize Sales Function
Reads transformed data from the lakehouse and publishes a sales dashboard
to a shareable public URL via the vizId configured in the pipeline YAML.

Returning a list publishes a multi-chart dashboard under a single vizId.
"""
import logging
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main(spark):
    """
    Reads sales_summary from the Iceberg lakehouse and returns a list of
    viz specs. The platform publishes all charts to one shareable URL
    because vizId is set in the pipeline YAML for this task.

    Args:
        spark: SparkSession with Iceberg catalog configured
    """
    logger.info("Building sales dashboard...")

    # Aggregate revenue and orders by region
    region_df = spark.sql("""
        SELECT
            region,
            SUM(total_revenue)  AS revenue,
            SUM(total_orders)   AS orders
        FROM analytics.sales_summary
        GROUP BY region
        ORDER BY revenue DESC
    """)

    rows = region_df.collect()
    logger.info(f"Loaded {len(rows)} regions from analytics.sales_summary")

    chart_data = [
        {
            "region": r.region,
            "revenue": round(float(r.revenue), 2),
            "orders": int(r.orders),
        }
        for r in rows
    ]

    total_revenue = sum(r["revenue"] for r in chart_data)
    total_orders  = sum(r["orders"]  for r in chart_data)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Returning a list publishes a dashboard — each dict is one chart.
    # The platform writes { "charts": [...] } to GCS under the single vizId.
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
            "subtitle": f"As of {today} UTC time",
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


if __name__ == "__main__":
    print("Use 'python dev.py run functions/visualize_sales.py' for local testing")
