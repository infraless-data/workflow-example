import logging
from datetime import date

logger = logging.getLogger(__name__)


def main(spark):
    """
    Read today's Graham screener results and publish a multi-chart dashboard.
    Returns a list of viz specs for the platform vizId publisher.
    Qualified stocks table appears first.
    """
    df = spark.table("analytics.graham_screener_results")

    # Filter to today's run only
    today = date.today().isoformat()
    df_today = df.filter(df.run_date == today)

    total_count = df_today.count()
    logger.info(f"Visualizing {total_count} qualifying stocks for {today}")

    if total_count == 0:
        logger.warning("No stocks to visualize for today")
        return [{"type": "metric", "title": "Graham-Qualifying Stocks Today", "value": 0}]

    rows = df_today.collect()

    # ── Pull scalar metrics from first row ───────────────────────────────────
    aa_yield  = round(rows[0]["aa_yield_pct"], 4) if rows[0]["aa_yield_pct"] else 0
    fair_pe   = round(rows[0]["fair_pe"], 2) if rows[0]["fair_pe"] else 0

    # ── Build qualified stocks table (sorted by discount) ──────────────────────
    # Convert to dicts, sorted by discount_pct descending (most undervalued first)
    stocks_table = [
        {
            "symbol": r["symbol"],
            "price": round(float(r["current_price"]), 2),
            "pe": round(float(r["pe_ratio"]), 2),
            "fair_pe": round(float(r["fair_pe"]), 2),
            "discount": round(float(r["discount_pct"]), 1),  # margin of safety %
            "market_cap_b": round(float(r["market_cap"]) / 1e9, 1) if r["market_cap"] else None,
        }
        for r in rows
    ]
    stocks_table.sort(key=lambda x: x["discount"], reverse=True)

    # ── Build bar chart: P/E vs Fair P/E ──────────────────────────────────────
    chart_data = [
        {
            "symbol": s["symbol"],
            "current_pe": s["pe"],
            "fair_pe": s["fair_pe"],
        }
        for s in stocks_table[:20]  # Top 20 most undervalued
    ]

    # ── Build market cap distribution ────────────────────────────────────────
    mcap_data = [
        {
            "symbol": s["symbol"],
            "market_cap_b": s["market_cap_b"] if s["market_cap_b"] else 0,
        }
        for s in stocks_table
        if s["market_cap_b"]  # Only include if we have market cap data
    ]
    mcap_data.sort(key=lambda x: x["market_cap_b"], reverse=True)
    mcap_data = mcap_data[:15]  # Top 15 by market cap

    # ── Build visualizations in order ─────────────────────────────────────────
    vizs = []

    # 1️⃣ QUALIFIED STOCKS TABLE (TOP) ───────────────────────────────────────────
    vizs.append({
        "type": "table",
        "title": f"Graham-Qualified Stocks ({total_count} found)",
        "description": f"Stocks with P/E below {fair_pe}x (AA yield: {aa_yield}%)",
        "columns": ["symbol", "price", "pe", "fair_pe", "discount", "market_cap_b"],
        "data": stocks_table,
    })

    # 2️⃣ Key Metrics ───────────────────────────────────────────────────────────
    vizs.append({
        "type": "metric",
        "title": "Total Graham-Qualified Stocks",
        "value": total_count,
        "unit": "stocks",
    })

    vizs.append({
        "type": "metric",
        "title": "Fair P/E Ratio",
        "value": fair_pe,
        "unit": "x",
    })

    vizs.append({
        "type": "metric",
        "title": "AA Corporate Bond Yield",
        "value": aa_yield,
        "unit": "%",
    })

    # 3️⃣ P/E Comparison Chart ──────────────────────────────────────────────────
    if chart_data:
        vizs.append({
            "type": "bar",
            "title": "P/E vs Fair P/E (Top 20 Undervalued)",
            "data": chart_data,
            "xKey": "symbol",
            "series": [
                {"key": "current_pe", "label": "Current P/E", "color": "#ef4444"},
                {"key": "fair_pe", "label": "Fair P/E", "color": "#10b981"},
            ],
        })

    # 4️⃣ Market Cap Distribution ──────────────────────────────────────────────
    if mcap_data:
        vizs.append({
            "type": "bar",
            "title": "Market Cap Distribution (Top 15 by size)",
            "data": mcap_data,
            "xKey": "symbol",
            "series": [
                {"key": "market_cap_b", "label": "Market Cap (B)", "color": "#3b82f6"},
            ],
        })

    logger.info(f"Built dashboard with {len(vizs)} visualizations")
    return vizs
