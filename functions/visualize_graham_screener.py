import logging
from datetime import date

logger = logging.getLogger(__name__)


def main(spark):
    """
    Read today's Graham screener results and publish a multi-chart dashboard.
    Returns a list of viz specs for the platform vizId publisher.
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
    aa_yield  = round(rows[0]["aa_yield_pct"], 4) if rows else 0.0
    max_pe    = round(rows[0]["max_pe"], 2)       if rows else 0.0
    is_test   = rows[0]["is_test_run"]            if rows else False

    # ── Chart 1: Metric cards ─────────────────────────────────────────────────
    metrics = [
        {"type": "metric", "title": "AA Bond Yield", "value": aa_yield, "unit": "%"},
        {"type": "metric", "title": "Graham Max P/E",  "value": max_pe},
        {"type": "metric", "title": "Qualifying Stocks", "value": total_count},
    ]

    # ── Chart 2: Qualifying stocks by sector (bar) ────────────────────────────
    from collections import Counter
    sector_counts = Counter(r["sector"] or "Unknown" for r in rows)
    sector_data = [
        {"sector": s, "count": c}
        for s, c in sorted(sector_counts.items(), key=lambda x: -x[1])
        if s  # skip blanks
    ]
    sector_bar = {
        "type":  "bar",
        "title": "Qualifying Stocks by Sector",
        "data":  sector_data,
        "xKey":  "sector",
        "series": [{"key": "count", "label": "# Stocks", "color": "#3b82f6"}],
    }

    # ── Chart 3: P/E distribution (bar histogram buckets) ────────────────────
    pe_buckets = Counter()
    for r in rows:
        pe = r["pe_ratio"]
        if pe and pe > 0:
            bucket = int(pe // 2) * 2  # 2-wide buckets: 0-2, 2-4, …
            label = f"{bucket}–{bucket+2}"
            pe_buckets[label] += 1

    pe_data = [
        {"bucket": k, "count": v}
        for k, v in sorted(pe_buckets.items(), key=lambda x: float(x[0].split("–")[0]))
    ]
    pe_bar = {
        "type":  "bar",
        "title": f"P/E Distribution (max {max_pe})",
        "data":  pe_data,
        "xKey":  "bucket",
        "series": [{"key": "count", "label": "# Stocks", "color": "#10b981"}],
    }

    # ── Chart 4: Top 30 by market cap — table ─────────────────────────────────
    sorted_rows = sorted(rows, key=lambda r: r["market_cap"] or 0, reverse=True)[:30]
    table_data = [
        {
            "symbol":        r["symbol"],
            "company":       r["company_name"],
            "sector":        r["sector"] or "",
            "exchange":      r["exchange"] or "",
            "price":         round(r["price"] or 0, 2),
            "pe_ratio":      round(r["pe_ratio"] or 0, 2),
            "market_cap_b":  round((r["market_cap"] or 0) / 1e9, 2),
            "beta":          round(r["beta"] or 0, 2),
            "div_yield_pct": round((r["dividend_yield"] or 0), 4),
        }
        for r in sorted_rows
    ]
    table_chart = {
        "type":    "table",
        "title":   f"Top 30 Graham-Qualifying Stocks by Market Cap {'(TEST — 1 page)' if is_test else ''}",
        "columns": ["symbol", "company", "sector", "exchange", "price",
                    "pe_ratio", "market_cap_b", "beta", "div_yield_pct"],
        "data":    table_data,
    }

    # ── Chart 5: Sector breakdown pie ────────────────────────────────────────
    pie_data = [{"label": k, "value": v} for k, v in sector_counts.most_common(10) if k]
    pie_chart = {
        "type":  "pie",
        "title": "Sector Mix (top 10)",
        "data":  pie_data,
    }

    dashboard = metrics + [sector_bar, pe_bar, pie_chart, table_chart]
    logger.info(f"Dashboard built: {len(dashboard)} panels")
    return dashboard
