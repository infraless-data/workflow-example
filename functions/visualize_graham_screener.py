import logging
from datetime import date
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


def get_company_name(symbol):
    """Fetch company name for a given symbol."""
    try:
        ticker = yf.Ticker(symbol)
        name = ticker.info.get('longName') or ticker.info.get('shortName') or symbol
        return name
    except Exception as e:
        logger.debug(f"Failed to fetch name for {symbol}: {e}")
        return symbol


def fetch_company_names(symbols):
    """Concurrently fetch company names for a list of symbols."""
    names = {}
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_symbol = {executor.submit(get_company_name, sym): sym for sym in symbols}
        
        for future in future_to_symbol:
            symbol = future_to_symbol[future]
            try:
                name = future.result()
                names[symbol] = name
            except Exception as e:
                logger.warning(f"Failed to get name for {symbol}: {e}")
                names[symbol] = symbol
    
    return names


def main(spark):
    """
    Read today's Graham screener results and publish a multi-chart dashboard.
    Returns a list of viz specs for the platform vizId publisher.
    Table of qualified stocks is now at the top for immediate visibility.
    """
    df = spark.table("analytics.graham_screener_results")

    # Filter to today's run only
    today = date.today().isoformat()
    df_today = df.filter(df.run_date == today)

    total_count = df_today.count()
    logger.info(f"Visualizing {total_count} qualifying stocks for {today}")

    if total_count == 0:
        logger.warning("No stocks to visualize for today")
        return [{
            "type": "metric",
            "title": "Graham-Qualifying Stocks Today",
            "value": 0
        }]

    rows = df_today.collect()

    # ── Fetch company names concurrently ──────────────────────────────────────
    symbols = [row["symbol"] for row in rows]
    logger.info(f"Fetching company names for {len(symbols)} stocks...")
    company_names = fetch_company_names(symbols)

    # ── Pull scalar metrics from first row ───────────────────────────────────
    aa_yield = round(rows[0]["aa_yield_pct"], 4) if rows else 4.5
    fair_pe = round(rows[0]["fair_pe"], 2) if rows else 22.2

    # ── Build table data (sorted by discount %) ──────────────────────────────
    table_data = []
    for row in rows:
        table_data.append({
            "symbol": row["symbol"],
            "name": company_names.get(row["symbol"], row["symbol"]),
            "price": round(float(row["current_price"]), 2),
            "p_e": round(float(row["pe_ratio"]), 2),
            "eps": round(float(row["eps"]), 2) if row["eps"] else None,
            "margin_of_safety_pct": round(float(row["discount_pct"]), 1),
            "market_cap_b": round(float(row["market_cap"]) / 1e9, 2) if row["market_cap"] else None,
        })

    # Sort by margin of safety (highest first)
    table_data = sorted(table_data, key=lambda x: x["margin_of_safety_pct"], reverse=True)

    # ── Build chart data (top 10 by margin of safety with fair_pe included) ──
    chart_data = []
    for item in table_data[:10]:
        chart_data.append({
            "symbol": item["symbol"],
            "p_e": item["p_e"],
            "fair_pe": fair_pe,
            "margin_of_safety_pct": item["margin_of_safety_pct"],
            "price": item["price"],
        })

    # ── Visualization dashboard (TABLE FIRST) ────────────────────────────────
    viz_specs = [
        # 1. QUALIFIED STOCKS TABLE (TOP) — Main insight
        {
            "type": "table",
            "title": f"Graham-Qualified Stocks ({total_count} found) — Sorted by Margin of Safety",
            "columns": ["symbol", "name", "price", "p_e", "eps", "margin_of_safety_pct", "market_cap_b"],
            "data": table_data,
        },

        # 2. Key metrics row
        {
            "type": "metric",
            "title": "Total Graham Qualifiers",
            "value": total_count,
        },
        {
            "type": "metric",
            "title": "Fair P/E (AA Yield Implied)",
            "value": fair_pe,
        },
        {
            "type": "metric",
            "title": "AA Corporate Bond Yield",
            "value": aa_yield,
            "unit": "%",
        },

        # 3. Top 10 by margin of safety
        {
            "type": "bar",
            "title": "Top 10 Graham Bargains (by Margin of Safety %)",
            "data": chart_data,
            "xKey": "symbol",
            "series": [
                {
                    "key": "margin_of_safety_pct",
                    "label": "Margin of Safety (%)",
                    "color": "#10b981",  # Green
                }
            ],
        },

        # 4. P/E comparison to fair value
        {
            "type": "bar",
            "title": f"P/E Ratio vs. Fair Value (Fair P/E = {fair_pe}x)",
            "data": chart_data,
            "xKey": "symbol",
            "series": [
                {
                    "key": "p_e",
                    "label": "Current P/E",
                    "color": "#3b82f6",  # Blue
                },
                {
                    "key": "fair_pe",
                    "label": "Fair P/E",
                    "color": "#ef4444",  # Red (reference line)
                }
            ],
        },
    ]

    logger.info(f"Generated dashboard with {len(viz_specs)} visualizations")
    return viz_specs
