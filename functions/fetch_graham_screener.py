import logging
import os
import requests
from datetime import date

logger = logging.getLogger(__name__)

# ── Test guard ─────────────────────────────────────────────────────────────────
# On the first test run we slice to TEST_STOCK_LIMIT stocks per exchange.
# Set to None to run the full universe.
TEST_STOCK_LIMIT = 200  # ~200 stocks per exchange for test run
# ─────────────────────────────────────────────────────────────────────────────

MIN_MARKET_CAP = 2_000_000_000  # $2B minimum market cap


def fetch_aa_yield(fred_api_key: str) -> tuple:
    """Fetch the latest ICE BofA AA US Corporate Index Effective Yield from FRED."""
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": "BAMLC0A2CAAEY",
        "api_key": fred_api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 5,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    observations = resp.json().get("observations", [])

    for obs in observations:
        if obs.get("value") not in (".", "", None):
            yield_pct = float(obs["value"])
            obs_date = obs["date"]
            logger.info(f"AA yield: {yield_pct}% as of {obs_date}")
            return yield_pct, obs_date

    raise ValueError("No valid AA yield observations found in FRED response")


def fetch_exchange_quotes(fmp_api_key: str, exchange: str) -> list:
    """
    Fetch all real-time quotes for a given exchange (NYSE or NASDAQ).
    Each record includes price, pe, marketCap, volume, beta, etc.
    This endpoint is available on the free FMP plan.
    """
    url = f"https://financialmodelingprep.com/api/v3/quotes/{exchange.lower()}"
    params = {"apikey": fmp_api_key}
    logger.info(f"Fetching all quotes for {exchange} ...")
    resp = requests.get(url, params=params, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and "Error Message" in data:
        raise ValueError(f"FMP API error on {exchange}: {data['Error Message']}")
    if not isinstance(data, list):
        raise ValueError(f"Unexpected response type for {exchange}: {type(data)}")
    logger.info(f"  → {len(data)} total quotes for {exchange}")
    return data


def main(spark):
    fred_api_key = os.environ.get("FRED_API_KEY")
    fmp_api_key = os.environ.get("FMP_API_KEY")

    if not fred_api_key:
        raise EnvironmentError("FRED_API_KEY environment variable is not set")
    if not fmp_api_key:
        raise EnvironmentError("FMP_API_KEY environment variable is not set")

    # ── Step 1: Fetch AA bond yield from FRED ────────────────────────────────
    aa_yield_pct, yield_date = fetch_aa_yield(fred_api_key)
    aa_yield_decimal = aa_yield_pct / 100.0
    max_pe = 1.0 / aa_yield_decimal
    logger.info(f"AA yield: {aa_yield_pct:.4f}% → Graham max P/E: {max_pe:.2f}")

    # ── Step 2: Fetch full exchange quote dumps from FMP (free endpoints) ────
    nyse_quotes   = fetch_exchange_quotes(fmp_api_key, "nyse")
    nasdaq_quotes = fetch_exchange_quotes(fmp_api_key, "nasdaq")

    all_quotes = nyse_quotes + nasdaq_quotes

    # Apply test limit if set
    if TEST_STOCK_LIMIT is not None:
        before = len(all_quotes)
        all_quotes = all_quotes[:TEST_STOCK_LIMIT]
        logger.info(
            f"TEST_MODE: sliced from {before} → {len(all_quotes)} stocks "
            f"(limit={TEST_STOCK_LIMIT})"
        )

    logger.info(f"Total quotes to evaluate: {len(all_quotes)}")

    # ── Step 3: Filter by Graham P/E criterion + market cap ──────────────────
    qualified = []
    skipped_no_pe       = 0
    skipped_negative_pe = 0
    skipped_high_pe     = 0
    skipped_small_cap   = 0

    for stock in all_quotes:
        # Market cap filter
        mkt_cap = stock.get("marketCap")
        try:
            mkt_cap = float(mkt_cap) if mkt_cap is not None else 0.0
        except (ValueError, TypeError):
            mkt_cap = 0.0
        if mkt_cap < MIN_MARKET_CAP:
            skipped_small_cap += 1
            continue

        # P/E filter
        pe = stock.get("pe")
        if pe is None:
            skipped_no_pe += 1
            continue
        try:
            pe = float(pe)
        except (ValueError, TypeError):
            skipped_no_pe += 1
            continue
        if pe <= 0:
            skipped_negative_pe += 1
            continue
        if pe >= max_pe:
            skipped_high_pe += 1
            continue

        qualified.append(stock)

    logger.info(
        f"Filter results: {len(qualified)} qualify | "
        f"{skipped_small_cap} small-cap | {skipped_no_pe} no P/E | "
        f"{skipped_negative_pe} negative P/E | {skipped_high_pe} P/E ≥ {max_pe:.2f}"
    )

    # ── Step 4: Build rows for Iceberg ───────────────────────────────────────
    run_date = date.today().isoformat()

    rows = []
    for s in qualified:
        # Determine exchange from the record itself (nyse/nasdaq mix)
        exch = str(s.get("exchange", "") or "")

        rows.append({
            "run_date":        run_date,
            "yield_date":      yield_date,
            "aa_yield_pct":    float(aa_yield_pct),
            "max_pe":          float(round(max_pe, 4)),
            "symbol":          str(s.get("symbol", "") or ""),
            "company_name":    str(s.get("name", "") or ""),
            "exchange":        exch,
            "price":           float(s.get("price") or 0.0),
            "pe_ratio":        float(s.get("pe") or 0.0),
            "market_cap":      float(s.get("marketCap") or 0.0),
            "volume":          float(s.get("volume") or 0.0),
            "avg_volume":      float(s.get("avgVolume") or 0.0),
            "year_high":       float(s.get("yearHigh") or 0.0),
            "year_low":        float(s.get("yearLow") or 0.0),
            "eps":             float(s.get("eps") or 0.0),
            "shares_outstanding": float(s.get("sharesOutstanding") or 0.0),
            "is_test_run":     TEST_STOCK_LIMIT is not None,
        })

    if not rows:
        logger.warning("No stocks passed the Graham filter — writing placeholder row")
        rows = [{
            "run_date": run_date, "yield_date": yield_date,
            "aa_yield_pct": float(aa_yield_pct), "max_pe": float(round(max_pe, 4)),
            "symbol": "", "company_name": "NO_RESULTS", "exchange": "",
            "price": 0.0, "pe_ratio": 0.0, "market_cap": 0.0,
            "volume": 0.0, "avg_volume": 0.0, "year_high": 0.0, "year_low": 0.0,
            "eps": 0.0, "shares_outstanding": 0.0, "is_test_run": True,
        }]

    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, BooleanType
    )

    schema = StructType([
        StructField("run_date",           StringType(),  True),
        StructField("yield_date",         StringType(),  True),
        StructField("aa_yield_pct",       DoubleType(),  True),
        StructField("max_pe",             DoubleType(),  True),
        StructField("symbol",             StringType(),  True),
        StructField("company_name",       StringType(),  True),
        StructField("exchange",           StringType(),  True),
        StructField("price",              DoubleType(),  True),
        StructField("pe_ratio",           DoubleType(),  True),
        StructField("market_cap",         DoubleType(),  True),
        StructField("volume",             DoubleType(),  True),
        StructField("avg_volume",         DoubleType(),  True),
        StructField("year_high",          DoubleType(),  True),
        StructField("year_low",           DoubleType(),  True),
        StructField("eps",                DoubleType(),  True),
        StructField("shares_outstanding", DoubleType(),  True),
        StructField("is_test_run",        BooleanType(), True),
    ])

    df = spark.createDataFrame(rows, schema=schema)
    df.writeTo("analytics.graham_screener_results").createOrReplace()

    count = df.count()
    logger.info(f"Wrote {count} qualifying stocks to analytics.graham_screener_results")
    logger.info(
        f"Summary — AA yield: {aa_yield_pct:.4f}% | Max P/E: {max_pe:.2f} | "
        f"Qualifying stocks: {len(qualified)} | Test limit: {TEST_STOCK_LIMIT}"
    )
