import logging
import os
import requests
from datetime import date
import yfinance as yf

logger = logging.getLogger(__name__)

# ── Test guard ─────────────────────────────────────────────────────────────────
# On the first test run we only process a small sample of large-cap stocks.
# Set to None to run the full list.
TEST_STOCK_SAMPLE = 20  # Test with 20 stocks (from a curated S&P 500 list)
# ─────────────────────────────────────────────────────────────────────────────

# A curated list of large-cap, liquid stocks across sectors for testing/demo
LARGE_CAP_SYMBOLS = [
    # Tech
    "AAPL", "MSFT", "GOOGL", "META", "NVDA", "TSLA", "AMD", "INTEL",
    # Finance
    "JPM", "BAC", "WFC", "GS", "BLK",
    # Healthcare
    "JNJ", "PFE", "UNH", "LLY", "AZN",
    # Industrials
    "BA", "CAT", "GE", "HON",
    # Energy
    "XOM", "CVX", "COP",
    # Consumer
    "WMT", "KO", "PG", "JNJ", "MCD",
    # Communications
    "VZ", "T", "CMCSA",
    # Utilities
    "NEE", "DUK",
    # Real Estate
    "AMT", "PLD",
    # Additional diversification
    "ABBV", "COST", "CSCO", "CRM", "IBM", "INTC", "MA", "V",
]


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


def fetch_stock_data(symbol: str) -> dict:
    """
    Fetch stock info and latest price via yfinance (free, no API key needed).
    Returns a dict with all available fields or None if fetch fails.
    """
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        
        # Extract key fields; yfinance returns None for missing fields
        return {
            "symbol": symbol,
            "name": info.get("longName") or info.get("shortName") or "",
            "sector": info.get("sector") or "",
            "industry": info.get("industry") or "",
            "marketCap": info.get("marketCap"),
            "price": info.get("currentPrice"),
            "trailingPE": info.get("trailingPE"),
            "forwardPE": info.get("forwardPE"),
            "beta": info.get("beta"),
            "dividendRate": info.get("dividendRate") or 0.0,
            "dividendYield": info.get("dividendYield") or 0.0,
            "volume": info.get("volume") or 0.0,
            "averageVolume": info.get("averageVolume") or 0.0,
            "fiftyTwoWeekHigh": info.get("fiftyTwoWeekHigh"),
            "fiftyTwoWeekLow": info.get("fiftyTwoWeekLow"),
            "trailingEps": info.get("trailingEps"),
        }
    except Exception as e:
        logger.warning(f"Failed to fetch yfinance data for {symbol}: {e}")
        return None


def main(spark):
    fred_api_key = os.environ.get("FRED_API_KEY")

    if not fred_api_key:
        raise EnvironmentError("FRED_API_KEY environment variable is not set")

    # ── Step 1: Fetch AA bond yield from FRED ────────────────────────────────
    aa_yield_pct, yield_date = fetch_aa_yield(fred_api_key)
    aa_yield_decimal = aa_yield_pct / 100.0
    max_pe = 1.0 / aa_yield_decimal
    logger.info(f"AA yield: {aa_yield_pct:.4f}% → Graham max P/E: {max_pe:.2f}")

    # ── Step 2: Fetch stock data via yfinance ────────────────────────────────
    symbols_to_check = LARGE_CAP_SYMBOLS
    if TEST_STOCK_SAMPLE is not None:
        symbols_to_check = LARGE_CAP_SYMBOLS[:TEST_STOCK_SAMPLE]
        logger.info(f"TEST_MODE: checking {len(symbols_to_check)} stocks")
    else:
        logger.info(f"Full mode: checking {len(symbols_to_check)} stocks")

    all_stocks = []
    for sym in symbols_to_check:
        logger.info(f"Fetching {sym} ...")
        data = fetch_stock_data(sym)
        if data:
            all_stocks.append(data)
    
    logger.info(f"Successfully fetched data for {len(all_stocks)} stocks")

    # ── Step 3: Filter by Graham P/E criterion ────────────────────────────────
    qualified = []
    skipped_no_pe       = 0
    skipped_negative_pe = 0
    skipped_high_pe     = 0
    skipped_small_cap   = 0
    skipped_no_price    = 0

    for stock in all_stocks:
        # Market cap filter (if available)
        mkt_cap = stock.get("marketCap")
        if mkt_cap is None:
            skipped_small_cap += 1
            logger.debug(f"{stock['symbol']}: skipped (no market cap)")
            continue
        if float(mkt_cap) < 2_000_000_000:  # $2B minimum
            skipped_small_cap += 1
            continue

        # Price filter
        price = stock.get("price")
        if price is None or price == 0:
            skipped_no_price += 1
            continue

        # P/E filter (use trailing P/E if available, else forward)
        pe = stock.get("trailingPE") or stock.get("forwardPE")
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
            logger.debug(f"{stock['symbol']}: P/E {pe:.2f} ≥ {max_pe:.2f} (skipped)")
            continue

        qualified.append(stock)

    logger.info(
        f"Filter results: {len(qualified)} qualify | "
        f"{skipped_small_cap} small-cap | {skipped_no_price} no price | "
        f"{skipped_no_pe} no P/E | {skipped_negative_pe} negative P/E | "
        f"{skipped_high_pe} P/E ≥ {max_pe:.2f}"
    )

    # ── Step 4: Build rows for Iceberg ───────────────────────────────────────
    run_date = date.today().isoformat()

    rows = []
    for s in qualified:
        pe = float(s.get("trailingPE") or s.get("forwardPE") or 0.0)
        price = float(s.get("price") or 0.0)
        mkt_cap = float(s.get("marketCap") or 0.0)
        div_yield_pct = float(s.get("dividendYield") or 0.0) * 100.0  # yfinance returns as decimal
        beta = float(s.get("beta") or 0.0)

        rows.append({
            "run_date":       run_date,
            "yield_date":     yield_date,
            "aa_yield_pct":   float(aa_yield_pct),
            "max_pe":         float(round(max_pe, 4)),
            "symbol":         str(s.get("symbol", "") or ""),
            "company_name":   str(s.get("name", "") or ""),
            "sector":         str(s.get("sector", "") or ""),
            "industry":       str(s.get("industry", "") or ""),
            "price":          price,
            "pe_ratio":       pe,
            "market_cap":     mkt_cap,
            "volume":         float(s.get("volume") or 0.0),
            "avg_volume":     float(s.get("averageVolume") or 0.0),
            "year_high":      float(s.get("fiftyTwoWeekHigh") or 0.0),
            "year_low":       float(s.get("fiftyTwoWeekLow") or 0.0),
            "beta":           beta,
            "dividend_yield": div_yield_pct,
            "is_test_run":    TEST_STOCK_SAMPLE is not None,
        })

    if not rows:
        logger.warning("No stocks passed the Graham filter — writing placeholder row")
        rows = [{
            "run_date": run_date, "yield_date": yield_date,
            "aa_yield_pct": float(aa_yield_pct), "max_pe": float(round(max_pe, 4)),
            "symbol": "", "company_name": "NO_RESULTS", "sector": "", "industry": "",
            "price": 0.0, "pe_ratio": 0.0, "market_cap": 0.0,
            "volume": 0.0, "avg_volume": 0.0, "year_high": 0.0, "year_low": 0.0,
            "beta": 0.0, "dividend_yield": 0.0, "is_test_run": True,
        }]

    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, BooleanType
    )

    schema = StructType([
        StructField("run_date",       StringType(),  True),
        StructField("yield_date",     StringType(),  True),
        StructField("aa_yield_pct",   DoubleType(),  True),
        StructField("max_pe",         DoubleType(),  True),
        StructField("symbol",         StringType(),  True),
        StructField("company_name",   StringType(),  True),
        StructField("sector",         StringType(),  True),
        StructField("industry",       StringType(),  True),
        StructField("price",          DoubleType(),  True),
        StructField("pe_ratio",       DoubleType(),  True),
        StructField("market_cap",     DoubleType(),  True),
        StructField("volume",         DoubleType(),  True),
        StructField("avg_volume",     DoubleType(),  True),
        StructField("year_high",      DoubleType(),  True),
        StructField("year_low",       DoubleType(),  True),
        StructField("beta",           DoubleType(),  True),
        StructField("dividend_yield", DoubleType(),  True),
        StructField("is_test_run",    BooleanType(), True),
    ])

    df = spark.createDataFrame(rows, schema=schema)
    df.writeTo("analytics.graham_screener_results").createOrReplace()

    count = df.count()
    logger.info(f"Wrote {count} qualifying stocks to analytics.graham_screener_results")
    logger.info(
        f"Summary — AA yield: {aa_yield_pct:.4f}% | Max P/E: {max_pe:.2f} | "
        f"Qualifying stocks: {len(qualified)} | Test sample: {TEST_STOCK_SAMPLE}"
    )
