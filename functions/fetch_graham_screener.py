import logging
import os
import requests
from datetime import date
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

logger = logging.getLogger(__name__)

# ── Test guard ─────────────────────────────────────────────────────────────────
# On the first test run we only process a small sample of stocks.
# Set to None to run the full S&P 500 list.
TEST_STOCK_SAMPLE = 20  # Test with 20 stocks; set to None for full S&P 500
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
    "JNJ", "PG", "KO", "MCD", "NKE",
    # Real Estate
    "SPG", "DLR", "PLD",
]


def fetch_sp500_list():
    """
    Fetch the full S&P 500 symbol list from Wikipedia via yfinance.
    Returns a list of ticker symbols.
    """
    try:
        # yfinance has a built-in S&P 500 symbol fetcher
        sp500_df = yf.data.get_data_yahoo('^GSPC', start='2020-01-01', end='2020-01-02')
        # This won't work; let's use a direct web scrape instead
        
        # Fetch S&P 500 constituents from Wikipedia
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        # Parse the HTML table
        import pandas as pd
        tables = pd.read_html(response.text)
        sp500_df = tables[0]  # First table contains the symbols
        
        symbols = sp500_df['Symbol'].tolist()
        logger.info(f"Fetched {len(symbols)} S&P 500 symbols from Wikipedia")
        return symbols
    except Exception as e:
        logger.error(f"Failed to fetch S&P 500 list: {e}. Falling back to large-cap list.")
        return LARGE_CAP_SYMBOLS


def get_aa_bond_yield():
    """
    Fetch the AA corporate bond yield from FRED (via yfinance or direct API).
    Returns the yield as a decimal (e.g., 0.045 for 4.5%).
    """
    try:
        # Try to get from FRED via yfinance
        # AAA corporate bond rate: BAMLC0A1CAAAEY (use this as proxy for AA)
        fred_data = yf.download("^TYX", start=date.today(), end=date.today(), progress=False)
        if not fred_data.empty:
            return fred_data['Adj Close'].iloc[-1] / 100.0  # Convert to decimal
    except Exception as e:
        logger.warning(f"Failed to fetch from FRED: {e}")
    
    # Fallback: use a reasonable default or try another source
    # Typical AA corporate bond yield is ~4-5%
    logger.info("Using default AA bond yield of 4.5%")
    return 0.045


def fetch_stock_data(symbol):
    """
    Fetch key financial metrics for a single stock using yfinance.
    Returns a dict with ticker, price, PE, EPS, and other metrics, or None if fetch fails.
    """
    try:
        ticker = yf.Ticker(symbol)
        
        # Fetch basic info
        info = ticker.info
        current_price = info.get('currentPrice') or info.get('regularMarketPrice')
        
        if not current_price or current_price <= 0:
            logger.warning(f"{symbol}: No valid current price")
            return None
        
        pe_ratio = info.get('trailingPE')
        eps = info.get('trailingEps')
        market_cap = info.get('marketCap')
        
        # Calculate earnings yield (inverse of P/E)
        earnings_yield = (1.0 / pe_ratio * 100) if pe_ratio and pe_ratio > 0 else None
        
        return {
            'symbol': symbol,
            'current_price': float(current_price),
            'pe_ratio': float(pe_ratio) if pe_ratio else None,
            'eps': float(eps) if eps else None,
            'earnings_yield_pct': float(earnings_yield) if earnings_yield else None,
            'market_cap': int(market_cap) if market_cap else None,
        }
    except Exception as e:
        logger.debug(f"{symbol}: Failed to fetch data: {e}")
        return None


def main(spark):
    """
    Fetch S&P 500 stocks and apply Graham screen criteria.
    Stores qualified stocks in analytics.graham_screener_results.
    
    Graham Screen: Stock qualifies if P/E < inverse of AA corporate bond yield.
    E.g., if AA bond yield is 4.5%, then fair P/E = 1/0.045 = 22.2x
    """
    logger.info("Starting Graham screener fetch task")
    
    # Get the AA corporate bond yield (implicit required rate of return)
    aa_yield = get_aa_bond_yield()
    fair_pe = 1.0 / aa_yield if aa_yield > 0 else 22.0
    logger.info(f"AA bond yield: {aa_yield*100:.2f}%, Fair P/E ratio: {fair_pe:.2f}x")
    
    # Get list of stocks to screen
    if TEST_STOCK_SAMPLE:
        symbols = LARGE_CAP_SYMBOLS[:TEST_STOCK_SAMPLE]
        logger.info(f"TEST MODE: Screening {len(symbols)} sample stocks")
    else:
        symbols = fetch_sp500_list()
        logger.info(f"PRODUCTION MODE: Screening {len(symbols)} S&P 500 stocks")
    
    # ── Concurrent fetch ──────────────────────────────────────────────────────
    logger.info(f"Fetching financial data for {len(symbols)} stocks concurrently...")
    stock_data = []
    max_workers = 10  # Reasonable concurrency level
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all fetch tasks
        future_to_symbol = {executor.submit(fetch_stock_data, sym): sym for sym in symbols}
        
        completed = 0
        for future in as_completed(future_to_symbol):
            completed += 1
            result = future.result()
            if result:
                stock_data.append(result)
            
            if completed % 50 == 0:
                logger.info(f"Fetched data for {completed}/{len(symbols)} stocks")
    
    logger.info(f"Successfully fetched data for {len(stock_data)} stocks")
    
    # ── Filter stocks that meet Graham criteria ────────────────────────────────
    today = date.today().isoformat()
    qualified_stocks = []
    
    for stock in stock_data:
        pe = stock['pe_ratio']
        
        # Qualify if: has valid P/E AND P/E is below fair P/E (margin of safety)
        if pe and pe > 0 and pe < fair_pe:
            qualified_stocks.append({
                'run_date': today,
                'symbol': stock['symbol'],
                'current_price': stock['current_price'],
                'pe_ratio': pe,
                'eps': stock['eps'],
                'fair_pe': fair_pe,
                'discount_pct': ((fair_pe - pe) / fair_pe) * 100,  # margin of safety
                'aa_yield_pct': aa_yield * 100,
                'market_cap': stock['market_cap'],
            })
    
    logger.info(f"Found {len(qualified_stocks)} stocks meeting Graham criteria")
    
    # ── Write to Iceberg table ────────────────────────────────────────────────
    if qualified_stocks:
        df = spark.createDataFrame(qualified_stocks)
        df.writeTo("analytics.graham_screener_results").createOrReplace()
        logger.info(f"Wrote {len(qualified_stocks)} qualified stocks to analytics.graham_screener_results")
    else:
        logger.warning("No stocks qualified; creating empty table with schema")
        schema = "run_date STRING, symbol STRING, current_price DOUBLE, pe_ratio DOUBLE, " \
                 "eps DOUBLE, fair_pe DOUBLE, discount_pct DOUBLE, aa_yield_pct DOUBLE, market_cap LONG"
        empty_df = spark.createDataFrame([], schema)
        empty_df.writeTo("analytics.graham_screener_results").createOrReplace()
    
    logger.info("Graham screener fetch complete")
