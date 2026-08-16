"""
Morning Prep Script - Curated 40-Stock LITE Version
=======================================================
Purpose: a lighter-weight companion to the full ~208-stock F&O version,
built specifically for deployment under a SEPARATE GitHub account/repo,
targeting Streamlit Community Cloud's free-tier ~1GB RAM limit. Tracking
40 stocks instead of ~208 cuts the live scoring loop's per-cycle work by
roughly 80%, aiming to stay comfortably within that limit rather than
hitting whatever resource constraint caused the full version's snapshot
table to fail intermittently on cloud (root cause never fully pinned
down - this sidesteps it rather than continuing to chase it).

This is a STANDALONE file - it does not modify or depend on
morning_prep_banknifty.py at all, so the existing working full-universe
setup (local PC + its own GitHub repo/cloud deployment) is completely
unaffected by this file's existence.

What it does (identical pipeline to the full version, just a fixed stock
list instead of dynamic F&O discovery):
1. Resolves instrument keys for a hardcoded list of 40 curated, high-
   liquidity F&O stocks (large-caps across banking, IT, auto, pharma,
   FMCG, metals, energy - not auto-discovered, so the list only changes
   when you deliberately edit LIQUID_40_STOCKS below).
2. For each stock, pulls historical DAILY candles, detects swing
   highs/lows -> S/R zones, and computes EMA50/EMA200/RSI(14)/MACD trend
   indicators from the same candles.
3. For each stock, pulls 10 days of 5-min INTRADAY candles from the
   FUTURES contract to build the RVOL baseline curve.
4. Saves to banknifty_prep_<YYYY-MM-DD>.json (same filename/format as the
   full version, so streamlit_scanner_banknifty.py / live_scanner_
   banknifty.py work with this file completely unchanged - just copy
   those two scripts into the new repo alongside this one).

Notes specific to Upstox API v3 (from prior sessions):
- Historical candle endpoint URL order is to_date THEN from_date.
- Candle row = [timestamp, open, high, low, close, volume, oi] (7 elements).
- Always sort candles by timestamp explicitly, never assume order.
- Rate limit: 25 req/sec - stays under 18/sec via a shared rate limiter.
"""

import os
import json
import gzip
import time
import threading
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, "config.txt")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "prep_output")

INSTRUMENT_MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

# Curated list of 40 high-liquidity F&O stocks - pulled from a confirmed
# real F&O universe fetch (not guessed), covering banking, IT, auto,
# pharma, FMCG, metals, and energy large-caps. Edit this list directly if
# you want to swap any names - it's intentionally a fixed list, not
# auto-discovered, to keep this "lite" version's runtime and cloud
# resource footprint small and predictable.
LIQUID_40_STOCKS = [
    "RELIANCE", "HDFCBANK", "ICICIBANK", "SBIN", "KOTAKBANK",
    "AXISBANK", "INDUSINDBK", "BANKBARODA", "PNB", "INFY",
    "TECHM", "BHARTIARTL", "LT", "MARUTI", "SUNPHARMA",
    "BAJFINANCE", "HINDUNILVR", "ASIANPAINT", "TITAN", "ULTRACEMCO",
    "NTPC", "ADANIENT", "DRREDDY", "CIPLA", "DIVISLAB",
    "EICHERMOT", "HEROMOTOCO", "HINDALCO", "JINDALSTEL", "VEDL",
    "NMDC", "UPL", "BPCL", "HDFCLIFE", "SBILIFE",
    "DMART", "NESTLEIND", "BRITANNIA", "GODREJCP", "DLF",
]

MAX_WORKERS = 4              # concurrent stocks being processed at once
MAX_REQUESTS_PER_SEC = 18    # shared across all workers, stays under Upstox's 25/sec limit

DAILY_CANDLE_LOOKBACK_DAYS = 450   # ~15 months - S/R needs 6mo, but EMA200 needs
                                    # 200+ trading days to seed properly, so we
                                    # extend the same fetch to cover both.
INTRADAY_LOOKBACK_DAYS = 10        # for RVOL baseline curve


class RateLimiter:
    """Token-bucket style limiter shared across worker threads, so TOTAL
    request rate (not per-thread rate) stays under the API limit. Same
    pattern used in eod_performance_cash_market.py."""
    def __init__(self, max_per_sec):
        self.min_interval = 1.0 / max_per_sec
        self.lock = threading.Lock()
        self.last_call = 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_call = time.monotonic()


rate_limiter = RateLimiter(MAX_REQUESTS_PER_SEC)
INTRADAY_INTERVAL = "5minute"
SWING_WINDOW = 3                   # bars on each side to confirm a swing point
ZONE_CLUSTER_PCT = 0.15            # cluster S/R levels within 0.15% of each other


def load_config():
    """Reads access token from config.txt (key=value format) if present.
    Missing entirely is not fatal here - get_access_token() also checks
    environment variables (used by the GitHub Actions automated run,
    which has no config.txt available)."""
    config = {}
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                config[key.strip()] = val.strip()
    return config


def get_access_token(config):
    """Checks environment variables first (for the automated GitHub Actions
    run), then falls back to config.txt (for local runs) - so the exact
    same script works in both places without changes."""
    env_token = os.environ.get("ACCESS_TOKEN") or os.environ.get("UPSTOX_ACCESS_TOKEN")
    if env_token:
        return env_token
    token = config.get("ACCESS_TOKEN") or config.get("UPSTOX_ACCESS_TOKEN")
    if not token:
        raise ValueError(
            "No ACCESS_TOKEN found - set it in config.txt or as an "
            "ACCESS_TOKEN environment variable."
        )
    return token


def get_headers(access_token):
    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }


# ---------------------------------------------------------------------------
# STEP 1: Instrument master -> instrument keys for the curated 40 stocks
# ---------------------------------------------------------------------------
def fetch_instrument_keys():
    """Downloads the NSE instrument master fresh and resolves instrument
    keys ONLY for the fixed LIQUID_40_STOCKS list above (not a full F&O
    discovery scan) - each stock's NSE_EQ instrument_key (for cash-market
    S/R zones + trend indicators) and its nearest-expiry futures
    instrument_key (for live LTP/RVOL/OI)."""
    print("Fetching instrument master...")
    resp = requests.get(INSTRUMENT_MASTER_URL, timeout=30)
    resp.raise_for_status()

    raw = gzip.decompress(resp.content)
    instruments = json.loads(raw)

    wanted = set(LIQUID_40_STOCKS)
    equity_by_symbol = {}  # trading_symbol -> (name, instrument_key)
    fut_candidates = {}    # name -> list of (expiry, instrument_key, trading_symbol)

    for inst in instruments:
        segment = inst.get("segment")
        if segment == "NSE_EQ":
            trading_symbol = inst.get("trading_symbol")
            if trading_symbol in wanted:
                equity_by_symbol[trading_symbol] = (inst.get("name"), inst.get("instrument_key"))
        elif segment == "NSE_FO" and inst.get("instrument_type") == "FUT":
            name = inst.get("name")
            expiry = inst.get("expiry")
            if name and expiry:
                fut_candidates.setdefault(name, []).append(
                    (expiry, inst.get("instrument_key"), inst.get("trading_symbol"))
                )

    missing = wanted - set(equity_by_symbol.keys())
    if missing:
        print(f"WARNING: could not find instrument keys for: {missing}")

    symbol_map = {}
    for trading_symbol, (name, instrument_key) in equity_by_symbol.items():
        info = {"instrument_key": instrument_key, "name": name}
        candidates = fut_candidates.get(name, [])
        if candidates:
            candidates.sort(key=lambda c: c[0])  # soonest expiry first (current month)
            nearest = candidates[0]
            info["futures_instrument_key"] = nearest[1]
            info["futures_trading_symbol"] = nearest[2]
        else:
            info["futures_instrument_key"] = None
            info["futures_trading_symbol"] = None
        symbol_map[trading_symbol] = info

    no_fut = [s for s, i in symbol_map.items() if not i.get("futures_instrument_key")]
    print(f"Resolved {len(symbol_map)}/{len(wanted)} instrument keys.")
    if no_fut:
        print(f"WARNING: no futures contract matched for: {no_fut}")

    return symbol_map


# ---------------------------------------------------------------------------
# STEP 2: Historical daily candles -> swing S/R zones
# ---------------------------------------------------------------------------
def fetch_daily_candles(instrument_key, access_token, days_back=DAILY_CANDLE_LOOKBACK_DAYS):
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    # v3 historical candle endpoint - URL order is to_date THEN from_date
    url = (
        f"https://api.upstox.com/v3/historical-candle/"
        f"{instrument_key}/days/1/{to_date}/{from_date}"
    )
    rate_limiter.wait()
    resp = requests.get(url, headers=get_headers(access_token), timeout=20)
    resp.raise_for_status()
    data = resp.json()

    candles = data.get("data", {}).get("candles", [])
    # Each candle: [timestamp, open, high, low, close, volume, oi]
    # Never assume order - sort explicitly by timestamp.
    candles.sort(key=lambda c: c[0])
    return candles


def find_swing_points(candles, window=SWING_WINDOW):
    """Simple local-extrema swing detection on daily highs/lows."""
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    n = len(candles)

    swing_highs = []
    swing_lows = []

    for i in range(window, n - window):
        window_highs = highs[i - window: i + window + 1]
        window_lows = lows[i - window: i + window + 1]

        if highs[i] == max(window_highs):
            swing_highs.append(highs[i])
        if lows[i] == min(window_lows):
            swing_lows.append(lows[i])

    return swing_highs, swing_lows


def cluster_levels(levels, pct=ZONE_CLUSTER_PCT):
    """Groups nearby price levels into zones and returns each zone's
    average price plus a 'strength' count of how many times it was touched."""
    if not levels:
        return []

    levels = sorted(levels)
    zones = []
    current_zone = [levels[0]]

    for price in levels[1:]:
        zone_avg = sum(current_zone) / len(current_zone)
        if abs(price - zone_avg) / zone_avg * 100 <= pct:
            current_zone.append(price)
        else:
            zones.append(current_zone)
            current_zone = [price]
    zones.append(current_zone)

    result = [
        {"level": round(sum(z) / len(z), 2), "strength": len(z)}
        for z in zones
    ]
    # Strongest (most-touched) zones first
    result.sort(key=lambda x: x["strength"], reverse=True)
    return result


def compute_sr_zones(candles):
    """Takes already-fetched daily candles (avoids a second API call - see
    main(), which fetches candles once and passes them to both this and
    compute_trend_indicators)."""
    if len(candles) < (2 * SWING_WINDOW + 1):
        print(f"  Not enough daily candles ({len(candles)}) to detect swings.")
        return {"resistance_zones": [], "support_zones": [], "candle_count": len(candles)}

    swing_highs, swing_lows = find_swing_points(candles)
    resistance_zones = cluster_levels(swing_highs)
    support_zones = cluster_levels(swing_lows)

    return {
        "resistance_zones": resistance_zones,
        "support_zones": support_zones,
        "candle_count": len(candles),
    }


# ---------------------------------------------------------------------------
# STEP 2b: Trend indicators - EMA50/200, RSI(14), MACD(12,26,9)
# ---------------------------------------------------------------------------
# Pure-Python implementations (no numpy/pandas) to match the rest of this
# project. Computed once here from daily closes, not recomputed live tick by
# tick - these act as a slower "trend regime" filter that the live scanner
# combines with its faster intraday signals (S/R + RVOL + VWAP + OI +
# aggression), the same way OI and order-book aggression already work as
# confirming/conflicting weights on top of the core intraday pattern.
def ema_series(values, period):
    """Standard EMA: seeded with an SMA of the first `period` values, then
    smoothed forward. Returns a list the same length as `values`, with None
    for indices before the series has enough data to start."""
    if len(values) < period:
        return [None] * len(values)

    k = 2 / (period + 1)
    result = [None] * (period - 1)
    sma_seed = sum(values[:period]) / period
    result.append(sma_seed)

    prev = sma_seed
    for price in values[period:]:
        current = price * k + prev * (1 - k)
        result.append(current)
        prev = current

    return result


def rsi_series(closes, period=14):
    """Wilder's RSI. Returns a list the same length as `closes`, with None
    for indices before the series has enough data."""
    n = len(closes)
    if n < period + 1:
        return [None] * n

    result = [None] * period
    gains, losses = [], []
    for i in range(1, period + 1):
        change = closes[i] - closes[i - 1]
        gains.append(max(change, 0))
        losses.append(max(-change, 0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    rs = avg_gain / avg_loss if avg_loss != 0 else float("inf")
    result.append(100 - (100 / (1 + rs)))

    for i in range(period + 1, n):
        change = closes[i] - closes[i - 1]
        gain = max(change, 0)
        loss = max(-change, 0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        rs = avg_gain / avg_loss if avg_loss != 0 else float("inf")
        result.append(100 - (100 / (1 + rs)))

    return result


def macd_series(closes, fast=12, slow=26, signal=9):
    """Returns (macd_line, signal_line) as lists aligned with `closes`,
    each with None where not yet computable."""
    ema_fast = ema_series(closes, fast)
    ema_slow = ema_series(closes, slow)

    macd_line = [
        (f - s) if (f is not None and s is not None) else None
        for f, s in zip(ema_fast, ema_slow)
    ]

    # Signal line = EMA of the MACD line itself, only over the non-None tail
    valid_macd = [m for m in macd_line if m is not None]
    signal_valid = ema_series(valid_macd, signal)

    none_count = len(macd_line) - len(valid_macd)
    signal_line = [None] * none_count + signal_valid

    return macd_line, signal_line


def compute_trend_indicators(candles):
    """Computes EMA50/200 trend state, RSI(14) zone, and MACD state from
    daily closes. Returns None values gracefully if there isn't enough
    history yet (e.g. a recently-listed stock) rather than raising."""
    closes = [c[4] for c in candles]
    n = len(closes)

    indicators = {
        "ema50": None, "ema200": None, "ema_trend": None,
        "rsi14": None, "rsi_zone": None,
        "macd_line": None, "macd_signal": None, "macd_histogram": None, "macd_state": None,
    }

    if n < 2:
        return indicators

    # --- EMA50 / EMA200 ---
    ema50 = ema_series(closes, 50)
    ema200 = ema_series(closes, 200)
    if ema50[-1] is not None and ema200[-1] is not None:
        indicators["ema50"] = round(ema50[-1], 2)
        indicators["ema200"] = round(ema200[-1], 2)
        current_bullish = ema50[-1] > ema200[-1]
        prev_bullish = (
            ema50[-2] > ema200[-2]
            if len(ema50) >= 2 and ema50[-2] is not None and ema200[-2] is not None
            else current_bullish
        )
        if current_bullish and not prev_bullish:
            indicators["ema_trend"] = "golden_cross"   # fresh bullish crossover today
        elif not current_bullish and prev_bullish:
            indicators["ema_trend"] = "death_cross"    # fresh bearish crossover today
        else:
            indicators["ema_trend"] = "bullish" if current_bullish else "bearish"

    # --- RSI(14) ---
    rsi = rsi_series(closes, 14)
    if rsi[-1] is not None:
        rsi_val = round(rsi[-1], 1)
        indicators["rsi14"] = rsi_val
        if rsi_val >= 70:
            indicators["rsi_zone"] = "overbought"
        elif rsi_val <= 30:
            indicators["rsi_zone"] = "oversold"
        else:
            indicators["rsi_zone"] = "neutral"

    # --- MACD(12,26,9) ---
    macd_line, signal_line = macd_series(closes)
    if macd_line[-1] is not None and signal_line[-1] is not None:
        m, s = macd_line[-1], signal_line[-1]
        indicators["macd_line"] = round(m, 3)
        indicators["macd_signal"] = round(s, 3)
        indicators["macd_histogram"] = round(m - s, 3)
        prev_m = macd_line[-2] if len(macd_line) >= 2 else m
        prev_s = signal_line[-2] if len(signal_line) >= 2 else s
        current_bullish = m > s
        prev_bullish = (prev_m > prev_s) if (prev_m is not None and prev_s is not None) else current_bullish
        if current_bullish and not prev_bullish:
            indicators["macd_state"] = "bullish_cross"
        elif not current_bullish and prev_bullish:
            indicators["macd_state"] = "bearish_cross"
        else:
            indicators["macd_state"] = "bullish" if current_bullish else "bearish"

    return indicators


# ---------------------------------------------------------------------------
# STEP 3: Intraday candles -> RVOL baseline curve (cumulative volume by time)
# ---------------------------------------------------------------------------
def fetch_intraday_candles(instrument_key, access_token, days_back):
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    url = (
        f"https://api.upstox.com/v3/historical-candle/"
        f"{instrument_key}/minutes/5/{to_date}/{from_date}"
    )
    rate_limiter.wait()
    resp = requests.get(url, headers=get_headers(access_token), timeout=20)
    resp.raise_for_status()
    data = resp.json()

    candles = data.get("data", {}).get("candles", [])
    candles.sort(key=lambda c: c[0])
    return candles


def build_rvol_baseline(instrument_key, access_token, days_back=INTRADAY_LOOKBACK_DAYS):
    """Builds an average cumulative-volume-by-time-of-day curve using the
    last `days_back` trading sessions. Keys are HH:MM bucket strings
    (5-min buckets from 09:15 to 15:30), values are the average cumulative
    volume seen by that time of day across the lookback window.
    """
    candles = fetch_intraday_candles(instrument_key, access_token, days_back + 5)
    if not candles:
        return {}

    # Group candles by trading date
    by_date = {}
    for c in candles:
        ts = c[0]  # ISO timestamp string, e.g. "2026-08-14T09:15:00+05:30"
        date_str = ts[:10]
        by_date.setdefault(date_str, []).append(c)

    # Use only the most recent `days_back` sessions
    recent_dates = sorted(by_date.keys())[-days_back:]

    # For each session, build cumulative volume by time bucket (HH:MM)
    cumulative_by_date = {}
    for date_str in recent_dates:
        day_candles = sorted(by_date[date_str], key=lambda c: c[0])
        cum_vol = 0
        bucket_map = {}
        for c in day_candles:
            time_str = c[0][11:16]  # "HH:MM"
            cum_vol += c[5]  # volume is index 5
            bucket_map[time_str] = cum_vol
        cumulative_by_date[date_str] = bucket_map

    # Average across sessions for each time bucket
    all_buckets = sorted({t for bm in cumulative_by_date.values() for t in bm.keys()})
    baseline = {}
    for bucket in all_buckets:
        values = [
            cumulative_by_date[d][bucket]
            for d in cumulative_by_date
            if bucket in cumulative_by_date[d]
        ]
        if values:
            baseline[bucket] = round(sum(values) / len(values), 2)

    return baseline


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def process_symbol(symbol, info, access_token):
    """Runs the full prep pipeline (S/R zones, trend indicators, RVOL
    baseline, prev_close) for one instrument. Shared by both the 12 Bank
    Nifty stocks and the two index futures (NIFTY, BANKNIFTY), since the
    shape of `info` (instrument_key + futures_instrument_key) is identical
    for both. Returns the dict to store under result["stocks"][symbol]."""
    instrument_key = info["instrument_key"]
    print(f"\nProcessing {symbol} ({instrument_key})...")

    try:
        daily_candles = fetch_daily_candles(instrument_key, access_token)
        sr_data = compute_sr_zones(daily_candles)
        print(f"  Resistance zones: {len(sr_data['resistance_zones'])}, "
              f"Support zones: {len(sr_data['support_zones'])}")
    except Exception as e:
        print(f"  ERROR computing S/R zones: {e}")
        daily_candles = []
        sr_data = {"resistance_zones": [], "support_zones": [], "candle_count": 0}

    # Previous day's close on the CASH/INDEX market (fallback + reference
    # alongside the cash/index-based S/R zones and trend indicators).
    cash_prev_close = daily_candles[-1][4] if daily_candles else None

    # Previous day's close on the FUTURES contract specifically - since live
    # LTP comes from futures, change% needs to compare against the SAME
    # instrument's prior close, not cash/index's, to avoid a basis skew.
    futures_key = info.get("futures_instrument_key")
    futures_prev_close = None
    if futures_key:
        try:
            futures_candles = fetch_daily_candles(futures_key, access_token, days_back=10)
            if futures_candles:
                futures_prev_close = futures_candles[-1][4]
        except Exception as e:
            print(f"  Could not fetch futures prev close: {e}")

    prev_close = futures_prev_close if futures_prev_close is not None else cash_prev_close
    prev_close_source = "futures" if futures_prev_close is not None else "cash/index (fallback)"
    print(f"  Previous close: {prev_close} ({prev_close_source})")

    try:
        trend_indicators = compute_trend_indicators(daily_candles)
        print(f"  EMA trend: {trend_indicators['ema_trend']}, "
              f"RSI14: {trend_indicators['rsi14']} ({trend_indicators['rsi_zone']}), "
              f"MACD: {trend_indicators['macd_state']}")
    except Exception as e:
        print(f"  ERROR computing trend indicators: {e}")
        trend_indicators = {
            "ema50": None, "ema200": None, "ema_trend": None,
            "rsi14": None, "rsi_zone": None,
            "macd_line": None, "macd_signal": None, "macd_histogram": None, "macd_state": None,
        }

    # RVOL baseline sourced from FUTURES, not cash/index - futures activity
    # is a more meaningful signal for leveraged intraday moves. Falls back
    # to cash/index if the futures contract is too new to have enough
    # intraday history yet, or if no futures contract was resolved at all.
    try:
        if futures_key:
            rvol_baseline = build_rvol_baseline(futures_key, access_token)
            if len(rvol_baseline) < 20:  # too sparse to be a useful baseline
                print(f"  Futures RVOL baseline too sparse ({len(rvol_baseline)} buckets, "
                      f"likely a recently-rolled contract) - falling back to cash/index.")
                rvol_baseline = build_rvol_baseline(instrument_key, access_token)
            else:
                print(f"  RVOL baseline buckets (from futures): {len(rvol_baseline)}")
        else:
            print("  No futures contract resolved - RVOL baseline falling back to cash/index.")
            rvol_baseline = build_rvol_baseline(instrument_key, access_token)
    except Exception as e:
        print(f"  ERROR building RVOL baseline: {e}")
        rvol_baseline = {}

    print(f"  Futures contract: {info.get('futures_trading_symbol')} "
          f"({info.get('futures_instrument_key')})")

    return {
        "instrument_key": instrument_key,
        "futures_instrument_key": info.get("futures_instrument_key"),
        "futures_trading_symbol": info.get("futures_trading_symbol"),
        "resistance_zones": sr_data["resistance_zones"],
        "support_zones": sr_data["support_zones"],
        "daily_candle_count": sr_data["candle_count"],
        "rvol_baseline": rvol_baseline,
        "trend_indicators": trend_indicators,
        "prev_close": prev_close,
        "cash_prev_close": cash_prev_close,
    }


def main():
    config = load_config()
    access_token = get_access_token(config)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    symbol_map = fetch_instrument_keys()

    result = {
        "generated_at": datetime.now().isoformat(),
        "stocks": {},
    }

    print(f"\nProcessing {len(symbol_map)} stocks with {MAX_WORKERS} workers "
          f"(rate-limited to {MAX_REQUESTS_PER_SEC}/sec)...")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_symbol, symbol, info, access_token): symbol
            for symbol, info in symbol_map.items()
        }
        done_count = 0
        for future in as_completed(futures):
            symbol = futures[future]
            done_count += 1
            try:
                result["stocks"][symbol] = future.result()
            except Exception as e:
                print(f"  ERROR processing {symbol}: {e}")
            if done_count % 20 == 0:
                elapsed = round(time.time() - start_time)
                print(f"  ...{done_count}/{len(symbol_map)} done ({elapsed}s elapsed)")

    elapsed = round(time.time() - start_time)
    print(f"\nProcessed {len(result['stocks'])}/{len(symbol_map)} stocks successfully in {elapsed}s.")

    out_filename = f"banknifty_prep_{datetime.now().strftime('%Y-%m-%d')}.json"
    out_path = os.path.join(OUTPUT_DIR, out_filename)
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\nDone. Saved to: {out_path}")


if __name__ == "__main__":
    main()
