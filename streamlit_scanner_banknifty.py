"""
Streamlit Live Scanner Dashboard - Bank Nifty Stocks
=======================================================
A visual dashboard version of live_scanner_banknifty.py - same S/R + RVOL +
VWAP pattern + OI + aggression scoring logic, displayed as auto-refreshing
tables instead of console output.

RUN THIS WITH (not plain python!):
    streamlit run streamlit_scanner_banknifty.py

Install deps (one-time, in addition to the console scanner's deps):
    pip install streamlit

IMPORTANT - how this differs from the console version:
Streamlit re-runs this entire script from top to bottom on every refresh.
The WebSocket connection and background scoring must NOT restart on every
rerun. A plain module-level variable would get silently recreated every
rerun (that was an early bug in this file - status got stuck on "starting"
forever because a fresh background thread kept spawning every 5 seconds
and never survived long enough to connect). The fix is st.cache_resource,
Streamlit's built-in mechanism for objects that must be created exactly
once and persist across reruns and browser sessions - the UI simply reads
the shared state on each rerun and redraws the tables; it never touches
the network itself.

Auto-refresh is done with a simple sleep + st.rerun() loop at the bottom
of the script (no extra package needed).

Same setup requirements as the console scanner:
- MarketDataFeedV3_pb2.py must exist in this folder (see
  live_scanner_banknifty.py's docstring for how to generate it)
- config.txt with ACCESS_TOKEN or UPSTOX_ACCESS_TOKEN
- prep_output/banknifty_prep_<date>.json from morning_prep_banknifty.py
"""

import os
import json
import csv
import glob
import ssl
import sys
import time
import threading
from datetime import datetime, time as dtime

import certifi
import requests
import websocket
import streamlit as st
import pandas as pd

try:
    import MarketDataFeedV3_pb2 as pb
except ImportError:
    pb = None

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, "config.txt")
PREP_DIR = os.path.join(SCRIPT_DIR, "prep_output")
EOD_LOG_DIR = os.path.join(SCRIPT_DIR, "eod_logs")

AUTHORIZE_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"

NEAR_ZONE_PCT = 0.75
RVOL_THRESHOLD = 1.5
MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)
UI_REFRESH_SEC = 5          # how often the browser table redraws
SCORING_INTERVAL_SEC = 60   # how often candidates are recomputed + logged

CANDIDATE_FIELDNAMES = [
    "timestamp", "symbol", "rvol", "pattern", "bias", "ltp", "zone_level",
    "distance_pct", "vwap", "zone_strength", "oi_signal", "aggression",
    "trend_signal", "score",
]

# ---------------------------------------------------------------------------
# SHARED STATE (must survive every Streamlit rerun)
# ---------------------------------------------------------------------------
# IMPORTANT: Streamlit re-executes this entire script top-to-bottom on every
# rerun (including the auto-refresh loop at the bottom). A plain module-level
# variable like `_app = {...}` would get silently RECREATED every single
# rerun, wiping out the "started" flag and respawning a brand-new background
# thread every few seconds - which is exactly the bug that caused the status
# to get stuck on "starting" forever. st.cache_resource is Streamlit's actual
# mechanism for objects that must be created once and persist across reruns
# (and across browser sessions) - the function body below only ever executes
# on the very first call; every later call just returns the same cached dict.
@st.cache_resource
def get_app_state():
    return {
        "lock": threading.Lock(),
        "started": False,
        "states": {},              # symbol -> SymbolState
        "last_candidates": [],
        "last_scored_at": None,
        "status": "not started",
        "error": None,
        "current_token": None,     # token currently in use by the running thread
        "generation": 0,           # bumped on each (re)connect - lets an old
                                    # background thread notice it's stale and
                                    # stop, so switching tokens doesn't leave
                                    # two threads running at once
        "ws_app": None,            # reference to the live WebSocketApp, so a
                                    # reconnect can cleanly close the old one
    }


_app = get_app_state()
_app_lock = _app["lock"]


def load_config():
    """Loads config.txt if present. On Streamlit Community Cloud there is
    no local config.txt (secrets are used instead - see get_access_token),
    so a missing file here is not fatal."""
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
    """Checks st.secrets first (for cloud deployment via Streamlit
    Community Cloud's Secrets manager), then falls back to config.txt
    (for local runs) - so the exact same file works in both places."""
    try:
        if "ACCESS_TOKEN" in st.secrets:
            return st.secrets["ACCESS_TOKEN"]
        if "UPSTOX_ACCESS_TOKEN" in st.secrets:
            return st.secrets["UPSTOX_ACCESS_TOKEN"]
    except Exception:
        pass  # st.secrets raises if no secrets.toml exists at all - fine locally
    return config.get("ACCESS_TOKEN") or config.get("UPSTOX_ACCESS_TOKEN")


def load_today_prep():
    pattern = os.path.join(PREP_DIR, "banknifty_prep_*.json")
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(
            f"No prep files found in {PREP_DIR}. Run morning_prep_banknifty.py first."
        )
    with open(files[-1], "r") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# SAME SCORING LOGIC AS THE CONSOLE SCANNER
# ---------------------------------------------------------------------------
class SymbolState:
    def __init__(self, symbol, instrument_key, futures_instrument_key, sr_data, rvol_baseline, trend_indicators=None, prev_close=None):
        self.symbol = symbol
        self.instrument_key = instrument_key
        self.futures_instrument_key = futures_instrument_key
        self.resistance_zones = sr_data.get("resistance_zones", [])
        self.support_zones = sr_data.get("support_zones", [])
        self.rvol_baseline = rvol_baseline
        self.trend_indicators = trend_indicators or {}
        self.prev_close = prev_close  # previous trading day's close, from prep JSON

        self.last_price = None
        self.atp = None
        self.cum_volume = 0
        self.oi_open = None
        self.oi_current = None
        self.tbq = None
        self.tsq = None
        self.lock = threading.Lock()

    def change_pct(self):
        """% change of live price vs previous day's close. None if either
        value isn't available yet."""
        with self.lock:
            if self.last_price is None or not self.prev_close:
                return None
            return (self.last_price - self.prev_close) / self.prev_close * 100

    def update(self, ltp, atp, total_traded_volume, tbq=None, tsq=None):
        with self.lock:
            self.last_price = ltp
            if atp:
                self.atp = atp
            if total_traded_volume and total_traded_volume > self.cum_volume:
                self.cum_volume = total_traded_volume
            if tbq is not None:
                self.tbq = tbq
            if tsq is not None:
                self.tsq = tsq

    def aggression_ratio(self):
        with self.lock:
            if not self.tbq or not self.tsq:
                return None
            return self.tbq / self.tsq

    def aggression_label(self):
        ratio = self.aggression_ratio()
        if ratio is None:
            return None
        if ratio > 1.3:
            return "buying_aggression"
        if ratio < (1 / 1.3):
            return "selling_aggression"
        return "balanced"

    def update_oi(self, oi):
        if not oi:
            return
        with self.lock:
            if self.oi_open is None:
                self.oi_open = oi
            self.oi_current = oi

    def oi_direction(self):
        with self.lock:
            if self.oi_open is None or self.oi_current is None:
                return None
            change_pct = (self.oi_current - self.oi_open) / self.oi_open * 100
            if change_pct > 0.5:
                return "up"
            elif change_pct < -0.5:
                return "down"
            return "flat"

    def oi_classification(self, price_up):
        direction = self.oi_direction()
        if direction is None:
            return None
        if price_up and direction == "up":
            return "fresh_long"
        if price_up and direction == "down":
            return "short_covering"
        if not price_up and direction == "up":
            return "fresh_short"
        if not price_up and direction == "down":
            return "long_unwinding"
        return "neutral"

    def vwap(self):
        with self.lock:
            return self.atp

    def rvol(self):
        now_str = datetime.now().strftime("%H:%M")
        buckets = sorted(self.rvol_baseline.keys())
        matched = None
        for b in buckets:
            if b <= now_str:
                matched = b
            else:
                break
        if matched is None or self.rvol_baseline.get(matched, 0) == 0:
            return None
        with self.lock:
            return self.cum_volume / self.rvol_baseline[matched]

    def nearest_zone(self):
        if self.last_price is None:
            return None
        candidates = []
        for z in self.resistance_zones:
            dist_pct = (z["level"] - self.last_price) / self.last_price * 100
            candidates.append(("resistance", z["level"], dist_pct, z["strength"]))
        for z in self.support_zones:
            dist_pct = (self.last_price - z["level"]) / self.last_price * 100
            candidates.append(("support", z["level"], dist_pct, z["strength"]))
        valid = [c for c in candidates if c[2] >= 0]
        if not valid:
            return None
        valid.sort(key=lambda c: c[2])
        return valid[0]

    def trend_weight(self, bias):
        """Daily EMA50/200 + RSI + MACD trend regime, computed once at prep
        time - agreeing with the intraday pattern's bias boosts the score,
        fighting the daily trend discounts it. See the console scanner's
        SymbolState.trend_weight() for the identical logic/rationale."""
        ti = self.trend_indicators
        weight = 1.0
        reasons = []

        ema_trend = ti.get("ema_trend")
        if ema_trend in ("bullish", "golden_cross"):
            weight *= 1.15 if bias == "bullish" else 0.8
            reasons.append(f"ema:{ema_trend}")
        elif ema_trend in ("bearish", "death_cross"):
            weight *= 1.15 if bias == "bearish" else 0.8
            reasons.append(f"ema:{ema_trend}")

        rsi_zone = ti.get("rsi_zone")
        if rsi_zone == "overbought":
            weight *= 0.85 if bias == "bullish" else 1.1
            reasons.append("rsi:overbought")
        elif rsi_zone == "oversold":
            weight *= 0.85 if bias == "bearish" else 1.1
            reasons.append("rsi:oversold")

        macd_state = ti.get("macd_state")
        if macd_state in ("bullish", "bullish_cross"):
            weight *= 1.1 if bias == "bullish" else 0.85
            reasons.append(f"macd:{macd_state}")
        elif macd_state in ("bearish", "bearish_cross"):
            weight *= 1.1 if bias == "bearish" else 0.85
            reasons.append(f"macd:{macd_state}")

        return round(weight, 3), ",".join(reasons) if reasons else "no_data"


def evaluate(states):
    results = []
    for sym, state in states.items():
        if state.last_price is None:
            continue
        zone = state.nearest_zone()
        if zone is None:
            continue
        zone_type, level, dist_pct, strength = zone
        if dist_pct > NEAR_ZONE_PCT:
            continue
        rvol = state.rvol()
        if rvol is None or rvol < RVOL_THRESHOLD:
            continue
        vwap = state.vwap()
        if vwap is None:
            continue

        price_above_vwap = state.last_price > vwap
        if zone_type == "support" and price_above_vwap:
            pattern, bias = "support_bounce", "bullish"
        elif zone_type == "support" and not price_above_vwap:
            pattern, bias = "support_breakdown_warning", "bearish"
        elif zone_type == "resistance" and price_above_vwap:
            pattern, bias = "resistance_breakout", "bullish"
        else:
            pattern, bias = "resistance_rejection_warning", "bearish"

        oi_class = state.oi_classification(price_above_vwap)
        oi_weight = {
            "fresh_long": 1.3, "fresh_short": 1.3,
            "short_covering": 0.8, "long_unwinding": 0.8,
            "neutral": 1.0, None: 1.0,
        }.get(oi_class, 1.0)

        aggression = state.aggression_label()
        if aggression == "buying_aggression":
            aggression_weight = 1.2 if bias == "bullish" else 0.75
        elif aggression == "selling_aggression":
            aggression_weight = 1.2 if bias == "bearish" else 0.75
        else:
            aggression_weight = 1.0

        trend_wt, trend_reason = state.trend_weight(bias)

        score = (1 / max(dist_pct, 0.01)) * rvol * (1 + strength * 0.1) * oi_weight * aggression_weight * trend_wt
        results.append({
            "symbol": sym, "pattern": pattern, "bias": bias,
            "ltp": round(state.last_price, 2), "zone_level": level,
            "distance_pct": round(dist_pct, 2), "rvol": round(rvol, 2),
            "vwap": round(vwap, 2), "zone_strength": strength,
            "oi_signal": oi_class or "no_data",
            "aggression": aggression or "no_data",
            "trend_signal": trend_reason,
            "score": round(score, 2),
        })

    results.sort(key=lambda r: r["rvol"], reverse=True)
    return results


# ---------------------------------------------------------------------------
# LOGGING (same continuous-write approach as the console scanner)
# ---------------------------------------------------------------------------
def init_daily_csvs():
    os.makedirs(EOD_LOG_DIR, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    summary_path = os.path.join(EOD_LOG_DIR, f"eod_summary_{date_str}.csv")
    candidates_path = os.path.join(EOD_LOG_DIR, f"eod_candidates_{date_str}.csv")
    if not os.path.exists(summary_path):
        with open(summary_path, "w", newline="") as f:
            csv.writer(f).writerow([
                "symbol", "final_ltp", "final_vwap", "final_volume",
                "oi_open", "oi_current", "oi_change_pct", "oi_direction", "last_updated",
            ])
    if not os.path.exists(candidates_path):
        with open(candidates_path, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=CANDIDATE_FIELDNAMES).writeheader()
    return {"summary": summary_path, "candidates": candidates_path}


def write_summary_snapshot(summary_path, states):
    rows = []
    for symbol, state in states.items():
        with state.lock:
            oi_change_pct = None
            if state.oi_open and state.oi_current:
                oi_change_pct = round((state.oi_current - state.oi_open) / state.oi_open * 100, 2)
            rows.append([
                symbol, state.last_price, state.atp, state.cum_volume,
                state.oi_open, state.oi_current, oi_change_pct, state.oi_direction(),
                datetime.now().strftime("%H:%M:%S"),
            ])
    with open(summary_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "symbol", "final_ltp", "final_vwap", "final_volume",
            "oi_open", "oi_current", "oi_change_pct", "oi_direction", "last_updated",
        ])
        writer.writerows(rows)
        f.flush()


def append_candidate_row(candidates_path, candidate):
    with open(candidates_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CANDIDATE_FIELDNAMES)
        writer.writerow({k: candidate.get(k, "") for k in CANDIDATE_FIELDNAMES})
        f.flush()


# ---------------------------------------------------------------------------
# WEBSOCKET
# ---------------------------------------------------------------------------
def get_ws_url(access_token):
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    resp = requests.get(AUTHORIZE_URL, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()["data"]["authorized_redirect_uri"]


def build_subscribe_message(instrument_keys):
    return json.dumps({
        "guid": "banknifty-scanner-streamlit",
        "method": "sub",
        "data": {"mode": "full", "instrumentKeys": instrument_keys},
    })


def make_on_message(states_by_key):
    """states_by_key: instrument_key (futures, per-stock) -> SymbolState.
    Live LTP/VWAP/volume/depth AND OI all come from the same futures feed -
    futures volume/OI better reflect real leveraged intraday activity than
    cash market volume. S/R zones and EMA/RSI/MACD trend indicators still
    come from cash market DAILY candles (a futures contract only exists for
    ~1 month, can't support a 200-day EMA)."""
    def on_message(ws, message):
        if pb is None:
            return
        try:
            feed_response = pb.FeedResponse()
            feed_response.ParseFromString(message)
        except Exception:
            return
        for instrument_key, feed in feed_response.feeds.items():
            state = states_by_key.get(instrument_key)
            if state is None:
                continue
            try:
                full_feed = feed.fullFeed.marketFF
                ltp = full_feed.ltpc.ltp
                atp = full_feed.atp if hasattr(full_feed, "atp") else None
                total_volume = int(full_feed.vtt) if hasattr(full_feed, "vtt") and full_feed.vtt else 0
                tbq = full_feed.tbq if hasattr(full_feed, "tbq") else None
                tsq = full_feed.tsq if hasattr(full_feed, "tsq") else None
                oi = full_feed.oi if hasattr(full_feed, "oi") else None
                state.update(ltp, atp, total_volume, tbq, tsq)
                state.update_oi(oi)
            except Exception:
                pass
    return on_message


def make_on_open(instrument_keys):
    def on_open(ws):
        ws.send(build_subscribe_message(instrument_keys))
        with _app_lock:
            _app["status"] = "connected"
    return on_open


def on_error(ws, error):
    with _app_lock:
        _app["status"] = f"error: {error!r}"


def on_close(ws, code, msg):
    with _app_lock:
        _app["status"] = "closed"


def scoring_and_ws_thread(access_token, my_generation):
    """Runs in the background. Connects the WebSocket and periodically
    scores candidates, writing results into the shared _app dict for the
    UI to read on each rerun. Checks `my_generation` against the shared
    state each loop - if a newer connect_with_token() call has bumped the
    generation counter (e.g. the user entered a new token), this older
    thread notices and exits cleanly instead of running alongside the new
    one indefinitely."""
    try:
        with _app_lock:
            if _app["generation"] != my_generation:
                return  # superseded before we even started
            _app["status"] = "loading prep JSON"

        prep_data = load_today_prep()
        stock_entries = prep_data.get("stocks", {})
        if not stock_entries:
            raise ValueError(
                f"prep JSON loaded but has 0 stocks under 'stocks' key. "
                f"Top-level keys found: {list(prep_data.keys())}"
            )

        states, states_by_key = {}, {}
        instrument_keys = []
        for symbol, info in stock_entries.items():
            instrument_key = info["instrument_key"]
            futures_key = info.get("futures_instrument_key")
            state = SymbolState(
                symbol, instrument_key, futures_key,
                {"resistance_zones": info.get("resistance_zones", []),
                 "support_zones": info.get("support_zones", [])},
                info.get("rvol_baseline", {}),
                info.get("trend_indicators", {}),
                info.get("prev_close"),
            )
            states[symbol] = state
            # Subscribe to futures for LTP/VWAP/volume/OI together - falls
            # back to cash market only if no futures contract was resolved.
            if futures_key:
                states_by_key[futures_key] = state
                instrument_keys.append(futures_key)
            else:
                states_by_key[instrument_key] = state
                instrument_keys.append(instrument_key)

        with _app_lock:
            if _app["generation"] != my_generation:
                return
            _app["states"] = states
            _app["status"] = f"prep loaded ({len(states)} symbols), connecting websocket"

        csv_paths = init_daily_csvs()

        ws_url = get_ws_url(access_token)
        ws_app = websocket.WebSocketApp(
            ws_url,
            on_open=make_on_open(instrument_keys),
            on_message=make_on_message(states_by_key),
            on_error=on_error,
            on_close=on_close,
        )
        with _app_lock:
            if _app["generation"] != my_generation:
                return
            _app["ws_app"] = ws_app

        ws_thread = threading.Thread(
            target=lambda: ws_app.run_forever(
                sslopt={"ca_certs": certifi.where(), "cert_reqs": ssl.CERT_REQUIRED}
            ),
            daemon=True,
        )
        ws_thread.start()

        # Scoring loop - runs until superseded by a newer generation (e.g.
        # the user entered a different token) or the process ends.
        while True:
            with _app_lock:
                if _app["generation"] != my_generation:
                    return
            now = datetime.now().time()
            if MARKET_OPEN <= now <= MARKET_CLOSE:
                candidates = evaluate(states)
                timestamp = datetime.now().strftime("%H:%M:%S")
                for c in candidates:
                    c["timestamp"] = timestamp
                    append_candidate_row(csv_paths["candidates"], c)
                write_summary_snapshot(csv_paths["summary"], states)
                with _app_lock:
                    if _app["generation"] != my_generation:
                        return
                    _app["last_candidates"] = candidates
                    _app["last_scored_at"] = timestamp
            time.sleep(SCORING_INTERVAL_SEC)

    except Exception as e:
        with _app_lock:
            if _app["generation"] == my_generation:
                _app["error"] = f"{type(e).__name__}: {e}"
                _app["status"] = "crashed"


def connect_with_token(access_token):
    """(Re)starts the background thread with the given token. Safe to call
    repeatedly with the same token (no-op if already connected with it);
    calling with a different token cleanly closes the old connection and
    starts a fresh one, without needing a full app reboot."""
    with _app_lock:
        if _app["started"] and _app["current_token"] == access_token:
            return  # already connected with this exact token, nothing to do
        old_ws_app = _app["ws_app"]
        _app["generation"] += 1
        _app["current_token"] = access_token
        _app["started"] = True
        _app["status"] = "starting"
        _app["error"] = None
        _app["states"] = {}
        _app["last_candidates"] = []
        _app["last_scored_at"] = None
        _app["ws_app"] = None
        my_generation = _app["generation"]

    if old_ws_app is not None:
        try:
            old_ws_app.close()
        except Exception:
            pass

    t = threading.Thread(target=scoring_and_ws_thread, args=(access_token, my_generation), daemon=True)
    t.start()


# ---------------------------------------------------------------------------
# STREAMLIT UI
# ---------------------------------------------------------------------------
st.set_page_config(page_title="F&O Live Scanner", layout="wide")
st.title("F&O Live Scanner")

if pb is None:
    st.error(
        "MarketDataFeedV3_pb2.py not found in this folder. Generate it from the "
        "official .proto file first (see live_scanner_banknifty.py's docstring)."
    )
    st.stop()

# --- Sidebar: token entry ---
# Falls back to secrets.toml (cloud) / config.txt (local) as the default
# value, so existing deployments keep working unchanged - the sidebar just
# lets you view/override it without touching those files or rebooting the
# whole app. Since Upstox's Analytics Token is valid long-term (not the
# daily-expiring standard OAuth token), you may only need to set this once.
st.sidebar.header("Configuration")
_config = load_config()
_default_token = get_access_token(_config) or ""

if "access_token_input" not in st.session_state:
    st.session_state["access_token_input"] = _default_token

token_input = st.sidebar.text_input(
    "Enter Access Token",
    value=st.session_state["access_token_input"],
    type="password",
    key="access_token_input_widget",
)

col_connect, col_status = st.sidebar.columns([1, 1])
connect_clicked = col_connect.button("Connect", use_container_width=True)

with _app_lock:
    _currently_connected_token = _app["current_token"]

if connect_clicked and token_input:
    st.session_state["access_token_input"] = token_input
    connect_with_token(token_input)
    st.rerun()

if token_input and token_input == _currently_connected_token:
    st.sidebar.success("Token loaded and connected")
elif token_input:
    st.sidebar.info("Token entered - click Connect to use it")
else:
    st.sidebar.warning("Enter your Upstox access token above to start")

# Auto-connect once on first load if a token is already available from
# secrets/config.txt and nothing is running yet - preserves the old
# zero-click behavior for cloud deployments using Secrets.
with _app_lock:
    _already_started = _app["started"]
if not _already_started and _default_token:
    connect_with_token(_default_token)

with _app_lock:
    status = _app["status"]
    error = _app["error"]
    candidates = list(_app["last_candidates"])
    last_scored_at = _app["last_scored_at"]
    states_snapshot = dict(_app["states"])

col1, col2, col3 = st.columns(3)
col1.metric("Connection status", status)
col2.metric("Last scored at", last_scored_at or "-")
col3.metric("Candidates now", len(candidates))

with _app_lock:
    _debug_generation = _app["generation"]
    _debug_started = _app["started"]
    _debug_current_token = _app["current_token"]

with st.expander("Debug info (click if something looks stuck)"):
    st.write("Raw status:", status)
    st.write("Symbols loaded in states:", len(states_snapshot))
    st.write("Symbol list:", list(states_snapshot.keys()))
    st.write("Background thread error:", error)
    st.write("Prep files found:", glob.glob(os.path.join(PREP_DIR, "banknifty_prep_*.json")))
    st.write("Config file exists:", os.path.exists(CONFIG_FILE))
    st.write("Protobuf module loaded:", pb is not None)
    st.write("**Generation:**", _debug_generation, "(if this keeps increasing across page refreshes, it's reconnecting repeatedly)")
    st.write("**Started:**", _debug_started)
    st.write("**Current token (masked):**", (_debug_current_token[:6] + "..." if _debug_current_token else None))

if error:
    st.error(f"Background thread error: {error}")

now_time = datetime.now().time()
if not (MARKET_OPEN <= now_time <= MARKET_CLOSE):
    st.warning(
        f"Outside market hours ({MARKET_OPEN.strftime('%H:%M')}-"
        f"{MARKET_CLOSE.strftime('%H:%M')}). Scoring is paused; live prices "
        "may still stream in if connected."
    )

st.subheader("Candidates")

# Sort controls always render, unconditionally - see the identical note in
# the "All X instruments" section below for why (conditional widget
# creation is a known source of subtle Streamlit rendering bugs).
sort_col1, sort_col2 = st.columns([2, 1])
sort_by = sort_col1.radio(
    "Sort by", ["RVOL", "Score"], horizontal=True, key="candidates_sort_by",
)
sort_desc = sort_col2.toggle("High to low", value=True, key="candidates_sort_desc")

if candidates:
    df = pd.DataFrame(candidates)
    # Signed score: positive for bullish conviction, negative for bearish -
    # this is a display-only transform (the underlying 'score' column, used
    # in CSV logs and the console scanner, stays a plain magnitude; nothing
    # about scoring/thresholds changes, just how it's shown and sorted here).
    df["signed_score"] = df.apply(
        lambda r: r["score"] if r["bias"] == "bullish" else -r["score"], axis=1
    )

    sort_field = "rvol" if sort_by == "RVOL" else "signed_score"
    df = df.sort_values(sort_field, ascending=not sort_desc)

    display_cols = ["symbol", "rvol", "pattern", "bias", "ltp", "zone_level",
                     "distance_pct", "vwap", "zone_strength", "oi_signal",
                     "aggression", "trend_signal", "signed_score"]
    df_display = df[display_cols].rename(columns={"signed_score": "score"})

    # Same fix as the snapshot table below - .style.apply() row-by-row is
    # too slow at scale across a 208-stock universe. Plain st.dataframe()
    # stays fast regardless of how many candidates get flagged at once.
    st.dataframe(df_display, use_container_width=True, hide_index=True)
else:
    st.info("No candidates flagged yet this cycle.")

# NOTE: this table was removed once earlier today after extensive testing
# (row count, loop count, styling, widget placement) failed to explain a
# render failure at ~200 stocks. Re-added here for a genuinely fresh test
# after a full PC restart, since accumulated session/process state from
# many rapid test cycles today (rather than scale itself) may have been
# the real cause - worth confirming with a clean environment before
# concluding it's a hard scale limitation.
st.subheader(f"All {len(states_snapshot)} F&O stocks - live snapshot")

snap_sort_col1, snap_sort_col2 = st.columns([2, 1])
snap_sort_by = snap_sort_col1.radio(
    "Sort by", ["Symbol", "RVOL", "Change %"], horizontal=True, key="snapshot_sort_by",
)
snap_sort_desc = snap_sort_col2.toggle("High to low", value=True, key="snapshot_sort_desc")

if states_snapshot:
    rows = []
    for symbol, state in states_snapshot.items():
        # NOTE: do NOT wrap this in `with state.lock:` - rvol()/oi_direction()/
        # aggression_label()/change_pct() each acquire state.lock internally
        # already. Nesting a second acquire on a plain (non-reentrant)
        # threading.Lock from the same thread deadlocks it forever.
        rv = state.rvol()
        chg = state.change_pct()
        rows.append({
            "symbol": symbol,
            "ltp": state.last_price,
            "prev_close": state.prev_close,
            "change_pct": round(chg, 2) if chg is not None else None,
            "vwap": state.atp,
            "rvol": round(rv, 2) if rv is not None else None,
            "oi_direction": state.oi_direction(),
            "aggression": state.aggression_label(),
        })

    snap_sort_field = {"Symbol": "symbol", "RVOL": "rvol", "Change %": "change_pct"}[snap_sort_by]
    snap_df = pd.DataFrame(rows).sort_values(snap_sort_field, ascending=not snap_sort_desc, na_position="last")
    st.dataframe(snap_df, use_container_width=True, hide_index=True)
else:
    st.info("Waiting for prep data / first ticks...")

st.caption(
    f"Auto-refreshing every {UI_REFRESH_SEC}s. Logs written continuously to "
    f"eod_logs/. Scoring runs every {SCORING_INTERVAL_SEC}s in the background."
)

time.sleep(UI_REFRESH_SEC)
st.rerun()
