from flask import Flask, jsonify, render_template

import requests
import pandas as pd

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import threading
import time
import os


app = Flask(__name__)

ALPACA_KEY = "PKUDXPULOHTYZOTCNLYSHYX3TS"
ALPACA_SECRET ="JDHW8k6aVArNYEzyG9btfryf54rZaAxBwVqGMFsahwY5"

HEADERS = {
    "APCA-API-KEY-ID": ALPACA_KEY or "",
    "APCA-API-SECRET-KEY": ALPACA_SECRET or "",
}

DATA_BASE = "https://data.alpaca.markets"
PAPER_BASE = "https://paper-api.alpaca.markets"

# ----------------------------
# In-memory data for dashboard
# ----------------------------
latest_matches = []
top_gainers_data = []
most_active_data = []
mach_setups_data = []

scanner_cache = {
    "top_gainers": [],
    "most_active": [],
    "mach_setups": [],
    "multi_highs": [],
    "market_feed": []
}

# Keep sample data here for now until we wire live multi-timeframe logic
multi_highs_data = [
    {
        "symbol": "ABCD",
        "price": 8.16,
        "prev_high": 8.67,
        "tf": "4H",
        "distance": "5.9%",
        "volume": "1.3M",
        "priority": 4,
        "time": "10:41"
    }
]

# ----------------------------
# Utility helpers
# ----------------------------
def require_keys():
    if not ALPACA_KEY or not ALPACA_SECRET:
        raise RuntimeError("Missing ALPACA_KEY or ALPACA_SECRET")


def format_volume(value):
    try:
        value = float(value)
    except Exception:
        return value

    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.1f}B"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value / 1_000:.1f}K"
    return str(int(value))


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


# ----------------------------
# Alpaca universe + snapshots
# ----------------------------
def get_tradeable_symbols():
    require_keys()

    url = f"{PAPER_BASE}/v2/assets"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    assets = r.json()

    symbols = []

    for asset in assets:
        if (
            asset.get("tradable")
            and asset.get("status") == "active"
            and asset.get("class") == "us_equity"
            and asset.get("exchange") in ["NASDAQ", "NYSE", "ARCA"]
        ):
            symbols.append(asset["symbol"])

    symbols = symbols[:7000]
    print("Symbols used for scanning:", len(symbols))

    return symbols

def get_filtered_symbols():
    require_keys()

    # Step 1: get all tradeable symbols
    symbols = get_tradeable_symbols()

    # Step 2: get snapshot data
    snapshots = get_stock_snapshots(symbols)
    print("Snapshots received:", len(snapshots))

    filtered = []

    for symbol, snap in snapshots.items():

        latest_trade = snap.get("latestTrade") or {}
        prev_daily_bar = snap.get("prevDailyBar") or {}
        daily_bar = snap.get("dailyBar") or {}

        price = latest_trade.get("p")
        prev_close = prev_daily_bar.get("c")
        volume = daily_bar.get("v")

        if price is None or prev_close in (None, 0) or volume is None:
            continue

        if price < 1 or price > 50:
            continue

        if volume < 2000:
            continue

        filtered.append(symbol)

    print(f"Filtered symbols: {len(filtered)}")

    return filtered


def get_stock_snapshots(symbols):
    require_keys()

    all_snaps = {}

    for batch in chunked(symbols, 150):

        url = f"{DATA_BASE}/v2/stocks/snapshots"

        params = {
            "symbols": ",".join(batch)
        }

        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()

        batch_data = r.json()
        all_snaps.update(batch_data)

    return all_snaps

def build_top_gainers(symbols):
    snapshots = get_stock_snapshots(symbols)
    rows = []

    for symbol, snap in snapshots.items():
        latest_trade = snap.get("latestTrade") or {}
        prev_daily_bar = snap.get("prevDailyBar") or {}
        daily_bar = snap.get("dailyBar") or {}

        price = latest_trade.get("p")
        prev_close = prev_daily_bar.get("c")
        volume = daily_bar.get("v")

        if price is None or prev_close in (None, 0) or volume is None:
            continue

        if price <= 3:
            continue

        chg = ((price - prev_close) / prev_close) * 100

        rows.append({
            "symbol": symbol,
            "price": round(price, 2),
            "chg": round(chg, 2),
            "volume": format_volume(volume)
        })

    rows.sort(key=lambda x: x["chg"], reverse=True)
    return rows[:25]


def build_most_active(symbols):
    snapshots = get_stock_snapshots(symbols)
    rows = []

    for symbol, snap in snapshots.items():
        latest_trade = snap.get("latestTrade") or {}
        prev_daily_bar = snap.get("prevDailyBar") or {}
        daily_bar = snap.get("dailyBar") or {}

        price = latest_trade.get("p")
        prev_close = prev_daily_bar.get("c")
        volume = daily_bar.get("v")

        if price is None or prev_close in (None, 0) or volume is None:
            continue

        if price <= 3:
            continue

        chg = ((price - prev_close) / prev_close) * 100

        rows.append({
            "symbol": symbol,
            "price": round(price, 2),
            "chg": round(chg, 2),
            "volume": volume
        })

    rows.sort(key=lambda x: x["volume"], reverse=True)

    trimmed = rows[:25]
    for row in trimmed:
        row["volume"] = format_volume(row["volume"])

    return trimmed


# ----------------------------
# Mach Setup scanner helpers
# ----------------------------
def get_recent_5min_bars(symbols):
    require_keys()

    if not symbols:
        return {}

    start_utc, end_utc = get_extended_session_window()
    all_bars = {}

    for batch in chunked(symbols, 100):
        params = {
            "symbols": ",".join(batch),
            "timeframe": "5Min",
            "start": start_utc.isoformat(),
            "end": end_utc.isoformat(),
            "limit": 1000,
            "feed": "iex"
        }

        url = f"{DATA_BASE}/v2/stocks/bars"
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()

        batch_bars = r.json().get("bars", {})
        all_bars.update(batch_bars)

    return all_bars


def matches_pattern(bars):
    if len(bars) < 3:
        return False

    df = pd.DataFrame(bars)
    last3 = df.tail(3).reset_index(drop=True)

    c1 = last3.iloc[0]
    c2 = last3.iloc[1]
    c3 = last3.iloc[2]

    green = c1["c"] > c1["o"]
    red2 = c2["c"] < c2["o"]
    red3 = c3["c"] < c3["o"]

    return green and red2 and red3

def build_mach_setups(symbols):
    bars_by_symbol = get_recent_5min_bars(symbols)
    latest_1min_volume_map = get_recent_1min_volume_map(symbols)
    avg_volume_map = get_average_daily_volume_map(symbols)
    matches = []

    for symbol, data in bars_by_symbol.items():
        if not data:
            continue

        last_price = data[-1]["c"]

        if last_price <= 4:
            continue

        total_volume = sum(int(b.get("v", 0)) for b in data)

        avg_daily_volume = avg_volume_map.get(symbol, 0)
        rvol = (total_volume / avg_daily_volume) if avg_daily_volume else 0

        if total_volume < 5000:
            continue

        if matches_pattern(data):
            green_candle = data[-3]
            green_high = green_candle.get("h")

            last_1m_volume = latest_1min_volume_map.get(symbol, 0)
            max_shares = int(last_1m_volume * 0.05)

            pullback_pct = None
            if green_high not in (None, 0):
                pullback_pct = ((green_high - last_price) / green_high) * 100

            raw_time = data[-1].get("t")
            display_time = str(raw_time) if raw_time else "-"

            matches.append({
                "symbol": symbol,
                "price": round(last_price, 2),
                "green_high": round(green_high, 2) if green_high is not None else "-",
                "pullback": f"{round(pullback_pct, 2)}%" if pullback_pct is not None else "-",
                "volume": format_volume(total_volume),
                "max_shares": max_shares,
                "rvol": round(rvol, 2),
                "setup": "Mach Setup",
                "time": display_time
            })

    return matches


NY = ZoneInfo("America/New_York")


def get_extended_session_window():
    now_ny = datetime.now(NY)
    session_start = now_ny.replace(hour=4, minute=0, second=0, microsecond=0)
    session_end = now_ny.replace(hour=20, minute=0, second=0, microsecond=0)

    return session_start.astimezone(timezone.utc), session_end.astimezone(timezone.utc)


def get_recent_1min_bars(symbols):
    require_keys()

    if not symbols:
        return {}

    start_utc, end_utc = get_extended_session_window()
    all_bars = {}

    for batch in chunked(symbols, 100):
        params = {
            "symbols": ",".join(batch),
            "timeframe": "1Min",
            "start": start_utc.isoformat(),
            "end": end_utc.isoformat(),
            "limit": 10000,
            "feed": "iex"
        }

        url = f"{DATA_BASE}/v2/stocks/bars"
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()

        batch_bars = r.json().get("bars", {})
        all_bars.update(batch_bars)

    return all_bars


def build_top_gainers_extended(symbols):
    bars_by_symbol = get_recent_1min_bars(symbols)
    snapshots = get_stock_snapshots(symbols)

    rows = []

    for symbol, bars in bars_by_symbol.items():
        if not bars:
            continue

        latest_price = bars[-1].get("c")
        total_volume = sum(int(b.get("v", 0)) for b in bars)

        snap = snapshots.get(symbol, {})
        prev_close = (snap.get("prevDailyBar") or {}).get("c")

        if latest_price is None or prev_close in (None, 0):
            continue

        if latest_price <= 3:
            continue

        chg = ((latest_price - prev_close) / prev_close) * 100

        rows.append({
            "symbol": symbol,
            "price": round(latest_price, 2),
            "chg": round(chg, 2),
            "volume": format_volume(total_volume)
        })

    rows.sort(key=lambda x: x["chg"], reverse=True)
    return rows[:25]


def build_most_active_extended(symbols):
    bars_by_symbol = get_recent_1min_bars(symbols)
    snapshots = get_stock_snapshots(symbols)

    rows = []

    for symbol, bars in bars_by_symbol.items():
        if not bars:
            continue

        latest_price = bars[-1].get("c")
        total_volume = sum(int(b.get("v", 0)) for b in bars)

        snap = snapshots.get(symbol, {})
        prev_close = (snap.get("prevDailyBar") or {}).get("c")

        if latest_price is None or prev_close in (None, 0):
            continue

        if latest_price <= 3:
            continue

        chg = ((latest_price - prev_close) / prev_close) * 100

        rows.append({
            "symbol": symbol,
            "price": round(latest_price, 2),
            "chg": round(chg, 2),
            "volume": total_volume
        })

    rows.sort(key=lambda x: x["volume"], reverse=True)

    trimmed = rows[:25]
    for row in trimmed:
        row["volume"] = format_volume(row["volume"])

    return trimmed

def get_timeframe_bars(symbols, timeframe, days_back=10):
    require_keys()

    if not symbols:
        return {}

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days_back)

    all_bars = {}

    for batch in chunked(symbols, 100):
        params = {
            "symbols": ",".join(batch),
            "timeframe": timeframe,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "limit": 1000,
            "feed": "iex"
        }

        url = f"{DATA_BASE}/v2/stocks/bars"
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()

        batch_bars = r.json().get("bars", {})
        all_bars.update(batch_bars)

    return all_bars

def build_multi_timeframe_highs(symbols):
    one_hour_bars = get_timeframe_bars(symbols, "1Hour", days_back=10)
    daily_bars = get_timeframe_bars(symbols, "1Day", days_back=30)
    five_min_bars = get_recent_5min_bars(symbols)
    latest_1min_volume_map = get_recent_1min_volume_map(symbols)
    avg_volume_map = get_average_daily_volume_map(symbols)

    rows = []

    for symbol in symbols:
        intraday = five_min_bars.get(symbol, [])
        hourly = one_hour_bars.get(symbol, [])
        daily = daily_bars.get(symbol, [])

        if not intraday:
            continue

        latest_price = intraday[-1].get("c")
        total_volume = sum(int(b.get("v", 0)) for b in intraday)
        avg_daily_volume = avg_volume_map.get(symbol, 0)
        rvol = (total_volume / avg_daily_volume) if avg_daily_volume else 0
        last_1m_volume = latest_1min_volume_map.get(symbol, 0)
        max_shares = int(last_1m_volume * 0.05)

        if latest_price is None:
            continue

        if latest_price <= 4 or latest_price >= 25:
            continue

        if total_volume < 5000:
            continue

        candidates = []

        if len(hourly) >= 2:
            prev_high_1h = max(b.get("h", 0) for b in hourly[:-1] if b.get("h") is not None)
            if prev_high_1h:
                dist_1h = ((prev_high_1h - latest_price) / prev_high_1h) * 100
                if 0 <= dist_1h <= 6:
                    candidates.append(("1H", prev_high_1h, dist_1h))

        if len(hourly) >= 5:
            prev_high_4h = max(b.get("h", 0) for b in hourly[:-1] if b.get("h") is not None)
            if prev_high_4h:
                dist_4h = ((prev_high_4h - latest_price) / prev_high_4h) * 100
                if 0 <= dist_4h <= 6:
                    candidates.append(("4H", prev_high_4h, dist_4h))

        if len(daily) >= 2:
            prev_high_daily = max(b.get("h", 0) for b in daily[:-1] if b.get("h") is not None)
            if prev_high_daily:
                dist_daily = ((prev_high_daily - latest_price) / prev_high_daily) * 100
                if 0 <= dist_daily <= 6:
                    candidates.append(("Daily", prev_high_daily, dist_daily))

        if not candidates:
            continue

        tf, prev_high, distance = min(candidates, key=lambda x: x[2])
        priority = 10 - min(int(distance), 9)

        raw_time = intraday[-1].get("t")
        display_time = str(raw_time) if raw_time else "-"

        rows.append({
            "symbol": symbol,
            "price": round(latest_price, 2),
            "prev_high": round(prev_high, 2),
            "tf": tf,
            "distance": f"{round(distance, 2)}%",
            "volume": format_volume(total_volume),
            "max_shares": max_shares,
            "rvol": round(rvol, 2),
            "priority": priority,
            "time": display_time
        })

    rows.sort(key=lambda x: x["priority"], reverse=True)
    return rows[:25]


def build_market_feed(symbols):

    snapshots = get_stock_snapshots(symbols)
    rows = []

    for symbol, snap in snapshots.items():

        latest_trade = snap.get("latestTrade") or {}
        prev_daily_bar = snap.get("prevDailyBar") or {}
        daily_bar = snap.get("dailyBar") or {}

        price = latest_trade.get("p")
        prev_close = prev_daily_bar.get("c")
        volume = daily_bar.get("v")

        if price is None or prev_close in (None, 0) or volume is None:
            continue

        chg = ((price - prev_close) / prev_close) * 100

        rows.append({
            "symbol": symbol,
            "price": round(price, 2),
            "chg": round(chg, 2),
            "volume": format_volume(volume)
        })

    rows.sort(key=lambda x: x["volume"], reverse=True)

    return rows[:50]

def get_recent_1min_volume_map(symbols):
    require_keys()

    if not symbols:
        return {}

    latest_volume_map = {}

    for batch in chunked(symbols, 100):

        url = f"{DATA_BASE}/v2/stocks/bars/latest"

        params = {
            "symbols": ",".join(batch),
            "feed": "iex"
        }

        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()

        data = r.json().get("bars", {})

        for symbol, bar in data.items():
            latest_volume_map[symbol] = int(bar.get("v", 0))

    return latest_volume_map

def get_average_daily_volume_map(symbols, days_back=20):
    require_keys()

    if not symbols:
        return {}

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days_back + 5)

    all_bars = {}

    for batch in chunked(symbols, 100):
        params = {
            "symbols": ",".join(batch),
            "timeframe": "1Day",
            "start": start.isoformat(),
            "end": end.isoformat(),
            "limit": 1000,
            "feed": "iex"
        }

        url = f"{DATA_BASE}/v2/stocks/bars"
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()

        batch_bars = r.json().get("bars", {})
        all_bars.update(batch_bars)

    avg_volume_map = {}

    for symbol, bars in all_bars.items():
        vols = [int(b.get("v", 0)) for b in bars if b.get("v") is not None]
        if vols:
            avg_volume_map[symbol] = sum(vols) / len(vols)
        else:
            avg_volume_map[symbol] = 0

    return avg_volume_map


def scanner_engine():
    global scanner_cache

    while True:
        try:
            print("Running scanner...")

            symbols = get_filtered_symbols()
            print("Symbols passed to scanner:", len(symbols))

            scanner_cache["top_gainers"] = build_top_gainers_extended(symbols)
            scanner_cache["most_active"] = build_most_active_extended(symbols)
            scanner_cache["mach_setups"] = build_mach_setups(symbols)
            scanner_cache["multi_highs"] = build_multi_timeframe_highs(symbols)
            scanner_cache["market_feed"] = build_market_feed(symbols)

            print("Scanner updated")

        except Exception as e:
            print("Scanner error:", e)

        time.sleep(30)

# ----------------------------
# Routes
# ----------------------------
@app.route("/top-gainers")
def top_gainers():
    return jsonify(scanner_cache["top_gainers"])



@app.route("/most-active")
def most_active():
    return jsonify(scanner_cache["most_active"])


@app.route("/mach-setups")
def mach_setups():
    return jsonify(scanner_cache["mach_setups"])


@app.route("/multi-timeframe-highs")
def multi_timeframe_highs():
    return jsonify(scanner_cache["multi_highs"])


@app.route("/")
def home():
    return render_template("dashboard.html")

@app.route("/market-feed")
def market_feed():
    return jsonify(scanner_cache["market_feed"])


@app.route("/ping")
def ping():
    return "ping works"


import os

if __name__ == "__main__":
    scanner_thread = threading.Thread(target=scanner_engine)
    scanner_thread.daemon = True
    scanner_thread.start()

    print("RUNNING FILE:", __file__)
    print("URL MAP:", app.url_map)

    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


