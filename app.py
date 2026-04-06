from flask import Flask, jsonify, render_template
import requests
import time
import os
import threading

app = Flask(__name__)

ALPACA_KEY = os.environ.get("ALPACA_KEY")
ALPACA_SECRET = os.environ.get("ALPACA_SECRET")

HEADERS = {
    "APCA-API-KEY-ID": ALPACA_KEY or "",
    "APCA-API-SECRET-KEY": ALPACA_SECRET or "",
}

DATA_BASE = "https://data.alpaca.markets"
PAPER_BASE = "https://paper-api.alpaca.markets"

scanner_cache = {
    "top_gainers": [],
    "most_active": [],
    "mach_setups": [],
    "multi_highs": [],
    "market_feed": [],
}

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

    # Keep this modest for stable performance
    symbols = symbols[:300]
    print("Symbols used for scanning:", len(symbols))
    return symbols

def get_stock_snapshots(symbols):
    require_keys()

    all_snaps = {}

    for batch in chunked(symbols, 100):
        url = f"{DATA_BASE}/v2/stocks/snapshots"
        params = {"symbols": ",".join(batch)}
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()

        batch_data = r.json()
        if isinstance(batch_data, dict):
            all_snaps.update(batch_data)

        time.sleep(0.5)

    return all_snaps

def get_filtered_symbols():
    symbols = get_tradeable_symbols()
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

        # Relaxed filters so you actually see activity
        if price < 0.5 or price > 100:
            continue

        if volume < 500:
            continue

        filtered.append(symbol)

    print("Filtered symbols:", len(filtered))
    return filtered[:100]

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

        if price <= 1:
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

        if price <= 1:
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

def scanner_engine():
    global scanner_cache

    while True:
        try:
            print("Running scanner...")

            symbols = get_filtered_symbols()
            print("Symbols passed to scanner:", len(symbols))

            # Lightweight stable scan only
            scanner_cache["top_gainers"] = build_top_gainers(symbols)
            scanner_cache["most_active"] = build_most_active(symbols)
            scanner_cache["market_feed"] = build_market_feed(symbols)

            # Temporarily disable heavy features until infrastructure is upgraded
            scanner_cache["mach_setups"] = []
            scanner_cache["multi_highs"] = []

            print(
                "Scanner updated |",
                "top_gainers:", len(scanner_cache["top_gainers"]),
                "most_active:", len(scanner_cache["most_active"]),
                "market_feed:", len(scanner_cache["market_feed"]),
            )

        except Exception as e:
            print("Scanner error:", e)

        # Slow refresh to avoid rate limits
        time.sleep(300)

# ----------------------------
# Routes
# ----------------------------
@app.route("/")
def home():
    return render_template("dashboard.html")

@app.route("/ping")
def ping():
    return "ping works"

@app.route("/top-gainers")
def top_gainers():
    return jsonify(scanner_cache["top_gainers"])

@app.route("/most-active")
def most_active():
    return jsonify(scanner_cache["most_active"])

@app.route("/market-feed")
def market_feed():
    return jsonify(scanner_cache["market_feed"])

@app.route("/mach-setups")
def mach_setups():
    return jsonify(scanner_cache["mach_setups"])

@app.route("/multi-timeframe-highs")
def multi_timeframe_highs():
    return jsonify(scanner_cache["multi_highs"])

@app.route("/debug-cache")
def debug_cache():
    return jsonify({
        "top_gainers_count": len(scanner_cache["top_gainers"]),
        "most_active_count": len(scanner_cache["most_active"]),
        "market_feed_count": len(scanner_cache["market_feed"]),
        "mach_setups_count": len(scanner_cache["mach_setups"]),
        "multi_highs_count": len(scanner_cache["multi_highs"]),
    })

# Start scanner thread for both local + gunicorn
scanner_thread = threading.Thread(target=scanner_engine)
scanner_thread.daemon = True
scanner_thread.start()

if __name__ == "__main__":
    print("RUNNING FILE:", __file__)
    print("URL MAP:", app.url_map)

    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
