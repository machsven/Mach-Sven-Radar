from flask import Flask, jsonify
import requests
import os

app = Flask(__name__)

ALPACA_KEY = os.environ.get("ALPACA_KEY")
ALPACA_SECRET = os.environ.get("ALPACA_SECRET")

HEADERS = {
    "APCA-API-KEY-ID": ALPACA_KEY or "",
    "APCA-API-SECRET-KEY": ALPACA_SECRET or "",
}

DATA_BASE = "https://data.alpaca.markets"
PAPER_BASE = "https://paper-api.alpaca.markets"


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

    # Keep this low enough to avoid 429s
    return symbols[:100]


def get_stock_snapshots(symbols):
    require_keys()

    all_snaps = {}

    for batch in chunked(symbols, 100):
        url = f"{DATA_BASE}/v2/stocks/snapshots"
        params = {"symbols": ",".join(batch)}
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()

        data = r.json()
        if isinstance(data, dict):
            all_snaps.update(data)

    return all_snaps


def build_rows():
    symbols = get_tradeable_symbols()
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

        try:
            chg = ((price - prev_close) / prev_close) * 100
        except Exception:
            chg = 0

        rows.append({
            "symbol": symbol,
            "price": round(price, 2),
            "chg": round(chg, 2),
            "volume_raw": volume,
            "volume": format_volume(volume),
        })

    return rows


@app.route("/")
def home():
    return "Mach Sven Radar is live"


@app.route("/ping")
def ping():
    return "ping works"


@app.route("/top-gainers")
def top_gainers():
    rows = build_rows()
    rows.sort(key=lambda x: x["chg"], reverse=True)
    for row in rows:
        row.pop("volume_raw", None)
    return jsonify(rows[:25])


@app.route("/most-active")
def most_active():
    rows = build_rows()
    rows.sort(key=lambda x: x["volume_raw"], reverse=True)
    for row in rows:
        row.pop("volume_raw", None)
    return jsonify(rows[:25])


@app.route("/market-feed")
def market_feed():
    rows = build_rows()
    rows.sort(key=lambda x: x["volume_raw"], reverse=True)
    for row in rows:
        row.pop("volume_raw", None)
    return jsonify(rows[:50])


@app.route("/debug-raw")
def debug_raw():
    symbols = get_tradeable_symbols()
    snaps = get_stock_snapshots(symbols)

    sample = []
    for sym, s in list(snaps.items())[:10]:
        sample.append({
            "symbol": sym,
            "price": (s.get("latestTrade") or {}).get("p"),
            "prev_close": (s.get("prevDailyBar") or {}).get("c"),
            "volume": (s.get("dailyBar") or {}).get("v"),
        })

    return jsonify({
        "symbol_count": len(symbols),
        "snapshot_count": len(snaps),
        "sample": sample
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)lse, use_reloader=False)
