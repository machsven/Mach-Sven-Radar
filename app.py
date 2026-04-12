from flask import Flask, jsonify, request, make_response
import os
import time
import requests

app = Flask(__name__)

# =====================================
# CONFIG
# =====================================
ALPACA_KEY = os.getenv("ALPACA_KEY", "").strip()
ALPACA_SECRET = os.getenv("ALPACA_SECRET", "").strip()

# Alpaca snapshots endpoint and assets endpoint
ASSETS_URL = "https://paper-api.alpaca.markets/v2/assets"
SNAPSHOTS_URL = "https://data.alpaca.markets/v2/stocks/snapshots"

# Free-plan friendly default feed.
# Docs say snapshots support iex, sip, delayed_sip, etc.  [oai_citation:2‡Alpaca API Docs](https://docs.alpaca.markets/reference/stocksnapshots-1)
ALPACA_FEED = os.getenv("ALPACA_FEED", "iex").strip()

# Scanner/watchlist rules
WATCHLIST_MIN_PRICE = 5.0
BATCH_SIZE = 150
CACHE_SECONDS = 180  # 3 minutes

scanner_cache = {
    "timestamp": 0,
    "rows": [],
    "universe_count": 0
}


# =====================================
# HELPERS
# =====================================
def add_cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp


def require_keys():
    if not ALPACA_KEY or not ALPACA_SECRET:
        raise RuntimeError("Missing ALPACA_KEY or ALPACA_SECRET environment variable.")


def alpaca_headers():
    return {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }


def safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


# =====================================
# DATA FETCH
# =====================================
def fetch_active_tradable_symbols():
    """
    Pull all active tradable US equities from Alpaca assets.
    Alpaca documents /v2/assets as the asset list endpoint.  [oai_citation:3‡Alpaca API Docs](https://docs.alpaca.markets/docs/working-with-assets)
    """
    require_keys()

    params = {
        "status": "active",
        "asset_class": "us_equity",
    }

    res = requests.get(ASSETS_URL, headers=alpaca_headers(), params=params, timeout=30)
    res.raise_for_status()
    assets = res.json()

    symbols = []
    for asset in assets:
        symbol = asset.get("symbol")
        tradable = asset.get("tradable", False)
        if symbol and tradable:
            symbols.append(symbol)

    return symbols


def fetch_snapshots(symbols):
    """
    Fetch Alpaca snapshots in batches.
    The snapshots endpoint accepts a comma-separated list of symbols.  [oai_citation:4‡Alpaca API Docs](https://docs.alpaca.markets/reference/stocksnapshots-1)
    """
    require_keys()

    all_snaps = {}

    for batch in chunked(symbols, BATCH_SIZE):
        params = {
            "symbols": ",".join(batch),
            "feed": ALPACA_FEED,
        }

        res = requests.get(SNAPSHOTS_URL, headers=alpaca_headers(), params=params, timeout=30)
        res.raise_for_status()
        payload = res.json()

        if isinstance(payload, dict):
            if "snapshots" in payload and isinstance(payload["snapshots"], dict):
                all_snaps.update(payload["snapshots"])
            else:
                all_snaps.update(payload)
        else:
            raise RuntimeError(f"Unexpected Alpaca response: {payload}")

    return all_snaps


# =====================================
# ROW BUILD / SCORING
# =====================================
def build_row(symbol, snap):
    """
    Snapshots provide latest trade, minute bar, daily bar, and prev daily bar.  [oai_citation:5‡Alpaca API Docs](https://docs.alpaca.markets/reference/stocksnapshots-1)
    """
    latest_trade_price = safe_get(snap, "latestTrade", "p", default=None)
    prev_close = safe_get(snap, "prevDailyBar", "c", default=None)
    day_volume = safe_get(snap, "dailyBar", "v", default=None)
    hod = safe_get(snap, "dailyBar", "h", default=None)
    lod = safe_get(snap, "dailyBar", "l", default=None)

    if latest_trade_price is None or prev_close in (None, 0):
        return None

    change_pct = round(((latest_trade_price - prev_close) / prev_close) * 100, 2)

    return {
        "symbol": symbol,
        "price": round(latest_trade_price, 2),
        "change_pct": change_pct,
        "volume": day_volume,
        "high_of_day": round(hod, 2) if hod is not None else None,
        "low_of_day": round(lod, 2) if lod is not None else None,
        "prev_close": round(prev_close, 2) if prev_close is not None else None,
    }


def best_stock_score(row):
    """
    Hybrid score for watchlist selection:
    reward positive % change and meaningful liquidity.
    """
    change_pct = row.get("change_pct") or 0
    volume = row.get("volume") or 0
    price = row.get("price") or 0

    # avoid overrewarding penny-ish names by price-weighting lightly
    price_factor = 1 if price >= 5 else 0.5

    return (change_pct * 1000) + (volume / 1000) * price_factor


def sort_rows(rows, sort_mode):
    if sort_mode == "volume_desc":
        return sorted(rows, key=lambda x: x.get("volume") or 0, reverse=True)
    if sort_mode == "price_desc":
        return sorted(rows, key=lambda x: x.get("price") or 0, reverse=True)
    if sort_mode == "price_asc":
        return sorted(rows, key=lambda x: x.get("price") or 0)
    if sort_mode == "change_asc":
        return sorted(rows, key=lambda x: x.get("change_pct") or 0)
    if sort_mode == "symbol_asc":
        return sorted(rows, key=lambda x: x.get("symbol") or "")
    if sort_mode == "hybrid":
        return sorted(rows, key=best_stock_score, reverse=True)

    # default
    return sorted(rows, key=lambda x: x.get("change_pct") or 0, reverse=True)


def apply_scanner_filters(rows, min_price=None, min_volume=None, min_change_pct=None, direction="all"):
    filtered = []

    for row in rows:
        price = row.get("price")
        volume = row.get("volume")
        change_pct = row.get("change_pct")

        if min_price is not None and (price is None or price < min_price):
            continue
        if min_volume is not None and (volume is None or volume < min_volume):
            continue
        if min_change_pct is not None and (change_pct is None or abs(change_pct) < min_change_pct):
            continue

        if direction == "gainers" and (change_pct is None or change_pct < 0):
            continue
        if direction == "losers" and (change_pct is None or change_pct > 0):
            continue

        filtered.append(row)

    return filtered


# =====================================
# CACHE
# =====================================
def get_scanner_rows():
    now = time.time()

    if now - scanner_cache["timestamp"] < CACHE_SECONDS and scanner_cache["rows"]:
        return scanner_cache["rows"], scanner_cache["universe_count"]

    symbols = fetch_active_tradable_symbols()
    snaps = fetch_snapshots(symbols)

    rows = []
    for symbol in symbols:
        snap = snaps.get(symbol)
        if not snap:
            continue
        row = build_row(symbol, snap)
        if row:
            rows.append(row)

    scanner_cache["timestamp"] = now
    scanner_cache["rows"] = rows
    scanner_cache["universe_count"] = len(rows)

    return rows, len(rows)


# =====================================
# ROUTES
# =====================================
@app.route("/", methods=["GET"])
def home():
    return "Mach Sven Scanner Running"


@app.route("/health", methods=["GET"])
def health():
    return add_cors(jsonify({"ok": True}))


@app.route("/scanner", methods=["GET", "OPTIONS"])
def scanner():
    if request.method == "OPTIONS":
        return add_cors(make_response("", 204))

    try:
        rows, universe_count = get_scanner_rows()

        # Query filters for free public scanner
        min_price = request.args.get("min_price", default=None, type=float)
        min_volume = request.args.get("min_volume", default=None, type=int)
        min_change_pct = request.args.get("min_change_pct", default=None, type=float)
        direction = request.args.get("direction", default="all", type=str)
        sort_mode = request.args.get("sort", default="change_desc", type=str)
        limit = request.args.get("limit", default=100, type=int)

        filtered = apply_scanner_filters(
            rows,
            min_price=min_price,
            min_volume=min_volume,
            min_change_pct=min_change_pct,
            direction=direction
        )

        sorted_rows = sort_rows(filtered, sort_mode)

        if limit is not None and limit > 0:
            sorted_rows = sorted_rows[:limit]

        resp = jsonify({
            "ok": True,
            "feed": ALPACA_FEED,
            "count": len(sorted_rows),
            "universe_count": universe_count,
            "filters": {
                "min_price": min_price,
                "min_volume": min_volume,
                "min_change_pct": min_change_pct,
                "direction": direction,
                "sort": sort_mode,
                "limit": limit
            },
            "data": sorted_rows
        })
        return add_cors(resp)

    except Exception as e:
        resp = jsonify({"ok": False, "error": str(e)})
        return add_cors(resp), 500


@app.route("/watchlist/<tier>", methods=["GET", "OPTIONS"])
def watchlist_tier(tier):
    if request.method == "OPTIONS":
        return add_cors(make_response("", 204))

    tier = tier.lower()
    tier_limits = {
        "silver": 3,
        "gold": 7,
        "platinum": 10,
        "diamond": 15,
    }

    if tier not in tier_limits:
        resp = jsonify({"ok": False, "error": "Invalid tier"})
        return add_cors(resp), 400

    try:
        rows, universe_count = get_scanner_rows()

        # Watchlists use stronger default standards
        filtered = apply_scanner_filters(
            rows,
            min_price=WATCHLIST_MIN_PRICE,
            min_volume=None,
            min_change_pct=None,
            direction="gainers"
        )

        # Watchlists should sort "best stocks", not just raw gainers
        sorted_rows = sort_rows(filtered, "hybrid")
        limited = sorted_rows[:tier_limits[tier]]

        resp = jsonify({
            "ok": True,
            "tier": tier,
            "feed": ALPACA_FEED,
            "count": len(limited),
            "universe_count": universe_count,
            "watchlist_rules": {
                "min_price": WATCHLIST_MIN_PRICE,
                "direction": "gainers",
                "sort": "hybrid"
            },
            "data": limited
        })
        return add_cors(resp)

    except Exception as e:
        resp = jsonify({"ok": False, "error": str(e)})
        return add_cors(resp), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5001"))
    app.run(host="0.0.0.0", port=port, debug=True)
