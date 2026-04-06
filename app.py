from flask import Flask, jsonify, render_template
import requests
import threading
import time
import os

app = Flask(__name__)

# ----------------------------
# Alpaca API
# ----------------------------

ALPACA_KEY = os.environ.get("ALPACA_KEY")
ALPACA_SECRET = os.environ.get("ALPACA_SECRET")

HEADERS = {
    "APCA-API-KEY-ID": ALPACA_KEY or "",
    "APCA-API-SECRET-KEY": ALPACA_SECRET or "",
}

DATA_BASE = "https://data.alpaca.markets"

# ----------------------------
# Data Storage
# ----------------------------

scanner_cache = {
    "market_feed": [],
    "most_active": [],
    "top_gainers": [],
    "mach_setups": [],
    "multi_highs": []
}

# ----------------------------
# Stock universe
# ----------------------------

SYMBOLS = [
    "AAPL","NVDA","TSLA","AMD","META","AMZN","MSFT",
    "PLTR","SOFI","NIO","RIVN","LCID","F","INTC",
    "CCL","AAL","MARA","RIOT","COIN","HOOD",
    "SPY","QQQ","TQQQ","SQQQ","IWM"
]

# ----------------------------
# Get stock snapshots
# ----------------------------

def get_market_data():

    url = f"{DATA_BASE}/v2/stocks/snapshots"

    params = {
        "symbols": ",".join(SYMBOLS),
        "feed": "iex"
    }

    r = requests.get(url, headers=HEADERS, params=params)

    print("Status:", r.status_code)
    print("Response:", r.text[:200])

    data = r.json()

    rows = []

    for symbol, snap in data.items():

        trade = snap.get("latestTrade", {})
        prev = snap.get("prevDailyBar", {})
        bar = snap.get("dailyBar", {})

        price = trade.get("p")
        prev_close = prev.get("c")
        volume = bar.get("v")

        if not price or not prev_close:
            continue

        change = ((price - prev_close) / prev_close) * 100

        rows.append({
            "symbol": symbol,
            "price": round(price, 2),
            "chg": round(change, 2),
            "volume": volume
        })

    return rows
# ----------------------------
# Scanner Engine
# ----------------------------

def scanner_engine():

    global scanner_cache

    while True:

        try:

            rows = get_market_data()

            # sort volume
            rows.sort(key=lambda x: x["volume"], reverse=True)

            scanner_cache["market_feed"] = rows[:25]
            scanner_cache["most_active"] = rows[:25]

            # top gainers
            gainers = sorted(rows, key=lambda x: x["chg"], reverse=True)
            scanner_cache["top_gainers"] = gainers[:25]

            print("Scanner updated")

        except Exception as e:

            print("Scanner error:", e)

        time.sleep(15)

# ----------------------------
# Routes
# ----------------------------

@app.route("/")
def home():
    return render_template("dashboard.html")

@app.route("/market-feed")
def market_feed():
    return jsonify(scanner_cache["market_feed"])

@app.route("/most-active")
def most_active():
    return jsonify(scanner_cache["most_active"])

@app.route("/top-gainers")
def top_gainers():
    return jsonify(scanner_cache["top_gainers"])

@app.route("/mach-setups")
def mach_setups():
    return jsonify(scanner_cache["mach_setups"])

@app.route("/multi-timeframe-highs")
def multi_highs():
    return jsonify(scanner_cache["multi_highs"])

# ----------------------------
# Start Server
# ----------------------------

if __name__ == "__main__":

    print("Starting Mach Sven Radar server...")

    scanner_thread = threading.Thread(target=scanner_engine)
    scanner_thread.daemon = True
    scanner_thread.start()

    app.run(
        host="0.0.0.0",
        port=5001,
        debug=True
    )
