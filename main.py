import os
import csv
import datetime
import requests
import pandas as pd
import matplotlib.pyplot as plt
from yahooquery import Ticker

# ---------------------------- CONFIG ----------------------------
CACHE_FILES = {
    'S&P 500': 'sp_cache.txt',
    'NASDAQ 100': 'nasdaq_cache.txt',
    'Russell 2000': 'russell_cache.txt'
}

SIGNALS = {
    'Daily Bottoms': {},
    'Daily Tops': {},
    'Weekly Bottoms': {},
    'Weekly Tops': {}
}

# ---------------------------- DeMark Signal Logic ----------------------------
def compute_dm_signals(df):
    dm9_top, dm9_bottom = False, False
    dm13_top, dm13_bottom = False, False
    if len(df) < 14:
        return dm9_top, dm9_bottom, dm13_top, dm13_bottom

    close = df['close']
    # TD9 logic
    if close.iloc[-1] > close.iloc[-5:-1].max():
        dm9_top = True
    if close.iloc[-1] < close.iloc[-5:-1].min():
        dm9_bottom = True

    # TD13 logic (simplified)
    if close.iloc[-1] > close.iloc[-13:-1].max():
        dm13_top = True
    if close.iloc[-1] < close.iloc[-13:-1].min():
        dm13_bottom = True

    return dm9_top, dm9_bottom, dm13_top, dm13_bottom

def scan_timeframe(ticker, interval, lookback=30):
    try:
        df = Ticker(ticker).history(interval=interval, start=datetime.date.today() - datetime.timedelta(days=lookback*2))
        if isinstance(df, pd.DataFrame):
            if 'symbol' in df.columns:
                df = df[df['symbol'] == ticker]
            df = df[['close']].dropna().reset_index(drop=True)
            return compute_dm_signals(df)
        return False, False, False, False
    except Exception:
        return False, False, False, False

# ---------------------------- Fear & Greed ----------------------------
def get_fear_and_greed():
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        raw_data = response.json()
        fg = raw_data['fear_and_greed']
        fg_value = round(fg['score'])
        fg_previous = round(fg['previous_close'])
        rating = fg['rating'].capitalize()
        timestamp = datetime.datetime.fromisoformat(fg['timestamp'].replace("Z", "+00:00"))
        date = timestamp.strftime("%Y-%m-%d")

        # log to CSV
        with open("fear_and_greed_history.csv", "a", newline="") as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(["Date", "Index", "Previous Close"])
            writer.writerow([date, fg_value, fg_previous])

        return fg_value, fg_previous, rating, date, raw_data['fear_and_greed_historical']['data']
    except Exception as e:
        print(f"⚠️ Error fetching Fear & Greed Index: {e}")
        return "N/A", "N/A", "N/A", "N/A", []

# ---------------------------- Plot Trend ----------------------------
def plot_fear_and_greed_trend(data):
    try:
        dates = [datetime.datetime.fromtimestamp(item['x'] / 1000.0) for item in data]
        values = [item['y'] for item in data]
        plt.figure(figsize=(6, 2))
        plt.plot(dates, values, marker='o', linestyle='-', color='purple')
        plt.title("Fear & Greed Trend")
        plt.grid(True, linestyle='--', alpha=0.5)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig("docs/fg_trend.png")
        plt.close()
    except Exception as e:
        print(f"⚠️ Error generating trend chart: {e}")

# ---------------------------- Run Scanner ----------------------------
def run_scan():
    for index_name, file in CACHE_FILES.items():
        if not os.path.exists(file):
            continue
        with open(file, 'r') as f:
            tickers = [line.strip() for line in f if line.strip()]
        for ticker in tickers:
            dm9_top_d, dm9_bottom_d, dm13_top_d, dm13_bottom_d = scan_timeframe(ticker, interval='1d')
            dm9_top_w, dm9_bottom_w, dm13_top_w, dm13_bottom_w = scan_timeframe(ticker, interval='1wk')

            group = f"<b>{index_name}</b>"

            if dm9_bottom_d or dm13_bottom_d:
                label = f"{ticker}"
                if dm9_bottom_d: label += " ✅DM9"
                if dm13_bottom_d: label += " ✅DM13"
                SIGNALS['Daily Bottoms'].setdefault(group, []).append(label)

            if dm9_top_d or dm13_top_d:
                label = f"{ticker}"
                if dm9_top_d: label += " ❌DM9"
                if dm13_top_d: label += " ❌DM13"
                SIGNALS['Daily Tops'].setdefault(group, []).append(label)

            if dm9_bottom_w or dm13_bottom_w:
                label = f"{ticker}"
                if dm9_bottom_w: label += " ✅DM9"
                if dm13_bottom_w: label += " ✅DM13"
                SIGNALS['Weekly Bottoms'].setdefault(group, []).append(label)

            if dm9_top_w or dm13_top_w:
                label = f"{ticker}"
                if dm9_top_w: label += " ❌DM9"
                if dm13_top_w: label += " ❌DM13"
                SIGNALS['Weekly Tops'].setdefault(group, []).append(label)

# ---------------------------- Generate HTML ----------------------------
def generate_html(fg_val, fg_prev, fg_rating, fg_date):
    color = "green" if fg_rating.lower() in ["greed", "extreme greed"] else ("red" if "fear" in fg_rating.lower() else "orange")

    def columnify(section):
        groups = SIGNALS[section]
        html = ""
        for group, tickers in groups.items():
            html += f"<div style='flex:1;padding:10px;'><h4>{group}</h4><ul>"
            for ticker in tickers:
                html += f"<li>{ticker}</li>"
            html += "</ul></div>"
        return html

    with open("docs/index.html", "w") as f:
        f.write("""
<html><head><meta charset='utf-8'><title>US DM Scanner</title></head>
<body style='font-family:sans-serif;padding:20px;'>
<h1>🧠 Daily DeMark Signal Scanner</h1>
""")
        f.write(f"<h3>🗓️ {datetime.date.today()}</h3>")
        f.write(f"<h2>🧭 CNN Fear & Greed Index</h2>")
        f.write(f"<p style='font-size:18px;'>Current: <b style='color:{color};'>{fg_val} ({fg_rating})</b> | Previous Close: {fg_prev} | Date: {fg_date}</p>")
        f.write("<img src='fg_trend.png' width='500'/><hr>")

        f.write("<h2>📉 Daily Signals</h2><div style='display:flex;'>")
        f.write(f"<div style='flex:1;'>{columnify('Daily Bottoms')}</div>")
        f.write(f"<div style='flex:1;'>{columnify('Daily Tops')}</div></div>")

        f.write("<h2>📈 Weekly Signals</h2><div style='display:flex;'>")
        f.write(f"<div style='flex:1;'>{columnify('Weekly Bottoms')}</div>")
        f.write(f"<div style='flex:1;'>{columnify('Weekly Tops')}</div></div>")

        f.write("</body></html>")

# ---------------------------- Main ----------------------------
def main():
    run_scan()
    fg_val, fg_prev, fg_rating, fg_date, fg_data = get_fear_and_greed()
    plot_fear_and_greed_trend(fg_data)
    generate_html(fg_val, fg_prev, fg_rating, fg_date)

if __name__ == '__main__':
    main()
