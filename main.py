import pandas as pd
from datetime import datetime, timedelta
import os
from yahooquery import Ticker
import requests
import csv


def fetch_tickers_from_cache(cache_file):
    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            tickers = [line.strip() for line in f if line.strip()]
            print(f"✅ Loaded {len(tickers)} from {cache_file}")
            return tickers
    return []

def compute_dm_signals(df):
    close = df["close"].values
    length = len(close)
    if length < 20:
        return False, False, False, False

    TD = [0] * length
    TDUp = [0] * length
    TS = [0] * length
    TDDn = [0] * length

    for i in range(4, length):
        TD[i] = TD[i - 1] + 1 if close[i] > close[i - 4] else 0
        TS[i] = TS[i - 1] + 1 if close[i] < close[i - 4] else 0

    def valuewhen_reset(arr, idx):
        for j in range(idx - 1, 0, -1):
            if arr[j] < arr[j - 1]:
                return arr[j]
        return 0

    for i in range(4, length):
        TDUp[i] = TD[i] - valuewhen_reset(TD, i)
        TDDn[i] = TS[i] - valuewhen_reset(TS, i)

    DM9Top = TDUp[-1] == 9
    DM13Top = TDUp[-1] == 13
    DM9Bot = TDDn[-1] == 9
    DM13Bot = TDDn[-1] == 13

    return DM9Top, DM13Top, DM9Bot, DM13Bot

def scan_timeframe(tickers, interval_label, interval):
    results = {"Tops": [], "Bottoms": []}
    print(f"\n🔍 Scanning {len(tickers)} tickers on {interval_label} timeframe...")

    for ticker in tickers:
        try:
            tk = Ticker(ticker)
            period = '2y' if interval == '1wk' else '6mo'
            hist = tk.history(period=period, interval=interval)
            if hist.empty:
                continue

            if isinstance(hist.index, pd.MultiIndex):
                df = hist.xs(ticker, level=0)
            else:
                df = hist

            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]

            if interval == '1wk':
                last_date = df['date'].iloc[-1].date()
                today = datetime.utcnow().date()
                if last_date >= today - timedelta(days=today.weekday()):
                    df = df.iloc[:-1]

            DM9Top, DM13Top, DM9Bot, DM13Bot = compute_dm_signals(df)

            if DM9Top or DM13Top:
                results["Tops"].append((ticker, "DM13 Top" if DM13Top else "DM9 Top"))
            if DM9Bot or DM13Bot:
                results["Bottoms"].append((ticker, "DM13 Bot" if DM13Bot else "DM9 Bot"))

        except Exception as e:
            print(f"⚠️ Skipping {ticker} [{interval_label}] due to error: {e}")

    return results
def get_fear_and_greed():
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        print("🔍 Raw Fear & Greed Data:", data)

        fg_data = data.get("fear_and_greed", {})
        fg_value = round(fg_data.get("score", 0))
        fg_previous = round(fg_data.get("previous_close", 0))
        timestamp = fg_data.get("timestamp")

        # Convert ISO timestamp to date
        if isinstance(timestamp, str):
            try:
                date_obj = datetime.fromisoformat(timestamp)
            except ValueError:
                date_obj = datetime.strptime(timestamp.split("+")[0], "%Y-%m-%dT%H:%M:%S")
            date = date_obj.strftime("%Y-%m-%d")
        else:
            date = datetime.utcnow().strftime("%Y-%m-%d")  # fallback

        # log to CSV
        with open("fear_and_greed_history.csv", "a", newline="") as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(["Date", "Index", "Previous Close"])
            writer.writerow([date, fg_value, fg_previous])

        return fg_value, fg_previous, date
    except Exception as e:
        print(f"⚠️ Error fetching Fear & Greed Index: {e}")
        return "N/A", "N/A", "N/A"

def write_html_report(timestamp, daily, weekly, fg_val, fg_prev, fg_date):
    sections = [
        ("Daily Bottoms", daily["Bottoms"]),
        ("Weekly Bottoms", weekly["Bottoms"]),
        ("Daily Tops", daily["Tops"]),
        ("Weekly Tops", weekly["Tops"]),
    ]

    def fg_color(value):
        try:
            value = float(value)
            if value < 25:
                return "#ff4d4d"  # extreme fear
            elif value < 50:
                return "#ffa64d"  # fear
            elif value < 75:
                return "#b3ff66"  # greed
            else:
                return "#66ff66"  # extreme greed
        except:
            return "#ccc"

    fg_html = f"""
    <h2>CNN Fear & Greed Index</h2>
    <p><strong>Date:</strong> {fg_date}</p>
    <p><strong>Current:</strong> <span style='background:{fg_color(fg_val)};padding:4px;'>{fg_val}</span></p>
    <p><strong>Previous Close:</strong> {fg_prev}</p>
    """

    html = f"""<html>
<head>
    <title>US DeMark Scanner</title>
    <style>
        body {{ font-family: Arial, sans-serif; padding: 2em; }}
        h2 {{ border-bottom: 1px solid #ccc; }}
        table {{ border-collapse: collapse; margin-bottom: 2em; }}
        th, td {{ border: 1px solid #ddd; padding: 0.5em; }}
        th {{ background-color: #f4f4f4; }}
    </style>
</head>
<body>
    <h1>US DeMark Scanner</h1>
    <p>Last updated: {timestamp}</p>
    {fg_html}
"""

    for title, data in sections:
        html += f"<h2>{title}</h2>"
        if data:
            html += "<table><tr><th>Ticker</th><th>Signal</th></tr>"
            for ticker, signal in data:
                html += f"<tr><td>{ticker}</td><td>{signal}</td></tr>"
            html += "</table>"
        else:
            html += "<p>None</p>"

    html += "</body></html>"

    os.makedirs("docs", exist_ok=True)
    with open("docs/index.html", "w") as f:
        f.write(html)

def main():
    sp500 = fetch_tickers_from_cache("sp_cache.txt")
    russell = fetch_tickers_from_cache("russell_cache.txt")
    nasdaq = fetch_tickers_from_cache("nasdaq_cache.txt")
    all_tickers = sp500 + russell + nasdaq

    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    fg_val, fg_prev, fg_date = get_fear_and_greed()

    daily_signals = scan_timeframe(all_tickers, "1D", "1d")
    weekly_signals = scan_timeframe(all_tickers, "1W", "1wk")

    def print_section(title, signals):
        print(f"\n🔸 {title}\n" + "-" * 40)
        if signals:
            df = pd.DataFrame(signals, columns=["Ticker", "Signal"])
            print(df.to_string(index=False))
        else:
            print("None")

    print(f"\n📋 DeMark Signals as of {now_str}\n" + "=" * 40)
    print_section("Daily Bottoms", daily_signals["Bottoms"])
    print_section("Weekly Bottoms", weekly_signals["Bottoms"])
    print_section("Daily Tops", daily_signals["Tops"])
    print_section("Weekly Tops", weekly_signals["Tops"])

    write_html_report(now_str, daily_signals, weekly_signals, fg_val, fg_prev, fg_date)

if __name__ == "__main__":
    main()
