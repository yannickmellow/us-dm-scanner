import pandas as pd
from datetime import datetime, timedelta
import os
from yahooquery import Ticker

def fetch_asx200_tickers():
    cache_file = "asx200_cache.txt"
    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            tickers = [line.strip() for line in f if line.strip()]
            print(f"✅ Loaded {len(tickers)} tickers from cache file.")
            return tickers
    else:
        print("❌ Ticker cache file not found!")
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
    results = {
        "Tops": [],
        "Bottoms": []
    }

    print(f"\n🔍 Scanning {len(tickers)} tickers on {interval_label} timeframe...")

    for ticker in tickers:
        try:
            tk = Ticker(ticker)
            # Fetch more data for weekly scans
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

            # Remove last candle if it's from the current (incomplete) week
            if interval == '1wk':
                last_date = df['date'].iloc[-1].date()
                today = datetime.utcnow().date()
                if last_date >= today - timedelta(days=today.weekday()):
                    df = df.iloc[:-1]

            DM9Top, DM13Top, DM9Bot, DM13Bot = compute_dm_signals(df)

            if DM9Top or DM13Top:
                results["Tops"].append((ticker, "DM13Top" if DM13Top else "DM9Top"))
            if DM9Bot or DM13Bot:
                results["Bottoms"].append((ticker, "DM13Bot" if DM13Bot else "DM9Bot"))

        except Exception as e:
            print(f"⚠️ Skipping {ticker} [{interval_label}] due to error: {e}")

    return results

def write_html_report(timestamp, daily, weekly):
    sections = [
        ("Daily Bottoms", daily["Bottoms"]),
        ("Weekly Bottoms", weekly["Bottoms"]),
        ("Daily Tops", daily["Tops"]),
        ("Weekly Tops", weekly["Tops"]),
    ]

    html = f"""<html>
<head>
    <title>ASX DeMark Signals</title>
    <style>
        body {{ font-family: Arial, sans-serif; padding: 2em; }}
        h2 {{ border-bottom: 1px solid #ccc; }}
        table {{ border-collapse: collapse; margin-bottom: 2em; }}
        th, td {{ border: 1px solid #ddd; padding: 0.5em; }}
        th {{ background-color: #f4f4f4; }}
    </style>
</head>
<body>
    <h1>ASX DeMark Signals</h1>
    <p>Last updated: {timestamp}</p>
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
    tickers = fetch_asx200_tickers()
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    daily_signals = scan_timeframe(tickers, "1D", "1d")
    weekly_signals = scan_timeframe(tickers, "1W", "1wk")

    print(f"\n📋 DeMark Signals as of {now_str}\n" + "=" * 40)

    def print_section(title, signals):
        print(f"\n🔸 {title}\n" + "-" * 40)
        if signals:
            df = pd.DataFrame(signals, columns=["Ticker", "Signal"])
            print(df.to_string(index=False))
        else:
            print("None")

    print_section("Daily Bottoms", daily_signals["Bottoms"])
    print_section("Weekly Bottoms", weekly_signals["Bottoms"])
    print_section("Daily Tops", daily_signals["Tops"])
    print_section("Weekly Tops", weekly_signals["Tops"])

    write_html_report(now_str, daily_signals, weekly_signals)

if __name__ == "__main__":
    main()
