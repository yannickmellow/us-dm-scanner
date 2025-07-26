import pandas as pd
from datetime import datetime, timedelta
import os
from yahooquery import Ticker
import requests
import csv
from collections import defaultdict

def fetch_tickers_and_sectors_from_csv(cache_file):
    mapping = {}
    if os.path.exists(cache_file):
        with open(cache_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                ticker = row.get('Ticker')
                sector = row.get('Sector')
                if ticker and sector:
                    mapping[ticker.strip()] = sector.strip()
        print(f"✅ Loaded {len(mapping)} tickers & sectors from {cache_file}")
    else:
        print(f"❌ Cache file {cache_file} not found!")
    return mapping

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

def scan_timeframe(ticker_sector_map, interval_label, interval):
    results = {"Tops": [], "Bottoms": []}
    sector_counts = {
        "Tops": defaultdict(int),
        "Bottoms": defaultdict(int)
    }

    tickers = list(ticker_sector_map.keys())
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

            sector = ticker_sector_map.get(ticker, "Unknown")

            if DM9Top or DM13Top:
                signal = "DM13 Top" if DM13Top else "DM9 Top"
                results["Tops"].append((ticker, signal))
                sector_counts["Tops"][sector] += 1

            if DM9Bot or DM13Bot:
                signal = "DM13 Bot" if DM13Bot else "DM9 Bot"
                results["Bottoms"].append((ticker, signal))
                sector_counts["Bottoms"][sector] += 1

        except Exception as e:
            print(f"⚠️ Skipping {ticker} [{interval_label}] due to error: {e}")

    return results, sector_counts

def get_fear_and_greed():
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
        "Referer": "https://edition.cnn.com/",
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        print("🔍 Raw Fear & Greed Data:", data)

        fg_data = data.get("fear_and_greed", {})
        fg_value = round(fg_data.get("score", 0))
        fg_previous = round(fg_data.get("previous_close", 0))
        timestamp = fg_data.get("timestamp")

        if isinstance(timestamp, str):
            try:
                date_obj = datetime.fromisoformat(timestamp)
            except ValueError:
                date_obj = datetime.strptime(timestamp.split("+")[0], "%Y-%m-%dT%H:%M:%S")
            date = date_obj.strftime("%Y-%m-%d")
        else:
            date = datetime.utcnow().strftime("%Y-%m-%d")

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

def sector_counts_to_html(title, sector_counts):
    if not sector_counts:
        return "<p>No sector data.</p>"

    html = f"<h3>{title}</h3><table><tr><th>Sector</th><th>Count</th></tr>"
    for sector, count in sorted(sector_counts.items(), key=lambda x: x[1], reverse=True):
        html += f"<tr><td>{sector}</td><td>{count}</td></tr>"
    html += "</table>"
    return html

def signals_to_html_table(signals):
    if not signals:
        return "<p>No signals.</p>"

    # Sort alphabetically by ticker
    signals_sorted = sorted(signals, key=lambda x: x[0])

    html = "<table><tr><th>Ticker</th><th>Signal</th></tr>"
    for ticker, signal in signals_sorted:
        if signal == "DM9 Top":
            style = "background-color: #f8d7da;"
        elif signal == "DM13 Top":
            style = "background-color: #f5c6cb; font-weight: bold;"
        elif signal == "DM9 Bot":
            style = "background-color: #d4edda;"
        elif signal == "DM13 Bot":
            style = "background-color: #c3e6cb; font-weight: bold;"
        else:
            style = ""
        html += f"<tr><td>{ticker}</td><td style='{style}'>{signal}</td></tr>"
    html += "</table>"
    return html

def write_html_report(daily_results, weekly_results, daily_sectors, weekly_sectors, fg_index, fg_prev, fg_date):
    fg_color = "#28a745" if fg_index != "N/A" and float(fg_index) >= 50 else "#dc3545"

    html = f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <title>DeMark Signal Report</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 20px;
            }}
            h1 {{
                color: #333;
            }}
            .fg-box {{
                background-color: {fg_color};
                color: white;
                padding: 10px;
                margin-bottom: 20px;
                border-radius: 5px;
                display: inline-block;
            }}
            .row {{
                display: flex;
                justify-content: space-between;
                margin-bottom: 30px;
            }}
            .column {{
                flex: 1;
                margin: 0 10px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 10px;
            }}
            th, td {{
                border: 1px solid #ccc;
                padding: 6px 8px;
                text-align: left;
            }}
            th {{
                background-color: #f0f0f0;
            }}
        </style>
    </head>
    <body>
        <h1>🧭 DeMark Signal Report</h1>
        <div class="fg-box">
            <strong>CNN Fear & Greed Index:</strong> {fg_index} (Prev: {fg_prev}) on {fg_date}
        </div><br>
    """

    # Row 1: Bottoms
    html += f"""
    <div class="row">
        <div class="column">
            <h2>Daily Bottoms</h2>
            {signals_to_html_table(daily_results["Bottoms"])}
            {sector_counts_to_html("Daily Bottoms by Sector", daily_sectors["Bottoms"])}
        </div>
        <div class="column">
            <h2>Weekly Bottoms</h2>
            {signals_to_html_table(weekly_results["Bottoms"])}
            {sector_counts_to_html("Weekly Bottoms by Sector", weekly_sectors["Bottoms"])}
        </div>
    </div>
    """

    # Row 2: Tops
    html += f"""
    <div class="row">
        <div class="column">
            <h2>Daily Tops</h2>
            {signals_to_html_table(daily_results["Tops"])}
            {sector_counts_to_html("Daily Tops by Sector", daily_sectors["Tops"])}
        </div>
        <div class="column">
            <h2>Weekly Tops</h2>
            {signals_to_html_table(weekly_results["Tops"])}
            {sector_counts_to_html("Weekly Tops by Sector", weekly_sectors["Tops"])}
        </div>
    </div>
    """

    html += """
    </body>
    </html>
    """

    os.makedirs("docs", exist_ok=True)
    with open("docs/index.html", "w", encoding="utf-8") as f:
        f.write(html)

def main():
    sp500_map = fetch_tickers_and_sectors_from_csv("sp_cache.csv")
    russell_map = fetch_tickers_and_sectors_from_csv("russell_cache.csv")
    nasdaq_map = fetch_tickers_and_sectors_from_csv("nasdaq_cache.csv")
    all_map = {**sp500_map, **russell_map, **nasdaq_map}

    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    fg_val, fg_prev, fg_date = get_fear_and_greed()

    daily_results, daily_sectors = scan_timeframe(all_map, "1D", "1d")
    weekly_results, weekly_sectors = scan_timeframe(all_map, "1W", "1wk")

    def print_section(title, signals):
        print(f"\n🔸 {title}\n" + "-" * 40)
        if signals:
            df = pd.DataFrame(signals, columns=["Ticker", "Signal"])
            print(df.to_string(index=False))
        else:
            print("None")

    print(f"\n📋 DeMark Signals as of {now_str}\n" + "=" * 40)
    print_section("Daily Bottoms", daily_results["Bottoms"])
    print_section("Weekly Bottoms", weekly_results["Bottoms"])
    print_section("Daily Tops", daily_results["Tops"])
    print_section("Weekly Tops", weekly_results["Tops"])

    write_html_report(daily_results, weekly_results, daily_sectors, weekly_sectors, fg_val, fg_prev, fg_date)

if __name__ == "__main__":
    main()
