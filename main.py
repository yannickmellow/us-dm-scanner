import os
import csv
import requests
import datetime
import base64
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup

CACHE_FILES = {
    "SP500": "sp_cache.txt",
    "NASDAQ100": "nasdaq_cache.txt",
    "RUSSELL2000": "russell_cache.txt",
}

SIGNALS = {
    "daily_bottoms": {},
    "daily_tops": {},
    "weekly_bottoms": {},
    "weekly_tops": {},
}

def get_fear_and_greed():
    import datetime
    import csv
    import requests

    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        print("🔍 Raw Fear & Greed Data:", data)  # Debug output

        fg_value = data["fear_and_greed"]["now"]
        fg_previous = data["fear_and_greed"]["previous_close"]
        timestamp = data["fear_and_greed"]["timestamp"]
        date = datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")

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

def generate_fear_and_greed_chart():
    dates, values = [], []
    if not os.path.exists("fear_and_greed_history.csv"):
        return ""

    with open("fear_and_greed_history.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                dates.append(row["Date"])
                values.append(int(row["Index"]))
            except:
                continue

    if not values:
        return ""

    plt.figure(figsize=(5, 2))
    plt.plot(dates, values, color="blue", marker="o", linewidth=2)
    plt.xticks(rotation=45)
    plt.title("Fear & Greed Index Trend")
    plt.tight_layout()

    img_path = "fear_greed_plot.png"
    plt.savefig(img_path)
    plt.close()

    with open(img_path, "rb") as f:
        encoded = base64.b64encode(f.read()).decode()
        return f'<img src="data:image/png;base64,{encoded}" width="100%">'

def parse_cache(group, path):
    with open(path) as f:
        content = f.read()

    for section, key in [
        ("Daily Bottoms", "daily_bottoms"),
        ("Daily Tops", "daily_tops"),
        ("Weekly Bottoms", "weekly_bottoms"),
        ("Weekly Tops", "weekly_tops"),
    ]:
        if section in content:
            tickers = content.split(section)[1].split("\n\n")[0].strip().splitlines()
            tickers = [t for t in tickers if t.strip()]
            SIGNALS[key][group] = tickers


def generate_signal_columns(title, key_left, key_right):
    col = f"<h2>{title}</h2><div style='display: flex; gap: 40px;'>"

    for label, signals in [("Bottoms", key_left), ("Tops", key_right)]:
        col += "<div style='flex: 1;'>"
        col += f"<h3>{label}</h3>"
        for group in CACHE_FILES:
            items = SIGNALS[signals].get(group, [])
            if items:
                col += f"<h4>{group}</h4><ul>"
                for ticker in items:
                    col += f"<li>{ticker}</li>"
                col += "</ul>"
        col += "</div>"

    col += "</div>"
    return col


def generate_fear_section(index, previous, date):
    if index == "N/A":
        return "<h2>Fear & Greed Index</h2><p>Data unavailable.</p>"

    try:
        idx = int(index)
        color = (
            "#d9534f" if idx < 30 else "#f0ad4e" if idx < 60 else "#5cb85c"
        )
    except:
        color = "#999"

    chart_html = generate_fear_and_greed_chart()

    return f"""
    <h2>Fear & Greed Index</h2>
    <div style="background-color: {color}; color: white; padding: 10px; font-size: 1.5em; border-radius: 8px;">
        {index} (Previous: {previous}) — {date}
    </div>
    <br>{chart_html}
    """


def main():
    for group, path in CACHE_FILES.items():
        if os.path.exists(path):
            parse_cache(group, path)

    fg_index, fg_previous, fg_date = get_fear_and_greed()
    fear_section = generate_fear_section(fg_index, fg_previous, fg_date)

    daily = generate_signal_columns("Daily Signals", "daily_bottoms", "daily_tops")
    weekly = generate_signal_columns("Weekly Signals", "weekly_bottoms", "weekly_tops")

    html = f"""
    <html><head>
    <meta charset="utf-8">
    <title>DeMark US Market Scanner</title>
    <style>
        body {{ font-family: sans-serif; padding: 20px; }}
        h2 {{ border-bottom: 2px solid #eee; padding-bottom: 5px; }}
        ul {{ padding-left: 20px; }}
    </style>
    </head><body>
    <h1>📈 US DeMark Scanner</h1>
    <p>Updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
    {fear_section}
    <hr>
    {daily}
    <hr>
    {weekly}
    </body></html>
    """

    os.makedirs("docs", exist_ok=True)
    with open("docs/index.html", "w") as f:
        f.write(html)

if __name__ == "__main__":
    main()
