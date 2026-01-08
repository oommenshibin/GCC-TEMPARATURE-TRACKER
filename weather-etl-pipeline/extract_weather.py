import requests
import sqlite3
from datetime import datetime
import pandas as pd

cities = {
    'Dubai_UAE': (25.2048, 55.2708),
    'AbuDhabi_UAE': (24.4539, 54.3773),
    'Riyadh_SA': (24.7136, 46.6753),
    'Doha_QA': (25.2854, 51.5310),
    'KuwaitCity_KW': (29.3759, 47.9774),
    'Manama_BH': (26.2235, 50.5876),
    'Muscat_OM': (23.5859, 58.4059)
}

def fetch_weather(city, lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {'latitude': lat, 'longitude': lon, 'current': 'temperature_2m,relative_humidity_2m,windspeed_10m,weathercode', 'timezone': 'Asia/Dubai'}
    resp = requests.get(url, params=params)
    if resp.status_code != 200: return None
    data = resp.json()['current']
    print(f"{city}: {data['temperature_2m']}°C")
    return {
        'timestamp': datetime.now(),
        'city': city,
        'temperature': data['temperature_2m'],
        'humidity': data['relative_humidity_2m'],
        'windspeed': data['windspeed_10m'],
        'weathercode': data['weathercode']
    }

print("🔄 ETL: Fetching GCC Weather...")
weather_data = [fetch_weather(city, lat, lon) for city, (lat, lon) in cities.items() if fetch_weather(city, lat, lon)]
df = pd.DataFrame(weather_data)
print(df.round(1))

conn = sqlite3.connect('weth.db')
cur = conn.cursor()
for _, row in df.iterrows():
    # Convert timestamp to string for SQLite
    ts_str = row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("INSERT INTO weatherdaily (timestamp, city, temperature, humidity, windspeed, weathercode) VALUES (?, ?, ?, ?, ?, ?)",
                (ts_str, row['city'], round(row['temperature'],1),
                 round(row['humidity'],1), round(row['windspeed'],1), row['weathercode']))
conn.commit()
print("🎉 Step 3 COMPLETE!")

