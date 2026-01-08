import streamlit as st
import sqlite3
import pandas as pd
import requests
from datetime import datetime

# GCC coords (lat, lon)
gcc_coords = {
    'Dubai_UAE': (25.20, 55.27),
    'AbuDhabi_UAE': (24.45, 54.38),
    'Riyadh_SA': (24.71, 46.68),
    'Doha_QA': (25.29, 51.53),
    'KuwaitCity_KW': (29.38, 47.98),
    'Manama_BH': (26.22, 50.59),
    'Muscat_OM': (23.59, 58.41)
}

DB_PATH = 'weth.db'

@st.cache_data(ttl=300)  # Cache 5min
def load_data():
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("SELECT * FROM weatherdaily ORDER BY timestamp DESC LIMIT 100", conn)
        conn.close()
        return df
    except:
        return pd.DataFrame()

def fetch_fresh():
    weather_data = []
    for city, (lat, lon) in gcc_coords.items():
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            'latitude': lat, 'longitude': lon,
            'current': 'temperature_2m,relative_humidity_2m,windspeed_10m,weathercode',
            'timezone': 'Asia/Dubai'
        }
        resp = requests.get(url, params=params)
        if resp.status_code == 200:
            data = resp.json()['current']
            weather_data.append({
                'timestamp': datetime.now(),
                'city': city,
                'temperature': data['temperature_2m'],
                'humidity': data['relative_humidity_2m'],
                'windspeed': data['windspeed_10m'],
                'weathercode': data['weathercode']
            })
    
    if weather_data:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        for row in weather_data:
            # ✅ FIXED: Timestamp to string
            ts_str = row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            cur.execute("""
                INSERT INTO weatherdaily (timestamp, city, temperature, humidity, windspeed, weathercode) 
                VALUES (?, ?, ?, ?, ?, ?)
            """, (ts_str, row['city'], row['temperature'], row['humidity'], row['windspeed'], row['weathercode']))
        conn.commit()
        conn.close()
        return pd.DataFrame(weather_data)
    return None

st.set_page_config(page_title="GCC Weather ETL", layout="wide")
st.title("🌡️ GCC Countries Temperature Tracker")
st.markdown("**Live Python ETL Pipeline | SQLite + Open-Meteo API**")

df = load_data()
if df.empty:
    st.warning("👆 No data yet. Click **ETL Fetch** button!")
else:
    # Map prep
    df_map = df.copy()
    df_map['lat'] = df_map['city'].map({city: coord[0] for city, coord in gcc_coords.items()})
    df_map['lon'] = df_map['city'].map({city: coord[1] for city, coord in gcc_coords.items()})
    df_map = df_map.dropna(subset=['lat', 'lon'])
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📊 Latest Readings")
        st.dataframe(df[['city', 'temperature', 'humidity', 'windspeed']].head(10).round(1))
    
    with col2:
        st.subheader("🗺️ GCC Live Map")
        if not df_map.empty:
            st.map(df_map[['lat', 'lon', 'temperature']])
    
    st.subheader("📈 Temperature Trends")
    st.line_chart(df.groupby('city')['temperature'].mean())

if st.button("🔄 **ETL: Fetch Fresh GCC Weather**", type="primary", use_container_width=True):
    with st.spinner("Fetching live data from 7 GCC capitals..."):
        fresh = fetch_fresh()
    if fresh is not None:
        st.success("✅ Fresh GCC weather ETL complete!")
        st.dataframe(fresh.round(1))
        st.balloons()
    else:
        st.error("❌ API fetch failed. Try again.")

# Footer
st.markdown("---")
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    st.markdown("""
    **Built by: Shibin Oommen**  
    *Data Engineering Portfolio: Python ETL + SQLite + Streamlit*  
 """)

