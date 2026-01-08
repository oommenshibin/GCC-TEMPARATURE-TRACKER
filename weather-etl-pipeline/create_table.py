import sqlite3
print("Creating weth.db + table...")
conn = sqlite3.connect('weth.db')
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS weatherdaily")
cur.execute("""
CREATE TABLE weatherdaily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    city TEXT NOT NULL,
    temperature REAL,
    humidity REAL,
    windspeed REAL,
    weathercode INTEGER
)
""")
conn.commit()
print("✅ Table created!")
cur.execute("PRAGMA table_info(weatherdaily)")
print("Schema:", [row[1] for row in cur.fetchall()])
cur.close()
conn.close()
print("🎉 Step 2 COMPLETE!")
