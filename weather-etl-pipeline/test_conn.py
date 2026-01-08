import sys
print(f"Python {sys.version}")
import sqlite3
print("sqlite3 OK")

print("Connecting to weth.db...")
conn = sqlite3.connect('weth.db')
print("✅ CONNECTED!")
cur = conn.cursor()
cur.execute("SELECT 1")
print("Query OK:", cur.fetchone())
cur.close()
conn.close()
print("🎉 Step 1 COMPLETE!")

