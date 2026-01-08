import requests

url = "https://api.open-meteo.com/v1/forecast?latitude=25.2048&longitude=55.2708&current=temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code&timezone=Asia/Dubai"
resp = requests.get(url)
print("Status:", resp.status_code)
print("Raw response:", repr(resp.text[:200]))  # First 200 chars
print("JSON:", resp.json() if resp.ok else "Fail")
