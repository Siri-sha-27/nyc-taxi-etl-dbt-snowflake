import requests
import json

url = "https://archive-api.open-meteo.com/v1/archive?latitude=40.7128&longitude=-74.0060&start_date=2024-01-01&end_date=2024-01-31&hourly=temperature_2m,precipitation,weathercode&timezone=America/New_York"

response = requests.get(url, timeout=60)
response.raise_for_status()
data = response.json()

out_path = r"data\bronze\weather\weather_2024-01.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)

print("Weather data saved to:", out_path)
print("Top-level keys:", list(data.keys()))