import json

path = "data/bronze/weather/weather_2024-01.json"
with open(path, "r") as f:
    data = json.load(f)

hourly = data.get("hourly", {})
times = hourly.get("time", [])

print("Timezone:", data.get("timezone"))
print("Hourly keys:", list(hourly.keys()))
print("Number of hourly timestamps:", len(times))
print("First time:", times[0] if times else None)
print("Last time:", times[-1] if times else None)