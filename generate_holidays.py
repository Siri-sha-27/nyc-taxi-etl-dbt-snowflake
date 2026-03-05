import holidays
import csv

us_holidays = holidays.US(years=2024)

out_path = r"data\bronze\holidays\us_holidays_2024.csv"
with open(out_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["date", "holiday_name"])

    for day, name in sorted(us_holidays.items()):
        writer.writerow([day.isoformat(), name])

print("Holiday file created:", out_path)
print("Total holidays:", len(us_holidays))