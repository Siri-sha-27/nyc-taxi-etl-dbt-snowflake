import pandas as pd

PATH = "data/bronze/taxi/yellow_tripdata_2024-01.parquet"

cols = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount",
    "payment_type",
]

df = pd.read_parquet(PATH, columns=cols)

print("=== BASIC ===")
print("rows:", len(df))
print("columns:", df.columns.tolist())

print("\n=== DATE RANGE ===")
print("pickup min/max:", df["tpep_pickup_datetime"].min(), df["tpep_pickup_datetime"].max())
print("dropoff min/max:", df["tpep_dropoff_datetime"].min(), df["tpep_dropoff_datetime"].max())

print("\n=== NULL RATES (%) ===")
null_pct = (df.isna().mean() * 100).round(2).sort_values(ascending=False)
print(null_pct)

print("\n=== INVALID / OUTLIER COUNTS ===")
invalid = {
    "pickup_after_dropoff": (df["tpep_pickup_datetime"] > df["tpep_dropoff_datetime"]).sum(),
    "fare_negative": (df["fare_amount"] < 0).sum(),
    "total_negative": (df["total_amount"] < 0).sum(),
    "trip_distance_negative": (df["trip_distance"] < 0).sum(),
    "trip_distance_zero": (df["trip_distance"] == 0).sum(),
    "passenger_negative": (df["passenger_count"] < 0).sum(),
    "passenger_too_high_gt8": (df["passenger_count"] > 8).sum(),
    "pulocation_null": df["PULocationID"].isna().sum(),
    "dolocation_null": df["DOLocationID"].isna().sum(),
}
for k, v in invalid.items():
    print(f"{k}: {v}")

print("\n=== DUPLICATE CHECK (candidate natural key) ===")
key_cols = ["VendorID","tpep_pickup_datetime","tpep_dropoff_datetime","PULocationID","DOLocationID","total_amount"]
dups = df.duplicated(subset=key_cols).sum()
print("duplicates on", key_cols, ":", dups)