import pandas as pd

df = pd.read_parquet(r"data\bronze\taxi\yellow_tripdata_2024-01.parquet")

print("Row count:", len(df))
print("Columns:", list(df.columns))
print(df.head())

import pandas as pd

df = pd.read_parquet("data/bronze/taxi/yellow_tripdata_2024-01.parquet", columns=["tpep_pickup_datetime","tpep_dropoff_datetime"])
print("pickup min/max:", df["tpep_pickup_datetime"].min(), df["tpep_pickup_datetime"].max())
print("dropoff min/max:", df["tpep_dropoff_datetime"].min(), df["tpep_dropoff_datetime"].max())