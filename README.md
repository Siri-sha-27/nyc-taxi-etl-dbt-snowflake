# NYC Taxi ETL Pipeline (Snowflake + dbt)

![CI](https://github.com/Siri-sha-27/nyc-taxi-etl-dbt-snowflake/actions/workflows/dbt-ci.yml/badge.svg)
![dbt](https://img.shields.io/badge/Transformation-dbt-orange)
![Snowflake](https://img.shields.io/badge/Warehouse-Snowflake-blue)
![Python](https://img.shields.io/badge/Python-3.10-blue)

Production-style **ELT data engineering pipeline** using **Snowflake + dbt** with **Medallion Architecture (RAW → SILVER → GOLD)** and automated **data quality testing with CI/CD**.

This pipeline ingests **NYC Taxi trip data**, enriches it with **hourly weather data** and **US holidays**, and publishes **analytics-ready fact tables**.

---

# Architecture

```
           Raw Data Sources
     (Taxi Trips, Weather API, Holidays)
                    │
                    ▼
             Snowflake RAW
        (Unmodified source data)
                    │
                    ▼
             dbt SILVER Layer
        - taxi_trips_clean
        - weather_hourly
        - dim_holidays
        (cleaning + standardization)
                    │
                    ▼
             dbt GOLD Layer
          fact_trips_enriched
     (joins taxi + weather + holidays)
                    │
                    ▼
           Analytics / BI / ML
```

---

# Data Sources

### NYC TLC Yellow Taxi Trip Records

* Format: **Parquet**
* Size: **~2.96M rows (Jan 2024)**
* Used as the **base fact dataset**

### Open-Meteo Weather API

* Format: **JSON**
* Data: **744 hourly weather records**
* Used for **temperature, precipitation, weather code**

### US Holiday Dataset

* Format: **CSV**
* Rows: **11 holidays for 2024**
* Used to build **holiday dimension**

---

# Medallion Architecture

## RAW Layer (Bronze)

Stores data **exactly as received from sources**.

Tables:

```
RAW.TAXI_TRIPDATA_2024_01_V2
RAW.WEATHER_2024_01_RAW
RAW.US_HOLIDAYS_2024
```

No cleaning is done here.

---

## SILVER Layer (Cleaned Data)

Created using **dbt models**.

### taxi_trips_clean

Contains cleaned taxi data:

* Parsed timestamps
* pickup_date
* pickup_hour
* trip_duration_minutes

Quality flags:

```
is_refund
is_zero_distance
is_passenger_unknown
```

---

### weather_hourly

Flattened hourly weather dataset.

Columns:

```
hour_ts
temperature_2m
precipitation
weathercode
```

---

### dim_holidays

Holiday dimension table.

Columns:

```
holiday_date
holiday_name
```

---

## GOLD Layer (Analytics)

### fact_trips_enriched

Final **analytics-ready fact table**.

Joins:

```
taxi trips
+ weather
+ holidays
```

Example columns:

```
pickup_datetime
trip_distance
temperature_2m
weathercode
holiday_name
```

This is the **serving layer for dashboards / ML models**.

---

# Data Quality

The pipeline includes **21 dbt tests**.

Examples:

* not_null tests
* uniqueness tests
* referential integrity
* custom SLA test

### Custom Quality Test

Passenger count null rate must remain below threshold.

Results:

```
Total trips: 2,964,550
Passenger nulls: 140,114
Null rate: ~4.7%
```

---

# CI/CD Pipeline

GitHub Actions automatically runs:

```
dbt run
dbt test
```

On every commit.

Snowflake credentials are stored securely using **GitHub Secrets**.

Secrets used:

```
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_ROLE
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
```

---

# Project Structure

```
nyc-taxi-etl-dbt-snowflake
│
├── nyc_etl_dbt
│   ├── models
│   │   ├── silver
│   │   │   ├── taxi_trips_clean.sql
│   │   │   ├── weather_hourly.sql
│   │   │   └── dim_holidays.sql
│   │   │
│   │   └── gold
│   │       └── fact_trips_enriched.sql
│   │
│   └── tests
│       └── passenger_count_null_rate.sql
│
└── .github
    └── workflows
        └── dbt-ci.yml
```

---

# Running the Project

### Prerequisites

* Python 3.10+
* dbt-snowflake
* Snowflake account

### Run locally

```
cd nyc_etl_dbt

dbt debug
dbt run
dbt test
```

---

# Highlights

* Built an **ELT pipeline using Snowflake + dbt** implementing **Medallion Architecture**
* Modeled **~2.96M NYC taxi trips** enriched with **weather and holiday context**
* Implemented **21 automated dbt data quality tests**
* Added **CI/CD automation with GitHub Actions**
* Delivered **analytics-ready fact tables for BI and ML**
