# 🏙️ SmartCity Energy Pipeline
### Microsoft Fabric | PySpark | Delta Lake | Power BI

> **"When should I use electricity?"**  
> This project answers that question by building an end-to-end Big Data Pipeline using Microsoft Fabric.

---

## 📌 Project Overview

This project is a **Big Data Engineering Pipeline** built on **Microsoft Fabric**.  
It collects real-time energy prices, weather, and air quality data from public APIs,  
processes it through **Bronze → Silver → Gold** layers, and visualizes insights in **Power BI**.

The main business question answered:
> **"Is now a good time to use electricity? Is the price cheap AND is renewable energy available (wind)?"**

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    DATA SOURCES (APIs)                   │
│   EnergyZero API   │   Open-Meteo API   │   Open-Meteo  │
│   (Energy Prices)  │   (Weather)        │   (Air Quality)│
└────────────────────────────┬────────────────────────────┘
                             │ Data Factory Pipeline
                             ▼
┌─────────────────────────────────────────────────────────┐
│                  BRONZE LAYER (Raw Data)                 │
│   bronze/energy/energy_raw.json                         │
│   bronze/weather/weather_raw.json                       │
│   bronze/air_quality/air_quality_raw.json               │
└────────────────────────────┬────────────────────────────┘
                             │ nb_generic_bronze_to_silver
                             ▼
┌─────────────────────────────────────────────────────────┐
│                  SILVER LAYER (Clean Data)               │
│   silver_energy        (48 records)                     │
│   silver_weather       (336 records)                    │
│   silver_air_quality   (288 records)                    │
└────────────────────────────┬────────────────────────────┘
                             │ nb_process_silver_to_gold
                             ▼
┌─────────────────────────────────────────────────────────┐
│                  GOLD LAYER (Analysis)                   │
│   gold_energy_analysis                                  │
│   (timestamp, price, temperature, wind_speed,           │
│    carbon_monoxide, pm10, avg_price_3h, opportunity)    │
└────────────────────────────┬────────────────────────────┘
                             │ Direct Lake
                             ▼
                      📊 Power BI Dashboard
```

---

## 🗂️ Project Structure

```
SmartCity_Lakehouse/
│
├── Files/
│   ├── bronze/
│   │   ├── energy/
│   │   │   └── energy_raw.json
│   │   ├── weather/
│   │   │   └── weather_raw.json
│   │   └── air_quality/
│   │       └── air_quality_raw.json
│   ├── silver/
│   └── gold/
│
└── Tables/
    ├── silver_energy
    ├── silver_weather
    ├── silver_air_quality
    └── gold_energy_analysis

Notebooks/
├── nb_config                      # Configuration
├── nb_logging                     # Logging system
├── nb_functions                   # Toolbox (7 functions)
├── nb_generic_bronze_to_silver    # ETL Engine
└── nb_process_silver_to_gold      # Analysis Engine
```

---

## 📓 Notebooks Explained

### 1️⃣ `nb_config` — Configuration (The Brain)

> **Single Source of Truth** for all constant values.

```python
LAKEHOUSE_NAME = "SmartCity_Lakehouse"
ROOT_PATH      = f"abfss://{LAKEHOUSE_NAME}@onelake.dfs.fabric.microsoft.com/..."
PATH_BRONZE    = f"{ROOT_PATH}/bronze"
PATH_SILVER    = f"{ROOT_PATH}/silver"
PATH_GOLD      = f"{ROOT_PATH}/gold"
FILE_FORMAT    = "json"
FULL_LOAD      = True
```

**Why?**  
Instead of hardcoding paths in 50 notebooks, we define them once here.  
If anything changes, we update only this file. This is called the **DRY Principle** (Don't Repeat Yourself).

---

### 2️⃣ `nb_logging` — Logging System (The Black Box)

> Professional logging instead of `print()`.

```python
def get_logger(name):
    logger = logging.getLogger(name)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ...
    return logger
```

**Log Levels:**

| Level | Meaning | When to use |
|-------|---------|-------------|
| DEBUG | Very detailed | Development only |
| INFO ✅ | Everything OK | Normal operations |
| WARNING ⚠️ | Watch out | Disk full, slow query |
| ERROR ❌ | Something failed | File not found |

**Why?**  
`print()` disappears when the screen closes. Logs persist with timestamps — like an airplane's black box.

---

### 3️⃣ `nb_functions` — Toolbox (7 Functions)

> Reusable tools following the **DRY Principle**.

| Function | Purpose |
|----------|---------|
| `clean_column_names(df)` | Converts column names to `snake_case` |
| `explode_and_flatten(df, col)` | Explodes nested JSON into flat table |
| `save_to_lakehouse(df, table)` | Saves DataFrame as Delta table |
| `generate_surrogate_key(df, cols)` | Creates unique MD5 hash ID per row |
| `create_time_series_frame(df, col)` | Converts and sorts by timestamp |
| `select_columns_safe(df, cols)` | Selects columns without crashing if missing |
| `join_dataframes(df1, df2, key)` | Joins two DataFrames on a common key |

**Example — clean_column_names:**
```
"Wind Speed (km/h)"  →  "wind_speed_km_h"
"Price.EUR"          →  "price_eur"
"First Name"         →  "first_name"
```

---

### 4️⃣ `nb_generic_bronze_to_silver` — ETL Engine (The Heart)

> **Metadata-Driven ETL** with Capsule Logic.

**Capsule Definition:**
```python
capsules = {
    "energy": {
        "source": "Files/bronze/energy/energy_raw.json",
        "table" : "silver_energy",
        "explode": "prices"
    },
    "weather": {
        "source": "Files/bronze/weather/weather_raw.json",
        "table" : "silver_weather",
        "zip"   : {"parent": "hourly", "children": ["time", "temperature_2m", ...]}
    },
    "air_quality": {
        "source": "Files/bronze/air_quality/air_quality_raw.json",
        "table" : "silver_air_quality",
        "zip"   : {"parent": "hourly", "children": ["time", "carbon_monoxide", ...]}
    }
}
```

**Engine (single loop processes all datasets):**
```python
for name, cap in capsules.items():
    if name == "energy":
        process_energy(cap["source"], cap["table"])
    else:
        process_zip(cap["source"], cap["table"], ...)
```

**Why Capsule Logic?**  
Like a washing machine — the engine is always the same, only the program changes.  
To add a new dataset tomorrow, just add a new capsule. No engine changes needed! 🚀

**Bronze → Silver Transformation:**

```
Energy JSON (nested):                    Silver Table (flat):
{                                        | timestamp        | price |
  "prices": [                            | 2024-12-01T00:00 | 0.05  |
    {"readingDate": "...", "price": 0.05}| 2024-12-01T01:00 | 0.08  |
  ]                                      | ...              | ...   |
}
```

---

### 5️⃣ `nb_process_silver_to_gold` — Analysis Engine

> Joins 3 Silver tables, calculates rolling averages, and labels opportunities.

**Step by step:**

```python
# Step 1: Read Silver tables
df_energy      = spark.read.format("delta").table("silver_energy")
df_weather     = spark.read.format("delta").table("silver_weather")
df_air_quality = spark.read.format("delta").table("silver_air_quality")

# Step 2: Align timestamp format
df_energy = df_energy.withColumn("timestamp", F.substring("timestamp", 1, 16))

# Step 3: Join all tables
df_gold = join_dataframes(df_energy, df_weather, "timestamp", "left")
df_gold = join_dataframes(df_gold, df_air_quality, "timestamp", "left")

# Step 4: Rolling 3-hour average price
window  = Window.orderBy("timestamp").rowsBetween(-3, 0)
df_gold = df_gold.withColumn("avg_price_3h", F.avg("price").over(window))

# Step 5: Opportunity label
df_gold = df_gold.withColumn("opportunity",
    F.when(
        (F.col("price") < F.col("avg_price_3h")) &
        (F.col("wind_speed_10m") > 20), "FIRSAT"
    ).otherwise("NORMAL")
)

# Step 6: Save to Gold layer
save_to_lakehouse(df_gold, "gold_energy_analysis")
```

**Gold Table Result:**

| timestamp | price | wind_speed | avg_price_3h | opportunity |
|-----------|-------|------------|--------------|-------------|
| 2024-12-01T00:00 | 0.05 | 32.2 | 0.08 | **FIRSAT** ✅ |
| 2024-12-01T01:00 | 0.08 | 28.4 | 0.065 | NORMAL |
| 2024-12-01T02:00 | 0.10 | 15.0 | 0.077 | NORMAL |

**FIRSAT = Price is below 3h average AND wind speed is high (renewable energy available)**

---

## 🔌 Data Sources (APIs)

| API | URL | Data |
|-----|-----|------|
| EnergyZero | `api.energyzero.nl` | Hourly electricity prices (Netherlands) |
| Open-Meteo Weather | `api.open-meteo.com` | Temperature, wind speed, solar radiation |
| Open-Meteo Air Quality | `air-quality-api.open-meteo.com` | CO, NO₂, PM10 |

---

## ⚙️ Tech Stack

| Technology | Purpose |
|-----------|---------|
| Microsoft Fabric | Cloud data platform |
| Apache Spark (PySpark) | Big data processing engine |
| Delta Lake | Reliable table storage format |
| Data Factory Pipeline | API → Bronze data ingestion |
| Power BI | Dashboard and visualization |
| Python | Notebook programming language |

---

## 📊 Power BI Dashboard

Connected to `gold_energy_analysis` via **Direct Lake** mode (no data copy).

**Visuals:**
- 📈 Price trend over time (Line Chart)
- 📊 FIRSAT vs NORMAL distribution (Bar Chart)
- 🌬️ Wind speed vs Price correlation (Scatter Chart)

---

## 🚀 How to Run

### Prerequisites
- Microsoft Fabric account (free trial available)
- Power BI Desktop

### Steps

**1. Set up Lakehouse**
```
app.fabric.microsoft.com → New → Lakehouse → SmartCity_Lakehouse
Create folders: bronze/energy, bronze/weather, bronze/air_quality, silver, gold
```

**2. Run Data Pipeline**
```
New → Data Pipeline → SmartCity_Pipeline
Add 3 Copy Data activities (Weather, Energy, Air Quality)
Run pipeline → JSON files saved to Bronze layer
```

**3. Run Notebooks in order**
```
1. nb_config                    → Loads configuration
2. nb_logging                   → Initializes logging
3. nb_functions                 → Loads 7 utility functions
4. nb_generic_bronze_to_silver  → Bronze → Silver transformation
5. nb_process_silver_to_gold    → Silver → Gold analysis
```

**4. Connect Power BI**
```
Fabric → EnergyModel → New Report
Connect gold_energy_analysis table
Build dashboard visuals
```

---

## 📈 Results

After running the pipeline:

| Layer | Tables | Records |
|-------|--------|---------|
| Bronze | 3 JSON files | Raw data |
| Silver | silver_energy | 48 |
| Silver | silver_weather | 336 |
| Silver | silver_air_quality | 288 |
| Gold | gold_energy_analysis | 48 (joined) |

---

## 🧠 Key Concepts Learned

- **Medallion Architecture** (Bronze → Silver → Gold)
- **Metadata-Driven ETL** (Capsule Logic)
- **DRY Principle** (Don't Repeat Yourself)
- **Delta Lake** (Reliable, versioned data storage)
- **Window Functions** (Rolling averages in PySpark)
- **Modular Code** (Config / Logging / Functions separation)

---

## 👤 Author

**Azizeke**  
Data Engineering Project — Microsoft Fabric  
April 2026
