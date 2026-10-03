# Air Quality Analysis of Indian Cities with Apache Spark

![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5%20%7C%204.x-E25A1C?logo=apachespark&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-SQL%20%2B%20MLlib-0B6A63)
![Dataset](https://img.shields.io/badge/Data-CPCB%202015--2020-4F5D59)

A big data pipeline that processes **29,531 days of air quality readings from 26 Indian cities (2015–2020)** with Apache Spark. It analyses pollution patterns with Spark SQL, predicts the Air Quality Index (AQI) with Spark MLlib, calculates every day's AQI using the official CPCB method, and presents the results in an interactive web dashboard.

**Mini project · Big Data Analysis**
Vismaya Katkar 
---

## Contents

- [Highlights](#highlights)
- [Key results](#key-results)
- [System architecture](#system-architecture)
- [Tech stack](#tech-stack)
- [Repository structure](#repository-structure)
- [Getting started](#getting-started)
- [Running the project](#running-the-project)
- [The dashboard](#the-dashboard)
- [How the daily AQI is calculated](#how-the-daily-aqi-is-calculated)
- [Results](#results)
- [Screenshots](#screenshots)
- [Troubleshooting](#troubleshooting)
- [Limitations and future work](#limitations-and-future-work)
- [Data source and references](#data-source-and-references)

---

## Highlights

- **Distributed processing:** the full pipeline (ingestion, cleaning, analysis and modelling) runs on Apache Spark and scales from a laptop (`local[*]`) to a Hadoop/YARN cluster without code changes.
- **Spark SQL analytics:** city rankings, yearly, monthly and seasonal trends, AQI category distribution, and the impact of the 2020 COVID-19 lockdown.
- **Machine learning:** four Spark MLlib regressors compared on a held-out test set. The best, Random Forest, predicts AQI with **R² = 0.908**.
- **CPCB AQI engine:** every city-day's AQI is recalculated from raw pollutant levels using the official sub-index method. This fills **895 days** where no AQI was recorded.
- **Verified end to end:** the web dashboard recalculates all 29,531 days in the browser and matches the Spark backend on **100%** of them.
- **Interactive dashboard:** India overview, a page for each of the 26 cities, a daily AQI calendar, and an AQI calculator.

## Key results

| Finding | Result |
| --- | --- |
| Most polluted major cities | Delhi (avg AQI 259.5), Patna (240.8), Gurugram (225.1), Lucknow (218.0) |
| Cleanest cities | Aizawl (34.8), Shillong (53.8), Coimbatore (73.0) |
| Worst months | November (229.4), December (224.5), January (223.6) |
| COVID-19 lockdown (25 Mar–31 May, 2020 vs 2019) | AQI −36%, PM2.5 −38%, NO2 −43% |
| Pollutant most linked to AQI | PM2.5 (correlation 0.73) |
| Best prediction model | Random Forest: R² 0.908, RMSE 37.34, MAE 21.76 |
| Daily AQI coverage (CPCB method) | 25,680 of 29,531 city-days |
| Calculated vs recorded AQI | Correlation 0.884; same category on 70.5% of days |
| Backend vs browser AQI | Identical on 29,531 of 29,531 days |

> Ahmedabad has the highest average AQI (388.3), but this is driven by unusually high CO readings (18.65 mg/m³, about nine times Delhi's level), which points to a likely sensor or reporting issue.

## System architecture

```
                         ┌───────────────────────────────┐
 city_day.csv ──────────▶│  Spark ingestion (DataFrame)  │
 (CPCB via Kaggle)       └───────────────┬───────────────┘
                                         ▼
                         ┌───────────────────────────────┐
                         │  Cleaning + feature building  │  duplicates, invalid readings,
                         │  (cached in memory)           │  Year / Month / Season
                         └─────┬──────────┬──────────┬───┘
                               ▼          ▼          ▼
                          Spark SQL   MLlib model   CPCB daily AQI
                          analysis    (4 models)    (sub-index method)
                               └──────────┬──────────┘
                                          ▼
                outputs/  charts (PNG), results.json, daily_aqi.csv,
                          dashboard_data.json, daily_aqi_web.json
                                          ▼
                       Web dashboard (dashboard/, served locally)
```

| Module | Description |
| --- | --- |
| 1. Ingestion | Loads the CSV into a distributed Spark DataFrame with an inferred schema |
| 2. Cleaning | Removes duplicates and invalid readings; adds Year, Month and Season |
| 3. Spark SQL analysis | City rankings, trends, seasonal patterns and AQI categories |
| 4. Lockdown impact | Compares 25 Mar–31 May 2020 with the same period in 2019 |
| 5. Correlation | Pearson correlation of each pollutant with AQI |
| 6. MLlib prediction | Linear Regression, Decision Tree, Random Forest, Gradient Boosted Trees |
| 7. Daily AQI | CPCB sub-indices for every city-day; AQI = highest sub-index |
| 8. Dashboard export | Writes all statistics the dashboard displays to `outputs/` |

## Tech stack

| Layer | Tools |
| --- | --- |
| Processing | Apache Spark (PySpark), Spark SQL |
| Machine learning | Spark MLlib (Imputer, VectorAssembler, Pipeline, regressors) |
| Analysis and charts | Python, pandas, Matplotlib, Seaborn, Jupyter |
| Dashboard | HTML, CSS, JavaScript (no framework), Python HTTP server |

## Repository structure

```
aqi-bigdata-india/
├── README.md
├── requirements.txt
├── run_dashboard.py              # starts the dashboard and opens the browser
├── data/
│   └── README.md                 # how to download city_day.csv
├── src/
│   └── aqi_project.py            # the full Spark pipeline (modules 1–8)
├── notebooks/
│   └── AQI_BigData_Project.ipynb # the pipeline step by step, with outputs
├── dashboard/
│   ├── index.html
│   ├── app.js                    # pages, charts and the in-browser AQI engine
│   └── style.css
├── outputs/
│   ├── 01_city_avg_aqi.png … 08_daily_aqi_check.png
│   ├── results.json              # every number used in the report
│   ├── dashboard_data.json       # data shown by the dashboard
│   └── daily_aqi.csv             # calculated AQI for every city-day
└── docs/
    ├── report.pdf                # project report
    └── screenshots/
```

`outputs/daily_aqi_web.json` (about 2 MB) is generated when you run the pipeline and is not committed.

## Getting started

### Prerequisites

| Requirement | Version |
| --- | --- |
| Python | 3.10 or newer |
| Java | 17 or newer for Spark 4.x (Spark 3.5 also works with Java 8, 11 or 17) |
| RAM | 8 GB recommended (Spark is given 4 GB) |

Check your versions:

```bash
python --version
java -version
```

### Installation

```bash
git clone https://github.com/katkarvismaya19-web/aqi-bigdata-india.git
cd aqi-bigdata-india
python -m venv .venv
```

Activate the virtual environment:

| Shell | Command |
| --- | --- |
| Windows PowerShell | `.venv\Scripts\Activate.ps1` |
| Windows Command Prompt | `.venv\Scripts\activate.bat` |
| macOS / Linux | `source .venv/bin/activate` |

Then install the dependencies:

```bash
pip install -r requirements.txt
```

### Dataset

Download `city_day.csv` from Kaggle, [Air Quality Data in India (2015–2020)](https://www.kaggle.com/datasets/rohanrao/air-quality-data-in-india), and place it in the `data/` folder. See [`data/README.md`](data/README.md) for details.

## Running the project

### 1. Run the Spark pipeline

**Windows PowerShell**

```powershell
$env:PYSPARK_PYTHON = "python"
python src\aqi_project.py
```

**macOS / Linux**

```bash
python src/aqi_project.py
```

The pipeline takes about 3–5 minutes. It writes the charts, `results.json`, `daily_aqi.csv` and the dashboard data to `outputs/`, and finishes with `Done. Charts saved in outputs`.

To run it step by step instead, open `notebooks/AQI_BigData_Project.ipynb` in Jupyter or Google Colab.

### 2. Start the dashboard

```bash
python run_dashboard.py
```

The dashboard opens at **http://localhost:8000/dashboard/**. Press `Ctrl + C` to stop it. To use another port, run `python run_dashboard.py 8080`.

You only need to rerun the pipeline when the code or data changes. After that, `python run_dashboard.py` on its own is enough.

### Running on a Hadoop cluster

```bash
hdfs dfs -put data/city_day.csv /data/
# In src/aqi_project.py: set DATA_PATH = "hdfs:///data/city_day.csv" and remove .master("local[*]")
spark-submit src/aqi_project.py
```

## The dashboard

| Page | What it shows |
| --- | --- |
| Sign in | Demo sign-in (format checks only; nothing is stored or sent), or continue as guest |
| India overview | National KPIs, all 26 cities ranked, pollution drivers, regional comparison, monthly pattern, AQI category share, lockdown impact, model comparison |
| City pages | For each city: rank, monthly and seasonal AQI, pollutants against safe limits, category share, yearly trend, record days, lockdown effect, health advice |
| Daily AQI calendar | Every city on any chosen date, a month grid of all cities, a full-year calendar for one city, and a per-day breakdown of how the AQI was calculated |
| AQI calculator | Enter pollutant levels and get the AQI and dominant pollutant instantly |

The dashboard reads only the files produced by the Spark pipeline, so every number it shows comes from the backend.

## How the daily AQI is calculated

The pipeline follows the CPCB National Air Quality Index method:

1. Each pollutant concentration (PM2.5, PM10, NO2, SO2, CO, O3, NH3) is converted to a **sub-index** by linear interpolation within its CPCB breakpoint band:

   `I = I_lo + (C − C_lo) × (I_hi − I_lo) / (C_hi − C_lo)`

2. The day's **AQI is the highest sub-index**.
3. An AQI is reported only when **at least three pollutants** are available and one of them is **PM2.5 or PM10**.

| AQI | Category | PM2.5 (µg/m³) | PM10 (µg/m³) |
| --- | --- | --- | --- |
| 0–50 | Good | 0–30 | 0–50 |
| 51–100 | Satisfactory | 31–60 | 51–100 |
| 101–200 | Moderate | 61–90 | 101–250 |
| 201–300 | Poor | 91–120 | 251–350 |
| 301–400 | Very Poor | 121–250 | 351–430 |
| 401–500 | Severe | 250+ | 430+ |

The calculated AQI correlates at 0.884 with the AQI recorded by CPCB. The two differ on some days because CPCB calculates from hourly data (24-hour averages, and 8-hour maximums for CO and O3), while this dataset provides daily averages.

## Results

| | |
| --- | --- |
| ![Average AQI by city](outputs/01_city_avg_aqi.png) | ![Yearly and monthly trends](outputs/02_yearly_monthly_trend.png) |
| ![AQI before and during the lockdown](outputs/05_lockdown_comparison.png) | ![Model results](outputs/07_model_results.png) |

![Recorded vs calculated daily AQI](outputs/08_daily_aqi_check.png)

The complete analysis is in the [project report](docs/report.pdf).

## Screenshots

| | |
| --- | --- |
| ![Sign in](docs/screenshots/login.png) | ![India overview](docs/screenshots/india-overview.png) |
| ![City page](docs/screenshots/city-page.png) | ![Daily AQI calendar](docs/screenshots/daily-aqi.png) |

## Troubleshooting

| Problem | Solution |
| --- | --- |
| `java.lang.OutOfMemoryError: Java heap space` | Spark is given 4 GB in `src/aqi_project.py` (`spark.driver.memory`). On machines with 8 GB of RAM or less, close other applications or reduce it to `"3g"`. |
| Warnings about `winutils.exe`, `HADOOP_HOME`, `NativeCodeLoader` or `libopenblas.dll` | Expected on Windows; they do not affect results. |
| Dashboard shows **Results not found** | Run `python src/aqi_project.py` first, then start the dashboard with `python run_dashboard.py`. Confirm that `outputs/dashboard_data.json` and `outputs/daily_aqi_web.json` both exist. |
| Dashboard is blank when opening `index.html` directly | Browsers block local file access. Always start it with `python run_dashboard.py`. |
| `running scripts is disabled on this system` (PowerShell) | Run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, then activate the environment again. |
| Port 8000 already in use | `run_dashboard.py` picks the next free port automatically, or pass one: `python run_dashboard.py 8080`. |

## Limitations and future work

**Limitations**

- Many readings are missing (up to 61% for Xylene), and imputed values add uncertainty.
- Cities joined the monitoring network in different years, so all-city averages over time must be read with care.
- Some stations report doubtful values, such as Ahmedabad's CO readings.
- The analysis covers January 2015 to July 2020 and does not update automatically.

**Future work**

- **Daily updates** from the Government of India's real-time AQI feed on data.gov.in, scheduled to refresh the data and dashboard every day.
- **Real-time streaming** with Apache Kafka and Spark Structured Streaming, with alerts when AQI crosses 200.
- **Forecasting** of next-day AQI using time-series models (ARIMA, LSTM) or lag features.
- **Weather data** such as temperature, wind speed, humidity and rainfall, which strongly affect pollution.
- **Station-level hourly data** processed on a multi-node cluster.

## Data source and references

1. Vopani (Rohan Rao), *Air Quality Data in India (2015–2020)*, Kaggle, compiled from Central Pollution Control Board (CPCB) data.
2. Central Pollution Control Board, *National Air Quality Index*, Ministry of Environment, Forest and Climate Change, Government of India, 2014.
3. M. Zaharia et al., "Apache Spark: A Unified Engine for Big Data Processing," *Communications of the ACM*, vol. 59, no. 11, 2016.
4. X. Meng et al., "MLlib: Machine Learning in Apache Spark," *Journal of Machine Learning Research*, vol. 17, 2016.
5. M. Armbrust et al., "Spark SQL: Relational Data Processing in Spark," *Proc. ACM SIGMOD*, 2015.
6. L. Breiman, "Random Forests," *Machine Learning*, vol. 45, 2001.

The dataset is not redistributed in this repository; please check its licence on Kaggle before sharing it.
