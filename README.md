Big data pipeline (Hadoop + Spark)

Storage: the raw dataset (29,531 records, 26 cities, 2015–2020) lives in Hadoop HDFS, on a NameNode and DataNode running in Docker.
Ingestion: Spark 4.2 reads the CSV from hdfs:// into a distributed DataFrame.
Cleaning: removes duplicates and invalid readings, adds Year, Month and Season, and caches the result in memory for reuse.
Spark SQL analysis:
city rankings, and yearly, monthly and seasonal trends
share of days in each AQI category
comparison of the polluted metros
COVID-19 lockdown impact: compares AQI, PM2.5 and NO2 for March–May 2020 against 2019, overall and per city.
Correlation analysis: measures which pollutants drive AQI (PM2.5 is strongest, at 0.73).
Machine learning (MLlib): an Imputer → VectorAssembler → regressor pipeline trains four models (Linear Regression, Decision Tree, Random Forest, GBT). Random Forest is best at R² 0.908, and the pipeline also reports feature importances.
CPCB AQI engine: recalculates every city-day's AQI from raw pollutant levels with the official sub-index method, filling 895 days that had no recorded AQI.
Write-back: saves the cleaned data and the daily AQI to HDFS as compressed Parquet, partitioned by city.
Exports: writes 8 charts, results.json, daily_aqi.csv and the dashboard data files.

Dashboard (live on GitHub Pages)

Sign in / guest mode: a demo login screen.
India overview: national KPIs, all 26 cities ranked, pollution drivers, a regional comparison, the monthly pattern, the category split, lockdown impact and the model comparison.
City pages (26):
rank, monthly and seasonal AQI, and yearly trend
pollutants against the safe limits, plus best and worst days
lockdown effect, health advice, and a live AQI card
Live AQI: current AQI for all 26 cities, ranked, with the pollutant breakdown, a last-24-hours chart, a city dropdown and a refresh button. It uses Open-Meteo data converted with the CPCB formula.
Daily AQI calendar:
any date across all cities, and a month heat-grid
a full-year calendar for one city
a per-day breakdown of how the AQI was calculated
Backend vs browser check: recalculates all 29,531 days in the browser and matches the Spark output on 100% of them.
AQI calculator: enter pollutant levels and get the AQI, category and dominant pollutant instantly. It can be pre-filled from any day or from live data.
Go-to-city dropdown: jump to any city from any page; the layout also works on mobile.
