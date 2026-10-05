# %% [markdown]
# # Big Data Analysis of Air Quality in Indian Cities and AQI Prediction using Apache Spark
# Mini project – Big Data Analysis
#
# **Dataset:** Air Quality Data in India (2015–2020), `city_day.csv` (CPCB data, published on Kaggle by Vopani)
#
# **Tools:** PySpark (Spark SQL + MLlib), Matplotlib, Seaborn
#
# Pipeline: Data ingestion → Cleaning → Spark SQL analysis → Correlation → MLlib model → Visualization

# %% [markdown]
# ## 0. Setup
# On Google Colab, run this first:
# ```
# !pip install pyspark
# ```
# Then upload `city_day.csv` (download it from Kaggle: "Air Quality Data in India (2015 - 2020)").

# %%
import os
import json
import matplotlib
matplotlib.use("Agg")  # comment this line out in Colab/Jupyter to see plots inline
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, VectorAssembler
from pyspark.ml.regression import (LinearRegression, DecisionTreeRegressor,
                                   RandomForestRegressor, GBTRegressor)
from pyspark.ml.evaluation import RegressionEvaluator

# Input path. Default: local file. With the Hadoop setup (hadoop/), this is an HDFS path,
# e.g. hdfs://namenode:9000/aqi/city_day.csv, set through the AQI_DATA_PATH environment variable.
DATA_PATH = os.environ.get("AQI_DATA_PATH", "data/city_day.csv")
ON_HDFS = DATA_PATH.startswith("hdfs://")
HDFS_OUT = DATA_PATH.rsplit("/", 1)[0] + "/output" if ON_HDFS else None
OUT = "outputs"
os.makedirs(OUT, exist_ok=True)
sns.set_theme(style="whitegrid")
results = {}

# %% [markdown]
# ## 1. Module 1 – Data Ingestion
# Create a Spark session and load the CSV into a distributed Spark DataFrame.

# %%
spark = (SparkSession.builder
         .appName("AQI_BigData_Analysis")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))   # all CPU cores by default
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))  # Random Forest needs more than 1 GB
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print("Spark version:", spark.version)
print("Reading data from:", DATA_PATH, "(HDFS)" if ON_HDFS else "(local file)")
results["data_source"] = {"path": DATA_PATH, "storage": "HDFS" if ON_HDFS else "local file"}

raw = spark.read.csv(DATA_PATH, header=True, inferSchema=True)
raw.printSchema()
n_rows, n_cols = raw.count(), len(raw.columns)
print(f"Rows: {n_rows}, Columns: {n_cols}")
raw.show(5)

POLLUTANTS = ["PM2.5", "PM10", "NO", "NO2", "NOx", "NH3", "CO",
              "SO2", "O3", "Benzene", "Toluene", "Xylene"]

# Column names containing '.' (PM2.5) are awkward in Spark, so rename them
df = raw.withColumnRenamed("PM2.5", "PM25")
POLL = [c.replace("PM2.5", "PM25") for c in POLLUTANTS]

# %% [markdown]
# ## 2. Module 2 – Data Exploration and Cleaning

# %%
# Missing values per column (%)
null_pct = df.select([
    F.round(F.avg(F.col(c).isNull().cast("int")) * 100, 2).alias(c) for c in df.columns
]).toPandas().T.reset_index()
null_pct.columns = ["column", "missing_pct"]
print(null_pct)

stats = df.agg(F.countDistinct("City").alias("cities"),
               F.min("Date").alias("start"), F.max("Date").alias("end")).collect()[0]
print("Cities:", stats["cities"], "| Date range:", stats["start"], "to", stats["end"])

results["overview"] = {"rows": n_rows, "cols": n_cols, "cities": stats["cities"],
                       "start": str(stats["start"])[:10], "end": str(stats["end"])[:10],
                       "aqi_missing_pct": float(null_pct.set_index("column").loc["AQI", "missing_pct"]),
                       "missing": null_pct.set_index("column")["missing_pct"].to_dict()}

# %%
# Cleaning steps
clean = (df
    .withColumn("Date", F.to_date("Date"))
    .dropDuplicates(["City", "Date"])                 # one record per city per day
    .filter(F.col("AQI").isNotNull())                 # AQI is our target – drop rows without it
    .filter((F.col("AQI") > 0) & (F.col("AQI") <= 1000)))

# Negative pollutant readings are sensor errors -> set to null
for c in POLL:
    clean = clean.withColumn(c, F.when(F.col(c) < 0, None).otherwise(F.col(c).cast(DoubleType())))

# Feature engineering: Year, Month, Season (Indian seasons)
clean = (clean
    .withColumn("Year", F.year("Date"))
    .withColumn("Month", F.month("Date"))
    .withColumn("Season",
        F.when(F.col("Month").isin(12, 1, 2), "Winter")
         .when(F.col("Month").isin(3, 4, 5), "Summer")
         .when(F.col("Month").isin(6, 7, 8, 9), "Monsoon")
         .otherwise("Post-Monsoon")))

clean.cache()                                         # reused many times -> keep in memory
n_clean = clean.count()
print("Rows after cleaning:", n_clean)
results["overview"]["clean_rows"] = n_clean
clean.createOrReplaceTempView("aqi")                  # register for Spark SQL

# %% [markdown]
# ## 3. Module 3 – Analysis with Spark SQL

# %%
# 3.1 Average AQI by city
city_aqi = spark.sql("""
    SELECT City, ROUND(AVG(AQI),1) AS avg_aqi, ROUND(AVG(PM25),1) AS avg_pm25, COUNT(*) AS days
    FROM aqi GROUP BY City ORDER BY avg_aqi DESC
""").toPandas()
print(city_aqi)
results["city_aqi"] = city_aqi.to_dict(orient="records")

plt.figure(figsize=(10, 7))
sns.barplot(data=city_aqi, y="City", x="avg_aqi", hue="City", palette="RdYlGn", legend=False)
plt.axvline(100, ls="--", c="grey"); plt.axvline(200, ls="--", c="grey")
plt.title("Average AQI by City (2015–2020)"); plt.xlabel("Average AQI"); plt.ylabel("")
plt.tight_layout(); plt.savefig(f"{OUT}/01_city_avg_aqi.png", dpi=150); plt.close()

# %%
# 3.2 Yearly trend (national average)
yearly = spark.sql("""
    SELECT Year, ROUND(AVG(AQI),1) AS avg_aqi, COUNT(DISTINCT City) AS cities
    FROM aqi GROUP BY Year ORDER BY Year
""").toPandas()
print(yearly)
results["yearly"] = yearly.to_dict(orient="records")

# Fair comparison: only cities that have data in every year 2015-2020
# (new, cleaner cities joined the network later and would pull the average down)
yearly_fixed = spark.sql("""
    WITH full_cities AS (
        SELECT City FROM aqi GROUP BY City HAVING COUNT(DISTINCT Year) = 6
    )
    SELECT Year, ROUND(AVG(AQI),1) AS avg_aqi
    FROM aqi WHERE City IN (SELECT City FROM full_cities)
    GROUP BY Year ORDER BY Year
""").toPandas()
full_cities = [r.City for r in spark.sql(
    "SELECT City FROM aqi GROUP BY City HAVING COUNT(DISTINCT Year) = 6").collect()]
print("Cities with data in all 6 years:", full_cities)
print(yearly_fixed)
results["yearly_fixed"] = yearly_fixed.to_dict(orient="records")
results["full_cities"] = full_cities

# 3.3 Monthly pattern
monthly = spark.sql("""
    SELECT Month, ROUND(AVG(AQI),1) AS avg_aqi, ROUND(AVG(PM25),1) AS avg_pm25
    FROM aqi GROUP BY Month ORDER BY Month
""").toPandas()
print(monthly)
results["monthly"] = monthly.to_dict(orient="records")

fig, ax = plt.subplots(1, 2, figsize=(13, 4.5))
ax[0].plot(yearly["Year"], yearly["avg_aqi"], marker="o", c="tab:grey", label="All cities")
ax[0].plot(yearly_fixed["Year"], yearly_fixed["avg_aqi"], marker="o", c="tab:red",
           label=f"{len(full_cities)} cities with data every year")
ax[0].legend(); ax[0].set_title("Average AQI by Year"); ax[0].set_xlabel("Year"); ax[0].set_ylabel("AQI")
months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
norm = plt.Normalize(monthly["avg_aqi"].min(), monthly["avg_aqi"].max())
ax[1].bar(months, monthly["avg_aqi"], color=plt.cm.RdYlGn_r(norm(monthly["avg_aqi"])))
ax[1].set_title("Average AQI by Month"); ax[1].set_ylabel("AQI")
plt.tight_layout(); plt.savefig(f"{OUT}/02_yearly_monthly_trend.png", dpi=150); plt.close()

# %%
# 3.4 Seasonal pattern
season = spark.sql("""
    SELECT Season, ROUND(AVG(AQI),1) AS avg_aqi
    FROM aqi GROUP BY Season ORDER BY avg_aqi DESC
""").toPandas()
print(season)
results["season"] = season.to_dict(orient="records")

# 3.5 AQI category distribution
bucket = spark.sql("""
    SELECT AQI_Bucket, COUNT(*) AS days, ROUND(COUNT(*)*100.0/(SELECT COUNT(*) FROM aqi),1) AS pct
    FROM aqi WHERE AQI_Bucket IS NOT NULL GROUP BY AQI_Bucket ORDER BY days DESC
""").toPandas()
print(bucket)
results["bucket"] = bucket.to_dict(orient="records")

order = ["Good", "Satisfactory", "Moderate", "Poor", "Very Poor", "Severe"]
colors = ["#2e7d32", "#9ccc65", "#fdd835", "#fb8c00", "#e53935", "#7b1fa2"]
b = bucket.set_index("AQI_Bucket").reindex(order)
plt.figure(figsize=(8, 4.5))
plt.bar(order, b["days"], color=colors)
for i, v in enumerate(b["pct"]):
    plt.text(i, b["days"].iloc[i], f"{v}%", ha="center", va="bottom")
plt.title("Number of City-Days in Each AQI Category"); plt.ylabel("City-days")
plt.tight_layout(); plt.savefig(f"{OUT}/03_aqi_category_distribution.png", dpi=150); plt.close()

# %%
# 3.6 Monthly heatmap for major metro cities
metros = ["Delhi", "Mumbai", "Kolkata", "Chennai", "Bengaluru", "Hyderabad", "Ahmedabad", "Lucknow"]
heat = spark.sql(f"""
    SELECT City, Month, ROUND(AVG(AQI),0) AS avg_aqi FROM aqi
    WHERE City IN ({",".join(f"'{c}'" for c in metros)})
    GROUP BY City, Month
""").toPandas().pivot(index="City", columns="Month", values="avg_aqi")
heat.columns = months
plt.figure(figsize=(12, 5))
sns.heatmap(heat, annot=True, fmt=".0f", cmap="RdYlGn_r", cbar_kws={"label": "Average AQI"})
plt.title("Average AQI by Month – Major Cities"); plt.ylabel("")
plt.tight_layout(); plt.savefig(f"{OUT}/04_metro_month_heatmap.png", dpi=150); plt.close()
results["heat"] = heat.to_dict()

# %%
# 3.7 Why is Ahmedabad so high? Compare its pollutant averages with Delhi
diag = spark.sql("""
    SELECT City, ROUND(AVG(AQI),1) AS aqi, ROUND(AVG(PM25),1) AS pm25, ROUND(AVG(PM10),1) AS pm10,
           ROUND(AVG(CO),2) AS co, ROUND(AVG(NO2),1) AS no2, ROUND(AVG(SO2),1) AS so2
    FROM aqi WHERE City IN ('Ahmedabad','Delhi','Mumbai') GROUP BY City
""").toPandas()
print(diag)
results["diag"] = diag.to_dict(orient="records")

# %% [markdown]
# ## 4. Module 4 – Effect of the 2020 COVID-19 Lockdown
# Compare the lockdown period (25 Mar – 31 May 2020) with the same dates in 2019.

# %%
lockdown = spark.sql("""
    SELECT City,
      ROUND(AVG(CASE WHEN Date BETWEEN '2019-03-25' AND '2019-05-31' THEN AQI END),1) AS aqi_2019,
      ROUND(AVG(CASE WHEN Date BETWEEN '2020-03-25' AND '2020-05-31' THEN AQI END),1) AS aqi_2020,
      ROUND(AVG(CASE WHEN Date BETWEEN '2019-03-25' AND '2019-05-31' THEN PM25 END),1) AS pm25_2019,
      ROUND(AVG(CASE WHEN Date BETWEEN '2020-03-25' AND '2020-05-31' THEN PM25 END),1) AS pm25_2020,
      ROUND(AVG(CASE WHEN Date BETWEEN '2019-03-25' AND '2019-05-31' THEN NO2 END),1) AS no2_2019,
      ROUND(AVG(CASE WHEN Date BETWEEN '2020-03-25' AND '2020-05-31' THEN NO2 END),1) AS no2_2020
    FROM aqi GROUP BY City
""").dropna(subset=["aqi_2019", "aqi_2020"]) \
    .withColumn("aqi_change_pct", F.round((F.col("aqi_2020") - F.col("aqi_2019")) / F.col("aqi_2019") * 100, 1)) \
    .orderBy("aqi_change_pct").toPandas()
print(lockdown)
results["lockdown"] = lockdown.to_dict(orient="records")

overall = spark.sql("""
    SELECT
      ROUND(AVG(CASE WHEN Date BETWEEN '2019-03-25' AND '2019-05-31' THEN AQI END),1) AS aqi_2019,
      ROUND(AVG(CASE WHEN Date BETWEEN '2020-03-25' AND '2020-05-31' THEN AQI END),1) AS aqi_2020,
      ROUND(AVG(CASE WHEN Date BETWEEN '2019-03-25' AND '2019-05-31' THEN PM25 END),1) AS pm25_2019,
      ROUND(AVG(CASE WHEN Date BETWEEN '2020-03-25' AND '2020-05-31' THEN PM25 END),1) AS pm25_2020,
      ROUND(AVG(CASE WHEN Date BETWEEN '2019-03-25' AND '2019-05-31' THEN NO2 END),1) AS no2_2019,
      ROUND(AVG(CASE WHEN Date BETWEEN '2020-03-25' AND '2020-05-31' THEN NO2 END),1) AS no2_2020
    FROM aqi WHERE City IN ({})
""".format(",".join(f"'{c}'" for c in lockdown["City"]))).toPandas().iloc[0].to_dict()
print(overall)
results["lockdown_overall"] = {k: float(v) for k, v in overall.items()}

plt.figure(figsize=(10, 6))
ld = lockdown.sort_values("aqi_2019", ascending=True)
y = range(len(ld))
plt.barh([i + 0.2 for i in y], ld["aqi_2019"], height=0.4, label="2019 (25 Mar–31 May)", color="#e57373")
plt.barh([i - 0.2 for i in y], ld["aqi_2020"], height=0.4, label="2020 lockdown", color="#64b5f6")
plt.yticks(list(y), ld["City"]); plt.xlabel("Average AQI")
plt.title("AQI Before vs During COVID-19 Lockdown"); plt.legend()
plt.tight_layout(); plt.savefig(f"{OUT}/05_lockdown_comparison.png", dpi=150); plt.close()

# %% [markdown]
# ## 5. Module 5 – Correlation of Pollutants with AQI

# %%
corr = {c: round(clean.stat.corr(c, "AQI"), 3) for c in POLL}
corr_s = pd.Series(corr).sort_values(ascending=False)
print(corr_s)
results["corr"] = corr_s.to_dict()

pdf = clean.select(POLL + ["AQI"]).toPandas().rename(columns={"PM25": "PM2.5"})
plt.figure(figsize=(10, 8))
sns.heatmap(pdf.corr(), annot=True, fmt=".2f", cmap="coolwarm", center=0)
plt.title("Correlation Matrix of Pollutants and AQI")
plt.tight_layout(); plt.savefig(f"{OUT}/06_correlation_heatmap.png", dpi=150); plt.close()

# %% [markdown]
# ## 6. Module 6 – AQI Prediction with Spark MLlib
# Features: 12 pollutant concentrations. Target: AQI.
# Missing feature values are filled with the median (Imputer). Data split 80% train / 20% test.

# %%
ml_df = clean.select(POLL + ["AQI"])
imputed_cols = [c + "_imp" for c in POLL]
imputer = Imputer(inputCols=POLL, outputCols=imputed_cols, strategy="median")
assembler = VectorAssembler(inputCols=imputed_cols, outputCol="features")

train, test = ml_df.randomSplit([0.8, 0.2], seed=42)
print("Train rows:", train.count(), "| Test rows:", test.count())

models = {
    "Linear Regression": LinearRegression(featuresCol="features", labelCol="AQI"),
    "Decision Tree": DecisionTreeRegressor(featuresCol="features", labelCol="AQI", maxDepth=8, seed=42),
    "Random Forest": RandomForestRegressor(featuresCol="features", labelCol="AQI",
                                           numTrees=100, maxDepth=10, seed=42),
    "Gradient Boosted Trees": GBTRegressor(featuresCol="features", labelCol="AQI",
                                           maxIter=60, maxDepth=6, seed=42),
}

ev = {m: RegressionEvaluator(labelCol="AQI", predictionCol="prediction", metricName=m)
      for m in ["rmse", "mae", "r2"]}
metrics, fitted, preds = [], {}, {}
for name, algo in models.items():
    model = Pipeline(stages=[imputer, assembler, algo]).fit(train)
    p = model.transform(test)
    row = {"Model": name, **{k.upper() if k != "r2" else "R2": round(e.evaluate(p), 3) for k, e in ev.items()}}
    print(row)
    metrics.append(row); fitted[name] = model; preds[name] = p
metrics_df = pd.DataFrame(metrics).sort_values("RMSE")
print(metrics_df)
results["metrics"] = metrics_df.to_dict(orient="records")

# %%
best_name = metrics_df.iloc[0]["Model"]
print("Best model:", best_name)
results["best"] = best_name

# Feature importance from Random Forest
rf = fitted["Random Forest"].stages[-1]
imp = pd.Series(rf.featureImportances.toArray(), index=POLLUTANTS).sort_values(ascending=False)
print(imp)
results["importance"] = imp.round(4).to_dict()

fig, ax = plt.subplots(1, 2, figsize=(14, 5))
sns.barplot(x=imp.values, y=imp.index, ax=ax[0], hue=imp.index, palette="viridis", legend=False)
ax[0].set_title("Feature Importance (Random Forest)"); ax[0].set_xlabel("Importance"); ax[0].set_ylabel("")
sample = preds[best_name].select("AQI", "prediction").sample(fraction=0.5, seed=1).toPandas()
ax[1].scatter(sample["AQI"], sample["prediction"], s=6, alpha=0.35)
lim = [0, max(sample["AQI"].max(), sample["prediction"].max())]
ax[1].plot(lim, lim, "r--", label="Perfect prediction")
ax[1].set_xlabel("Actual AQI"); ax[1].set_ylabel("Predicted AQI")
ax[1].set_title(f"Actual vs Predicted AQI – {best_name}"); ax[1].legend()
plt.tight_layout(); plt.savefig(f"{OUT}/07_model_results.png", dpi=150); plt.close()

# %%
# Predict AQI for a new day (example input)
example = spark.createDataFrame([(180.0, 300.0, 20.0, 60.0, 50.0, 40.0, 1.5, 15.0, 40.0, 4.0, 15.0, 3.0)],
                                POLL)
pred = fitted[best_name].transform(example.withColumn("AQI", F.lit(0.0))).select("prediction").first()[0]
print(f"Predicted AQI for example input: {pred:.0f}")
results["example_pred"] = round(pred, 1)


# %% [markdown]
# ## 7. Module 7 – Calculating Every Day's AQI (CPCB method)
# The Indian National AQI is calculated in two steps:
# 1. Each pollutant's concentration is converted to a **sub-index** (0–500) using CPCB breakpoint tables
#    (straight-line interpolation between breakpoints).
# 2. The day's **AQI = the highest sub-index**. It is only valid when at least 3 pollutants are measured,
#    and at least one of them is PM2.5 or PM10.
#
# We calculate this for **every city-day in the dataset (all 29,531 rows)**, including days where the
# recorded AQI is missing, and compare our values with the recorded AQI.
# The same breakpoint table is exported to JSON so the web dashboard (frontend) uses identical logic.

# %%
# CPCB breakpoints: concentration breakpoints -> AQI breakpoints [0, 50, 100, 200, 300, 400, 500]
AQI_BP = [0, 50, 100, 200, 300, 400, 500]
CONC_BP = {
    "PM25": [0, 30, 60, 90, 120, 250, 380],      # µg/m³, 24-hour
    "PM10": [0, 50, 100, 250, 350, 430, 510],    # µg/m³, 24-hour
    "NO2":  [0, 40, 80, 180, 280, 400, 520],     # µg/m³, 24-hour
    "SO2":  [0, 40, 80, 380, 800, 1600, 2400],   # µg/m³, 24-hour
    "CO":   [0, 1, 2, 10, 17, 34, 51],           # mg/m³, 8-hour
    "O3":   [0, 50, 100, 168, 208, 748, 1288],   # µg/m³, 8-hour
    "NH3":  [0, 200, 400, 800, 1200, 1800, 2400] # µg/m³, 24-hour
}
# (The last breakpoint continues the slope of the "Severe" band, so values above it keep rising.)

def sub_index(col_name):
    """Build a Spark column expression for one pollutant's CPCB sub-index."""
    c, bp = F.col(col_name), CONC_BP[col_name]
    expr = None
    for i in range(1, len(bp)):
        lo_c, hi_c, lo_i, hi_i = bp[i-1], bp[i], AQI_BP[i-1], AQI_BP[i]
        seg = lo_i + (c - lo_c) * (hi_i - lo_i) / (hi_c - lo_c)
        cond = (c <= hi_c) if i < len(bp) - 1 else F.lit(True)   # last band: open-ended
        expr = F.when(cond, seg) if expr is None else expr.when(cond, seg)
    return F.when(c.isNull(), None).otherwise(expr)

SI_COLS = list(CONC_BP.keys())
daily = (df.withColumn("Date", F.to_date("Date"))
           .dropDuplicates(["City", "Date"]))
for c in SI_COLS:
    daily = daily.withColumn(c, F.when(F.col(c) < 0, None).otherwise(F.col(c).cast(DoubleType())))
    daily = daily.withColumn(f"SI_{c}", sub_index(c))

si = [F.col(f"SI_{c}") for c in SI_COLS]
n_valid = sum(F.when(x.isNotNull(), 1).otherwise(0) for x in si)
has_pm = F.col("SI_PM25").isNotNull() | F.col("SI_PM10").isNotNull()
daily = daily.withColumn("AQI_calc",
    F.when((n_valid >= 3) & has_pm, F.round(F.greatest(*si))).otherwise(None))

# Dominant pollutant = the one whose sub-index equals the AQI
dom = None
for c in SI_COLS:
    cond = F.round(F.col(f"SI_{c}")) == F.col("AQI_calc")
    dom = F.when(cond, F.lit(c.replace("PM25", "PM2.5"))) if dom is None else dom.when(cond, F.lit(c.replace("PM25", "PM2.5")))
daily = daily.withColumn("Dominant", dom)

def bucket(col):
    return (F.when(col <= 50, "Good").when(col <= 100, "Satisfactory").when(col <= 200, "Moderate")
             .when(col <= 300, "Poor").when(col <= 400, "Very Poor").otherwise("Severe"))
daily = daily.withColumn("Bucket_calc", F.when(F.col("AQI_calc").isNull(), None).otherwise(bucket(F.col("AQI_calc"))))
daily.cache()
daily.createOrReplaceTempView("daily")

# %%
check = spark.sql("""
    SELECT COUNT(*)                                             AS total_days,
           SUM(CASE WHEN AQI_calc IS NOT NULL THEN 1 ELSE 0 END) AS calculated_days,
           SUM(CASE WHEN AQI IS NOT NULL THEN 1 ELSE 0 END)      AS recorded_days,
           SUM(CASE WHEN AQI IS NULL AND AQI_calc IS NOT NULL THEN 1 ELSE 0 END) AS gaps_filled,
           SUM(CASE WHEN AQI IS NOT NULL AND AQI_calc IS NOT NULL THEN 1 ELSE 0 END) AS both_days,
           ROUND(AVG(CASE WHEN AQI IS NOT NULL AND AQI_calc IS NOT NULL THEN ABS(AQI - AQI_calc) END), 1) AS mae,
           ROUND(AVG(CASE WHEN AQI IS NOT NULL AND AQI_calc IS NOT NULL
                          THEN CASE WHEN AQI_Bucket = Bucket_calc THEN 1.0 ELSE 0.0 END END) * 100, 1) AS bucket_match_pct
    FROM daily
""").toPandas().iloc[0].to_dict()
check["corr"] = round(daily.filter("AQI IS NOT NULL AND AQI_calc IS NOT NULL").stat.corr("AQI", "AQI_calc"), 3)
check = {k: (float(v) if not isinstance(v, str) else v) for k, v in check.items()}
print(check)
results["daily_check"] = check

dominant = spark.sql("""
    SELECT Dominant, COUNT(*) AS days, ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER (), 1) AS pct
    FROM daily WHERE Dominant IS NOT NULL GROUP BY Dominant ORDER BY days DESC
""").toPandas()
print(dominant)
results["dominant"] = dominant.to_dict(orient="records")

# %%
# Chart: recorded vs calculated AQI
both = daily.filter("AQI IS NOT NULL AND AQI_calc IS NOT NULL").select("AQI", "AQI_calc") \
            .sample(fraction=0.4, seed=7).toPandas()
fig, ax = plt.subplots(1, 2, figsize=(14, 5))
ax[0].scatter(both["AQI"], both["AQI_calc"], s=5, alpha=0.3)
lim = [0, 1000]
ax[0].plot(lim, lim, "r--", label="Perfect agreement"); ax[0].set_xlim(lim); ax[0].set_ylim(lim)
ax[0].set_xlabel("Recorded AQI (CPCB, from hourly data)"); ax[0].set_ylabel("Calculated AQI (our Spark backend)")
ax[0].set_title(f"Recorded vs calculated daily AQI (r = {check['corr']})"); ax[0].legend()
sns.barplot(data=dominant, x="pct", y="Dominant", ax=ax[1], hue="Dominant", palette="viridis", legend=False)
ax[1].set_title("Dominant pollutant (share of days)"); ax[1].set_xlabel("% of days"); ax[1].set_ylabel("")
plt.tight_layout(); plt.savefig(f"{OUT}/08_daily_aqi_check.png", dpi=150); plt.close()

# %%
# Save every day's AQI (backend output) for the report and the web dashboard
out_cols = ["City", "Date", "PM25", "PM10", "NO2", "SO2", "CO", "O3", "NH3",
            "AQI", "AQI_calc", "Bucket_calc", "Dominant"]
daily_pd = daily.select(out_cols).orderBy("City", "Date").toPandas()
daily_pd.rename(columns={"PM25": "PM2.5", "AQI": "AQI_recorded"}).to_csv(f"{OUT}/daily_aqi.csv", index=False)

# Compact JSON for the frontend: one array per city, one entry per day
import math
def clean_num(v, nd=2):
    return None if v is None or (isinstance(v, float) and math.isnan(v)) else round(float(v), nd)
web = {"breakpoints": {"aqi": AQI_BP, "conc": CONC_BP}, "cities": {}}
for city, g in daily_pd.groupby("City"):
    web["cities"][city] = [[str(r.Date)[:10],
                            clean_num(r.PM25), clean_num(r.PM10), clean_num(r.NO2), clean_num(r.SO2), clean_num(r.CO),
                            clean_num(r.O3), clean_num(r.NH3), clean_num(r.AQI, 0), clean_num(r.AQI_calc, 0)]
                           for r in g.itertuples()]
with open(f"{OUT}/daily_aqi_web.json", "w") as f:
    json.dump(web, f, separators=(",", ":"))
print("Saved", len(daily_pd), "daily rows")

# On HDFS, also write the processed datasets back to the cluster as Parquet (columnar, compressed),
# partitioned by city so later jobs can read one city without scanning the rest.
if ON_HDFS:
    clean.write.mode("overwrite").parquet(f"{HDFS_OUT}/clean_city_day")
    daily.select(out_cols).write.mode("overwrite").partitionBy("City").parquet(f"{HDFS_OUT}/daily_aqi")
    print("Wrote Parquet to HDFS:", f"{HDFS_OUT}/clean_city_day", "and", f"{HDFS_OUT}/daily_aqi")
    results["data_source"]["hdfs_outputs"] = [f"{HDFS_OUT}/clean_city_day", f"{HDFS_OUT}/daily_aqi"]


# %% [markdown]
# ## 8. Module 8 – Export data for the web dashboard
# Everything the dashboard shows is calculated here by Spark and saved to `outputs/dashboard_data.json`.
# The dashboard (in the `dashboard/` folder) reads this file plus `outputs/daily_aqi_web.json`.

# %%
STATES = {"Ahmedabad": "Gujarat", "Aizawl": "Mizoram", "Amaravati": "Andhra Pradesh", "Amritsar": "Punjab",
          "Bengaluru": "Karnataka", "Bhopal": "Madhya Pradesh", "Brajrajnagar": "Odisha",
          "Chandigarh": "Chandigarh (UT)", "Chennai": "Tamil Nadu", "Coimbatore": "Tamil Nadu",
          "Delhi": "Delhi (NCT)", "Ernakulam": "Kerala", "Gurugram": "Haryana", "Guwahati": "Assam",
          "Hyderabad": "Telangana", "Jaipur": "Rajasthan", "Jorapokhar": "Jharkhand", "Kochi": "Kerala",
          "Kolkata": "West Bengal", "Lucknow": "Uttar Pradesh", "Mumbai": "Maharashtra", "Patna": "Bihar",
          "Shillong": "Meghalaya", "Talcher": "Odisha", "Thiruvananthapuram": "Kerala",
          "Visakhapatnam": "Andhra Pradesh"}
REGIONS = {"North": ["Delhi", "Gurugram", "Lucknow", "Amritsar", "Chandigarh", "Jaipur"],
           "East": ["Kolkata", "Patna", "Brajrajnagar", "Talcher", "Jorapokhar"],
           "North-east": ["Guwahati", "Shillong", "Aizawl"],
           "West": ["Mumbai", "Ahmedabad", "Bhopal"],
           "South": ["Chennai", "Bengaluru", "Hyderabad", "Coimbatore", "Kochi", "Ernakulam",
                     "Thiruvananthapuram", "Visakhapatnam", "Amaravati"]}
BUCKETS = ["Good", "Satisfactory", "Moderate", "Poor", "Very Poor", "Severe"]

def num(v, nd=1):
    return None if v is None or (isinstance(v, float) and math.isnan(v)) else round(float(v), nd)

def nice_date(d):
    d = pd.Timestamp(d)
    return f"{d.day} {d.strftime('%b %Y')}"

cp = clean.select("City", "Date", "AQI", "AQI_Bucket", "Year", "Month", "Season", *POLL).toPandas()
cp["Date"] = pd.to_datetime(cp["Date"])
city_avg = cp.groupby("City")["AQI"].mean()
city_rank = city_avg.rank(ascending=False, method="first").astype(int)
season_order = ["Winter", "Summer", "Monsoon", "Post-Monsoon"]
lock19 = (cp["Date"] >= "2019-03-25") & (cp["Date"] <= "2019-05-31")
lock20 = (cp["Date"] >= "2020-03-25") & (cp["Date"] <= "2020-05-31")

cities_out = {}
for city, g in cp.groupby("City"):
    a19, a20 = num(g.loc[lock19, "AQI"].mean()), num(g.loc[lock20, "AQI"].mean())
    bk = g["AQI_Bucket"].value_counts(normalize=True) * 100
    worst = g.loc[g["AQI"].idxmax()]
    cities_out[city] = {
        "state": STATES.get(city, ""),
        "rank": int(city_rank[city]),
        "avg": num(city_avg[city]),
        "pm25": num(g["PM25"].mean()),
        "days": int(len(g)),
        "first": nice_date(g["Date"].min()), "last": nice_date(g["Date"].max()),
        "monthly": [num(g.loc[g["Month"] == m, "AQI"].mean(), 0) for m in range(1, 13)],
        "season": [num(g.loc[g["Season"] == s, "AQI"].mean(), 0) for s in season_order],
        "pollutants": {k: num(g[c].mean(), 2 if c == "CO" else 1)
                       for k, c in [("PM2.5", "PM25"), ("PM10", "PM10"), ("NO2", "NO2"),
                                    ("SO2", "SO2"), ("CO", "CO"), ("O3", "O3")]},
        "buckets": [num(bk.get(b, 0.0)) for b in BUCKETS],
        "yearly": [[int(y), num(v, 0)] for y, v in g.groupby("Year")["AQI"].mean().items()],
        "worst_day": nice_date(worst["Date"]), "worst_aqi": int(worst["AQI"]),
        "best_aqi": int(g["AQI"].min()),
        "lockdown": None if a19 is None or a20 is None else {
            "aqi": [a19, a20], "change_pct": num((a20 - a19) / a19 * 100),
            "pm25": [num(g.loc[lock19, "PM25"].mean()), num(g.loc[lock20, "PM25"].mean())],
            "no2": [num(g.loc[lock19, "NO2"].mean()), num(g.loc[lock20, "NO2"].mean())]},
    }

regions_out = []
for name, members in REGIONS.items():
    regions_out.append({"name": name, "cities": len(members),
                        "avg": num(sum(cities_out[c]["avg"] for c in members) / len(members))})
regions_out.sort(key=lambda r: -r["avg"])

lo = results["lockdown_overall"]
dash = {
    "generated_by": f"Apache Spark {spark.version}",
    "overview": {
        "records": results["overview"]["rows"], "clean_records": n_clean,
        "cities": results["overview"]["cities"],
        "start": nice_date(results["overview"]["start"]), "end": nice_date(results["overview"]["end"]),
        "avg_aqi": num(cp["AQI"].mean()), "avg_pm25": num(cp["PM25"].mean()),
        "monthly": [num(m["avg_aqi"], 0) for m in results["monthly"]],
        "season": {s["Season"]: num(s["avg_aqi"], 0) for s in results["season"]},
        "buckets": [num(next((b["pct"] for b in results["bucket"] if b["AQI_Bucket"] == n), 0.0))
                    for n in BUCKETS],
        "lockdown": {"cities": len(results["lockdown"]),
                     "aqi": [lo["aqi_2019"], lo["aqi_2020"]], "pm25": [lo["pm25_2019"], lo["pm25_2020"]],
                     "no2": [lo["no2_2019"], lo["no2_2020"]],
                     "biggest_drops": [[r["City"], r["aqi_change_pct"]] for r in results["lockdown"][:5]]},
        "correlation": [[k.replace("PM25", "PM2.5"), v] for k, v in results["corr"].items()],
        "regions": regions_out,
    },
    "model": {"metrics": results["metrics"], "best": results["best"],
              "importance": [[k, v] for k, v in results["importance"].items()],
              "example": {"inputs": {"PM2.5": 180, "PM10": 300, "NO2": 60, "CO": 1.5, "SO2": 15},
                          "prediction": results["example_pred"]}},
    "daily_check": results["daily_check"],
    "dominant": results["dominant"],
    "cities": cities_out,
}
with open(f"{OUT}/dashboard_data.json", "w") as f:
    json.dump(dash, f, indent=1, default=float)   # Spark ROUND() returns Decimal values
print("Saved dashboard data for", len(cities_out), "cities. Start the dashboard with: python run_dashboard.py")

# %%
with open(f"{OUT}/results.json", "w") as f:
    json.dump(results, f, indent=2, default=str)
spark.stop()
print("Done. Charts saved in", OUT)
