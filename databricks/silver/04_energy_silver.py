# Databricks notebook source
raw_energy_path = "abfss://datalake@stsmartenergy.dfs.core.windows.net/raw/energy/"

energy_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_energy_path)
)

display(energy_df)

# COMMAND ----------

energy_df.printSchema()

print(f"Rows: {energy_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Energy Dataset
# MAGIC
# MAGIC This notebook processes the daily household energy consumption dataset.
# MAGIC
# MAGIC Dataset characteristics:
# MAGIC - Source: ADLS Gen2 (Raw Layer)
# MAGIC - Format: CSV
# MAGIC - Total Records: 3,510,433
# MAGIC - Grain: One row per household per day
# MAGIC
# MAGIC This dataset will be cleaned and standardized before being written to the Silver layer as a Delta table.

# COMMAND ----------

## null  check
from pyspark.sql.functions import col, count, when

energy_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in energy_df.columns
]).show(truncate=False)

# COMMAND ----------

## Duplicate Check
from pyspark.sql.functions import col

energy_df.groupBy("LCLid", "day") \
    .count() \
    .filter(col("count") > 1) \
    .show()

# COMMAND ----------

## Distinct Household
print(
    f"Distinct Households: {energy_df.select('LCLid').distinct().count()}"
)

# COMMAND ----------

## Date Range
from pyspark.sql.functions import min, max

energy_df.select(
    min("day").alias("Start Date"),
    max("day").alias("End Date")
).show()

# COMMAND ----------

## Data Summary
energy_df.describe().show()

# COMMAND ----------

energy_df.filter(
    col("energy_median").isNull()
).display()

# COMMAND ----------

energy_df.filter(
    col("energy_std").isNull()
).select(
    "energy_count",
    "energy_std"
).display()

# COMMAND ----------

energy_df.filter(col("energy_count") == 0).display()

# COMMAND ----------

energy_df.filter(col("energy_count") == 0).count()

# COMMAND ----------

energy_df.count()

# COMMAND ----------

energy_df.filter(col("energy_count") == 0) \
         .select("LCLid") \
         .distinct() \
         .count()

# COMMAND ----------

energy_df.filter(col("LCLid") == "MAC005037") \
         .orderBy("day") \
         .display()

# COMMAND ----------

from pyspark.sql.functions import count

energy_df.filter(col("energy_count") == 0) \
    .groupBy("day") \
    .agg(count("*").alias("missing_households")) \
    .orderBy("day") \
    .display()

# COMMAND ----------

energy_df.filter(col("LCLid").isin(
    [r["LCLid"] for r in energy_df.filter(col("energy_count")==0).select("LCLid").collect()]
)).orderBy("LCLid","day").display()

# COMMAND ----------

from pyspark.sql.functions import col, max

# All households that contain a zero-count record
missing_households = (
    energy_df
    .filter(col("energy_count") == 0)
    .select("LCLid")
    .distinct()
)

# Find the last record for each of those households
display(
    energy_df
    .join(missing_households, "LCLid")
    .groupBy("LCLid")
    .agg(max("day").alias("last_day"))
    .orderBy("last_day", "LCLid")
)

# COMMAND ----------

from pyspark.sql.functions import col

missing_households = (
    energy_df
    .filter(col("energy_count") == 0)
    .select("LCLid")
    .distinct()
)

display(
    energy_df
    .join(missing_households, "LCLid")
    .orderBy("LCLid", "day")
)

# COMMAND ----------

from pyspark.sql.functions import col, min, max, when

zero_days = (
    energy_df
    .filter(col("energy_count") == 0)
    .groupBy("LCLid")
    .agg(min("day").alias("zero_day"))
)

last_days = (
    energy_df
    .groupBy("LCLid")
    .agg(max("day").alias("last_day"))
)

zero_vs_last = (
    zero_days
    .join(last_days, "LCLid", "left")
    .withColumn(
        "status",
        when(col("zero_day") == col("last_day"), "stopped_after_zero")
        .otherwise("continued_after_zero")
    )
)

display(
    zero_vs_last
    .groupBy("status")
    .count()
)

# COMMAND ----------

display(
    zero_vs_last
    .orderBy("status", "zero_day", "LCLid")
)

# COMMAND ----------

from pyspark.sql.functions import col

continued = [
    "MAC005558",
    "MAC005037",
    "MAC004248",
    "MAC002796",
    "MAC001478",
    "MAC002629",
    "MAC005510",
    "MAC003559"
]

display(
    energy_df
    .filter(col("LCLid").isin(continued))
    .orderBy("LCLid", "day")
)

# COMMAND ----------

# MAGIC %md
# MAGIC energy_count = 0 has two meanings:
# MAGIC 1. If zero_day = last_day → household stopped reporting after that day.
# MAGIC 2. If zero_day < last_day → isolated missing/invalid daily reading, but household continued reporting.

# COMMAND ----------

from pyspark.sql.functions import col, max, min, when

zero_days = (
    energy_df
    .filter(col("energy_count") == 0)
    .groupBy("LCLid")
    .agg(min("day").alias("day"))
)

last_days = (
    energy_df
    .groupBy("LCLid")
    .agg(max("day").alias("last_day"))
)

zero_vs_last = (
    zero_days
    .join(last_days, "LCLid", "left")
    .withColumn(
        "data_quality_status",
        when(col("day") == col("last_day"), "stopped_after_zero")
        .otherwise("isolated_missing_day")
    )
    .select("LCLid", "day", "data_quality_status")
)

energy_silver_df = (
    energy_df
    .join(zero_vs_last, ["LCLid", "day"], "left")
    .withColumn(
        "data_quality_status",
        when(col("data_quality_status").isNull(), "valid")
        .otherwise(col("data_quality_status"))
    )
)

# COMMAND ----------

display(
    energy_df
    .filter(col("energy_count") == 0)
    .groupBy("LCLid")
    .count()
    .orderBy(col("count").desc())
)

# COMMAND ----------

energy_silver_df.display() 

# COMMAND ----------

energy_silver_df.filter(col("data_quality_status") != "valid").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Assessment – Missing Energy Readings
# MAGIC
# MAGIC Before writing the cleaned dataset to the Silver layer, we perform a data quality assessment on daily energy records.
# MAGIC
# MAGIC ### Objective
# MAGIC
# MAGIC Identify households that contain daily records where no half-hourly readings were available (`energy_count = 0`).
# MAGIC
# MAGIC A zero reading count indicates that no measurements were recorded for that household on that day. However, this situation can occur for different reasons:
# MAGIC
# MAGIC - The household permanently stopped reporting data.
# MAGIC - The household resumed reporting on later dates, indicating only a temporary missing day.
# MAGIC
# MAGIC ### Data Quality Logic
# MAGIC
# MAGIC For every household containing at least one zero-count record:
# MAGIC
# MAGIC 1. Find the first day where `energy_count = 0`.
# MAGIC 2. Find the last available record for that household.
# MAGIC 3. Compare these two dates.
# MAGIC
# MAGIC Classification:
# MAGIC
# MAGIC - **stopped_after_zero**
# MAGIC   - The zero-count day is also the household's last available record.
# MAGIC   - Indicates that data collection stopped after this date.
# MAGIC
# MAGIC - **isolated_missing_day**
# MAGIC   - The household contains additional records after the zero-count day.
# MAGIC   - Indicates a temporary missing observation rather than a permanent stop.
# MAGIC
# MAGIC - **valid**
# MAGIC   - All remaining records with `energy_count > 0`.
# MAGIC
# MAGIC ### Result
# MAGIC
# MAGIC A new column named **`data_quality_status`** is added to the Silver dataset.
# MAGIC
# MAGIC This preserves the original source data while enriching it with quality metadata that can be used by downstream Gold transformations, reporting, and forecasting models.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver Dataset
# MAGIC
# MAGIC The transformed dataset is now written to the Silver layer in Delta format.
# MAGIC
# MAGIC ### Output
# MAGIC
# MAGIC - Cleansed daily household energy dataset
# MAGIC - Stored in Azure Data Lake Storage (Silver zone)
# MAGIC - Registered as a Unity Catalog managed table
# MAGIC - Includes the `data_quality_status` column for downstream quality-aware analytics
# MAGIC
# MAGIC ### Storage Format
# MAGIC
# MAGIC - Format: Delta Lake
# MAGIC - Write Mode: Overwrite
# MAGIC - Storage Location: Silver layer in ADLS Gen2
# MAGIC
# MAGIC ### Purpose
# MAGIC
# MAGIC The Silver dataset serves as the trusted, analytics-ready source for downstream Gold transformations. It contains validated records, standardized data types, and data quality metadata while preserving the original business information from the Bronze layer.

# COMMAND ----------

# Write enriched energy dataset to Silver Delta table

silver_energy_path = "abfss://datalake@stsmartenergy.dfs.core.windows.net/silver/energy_daily/"

(
    energy_silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("path", silver_energy_path)
    .saveAsTable("smart_energy_databricks.silver.energy_daily")
)

print("✓ Silver energy_daily table written successfully.")

# COMMAND ----------

# Validate Silver table

display(spark.table("smart_energy_databricks.silver.energy_daily").limit(10))

print(f"Total Records : {spark.table('smart_energy_databricks.silver.energy_daily').count():,}")

display(
    spark.table("smart_energy_databricks.silver.energy_daily")
         .groupBy("data_quality_status")
         .count()
)