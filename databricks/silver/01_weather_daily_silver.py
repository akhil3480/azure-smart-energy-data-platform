# Databricks notebook source
raw_weather_path = "abfss://datalake@stsmartenergy.dfs.core.windows.net/raw/weather_csv/"
weather_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(raw_weather_path)
display(weather_df)


# COMMAND ----------

weather_df.printSchema()

# COMMAND ----------

for c in weather_df.columns:
  print(c)

# COMMAND ----------

from pyspark.sql.functions import col, count, when

weather_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in weather_df.columns
]).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weather Silver Transformation Logic
# MAGIC
# MAGIC For the Silver weather table, we keep only daily weather features needed for energy analytics and forecasting.
# MAGIC
# MAGIC Kept columns:
# MAGIC - Daily temperature measures
# MAGIC - Humidity, pressure, wind, cloud cover, visibility
# MAGIC - UV index, dew point, moon phase
# MAGIC - Sunrise/sunset times
# MAGIC - Precipitation type and weather summary
# MAGIC
# MAGIC Removed columns:
# MAGIC - Exact event timing columns such as temperatureMaxTime and uvIndexTime
# MAGIC - Apparent temperature fields, because actual temperature fields are sufficient for this analysis
# MAGIC
# MAGIC Reason:
# MAGIC The project uses daily-grain energy consumption data, so the weather Silver table should also stay at daily grain and contain only useful daily-level features.

# COMMAND ----------

from pyspark.sql.functions import col, to_date

weather_clean_df = weather_df.select(
    to_date(col("time")).alias("weather_date"),
    col("temperatureMax").alias("temperature_max"),
    col("temperatureMin").alias("temperature_min"),
    col("temperatureHigh").alias("temperature_high"),
    col("temperatureLow").alias("temperature_low"),
    col("humidity"),
    col("pressure"),
    col("windSpeed").alias("wind_speed"),
    col("windBearing").alias("wind_bearing"),
    col("cloudCover").alias("cloud_cover"),
    col("visibility"),
    col("dewPoint").alias("dew_point"),
    col("uvIndex").alias("uv_index"),
    col("sunriseTime").alias("sunrise_time"),
    col("sunsetTime").alias("sunset_time"),
    col("moonPhase").alias("moon_phase"),
    col("precipType").alias("precip_type"),
    col("summary").alias("weather_summary")
)

display(weather_clean_df)

# COMMAND ----------

weather_clean_df.printSchema()

print(f"Clean Row Count: {weather_clean_df.count()}")
print(f"Distinct Dates: {weather_clean_df.select('weather_date').distinct().count()}")

# COMMAND ----------

weather_clean_df.groupBy("weather_date") \
    .count() \
    .filter(col("count") > 1) \
    .show(truncate=False)

# COMMAND ----------

weather_clean_df.filter(
    col("weather_date").isin(
        "2012-03-25",
        "2013-03-31",
        "2014-03-30"
    )
).orderBy("weather_date").display()

# COMMAND ----------

weather_df.filter(
    to_date(col("time")).isin(
        "2012-03-25",
        "2013-03-31",
        "2014-03-30"
    )
).select("time", "summary").orderBy("time").display()

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    max,
    min,
    avg,
    first
)

weather_daily_df = (
    weather_clean_df
    .groupBy("weather_date")
    .agg(
        max("temperature_max").alias("temperature_max"),
        min("temperature_min").alias("temperature_min"),
        max("temperature_high").alias("temperature_high"),
        min("temperature_low").alias("temperature_low"),

        avg("humidity").alias("humidity"),
        avg("pressure").alias("pressure"),
        avg("wind_speed").alias("wind_speed"),
        avg("wind_bearing").alias("wind_bearing"),
        avg("cloud_cover").alias("cloud_cover"),
        avg("visibility").alias("visibility"),
        avg("dew_point").alias("dew_point"),

        max("uv_index").alias("uv_index"),

        first("sunrise_time").alias("sunrise_time"),
        first("sunset_time").alias("sunset_time"),
        first("moon_phase").alias("moon_phase"),
        first("precip_type").alias("precip_type"),
        first("weather_summary").alias("weather_summary")
    )
)

display(weather_daily_df)

# COMMAND ----------

print(f"Rows: {weather_daily_df.count()}")
print(f"Distinct Dates: {weather_daily_df.select('weather_date').distinct().count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling Duplicate Weather Records
# MAGIC
# MAGIC During data profiling, three dates (2012-03-25, 2013-03-31, and 2014-03-30) contained two weather observations instead of one.
# MAGIC
# MAGIC Investigation showed that these were separate timestamped observations (00:00 and 23:00) rather than exact duplicate records. Since the energy consumption dataset is stored at a daily grain (one record per household per day), the weather dataset was aggregated to a single daily record.
# MAGIC
# MAGIC Aggregation rules:
# MAGIC - Maximum temperatures → MAX
# MAGIC - Minimum temperatures → MIN
# MAGIC - Humidity, pressure, wind, visibility and cloud cover → AVG
# MAGIC - UV Index → MAX
# MAGIC - Sunrise, sunset, moon phase, precipitation type and weather summary → FIRST
# MAGIC
# MAGIC This produces one weather record per day while preserving the most representative daily weather information.

# COMMAND ----------

weather_daily_df.printSchema()

display(weather_daily_df)

print(weather_daily_df.count()

# COMMAND ----------

(
    weather_daily_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "path",
        "abfss://datalake@stsmartenergy.dfs.core.windows.net/silver/weather_daily/"
    )
    .saveAsTable("smart_energy_databricks.silver.weather_daily")
)

# COMMAND ----------

spark.sql("SHOW TABLES IN smart_energy_databricks.silver").show(truncate=False)

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://datalake@stsmartenergy.dfs.core.windows.net/silver/weather_daily/"
    )
)

# COMMAND ----------

weather_check_df = spark.table("smart_energy_databricks.silver.weather_daily")

print(f"Rows: {weather_check_df.count()}")

display(weather_check_df)