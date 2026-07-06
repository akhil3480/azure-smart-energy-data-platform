# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

raw_holiday_path = "abfss://datalake@stsmartenergy.dfs.core.windows.net/raw/holiday/"
holiday_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(raw_holiday_path)
display(holiday_df)


# COMMAND ----------

holiday_df.printSchema()


# COMMAND ----------

print(f"Row coount {holiday_df.count()}")


# COMMAND ----------

print("Null dates:", holiday_df.filter(col("Bank holidays").isNull()).count())
print("Null types:", holiday_df.filter(col("Type").isNull()).count())
print("Duplicate Dates:", holiday_df.groupBy("Bank holidays").count().filter(col("count") > 1).count())

# COMMAND ----------

display(holiday_df.orderBy("Bank holidays"))

# COMMAND ----------

holiday_clean_df = holiday_df.select(
    col("Bank holidays").alias("holiday_date"), 
    col("Type").alias("holiday_name")
    )
display(holiday_clean_df)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

holiday_clean_df = holiday_clean_df.withColumn(
    "holiday_name",
    regexp_replace(col("holiday_name"), r"\?", "'")
)
display(holiday_clean_df)

# COMMAND ----------

(
    holiday_clean_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "path",
        "abfss://datalake@stsmartenergy.dfs.core.windows.net/silver/holiday/"
    )
    .saveAsTable("smart_energy_databricks.silver.holiday")
)

# COMMAND ----------

spark.sql("SHOW TABLES IN smart_energy_databricks.silver").show(truncate=False)