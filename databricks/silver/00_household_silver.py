# Databricks notebook source
spark.sql("""
SHOW EXTERNAL LOCATIONS
""").show(truncate=False)

# COMMAND ----------

display(dbutils.fs.ls("abfss://datalake@stsmartenergy.dfs.core.windows.net/raw/"))

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS smart_energy_databricks.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS smart_energy_databricks.gold")

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN smart_energy_databricks").show(truncate=False)

# COMMAND ----------

raw_household_path = "abfss://datalake@stsmartenergy.dfs.core.windows.net/raw/household/"

household_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_household_path)
)

display(household_df.limit(10))

# COMMAND ----------

household_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import trim, col

household_clean_df = (
    household_df
    .withColumnRenamed("LCLid", "lclid")
    .withColumnRenamed("stdorToU", "tariff_type")
    .withColumnRenamed("Acorn", "acorn")
    .withColumnRenamed("Acorn_grouped", "acorn_grouped")
    .withColumnRenamed("file", "source_file")
    .dropDuplicates()
    .filter(col("lclid").isNotNull())
)

display(household_clean_df.limit(10))

# COMMAND ----------

print("raw count:",household_df.count())
print("silver count:",household_clean_df.count())
print("LCLid Distinct count:", household_clean_df.select("lclid").distinct().count())
print(household_clean_df.filter(col("lclid").isNull()).count())


# COMMAND ----------

household_clean_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path","abfss://datalake@stsmartenergy.dfs.core.windows.net/silver/household/").option("overwriteSchema", "true") \
    .saveAsTable("smart_energy_databricks.silver.household")

# COMMAND ----------

spark.sql("SHOW TABLES IN smart_energy_databricks.silver").show(truncate=False)

# COMMAND ----------

spark.sql("DESCRIBE DETAIL smart_energy_databricks.silver.household").show(truncate=False)

# COMMAND ----------

spark.sql("""
SELECT COUNT(*)
FROM smart_energy_databricks.silver.household
""").show()

# COMMAND ----------

dbutils.fs.ls("abfss://datalake@stsmartenergy.dfs.core.windows.net/raw/")