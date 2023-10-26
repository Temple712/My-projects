# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_result")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col, desc
driver_standings_df = race_results_df\
.groupby("race_year", "driver_name", "driver_nationality", "team")\
.agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(driver_standings_df.filter("race_year =2020").orderBy(desc("wins")))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank 

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
demo = driver_standings_df.withColumn("rank", rank().over(driverRankSpec))

# COMMAND ----------

display(demo.filter("race_year = 2020"))

# COMMAND ----------

# MAGIC %md
# MAGIC demo.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------

demo.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")