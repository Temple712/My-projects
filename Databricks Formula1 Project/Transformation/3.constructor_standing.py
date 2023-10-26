# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_result")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import col, sum, desc, count, when

# COMMAND ----------

constructor_df = race_results_df\
.groupby("race_year", "team")\
.agg(sum("points").alias("total_points"), count(when(col("position")== 1, True)).alias("wins"))

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank 

constructorRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
demo = constructor_df.withColumn("rank", rank().over(constructorRankSpec))

# COMMAND ----------

display(demo.filter("race_year == 2020"))

# COMMAND ----------

# MAGIC %md
# MAGIC demo.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------

demo.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")