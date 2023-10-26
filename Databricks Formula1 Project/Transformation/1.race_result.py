# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

circuit_df=spark.read.parquet(f"{processed_folder_path}/circuits")\
.withColumnRenamed("name", "circuit_name")\
.withColumnRenamed("location", "circuit_location")

# COMMAND ----------

display(circuit_df)

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")\
.withColumnRenamed("name", "driver_name")\
.withColumnRenamed ("nationality", "driver_nationality")\
.withColumnRenamed ("number", "driver_number")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

constructor_df = spark.read.parquet(f"{processed_folder_path}/constructors")\
.withColumnRenamed("name", "team")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")\
.withColumnRenamed("name", "races_name")\
.withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

display(races_df)

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results")\
.withColumnRenamed("time", "race_time")

# COMMAND ----------

display(results_df)

# COMMAND ----------

df = results_df.join(races_df, results_df.race_id == races_df.race_id, "inner")\
.select(results_df.grid, results_df.fastest_lap, results_df.race_time, results_df.points, results_df.constructor_id, results_df.driver_id, races_df.race_year, races_df.races_name, races_df.race_date, races_df.circuit_id, results_df.position)

# COMMAND ----------

display(df)

# COMMAND ----------

df1 = df.join(circuit_df, df.circuit_id == circuit_df.circuit_id, "inner")\
.select(results_df.grid, results_df.fastest_lap, results_df.race_time, results_df.points, results_df.constructor_id, results_df.driver_id, races_df.race_year, races_df.races_name, races_df.race_date, races_df.circuit_id, circuit_df.circuit_location, results_df.position)

# COMMAND ----------

display(df1)

# COMMAND ----------

df2 = df1.join(drivers_df, df1.driver_id == drivers_df.driver_id, "inner")\
.select(results_df.grid, results_df.fastest_lap, results_df.race_time, results_df.points, results_df.constructor_id, results_df.driver_id, races_df.race_year, races_df.races_name, races_df.race_date, races_df.circuit_id, circuit_df.circuit_location, drivers_df.driver_name, drivers_df.driver_nationality, drivers_df.driver_number, results_df.position)

# COMMAND ----------

display(df2)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

df3 = df2.join(constructor_df, constructor_df.constructor_id == df2.constructor_id)\
.select(results_df.grid, results_df.fastest_lap, results_df.race_time, results_df.points, races_df.race_year, races_df.races_name, races_df.race_date, circuit_df.circuit_location, drivers_df.driver_name, drivers_df.driver_nationality, drivers_df.driver_number, constructor_df.team, results_df.position)\
.withColumn("created_date", current_timestamp())

# COMMAND ----------

display(df3.filter("race_year ==2020 and races_name == 'Abu Dhabi Grand Prix' "))

# COMMAND ----------

# MAGIC %md
# MAGIC df_final = df3.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

df3.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_result")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.race_result

# COMMAND ----------

