# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/formula1dl712/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType

# COMMAND ----------

lap_times_schema = StructType([
StructField("raceId", IntegerType(), False),
StructField("driverId", IntegerType(), True),
StructField("position", IntegerType(), True),
StructField("milliseconds", IntegerType(), True),
StructField("time", StringType(), True)


])

# COMMAND ----------

lap_times_df= spark.read\
.schema(lap_times_schema)\
.csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

add_df= add_ingestion_date(lap_times_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

lap_times_final_df= add_df.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumn("data_source", lit(v_data_source))


# COMMAND ----------

# MAGIC %md
# MAGIC lap_times_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

display(lap_times_final_df)

# COMMAND ----------

dbutils.notebook.exit("success")