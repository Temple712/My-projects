# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source= dbutils.widgets.get("p_data_source")

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

pit_stop_schema = StructType([
StructField("raceId", IntegerType(), False),
StructField("driverId", IntegerType(), True),
StructField("stop", StringType(), True),
StructField("lap", IntegerType(), True),
StructField("duration", StringType(), True),
StructField("milliseconds", IntegerType(), True),


])

# COMMAND ----------

df= spark.read\
.schema(pit_stop_schema)\
.option("multiLine", True)\
.json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

display(df)

# COMMAND ----------

add_df= add_ingestion_date(df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pit_spot_final_df= add_df.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumn("data_source", lit(v_data_source))


# COMMAND ----------

# MAGIC %md
# MAGIC pit_spot_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

pit_spot_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

display(pit_spot_final_df)

# COMMAND ----------

dbutils.notebook.exit("success")