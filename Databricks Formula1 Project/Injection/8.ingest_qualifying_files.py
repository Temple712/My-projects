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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

qualifying_schema = StructType([
StructField("raceId", IntegerType(), False),
StructField("driverId", IntegerType(), True),
StructField("position", IntegerType(), True),
StructField("constructorId", IntegerType(), True),
StructField("number", IntegerType(), True),
StructField("q1", StringType(), True),
StructField("q2", StringType(), True),
StructField("q3", StringType(), True),
StructField("qualifyId", IntegerType(), True)


])

# COMMAND ----------

qualifying_df= spark.read\
.schema(qualifying_schema)\
.option("multiLine", True)\
.json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

add_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualify_final_df= add_df.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumnRenamed("constructorId", "constructor_id")\
.withColumnRenamed("qualifyId", "qualify_id")\
.withColumn("data_source", lit(v_data_source))


# COMMAND ----------

# MAGIC %md
# MAGIC qualify_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

qualify_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

display(qualify_final_df)

# COMMAND ----------

dbutils.notebook.exit("success")