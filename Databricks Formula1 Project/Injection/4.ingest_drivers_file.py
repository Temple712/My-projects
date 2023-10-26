# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source= dbutils.widgets.get ("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date= dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/formula1dl712/raw

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Read nested Json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

name_schema = StructType([StructField("forename", StringType(), True),
                          StructField("surname", StringType(), True)
                          

])

# COMMAND ----------

drivers_schema = StructType([StructField("driverId", IntegerType(), False),
                          StructField("code", StringType(), True),
                          StructField("driverRef", StringType(), True),
                          StructField("name", name_schema, True),
                          StructField("dob", DateType(), True),
                          StructField("nationality", StringType(), True),
                          StructField("url", StringType(), True),
                          StructField("number", IntegerType(), True)





])

# COMMAND ----------

drivers_df= spark.read\
.schema(drivers_schema)\
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit, concat

# COMMAND ----------

add_df= add_ingestion_date(drivers_df)

# COMMAND ----------

drivers_rename = add_df.withColumnRenamed("driverId", "driver_id")\
.withColumnRenamed("driverRef", "driver_ref")\
.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
.withColumn("data_source", lit(v_data_source))\
.withColumn("p_file_date", lit(v_file_date))

# COMMAND ----------

display(drivers_rename)

# COMMAND ----------

drivers_final_df = drivers_rename.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC drivers_fina_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl712/processed/drivers"))

# COMMAND ----------

dbutils.notebook.exit("success")