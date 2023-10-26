# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Injest circuit.csv file

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date= dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 1 - Read the csv file using the spark dataframe reader 

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl712/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

# Define a schema 
circuit_schema = StructType([
    StructField("circuitid", IntegerType(), nullable=False),
    StructField("circuitRef", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("location", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("lat", DoubleType(), nullable=True),
    StructField("lng", DoubleType(), nullable=True),
    StructField("alt", IntegerType(), nullable=True),
    StructField("url", StringType(), nullable=True)
])


# COMMAND ----------

circuit_df= spark.read\
.option("header", True)\
.schema(circuit_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

display(circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select required columns

# COMMAND ----------

circuit_selected_df= circuit_df.select("circuitid", "circuitRef" , "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuit_selected_df= circuit_df.select(col("circuitid"), col("circuitRef") , col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display(circuit_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Rename cthe columns as required
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuit_selected_df.withColumnRenamed("circuitid", "circuit_id")\
.withColumnRenamed("circuitRef", "circuit_ref")\
.withColumnRenamed("lat", "latitude")\
.withColumnRenamed("lng", "longitude")\
.withColumnRenamed("alt", "altitude")\
.withColumn("data_souurce", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))


# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add injestion date to the dataframe

# COMMAND ----------

circuit_final_df= add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuit_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Write data to datalake as parquet file

# COMMAND ----------

# MAGIC %md
# MAGIC -- circuit_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

circuit_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/formula1dl712/processed

# COMMAND ----------

display(spark.read.parquet("dbfs:/mnt/formula1dl712/processed/circuits"))

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits

# COMMAND ----------

