# Databricks notebook source
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

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/formula1dl712/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# COMMAND ----------

# Define a schema 
race_schema = StructType([
    StructField("raceId", IntegerType(), nullable=False),
    StructField("year", IntegerType(), nullable=True),
    StructField("round", IntegerType(), nullable=True),
    StructField("circuitId", IntegerType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("date", StringType(), nullable=True),
    StructField("time", StringType(), nullable=True)
])

# COMMAND ----------

df= spark.read\
.option("header", True)\
.schema(race_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

race_renamed_df = df.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("year", "race_year")\
.withColumnRenamed("circuitId", "circuit_id")\
.withColumn("data_source", lit(v_data_source))\
.withColumn("p_file_date", lit(v_file_date))
    

# COMMAND ----------

display(race_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import lit, to_timestamp, concat, col, current_timestamp

# COMMAND ----------

race_join_df = race_renamed_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

display(race_join_df)

# COMMAND ----------

race_new_column = add_ingestion_date(race_join_df)

# COMMAND ----------

display(race_new_column)

# COMMAND ----------

race_final_df= race_new_column.select("race_id", "race_year", "round", "circuit_id", "name", "race_timestamp", "ingestion_date")

# COMMAND ----------

# MAGIC %md
# MAGIC -- race_final_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/races")

# COMMAND ----------

race_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl712/processed/races"))

# COMMAND ----------

dbutils.notebook.exit("success")