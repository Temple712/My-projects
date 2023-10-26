# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

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
# MAGIC ### Read the JSON file using the spark dataframe reader

# COMMAND ----------

construct_schema= "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

df= spark.read\
.schema(construct_schema)\
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### drop the unwanted column

# COMMAND ----------

df_drop= df.drop("url")

# COMMAND ----------

display(df_drop)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

construct_select_df = df.withColumnRenamed("constructorId", "constructor_id")\
.withColumnRenamed("constructorRef", "constructor_ref")\
.withColumn("data_source", lit(v_data_source))\
.withColumn("p_file_date", lit(v_file_date))


# COMMAND ----------

construct_add_df= add_ingestion_date(construct_select_df)

# COMMAND ----------

construct_final_df = construct_add_df.drop("url")

# COMMAND ----------

display(construct_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC -- construct_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

construct_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl712/processed/constructors"))

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_processed.constructors

# COMMAND ----------

