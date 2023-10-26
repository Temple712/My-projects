-- Databricks notebook source
create database if not exists f1_processed
location "/mnt/formula1dl712/processed"


-- COMMAND ----------

-- circuit_final_df.write.mode("overwrite").format(parquet).saveAsTable("f1_processed.circuits")

-- COMMAND ----------

create database if not exists f1_presentation
location "/mnt/formula1dl712/presentation"

-- COMMAND ----------

show tables

-- COMMAND ----------

