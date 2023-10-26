# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

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

results_schema = StructType([ 
StructField("constructorId", IntegerType(), False),
StructField("driverId", IntegerType(), True),
StructField("fastestLap", IntegerType(), True),
StructField("fastestLapSpeed", StringType(), True),
StructField("fastestLapTime", StringType(), True),
StructField("grid", IntegerType(), True),
StructField("laps", IntegerType(), True),
StructField("milliseconds", IntegerType(), True),
StructField("number", IntegerType(), True),
StructField("points", FloatType(), True),
StructField("position", IntegerType(), True),
StructField("positionOrder", IntegerType(), True),
StructField("positionText", StringType(), True),
StructField("raceId", IntegerType(), True),
StructField("time", StringType(), True),
StructField("rank", IntegerType(), True),
StructField("statusId", IntegerType(), True),
StructField("resultId", IntegerType(), True)





])

# COMMAND ----------

df= spark.read\
.schema(results_schema)\
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

add_df= add_ingestion_date(df)

# COMMAND ----------

df.rename= add_df.withColumnRenamed("constructorId", "constructor_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumnRenamed("fastestLap", "fastest_lap")\
.withColumnRenamed("fastestLapTime", "fastest_lap_time")\
.withColumnRenamed("positionOrder", "position_order")\
.withColumnRenamed("positionText", "position_text")\
.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("resultId", "result_id")\
.withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
.withColumn("data_source", lit(v_data_source))\
.withColumn("p_file_date", lit(v_file_date))


# COMMAND ----------

display(df.rename)

# COMMAND ----------

results_final_df = df.rename.drop("statusId")

# COMMAND ----------

# MAGIC %md
# MAGIC results_final_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Method 1

# COMMAND ----------


#for race_id_list in results_final_df.select("race_id").distinct().collect():
#    if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#        spark.sql(f"alter table f1_processed.results drop if exists partition (race_id = {race_id_list.race_id})")

# COMMAND ----------

#results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Functions

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_column:
           column_list.append(column_name)
    column_list.append(partition_column)
    print(column_list)

    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

output_df = re_arrange_partition_column(results_final_df, "race_id")

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = re_arrange_partition_column(input_df, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
          output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
         output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

overwrite_partition(results_final_df, "f1_processed", "results", 'race_id')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## method 2

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_processed.results

# COMMAND ----------

#spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

#results_final_df= results_final_df.select("result_id", "driver_id", "constructor_id", "number", "grid", "position", "position_text", "position_order", "points", "laps", "time", "milliseconds", "fastest_lap", "rank", "fastest_lap_time", "fastest_lap_speed", "data_source", "p_file_date", "ingestion_date", "race_id")

# COMMAND ----------

#if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#    results_final_df.write.mode("overwrite").insertInto("f1_processed.results")
#else:
#    results_final_df.write.mode("overwrite").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")


# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select race_id, count(*) from f1_processed.results
# MAGIC group by race_id

# COMMAND ----------

