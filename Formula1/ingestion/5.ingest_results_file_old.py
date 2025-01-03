# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "testing")
v_data_source = dbutils.widgets.get("p_data_source")

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_ddl_schema = """
    resultId INT NOT NULL,
    raceId INT,
    driverId INT,
    constructorId INT,
    number INT,
    grid INT,
    position INT,
    positionText STRING,
    positionOrder INT,
    points FLOAT,
    laps INT,
    time STRING,
    milliseconds INT,
    fastestLap INT,
    rank INT,
    fastestLapTime STRING,
    fastestLapSpeed FLOAT,
    statusId STRING
"""

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))  

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write to output to processed container in parquet format

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1 incremental load
# MAGIC ##append

# COMMAND ----------

#  for race_id_list in results_final_df.select("race_id").distinct().collect():
#   if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### METHOD2
# MAGIC ## overwrite
# MAGIC

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

#  if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
   
#    results_final_df.write.mode("overwrite").insertInto("f1_processed.results")
   
#  else:
#     results_final_df.write.mode("overwrite").partitionBy('race_id').format("parquet").saveAsTable('f1_processed.results')

# COMMAND ----------

# results_final_df = results_final_df.select("result_id", "driver_id", "constructor_id", "number", "grid", "position", "position_text", "position_order", "points", "laps", "time", "milliseconds", "fastest_lap", "rank", "fastest_lap_time", "fastest_lap_speed", "data_source", "file_date", "ingestion_date", "race_id")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy('race_id').parquet("abfss://processed@adlsf1.dfs.core.windows.net/results")

# COMMAND ----------

# %run "../includes/common_functions"

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable('f1_processed.results')

# COMMAND ----------

# %run "../includes/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore

# COMMAND ----------

# overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

# results_final_df.write.mode("overwrite").partitionBy('race_id').parquet("abfss://processed@adlsf1.dfs.core.windows.net/results")

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore

# COMMAND ----------

    # deltaTable = DeltaTable.forPath(spark, "abfss://processed@adlsf1.dfs.core.windows.net/results")
    # deltaTable.alias("tgt") \
    #     .merge(
    #         results_final_df.alias("src"),
    #         "tgt.result_id = src.result_id and tgt.race_id = src.race_id"
    #     ) \
    #     .whenMatchedUpdateAll() \
    #     .whenNotMatchedInsertAll() \
    #     .execute()

# COMMAND ----------

spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
from delta.tables import DeltaTable
if spark._jsparkSession.catalog().tableExists("fl_processed.results"):
    deltaTable = DeltaTable.forPath(spark, "abfss://processed@adlsf1.dfs.core.windows.net/results")
    deltaTable.alias("tgt") \
        .merge(
            results_final_df.alias("src"),
            "tgt.result_id = src.result_id"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
else:
    results_final_df.write \
        .mode("append") \
        .partitionBy("race_id") \
        .format("delta") \
        .saveAsTable("hive_metastore.f1_processed.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT file_date, count(*) , count ( distinct race_id)
# MAGIC FROM f1_processed.results
# MAGIC group by 1
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_processed.results