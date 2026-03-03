# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df=spark.read.format("csv")\
            .option("header",True)\
            .option("inferSchema",True)\
            .load("/Workspace/default/music_streaming_data.csv")

# COMMAND ----------

spark.sql("DROP TABLE default.bronze_streams")

spark.sql("""
CREATE TABLE default.bronze_streams
AS SELECT * FROM default.music_streaming_data
""")

# COMMAND ----------

spark.sql("SHOW TABLES IN default").show()

# COMMAND ----------

display(spark.table("default.bronze_streams"))

# COMMAND ----------

