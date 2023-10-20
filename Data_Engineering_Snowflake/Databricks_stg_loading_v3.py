import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from spark.sql.session import SparkSession
from spark.sql.function import *

from datetime import datetime as dt

airlines_Path = "/FileStore/shared_upload/jitendra.nikhare1@yahoo.com/airlines.csv"
airports_Path = "/FileStore/shared_upload/jitendra.nikhare1@yahoo.com/airports.csv"
flights_path  = "/FileStore/shared_upload/jitendra.nikhare1@yahoo.com/flights"

output_airlines = "/test/output/stg/airlines"
output_airports = "/test/output/stg/airports"
output_flights = "/test/output/stg/flights"

sc = SparkContext()
# initialize the session, buid your Spark context
spark=SparkSession.builder\
.appName("csv_data_loading")\
.config("hive.exec.dynamic.partition","true")\
.config("hive.exec.dynamic.partition.mode","nonstrict")\
.config("spark.sql.broadcastTimeout", "36000")\
.getOrCreate() 

# Use secrets DBUtil to get Snowflake credentials.
user = dbutils.secrets.get("data-warehouse", "jsnikhare")
password = dbutils.secrets.get("data-warehouse", "<snowflake-password>")

# setup connection with sbowflake SQL sources
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

# snowflake connection options
sfOptions = {
  "sfUrl": "qe65185.west-us-2.azure.snowflakecomputing.com",
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "sf_tuts_db",
  "sfSchema": "sf_tuts_sch",
  "sfWarehouse": "sf_tuts_wh"
}
	

airlines_sq = spark.read.format("csv").option("sep", ",").option("header", "true").option("inferSchema", "true").load(airlines_Path)
airlines_sq.createOrReplaceTempView("tbl_airlines")

airports_sq = spark.read.format("csv").option("sep", ",").option("header", "true").option("inferSchema", "true").load(airports_Path)
airports_sq.createOrReplaceTempView("tbl_airports")

flights_sq = spark.read.format("csv").option("sep", ",").option("header", "true").option("inferSchema", "true").load(flights_path)
flights_sq.createOrReplaceTempView("tbl_flights")
 
# Write the data into HDFS in parquet format
airlines_sq.write.parquet(output_airlines)
airports_sq.write.parquet(output_airports)
flights_sq.write.parquet(output_flights)


# Write the data into snowSQL temporary tables.
airlines_sq.write.format(SNOWFLAKE_SOURCE_NAME)\
    .options(sfOptions)\
    .option("dbtable", "airlines")\
    .mode(SaveMode.Overwrite)\
    .save()

airports_sq.write.format(SNOWFLAKE_SOURCE_NAME)\
    .options(sfOptions)\
    .option("dbtable", "airports")\
    .mode(SaveMode.Overwrite)\
    .save()

flights_sq.write.format(SNOWFLAKE_SOURCE_NAME)\
    .options(sfOptions)\
    .option("dbtable", "flights")\
    .mode(SaveMode.Overwrite)\
    .save()

spark.stop()