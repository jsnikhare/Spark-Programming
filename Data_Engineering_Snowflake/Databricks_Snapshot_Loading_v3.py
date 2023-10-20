import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from spark.sql.session import SparkSession
from spark.sql.function import *


from datetime import datetime as dt

output_airlines="/test/output/snapshot/airlines"
output_airports="/test/output/snapshot/airports"
output_flights="/test/output/snapshot/flights"

sc = SparkContext()

# initialize the session, build your Spark context
spark=SparkSession.builder\
.appName("snapshot_loading")\
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
	
# read data from snowflake tables	
airlines_sq = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
  .options(sfOptions) \
  .option("query",  "select iata_code, airline from sf_tuts_db.sf_tuts_sch.airlines ") \
  .load()
airlines_sq.createOrReplaceTempView("airlines_tb")
	
airports_sq = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
  .options(sfOptions) \
  .option("query",  "select iata_code, airline from sf_tuts_db.sf_tuts_sch.airlines ") \
  .load()
airports_sq.createOrReplaceTempView("airports_tb")

flights_sq = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
  .options(sfOptions) \
  .option("query",  "select year, month, day, concat(year, month, day) as date, day_of_week, airline, flight_number, 
	tail_number, origin_airport, destination_airport, scheduled_departure, departure_time, departure_delay, wheels_off, scheduled_time, 
	elapsed_time, air_time, distance, wheels_on, taxi_in, scheduled_arrival, arrival_time, arrival_delay, diverted, cancelled, 
	cancellation_reason, air_system_delay, security_delay, airline_delay, late_aircraft_delay, weather_delay, 
	case when nvl(air_system_delay, 0) != 0 then 'air_system_delay' 
		 when nvl(security_delay, 0) != 0 then 'security_delay'
		 when nvl(airline_delay, 0) != 0 then 'airline_delay' 
		 when nvl(late_aircraft_delay, 0) != 0 then 'late_aircraft_delay' 
		 when nvl(weather_delay, 0) != 0 then 'weather_delay' 
	as delay_reasons from sf_tuts_db.sf_tuts_sch.flights ") \
  .load()
flights_sq.createOrReplaceTempView("flights_tb")

 
# Write the data into HDFS in parquet format
airlines_tb.write.parquet(output_airlines)
airports_tb.write.parquet(output_airports)
flights_tb.write.parquet(output_flights)


# Write the data into snapshot tables.
airlines_sq.write.format(SNOWFLAKE_SOURCE_NAME)\
    .options(sfOptions)\
    .option("dbtable", "airlines_snapshot")\
    .mode(SaveMode.Overwrite)\
    .save()

airports_sq.write.format(SNOWFLAKE_SOURCE_NAME)\
    .options(sfOptions)\
    .option("dbtable", "airports_snapshot")\
    .mode(SaveMode.Overwrite)\
    .save()

flights_sq.write.format(SNOWFLAKE_SOURCE_NAME)\
    .options(sfOptions)\
    .option("dbtable", "flights_snapshot")\
    .mode(SaveMode.Overwrite)\
    .save()

#spark.sql("""insert overwrite sf_tuts_db.sf_tuts_sch.flights_snapshot partition(date) select * from flights_tb """)


spark.stop()