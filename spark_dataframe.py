#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat, col

spark = SparkSession.builder.appName('Dataframe_Example').getOrCreate()
df = spark.read.csv('./data/input/Restaurants_in_Wake_County.csv', mode='DROPMALFORMED', inferSchema=True, header=True)
df_json = spark.read.json('./data/input/Restaurants_in_Durham_County_NC.json')

# add columns
df = df.withColumn("COUNTY", lit("Wake"))

# remove columns
drop = ['OBJECTID', 'PERMITID', 'GEOCODESTATUS']
df = df.drop(*drop)

# rename columns
df = df.withColumnRenamed("HSISID", "datasetId")
df = df.withColumnRenamed("NAME", "name")
df = df.withColumnRenamed("ADDRESS1", "address1")
df = df.withColumnRenamed("ADDRESS2", "address2")
df = df.withColumnRenamed("CITY", "city")
df = df.withColumnRenamed("STATE", "state")
df = df.withColumnRenamed("POSTALCODE", "zip")
df = df.withColumnRenamed("PHONENUMBER", "tel")
df = df.withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
df = df.withColumnRenamed("FACILITYTYPE", "type")
df = df.withColumnRenamed("X", "geoX")
df = df.withColumnRenamed("Y", "geoy")

# add id
df = df.select("*", concat(col("STATE"), lit("_"), col("COUNTY"), lit("_"), col("datasetID")).alias("ID"))

df.select(col("ID"), col("name"), col("address1"), col("city"), col("state"), col("zip")).show()

print(df.rdd.getNumPartitions())
df = df.repartition(4)
print(df.rdd.getNumPartitions())

spark.stop()
