import os
from pyspark.sql import SparkSession
from datetime import date
from delta import *
import requests
import json
from pyspark.sql import functions as F

today = date.today().strftime("%b-%d-%Y")

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_S3_ENDPOINT = os.getenv("AWS_S3_ENDPOINT")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
HIVE_METASTORE_URI = os.getenv("HIVE_METASTORE_URI")

spark = SparkSession.builder \
    .appName('Silver data') \
    .getOrCreate()

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "AKIATLEGUPNKMJLK7I7R")
hadoop_conf.set("fs.s3a.secret.key", "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")
hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
hadoop_conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
hadoop_conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark.sparkContext.setLogLevel("ERROR")
spark.sql("CREATE DATABASE IF NOT EXISTS dwh COMMENT 'Data Warehouse for dvdrental'")
# spark.sql("USE Database")

# Reading tables from landing area
print('\nReading ...')
actor = spark.read.format("delta").load(f's3a://new-wave-delta-lake-silver/bronze/dvdrentalDB_delta/{today}/actor')
address = spark.read.format("delta").load(f's3a://new-wave-delta-lake-silver/bronze/dvdrentalDB_delta/{today}/address')
category = spark.read.format("delta").load(f's3a://new-wave-delta-lake-silver/bronze/dvdrentalDB_delta/{today}/category')
city = spark.read.format("delta").load(f's3a://new-wave-delta-lake-silver/bronze/dvdrentalDB_delta/{today}/city')
country = spark.read.format("delta").load(f's3a://new-wave-delta-lake-silver/bronze/dvdrentalDB_delta/{today}/country')
customer = spark.read.format("delta").load(f's3a://new-wave-delta-lake-silver/bronze/dvdrentalDB_delta/{today}/customer')
film = spark.read.format("delta").load(f's3a://new-wave-delta-lake-silver/bronze/dvdrentalDB_delta/{today}/film')
film_actor = spark.read.format("delta").load(f's3a://new-wave-delta-lake-silver/bronze/dvdrentalDB_delta/{today}/film_actor')
film_category = spark.read.format("delta").load(f's3a://new-wave-delta-lake-silver/bronze/dvdrentalDB_delta/{today}/film_category')
inventory = spark.read.format("delta").load(f's3a://new-wave-delta-lake-silver/bronze/dvdrentalDB_delta/{today}/inventory')
language = spark.read.format("delta").load(f's3a://new-wave-delta-lake-silver/bronze/dvdrentalDB_delta/{today}/language')
payment = spark.read.format("delta").load(f's3a://new-wave-delta-lake-silver/bronze/dvdrentalDB_delta/{today}/payment')
rental = spark.read.format("delta").load(f's3a://new-wave-delta-lake-silver/bronze/dvdrentalDB_delta/{today}/rental')
staff = spark.read.format("delta").load(f's3a://new-wave-delta-lake-silver/bronze/dvdrentalDB_delta/{today}/staff')
store = spark.read.format("delta").load(f's3a://new-wave-delta-lake-silver/bronze/dvdrentalDB_delta/{today}/store')


print('End of reading... \n')

# transforming tables to a set of dimensionel tables
print('\ Instantiating the tables ...')

actor.write.format('delta').mode('overwrite').save('s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Actor')
address.write.format('delta').mode('overwrite').save('s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Address')
category.write.format('delta').mode('overwrite').save('s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Category')
city.write.format('delta').mode('overwrite').save('s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_City')
country.write.format('delta').mode('overwrite').save('s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Country')
film.write.format('delta').mode('overwrite').save('s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Film')
film_actor.write.format('delta').mode('overwrite').save('s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Film_actor')
film_category.write.format('delta').mode('overwrite').save('s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Film_category')
inventory.write.format('delta').mode('overwrite').save('s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Inventory')
language.write.format('delta').mode('overwrite').save('s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Language')
payment.write.format('delta').mode('overwrite').save('s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Payment')
rental.write.format('delta').mode('overwrite').save('s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Rental')
staff.write.format('delta').mode('overwrite').save('s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Staff')
store.write.format('delta').mode('overwrite').save('s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Store')

print('\n Transforming tables')

actor.write.format('delta').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Actor').saveAsTable("dwh.DimActor")
address.write.format('delta').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Address').saveAsTable("dwh.DimAddress")
category.write.format('delta').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Category').saveAsTable("dwh.DimCategory")
city.write.format('delta').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_City').saveAsTable("dwh.DimCity")
country.write.format('delta').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Country').saveAsTable("dwh.DimCountry")
film.write.format('delta').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Film').saveAsTable("dwh.DimFilm")
film_actor.write.format('delta').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Film_actor').saveAsTable("dwh.DimFilm_actor")
film_category.write.format('delta').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Film_category').saveAsTable("dwh.DimFilm_category")
inventory.write.format('delta').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Inventory').saveAsTable("dwh.DimInventory")
language.write.format('delta').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Language').saveAsTable("dwh.DimLanguage")
payment.write.format('delta').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Payment').saveAsTable("dwh.DimPayment")
rental.write.format('delta').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Rental').saveAsTable("dwh.DimRental")
staff.write.format('delta').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Staff').saveAsTable("dwh.DimStaff")
store.write.format('delta').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Store').saveAsTable("dwh.DimStore")

print('\n Done with Data Warehouse and Table Creations!!! \n')

payment.join(rental, 'customer_id') \
    .join(customer,'customer_id')\
    .join(address, 'address_id')\
    .join(city, 'city_id')\
    .join(country, 'country_id')\
    .write.format('delta').mode('overwrite')\
    .option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Fact_part_in_Order').saveAsTable("dwh.FactPartInOrder")
print('End Of Transforming')