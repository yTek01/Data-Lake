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


# Reading tables from landing area
print('\nReading ...')
actor = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/dvdrentalDB/{today}/actor')
address = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/dvdrentalDB/{today}/address')
category = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/dvdrentalDB/{today}/category')
city = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/dvdrentalDB/{today}/city')
country = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/dvdrentalDB/{today}/country')
customer = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/dvdrentalDB/{today}/customer')
film = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/dvdrentalDB/{today}/film')
film_actor = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/dvdrentalDB/{today}/film_actor')
film_category = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/dvdrentalDB/{today}/film_category')
inventory = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/dvdrentalDB/{today}/inventory')
language = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/dvdrentalDB/{today}/language')
payment = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/dvdrentalDB/{today}/payment')
rental = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/dvdrentalDB/{today}/rental')
staff = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/dvdrentalDB/{today}/staff')
store = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/dvdrentalDB/{today}/store')


print('End of reading... \n')

# transforming tables to a set of dimensionel tables
print('\ntransforming ...')
actor.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Actor').saveAsTable("dwh.Dim.Actor")
address.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Address').saveAsTable("dwh.Dim.Address")
category.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Category').saveAsTable("dwh.Dim.Category")
city.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_City').saveAsTable("dwh.Dim.City")
country.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Country').saveAsTable("dwh.Dim.Country")
film.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Film').saveAsTable("dwh.Dim.Film")
film_actor.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Film_actor').saveAsTable("dwh.Dim.Film_actor")
film_category.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Film_category').saveAsTable("dwh.Dim.Film_category")
inventory.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Inventory').saveAsTable("dwh.Dim.Inventory")
language.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Language').saveAsTable("dwh.Dim.Language")
payment.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Payment').saveAsTable("dwh.Dim.Payment")
rental.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Rental').saveAsTable("dwh.Dim.Rental")
staff.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Staff').saveAsTable("dwh.Dim.Staff")
store.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake-silver/silver/warehouse/dvdrental/Dim_Store').saveAsTable("dwh.Dim.Store")

payment.join(rental, 'customer_id') \
    .join(customer,'customer_id')\
    .join(address, 'address_id')\
    .join(city, 'city_id')\
    .join(country, 'country_id')\
    .write.format('parquet').mode('overwrite')\
    .option('path','s3a://datalake/silver/warehouse/dvdrental/Fact_part_in_Order').saveAsTable("dwh.FactPartInOrder")
print('End Of Transforming')