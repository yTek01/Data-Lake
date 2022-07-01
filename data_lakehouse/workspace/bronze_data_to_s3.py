import requests
import json
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql import functions as F
from decouple import config
from datetime import date
from delta import *
from great_expectations_helper import expectations_helper

today = date.today().strftime("%b-%d-%Y")

AWS_ACCESS_KEY = config('AWS_ACCESS_KEY')
AWS_SECRET_KEY = config('AWS_SECRET_KEY')
AWS_S3_ENDPOINT = config('AWS_S3_ENDPOINT')
AWS_BUCKET_NAME = config('AWS_BUCKET_NAME')

spark = SparkSession \
    .builder \
    .appName("Bronze") \
    .getOrCreate() 

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "AKIATLEGUPNKMJLK7I7R")
hadoop_conf.set("fs.s3a.secret.key", "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")
hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
hadoop_conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
hadoop_conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

response = requests.get("https://api.mfapi.in/mf/118550")
data = response.text
sparkContext = spark.sparkContext
RDD = sparkContext.parallelize([data])
raw_json_dataframe = spark.read.json(RDD)

raw_json_dataframe.printSchema()
raw_json_dataframe.createOrReplaceTempView("Mutual_benefit")

dataframe = raw_json_dataframe.withColumn("data", F.explode(F.col("data"))) \
        .withColumn('meta', F.expr("meta")) \
        .select("data.*", "meta.*")

tables_names = ['actor', 'address', 'category', 'city', 'country', 'customer', \
               'film', 'film_actor', 'film_category', 'inventory', 'language', 'payment', 'rental', 'staff', 'store']

postgres_url= "jdbc:postgresql://yb-tserver-n1:5433/dvdrental"

# expectations_helper(dataframe)

for table_name in tables_names:
    print(f"{table_name} table transformation ...")

    dataframe = spark.read \
    .format("jdbc") \
    .option("url", postgres_url) \
    .option("dbtable", table_name) \
    .option("user", "postgres") \
    .option("password", "") \
    .option("driver", "org.postgresql.Driver") \
    .load() \

    dataframe.show()

    dataframe.write \
    .format("delta")\
    .mode("overwrite")\
    .save(f"s3a://new-wave-delta-lake-silver/bronze/dvdrentalDB_delta/{today}/{table_name}")
    print(f"{table_name} table done!")


for table_name in tables_names:
    # NOTE This line requires Java 8 instead of Java 11 work it to work on Airflow
    # We are saving locally for now.
    dataframe.write \
    .format("delta")\
    .mode("overwrite")\
    .save(f"s3a://new-wave-delta-lake/bronze/dvdrentalDB_delta/{today}/{table_name}")
    # dataframe.write.parquet('s3a://sparkjobresult/output',mode='overwrite')
    # dataframe.write.format('csv').option('header','true').save('s3a://sparkjobresult/output',mode='overwrite')
