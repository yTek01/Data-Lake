import requests
import json
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql import functions as F
from decouple import config
from datetime import date
from delta import *

today = date.today().strftime("%b-%d-%Y")

AWS_ACCESS_KEY = config('AWS_ACCESS_KEY')
AWS_SECRET_KEY = config('AWS_SECRET_KEY')
AWS_S3_ENDPOINT = config('AWS_S3_ENDPOINT')
AWS_BUCKET_NAME = config('AWS_BUCKET_NAME')

spark = SparkSession \
    .builder \
    .appName("Data2bot_Assessment") \
    .getOrCreate()

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "AKIA5NLUPFOOEVX3VZP6")
hadoop_conf.set("fs.s3a.secret.key", "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")
hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
hadoop_conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
hadoop_conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

file_type = "csv"
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

order_df = spark.read.format(file_type).option("inferSchema", infer_schema) \
                .option("header", first_row_is_header) \
                .option("sep", delimiter) \
                .load("s3a://d2b-internal-assessment-bucket-assessment/orders_data/orders.csv")
display(order_df)

# orders = spark.read.csv("s3a://d2b-internal-assessment-bucket-assessment/orders_data/orders.csv") \
#                     .option("inferSchema", infer_schema) \
#                     .option("header", first_row_is_header) \
#                     .option("sep", delimiter) 


# reviews = spark.read.csv("s3a://d2b-internal-assessment-bucket-assessment/orders_data/reviews.csv")
# shipments_deliveries = spark.read.csv("s3a://d2b-internal-assessment-bucket-assessment/orders_data/shipments_deliveries.csv")

# orders.show()
# # reviews.show()
# # shipments_deliveries.show()


# file_location = "/mnt/d2b-internal-assessment-bucket-assessment/orders_data/warehouse/orders/"




# order_df = spark.read.format(file_type).option("inferSchema", infer_schema) \
#   .option("header", first_row_is_header) \
#   .option("sep", delimiter) \
#   .load(file_location)

# display(order_df)



# response = requests.get("https://api.mfapi.in/mf/118550")
# data = response.text
# sparkContext = spark.sparkContext
# RDD = sparkContext.parallelize([data])
# raw_json_dataframe = spark.read.json(RDD)

# raw_json_dataframe.printSchema()
# raw_json_dataframe.createOrReplaceTempView("Mutual_benefit")

# dataframe = raw_json_dataframe.withColumn("data", F.explode(F.col("data"))) \
#         .withColumn('meta', F.expr("meta")) \
#         .select("data.*", "meta.*")

# tables_names = ['actor', 'address', 'category', 'city', 'country', 'customer', \
#                'film', 'film_actor', 'film_category', 'inventory', 'language', 'payment', 'rental', 'staff', 'store']

# postgres_url= "jdbc:postgresql://yb-tserver-n1:5433/dvdrental"

# # expectations_helper(dataframe)

# for table_name in tables_names:
#     print(f"{table_name} table transformation ...")

#     dataframe = spark.read \
#     .format("jdbc") \
#     .option("url", postgres_url) \
#     .option("dbtable", table_name) \
#     .option("user", "postgres") \
#     .option("password", "") \
#     .option("driver", "org.postgresql.Driver") \
#     .load() \

#     dataframe.show()

#     dataframe.write \
#     .format("delta")\
#     .mode("overwrite")\
#     .save(f"s3a://dvdrental-play/bronze/dvdrentalDB_delta/{today}/{table_name}")
#     print(f"{table_name} table done!")


# for table_name in tables_names:
#     # NOTE This line requires Java 8 instead of Java 11 work it to work on Airflow
#     # We are saving locally for now.
#     dataframe.write \
#     .format("delta")\
#     .mode("overwrite")\
#     .save(f"s3a://dvdrental-play/bronze/dvdrentalDB_delta/{today}/{table_name}")
#     # dataframe.write.parquet('s3a://sparkjobresult/output',mode='overwrite')
#     # dataframe.write.format('csv').option('header','true').save('s3a://sparkjobresult/output',mode='overwrite')
