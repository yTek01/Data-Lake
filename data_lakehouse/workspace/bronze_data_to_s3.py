import requests
import json
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql.functions import dayofmonth
from pyspark.sql.functions import to_date
from pyspark.sql.functions import dayofweek
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from pyspark.sql.functions import year
from pyspark.sql.functions import month


from decouple import config
from datetime import date
from delta import *
import psycopg2

today = date.today().strftime("%b-%d-%Y")

AWS_ACCESS_KEY = config('AWS_ACCESS_KEY')
AWS_SECRET_KEY = config('AWS_SECRET_KEY')
AWS_S3_ENDPOINT = config('AWS_S3_ENDPOINT')
AWS_BUCKET_NAME = config('AWS_BUCKET_NAME')

spark = SparkSession \
    .builder \
    .appName("Bronze") \
    .getOrCreate()

file_type = "csv"
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","
month_list = []


hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "AKIA5NLUPFOOEVX3VZP6")
hadoop_conf.set("fs.s3a.secret.key", "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")
hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
hadoop_conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
hadoop_conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

postgres_url= "jdbc:postgresql://yb-tserver-n1:5433/Postgres" #"jdbc:postgresql://yb-tserver-n1:5433/Postgres"
postgres_schema = "1841_staging"

conn = psycopg2.connect("host=yb-tserver-n1 port=5433 dbname=Postgres user=postgres password=")
conn.set_session(autocommit=True)
cur = conn.cursor()

order_table = spark.read.format(file_type).option("inferSchema", infer_schema) \
                .option("header", first_row_is_header) \
                .option("sep", delimiter) \
                .load(f"s3a://d2b-internal-assessment-bucket-assessment/orders_data/orders.csv")

reviews_table = spark.read.format(file_type).option("inferSchema", infer_schema) \
                .option("header", first_row_is_header) \
                .option("sep", delimiter) \
                .load(f"s3a://d2b-internal-assessment-bucket-assessment/orders_data/reviews.csv")

shipment_deliveries_table = spark.read.format(file_type).option("inferSchema", infer_schema) \
                .option("header", first_row_is_header) \
                .option("sep", delimiter) \
                .load(f"s3a://d2b-internal-assessment-bucket-assessment/orders_data/shipment_deliveries.csv")


conn.set_session(autocommit=False)
cur = conn.cursor()
for i in order_table.collect():
    cur.execute("""INSERT INTO "1841_staging"."orders" (order_id, customer_id, order_date, product_id, unit_price, quantity, amount) VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (i["order_id"], i["customer_id"], i["order_date"], i["product_id"], i["unit_price"], i["quantity"], i["total_price"]))
print("Done with order_table!")   

for i in reviews_table.collect():
    cur.execute("""INSERT INTO "1841_staging"."reviews" (product_id, review) VALUES (%s, %s)""",
            (i["product_id"], i["review"]))
print("Done with reviews_table!")

for i in shipment_deliveries_table.collect():
    cur.execute("""INSERT INTO "1841_staging"."shipments_deliveries" (shipment_id, order_id, shipment_date, delivery_date) VALUES (%s, %s, %s, %s)""",
            (i["shipment_id"], i["order_id"], i["shipment_date"], i["delivery_date"]))
print("Done with shipment_deliveries_table!")   

Fact_table = reviews_table.join(order_table, 'product_id') \
                      .join(shipment_deliveries_table, 'order_id') \
                      .drop_duplicates()

total_order = Fact_table.withColumn('day_of_month',dayofmonth(Fact_table.order_date)) \
                                  .withColumn('day_of_week',dayofweek(Fact_table.order_date)) \
                                  .withColumn("working_day",F.when(F.col("day_of_week") < 6, True).otherwise(False)) \
                                  .withColumn('year_num',year(Fact_table.order_date)) \
                                  .withColumn('month_of_the_year_num',month(Fact_table.order_date)) \
                                  .select("total_price","quantity", "day_of_month", "day_of_week", "working_day", "year_num", "month_of_the_year_num", "year_num") 

total_order = total_order.filter(total_order.working_day != "true")
expr = [F.sum(F.col("quantity")).alias("total_order"),
        F.max(F.col("total_price")).alias("total_prices")]
agg_public_holiday = total_order.groupBy(["month_of_the_year_num"]).agg(*expr) \
                                .orderBy(col("month_of_the_year_num").asc()) 

for i in range(1, 13):
    data = agg_public_holiday.where(col('month_of_the_year_num')==str(i)).select('total_order').collect()[0].total_order
    month_list.append(data)

agg_public_holiday = spark.createDataFrame([tuple(month_list)], ["tt_order_hol_jan", "tt_order_hol_feb", "tt_order_hol_mar",
                                                                "tt_order_hol_apr", "tt_order_hol_may", "tt_order_hol_jun", 
                                                                "tt_order_hol_jul", "tt_order_hol_aug", "tt_order_hol_sep", 
                                                                "tt_order_hol_oct", "tt_order_hol_nov", "tt_order_hol_dec"])
agg_public_holiday = agg_public_holiday.withColumn("ingestion_date",current_date())



for i in agg_public_holiday.collect():
    cur.execute("""INSERT INTO "1841_analytics"."agg_public_holiday" (ingestion_date, tt_order_hol_jan, tt_order_hol_feb, tt_order_hol_mar, tt_order_hol_apr, tt_order_hol_may, tt_order_hol_jun, tt_order_hol_jul, tt_order_hol_aug, tt_order_hol_sep, tt_order_hol_oct, tt_order_hol_nov, tt_order_hol_dec ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (i["ingestion_date"], i["tt_order_hol_jan"], i["tt_order_hol_feb"], i["tt_order_hol_mar"], i["tt_order_hol_apr"], i["tt_order_hol_may"], i["tt_order_hol_jun"],i["tt_order_hol_jul"],i["tt_order_hol_aug"],i["tt_order_hol_sep"], i["tt_order_hol_oct"], i["tt_order_hol_nov"] , i["tt_order_hol_dec"]  ))
print("Done with agg_public_holiday!")  


shipment_details = Fact_table.withColumn("dateDiff", datediff(Fact_table.shipment_date, Fact_table.order_date))
shipment_details = shipment_details.withColumn("late_shipments", F.when((F.col("dateDiff") >= 6) & (F.col("delivery_date").isNull()), \
                                            1) \
                                            .otherwise(0))
#Scaffolding
shipment_details = shipment_details.withColumn("current_date", lit("2022-09-05")) \
                                   .withColumn("ingestion_date",current_date())           
shipment_details = shipment_details.withColumn("days_left", datediff(shipment_details.current_date, shipment_details.order_date)) 
shipment_details = shipment_details.withColumn("undelivered_shipments", F.when((F.col("delivery_date").isNull()) & \
                                                                    (F.col("shipment_date").isNull()) & \
                                                                    (F.col("days_left")>15),1) \
                                                                    .otherwise(0))

undelivered = shipment_details.select("ingestion_date", "order_date", "undelivered_shipments","delivery_date", "late_shipments", "shipment_date", "quantity")
undelivered.createOrReplaceTempView("undeliveredOrders")
undelivered_orders = spark.sql("SELECT ingestion_date, SUM(undelivered_shipments) OVER () AS tt_undelivered_items, SUM(late_shipments) OVER () AS tt_late_shipments FROM undeliveredOrders LIMIT 1")
undelivered_orders.show()


for i in undelivered_orders.collect():
    cur.execute("""INSERT INTO "1841_analytics"."agg_shipments" (ingestion_date, tt_late_shipments, tt_undelivered_items) VALUES (%s, %s, %s)""",
            (i["ingestion_date"], i["tt_late_shipments"], i["tt_undelivered_items"] ))

print("Done with undelivered_orders!!!")  


landing = Fact_table.withColumn('day_of_month',dayofmonth(Fact_table.order_date)) \
                                .withColumn('day_of_week',dayofweek(Fact_table.order_date)) \
                                .withColumn("is_public_holiday",F.when(F.col("day_of_week") > 5, True).otherwise(False)) \
                                .withColumn('year_num',year(Fact_table.order_date)) \
                                .withColumn('month_of_the_year_num',month(Fact_table.order_date)) 


shipment_details = landing.withColumn("dateDiff", datediff(landing.shipment_date, landing.order_date)) \
                            .withColumn("ingestion_date",current_date())
shipment_details = shipment_details.withColumn("late_shipments", F.when((F.col("dateDiff") >= 6) & (F.col("delivery_date").isNull()), \
                                        shipment_details.dateDiff) \
                                        .otherwise(shipment_details.dateDiff))
Undelivered_shipments = shipment_details.withColumn("undelivered_shipments", F.when((F.col("delivery_date").isNull()) & \
                                                                    (F.col("shipment_date").isNull()) & \
                                                                    (F.col("dateDiff")>15),1) \
                                                                    .otherwise(0))

Undelivered_shipments.createOrReplaceTempView("PerformingProduct")
PerformingProduct = spark.sql("""SELECT ingestion_date, is_public_holiday, product_id, 
                    SUM(highest_reviews) AS tt_review_points, SUM(review) AS highest_reviews, day_of_week, 
                    (SUM(review) OVER (PARTITION BY review=1)/ SUM(review) OVER ())*100 AS pct_one_star_review, 
                    (SUM(review) OVER (PARTITION BY review=2)/ SUM(review) OVER ()) *100 AS pct_two_star_review, 
                    (SUM(review) OVER (PARTITION BY review=3)/ SUM(review) OVER ()) *100 AS pct_three_star_review, 
                    (SUM(review) OVER (PARTITION BY review=4)/ SUM(review) OVER ()) * 100 AS pct_four_star_review, 
                    (SUM(review) OVER (PARTITION BY review=5)/ SUM(review) OVER ()) * 100 AS pct_five_star_review, 
                    (COUNT(CASE WHEN late_shipments<6 THEN product_id END) OVER (PARTITION BY product_id))/(COUNT(product_id) OVER()) AS pct_early_shipments,
                    (COUNT(CASE WHEN late_shipments>6 OR late_shipments IS NULL THEN product_id END) OVER (PARTITION BY product_id))/(COUNT(product_id) OVER()) AS pct_late_shipments 
                        FROM (SELECT ingestion_date, is_public_holiday, product_id, review, late_shipments,SUM(review) AS highest_reviews, day_of_week 
                            FROM PerformingProduct GROUP BY product_id, review, late_shipments, day_of_week, ingestion_date, is_public_holiday ORDER BY highest_reviews DESC) 
                                GROUP BY product_id, review,late_shipments, day_of_week, ingestion_date, is_public_holiday ORDER BY tt_review_points DESC""")

PerformingProduct.show(truncate=False)



for i in undelivered_orders.collect():
    cur.execute("""INSERT INTO "1841_analytics"."agg_shipments" (ingestion_date, tt_late_shipments, tt_undelivered_items) VALUES (%s, %s, %s)""",
            (i["ingestion_date"], i["tt_late_shipments"], i["tt_undelivered_items"] ))

print("Done with undelivered_orders!!!")  













conn.commit()
cur.close()
conn.close()





#     for row in data:
#         data[row] 
#     data.show()
#     print(f" Loading {table} to Postgres Database ...")

#     data.write \
#     .format("jdbc") \
#     .option("url", postgres_url) \
#     .option("dbtable", f"1841_staging.{table}") \
#     .option("user", "postgres") \
#     .option("password", "") \
#     .option("driver", "org.postgresql.Driver") \
#     .mode("overwrite") \
#     .save()

#     print(f"DONE WRITING {table}")

# conn.set_session(autocommit=False)
# cur = conn.cursor()


# cur.execute("INSERT INTO 1841_staging.orders (order_id, customer_id,order_date,product_id,unit_price,quantity,amount) VALUES (%s, %s, %s, %s)",
#             (1, 'John', 35, 'Python'))


# print("Inserted (id, name, age, language) = (1, 'John', 35, 'Python')")




# cur.execute("SELECT name, age, language FROM employee WHERE id = 1")
# row = cur.fetchone()
# print("Query returned: %s, %s, %s" % (row[0], row[1], row[2]))
# # Commit and close down.
# conn.commit()
# cur.close()
# conn.close()

# print("Done!!!")

# for table in tables:
#     print(f" Loading {table} from S3 bucket ...")
#     data = spark.read.format(file_type).option("inferSchema", infer_schema) \
#                     .option("header", first_row_is_header) \
#                     .option("sep", delimiter) \
#                     .load(f"s3a://d2b-internal-assessment-bucket-assessment/orders_data/{table}.csv")
    
#     data.show()
#     print(f" Loading {table} to Postgres Database ...")

#     data.write \
#     .format("jdbc") \
#     .option("url", postgres_url) \
#     .option("dbtable", f"1841_staging.{table}") \
#     .option("user", "postgres") \
#     .option("password", "") \
#     .option("driver", "org.postgresql.Driver") \
#     .mode("overwrite") \
#     .save() 

#     print(f"DONE WRITING {table}")


# df.write \
#         .mode('append') \
#         .format("jdbc") \
#         .option("url", f"jdbc:postgresql://localhost:5432/postgres") \
#         .option("driver", "org.postgresql.Driver") \
#         .option("dbtable", 'table_test') \
#         .option("user", 'user') \
#         .option("password", 'password') \
#         .save()

#     dataframe.show()






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
