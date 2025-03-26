# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Data from ADF

# COMMAND ----------

storage_account = "XXXXXXXX"
application_id = "yyyyyyyyyyyy"
directory_id = "zzzz-zzz-zzzz"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.XXXXXXX", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.xxxxxxxx", "xxxxxxxxx")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.xxxxxxxxxxxxx", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.xxxxxxxxxxxxx", "xxxxxxxxxxxxxxx")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.xxxxxxxxxxxxxxxxx", f"httx:XXXXXXXXXXXXXXXXXXXXXXXXX/{directory_id}/xxxxxxx")

# COMMAND ----------

base_path="xxxx://xxxxxx@xxxxxxxx"
customer_path=base_path+"olist_customers_dataset.csv"
geolocation_path=base_path+"olist_geolocation_dataset.csv"
order_path=base_path+"olist_orders_dataset.csv"
order_items_path=base_path +"olist_order_items_dataset.csv"
order_payments_path=base_path + "olist_order_payments_dataset"
order_reviews_path=base_path+"olist_order_reviews_dataset.csv"
products_path=base_path+"olist_products_dataset.csv"
sellers_path=base_path+"olist_sellers_dataset.csv"  

orders_df = spark.read.format("csv").option("header", "true").load(order_path)
customer_df = spark.read.format("csv").option("header", "true").load(customer_path)
geolocation_df = spark.read.format("csv").option("header", "true").load(geolocation_path)
order_items_df = spark.read.format("csv").option("header", "true").load(order_items_path)
order_payments_df = spark.read.format("csv").option("header", "true").load(order_payments_path)
order_reviews_df = spark.read.format("csv").option("header", "true").load(order_reviews_path)
products_df = spark.read.format("csv").option("header", "true").load(products_path)
sellers_df = spark.read.format("csv").option("header", "true").load(sellers_path)




# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Data from MongoDB using pymongo

# COMMAND ----------

import pymongo

# COMMAND ----------

from pymongo import MongoClient

# COMMAND ----------

# importing module
from pymongo import MongoClient
import pandas as pd

hostname = "xxxxxxxx"
database = "NoSQLDB_xxxxxxxx"
port = "xxxxx"
username = "NoSQLDB_xxxxxx"
password = "xxxxxxxxxxxxxxxx"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]
mydatabase

# COMMAND ----------

import pandas as pd
collection=mydatabase["product_categories"]

mongo_data=pd.DataFrame(list(collection.find()))

# COMMAND ----------

mongo_data

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleaning Data

# COMMAND ----------

from pyspark.sql.functions import col,current_date,to_date,datediff,count,when,to_timestamp

# COMMAND ----------

customer_df.printSchema()

# COMMAND ----------

customer_df.describe().show()

# COMMAND ----------

display(customer_df.head(5))

# COMMAND ----------

customer_df.select([count(when(col(c).isNull(), c)).alias(c) for c in customer_df.columns]).show()

# COMMAND ----------

display(geolocation_df.head(5))

# COMMAND ----------

geolocation_df.select([count(when(col(c).isNull(), c)).alias(c) for c in geolocation_df.columns]).show()

# COMMAND ----------

geolocation_df=geolocation_df.withColumn("geolocation_lat", col("geolocation_lat").cast("float"))
geolocation_df=geolocation_df.withColumn("geolocation_lng", col("geolocation_lng").cast("float"))
geolocation_df.printSchema()

# COMMAND ----------

display(order_items_df.head(5))

# COMMAND ----------

order_items_df.printSchema()

# COMMAND ----------

order_items_df=order_items_df.withColumn("price", col("price").cast("float"))
order_items_df=order_items_df.withColumn("freight_value", col("freight_value").cast("float"))

# COMMAND ----------

order_items_df.printSchema()

# COMMAND ----------

display(order_payments_df.head(5))

# COMMAND ----------

order_payments_df.printSchema()

# COMMAND ----------

order_payments_df=order_payments_df.withColumn("payment_sequential",col("payment_sequential").cast("int"))
order_payments_df=order_payments_df.withColumn("payment_installments",col("payment_installments").cast("int"))
order_payments_df=order_payments_df.withColumn("payment_value", col("payment_value").cast("float"))
order_payments_df.printSchema()


# COMMAND ----------

order_payments_df.select([count(when(col(c).isNull(), c)).alias(c) for c in order_payments_df.columns]).show()

# COMMAND ----------

display(order_reviews_df)

# COMMAND ----------

order_reviews_df.printSchema()

# COMMAND ----------

order_reviews_df=order_reviews_df.withColumn("review_score", col("review_score").cast("int"))
order_reviews_df=order_reviews_df.withColumn("review_creation_date", to_date(col("review_creation_date")))  
order_reviews_df=order_reviews_df.withColumn("review_answer_timestamp", to_date(col("review_answer_timestamp")))
order_reviews_df.printSchema()

# COMMAND ----------

order_reviews_df.select([count(when(col(c).isNull(), c)).alias(c) for c in order_reviews_df.columns]).show()

# COMMAND ----------

from pyspark.sql.functions import bround
total_rows = order_reviews_df.count()
null_percentage_df = order_reviews_df.select([
    bround((count(when(col(c).isNull(), c)) / total_rows * 100), 2).alias(f"{c}_null_pct")
    for c in order_reviews_df.columns
])

display(null_percentage_df)


# COMMAND ----------

order_reviews_df = order_reviews_df.na.drop(subset=["review_creation_date", "review_answer_timestamp"])


# COMMAND ----------

from pyspark.sql.functions import bround

null_percentage_df = order_reviews_df.select([
    bround((count(when(col(c).isNull(), c)) / total_rows * 100), 2).alias(f"{c}_null_pct")
    for c in order_reviews_df.columns
])

display(null_percentage_df)


# COMMAND ----------

order_reviews_df=order_reviews_df.fillna('no_title', subset=['review_comment_title'])
order_reviews_df=order_reviews_df.fillna('no_message', subset=['review_comment_message'])


# COMMAND ----------

from pyspark.sql.functions import bround

null_percentage_df = order_reviews_df.select([
    bround((count(when(col(c).isNull(), c)) / total_rows * 100), 2).alias(f"{c}_null_pct")
    for c in order_reviews_df.columns
])

display(null_percentage_df)


# COMMAND ----------

sellers_df.printSchema()

# COMMAND ----------

display(sellers_df.head(5))

# COMMAND ----------

sellers_df.select([count(when(col(c).isNull(), c)).alias(c) for c in sellers_df.columns]).show()

# COMMAND ----------

display(products_df.head(5))

# COMMAND ----------

products_df.printSchema()

# COMMAND ----------

products_df=products_df.withColumn("product_weight_g", products_df["product_weight_g"].cast("float"))
products_df=products_df.withColumn("product_length_cm", products_df["product_length_cm"].cast("float"))
products_df=products_df.withColumn("product_height_cm", products_df["product_height_cm"].cast("float"))
products_df=products_df.withColumn("product_width_cm", products_df["product_width_cm"].cast("float"))
products_df=products_df.withColumn("product_name_lenght", products_df["product_name_lenght"].cast("int"))
products_df=products_df.withColumn("product_description_lenght", products_df["product_description_lenght"].cast("int    "))
products_df=products_df.withColumn("product_photos_qty", products_df["product_photos_qty"].cast("int"))

# COMMAND ----------

products_df.printSchema()

# COMMAND ----------

display(products_df.select([count(when(col(c).isNull(), c)).alias(c) for c in products_df.columns]))

# COMMAND ----------

from pyspark.sql.functions import col, count, when

total_rows = products_df.count()

null_percentage_df = products_df.select([
    ((count(when(col(c).isNull(), c)) / total_rows) * 100).alias(c) for c in products_df.columns
])

display(null_percentage_df)


# COMMAND ----------

products_df = products_df.na.drop(how='any')

# COMMAND ----------

from pyspark.sql.functions import col, count, when

total_rows = products_df.count()

null_percentage_df = products_df.select([
    ((count(when(col(c).isNull(), c)) / total_rows) * 100).alias(c) for c in products_df.columns
])

display(null_percentage_df)


# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

display(orders_df.head(5))

# COMMAND ----------


orders_df = orders_df.withColumn("order_purchase_timestamp", to_date(col("order_purchase_timestamp")))
ordesr_df = orders_df.withColumn("order_approved_at", to_date(col("order_approved_at")))
orders_df = orders_df.withColumn("order_delivered_carrier_date", to_date(col("order_delivered_carrier_date")))
orders_df = orders_df.withColumn("order_delivered_customer_date", to_date(col("order_delivered_customer_date")))
orders_df = orders_df.withColumn("order_estimated_delivery_date", to_date(col("order_estimated_delivery_date")))

orders_df.printSchema()


# COMMAND ----------

display(orders_df.select([count(when(col(c).isNull(), c)).alias(c) for c in orders_df.columns]))

# COMMAND ----------

from pyspark.sql.functions import col, count, when

total_rows = orders_df.count()

null_percentage_df = orders_df.select([
    ((count(when(col(c).isNull(), c)) / total_rows) * 100).alias(c) for c in orders_df.columns
])

display(null_percentage_df)


# COMMAND ----------

orders_df.count()

# COMMAND ----------

orders_df = orders_df.dropna()
display(orders_df.head(5))

# COMMAND ----------

orders_df.count()

# COMMAND ----------

mongo_data.dtypes

# COMMAND ----------

type(mongo_data)


# COMMAND ----------

type(orders_df  )

# COMMAND ----------

mongo_data["_id"] = mongo_data["_id"].astype(str)  
product_category_df = spark.createDataFrame(mongo_data)


# COMMAND ----------

product_category_df.printSchema()

# COMMAND ----------

product_category_df.select([count(when(col(c).isNull(), c)).alias(c) for c in product_category_df.columns]).show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Joining Data Frames

# COMMAND ----------

orders_customer_df = orders_df.join(customer_df, orders_df.customer_id == customer_df.customer_id, how='left')


# COMMAND ----------

orders_customer_payments_df=orders_customer_df.join(order_payments_df, orders_customer_df.order_id == order_payments_df.order_id, how='left')


# COMMAND ----------

orders_customer_payments_items_df=orders_customer_payments_df.join(order_items_df,"order_id", how='left')


# COMMAND ----------

orders_customer_payments_items_products_df=orders_customer_payments_items_df.join(products_df,orders_customer_payments_items_df['product_id']== products_df['product_id'], how='left')

# COMMAND ----------

final_df=orders_customer_payments_items_products_df.join(product_category_df, orders_customer_payments_items_products_df['product_category_name']== product_category_df['product_category_name'], how='left')

# COMMAND ----------

display(final_df)

# COMMAND ----------

def remove_dup_cols(df):
    columns=df.columns
    seen_columns = set()
    columns_to_drop = []
    for column in columns:
        if column in seen_columns:
            columns_to_drop.append(column)
        else:
            seen_columns.add(column)
    df_cleaned = df.drop(*columns_to_drop)
    return df_cleaned

final_df=remove_dup_cols(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet("xxxxx://xxxxxxx@xxxxxxxxxx/silver/")