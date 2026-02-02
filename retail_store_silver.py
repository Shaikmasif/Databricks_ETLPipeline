# importing sparksession and the necessary functions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, initcap, current_timestamp
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()


#reading the data as the input stream from the bronze streaming table
bronze_df = spark.readStream.table("shop_mart.shop_mart.retail_store_bronze")


#cleaning & transforming the data by removing the null values.
silver_df = (bronze_df
    .withColumn("price", col("price").cast("double"))
    .withColumn("quantity", col("quantity").cast("double"))
    .withColumn("totalAmount", col("totalAmount").cast("double"))
    .withColumn("category", initcap(col("category")))
    .withColumn("location", initcap(col("location")))
    .withColumn("totalAmount", 
        when(col("totalAmount").isNull(), col("price") * col("quantity"))
        .otherwise(col("totalAmount")))
    .withColumn("totalTax",col("totalAmount") * 0.18)
    .withColumn("totalAmount", col("totalAmount") + col("totalTax"))
    .dropDuplicates(["transactionId"])
    .withColumn("silver_processing_time", current_timestamp())
)


#removing the null valued data
silver_df = silver_df.dropna()


#checkpointLocation for storing the schema structure
checkpoint_directory = "/Volumes/shop_mart/default/shop_mart/check/sil/v1"


#writing the data to the delta table
(silver_df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_directory)
    .trigger(availableNow=True)
    .toTable("shop_mart.shop_mart.retail_store_silver"))


#displaying the data
%sql
select * from shop_mart.shop_mart.retail_store_silver;