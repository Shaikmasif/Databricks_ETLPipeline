#importing spark session and necessary functions
from pyspark.sql import SparkSession
import pyspark.sql.functions as F   

spark = SparkSession.builder.getOrCreate()


#reading the streaming table data into a dataframe
silver_df = spark.readStream.table("shop_mart.shop_mart.retail_store_silver")


#calculating the aggregations according to the need
gold_df = (silver_df.
           groupby("transactionDate", "category")
           .agg(
               F.sum("totalAmount").alias("Total_Revenue"),
               F.sum("quantity").alias("Total_Quantity_Sold"),
               F.count("transactionId").alias("Total_Transactions"),
               F.avg("price").alias("Average_Price"),
               F.sum("totalTax").alias("Total_Tax")
           ))


#checkpointLocation for storing the schema structure
gold_checkpoint_directory = "/Volumes/shop_mart/default/shop_mart/check/gold/v2"


#writing the data to a delta table
(gold_df.
 writeStream.
 format("delta").
 outputMode("complete").
 trigger(availableNow=True).
 option("checkpointLocation", gold_checkpoint_directory).
 toTable("shop_mart.shop_mart.retail_store_gold"))



#displaying the data
%sql
select * from shop_mart.shop_mart.retail_store_gold;