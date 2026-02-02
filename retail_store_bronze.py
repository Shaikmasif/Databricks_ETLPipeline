#import SparkSession which is the entry point of a spark application
#import necessary functions from pyspark to work with

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, DateType, StringType, BooleanType

spark = SparkSession.builder.getOrCreate()


#input directory path from the cloud volume where the files will be uploaded
input_path = "/Volumes/shop_mart/default/shop_mart/raw_data"

#checkpointLocation for storing the schema structure

checkpoint_directory = "/Volumes/shop_mart/default/shop_mart/check/bronze/v1

#reading the files as stream
df = (spark.readStream.format("cloudFiles")
	.option("cloudFiles.format", "csv")
	.option("cloudFiles.inferColumnTypes", "true")
	.option("cloudFiles.schemaLocation", checkpoint_directory)
	.option("cloudFiles.schemaEvolutionMode", "rescue")
	.option("header", "True")
	.load(input_path))

#selecting columns by renaming them

df = df.select(col("Transaction ID").alias("transactionId"), col("Customer ID").alias("customerId"), col("Category").alias("category"), col("Item").alias("item"), col("Price Per Unit").alias("price"), col("Quantity").alias("quantity"), col("Total Spent").alias("totalAmount"), col("Payment Method").alias("paymentMethod"), col("Location").alias("location"), col("Transaction Date").alias("transactionDate"), col("Discount Applied").alias("discountApplied"))



#writing the data to a streaming delta table in the database

(df.writeStream
 .format("delta")
 .option("checkpointLocation", checkpoint_directory) 
 .trigger(availableNow=True)
 .toTable("shop_mart.shop_mart.retail_store_bronze"))


#displaying of the data
%sql
select * from shop_mart.shop_mart.retail_store_bronze;

