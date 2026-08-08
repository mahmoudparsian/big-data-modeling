# root of parquet data: s3://caselogdev/scu/output_as_parquet/

import os
import sys
from pyspark.sql import SparkSession


# Create a SparkSession object
spark = SparkSession.builder.getOrCreate()


# input_path = 's3://mybucket/INPUT2/continents_countries.csv'
# input_path = 'continents_countries_temp.csv'
input_path = sys.argv[1]
print("input_path=", input_path)

df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema", "true")\
    .load(input_path)

print("df.count()=", df.count())
df.show(10, truncate=False)
df.printSchema()



# output_path = "s3://caselogdev/SCU/output/temp000/"
output_path = sys.argv[2]
print("output_path=", output_path)


df.repartition("continent")\
    .write.mode("append")\
    .partitionBy("continent")\
    .parquet(output_path)


spark.stop()
