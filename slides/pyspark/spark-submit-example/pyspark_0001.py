from __future__ import print_function

from pyspark.sql import SparkSession

import sys

# A SparkSession can be used create DataFrame, 
# register DataFrame as tables, execute SQL 
# over tables, cache tables, and read parquet files. 
# To create a SparkSession, use the following 
# builder pattern:

spark = SparkSession.builder\
   .appName("testing...") \
   .getOrCreate()

input_path = sys.argv[1]
print("input_path=", input_path)

records = spark.sparkContext.textFile(input_path)

print("records.count()=", records.count())

print("records.collect()=", records.collect())

spark.stop()