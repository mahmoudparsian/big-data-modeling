import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("continents_countries") \
    .getOrCreate()


input_path= 's3://mybucket/INPUT2/continents_countries.csv'
# input_path = sys.argv[1]

df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema", "true")\
    .load(input_path)

df.show(10, truncate=False)
df.printSchema()

output_path = "s3://mybucket/SCU/OUTPUT2/continents_countries2/"
# output_path = sys.argv[2]

# exit()


# df.write.mode("append").parquet(output_path)

df.repartition("continent", "country")\
    .write.mode("append")\
    .partitionBy("continent", "country")\
    .parquet(output_path)

spark.stop()
