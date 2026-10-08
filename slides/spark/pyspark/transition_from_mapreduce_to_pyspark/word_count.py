#----------------------------------------
# word_count.py: a sample PySpark program
#----------------------------------------
 
# 0. Import required libraries
import sys
from pyspark.sql import SparkSession


# 1. check the number of arguments passed
if len(sys.argv) != 3:
    print("Usage: word_count <input-path> <output-path>", file=sys.stderr)
    sys.exit(-1)
#end-if

# 2. Create a SparkSession object
spark = SparkSession.builder.appName("word-count").getOrCreate()
print("spark=", spark)

# 3. Define input path
input_path = sys.argv[1]
print("input_path=", input_path)

# 4. Define output path
output_path = sys.argv[2]
print("output_path=", output_path)

# 5. Create an RDD[String] from input_path
lines = spark.sparkContext.textFile(input_path)


# 6. Count the number of records read
print("lines.count()=", lines.count())

# 7. Create word_count as RDD[(String, Integer)]
word_counts = lines.flatMap(lambda x: x.split(' ')) \
    .map(lambda x: (x, 1)) \
    .reduceByKey(lambda a, b: a+b)

# 8. Get elements of word_count as a List
output = word_counts.collect()

for (word, count) in output:
    print("%s: %i" % (word, count))

# 9. Save word counts:
word_counts.saveAsTextFile(output_path)

# 10 done!
spark.stop()
