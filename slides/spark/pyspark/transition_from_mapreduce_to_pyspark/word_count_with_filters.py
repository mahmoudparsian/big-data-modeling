#-----------------------------------------------------
# word_count_with_filters.py: a sample PySpark program
#-----------------------------------------------------
 
 # 0. Import required libraries
import sys
from pyspark.sql import SparkSession

"""
Applying Filters:

giving the following pyspark program (word_count.py) 
for word count:  modify it so that it will discard 
every word which is  less than 3 characters and then 
after finding word_counts as (unique-word, frequency) 
it will discard the words if their frequency is less 
than 4.

To apply these two filters, add .filter() 
transformations at two specific stages:

    1. Length Filter (len(word) >= 3): 
    Add right after .flatMap(...) to 
    discard short words before mapping them to pairs.
    
    we call the Length Filter value as: L 

    2. Frequency Filter (count >= 4): 
    Add right after .reduceByKey(...) 
    to discard words appearing fewer than 4 times.
    
    we call the Frequency Filter value as: F 

"""

# 1. Check the number of arguments passed
if len(sys.argv) != 5:
    print("Usage: wordcount <input-path> <output-path> <L> <F>", file=sys.stderr)
    sys.exit(-1)
# end-if

# 2. Create a SparkSession object
spark = SparkSession.builder.appName("word-count").getOrCreate()
print("spark=", spark)

# 3. Define input path
input_path = sys.argv[1]
print("input_path=", input_path)

# 4. Define output path
output_path = sys.argv[2]
print("output_path=", output_path)

# 5. Define filter threshold values: L and F
L = int(sys.argv[3])
print("L=", L)

F = int(sys.argv[4])
print("F=", F)

# 6. Create an RDD[String] from input_path
lines = spark.sparkContext.textFile(input_path)

# 7. Count the number of records read
print("lines.count()=", lines.count())

# 8. Create word_counts as RDD[(String, Integer)] 
#    with length and frequency filters
word_counts = (
    lines.flatMap(lambda x: x.split(" "))
    .filter(lambda word: len(word) >= L)  # Filter 1: Keep words with L+ characters
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
    .filter(lambda item: item[1] >= F)    # Filter 2: Keep words with frequency >= F
)

# 9. Get elements of word_counts as a List
output = word_counts.collect()

# 10. output (word, count)
for word, count in output:
    print("%s: %i" % (word, count))

# 11. Save word counts:
word_counts.saveAsTextFile(output_path)

# 12. Done!
spark.stop()
