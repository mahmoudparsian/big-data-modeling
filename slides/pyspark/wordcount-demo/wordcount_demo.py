# import required libraries
from __future__ import print_function

import sys

# pyspark.sql.SparkSession
from pyspark.sql import SparkSession
# pyspark.SparkContext
from pyspark import SparkContext

"""
Word Count in PySpark
Read input
Output:
        word1  freq1
        word2  freq2
        ...
        wordN  freqN

word1, word2, ..., wordN are all unique words

"""

# the following 3 prints are for debugging purposes
print ("This is the name of the script: ", sys.argv[0])
print ("Number of arguments: ", len(sys.argv))
print ("The arguments are: " , str(sys.argv))
#

# DEFINE your input path
# 1st parameter passed from command line is sys.argv[1]
input_path = sys.argv[1]
print("input_path: ", input_path)

  
# CREATE an instance of a SparkSession object
# spark = SparkSession.builder.appName("WordCount").getOrCreate()
spark = SparkSession.builder.getOrCreate()  

# NOTE: Lines [46-49 (shorthand notation)] are Equivalent to [Lines 54-64 (Detailed)]

# shorthand notation
# spark.sparkContext.textFile(input_path) => RDD[String]
# .flatMap(lambda x: x.split(' ')) => RDD[String]
# .map(lambda x: (x, 1)) => RDD[(String, Integer)]
counts = spark.sparkContext.textFile(input_path)\
    .flatMap(lambda x: x.split(' ')) \
    .map(lambda x: (x, 1)) \
    .reduceByKey(lambda a,b : a+b)
"""
=> Comment lines in Python...
=> Equivalent to pipeline transformations

# records : RDD[String] => String denotes one single record of input
records = spark.sparkContext.textFile(input_path)

# words : RDD[String] => String denotes a single word
words = records.flatMap(lambda x: x.split(' '))

# pairs : RDD[(String, Integer)] => String denotes a word, Integer is 1
pairs = words.map(lambda x: (x, 1)) 

# counts : RDD[(String, Integer)] => String denotes a unique word, Integer is frequency
counts = pairs.reduceByKey(lambda a,b : a+b)
"""

# drop (word, freq) if freq < 2
# x denotes (word, freq) = (x[0], x[1])
filtered = counts.filter(lambda x: x[1] >= 2)

#   output = [(word1, count1), (word2, count2), ...]                  
print("====== output =======")
output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))

print("======= output2 ======")
#   output2 = [(word1, count1), (word2, count2), ...]                  
output2 = filtered.collect()
for (word, count) in output2:
    print("%s: %i" % (word, count))

#  DONE!
spark.stop()
