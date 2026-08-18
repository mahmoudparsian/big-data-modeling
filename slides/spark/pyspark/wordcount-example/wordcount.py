from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
#
    print ("This is the name of the script: ", sys.argv[0])
    print ("Number of arguments: ", len(sys.argv))
    print ("The arguments are: " , str(sys.argv))
#
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

#   DEFINE your input path
    input_path = sys.argv[1]
    
#   CREATE an instance of a SparkSession object
    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

#   CREATE a new RDD[String]
#    lines = spark.sparkContext.textFile(input_path)
    lines = spark.read.text(input_path).rdd.map(lambda r: r[0])
    
#   APPLY a SET of TRANSFORMATIONS...
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
#                 .reduceByKey(lambda a,b : a+b)

#   output = [(word1, count1), (word2, count2), ...]                  
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

#   DONE!
    spark.stop()
