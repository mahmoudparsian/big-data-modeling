# Word Count Problem

	The word count problem is the “Hello World” 
	program of distributed data processing. Given 
	a massive collection of text documents, the 
	goal is to count how many times each unique 
	word appears across the entire dataset using 
	a distributed MapReduce framework.


# Input Data

```
$ cat data.txt
crazy crazy fox jumped
crazy fox jumped
fox is fast
fox is smart
dog is smart
```

# PySpark Program: `wordcount.py`

```python
from __future__ import print_function

import sys

from pyspark.sql import SparkSession

#
print ("This is the name of the script: ", sys.argv[0])
print ("Number of arguments: ", len(sys.argv))
print ("The arguments are: " , str(sys.argv))
#

#   DEFINE your input path
input_path = sys.argv[1]
print("input_path: ", input_path)

  
#   CREATE an instance of a SparkSession object
spark = SparkSession\
    .builder\
    .appName("PythonWordCount")\
    .getOrCreate()

#   CREATE a new RDD[String]
#lines = spark.sparkContext.textFile(input_path)
    
#   APPLY a SET of TRANSFORMATIONS...
#counts = lines.flatMap(lambda x: x.split(' ')) \
#                .map(lambda x: (x, 1)) \
#                .reduceByKey(lambda a,b : a+b)

counts = spark.sparkContext.textFile(input_path)\
    .flatMap(lambda x: x.split(' ')) \
    .map(lambda x: (x, 1)) \
    .reduceByKey(lambda a,b : a+b)

#   output = [(word1, count1), (word2, count2), ...]                  
output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))

#  DONE!
spark.stop()
```

# Run PySpark Program

```
$SPARK_HOME/bin/spark-submit wordcount.py data.txt

This is the name of the script: wordcount.py
Number of arguments:  2
The arguments are:  ['wordcount.py', 'data.txt']

crazy: 3
fox: 4
jumped: 2
is: 3
fast: 1
smart: 2
dog: 1
```
