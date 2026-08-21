# Understanding Partitions in Spark

* In Spark, partitions are units of parallelisim.

* A partition in spark is an atomic chunk of data 
  (logical division of data) stored on a node in the 
  cluster. Partitions are basic units of parallelism 
  in Apache Spark. 

* RDDs in Apache Spark are collection of partitions



## Invoke PySpark Interactively

~~~sh

% export SPARK_HOME=/Users/mparsian/spark-3.3.2
% $SPARK_HOME/bin/pyspark
Python 3.10.5 (v3.10.5:f377153967, Jun  6 2022, 12:36:10) [Clang 13.0.0 (clang-1300.0.29.30)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.3.2
      /_/

Using Python version 3.10.5 (v3.10.5:f377153967, Jun  6 2022 12:36:10)
Spark context Web UI available at http://192.168.0.108:4040
Spark context available as 'sc' (master = local[*], app id = local-1682557311718).
SparkSession available as 'spark'.
~~~


## Understanding Partitions

~~~python

>>>
>>> spark.version
'3.3.2'
>>> numbers = range(0,50)
>>> numbers
range(0, 50)
>>> rdd = spark.sparkContext.parallelize(numbers)
>>> rdd.collect()
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49]
>>> rdd.count()
50
>>> # rdd: RDD[Integer]
>>> # RDD.glom() → pyspark.rdd.RDD[List[T]][source]
>>> # Return an RDD created by coalescing all elements within each partition into a list.
>>> rdd.glom().collect()
[
 [0, 1, 2], 
 [3, 4, 5], 
 [6, 7, 8], 
 [9, 10, 11], 
 [12, 13, 14], 
 [15, 16, 17], 
 [18, 19, 20], 
 [21, 22, 23, 24], 
 [25, 26, 27], 
 [28, 29, 30], 
 [31, 32, 33], 
 [34, 35, 36], 
 [37, 38, 39], 
 [40, 41, 42], 
 [43, 44, 45], 
 [46, 47, 48, 49]
]
>>> len(rdd.glom().collect())
16
>>> rdd2 = spark.sparkContext.parallelize(numbers, 10)
>>> rdd2.glom().collect()
[
 [0, 1, 2, 3, 4], 
 [5, 6, 7, 8, 9], 
 [10, 11, 12, 13, 14], 
 [15, 16, 17, 18, 19], 
 [20, 21, 22, 23, 24], 
 [25, 26, 27, 28, 29], 
 [30, 31, 32, 33, 34], 
 [35, 36, 37, 38, 39], 
 [40, 41, 42, 43, 44], 
 [45, 46, 47, 48, 49]
]
>>> len(rdd2.glom().collect())
10
>>> rdd3 = rdd2.repartition(7)
>>> rdd3.glom().collect()
[
 [], 
 [45, 46, 47, 48, 49], 
 [5, 6, 7, 8, 9, 30, 31, 32, 33, 34], 
 [10, 11, 12, 13, 14], 
 [15, 16, 17, 18, 19], 
 [20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 35, 36, 37, 38, 39], 
 [0, 1, 2, 3, 4, 40, 41, 42, 43, 44]
]
>>> len(rdd3.glom().collect())
7
>>> rdd3.getNumPartitions()
7
>>> rdd.getNumPartitions()
16
>>># Data URL: https://github.com/mahmoudparsian/big-data-mapreduce-course/blob/master/data/815.txt
>>> input_path = "/tmp/815.txt"
>>> book = spark.sparkContext.textFile(input_path)
>>> book.count()
19235
>>> book
/tmp/815.txt MapPartitionsRDD[16] at textFile at NativeMethodAccessorImpl.java:0
>>> book.getNumPartitions()
2
>>> book = spark.sparkContext.textFile(input_path, 12)
>>> book.getNumPartitions()
12

>>>
>>> book.take(4)
[
 'The Project Gutenberg EBook of Democracy In America, Volume 1 (of 2), by ', 
 'Alexis de Toqueville', 
 '', 
 'This eBook is for the use of anyone anywhere at no cost and with'
]
>>> book.getNumPartitions()
12
>>> book = spark.sparkContext.textFile(input_path, 100)
>>>
>>>
>>> book.getNumPartitions()
100
>>> book2 = book.repartition(56)
>>> book2.getNumPartitions()
56
>>>
~~~