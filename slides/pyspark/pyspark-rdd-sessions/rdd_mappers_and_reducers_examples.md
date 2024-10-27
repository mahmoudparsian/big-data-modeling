# RDD Mappers and Reducers by Examples

## Sample Data 

~~~
% cat /home/mparsian/sample_numbers.txt
1
2
3
4
10
20
30
40
6
7
8
9
300
400
500
600
700
800
3
4
5
6
7
~~~

## PySpark Interactive Demo

~~~
% cd spark-3.5.3
% ./bin/pyspark
Python 3.12.0 (v3.12.0:0fb18b02c8, Oct  2 2023, 09:45:56) 
[Clang 13.0.0 (clang-1300.0.29.30)] on darwin
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.3
      /_/

Using Python version 3.12.0 (v3.12.0:0fb18b02c8, Oct  2 2023 09:45:56)
Spark context Web UI available at http://172.20.210.166:4040
Spark context available as 'sc' 
(master = local[*], app id = local-1729824681801).
SparkSession available as 'spark'.
>>>
>>> # Read a text file and create an RDD[String]
>>> rdd = sc.textFile("/home/mparsian/sample_numbers.txt")
>>> rdd.count()
23
>>> rdd.collect()
['1', '2', '3', '4', '10', '20', '30', '40', 
'6', '7', '8', '9', '300', '400', '500', '600', 
'700', '800', '3', '4', '5', '6', '7']
>>> rdd.getNumPartitions()
2
>>>
>>>
>>> # Read a text file and create an RDD[String] with more partitions
>>> rdd = sc.textFile("/Users/mparsian/sample_numbers.txt", 4)
>>> rdd.getNumPartitions()
5
>>> rdd.repartition(4)
MapPartitionsRDD[9] at coalesce at NativeMethodAccessorImpl.java:0
>>> rdd.getNumPartitions()
5

>>> # Create a function to debug partitions
>>> def debug_partition(partition):
...     print("elements=", list(partition))
...
>>>
>>>
>>> rdd.foreachPartition(debug_partition)
elements= ['1', '2', '3', '4', '10', '20', '30']
elements= []
elements= ['40', '6', '7', '8', '9', '300']
elements= ['800', '3', '4', '5', '6', '7']
elements= ['400', '500', '600', '700']
>>>
>>>
>>> # Create a Python collection of Numbers
>>> numbers = [1, 2, 3, 4, 100, 200, 300, 400, 500, 6, 7, 8, 9]
>>> rdd = sc.parallelize(numbers, 3)
>>> rdd.getNumPartitions()
3

>>> # Examine the content of each partition
>>> rdd.foreachPartition(debug_partition)
elements= [500, 6, 7, 8, 9]
elements= [1, 2, 3, 4]
elements= [100, 200, 300, 400]
>>>
>>>

>>> # this function will be used for RDD.mapPartitions()
>>> # Find (count, min, max) for a partition
>>> def find_count_min_max(partition):
...     first_time = True
...     for v in partition:
...         if (first_time):
...             local_count = 1
...             local_min = v
...             local_max = v
...             first_time = False
...         else:
...             local_count += 1
...             local_min = min(local_min, v)
...             local_max = max(local_max, v)
...         #end-if
...     #end-for
...     return [(local_count, local_min, local_max)]
... #end-def
>>>
>>> # Test find_count_min_max() function
>>> find_count_min_max([1, 2, 3, 7, 8, 9, 10])
[(7, 1, 10)]
>>> find_count_min_max([1, 2, 3, 7])
[(4, 1, 7)]

>>> # Examine content of all partitions
>>> rdd.foreachPartition(debug_partition)
elements= [100, 200, 300, 400]
elements= [1, 2, 3, 4]
elements= [500, 6, 7, 8, 9]

>>> # Apply mapPartitions() transformation
>>> # Map each partition to a triplet: (count, min, max)
>>> mapped = rdd.mapPartitions(find_count_min_max)
>>> mapped.collect()
[
 (4, 1, 4), 
 (4, 100, 400), 
 (5, 6, 500)
]

>>> # reduce mapped to a final triplet
>>> final_triplet = mapped.reduce(
  lambda x, y: (x[0]+y[0], min(x[1], y[1]), max(x[2], y[2]))
)
>>> final_triplet
(13, 1, 500)
>>>
>>>
>>>
>>> rdd.collect()
[1, 2, 3, 4, 100, 200, 300, 
400, 500, 6, 7, 8, 9]
>>> rdd.count()
13
>>> rdd2 = rdd.map(lambda x: x+1000)
>>> rdd2.collect()
[1001, 1002, 1003, 1004, 1100, 1200, 1300, 
1400, 1500, 1006, 1007, 1008, 1009]
>>> rdd2.count()
13
>>>
>>> # Practice with map() and flatMap()
>>> # Create a Python collection
>>> lists = [ [1, 2, 3], [], [4, 5, 6, 7, 8], [], [9] ]
>>> rdd = sc.parallelize(lists)
>>>
>>> rdd.count()
5
>>> rdd.collect()
[
 [1, 2, 3], 
 [], 
 [4, 5, 6, 7, 8], 
 [], 
 [9]
]
>>> # Apply and identity mapper
>>> rdd2 = rdd.map(lambda x: x)
>>> rdd2.count()
5
>>> rdd2.collect()
[
 [1, 2, 3], 
 [], 
 [4, 5, 6, 7, 8], 
 [], 
 [9]
]
>>> # Apply a flatMap():
>>> rdd3 = rdd.flatMap(lambda x: x)
>>> rdd3.count()
9
>>> rdd3.collect()
[1, 2, 3, 4, 5, 6, 7, 8, 9]
>>>
>>>
>>> key_value = [('A', 2), ('A', 3), ('B', 7), ('C', 9)]
>>> rdd = sc.parallelize(key_value)
>>> rdd.collect()
[('A', 2), ('A', 3), ('B', 7), ('C', 9)]
>>> rdd.count()
4
>>> mapped = rdd.mapValues(lambda v: v+10)
>>> mapped.collect()
[('A', 12), ('A', 13), ('B', 17), ('C', 19)]
>>>
>>>
>>> mappped2 = rdd.map(lambda x: (x[0], x[1]+10))
>>> mappped2.collect()
[('A', 12), ('A', 13), ('B', 17), ('C', 19)]
>>>
~~~

## Applying Filters

~~~
>>> mappped2.collect()
[('A', 12), ('A', 13), ('B', 17), ('C', 19)]
>>> filtered = mappped2.filter(lambda x: x[1] > 15)
>>> filtered.collect()
[('B', 17), ('C', 19)]
>>> # (x[1] > 15) is called a boolean predicate
>>>
>>>
>>> key_value = [('A', 2), ('A', 3), ('B', 7), ('C', 9), ('A', 2), ('A', 3)]
>>> rdd = sc.parallelize(key_value)
>>> rdd.count()
6
>>> unique = rdd.distinct()
>>> unique.collect()
[('C', 9), ('B', 7), ('A', 3), ('A', 2)]
>>>
~~~

## Implement distinct() using groupByKey()

~~~
>>> #-----------------------------------------------
>>> # implement distinct() using groupByKey()
>>> # without using distinct()
>>> #-----------------------------------------------

>>> rdd.collect()
[('A', 2), ('A', 3), ('B', 7), ('C', 9), ('A', 2), ('A', 3)]
>>> key_values = rdd.map(lambda x: (x, None))
>>> key_values.collect()
[
 (('A', 2), None), 
 (('A', 3), None), 
 (('B', 7), None), 
 (('C', 9), None), 
 (('A', 2), None), 
 (('A', 3), None)
]
>>> grouped = key_values.groupByKey()
>>> grouped.collect()
[
 (('C', 9), <pyspark.resultiterable.ResultIterable object at 0x111f970b0>), 
 (('B', 7), <pyspark.resultiterable.ResultIterable object at 0x11202edb0>), 
 (('A', 3), <pyspark.resultiterable.ResultIterable object at 0x11202ecf0>), 
 (('A', 2), <pyspark.resultiterable.ResultIterable object at 0x11202ea50>)
]
>>> grouped.mapValues(lambda values: list(values))
PythonRDD[43] at RDD at PythonRDD.scala:53
>>> grouped.mapValues(lambda values: list(values)).collect()
[
 (('C', 9), [None]), 
 (('B', 7), [None]), 
 (('A', 3), [None, None]), 
 (('A', 2), [None, None])
]
>>> unique = grouped.map(lambda x: x[0])
>>> unique.collect()
[('C', 9), ('B', 7), ('A', 3), ('A', 2)]
>>>
~~~

## Implement distinct() using reduceByKey()

~~~
>>> #-----------------------------------------------
>>> # implement distinct() using reduceByKey()
>>> # without using distinct()
>>> #-----------------------------------------------

>>> key_values.collect()
[
 (('A', 2), None), 
 (('A', 3), None), 
 (('B', 7), None), 
 (('C', 9), None), 
 (('A', 2), None), 
 (('A', 3), None)
]
>>>
>>> reduced = key_values.reduceByKey(lambda x, y: None)
>>> reduced.collect()
[
 (('C', 9), None), 
 (('B', 7), None), 
 (('A', 3), None), 
 (('A', 2), None)
]
>>> unique = reduced.map(lambda x: x[0])
>>> unique.collect()
[('C', 9), ('B', 7), ('A', 3), ('A', 2)]
>>>
>>> #-----------------------------------------------
>>> # implement distinct() using reduceByKey()
>>> # without using distinct()
>>> #-----------------------------------------------

>>> strings = ['fox', 'fox', 'fox', 'fox', 'jumped', 'jumped','jumped']
>>> rdd = sc.parallelize(strings)
>>> rdd.collect()
['fox', 'fox', 'fox', 'fox', 'jumped', 'jumped', 'jumped']
>>> rdd.distinct().collect()
['jumped', 'fox']
>>>
>>>
>>> key_values = rdd.map(lambda x: (x, 'junk'))
>>> key_values.collect()
[
 ('fox', 'junk'), 
 ('fox', 'junk'), 
 ('fox', 'junk'), 
 ('fox', 'junk'), 
 ('jumped', 'junk'), 
 ('jumped', 'junk'), 
 ('jumped', 'junk')
]
>>> reduced = key_values.reduceByKey(lambda x, y: 'junk')
>>> reduced.collect()
[
 ('jumped', 'junk'), 
 ('fox', 'junk')
]
>>> unique = reduced.map(lambda x: x[0])
>>> unique.collect()
[
 'jumped', 
 'fox'
]
~~~