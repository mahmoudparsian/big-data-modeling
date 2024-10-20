# Checking RDD Partitions

#### Author: Mahmoud Parsian
#### Last updated: October 20, 2024

## Introduction

~~~
Data is split ito segments called partitions.

The main data  abstraction Spark provides is 
a resilient distributed dataset (RDD), which   
is a collection of elements partitioned across 
the nodes of the cluster that can be operated 
on in parallel.

When programming in Spark, we should note that
some of the partitions can be empty (no elements
at all).
~~~

## Example

~~~
% cd spark-3.5.3
spark-3.5.3  % ./bin/pyspark
Python 3.12.0 (v3.12.0:0fb18b02c8, Oct  2 2023, 09:45:56) 
[Clang 13.0.0 (clang-1300.0.29.30)] on darwin
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.3
      /_/

Using Python version 3.12.0 (v3.12.0:0fb18b02c8, Oct  2 2023 09:45:56)
Spark context Web UI available at http://c02fj5kymd6n.hsd1.ca.comcast.net:4040
Spark context available as 'sc' (master = local[*], app id = local-1729458975093).
SparkSession available as 'spark'.

>>> nums = [1, 2, 3, 4]
>>> nums
[1, 2, 3, 4]

>>> # create an RDD[Integer]
>>> rdd = sc.parallelize(nums)
>>> rdd.collect()
[1, 2, 3, 4]

>>> # check the number of partitions
>>> rdd.getNumPartitions()
16

>>> # checks if a given partition is empty or not
>>> def check_empty_partition(iterator):
...     if not any(True for _ in iterator):
...         print("Partition is empty")
...         yield True  # Partition is empty
...     else:
...         print("Partition is not empty")
...         yield False # Partition is not empty
...

>>> # lists all elements of a given partition
>>> def debug_partition(partition):
...     print("elements=", list(partition))
...
>>>
>>> # Examine each partition
>>> rdd.foreachPartition(debug_partition)
elements= []                                                      (0 + 16) / 16]
elements= []
elements= []
elements= []
elements= []
elements= [2]
elements= [3]
elements= [1]
elements= []
elements= []
elements= []
elements= []
elements= [4]
elements= []===========================================>          (13 + 3) / 16]
elements= []
elements= []

>>> # check if a given partition is empty or not
>>> rdd.foreachPartition(check_empty_partition)
Partition is empty
Partition is empty
Partition is empty
Partition is empty
Partition is empty
Partition is empty
Partition is empty
Partition is empty
Partition is not empty
Partition is not empty
Partition is empty
Partition is not empty
Partition is empty
Partition is not empty
Partition is empty
Partition is empty
>>>
~~~
