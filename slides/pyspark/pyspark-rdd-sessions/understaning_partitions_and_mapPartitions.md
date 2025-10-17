# Understanding Partitions & `mapPartitions()`


```python

~  % ./spark-4.0.0/bin/pyspark
Python 3.11.2 (v3.11.2:878ead1ac1, Feb  7 2023, 10:02:41) 
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 4.0.0
      /_/

Using Python version 3.11.2 
Spark context Web UI available at http://localhost:4040
Spark context available as 'sc' 
(master = local[*], app id = local-1760663048515).
SparkSession available as 'spark'.
>>>
>>>
>>>
>>> numbers = [1, 2, 3, 6, 7, 8, 8, 9, 2, 3, 5, 6, 7, 8, 10, 11, 12]
>>>
>>> rdd = sc.parallelize(numbers)
>>> rdd.count()
17
>>>
>>> rdd.collect()
[1, 2, 3, 6, 7, 8, 8, 9, 2, 3, 5, 6, 7, 8, 10, 11, 12]
>>>
>>>
>>> rdd.getNumPartitions()
8
>>> rdd = sc.parallelize(numbers, 4)
>>> rdd.getNumPartitions()
4
>>> def debug_partition(iterator):
...     # This function will be applied to each partition
...     partition_elements = list(iterator)
...     print(f"Debugging partition with elements: {partition_elements}")
...     # You can add more complex debugging logic here, e.g., assert statements, conditional prints
...     return partition_elements # Return the elements to continue the transformation
...
>>> rdd2 = rdd.mapPartitions(debug_partition)
>>>
>>> rdd2.count()
Debugging partition with elements: [1, 2, 3, 6]
Debugging partition with elements: [7, 8, 10, 11, 12]
Debugging partition with elements: [2, 3, 5, 6]
Debugging partition with elements: [7, 8, 8, 9]
17
>>> def custom_partition(iterator):
...     # This function will be applied to each partition
...     partition_elements = list(iterator)
...     print(f"Debugging partition with elements: {partition_elements}")
...     # You can add more complex debugging logic here, e.g., assert statements, conditional prints
...     return [partition_elements]
...
>>>
>>> rdd3 = rdd.mapPartitions(custom_partition)
>>> rdd3.count()
Debugging partition with elements: [7, 8, 8, 9]
Debugging partition with elements: [1, 2, 3, 6]
Debugging partition with elements: [2, 3, 5, 6]
Debugging partition with elements: [7, 8, 10, 11, 12]
4
>>> rdd3.collect()
Debugging partition with elements: [7, 8, 10, 11, 12]
Debugging partition with elements: [2, 3, 5, 6]
Debugging partition with elements: [7, 8, 8, 9]
Debugging partition with elements: [1, 2, 3, 6]
[[1, 2, 3, 6], [7, 8, 8, 9], [2, 3, 5, 6], [7, 8, 10, 11, 12]]
>>>
>>>
>>> numbers = [0, 0, -1, -1, +1, +2, 0, 0, 7, 8, -7, -8, -9, 2, 3, 4, 5, 0, 0, 0, -3, -5, -6]
>>> len(numbers)
23
>>> numbers = [0, 0, -1, -1, +1, +2, 0, 0, 7, 8, -7, -8, -9, 2, 3, 4, 5, 0, 0, 0, -3, -5, -6, 0, 0, 9, 9]
>>>
>>> numbers;
[0, 0, -1, -1, 1, 2, 0, 0, 7, 8, -7, -8, -9, 2, 3, 4, 5, 0, 0, 0, -3, -5, -6, 0, 0, 9, 9]
>>> numbers
[0, 0, -1, -1, 1, 2, 0, 0, 7, 8, -7, -8, -9, 2, 3, 4, 5, 0, 0, 0, -3, -5, -6, 0, 0, 9, 9]
>>> len(numbers)
27
>>> rdd = sc.parallelize(numbers, 5)
>>> rdd.count()
27

>>> rdd.getNumPartitions()
5
>>> rdd2 = rdd.mapPartitions(debug_partition)
>>> rdd2.collect()
Debugging partition with elements: [-7, -8, -9, 2, 3]
Debugging partition with elements: [2, 0, 0, 7, 8]
Debugging partition with elements: [-3, -5, -6, 0, 0, 9, 9]
Debugging partition with elements: [0, 0, -1, -1, 1]
Debugging partition with elements: [4, 5, 0, 0, 0]
[0, 0, -1, -1, 1, 2, 0, 0, 7, 8, -7, -8, -9, 2, 3, 4, 5, 0, 0, 0, -3, -5, -6, 0, 0, 9, 9]
>>> rdd2 = rdd.mapPartitions(custom_partition)
>>> rdd2.collect()
Debugging partition with elements: [4, 5, 0, 0, 0]
Debugging partition with elements: [0, 0, -1, -1, 1]
Debugging partition with elements: [-7, -8, -9, 2, 3]
Debugging partition with elements: [2, 0, 0, 7, 8]
Debugging partition with elements: [-3, -5, -6, 0, 0, 9, 9]
[[0, 0, -1, -1, 1], [2, 0, 0, 7, 8], [-7, -8, -9, 2, 3], [4, 5, 0, 0, 0], [-3, -5, -6, 0, 0, 9, 9]]

>>> # Z denotes number of zeros
>>> # N denotes number of negative numbers
>>> # P denotes number of positive numbers
>>>
>>> # partition denotes a single partition of source RDD
>>> # from each partition we will return [(Z, N, P)]
>>> def ZNP_counter(partition):
...     Z = 0
...     N = 0
...     P = 0
...     # iterate every element of a partition
...     for x in partition:
...         if x == 0:
...             Z += 1
...         elif x > 0:
...             P += 1
...         else:
...             N += 1
...         #end-if
...     #end-for
...
...     return [(Z, N, P)]
... #end-def
...
>>> # test the function
>>> ZNP_counter([0, 0, 0, 0, -1, -2, 4, 5, 6, 7])
[(4, 2, 4)]
>>>
>>>

>>> mapped = rdd.mapPartitions(ZNP_counter)
>>> mapped.count()
5
>>> mapped.collect()
[
 (2, 2, 1), 
 (2, 0, 3), 
 (0, 3, 2), 
 (3, 0, 2), 
 (2, 3, 2)
]
>>>
>>> reduced = mapped.reduce(lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+b[2]))
>>> reduced
(9, 8, 10)
>>>
>>>
>>> a = [10, 20, [1, 2], [3, 4, 5], [] ]
>>> len(a)
5
>>> rdd = sc.parallelize(a)
>>> rdd.count()
5
>>> rdd.collect()
[10, 20, [1, 2], [3, 4, 5], []]
>>> # flatMap() required all elements 
>>> # to be iterable (such as array/list)
>>> flattened = rdd.flatMap(lambda x: x)

>>> flattened.count()
25/10/16 18:59:09 ERROR Executor: 
Exception in task 3.0 in stage 13.0 (TID 77)
org.apache.spark.api.python.PythonException: 
Traceback (most recent call last):
       ^^^^^^^^^^^^^^^^^^^^^^^^^^
TypeError: 'int' object is not iterable
....
....

>>> a = [ [10, 20], [1, 2], [3, 4, 5], [] ]
>>> rdd = sc.parallelize(a)
>>> rdd.count()
4
>>> rdd.collect()
[[10, 20], [1, 2], [3, 4, 5], []]
>>> flattened = rdd.flatMap(lambda x: x)
>>> flattened.count()
7
>>> flattened.collect()
[10, 20, 1, 2, 3, 4, 5]
>>>
>>>
```