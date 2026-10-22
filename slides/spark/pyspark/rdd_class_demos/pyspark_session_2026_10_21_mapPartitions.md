# `mapPartitions()` & `mapPartitionsWithIndex()`

* Last updated Date: October 21, 2026
* Spark version: 4.2.0

These two mapper transformations are called 
"summarization design patterns": they summarize 
each partition into a single python object
(such as a tuple, list, dictionary, ...).


```
mapPartitions(f, preservesPartitioning=False)
   Return a new RDD by applying a function (f) 
   to each partition of this RDD.
   
   # iterator represents a single partition
   def f(iterator):
      <use iterator to build/create 
       a single python object>
      return <single-object>
```

```
mapPartitionsWithIndex(f, preservesPartitioning=False)
   Return a new RDD by applying a function (f) 
   to each partition of this RDD, while tracking 
   the index of the original partition.
   
   # partition_index: index of the original partition
   # iterator: represents a single partition
   def f(partition_index, iterator):
      <use iterator to build/create
       a single python object>
      return <single-object>
      
```

```python
% pyspark
Python 3.11.2 (v3.11.2:878ead1ac1, Feb  7 2023, 10:02:41) 
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 4.2.0
      /_/

Using Python version 3.11.2 (v3.11.2:878ead1ac1, Feb  7 2023 10:02:41)
Spark context Web UI available at http://localhost:4040
Spark context available as 'sc' (master = local[*], app id = local-1792633911493).
SparkSession available as 'spark'.

>>> # iterator: an iterator such as a list or a partition
>>> # this function summarizes the whole partition
>>> # into a single triplet: (count, min, max)
>>> def count_min_max(iterator):
...     first_time = True
...     #count = 0
...     #min = 0;
...     #max = 0;
...     for x in iterator:
...         if (first_time):
...             count = 1
...             min = x;
...             max = x;
...             first_time = False
...         else:
...             count += 1
...             if x > max: max = x
...             if x < min: min = x
...         #end-if
...     return [(count, min, max)]
... #end-def
...
>>> count_min_max([10, 20, 3, 4, 5, 6, 7])
[(7, 3, 20)]

>>> count_min_max([10, 20, 3, 4, 5, 6, 7, 900])
[(8, 3, 900)]

>>> numbers = [2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 19, 40]
>>> numbers
[2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 19, 40]

>>> # create an RDD[Integer] with 3 partitions
>>> rdd = sc.parallelize(numbers, 3)
>>> rdd.count()
15

>>> rdd.collect()
[2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 19, 40]
>>>
>>>
>>> rdd2 = rdd.mapPartitions(count_min_max)
>>> 
>>> rdd2.count()
3

>>> rdd2.collect()
[
 (5, 2, 6), 
 (5, 7, 12), 
 (5, 13, 40)
]

>>>
>>>
>>> def debug(iterator):
...     elements = []
...     for x in iterator:
...         elements.append(x)
...     return elements
...
...
>>> debug([1, 2, 3])
[1, 2, 3]
>>> rdd.mapPartitions(debug)
PythonRDD[10] at RDD at PythonRDD.scala:59

>>> rdd3 = rdd.mapPartitions(debug)
>>> rdd3.collect()
[2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 19, 40]

>>> def debug(iterator):
...     elements = []
...     for x in iterator:
...         elements.append(x)
...     return ("elements", elements)
...
...
>>> rdd3 = rdd.mapPartitions(debug)
>>> rdd3.collect()
[
 'elements', [2, 3, 4, 5, 6], 
 'elements', [7, 8, 9, 10, 12], 
 'elements', [13, 14, 15, 19, 40]
]
>>> rdd4 = rdd.mapPartitions(count_min_max)
>>> rdd4.collect()
[
 (5, 2, 6), 
 (5, 7, 12), 
 (5, 13, 40)
]
>>>
>>>
>>>
>>>
>>> def count_min_max_index(partition_index, iterator):
...     first_time = True
...     #count = 0
...     #min = 0;
...     #max = 0;
...     for x in iterator:
...         if (first_time):
...             count = 1
...             min = x;
...             max = x;
...             first_time = False
...         else:
...             count += 1
...             if x > max: max = x
...             if x < min: min = x
...         #end-if
...     return [(partition_index, count, min, max)]
... #end-def
...
>>>
>>> rdd5 = rdd.mapPartitionsWithIndex(count_min_max_index)
>>> rdd5.collect()
[
 (0, 5, 2, 6), 
 (1, 5, 7, 12), 
 (2, 5, 13, 40)
]
>>>
