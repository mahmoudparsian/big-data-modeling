# Find Sum, Count, Average Per key

The purpose of this post is to show how to use
`groupByKey()` and `reduceByKey()` reducers
in PySpark by simple examples.


Given `source_rdd as RDD[(String, Integer)]`, 
which represents `(key, value)` pairs, the 
goal is to find `RDD[(String, (T3)]` where 
`key` is the key of `source_rdd` and `T3` 
represents a triplet as `(Integer, Integer, Float)` 
which represents `(sum, count, average)` per key.


Two solutions are provided:

1. `groupByKey()` solution

`groupByKey()` groups the values for each key in the 
RDD into a single sequence. Hash-partitions the resulting 
RDD with `numPartitions` partitions.


2. `reduceByKey()` solution

`reduceByKey()` merges the values for each key using an 
**associative** and **commutative** reduce function.


# 1. `groupByKey()` solution

~~~python
spark-3.5.0  % ./bin/pyspark
Python 3.11.4 (v3.11.4:d2340ef257, Jun  6 2023, 19:15:51) 
[Clang 13.0.0 (clang-1300.0.29.30)] on darwin
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.0
      /_/

Using Python version 3.11.4 
(v3.11.4:d2340ef257, Jun  6 2023 19:15:51)
Spark context available as 'sc' 
(master = local[*], app id = local-1697682144202).
SparkSession available as 'spark'.
>>>
>>>
>>> sc.version
'3.5.0'
>>>
>>> pairs = [('A', 2), ('A', 4), ('A', 9), 
             ('B', 6), ('B', 1),('B', 2), 
             ('C', 1), ('C', 6)]

>>> # rdd: RDD[(String, Integer)]
>>> rdd = sc.parallelize(pairs)
>>> rdd.count()
8

>>> # grouped: RDD[(String, [Integer])]
>>> grouped = rdd.groupByKey()
>>> grouped.mapValues(lambda values : list(values)).collect()
[
 ('B', [6, 1, 2]), 
 ('A', [2, 4, 9]), 
 ('C', [1, 6])
]
>>> sum_count_avg = grouped.mapValues(
   lambda v: (sum(v), len(v), float(sum(v)) / len(v)))
>>> sum_count_avg.collect()
[
 ('B', (9, 3, 3.0)), 
 ('A', (15, 3, 5.0)), 
 ('C', (7, 2, 3.5))
]
>>>
~~~


# 2. `reduceByKey()` solution

~~~python
spark-3.5.0  % ./bin/pyspark
Python 3.11.4 (v3.11.4:d2340ef257, Jun  6 2023, 19:15:51) 
[Clang 13.0.0 (clang-1300.0.29.30)] on darwin
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.0
      /_/

Using Python version 3.11.4 
(v3.11.4:d2340ef257, Jun  6 2023 19:15:51)
Spark context available as 'sc' 
(master = local[*], app id = local-1697682144202).
SparkSession available as 'spark'.
>>>
>>>
>>> sc.version
'3.5.0'
>>>
>>> pairs = [('A', 2), ('A', 4), ('A', 9), 
             ('B', 6), ('B', 1),('B', 2), 
             ('C', 1), ('C', 6)]

>>> # rdd: RDD[(String, Integer)]
>>> rdd = sc.parallelize(pairs)
>>> rdd.count()
>>>
>>> # rdd2: RDD[(String, (Integer, Integer))]
>>> # rdd2: RDD[(key, (sum, count))]
>>> rdd2 = rdd.mapValues(lambda v: (v, 1))
>>> rdd2.collect()
[
 ('A', (2, 1)), 
 ('A', (4, 1)), 
 ('A', (9, 1)), 
 ('B', (6, 1)), 
 ('B', (1, 1)), 
 ('B', (2, 1)), 
 ('C', (1, 1)), 
 ('C', (6, 1))
]
>>> # Note that + is a commutative and associative function
>>> # x:(sum1, count1)
>>> # y:(sum2, count2)
>>> sum_count = rdd2.reduceByKey(
  lambda x, y: (x[0]+y[0], x[1]+y[1]))
>>> sum_count.collect()
[
 ('B', (9, 3)), 
 ('A', (15, 3)), 
 ('C', (7, 2))
]

>>> sum_count_avg = sum_count.mapValues(
  lambda v: (v[0], v[1], float(v[0]) / v[1]))
>>> sum_count_avg.collect()
[
 ('B', (9, 3, 3.0)), 
 ('A', (15, 3, 5.0)), 
 ('C', (7, 2, 3.5))
]
>>>
~~~