# Big Data Modeling

~~~
================================
TODAY: Tuesday, October 22, 2024
================================
1. Questions/Answers

2. Introduce PySpark Transformations & Actions

   2.1 Mappers       type-of-transformation
   -----------       ----------------------
   * map():          1-to-1
   * flatMap()       1-to-Many (Many can be 0, 1, 2, 3, ....)
   * mapPartitions() Many-to-1 (Many can be 0, 1, 2, 3, ..., 1000,000)
   * mapValues()     1-to-1: transform values only
   
   2.2 Reducers by (K, V)
   -----------------------
   * groupByKey()
         source_rdd: RDD[(K, V)]
         target_rdd: RDD[(K, Iterable<V>)]
         target_rdd = source_rdd.groupByKey()
         
   * reduceByKey():  RDD[(K, V)] --> RDD[(K, V)] 
       
   * combineByKey(): RDD[(K, V)] --> RDD[(K, C)] 
         where C and V can be the same or different
         
   2.3 Reducers with NO Keys
       * reduce()
             
   2.4 Filters
       * filter()
       * distinct()
~~~

## [RDD.reduce()](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.reduce.html)

~~~
RDD.reduce(f: Callable[[T, T], T]) → T[source]
~~~

Description:

~~~
	Reduces the elements of this RDD 
	using the specified commutative 
	and associative binary operator. 
	Currently reduces partitions locally.
~~~


## [RDD.reduceByKey()](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.reduceByKey.html)

~~~
RDD.reduceByKey(func: Callable[[V, V], V], 
                numPartitions: Optional[int] = None, 
                partitionFunc: Callable[[K], int] = 
                 <function portable_hash>) 
                → pyspark.rdd.RDD[Tuple[K, V]]
~~~

Description:

~~~              
	Merge the values for each key using an 
	associative and commutative reduce function.

	This will also perform the merging locally 
	on each mapper before sending results to a 
	reducer, similarly to a “combiner” in MapReduce.

	Output will be partitioned with numPartitions 
	partitions, or the default parallelism level 
	if numPartitions is not specified. Default 
	partitioner is hash-partition.
~~~

## PySpark Demo

~~~
% ./bin/pyspark
Python 3.12.0 (v3.12.0:0fb18b02c8, Oct  2 2023, 09:45:56) 
[Clang 13.0.0 (clang-1300.0.29.30)] on darwin
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.3
      /_/

Using Python version 3.12.0 
(v3.12.0:0fb18b02c8, Oct  2 2023 09:45:56)
Spark context Web UI available at http://172.20.192.229:4040
Spark context available as 'sc' 
(master = local[*], app id = local-1729651336195).
SparkSession available as 'spark'.
>>>
>>> numbers = [1, 2, 3, 4, 5]

>>> # create rdd : RDD[Integer]
>>> rdd = sc.parallelize(numbers)
>>>
>>> rdd.count()
5
>>> rdd.collect()
[1, 2, 3, 4, 5]
>>>
>>>
>>>
>>> rdd.sum()
15

>>> total = rdd.reduce(lambda x, y: x+y)
>>> total
15

>>> triplets =[(1, 2, 3), (4, 5, 6), 
               (7, 8, 9), (10, 20, 30)]
>>> # create rdd : RDD[(Integer, Integer, Integer)]
>>> rdd = sc.parallelize(triplets)
>>> rdd.count()
4

>>> rdd.collect()
[
 (1, 2, 3), 
 (4, 5, 6), 
 (7, 8, 9), 
 (10, 20, 30)
]
>>>
>>> x = (1, 2, 3)
>>> min(x)
1

>>> minimums = rdd.map(lambda x: min(x))
>>> minimums.collect()
[
 1, 
 4, 
 7, 
 10
]
>>> minimums.sum()
22

>>> minimums.reduce(lambda x, y: x+y)
22
>>>
>>> key_value = 
    [
     ('A', 2), ('A', 3), ('A', 4), ('A', 5), 
     ('B', 20), ('B', 30), ('B', 40), 
     ('C', 8)
    ]
>>>
>>> key_value
[
 ('A', 2), ('A', 3), ('A', 4), ('A', 5), 
 ('B', 20), ('B', 30), ('B', 40), 
 ('C', 8)
]
>>> # create rdd : RDD[(String, Integer)]
>>> rdd = sc.parallelize(key_value)
>>>
>>> rdd.count()
8

>>> rdd.collect()
[
 ('A', 2), 
 ('A', 3), 
 ('A', 4), 
 ('A', 5), 
 ('B', 20), 
 ('B', 30), 
 ('B', 40), 
 ('C', 8)
]
>>>
>>>
>>> grouped = rdd.groupByKey()
>>> grouped.collect()
[
 ('B', <pyspark.resultiterable.ResultIterable object at 0x106504920>), 
 ('A', <pyspark.resultiterable.ResultIterable object at 0x10451ec90>), 
 ('C', <pyspark.resultiterable.ResultIterable object at 0x108c94470>)
]

>>> grouped.mapValues(lambda x: list(x)).collect()
[
 ('B', [20, 30, 40]), 
 ('A', [2, 3, 4, 5]), 
 ('C', [8])
]
>>>
>>> avg_by_key = grouped.mapValues(
    lambda values: float(sum(values))/len(values)
)
>>> avg_by_key.collect()
[('B', 30.0), ('A', 3.5), ('C', 8.0)]
>>>
>>> #### WRONG Transformation to find average by key
>>> #### AVG of AVG is not an AVG
>>> #### AVG is not an associative function
>>> avg_by_key = rdd.reduceByKey(lambda x, y: (x+y)/2) #### WRONG

>>>
>>>
>>> rdd.collect()
[
 ('A', 2), ('A', 3), ('A', 4), ('A', 5), 
 ('B', 20), ('B', 30), ('B', 40), 
 ('C', 8)
]

>>> rdd2 = rdd.mapValues(lambda v: (v, 1))
>>> rdd2.collect()
[
 ('A', (2, 1)), 
 ('A', (3, 1)), 
 ('A', (4, 1)),
 ('A', (5, 1)), 
 ('B', (20, 1)), 
 ('B', (30, 1)), 
 ('B', (40, 1)), 
 ('C', (8, 1))
]
>>>
>>>
>>> sum_count = rdd2.reduceByKey(
      lambda x, y: (x[0]+y[0], x[1]+y[1])
)
>>> sum_count.collect()
[
 ('B', (90, 3)), 
 ('A', (14, 4)), 
 ('C', (8, 1))
]
>>>
>>> avg_by_key = sum_count.mapValues(lambda v: v[0]/v[1])
>>> avg_by_key.collect()
[
 ('B', 30.0), 
 ('A', 3.5), 
 ('C', 8.0)
]
>>>
~~~
