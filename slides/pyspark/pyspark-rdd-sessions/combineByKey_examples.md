# RDD.combineByKey() Examples

## combineByKey() Definition
~~~
		Generic function to combine the 
		elements for each key using a custom 
		set of aggregation functions.

		Turns an RDD[(K, V)] into a result of 
		type RDD[(K, C)], for a “combined type” 
		C, where V and C can be the same OR 
		different data types. 
		
		The real power of combineByKey() comes 
		from the fact that V and C can be different 
		data types, while in reduceByKey(), V and C 
		must be the same data types.
		 
		


         RDD.combineByKey()
         source_rdd: RDD[(K, V)]
         target_rdd: RDD[(K, C)]
         
         V and C can be the same data type
         V and C can be different data type
         
         Examples:
             V: Integer, C: Integer
             V: Integer, C: (Integer, Double)
             V: Integer, C: (String, Integer, Integer)
             V: (Integer, Double), C: Integer
             V: (Integer, Double), C: (Integer, Double, Integer)
         

         # source_rdd: RDD[(K, V)]
         # target_rdd: RDD[(K, C)]
         target_rdd = source_rdd.combineByKey(f1, f2, f3)
         
         RDD.combineByKey(
                 f1: createCombiner: Callable[[V], C], 
                 f2: mergeValue: Callable[[C, V], C], 
                 f3: mergeCombiners: Callable[[C, C], C], 
                 numPartitions: Optional[int] = None, 
                 partitionFunc: Callable[[K], int] = 
                 <function portable_hash>) → pyspark.rdd.RDD[Tuple[K, C]]
~~~
                 
## combineByKey() in Action

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
Spark context Web UI available at http://172.20.216.95:4040
Spark context available as 'sc' 
(master = local[*], app id = local-1730257212445).
SparkSession available as 'spark'.
>>>
>>> pairs = [('A', 2), ('A', 3), ('A', 7), ('B', 3), ('B', 4), ('A', 5), ('B', 9)]
>>> pairs
[('A', 2), ('A', 3), ('A', 7), ('B', 3), ('B', 4), ('A', 5), ('B', 9)]
>>>
>>> rdd = sc.parallelize(pairs)
>>> rdd.count()
7

>>> #-----------------------------------------------------------
>>> # Given rdd as RDD[(String, Integer)],
>>> # find (count, avg) per key by using groupByKey()
>>> #-----------------------------------------------------------
>>> rdd.collect()
[('A', 2), ('A', 3), ('A', 7), ('B', 3), ('B', 4), ('A', 5), ('B', 9)]
>>> # find (count, avg) per key
>>>
>>> # ('A', (4, 4.25))
>>> # ('B', (3, 5.33))
>>>
>>> # using groupByKey()
>>> grouped = rdd.groupByKey()
>>> grouped.mapValues(lambda values: list(values)).collect()
[
 ('B', [3, 4, 9]), 
 ('A', [2, 3, 7, 5])
]
>>> answer = grouped.mapValues(
  lambda values: (len(values), float(sum(values))/len(values)))
>>> answer.collect()
[
 ('B', (3, 5.333333333333333)), 
 ('A', (4, 4.25))
]
>>>
>>> #-----------------------------------------------------------
>>> # Given rdd as RDD[(String, Integer)],
>>> # find (count, avg) per key by using reduceByKey()
>>> #-----------------------------------------------------------
>>>
>>> # using reduceByKey()
>>> # first create a monoid data structure as (sum, count)
>>> rdd2 = rdd.mapValues(lambda v: (v, 1))
>>> rdd2.collect()
[
 ('A', (2, 1)), 
 ('A', (3, 1)), 
 ('A', (7, 1)), 
 ('B', (3, 1)), 
 ('B', (4, 1)), 
 ('A', (5, 1)), 
 ('B', (9, 1))
]
>>> sum_count = rdd2.reduceByKey(
  lambda x, y: (x[0]+y[0], x[1]+y[1]))
>>> sum_count.collect()
[
 ('B', (16, 3)), 
 ('A', (17, 4))
]
>>> answer2 = sum_count.mapValues(lambda v: (v[1], float(v[0])/v[1]))
>>> answer2.collect()
[
 ('B', (3, 5.333333333333333)), 
 ('A', (4, 4.25))
]
>>>
>>> #-----------------------------------------------------------
>>> # Given rdd as RDD[(String, Integer)],
>>> # find (count, avg) per key by using combineByKey()
>>> #-----------------------------------------------------------
>>>
>>> # combineByKey
>>> # C : (Integer, Integer)
>>> # V:  Integer
>>> def f1(v):
...     return (v, 1)
...
>>> def f2(C, v):
...     return (C[0]+v, C[1]+1)
...
>>>
>>> def f3(C1, C2):
...     return (C1[0]+C2[0], C1[1]+C2[1])
...
>>> # C = (Integer, Integer) = (sum, count)
>>>
>>> sum_count = rdd.combineByKey(f1, f2, f3)
>>>
>>> sum_count.collect()
[
 ('B', (16, 3)), 
 ('A', (17, 4))
]
>>> # rdd: RDD[(String, Integer)]
>>> # sum_count: RDD[(String, (Integer, Integer)]
>>> answer3 = sum_count.mapValues(lambda v: (v[1], float(v[0])/v[1]))
>>> answer3.collect()
[
 ('B', (3, 5.333333333333333)), 
 ('A', (4, 4.25))
]
>>>
~~~

## combineByKey() in Action 2

~~~
	Given rdd: RDD[(String, Integer)],
	then find (K, (P, N, Count, AVG))

    given rdd: RDD[(String, Integer)]
    using combineByKey find (K, (P, N, Count, AVG))
    where
       K = key of source rdd
       P = # of positives per key
       N = # of negatives per key
       Count = total number of values per key
       AVG = average of all values per key

	   Our combined data type C:
	   
         C = (Positives, Negatives, Count, Sum)
             (Integer, Integer, Integer, Integer)

>>> # Create (K, V) data collection in Python
>>> pairs = [('A', 2), ('A', 4), ('A', -1), ('A', 0), 
             ('B', 9), ('B', -4), ('B', -2), ('B', 0), ('B', 0)]
>>> pairs
[('A', 2), ('A', 4), ('A', -1), ('A', 0), 
('B', 9), ('B', -4), ('B', -2), ('B', 0), ('B', 0)]

>>> # create rdd: RDD[(String, Integer)]
>>> rdd = sc.parallelize(pairs)
>>> rdd.collect()
[
 ('A', 2), ('A', 4), ('A', -1), ('A', 0), 
 ('B', 9), ('B', -4), ('B', -2), ('B', 0), ('B', 0)
]
>>> #rdd: RDD[(K=String, V=Integer)]
>>>
>>> # v: Integer
>>> # C = (P, N, Count, sum)
>>> # C = (Integer, Integer, Integer, Integer)

>>> def f1(v):
...     if (v > 0): return (1, 0, 1, v)
...     if (v < 0): return (0, 1, 1, v)
...     return (0, 0, 1, 0)
...
>>>
>>> # v: Integer
>>> # C = (P, N, Count, Sum)
>>> def f2(C, v):
...     if (v > 0): return (C[0]+1, C[1], C[2]+1, C[3]+v)
...     if (v < 0): return (C[0], C[1]+1, C[2]+1, C[3]+v)
...     return (C[0], C[1], C[2]+1, C[3])
...
>>> #index: 0  1  2      3
>>> # C1 = (P, N, Count, Sum)
>>> # C2 = (P, N, Count, Sum)
>>> def f3(C1, C2):
...     return (C1[0]+C2[0], C1[1]+C2[1], C1[2]+C2[2], C1[3]+C2[3])
...
>>> reduced = rdd.combineByKey(f1, f2, f3)
>>>
>>> reduced.collect()
[
 ('B', (1, 2, 5, 3)), 
 ('A', (2, 1, 4, 5))
]
>>> final = reduced.mapValues(
 lambda t: (t[0], t[1], t[2], float(t[3])/t[2]))
>>> final.collect()
[
 ('B', (1, 2, 5, 0.6)), 
 ('A', (2, 1, 4, 1.25))
]
~~~


