Problem: Given a set of (K, V) pairs,
find (average, min, max) per key 
using the combineByKey() transformation.

$ ./bin/pyspark
Python 3.7.2 (v3.7.2:9a3ffc0492, Dec 24 2018, 02:44:43)
[Clang 6.0 (clang-600.0.57)] on darwin
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.4.4
      /_/

Using Python version 3.7.2 (v3.7.2:9a3ffc0492, Dec 24 2018 02:44:43)
SparkSession available as 'spark'.
>>>

>>>
>>> spark
<pyspark.sql.session.SparkSession object at 0x119f71668>
>>>
>>> data = [('A', 2), ('A', 3), ('A', 4), ('A', 5), 
            ('B', 6), ('B', 7), ('B', 8) ]
>>> data
[('A', 2), ('A', 3), ('A', 4), ('A', 5), 
 ('B', 6), ('B', 7), ('B', 8)]

>>> # Create an RDD[(K, V)]
>>> # rdd: RDD[(String, Integer)]
>>> rdd = spark.sparkContext.parallelize(data)
>>>
>>>
>>> rdd.count()
7
>>> rdd.collect()
[('A', 2), ('A', 3), ('A', 4), ('A', 5), 
 ('B', 6), ('B', 7), ('B', 8)]
 
>>> # Using combineByKey(), create:
>>> # (K, (sum, count, min, max))

>>> # Define 3 functions for combineByKey():
>>> # Combined data type is (sum, count, min, max)

>>> # create a C as (sum, count, min, max)
>>> def single(v):
...    #      (sum, count, min, max)
...    return (v,   1,     v,   v)
...
>>> # C = (C[0]=sum, C[1]=count, C[2]=min, C[3]=max)
>>> def merge(C, v):
...    return (C[0]+v, C[1]+1, min(C[2],v), max(C[3],v))
...
>>> # C = (C[0]=sum, C[1]=count, C[2]=min, C[3]=max)
>>> # D = (D[0]=sum, D[1]=count, D[2]=min, D[3]=max)
>>> def combine(C, D):
...    return (
               C[0]+D[0], 
               C[1]+D[1], 
               min(C[2], D[2]), 
               max(C[3], D[3]) 
              )
...
>>> rdd2 = rdd.combineByKey(single, merge, combine)
>>> rdd2.collect()
[
 ('B', (21, 3, 6, 8)), 
 ('A', (14, 4, 2, 5))
]
>>>
>>> final = rdd2.mapValues(lambda t: (float(t[0])/t[1], t[2], t[3]))
>>> final.collect()
[
 ('B', (7.0, 6, 8)), 
 ('A', (3.5, 2, 5))
]
