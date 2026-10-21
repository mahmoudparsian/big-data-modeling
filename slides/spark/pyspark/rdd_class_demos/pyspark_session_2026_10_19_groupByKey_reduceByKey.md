```
% ./bin/pyspark
Python 3.11.2 (v3.11.2:878ead1ac1, Feb  7 2023, 10:02:41) 
Type "help", "copyright", "credits" or "license" for more information.
Using Spark's default log4j profile: org/apache/spark/log4j2-defaults.properties
Setting default log level to "WARN".
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 4.2.0
      /_/

Spark context Web UI available at http://localhost:4040
Spark context available as 'sc' (master = local[*], app id = local-1792457688996).
SparkSession available as 'spark'.
>>>
>>>
>>>
>>> numbers = [1, 2, 3, 6, 7, 8, 9]
>>> rdd = sc.parallelize(numbers)
>>> rdd.collect()
[1, 2, 3, 6, 7, 8, 9]
>>> rdd.count()
7
>>>
>>> rdd.min()
1
>>> rdd.max()
9
>>> rdd.sum()
36
>>> strings = ['alex', 'jane', 'david', 'terry']
>>> rdd_str = sc.parallelize(strings)
>>> rdd_str.collect()
['alex', 'jane', 'david', 'terry']
>>> rdd_str.count()
4
>>> rdd_str.min()
'alex'
>>> rdd_str.max()
'terry'
>>> rdd_str.sum()
26/10/19 17:58:02 ERROR Executor: Exception in task 5.0 in stage 9.0 (TID 77)
org.apache.spark.api.python.PythonException: [PYTHON_EXCEPTION] An exception was thrown from the Python worker: Traceback (most recent call last):
     ^^^^^^
TypeError: unsupported operand type(s) for +: 'int' and 'str'

>>>
>>>
>>> rdd.collect()
[1, 2, 3, 6, 7, 8, 9]
>>> multiply = rdd.reduce(lambda x, y: x*y)
>>> multiply
18144
>>> rdd.collect()
[1, 2, 3, 6, 7, 8, 9]
>>> multiply = rdd.reduce(lambda x, y: x*y)
>>> multiply
18144
>>> powered = rdd.map(lambda x: x**x)
>>> powered.collect()
[1, 4, 27, 46656, 823543, 16777216, 387420489]
>>> rdd.map(lambda x: x**x).sum()
405067936
>>>
>>>
>>>
>>> kv = [('alex', 2), ('alex', 7), ('jane', 20), ('jane', 70), ('max', 40)]
>>>
>>>
>>>
>>>
>>> rdd = sc.parallelize(kv)
>>> rdd.collect()
[('alex', 2), ('alex', 7), ('jane', 20), ('jane', 70), ('max', 40)]
>>> rdd.count()
5
>>> avg_per_key = rdd.groupByKey().mapValues(lambda v: sum(v)/len(v)).collect()
>>> avg_per_key
[('alex', 4.5), ('jane', 45.0), ('max', 40.0)]
>>> rdd.groupByKey().mapValues(lambda v: list(v)).collect()
[('alex', [2, 7]), ('jane', [20, 70]), ('max', [40])]
>>>
>>> rdd.collect()
[('alex', 2), ('alex', 7), ('jane', 20), ('jane', 70), ('max', 40)]
>>> reduced_by_key = rdd.reduceByKey(lambda x, y: x+y)
>>> reduced_by_key.collect()
[('alex', 9), ('jane', 90), ('max', 40)]
>>>
>>> # The following WILL NOT WORK
>>> avg_by_key = rdd.reduceByKey(lambda x, y: (x+y)/2) XXXXXXXXXXX WRONG
                                                       ^^^^^^^^^^^

>>> rdd.collect()
[('alex', 2), ('alex', 7), ('jane', 20), ('jane', 70), ('max', 40)]

>>>
>>> rdd_with_count = rdd.mapValues(lambda v: (v,1))
>>> rdd_with_count.collect()
[('alex', (2, 1)), ('alex', (7, 1)), ('jane', (20, 1)), ('jane', (70, 1)), ('max', (40, 1))]


>>> rdd_with_count2 = rdd.map(lambda x: (x[0], (x[1],1)))
>>>
>>> rdd_with_count2.collect()
[('alex', (2, 1)), ('alex', (7, 1)), ('jane', (20, 1)), ('jane', (70, 1)), ('max', (40, 1))]
>>>
>>> sum_count_per_key = rdd_with_count2.reduceByKey(lambda x, y: (x[0]+y[0],x[1]+y[1]))
>>> sum_count_per_key.collect()
[('alex', (9, 2)), ('jane', (90, 2)), ('max', (40, 1))]
>>> avg_per_key = sum_count_per_key.mapValues(lambda v: v[0]/v[1])
>>> avg_per_key.collect()
[('alex', 4.5), ('jane', 45.0), ('max', 40.0)]
>>>
>>>
>>> lists = [ [1, 2, 3], [], [], [4, 5, 6] ]
>>> lists
[[1, 2, 3], [], [], [4, 5, 6]]
>>> len(lists)
4
>>> rdd = sc.parallelize(lists)
>>> rdd.count()
4
>>> rdd.collect()
[[1, 2, 3], [], [], [4, 5, 6]]

>>> rdd2 = rdd.flatMap(lambda x: x)
>>> rdd2.collect()
[1, 2, 3, 4, 5, 6]
>>> kv
[('alex', 2), ('alex', 7), ('jane', 20), ('jane', 70), ('max', 40)]
>>>
>>>
>>> lists = [ [('alex', 2), ('alex', 7), ('jane', 20)], [], [('jane', 20), ('jane', 70), ('max', 40)] ]
>>> lists
[[('alex', 2), ('alex', 7), ('jane', 20)], [], [('jane', 20), ('jane', 70), ('max', 40)]]
>>> len(lists)
3
>>> rdd = sc.parallelize(lists)
>>> rdd.collect()
[[('alex', 2), ('alex', 7), ('jane', 20)], [], [('jane', 20), ('jane', 70), ('max', 40)]]
>>> rdd.count()
3
>>> flattened = rdd.flatMap(lambda x: x)
>>> flattened.collect()
[('alex', 2), ('alex', 7), ('jane', 20), ('jane', 20), ('jane', 70), ('max', 40)]
>>> flattened.count()
6



