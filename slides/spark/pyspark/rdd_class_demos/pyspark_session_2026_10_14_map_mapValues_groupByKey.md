# PySpark Demo Session on October 14, 2026

# Data File

```
% cat /home/mparsian/genes.txt
g1,10
g1,20
g1,55
g2,20
g2,30
g3,70
g3,60
g3,40
g3,80
```

# PySpark Interactive Session

```
% pwd
/home/mparsian/spark-4.2.0

% echo $SPARK_HOME
/home/mparsian/spark-4.2.0

% export SPARK_HOME="/home/mparsian/spark-4.2.0"
% echo $SPARK_HOME
/home/mparsian/spark-4.2.0

% $SPARK_HOME/bin/pyspark
Python 3.11.2 (v3.11.2:878ead1ac1, Feb  7 2023, 10:02:41) 
[Clang 13.0.0 (clang-1300.0.29.30)] on darwin
To adjust logging level use sc.setLogLevel(newLevel). 
For SparkR, use setLogLevel(newLevel).
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 4.2.0
      /_/

Using Python version 3.11.2 (v3.11.2:878ead1ac1, Feb  7 2023 10:02:41)
Spark context Web UI available at http://localhost:4040
Spark context available as 'sc' (master = local[*], app id = local-1792026210970).
SparkSession available as 'spark'.
>>>
>>>
>>> spark
<pyspark.sql.session.SparkSession object at 0x102f10690>
>>>
>>>
>>> sc
<SparkContext master=local[*] appName=PySparkShell>
>>>
>>>
>>> kv = [('alex', 20), ('alex', 30), ('jane', 40), ('jane', 60), ('max', 14)]
>>>
>>> kv
[('alex', 20), ('alex', 30), ('jane', 40), ('jane', 60), ('max', 14)]
>>>
>>> rdd = sc.parallelize(kv)
>>>
>>> rdd.count()
5
>>> rdd.collect()
[('alex', 20), ('alex', 30), ('jane', 40), ('jane', 60), ('max', 14)]
>>>
>>> rdd2 = rdd.map(lambda x: (x[0], x[1]+100))
>>> rdd2.collect()
[('alex', 120), ('alex', 130), ('jane', 140), ('jane', 160), ('max', 114)]
>>> # rdd: RDD[(String, Integer)]
>>> # rdd2: RDD[(String, Integer)]
>>>
>>>
>>> rdd2.count()
5
>>> rdd2.collect()
[('alex', 120), ('alex', 130), ('jane', 140), ('jane', 160), ('max', 114)]
>>>
>>> rdd3 = rdd2.filter(lambda x: x[1] > 125)
>>> rdd3.collect()
[('alex', 130), ('jane', 140), ('jane', 160)]
>>>
>>>
>>> x = ('alex', 20)
>>> x[0]
'alex'
>>> x[1]
20

>>>
>>> nums = sc.parallelize([1, 2, 45, 50, 60, 68, 100])
>>> # nums: RDD[Integer]
>>>
>>> nums.collect()
[1, 2, 45, 50, 60, 68, 100]
>>> nums.count()
7
>>> nums_greater_than_50 = nums.filter(lambda x: x > 50)
>>> nums_greater_than_50.collect()
[60, 68, 100]
>>> nums_greater_than_50.count()
3
>>> rdd.collect()
[('alex', 20), ('alex', 30), ('jane', 40), ('jane', 60), ('max', 14)]
>>> # rdd : RDD[(String, Integer)]
>>>
>>> grouped = rdd.groupByKey()
>>> grouped.collect()
[
 ('alex', <pyspark.resultiterable.ResultIterable object at 0x107e44090>),
 ('jane', <pyspark.resultiterable.ResultIterable object at 0x107e440d0>), 
 ('max', <pyspark.resultiterable.ResultIterable object at 0x107e441d0>)
]
>>> grouped.mapValues(lambda iter: list(iter)).collect()
[('alex', [20, 30]), ('jane', [40, 60]), ('max', [14])]

>>> grouped.map(lambda x: (x[0], list(x[1]))).collect()
[('alex', [20, 30]), ('jane', [40, 60]), ('max', [14])]
>>>
>>>
>>> input_path = "/home/mparsian/genes.txt"
>>> # records = RDD[String]
>>> records = sc.textFile(input_path)
>>> records.count()
9
>>> records.collect()
['g1,10', 'g1,20', 'g1,55', 'g2,20', 'g2,30', 'g3,70', 'g3,60', 'g3,40', 'g3,80']
>>>
>>> key_value = records.map(lambda x: x.split(","))
>>> key_value.collect()
[['g1', '10'], ['g1', '20'], ['g1', '55'], ['g2', '20'], ['g2', '30'], ['g3', '70'], ['g3', '60'], ['g3', '40'], ['g3', '80']]
>>>
>>>
>>> def tokenize(rec):
...     tokens = rec.split(",")
...     return (tokens[0], tokens[1])
...
>>>
>>> tokenize("g1,5")
('g1', '5')
>>> tokenize("g23,25")
('g23', '25')
>>>
>>> key_value = records.map(tokenize)
>>> key_value.collect()
[('g1', '10'), ('g1', '20'), ('g1', '55'), ('g2', '20'), ('g2', '30'), ('g3', '70'), ('g3', '60'), ('g3', '40'), ('g3', '80')]
>>>
>>> key_value_2 = records.map(lambda x : tokenize(x))
>>> key_value_2.collect()
[('g1', '10'), ('g1', '20'), ('g1', '55'), ('g2', '20'), ('g2', '30'), ('g3', '70'), ('g3', '60'), ('g3', '40'), ('g3', '80')]
>>>
>>> frequencies = key_value.groupByKey().mapValues(lambda: v: sum(v))
  File "<stdin>", line 1
    frequencies = key_value.groupByKey().mapValues(lambda: v: sum(v))
                                                            ^
SyntaxError: invalid syntax
>>> frequencies = key_value.groupByKey().mapValues(lambda v: sum(v))
>>> frequencies.collect()
26/10/14 18:56:22 ERROR Executor: Exception in task 1.0 in stage 23.0 (TID 133)
org.apache.spark.api.python.PythonException: [PYTHON_EXCEPTION] An exception was thrown from the Python worker: Traceback (most recent call last):
  File "/Users/mparsian/spark-4.2.0/python/lib/pyspark.zip/pyspark/worker.py", line 3622, in main
    process()
  File "/Users/mparsian/spark-4.2.0/python/lib/pyspark.zip/pyspark/worker.py", line 3612, in process
    serializer.dump_stream(out_iter, outfile)
  File "/Users/mparsian/spark-4.2.0/python/lib/pyspark.zip/pyspark/serializers.py", line 257, in dump_stream
    vs = list(itertools.islice(iterator, batch))
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/mparsian/spark-4.2.0/python/lib/pyspark.zip/pyspark/util.py", line 159, in wrapper
    return f(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^
  File "/Users/mparsian/spark-4.2.0/python/pyspark/core/rdd.py", line 4122, in map_values_fn
    return kv[0], f(kv[1])
                  ^^^^^^^^
  File "<stdin>", line 1, in <lambda>
TypeError: unsupported operand type(s) for +: 'int' and 'str'
 SQLSTATE: 38000
TypeError: unsupported operand type(s) for +: 'int' and 'str'
>>>
>>> # redefine the tokenize(), cast gene values to Integer (int())
>>> def tokenize(rec):
...     tokens = rec.split(",")
...     return (tokens[0], int(tokens[1]))
...
>>> key_value = records.map(tokenize)
>>> key_value.collect()
[('g1', 10), ('g1', 20), ('g1', 55), ('g2', 20), ('g2', 30), ('g3', 70), ('g3', 60), ('g3', 40), ('g3', 80)]
>>> frequencies = key_value.groupByKey().mapValues(lambda v: sum(v))
>>> frequencies.collect()
[('g3', 250), ('g1', 85), ('g2', 50)]
>>> filtered = frequencies.filter(lambda x: x[1] > 60)
>>> filtered.collect()
[('g3', 250), ('g1', 85)]
>>>
>>>
>>> triplets = [('alex', 20, 30), ('alex', 40, 50), ('jane', 20, 300) ]
>>> rdd = sc.parallelize(triplets)
>>> rdd.collect()
[('alex', 20, 30), ('alex', 40, 50), ('jane', 20, 300)]
>>> rdd.count()
3
>>> rdd2 = rdd.map(lambda x: (x[0], x[1]+x[2]))
>>> rdd2.collect()
[('alex', 50), ('alex', 90), ('jane', 320)]
>>> keys = rdd2.map(lambda x: x[0])
>>> keys.collect()
['alex', 'alex', 'jane']
>>> keys.distinct()
PythonRDD[42] at RDD at PythonRDD.scala:59
>>> keys.distinct().collect()
['alex', 'jane']
>>>
>>>
>>>
>>> x = (10, 20)
>>> sum(x)
30
>>> x = ('alex', 20, 40)
>>> sum(x)
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
TypeError: unsupported operand type(s) for +: 'int' and 'str'
>>>
>>> (10, 20)
(10, 20)
>>> ('alex', 30)
('alex', 30)
>>> ('alex', (10, 20, 30, 40))
('alex', (10, 20, 30, 40))
>>>
>>> ( ('alex', 'sales'), (10, 20, 40, 'jane', 'max'))
(('alex', 'sales'), (10, 20, 40, 'jane', 'max'))
>>> x = [('alex', (20, 30)), ('alex', (40, 50))]
>>> x
[('alex', (20, 30)), ('alex', (40, 50))]
>>> rdd = sc.parallelize(x)
>>> rdd.collect()
[('alex', (20, 30)), ('alex', (40, 50))]

>>> x = [('alex', (20, 30)), ('alex', (40, 50))]
>>> x = [('alex', [20, 30]), ('alex', [40, 50, 60])]
>>>
>>> x
[('alex', [20, 30]), ('alex', [40, 50, 60])]

