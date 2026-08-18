# Demo on Interactive PySpark

Date: October 5, 2023

~~~python
spark-3.5.0  % ./bin/pyspark
Python 3.11.4 (v3.11.4:d2340ef257, Jun  6 2023, 19:15:51) [Clang 13.0.0 (clang-1300.0.29.30)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
Setting default log level to "WARN".

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.0
      /_/

Using Python version 3.11.4 (v3.11.4:d2340ef257, Jun  6 2023 19:15:51)
Spark context Web UI available at http://10.112.112.189:4040
Spark context available as 'sc' (master = local[*], app id = local-1696554647420).
SparkSession available as 'spark'.
>>>
>>>
>>> sc
<SparkContext master=local[*] appName=PySparkShell>
>>>
>>> spark
<pyspark.sql.session.SparkSession object at 0x10e9569d0>
>>>
>>> spark.sparkContext
<SparkContext master=local[*] appName=PySparkShell>
>>>
>>>
>>> sc.version
'3.5.0'
>>>
>>>
>>> # create a list of numbers in Python
>>> numbers = [1, 2, 3, -2, -7, 8, 9, -7, -2, -3, 10, 12]
>>>
>>> # create a new rdd: RDD[Integer]
>>> rdd = sc.parallelize(numbers)
>>>
>>> rdd.collect()
[1, 2, 3, -2, -7, 8, 9, -7, -2, -3, 10, 12]
>>> rdd.count()
12
>>> 
>>> # create an RDD, where all elements are positive 
>>> positives = rdd.filter(lambda x: x > 0)
>>> positives.collect()
[1, 2, 3, 8, 9, 10, 12]
>>> # (lambda x: x > 0) is called a Boolean Predicate
>>>
>>>
>>> # create an RDD, where all elements are negative 
>>> negatives = rdd.filter(lambda x: x < 0)
>>> negatives.collect()
[-2, -7, -7, -2, -3]
>>>
>>> # inputfile.txt has 36000 records
>>> # an each record is a floating point number
>>> input_file = "/tmp/inputfile.txt"
>>> # create an RDD recs: RDD[String]
>>> recs = sc.textFile(input_file)
>>>
>>> recs.take(5)
['-76.000000', '24.000000', '-8.000000', '12.000000', '52.000000']
>>> recs.count()
36000
>>> recs_float = recs.map(lambda x: float(x))
>>> recs_float.take(5)
[-76.0, 24.0, -8.0, 12.0, 52.0]

>>> # source RDD: recs: RDD[String]
>>> # transformation: map()
>>> # target RDD: RDD[float]
>>>
>>> recs_float.count()
36000
>>> pos = recs_float.filter(lambda x: x > 0)
>>> pos.count()
15963
>>>
>>> sum_of_all = pos.reduce(lambda x, y: x+y)
>>> sum_of_all
10459220.0
>>>
>>>
>>>
>>> positives.collect()
[1, 2, 3, 8, 9, 10, 12]
>>> # p1: 1, 2, 3, 8
>>> # p2: 9, 10, 12
>>> my_sum = positives.reduce(lambda x, y: x+y)
>>> my_sum
45
>>> # p1: 1, 2, 3, 8 => 3, 3, 8 => 6, 8 => 14
>>> # p2: 9, 10, 12 => 19, 12 => 31
>>> # p1 + p2 => 45
>>>
>>>
>>>
>>>
>>> # Spark: has functions: Transformations & Actions
>>> # Transformations : RDD --> RDD
>>> # Action: RDD --> NON-RDD
>>>
>>> # Transformations: 
>>> #    textFile, 
>>> #    parallelize, 
>>> #    map,
>>> #    flatMap,
>>> #    groupByKey, 
>>> #    filter 
>>> #    mapValues
>>> #    reduceByKey
>>> 
>>> # Actions: 
>>> #   collect, 
>>> #   take
>>> #   count,
>>> #   reduce
>>> 
>>>
>>> # Transformations : SOURCE-RDD --> TARGET-RDD
>>> # Action: SOURCE-RDD --> Target as a NON-RDD
>>>
>>> # map: 1-to-1 Transformation
>>> # flatMap: 1-to-Many Transformation
>>>
>>>
>>> my_list = [ [1, 2, 3], [], [4, 5, 6, 7, 8], [] ]
>>> len(my_list)
4
>>> rdd = sc.parallelize(my_list)
>>> rdd.count()
4
>>> rdd.collect()
[[1, 2, 3], [], [4, 5, 6, 7, 8], []]
>>> # target RDD as: [1, 2, 3, 4, 5, 6, 7, 8]
>>>
>>> # flatMap: 1-to-Many Transformation, 
>>> # where Many refers to 0, 1, 2, 3, 4, 5, ....
>>>
>>> flattenned = rdd.flatMap(lambda x: x)
>>> flattenned.collect()
[1, 2, 3, 4, 5, 6, 7, 8]
>>> flattenned.count()
8
>>> pairs = [ [('a', 1), ('a', 2), ('b', 10), ('b', 20)], [], [('a', 7), ('a', 8), ('b', 60)] ]
>>> pairs
[[('a', 1), ('a', 2), ('b', 10), ('b', 20)], [], [('a', 7), ('a', 8), ('b', 60)]]
>>> len(pairs)
3
>>> rdd = sc.parallelize(pairs)
>>>
>>>
>>> rdd.collect()
[[('a', 1), ('a', 2), ('b', 10), ('b', 20)], [], [('a', 7), ('a', 8), ('b', 60)]]
>>> rdd.count()
3
>>> rdd_mapped = rdd.map(lambda x: x)
>>> rdd_mapped.collect()
[[('a', 1), ('a', 2), ('b', 10), ('b', 20)], [], [('a', 7), ('a', 8), ('b', 60)]]
>>> rdd_mapped.count()
3
>>> rdd_flatmapped = rdd.flatMap(lambda x: x)
>>> rdd_flatmapped.count()
7
>>> rdd_flatmapped.collect()
[('a', 1), ('a', 2), ('b', 10), ('b', 20), ('a', 7), ('a', 8), ('b', 60)]
>>> rdd.collect()
>>> [[('a', 1), ('a', 2), ('b', 10), ('b', 20)], [], [('a', 7), ('a', 8), ('b', 60)]]

>>> rdd.count()
>>> 3
3
>>> rdd_mapped = rdd.map(lambda x: x)
>>> rdd_mapped.collect()
[[('a', 1), ('a', 2), ('b', 10), ('b', 20)], [], [('a', 7), ('a', 8), ('b', 60)]]
>>> rdd_mapped.count()
3
>>> rdd_flatmapped = rdd.flatMap(lambda x: x)
>>> rdd_flatmapped.collect()
>>> [('a', 1), ('a', 2), ('b', 10), ('b', 20), ('a', 7), ('a', 8), ('b', 60)]
>>>
>>> rdd_flatmapped.collect()
[('a', 1), ('a', 2), ('b', 10), ('b', 20), ('a', 7), ('a', 8), ('b', 60)]
>>> # reduceBykey()
>>>
>>> # associative and commutative reduce function
>>> # associative: a + b + c = (a + (b +c)) = ((a+b) + c)
>>> # commutative: a + b = b + a
>>> # + : addition
>>> # (1 + (2+3)) = ((1+2)+3) = 6 => addition is associative
>>> # 1 + 7 = 7 + 1 = 8 => addition is commutative
>>>
>>> # find sum of values per key by using reduceBykey() transformation
>>>
>>> rdd_flatmapped.collect()
[('a', 1), ('a', 2), ('b', 10), ('b', 20), ('a', 7), ('a', 8), ('b', 60)]

>>> sum_per_key = rdd_flatmapped.reduceByKey(lambda x, y : x+y)
>>> sum_per_key.collect()
[('a', 18), ('b', 90)]
>>> # 1 - 7 = -6
>>> # 7 - 1 = +6
>>> # find sum of values per key by using groupByKey() transformation
>>> rdd_flatmapped.collect()
[('a', 1), ('a', 2), ('b', 10), ('b', 20), ('a', 7), ('a', 8), ('b', 60)]
>>> grouped_by_key = rdd_flatmapped.groupByKey()
>>> grouped_by_key.collect()
[
('a', <pyspark.resultiterable.ResultIterable object at 0x1113d5e90>),
('b', <pyspark.resultiterable.ResultIterable object at 0x1113c5ed0>)
]
>>> grouped_by_key.map(lambda x : (x[0], list(x[1]))).collect()
[('a', [1, 2, 7, 8]), ('b', [10, 20, 60])]
>>> sum_per_key = grouped_by_key.mapValues(lambda values: sum(values))
>>> sum_per_key.collect()
[('a', 18), ('b', 90)]
>>>
~~~