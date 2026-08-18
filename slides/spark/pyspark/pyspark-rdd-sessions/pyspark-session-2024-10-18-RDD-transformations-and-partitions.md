# Big Data Modeling
# TODAY: Thursday, October 17, 2024
# Agenda:


# Introduce PySpark Transformations & Actions
~~~

Transformation: RDD --> RDD

Action: RDD --> NON-RDD

~~~

# Mappers
       * `map()`:          1-to-1
       * `flatMap()`:      1-to-Many (Many can be 0, 1, 2, 3, ....)
       * `mapPartitions()`: Many-to-1
   
# Reducers
   
       * groupByKey()
         	
         # source_rdd: RDD[(K, V)]
         # target_rdd: RDD[(K, Iterable<V>)]
         target_rdd = source_rdd.groupByKey()
                  
       * reduceByKey()
       * combineByKey()
       
# Filters
   
       * filter()
       * distinct()

   
# PySpark Interactive Session

~~~
% cd /home/mparsian/spark-3.5.3
% ./bin/pyspark
Python 3.12.0 (v3.12.0:0fb18b02c8, Oct  2 2023, 09:45:56) 
[Clang 13.0.0 (clang-1300.0.29.30)] on darwin
Type "help", "copyright", "credits" or "license" for more information.

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.3
      /_/

Using Python version 3.12.0 
Spark context Web UI available at http://172.20.206.26:4040
Spark context available as 'sc' 
(master = local[*], app id = local-1729219382654).
SparkSession available as 'spark'.
>>>
>>>
>>> strings = ["fox jumped", "fox jumped over", "fox jumped fast", "fox jumped big"]

>>> len(strings)
4

>>> # create an RDD[String] from a Python collection
>>> rdd = sc.parallelize(strings)
>>> rdd.count()
4
>>> rdd.collect()
[
 'fox jumped', 
 'fox jumped over', 
 'fox jumped fast', 
 'fox jumped big'
]
>>>
>>> # rdd: source RDD: RDD[String]
>>> # rdd2: target RDD: RDD[(String, Integer)]
>>> rdd2 = rdd.map(lambda x: (x, len(x)))
>>> rdd2.collect()
[
 ('fox jumped', 10), 
 ('fox jumped over', 15), 
 ('fox jumped fast', 15), 
 ('fox jumped big', 14)
]
>>>
>>> rdd3 = rdd.map(lambda x: (x, len(x), 100))
>>> rdd3.collect()
[
 ('fox jumped', 10, 100), 
 ('fox jumped over', 15, 100), 
 ('fox jumped fast', 15, 100), 
 ('fox jumped big', 14, 100)
]
>>>
>>> def func1(e):
...     return (e, len(e), 300)
...
>>> # test func1()
>>> func1("I am here")
('I am here', 9, 300)
>>> func1("fox jumped")
('fox jumped', 10, 300)
>>>
>>>
>>> # map() transformation with a Python function
>>> rdd4 = rdd.map(func1)
>>> rdd4.collect()
[
 ('fox jumped', 10, 300), 
 ('fox jumped over', 15, 300), 
 ('fox jumped fast', 15, 300), 
 ('fox jumped big', 14, 300)
]
>>>
>>> def tokenize(e):
...     words = []
...     tokens = e.split(" ")
...     for word in tokens:
...        words.append(word)
...
...     return words
...
...
>>> # test tokenize() function
>>> tokenize("I am here and there")
['I', 'am', 'here', 'and', 'there']
>>>
>>>
>>> rdd.collect()
[
 'fox jumped', 
 'fox jumped over', 
 'fox jumped fast', 
 'fox jumped big'
]
>>>
>>> rdd2 = rdd.map(tokenize)
>>>
>>> rdd2.collect()
[
 ['fox', 'jumped'], 
 ['fox', 'jumped', 'over'], 
 ['fox', 'jumped', 'fast'], 
 ['fox', 'jumped', 'big']
]
>>> rdd2.count()
4
>>>
>>>
>>> rdd3 = rdd2.flatMap(lambda e: e)
>>> rdd3.collect()
[
 'fox', 
 'jumped', 
 'fox', 
 'jumped', 
 'over', 
 'fox', 
 'jumped', 
 'fast', 
 'fox', 
 'jumped', 
 'big'
]
>>> rdd3.count()
11
>>>
>>>
>>>
>>> strings = ["fox jumped", "", "fox jumped over", 
>>>            "", "fox jumped fast", "fox jumped big"]
>>> len(strings)
6
>>> def tokenize(e):
...     if len(e) == 0:
...         return []
...
...     words = []
...     tokens = e.split(" ")
...     for word in tokens:
...        words.append(word)
...
...     return words
...
...
>>> # test tokenize() function
>>> tokenize("fox jumped fast")
['fox', 'jumped', 'fast']

>>> # test tokenize() function
>>> tokenize("")
[]

>>>
>>> rdd4 = sc.parallelize(strings)
>>> rdd4.collect()
[
 'fox jumped', 
 '', 
 'fox jumped over', 
 '', 
 'fox jumped fast', 
 'fox jumped big'
]
>>> rdd4.count()
6
>>>
>>> rdd5 = rdd4.flatMap(tokenize)
>>> rdd5.collect()
[
 'fox', 
 'jumped', 
 'fox', 
 'jumped', 
 'over', 
 'fox', 
 'jumped', 
 'fast', 
 'fox', 
 'jumped', 
 'big'
]
>>> rdd5.count()
11
>>>
>>> # create (K, V) pairs
>>> key_value = rdd5.map(lambda x: (x, 1))
>>> key_value.collect()
[
 ('fox', 1), 
 ('jumped', 1), 
 ('fox', 1), 
 ('jumped', 1), 
 ('over', 1), 
 ('fox', 1), 
 ('jumped', 1), 
 ('fast', 1), 
 ('fox', 1), 
 ('jumped', 1), 
 ('big', 1)
]
>>>
>>>
>>> # grouped: RDD[(String, ResultIterable)]
>>> grouped = key_value.groupByKey()
>>> grouped.collect()
[
 ('over', <pyspark.resultiterable.ResultIterable object at 0x10d1064e0>), 
 ('fast', <pyspark.resultiterable.ResultIterable object at 0x10d106e10>), 
 ('jumped', <pyspark.resultiterable.ResultIterable object at 0x10d106630>), 
 ('fox', <pyspark.resultiterable.ResultIterable object at 0x10d1066f0>), 
 ('big', <pyspark.resultiterable.ResultIterable object at 0x10d107290>)
]
>>> # debug grouped object
>>> grouped.map(lambda x: (x[0], list(x[1]))).collect()
[
 ('over', [1]), 
 ('fast', [1]), 
 ('jumped', [1, 1, 1, 1]), 
 ('fox', [1, 1, 1, 1]), 
 ('big', [1])
]
>>>
>>>
>>> frequency = grouped.map(lambda x: (x[0], sum(x[1])))
>>> frequency.collect()
[
 ('over', 1), 
 ('fast', 1), 
 ('jumped', 4), 
 ('fox', 4), 
 ('big', 1)
]
>>>
>>> # using mapValues() transformation
>>> frequency2 = grouped.mapValues(lambda values:  sum(values))
>>> frequency2.collect()
[
 ('over', 1), 
 ('fast', 1), 
 ('jumped', 4), 
 ('fox', 4), 
 ('big', 1)
]
>>> frequency3 = grouped.mapValues(lambda x:  sum(x))
>>> frequency3.collect()
[
 ('over', 1), 
 ('fast', 1), 
 ('jumped', 4), 
 ('fox', 4), 
 ('big', 1)
]
>>> rdd.collect()
[
 'fox jumped', 
 'fox jumped over', 
 'fox jumped fast', 
 'fox jumped big'
]
>>> rdd.take(2)
['fox jumped', 'fox jumped over']
>>>
>>>
>>>
>>> #---------------------------
>>> # Understandings Partitions
>>> #---------------------------
>>> numbers = [1, 2, 3, 4, 5, 1, 2, 
>>>            3, 4, 5, 1, 2, 3, 4, 
>>>            5, 1, 2, 3, 4, 5, 1, 
>>>            2, 3, 4, 5, 1, 2, 3, 
>>>            4, 5, 1, 2, 3, 4, 5]

>>> len(numbers)
35
>>> rdd = sc.parallelize(numbers)
>>> rdd.count()
35
>>> rdd.collect()
[1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 
 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 
 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 
 1, 2, 3, 4, 5]

>>> # find the number of partitions for rdd
>>> rdd.getNumPartitions()
16

>>> numbers = [1, 2, 3, 4, 5, 10, 20, 
>>>            30, 40, 50, 100, 200, 
>>>            300, 400, 500, 1000, 2000, 
>>>            3000, 4000, 5000, 100, 200, 
>>>            300, 400, 500, 100, 20000, 
>>>            30000, 40000, 50000, 10000, 
>>>            20000, 30000, 40000, 50000]

>>> # create and RDD[Integer] from a Python collection 
>>> rdd = sc.parallelize(numbers)
>>> rdd.collect()
[1, 2, 3, 4, 5, 10, 20, 30, 40, 50, 
100, 200, 300, 400, 500, 1000, 2000, 
3000, 4000, 5000, 100, 200, 300, 400, 
500, 100, 20000, 30000, 40000, 50000, 
10000, 20000, 30000, 40000, 50000]

>>> rdd.count()
35
>>> rdd.getNumPartitions()
16

>>> # create a debugger function 
>>> # to examine the content of partitions
>>> # one by one
>>> def debug_partition(partition):
...     print("elements = ", list(partition))
...
>>>
>>> # examine the content of each partition
>>> rdd.foreachPartition(debug_partition)
elements =  [20, 30]
elements =  [30000, 40000, 50000]
elements =  [10000, 20000]
elements =  [20000, 30000]
elements =  [500, 100]
elements =  [40000, 50000]
elements =  [100, 200]
elements =  [300, 400]
elements =  [4000, 5000]
elements =  [500, 1000, 2000, 3000]
elements =  [300, 400]
elements =  [100, 200]
elements =  [5, 10]
elements =  [3, 4]
elements =  [40, 50]
elements =  [1, 2]
>>>
>>>
>>> #-------------------------------------
>>> # Create an RDD with 4 partitions only
>>> #-------------------------------------
>>> rdd = sc.parallelize(numbers, 4)
>>> rdd.foreachPartition(debug_partition)
elements =  [40, 50, 100, 200, 300, 400, 500, 1000]
elements =  [1, 2, 3, 4, 5, 10, 20, 30]
elements =  [500, 100, 20000, 30000, 40000, 50000, 10000, 20000, 30000, 40000, 50000]
elements =  [2000, 3000, 4000, 5000, 100, 200, 300, 400]
>>>
~~~


# Question: Simple Word Count in PySpark

* Given a set of documents (text files) in a folder,
* Find frequency of every unique word.
* If a word length is less than 3, ignore it.
* If a final frequency of a word is less than 4, then ignore it.


