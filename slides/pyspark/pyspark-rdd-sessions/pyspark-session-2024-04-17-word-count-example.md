# RDD Demo Session: Word Count

# Input Data

~~~text
% ls -l /tmp/mydata/
total 16
-rw-r--r--  1 aaaa  wheel  63 Apr 17 18:45 file1
-rw-r--r--  1 aaaa  wheel  50 Apr 17 18:45 file2

% cat file1
fox jumped
fox jumped and fox jumped
fox jumped here and there

% cat file2
fox jumped
fox is crazy
fox is crazy
fox is crazy

% wc -l /tmp/mydata/file1 /tmp/mydata/file2
       3 /tmp/mydata/file1
       4 /tmp/mydata/file2
       7 total
~~~

# Interactive PySpark

~~~text
spark-3.5.0  % ./bin/pyspark
Python 3.12.0 (v3.12.0:0fb18b02c8, Oct  2 2023, 09:45:56) 
[Clang 13.0.0 (clang-1300.0.29.30)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.0
      /_/

Using Python version 3.12.0 (v3.12.0:0fb18b02c8, Oct  2 2023 09:45:56)
Spark context Web UI available at http://c02fj5kymd6n.hsd1.ca.comcast.net:4040
Spark context available as 'sc' (master = local[*], app id = local-1713404822109).
SparkSession available as 'spark'.
>>>
>>> sc.version
'3.5.0'
>>> sc
<SparkContext master=local[*] appName=PySparkShell>
>>> spark.version
'3.5.0'
>>> spark
<pyspark.sql.session.SparkSession object at 0x10a427c80>
>>>
>>> spark.sparkContext
<SparkContext master=local[*] appName=PySparkShell>
>>>
>>>
>>> # define your input path
>>> input_path = "/tmp/mydata/"
>>>
>>> # creat a new RDD[String] and call it rdd1
>>> rdd1 = sc.textFile(input_path)
>>>
>>>
>>> rdd1
/tmp/mydata/ MapPartitionsRDD[1] at textFile at NativeMethodAccessorImpl.java:0
>>> rdd1.collect()
[
 'fox jumped', 
 'fox is crazy', 
 'fox is crazy', 
 'fox is crazy', 
 'fox jumped', 
 'fox jumped and fox jumped', 
 'fox jumped here and there'
]
>>> rdd1.count()
7
>>> rdd1.take(3)
[
 'fox jumped', 
 'fox is crazy', 
 'fox is crazy'
]

>>> rdd1.getNumPartitions()
3
>>> # e denotes an element of an RDD[String]
>>> # e: "fox jumped"
>>> def tokenize(e):
...     words = e.split(" ")
...     output_list = []
...     for word in words:
...        key_value = (word, 1)
...        output_list.append(key_value)
...     #end-for
...     return output_list
... #end-def
...
>>>
>>> tokenize("fox jumped and jumped")
[('fox', 1), ('jumped', 1), ('and', 1), ('jumped', 1)]
>>>
>>>
>>> # apply a map() transformation
>>> key_value = rdd1.map(tokenize)
>>> key_value.collect()
[
 [('fox', 1), ('jumped', 1)], 
 [('fox', 1), ('is', 1), ('crazy', 1)], 
 [('fox', 1), ('is', 1), ('crazy', 1)], 
 [('fox', 1), ('is', 1), ('crazy', 1)], 
 [('fox', 1), ('jumped', 1)], 
 [('fox', 1), ('jumped', 1), ('and', 1), ('fox', 1), ('jumped', 1)], 
 [('fox', 1), ('jumped', 1), ('here', 1), ('and', 1), ('there', 1)]
]
>>> key_value.count()
7
>>> # apply a flatMap() transformation: 1-to-Many
>>> pairs = key_value.flatMap(lambda x: x)
>>> pairs.count()
23

>>> pairs.collect()
[
 ('fox', 1), ('jumped', 1), ('fox', 1), ('is', 1), 
 ('crazy', 1), ('fox', 1), ('is', 1), ('crazy', 1), 
 ('fox', 1), ('is', 1), ('crazy', 1), ('fox', 1), 
 ('jumped', 1), ('fox', 1), ('jumped', 1), ('and', 1), 
 ('fox', 1), ('jumped', 1), ('fox', 1), ('jumped', 1), 
 ('here', 1), ('and', 1), ('there', 1)
]
>>> # pairs: RDD[(key, value)] : [RDD(String, Integer)]
>>>
>>> grouped = pairs.groupByKey()
>>> grouped.collect()
[
 ('fox', <pyspark.resultiterable.ResultIterable object at 0x10a49ef90>), 
 ('and', <pyspark.resultiterable.ResultIterable object at 0x106526ea0>), 
 ('jumped', <pyspark.resultiterable.ResultIterable object at 0x10a49ef30>), 
 ('is', <pyspark.resultiterable.ResultIterable object at 0x10a49f020>), 
 ('crazy', <pyspark.resultiterable.ResultIterable object at 0x10a49f080>), 
 ('here', <pyspark.resultiterable.ResultIterable object at 0x10a49f0e0>), 
 ('there', <pyspark.resultiterable.ResultIterable object at 0x10a49f1d0>)
]
>>> grouped.count()
7

>>> grouped.map(lambda x: (x[0], list(x[1]))).collect()
[
 ('fox', [1, 1, 1, 1, 1, 1, 1, 1]), 
 ('and', [1, 1]), 
 ('jumped', [1, 1, 1, 1, 1]), 
 ('is', [1, 1, 1]), 
 ('crazy', [1, 1, 1]), 
 ('here', [1]), 
 ('there', [1])
]
>>>
>>> frequncy = grouped.mapValues(lambda x: sum(x))
>>> frequncy.collect()
[
 ('fox', 8), 
 ('and', 2), 
 ('jumped', 5), 
 ('is', 3), 
 ('crazy', 3), 
 ('here', 1), 
 ('there', 1)
]

>>> # map: 1-to-1 transformation
>>> # flatMap : 1-to-Many transformation
>>> # target_RDD = source_RDD.T(...)

