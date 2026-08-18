# Read Files and Create an RDD

## Input Data

~~~
$ pwd
/home/mparsian/DATA

$ ls -1 *
f1
f2

$ cat f1
hello world
fox jumped high
fox jumped

$ cat f2
fox is here and jumped
fox jumped
~~~

## Invoke Interactive PySpark

~~~
% pwd
/home/mparsian/spark-3.5.3

% ./bin/pyspark
Python 3.12.0 (v3.12.0:0fb18b02c8, Oct  2 2023, 09:45:56) 
[Clang 13.0.0 (clang-1300.0.29.30)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). 
For SparkR, use setLogLevel(newLevel).

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.3
      /_/

Using Python version 3.12.0 (v3.12.0:0fb18b02c8, Oct  2 2023 09:45:56)
Spark context Web UI available at http://172.20.201.137:4040
Spark context available as 'sc' (master = local[*], app id = local-1728615692154).
SparkSession available as 'spark'.
>>>
>>>
>>> sc
<SparkContext master=local[*] appName=PySparkShell>
>>>
>>> sc.version
'3.5.3'
>>>
>>>
>>> spark
<pyspark.sql.session.SparkSession object at 0x107b0c920>
>>>
>>> spark.version
'3.5.3'
>>>
>>>
>>> spark.sparkContext
<SparkContext master=local[*] appName=PySparkShell>
>>>
>>>
>>>
>>> input_path = "/home/mparsian/DATA/"
>>>
>>> # read all files in input_path
>>> # and create an rdd as RDD[String]
>>> rdd = sc.textFile(input_path)
>>>
>>> rdd.count()
5
>>> rdd.collect()
[
 'fox is here and jumped', 
 'fox jumped', 
 'hello world', 
 'fox jumped high', 
 'fox jumped'
]
>>> 
>>>
>>> # create an rdd2 as RDD[(String, Integer)]
>>> # map() is a 1 to 1 transformation
>>> rdd2 = rdd.map(lambda x: (x, len(x)))
>>>
>>> rdd2.collect()
[
 ('fox is here and jumped', 22), 
 ('fox jumped', 10), 
 ('hello world', 11), 
 ('fox jumped high', 15), 
 ('fox jumped', 10)
]
>>>
>>> # keep elements if their length is greater than 12
>>> # filter() is a transformation
>>> rdd_filtered = rdd.filter(lambda x: len(x) > 12)
>>>
>>> rdd_filtered.collect()
[
 'fox is here and jumped', 
 'fox jumped high'
]
>>>
~~~
