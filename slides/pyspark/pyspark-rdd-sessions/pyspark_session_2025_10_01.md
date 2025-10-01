# prepare some data files...

~~~
% ls -l /home/max/spark-4.0.0/zmp/data/
-rw-r--r--@ 1 max  staff  87 Apr 19  2023 file1
-rw-r--r--@ 1 max  staff  70 Oct  1 15:59 file2

% cat file1
file1: fox jumped
file1: fox jumped and jumped
file1: fox jumped and jumped and jumped

% cat file2
file2: hello alex
file2: hello jane
file2: fox is cute
file2: fox ran
~~~

# Start Interactive PySpark

~~~python

% ~/spark-4.0.0/bin/pyspark
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 4.0.0
      /_/

Using Python version 3.13.2 
(v3.13.2:4f8bb3947cf, Feb  4 2025 11:51:10)
Spark context Web UI available at http://192.168.0.56:4040
Spark context available as 'sc' 
(master = local[*], app id = local-1759359659763).
SparkSession available as 'spark'.
>>>
>>> sc.version
'4.0.0'
>>>
>>> spark.version
'4.0.0'
>>>
>>> sc
<SparkContext master=local[*] appName=PySparkShell>
>>>
>>>
>>> spark
<pyspark.sql.session.SparkSession object at 0x10bde2ba0>
>>>
>>> input_path = '/home/max/spark-4.0.0/zmp/data'
>>>
>>> rdd = sc.textFile(input_path)
>>>
>>> rdd.count()
7
>>>
>>>
>>> rdd.collect()
[
 'file2: hello alex', 
 'file2: hello jane', 
 'file2: fox is cute', 
 'file2: fox ran', 
 'file1: fox jumped', 
 'file1: fox jumped and jumped', 
 'file1: fox jumped and jumped and jumped'
]
>>>
>>> rdd2 = rdd.map(lambda x: (x, len(x)))
>>>
>>> rdd2.count()
7
>>>
>>> rdd2.collect()
[
 ('file2: hello alex', 17), 
 ('file2: hello jane', 17), 
 ('file2: fox is cute', 18), 
 ('file2: fox ran', 14), 
 ('file1: fox jumped', 17), 
 ('file1: fox jumped and jumped', 28), 
 ('file1: fox jumped and jumped and jumped', 39)
]
>>>
>>> filtered = rdd.filter(lambda x: len(x) > 20)
>>> filtered.count()
2
>>> filtered.collect()
[
 'file1: fox jumped and jumped', 
 'file1: fox jumped and jumped and jumped'
]
>>>
~~~