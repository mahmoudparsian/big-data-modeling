# PySpark Demo Session

* Date: April 19, 2023
* Author: Mahmoud Parsian


# Data Files 

## One file: `/tmp/foxdata.txt`
~~~sh
% cat /tmp/foxdata.txt
a red fox jumped of high
fox jumped over a high fence
red of fox jumped

% wc -l /tmp/foxdata.txt
       3 /tmp/foxdata.txt
~~~

## Multiple files: `/tmp/data/`

~~~sh
# directroy /tmp/data/ has two files: file1 and file2

% cat /tmp/data/file1
file1: fox jumped
file1: fox jumped and jumped
file1: fox jumped and jumped and jumped

% cat /tmp/data/file2
file2: hello alex
file2: hello jane
~~~

# Invoke PySpark Interactively

~~~sh
% export SPARK_HOME=/Users/mparsian/spark-3.3.2
% echo $SPARK_HOME
/Users/mparsian/spark-3.3.2

# Invoke PySpark Interactively
% $SPARK_HOME/bin/pyspark
Python 3.10.5 (v3.10.5:f377153967, Jun  6 2022, 12:36:10) 
Type "help", "copyright", "credits" or "license" for more information.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.3.2
      /_/

Using Python version 3.10.5 (v3.10.5:f377153967, Jun  6 2022 12:36:10)
Spark context Web UI available at http://c02fj5kymd6n.hsd1.ca.comcast.net:4040
Spark context available as 'sc' (master = local[*], app id = local-1681952132459).
SparkSession available as 'spark'.
>>>
>>> sc.version
'3.3.2'
>>> sc
<SparkContext master=local[*] appName=PySparkShell>
~~~

# Perform some Transformations and Actions

~~~py
>>>
>>> input_path = "/tmp/foxdata.txt"
>>> input_path
'/tmp/foxdata.txt'
>>>
>>># Create an RDD from a text file
>>> rdd = sc.textFile(input_path)
>>> rdd.collect()
[
'a red fox jumped of high', 
'fox jumped over a high fence', 
'red of fox jumped'
]
>>> rdd.count()
3
>>> rdd
/tmp/foxdata.txt MapPartitionsRDD[1] at textFile at NativeMethodAccessorImpl.java:0
>>>
>>>
>>> dir_path = "/tmp/data/"
>>> dir_path
'/tmp/data/'
>>>
>>># Create an RDD from a DIR (all files in the directory)
>>># all_records: RDD[String]
>>> all_records = sc.textFile(dir_path)
>>> 
>>># collect() is an action (returns the whole RDD elements as a list)
>>> all_records.collect()
[
'file2: hello alex', 
'file2: hello jane', 
'file1: fox jumped', 
'file1: fox jumped and jumped', 
'file1: fox jumped and jumped and jumped'
]
>>> all_records.count()
5
>>># Create a Python collection of (key, value) pairs
>>> my_list = [("alex", 20), ("jane", 30), ("jane", 40), ("alex", 50), ("jane", 12)]
>>> my_list
[
('alex', 20), 
('jane', 30), 
('jane', 40), 
('alex', 50), 
('jane', 12)
]
>>>
>>># Create an RDD from a Python collection
>>># key_values: RDD[(String, Integer)]
>>> key_values = sc.parallelize(my_list)
>>> key_values.collect()
[
('alex', 20), 
('jane', 30), 
('jane', 40), 
('alex', 50), 
('jane', 12)
]
>>> key_values.count()
5
>>># reduced: RDD[(String, Integer)]
>>># reduceByKey: key_values --> reduced
>>># reduceByKey can only be applied to an RDD[(key, value)]
>>> reduced = key_values.reduceByKey(lambda x, y: x+y)
>>> reduced.collect()
[
('jane', 82), 
('alex', 70)
]
>>> reduced.count()
2
>>> numbers = [1, 3, 4, 5, 6, 7]
>>># Create an RDD[Integer] from a Python Collection
>>> ints = sc.parallelize(numbers)
>>> ints.collect()
[1, 3, 4, 5, 6, 7]
>>> ints.count()
6
>>> ints
ParallelCollectionRDD[14] at readRDDFromFile at PythonRDD.scala:274
>>># Apply an action to ints RDD
>>> sum_of_all = ints.reduce(lambda x, y: x+y)
>>> sum_of_all
26
>>> # Transformation: source_RDD --> target_RDD
>>> # action: source_RDD --> NON-RDD
>>>
~~~