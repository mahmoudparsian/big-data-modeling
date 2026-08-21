# Interactive Session with PySpark: Word Count

## Input Data

~~~
% pwd
/home/mparsian/mp/data

% ls -l

-rw-r--r--@ 1 mparsian  staff  37 Sep 30 19:11 f1.txt
-rw-r--r--@ 1 mparsian  staff  70 Sep 30 19:11 f2.txt
-rw-r--r--@ 1 mparsian  staff  26 Oct  2 18:53 f3.txt

% cat f1.txt
1 fox jumped
2 fox ran
3 fox is here

% cat f2.txt
100 hello world
200 hello fox and fox
300 fox is cute
400 fox is here

% cat f3.txt
50 fox is cute
60 fox ran
~~~

## Interactive PySpark: Word Count

~~~
% /home/mparsian/spark/4.0.0/bin/pyspark
Python 3.11.2 (v3.11.2:878ead1ac1, Feb  7 2023, 10:02:41) 
[Clang 13.0.0 (clang-1300.0.29.30)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 4.0.0
      /_/

Using Python version 3.11.2 
(v3.11.2:878ead1ac1, Feb  7 2023 10:02:41)
Spark context Web UI available at http://localhost:4040
Spark context available as 'sc' 
(master = local[*], app id = local-1759454797458).
SparkSession available as 'spark'.
>>>
>>>#-------------------------------------
>>># Define your input path
>>>#-------------------------------------
>>> input_path = '/home/mparsian/mp/data'
>>>
>>> input_path
'/Users/mparsian/mp/data'
>>>
>>>
>>>
>>> sc
<SparkContext master=local[*] appName=PySparkShell>
>>>
>>>#--------------------------------------------------------
>>># Read all of your input files and create and RDD[String]
>>>#--------------------------------------------------------
>>> rdd = sc.textFile(input_path)
>>>
>>>#--------------------------------------------------------
>>># Count the number of elements in your rdd
>>>#--------------------------------------------------------
>>> rdd.count()
9

>>>#--------------------------------------------------------
>>># Get all of your rdd elements as a list
>>>#--------------------------------------------------------
>>> rdd.collect()
[
 '1 fox jumped', 
 '2 fox ran', 
 '3 fox is here', 
 '50 fox is cute', 
 '60 fox ran', 
 '100 hello world', 
 '200 hello fox and fox', 
 '300 fox is cute', 
 '400 fox is here'
]
>>>
>>>#--------------------------------------------------------
>>># map every element to a array/list of words
>>>#--------------------------------------------------------
>>> words = rdd.map(lambda x: x.split(" "))
>>>
>>>#--------------------------------------------------------
>>># Get all of your words elements as a list
>>>#--------------------------------------------------------
>>> words.collect()
[
 ['1', 'fox', 'jumped'], 
 ['2', 'fox', 'ran'], 
 ['3', 'fox', 'is', 'here'], 
 ['50', 'fox', 'is', 'cute'], 
 ['60', 'fox', 'ran'], 
 ['100', 'hello', 'world'], 
 ['200', 'hello', 'fox', 'and', 'fox'], 
 ['300', 'fox', 'is', 'cute'], 
 ['400', 'fox', 'is', 'here']
]
>>>
>>>#--------------------------------------------------------
>>># flatten each element into a set of new elements
>>>#--------------------------------------------------------
>>> flattened = words.flatMap(lambda x: x)
>>>
>>> words.count()
9
>>> flattened.count()
33
>>> flattened.collect()
[
 '1', 
 'fox', 
 'jumped', 
 '2', 
 'fox', 
 'ran', 
 '3', 
 'fox', 
 'is', 
 'here', 
 '50', 
 'fox', 
 'is', 
 'cute', 
 '60', 
 'fox', 
 'ran', 
 '100', 
 'hello', 
 'world', 
 '200', 
 'hello', 
 'fox', 
 'and', 
 'fox', 
 '300', 
 'fox', 
 'is', 
 'cute', 
 '400', 
 'fox', 
 'is', 
 'here'
]

>>>
>>>#--------------------------------------------------------
>>># map each word to (word, 1)
>>>#--------------------------------------------------------
>>> key_value = flattened.map(lambda x: (x, 1))
>>>
>>> key_value.collect()
[
 ('1', 1), 
 ('fox', 1), 
 ('jumped', 1), 
 ('2', 1), 
 ('fox', 1), 
 ('ran', 1), 
 ('3', 1), 
 ('fox', 1), 
 ('is', 1), 
 ('here', 1), 
 ('50', 1), 
 ('fox', 1), 
 ('is', 1), 
 ('cute', 1), 
 ('60', 1), 
 ('fox', 1), 
 ('ran', 1), 
 ('100', 1), 
 ('hello', 1), 
 ('world', 1), 
 ('200', 1), 
 ('hello', 1), 
 ('fox', 1), 
 ('and', 1), 
 ('fox', 1), 
 ('300', 1), 
 ('fox', 1), 
 ('is', 1), 
 ('cute', 1), 
 ('400', 1), 
 ('fox', 1), 
 ('is', 1), 
 ('here', 1)]
>>>
>>>
>>>#-----------------------------------------------------------------
>>># groupByKey(): Group the values for each key in the RDD into a single sequence. 
>>>#-----------------------------------------------------------------
>>> grouped = key_value.groupByKey()
>>>
>>>
>>> grouped.collect()
[
 ('fox', <pyspark.resultiterable.ResultIterable object at 0x1048101d0>), 
 ('ran', <pyspark.resultiterable.ResultIterable object at 0x104810290>), 
 ('cute', <pyspark.resultiterable.ResultIterable object at 0x1048103d0>), 
 ('60', <pyspark.resultiterable.ResultIterable object at 0x104808350>), 
 ('100', <pyspark.resultiterable.ResultIterable object at 0x104810510>), 
 ('world', <pyspark.resultiterable.ResultIterable object at 0x104810590>), 
 ('200', <pyspark.resultiterable.ResultIterable object at 0x104810610>), 
 ('and', <pyspark.resultiterable.ResultIterable object at 0x1048102d0>), 
 ('300', <pyspark.resultiterable.ResultIterable object at 0x104810750>), 
 ('jumped', <pyspark.resultiterable.ResultIterable object at 0x1048106d0>), 
 ('2', <pyspark.resultiterable.ResultIterable object at 0x1048107d0>), 
 ('3', <pyspark.resultiterable.ResultIterable object at 0x1048083d0>), 
 ('is', <pyspark.resultiterable.ResultIterable object at 0x104810690>), 
 ('50', <pyspark.resultiterable.ResultIterable object at 0x104810a10>), 
 ('hello', <pyspark.resultiterable.ResultIterable object at 0x104810a90>), 
 ('1', <pyspark.resultiterable.ResultIterable object at 0x104810950>), 
 ('here', <pyspark.resultiterable.ResultIterable object at 0x104810b10>), 
 ('400', <pyspark.resultiterable.ResultIterable object at 0x104810cd0>)
]
>>>
>>>#-----------------------------------------------------------------
>>>#debug elements of the grouped RDD:
>>>#-----------------------------------------------------------------
>>> grouped.mapValues(lambda iter: list(iter)).collect()
[
 ('fox', [1, 1, 1, 1, 1, 1, 1, 1, 1]), 
 ('ran', [1, 1]), 
 ('cute', [1, 1]), 
 ('60', [1]), 
 ('100', [1]), 
 ('world', [1]), 
 ('200', [1]), 
 ('and', [1]), 
 ('300', [1]), 
 ('jumped', [1]), 
 ('2', [1]), 
 ('3', [1]), 
 ('is', [1, 1, 1, 1]), 
 ('50', [1]), 
 ('hello', [1, 1]), 
 ('1', [1]), 
 ('here', [1, 1]), 
 ('400', [1])
]

>>>
>>>#-----------------------------------------------------------------
>>># find the final frequency of every unique word
>>>#-----------------------------------------------------------------
>>> frequency = grouped.mapValues(lambda x: sum(x))
>>>
>>> frequency.collect()
[
 ('fox', 9), 
 ('ran', 2), 
 ('cute', 2), 
 ('60', 1), 
 ('100', 1), 
 ('world', 1), 
 ('200', 1), 
 ('and', 1), 
 ('300', 1), 
 ('jumped', 1), 
 ('2', 1), 
 ('3', 1), 
 ('is', 4), 
 ('50', 1), 
 ('hello', 2), 
 ('1', 1), 
 ('here', 2), 
 ('400', 1)
]

~~~

