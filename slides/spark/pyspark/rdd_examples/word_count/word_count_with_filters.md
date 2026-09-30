# Word Count with Filters

	The word count problem is the “Hello World” 
	program of distributed data processing. Given 
	a massive collection of text documents, the 
	goal is to count how many times each unique 
	word appears across the entire dataset using 
	a distributed MapReduce framework.

# Input Data 

```
$ cat ./data/foxy.txt
a Fox jumped high and high and jumped and jumped
fox of red jumped
fox of blue jumped
a Fox is a red fox of hen 
a fox is a high fox
orange fox is high and blue and blue
```

# Desired Output:

```
<unique-word> <frequency>
```

# Requirements for PySpark Program

The following rules will be applied:

* if a word's length is smaller than 3, that word will not appear in output

* if a frequency of a unique-word is less than 3,  that word will not appear in output

* all words must be converted to lowercase (example: 'fox' is the same as 'Fox', etc.)

* all of your solution must be properly documented

* even though your input is very small, but your program must be as generic as possible to handle any size input

* efficiency is very important for writing this program and all big data programs (it means that your solution should be optimized to handle big volume data without performance penalties)

* you can not change the content of input file
the output does not need to be sorted


# PySpark Solution

```python
$SPARK_HOME/bin/pyspark
Python 3.7.2 
Spark version 2.4.4

SparkSession available as 'spark'.


# Define your input path
>>> input_path = './data/foxy.txt'

# Read input and create RDD[String]
>>> records = spark.sparkContext.textFile(input_path)
>>> records.collect()
[
 'a Fox jumped high and high and jumped and jumped', 
 'fox of red jumped', 
 'fox of blue jumped', 
 'a Fox is a red fox of hen', 
 'a fox is a high fox', 
 'orange fox is high and blue and blue'
]
>>> records.count();
6

# this function accepts a record 
# and create (word, 1) pairs for
# all valid pairs: we create a list
# data structure required by the flatMap()
# if a len(word) < 3, then it is dropped
>>> def create_key_value(rec):
...   valid_list = []
...   words = rec.strip().lower().split(" ")
...   for word in words:
...     if (len(word) > 2):
...       valid_list.append((word, 1))
...
...   return valid_list
...
# Test the function
>>> x = "a fox is here"
>>> valid_list = create_key_value(x)
>>> valid_list
[('fox', 1), ('here', 1)]

# run the flatMap() transformation on each element of rdd
# result is RDD[(String, Integer)]

>>> pairs = records.flatMap(create_key_value)
>>> pairs.collect()
[
 ('fox', 1), ('jumped', 1), ('high', 1), 
 ('and', 1), ('high', 1), ('and', 1), 
 ('jumped', 1), ('and', 1), ('jumped', 1), 
 ('fox', 1), ('red', 1), ('jumped', 1), 
 ('fox', 1), ('blue', 1), ('jumped', 1), 
 ('fox', 1), ('red', 1), ('fox', 1), ('hen', 1), 
 ('fox', 1), ('high', 1), ('fox', 1), 
 ('orange', 1), ('fox', 1), ('high', 1), 
 ('and', 1), ('blue', 1), ('and', 1), ('blue', 1)
]

# find frequencies per word 
# (sum up the counts per unique word)
>>> frequencies = pairs.reduceByKey(lambda a, b : a+b)
>>> frequencies.collect()
[
 ('high', 4), 
 ('hen', 1), 
 ('orange', 1), 
 ('fox', 8), 
 ('jumped', 5), 
 ('and', 5), 
 ('red', 2), 
 ('blue', 3)
]

# now, drop (k, v), if v < 3 
# [v refers to the frequency of a word)]
>>> filtered = frequencies.filter( lambda kv: kv[1] > 2)
>>> filtered.collect()
[
 ('high', 4), 
 ('fox', 8), 
 ('jumped', 5), 
 ('and', 5), 
 ('blue', 3)
]
>>>
```
