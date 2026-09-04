# `spark-submit` Example

A second, illustrative example of submitting a PySpark program with
`$SPARK_HOME/bin/spark-submit`, using a different sample input file
than the word-count example in this folder. The program shown below
is inline only (there is no standalone `.py` file for it in this
folder) — copy it out to try it, and point it at a text file of your
own rather than `/tmp/books.txt`, which is just illustrative.

## Input

```
$ cat /tmp/books.txt
ISBN-100,sales,biology
IS-01235,sales,econ
ISBN-101,sales,econ
ISBN-102,sales,biology
ISBN-109,econ,sales
ISBN-103,CS,sales
ISBN-104,CS,biology
ISBN-105,CS,econ
ISBN-200,CS
```
## PySpark Program

```python
from pyspark.sql import SparkSession

import sys

# A SparkSession can be used create DataFrame, 
# register DataFrame as tables, execute SQL 
# over tables, cache tables, and read parquet files. 
# To create a SparkSession, use the following 
# builder pattern:

spark = SparkSession.builder\
   .appName("spark-submit-example") \
   .getOrCreate()

input_path = sys.argv[1]
print("input_path=", input_path)

records = spark.sparkContext.textFile(input_path)

print("records.count()=", records.count())

print("records.collect()=", records.collect())

spark.stop()
```

## Submitting a PySpark Program

```
$SPARK_HOME/bin/spark-submit  my_program.py  /tmp/books.txt
input_path= /tmp/books.txt
records.count()= 9
records.collect()= 
[
 'ISBN-100,sales,biology', 
 'IS-01235,sales,econ', 
 'ISBN-101,sales,econ', 
 'ISBN-102,sales,biology', 
 'ISBN-109,econ,sales', 
 'ISBN-103,CS,sales', 
 'ISBN-104,CS,biology', 
 'ISBN-105,CS,econ', 
 'ISBN-200,CS'
]
```
