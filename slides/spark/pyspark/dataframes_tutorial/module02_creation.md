# Module 2 – Creating DataFrames in PySpark

## Overview
DataFrames can be created from:

1. Python objects (lists, tuples, dicts)
2. Row objects
3. Files (CSV, JSON, Parquet)
4. RDDs
5. External systems (databases, cloud)

---

## Creating from Python Objects
```python
data = [(1,"Alice",30),(2,"Bob",25)]
df = spark.createDataFrame(data, ["id","name","age"])
df.show()

>>>
>>>
>>> data = [(1,"Alice",30),(2,"Bob",25)]
>>> df = spark.createDataFrame(data, ["id","name","age"])
>>> df.show()
+---+-----+---+
| id| name|age|
+---+-----+---+
|  1|Alice| 30|
|  2|  Bob| 25|
+---+-----+---+


>>>
```

---

## Creating from Dictionaries
```python
data = [{"id":1,"name":"Alice","age":30},
        {"id":2,"name":"Bob","age":25}]
df = spark.createDataFrame(data)
df.show()

>>> data = [{"id":1,"name":"Alice","age":30},
...         {"id":2,"name":"Bob","age":25}]
>>> df = spark.createDataFrame(data)
>>> df.show()
+---+---+-----+
|age| id| name|
+---+---+-----+
| 30|  1|Alice|
| 25|  2|  Bob|
+---+---+-----+

```

---

## Creating from Row Objects
```python
from pyspark.sql import Row

rows = [Row(id=1,name="Alice",age=30),
        Row(id=2,name="Bob",age=25)]
df = spark.createDataFrame(rows)
df.show()

+---+-----+---+
| id| name|age|
+---+-----+---+
|  1|Alice| 30|
|  2|  Bob| 25|
+---+-----+---+

```

---

## Creating with Schema
```python
from pyspark.sql.types import *

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)
df.printSchema()
```

---

## Creating from CSV
```python
df = spark.read.option("header", True).csv("people.csv")
df.show()
```

---

## Creating from JSON
```python
df = spark.read.json("data.json")
df.printSchema()
```

---

## Creating from Parquet
```python
df = spark.read.parquet("data.parquet")
df.show()
```

---

## Creating from an RDD
```python
rdd = spark.sparkContext.parallelize([(1,"Alice"),(2,"Bob")])
df = rdd.toDF(["id","name"])
df.show()

>>> rdd = spark.sparkContext.parallelize([(1,"Alice"),(2,"Bob")])
>>> df = rdd.toDF(["id","name"])
>>> df.show()
+---+-----+
| id| name|
+---+-----+
|  1|Alice|
|  2|  Bob|
+---+-----+
```

---

## Writing DataFrames to Files

### Write to CSV
```python
df.write.option("header", True).csv("/tmp/emps")

```

View output directory:

```
~  % cd /tmp/emps/
emps  % ls -l
total 24
-rw-r--r--@ 1 mparsian  wheel   0 Dec  2 18:43 _SUCCESS
-rw-r--r--@ 1 mparsian  wheel   8 Dec  2 18:43 part-00000-23a3e8e4-a7f5-4c09-99b2-31621467ddc5-c000.csv
-rw-r--r--@ 1 mparsian  wheel  16 Dec  2 18:43 part-00003-23a3e8e4-a7f5-4c09-99b2-31621467ddc5-c000.csv
-rw-r--r--@ 1 mparsian  wheel  14 Dec  2 18:43 part-00007-23a3e8e4-a7f5-4c09-99b2-31621467ddc5-c000.csv
emps  % cat part*
id,name
id,name
1,Alice
id,name
2,Bob
emps  %
emps  % cat part-00000-23a3e8e4-a7f5-4c09-99b2-31621467ddc5-c000.csv
id,name
emps  % cat part-00003-23a3e8e4-a7f5-4c09-99b2-31621467ddc5-c000.csv
id,name
1,Alice
emps  % cat part-00007-23a3e8e4-a7f5-4c09-99b2-31621467ddc5-c000.csv
id,name
2,Bob
```

### Write to Parquet
```python
df.write.parquet("out/people.parquet")
```

---

## Teaching Notes
- Use small sample files first
- Compare schema inference vs explicit schema
- Parquet is preferred for performance

---

## Exercises
1. Create a DataFrame manually with 5 rows  
2. Load data from CSV  
3. Specify schema explicitly  
4. Write DataFrame to Parquet  
5. Convert RDD to DataFrame

---

## Summary
- Many ways to build DataFrames
- Manual schema reduces errors
- Parquet is fast and optimized
- DataFrames can be persisted
