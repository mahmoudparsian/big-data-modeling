# Module 6 – Grouping and Aggregations in PySpark

## Overview
**Grouping and aggregation are essential for:**

- Summarizing data
- Producing statistics
- Supporting analytics and reporting
- Feeding downstream ML pipelines

**PySpark provides a rich set of tools to:**

- Group data
- Apply built-in functions
- Compute multi-column aggregates
- Windowed aggregations (later)

---

## Basic GroupBy

```python
df.groupBy("country").count().show()

>>> rows = [(1, 'alex', 100000, 'USA'), 
            (2, 'jane', 90000, 'USA'), 
            (3, 'ted', 85000, 'CHINA'), 
            (4, 'alex', 90000, 'CHINA')]
>>> df = spark.createDataFrame(
    rows, ["id", "name", "salary", "country"])
>>> df.show()
+---+----+------+-------+
| id|name|salary|country|
+---+----+------+-------+
|  1|alex|100000|    USA|
|  2|jane| 90000|    USA|
|  3| ted| 85000|  CHINA|
|  4|alex| 90000|  CHINA|
+---+----+------+-------+

>>> df.groupBy("country").count().show()
+-------+-----+
|country|count|
+-------+-----+
|    USA|    2|
|  CHINA|    2|
+-------+-----+

```

Produces counts per group.

---

## Multiple Aggregations

```python
from pyspark.sql.functions import avg, max, min

df.groupBy("country").agg(
    avg("salary").alias("avg_salary"),
    max("salary").alias("max_salary"),
    min("salary").alias("min_salary")
).show()
+-------+----------+----------+----------+
|country|avg_salary|max_salary|min_salary|
+-------+----------+----------+----------+
|    USA|   95000.0|    100000|     90000|
|  CHINA|   87500.0|     90000|     85000|
+-------+----------+----------+----------+

```

---

## Aggregating Multiple Columns

```python
df.groupBy("gender", "country").agg({"salary":"avg"})
```

Example:

```
>>> df.groupBy("country").agg({"salary":"avg"}).show()
+-------+-----------+
|country|avg(salary)|
+-------+-----------+
|    USA|    95000.0|
|  CHINA|    87500.0|
+-------+-----------+
```

---

## Using Built-in Functions

Import functions:

```python
from pyspark.sql import functions as F
```

Example:

```python
df.groupBy("department").agg(
    F.avg("salary"),
    F.stddev("salary"),
    F.count("*")
)
```

Example:

```
>>> from pyspark.sql.functions import col, avg, stddev, count
>>> df.groupBy("country").agg(
...     avg("salary"),
...     stddev("salary"),
...     count("*")
... ).show()
+-------+-----------+------------------+--------+
|country|avg(salary)|    stddev(salary)|count(1)|
+-------+-----------+------------------+--------+
|    USA|    95000.0| 7071.067811865475|       2|
|  CHINA|    87500.0|3535.5339059327375|       2|
+-------+-----------+------------------+--------+
```

---

## Aliasing Results

```python
df.groupBy("department")
  .agg(F.avg("salary").alias("avg_sal"))
```

Example:

```
>>> df.groupBy("country")\
...     .agg(avg("salary").alias("avg_salary"))\
...     .show()
+-------+----------+
|country|avg_salary|
+-------+----------+
|    USA|   95000.0|
|  CHINA|   87500.0|
+-------+----------+
```
---

## Sorting by Aggregates

```python
df.groupBy("department")
  .agg(F.avg("salary").alias("avg_sal"))
  .orderBy("avg_sal", ascending=False)
  .show()
```

---

## Filter Groups Using Having

PySpark supports HAVING via filter/where after groupBy.

```python
(df.groupBy("department")
   .agg(F.avg("salary").alias("avg_sal"))
   .filter("avg_sal > 80000")
   .show())
```

or with column objects:

```python
(df.groupBy("department")
   .agg(F.avg("salary").alias("avg_sal"))
   .filter(F.col("avg_sal") > 80000)
   .show())
```

---

## Count Distinct Values

```python
df.groupBy("country").agg(F.countDistinct("name"))
```

---

## Pivot Tables

```python
df.groupBy("country").pivot("gender").agg(F.avg("salary")).show()
```

Example:

```
>>> df.groupBy("country")\
...     .pivot("id")\
...     .agg(avg("salary"))\
...     .show()
+-------+--------+-------+-------+-------+
|country|       1|      2|      3|      4|
+-------+--------+-------+-------+-------+
|  CHINA|    NULL|   NULL|85000.0|90000.0|
|    USA|100000.0|90000.0|   NULL|   NULL|
+-------+--------+-------+-------+-------+
```
---

## Cubes and Rollups

### Rollup
```python
df.rollup("country","gender").count().show()
```

Example:

```
>>> df.rollup("country").count().show()
+-------+-----+
|country|count|
+-------+-----+
|   NULL|    4|
|    USA|    2|
|  CHINA|    2|
+-------+-----+
```

### Cube
```python
df.cube("country","gender").count().show()
```

---

## Approximate Aggregation

Useful for large data:

```python
df.groupBy("department").agg(F.approx_count_distinct("id"))
```

---

## Practical Example

```python
(df.filter(F.col("age") > 25)
   .groupBy("country","gender")
   .agg(
       F.avg("salary").alias("avg_sal"),
       F.count("*").alias("count")
   )
   .orderBy("avg_sal", ascending=False)
   .show())
```

---

## Teaching Notes
- Emphasize aliasing for readability
- Teach HAVING with post-group filtering
- Demonstrate pivot tables on small datasets
- Explain distinct vs approximate distinct
- Show performance impact of wide aggregations

---

## Exercises
1. Count employees per country  
2. Compute average salary per department  
3. Find top 3 departments by avg salary  
4. Count distinct names per country  
5. Create a pivot of gender vs country  
6. Filter groups where avg salary > 80,000  
7. Compute count, avg, and max by gender  
8. Show groups sorted by count descending  

---

## Summary
- groupBy + agg drives summarization  
- Many built-ins: avg, min, max, stddev, count  
- HAVING uses filter on aggregated column  
- Pivot, cube, rollup enable advanced analytics  
- Aggregations feed ML and BI workloads  
