# Module 1 – Introduction to PySpark DataFrames

## What Are DataFrames?
A DataFrame in PySpark is:

- A distributed collection of rows
- Organized into named columns
- Typed, structured, and optimized
- Similar to Pandas DataFrames or SQL tables

## Why DataFrames?
- Declarative API
- Automatic optimization
- Faster than RDDs
- SQL-like interface

### Student takeaway
“If you’re writing SQL-like data transformations, use DataFrames, not RDDs.”

## SparkSession: The Entry Point
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("demo").getOrCreate()
```

## Creating DataFrames
```python
data = [(1,"Alice",30),(2,"Bob",25)]
df = spark.createDataFrame(data,["id","name","age"])
df.show()
```

## Schema
```python
df.printSchema()
```

## Loading Structured Data
```python
df = spark.read.option("header",True).csv("people.csv")
```

## Transformations
```python
df.filter(df.age>30)
df.groupBy("country").count()

>>> rows = [(1, 'alex', 100000, 'USA'), 
            (2, 'jane', 90000, 'USA'), 
            (3, 'ted', 85000, 'CHINA'), 
            (4, 'alex', 90000, 'CHINA')]

>>> column_names = ["id", "name", "salary", "country"]
>>> df = spark.createDataFrame(rows, column_names)
>>> df.show()
+---+----+------+-------+
| id|name|salary|country|
+---+----+------+-------+
|  1|alex|100000|    USA|
|  2|jane| 90000|    USA|
|  3| ted| 85000|  CHINA|
|  4|alex| 90000|  CHINA|
+---+----+------+-------+

>>> df2 = df.groupBy("country").count()
>>> df2.show()
+-------+-----+
|country|count|
+-------+-----+
|    USA|    2|
|  CHINA|    2|
+-------+-----+

```

## Actions
```python
df.show()
df.count()
```

## Lazy Evaluation
```python
df.filter(df.age > 30).count()
```

## explain()
```python
df.filter(df.age>30).explain()
```

## DataFrames vs RDDs
| Feature | DataFrame | RDD |
|---|---|---|
| Structure | Schema | None |

## Built-In Functions
```python
from pyspark.sql import functions as F
df.select(F.upper("name"))
```

## SQL Integration
```python
df.createOrReplaceTempView("people")
spark.sql("SELECT name FROM people").show()

>>> df.dtypes
[('id', 'bigint'), ('name', 'string'), ('salary', 'bigint'), ('country', 'string')]
>>> df.printSchema()
root
 |-- id: long (nullable = true)
 |-- name: string (nullable = true)
 |-- salary: long (nullable = true)
 |-- country: string (nullable = true)

>>> spark.sql("select * from people").show()
+---+----+------+-------+
| id|name|salary|country|
+---+----+------+-------+
|  1|alex|100000|    USA|
|  2|jane| 90000|    USA|
|  3| ted| 85000|  CHINA|
|  4|alex| 90000|  CHINA|
+---+----+------+-------+

>>> spark.sql("select country, avg(salary) as avg_salary, max(salary) as max_salry from people group by country").show()
+-------+----------+---------+
|country|avg_salary|max_salry|
+-------+----------+---------+
|    USA|   95000.0|   100000|
|  CHINA|   87500.0|    90000|
+-------+----------+---------+

>>
>>> spark.sql("select country, avg(salary) as avg_salary, max(salary) as max_salry from people group by country").show()
+-------+----------+---------+
|country|avg_salary|max_salry|
+-------+----------+---------+
|    USA|   95000.0|   100000|
|  CHINA|   87500.0|    90000|
+-------+----------+---------+

>>>
>>> # WHERE : filters raw rows before aggregation
>>> # HAVING: filters aggregated result

>>> spark.sql("""
... select country,
...        avg(salary) as avg_salary,
...        max(salary) as max_salry
... from people
... group by country
... HAVING avg_salary > 90000
... """).show()
+-------+----------+---------+
|country|avg_salary|max_salry|
+-------+----------+---------+
|    USA|   95000.0|   100000|
+-------+----------+---------+

>>>
```

## Common Mistakes
Avoid Python UDFs when possible

## Teaching Notes
- Start from Pandas
- Teach lazy evaluation

## Exercises
1. Create DataFrame
2. Print schema
3. Filter
4. Group

## Summary
- DataFrame = distributed table
- Lazy evaluation
- Optimized
