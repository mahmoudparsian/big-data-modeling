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
