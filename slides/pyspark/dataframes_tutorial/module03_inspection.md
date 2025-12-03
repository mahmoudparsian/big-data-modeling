# Module 3 – Inspecting & Exploring DataFrames in PySpark

## Overview
Before transforming or analyzing a DataFrame, it is essential to:

- Inspect its structure
- Assess data types
- Explore sample records
- Perform basic profiling

---

## Display Rows

### show()
```python
df.show()
df.show(20, truncate=False)
```

### head()
```python
df.head(5)
```

### take()
```python
df.take(5)
```

---

## Viewing Schema

### printSchema()
```python
df.printSchema()
```

### schema
```python
df.schema
```

---

## Descriptive Statistics

### describe()
```python
df.describe().show()
```

### summary()
```python
df.summary().show()
```

---

## Counting Rows

```python
df.count()
```

---

## Column-Level Exploration

### Select specific columns
```python
df.select("name", "age").show()
```

### Distinct values
```python
df.select("country").distinct().show()
```

### Value counts
```python
df.groupBy("country").count().show()
```

---

## Null / Missing Value Analysis

### Count nulls per column
```python
from pyspark.sql.functions import col, sum

df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()
```

---

## Data Profiling Example

```python
(df.groupBy("gender")
   .agg({"age": "avg", "salary": "max", "*": "count"})
   .show())
```

---

## Filtering Examples

```python
df.filter(df.age > 30).show()

df.filter((df.age > 25) & (df.country == "USA")).show()
```

---

## Sorting

```python
df.orderBy("age", ascending=False).show()
```

---

## Checking Data Types

```python
df.dtypes
```

Example output:
```
[('name', 'string'),('age', 'int'),...]
```

---

## Using explain()

### Logical & physical plan
```python
df.groupBy("country").count().explain()
```

---

## Caching Data

For repeated use:
```python
df.cache()
df.count()
```

---

## Teaching Notes
- Always inspect data BEFORE modeling
- `summary()` provides percentiles
- Show students how nulls affect metrics
- Encourage using `explain()`

---

## Exercises
1. Print schema  
2. Count rows  
3. Show first 10 records without truncation  
4. Count distinct countries  
5. Find null counts per column  
6. Show summary statistics  
7. Sort by salary descending  
8. Show top 5 oldest employees  

---

## Summary
- Use show/head/take for samples  
- printSchema & dtypes to inspect metadata  
- describe/summary for statistics  
- Filter, sort, and group for profiling  
- explain() reveals execution plan  
