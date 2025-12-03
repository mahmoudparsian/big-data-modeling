# Module 5 – Filtering, Sorting, and Logic in PySpark

## Overview
Filtering and sorting operations are essential for:

- Selecting subsets of data
- Cleaning noisy datasets
- Preparing input to aggregations or joins
- Improving performance via predicate pushdown

---

## Basic Filtering

### Simple comparison
```python
df.filter(df.age > 30).show()
```

### Equality
```python
df.filter(df.country == "USA").show()
```

### Inequality
```python
df.filter(df.salary != 50000).show()
```

---

## Compound Conditions

### AND
```python
df.filter((df.age > 30) & (df.country == "USA")).show()
```

### OR
```python
df.filter((df.country == "USA") | (df.country == "Canada")).show()
```

### NOT
```python
from pyspark.sql.functions import col
df.filter(~col("country").isin("USA","Canada")).show()
```

---

## Filtering Nulls

### Check for nulls
```python
df.filter(df.salary.isNull()).show()
```

### Exclude nulls
```python
df.filter(df.salary.isNotNull()).show()
```

Example:

```python
>>> rows = [(1, 'alex', 100000), (2, 'jane', 90000), 
            (3, 'ted', 85000), (4, 'alex', None)]
>>> df = spark.createDataFrame(rows, ["id", "name", "salary"])
>>> df.show()
+---+----+------+
| id|name|salary|
+---+----+------+
|  1|alex|100000|
|  2|jane| 90000|
|  3| ted| 85000|
|  4|alex|  NULL|
+---+----+------+

>>> df.filter(df.salary.isNull()).show()
+---+----+------+
| id|name|salary|
+---+----+------+
|  4|alex|  NULL|
+---+----+------+

>>> df.filter(df.salary.isNotNull()).show()
+---+----+------+
| id|name|salary|
+---+----+------+
|  1|alex|100000|
|  2|jane| 90000|
|  3| ted| 85000|
+---+----+------+
```
---

## Filtering with isin()

```python
df.filter(df.country.isin("USA","Canada","Mexico")).show()
```

---

## Using between()

```python
df.filter(df.age.between(25, 40)).show()
```

---

## Using like() and rlike()

### SQL LIKE
```python
df.filter(df.name.like("%a%")).show()
```

### Regex
```python
df.filter(df.name.rlike("^[A-Z].*")).show()
```

---

## Sorting

### Ascending
```python
df.orderBy("age").show()
```

### Descending
```python
from pyspark.sql.functions import col
df.orderBy(col("age").desc()).show()
```

### Multi-column sort
```python
df.orderBy(col("country"), col("salary").desc()).show()
```

---

## Limit & Sampling

```python
df.limit(10).show()
df.sample(0.1).show()
```

---

## Removing Duplicates

### distinct()
```python
df.select("country").distinct().show()
```

### dropDuplicates()
```python
df.dropDuplicates(["country", "gender"]).show()
```

---

## Predicate Pushdown

Spark pushes filtering to the **data source** when possible.

### Example
```python
df = spark.read.parquet("data/people.parquet")
df.filter(df.age > 30).count()
```

Benefits:
- Scans fewer rows
- Saves I/O
- Faster

---

## Practical Example

```python
(df.filter(col("age") > 25)
   .filter(col("country") == "USA")
   .orderBy(col("salary").desc())
   .select("name","country","age","salary")
   .show())
```

---

## Teaching Notes
- Encourage using parentheses for readability  
- Explain difference between **filter** and **where**  
- Teach students to combine filters when possible  
- Demonstrate performance differences on large datasets  

---

## Exercises
1. Filter employees with salary > 80,000  
2. Show female employees in USA or Canada  
3. Show employees age 30–50  
4. Sort employees by salary descending  
5. Remove duplicates by country  
6. Filter names containing 'a'  
7. Use regex to find names starting with vowels  
8. Randomly sample 10% of rows  

---

## Summary
- Filtering and sorting extract meaningful subsets  
- Compound logic handles complex conditions  
- Built-ins help with strings, regex, ranges  
- Predicate pushdown improves performance  
