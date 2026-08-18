# Module 4 – Column Operations in PySpark

## Overview
Column operations are at the core of DataFrame transformations.  
They are used to:

- Select columns
- Create derived columns
- Rename columns
- Drop columns
- Apply expressions and built-in functions

---

## Selecting Columns
```python
df.select("name", "age").show()
```

### Select with expressions
```python
from pyspark.sql.functions import col
df.select(col("age") + 10).show()
```

---

## Renaming Columns

### withColumnRenamed()
```python
df.withColumnRenamed("age", "age_years").show()
```

---

## Adding Derived Columns

### withColumn()
```python
from pyspark.sql.functions import col

df = df.withColumn("age2", col("age") * 2)
df.show()
```

### Conditional Columns
```python
from pyspark.sql.functions import when

df = df.withColumn(
    "is_adult",
    when(col("age") >= 18, "yes").otherwise("no")
)
```
Example:

```python
>>> from pyspark.sql.functions import when
>>> from pyspark.sql.functions import col
>>> rows = [(1, 'alex'), (2, 'jane'), (3, 'ted')]
>>> df = spark.createDataFrame(rows, ["id", "name"])
>>> df.show()
+---+----+
| id|name|
+---+----+
|  1|alex|
|  2|jane|
|  3| ted|
+---+----+

>>> df = df.withColumn("id", when(col("id") <=1, "yes").otherwise("no"))
>>> df.show()
+---+----+
| id|name|
+---+----+
|yes|alex|
| no|jane|
| no| ted|
+---+----+
```
---

## Dropping Columns

Drop single column:

```python

df.drop("age2").show()
```

Drop multiple columns:

```python
df.drop("age2", "is_adult").show()
```

---

## Column Expressions

### Arithmetic
```python
df.withColumn("bonus", col("salary") * 0.1)
```

### String functions
```python
from pyspark.sql.functions import upper, lower

df.select(upper("name"), lower("name")).show()
```

### Date functions
```python
from pyspark.sql.functions import year

df.withColumn("year_joined", year(col("date_joined")))
```

---

## Filtering Rows

### Simple filter
```python
df.filter(col("age") > 30).show()
```

### Multiple conditions
```python
df.filter((col("age") > 25) & (col("country") == "USA")).show()
```

---

## Sorting

### Ascending
```python
df.orderBy("age").show()
```

### Descending
```python
df.orderBy(col("age").desc()).show()
```

---

## Boolean Columns

### Creating boolean column
```python
df.withColumn("is_usa", col("country") == "USA")
```

### Filter using boolean column
```python
df.filter(col("is_usa") == True)
```

---

## Using expr()

```python
from pyspark.sql.functions import expr

df.select(expr("age * 2 as double_age")).show()
```

---

## Combining Multiple Functions
```python
from pyspark.sql.functions import col, lower, trim

df.withColumn("clean_name", trim(lower(col("name"))))
```

---

## Chaining Transformations
```python
(df.withColumn("age2", col("age") * 2)
   .filter(col("age2") > 60)
   .orderBy("age2")
   .show())
```

---

## Teaching Notes
- Use col() instead of string expressions where possible  
- Avoid Python UDFs unless necessary  
- Combine transformations to reduce execution cost  
- Encourage chaining to express logical workflows  

---

## Exercises
1. Create a new column age2 = age * 2  
2. Create a boolean column for USA residents  
3. Filter rows where age2 > 50  
4. Sort by salary descending  
5. Create a column first_initial = first letter of name  
6. Normalize names to lowercase  
7. Drop derived columns  
8. Count how many adults exist  

---

## Summary
- Column operations are central to DataFrame transformations  
- withColumn(), drop(), select(), rename() are core tools  
- Use built-in functions whenever possible  
- Avoid UDFs for performance  
