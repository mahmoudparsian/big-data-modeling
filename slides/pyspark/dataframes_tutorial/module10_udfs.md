# Module 10 – User-Defined Functions (UDFs) and Advanced Functions in PySpark

## Overview
PySpark provides thousands of built-in functions for:
- String manipulation
- Math
- Aggregations
- Date/time processing
- Window analytics

However, sometimes custom logic is required.  
For that, we use **User-Defined Functions (UDFs)**.

---

## When to Use UDFs (and When NOT to)

### Use UDFs if:
- Logic cannot be expressed with built-ins
- Needs complex business rules
- Requires external libraries

### Avoid UDFs when:
- Built-ins cover your need
- Performance is critical

💡 UDFs break Catalyst optimization and can be much slower.

---

## Registering UDFs (Python API)

### Python UDF
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def double(x):
    return x * 2

double_udf = udf(double, IntegerType())

df.withColumn("age2", double_udf("age")).show()
```

---

## SQL UDFs

Register:
```python
spark.udf.register("double", double, IntegerType())
```

Use in SQL:
```python
spark.sql("SELECT name, double(age) FROM employees").show()
```

---

## lambda UDF

```python
from pyspark.sql.types import StringType

upper_udf = udf(lambda x: x.upper(), StringType())
df.withColumn("name_upper", upper_udf("name")).show()
```

---

## pandas UDFs (Vectorized UDFs)

Faster and more scalable.

```python
from pyspark.sql.functions import pandas_udf
@pandas_udf("double")
def add_one(s):
    return s + 1

df.withColumn("age1", add_one("age")).show()
```

---

## Built-in Functions (Preferred Approach)

### String functions
```python
from pyspark.sql.functions import upper, lower, trim, length

df.select(
  upper("name").alias("name_upper"),
  length("name").alias("name_len")
).show()
```

---

### Math functions
```python
from pyspark.sql.functions import abs, round

df.select(round("salary",2), abs("bonus")).show()
```

---

### Date functions
```python
from pyspark.sql.functions import year, month, dayofmonth

df.select(
  year("date_joined").alias("year"),
  month("date_joined").alias("month")
).show()
```

---

## Conditional Logic

### when() / otherwise()

```python
from pyspark.sql.functions import when, col

df.withColumn(
  "high_earner",
  when(col("salary") > 100000, "Y").otherwise("N")
)
```

---

## CASE WHEN in SQL

```python
spark.sql(
  "SELECT name, CASE WHEN age>18 THEN 'adult' ELSE 'minor' END FROM employees"
).show()
```

---

## Complex Logic with expr()

```python
from pyspark.sql.functions import expr

df.select(
  "name",
  expr("salary * 0.1 as bonus"),
  expr("IF(salary>100000, 'Y', 'N') as high_earner")
).show()
```

---

## JSON Processing

```python
from pyspark.sql.functions import get_json_object

df.select(get_json_object("json_col","$.user.name"))
```

---

## Array Functions

```python
from pyspark.sql.functions import size, explode

df.select(size("skills")).show()
df.select(explode("skills")).show()
```

---

## Map Functions

```python
from pyspark.sql.functions import map_keys, map_values

df.select(map_keys("metadata")).show()
```

---

## Performance Notes

### UDFs
- Slow
- No pushdown
- Limited optimization

### pandas UDFs
- Vectorized, faster

### Built-ins
- Fastest
- Catalyst-optimized
- Native code generation

💡 Always prefer built-ins when possible.

---

## Teaching Notes
- Show performance comparisons
- Have students rewrite UDF logic using built-ins
- Introduce pandas UDF for advanced use

---

## Exercises
1. Create a UDF to reverse names  
2. Create a SQL UDF to categorize salary levels  
3. Use built-ins to compute string lengths  
4. Use when() to label minor/adult  
5. Use expr() to compute a bonus column  
6. Explode an array column into rows  
7. Use pandas UDF to add 1 to numeric column  
8. Compare performance of UDF vs built-ins  

---

## Summary
- UDFs provide custom logic
- pandas UDFs are faster than regular UDFs
- Built-in functions are preferred
- Use when(), expr() for conditional logic
- Avoid UDFs when possible
