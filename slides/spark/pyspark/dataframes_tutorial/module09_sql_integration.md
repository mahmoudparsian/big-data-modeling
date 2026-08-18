# Module 9 – SQL Integration with PySpark DataFrames

## Overview
PySpark supports **seamless SQL integration**, enabling:

- Writing SQL queries against DataFrames
- Mixing DataFrame API + SQL
- Leveraging SQL skills in Spark
- Interoperability with BI tools

This is one of the **most powerful features** of PySpark.

---

## Creating Temporary Views

### createOrReplaceTempView()
```python
df.createOrReplaceTempView("employees")
```

### createGlobalTempView()
```python
df.createGlobalTempView("employees")
```

Global views persist across sessions.

---

## Running SQL Queries

```python
spark.sql(
    "SELECT name, age FROM employees WHERE age > 30"
).show()
```

---

## SQL + DataFrames: Equivalent Code

SQL:

```sql
SELECT country, COUNT(*) 
FROM employees 
GROUP BY country
```

DataFrame:

```python
df.groupBy("country").count()
```

---

## Aggregations in SQL

```python
spark.sql(
    "SELECT department, AVG(salary) AS avg_sal "
    "FROM employees GROUP BY department ORDER BY avg_sal DESC"
).show()
```

---

## Multi-Table SQL Joins

Assume views exist:

```python
emp.createOrReplaceTempView("emp")
dept.createOrReplaceTempView("dept")
```

```python
spark.sql(
    "SELECT e.name, e.dept, d.dept_name "
    "FROM emp e LEFT JOIN dept d ON e.dept = d.dept"
).show()
```

---

## Top-N with Window Functions (SQL)

```python
spark.sql(
    "SELECT name, dept, salary FROM ( "
    "  SELECT *, ROW_NUMBER() OVER(PARTITION BY dept ORDER BY salary DESC) AS rn "
    "  FROM employees "
    ") WHERE rn <= 2"
).show()
```

---

## SQL UDFs

Register a UDF:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

@udf(IntegerType())
def double(x):
    return x*2

spark.udf.register("double", double)
```

Use in SQL:
```python
spark.sql("SELECT name, double(age) FROM employees").show()
```

---

## Views vs Tables

Temporary view:
- Session-scoped
- Exists until Spark session ends

Global temp view:
- System-scoped
- Requires prefix: `global_temp`

```python
spark.sql("SELECT * FROM global_temp.employees")
```

---

## SQL with CTEs

```python
spark.sql(
    "WITH avg_sal AS ( "
    "  SELECT department, AVG(salary) AS avg_sal "
    "  FROM employees GROUP BY department "
    ") "
    "SELECT e.name, e.salary, a.avg_sal "
    "FROM employees e JOIN avg_sal a ON e.department = a.department "
    "WHERE e.salary > a.avg_sal"
).show()
```

---

## SQL + DataFrame Hybrid Workflow

1. Load data  
2. Register view  
3. Run SQL  
4. Convert back to DataFrame

```python
result = spark.sql("SELECT * FROM employees WHERE age > 30")
result.filter(result.salary > 80000).show()
```

---

## Performance Tips

- Register only needed DataFrames  
- Avoid wide SELECT *  
- Use broadcast in DataFrame API (not SQL)  
- Cache SQL result if reused  

```python
df = spark.sql("SELECT * FROM big_table WHERE year=2023")
df.cache()
df.count()
```

---

## Teaching Notes
- Students with SQL background progress faster  
- Encourage equivalency between APIs  
- Use SQL for readability, DF for performance  
- Show explain() on SQL queries  

---

## Exercises
1. Register employees as a view  
2. Query employees older than 30  
3. Compute avg salary by department using SQL  
4. Join employees and departments using SQL  
5. Find top 2 highest salaries per department  
6. Write a CTE that finds employees above avg salary  
7. Use SQL + UDF to transform a column  
8. Persist SQL result and filter further  

---

## Summary
- SQL integration is a first-class feature in PySpark  
- Register views and query with spark.sql()  
- SQL and DataFrame APIs are interchangeable  
- SQL handles analytics beautifully  
