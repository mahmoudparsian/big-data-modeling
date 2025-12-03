# Module 7 – Joins in PySpark

## Overview
Joins combine rows from two DataFrames based on a condition.  

PySpark supports:

- inner
- left
- right
- full outer
- cross
- semi
- anti
- broadcast joins

---

## Sample DataFrames

```python
employees = spark.createDataFrame(
    [(1,"Alice", "IT"),
     (2,"Bob","HR"),
     (3,"Carol","IT"),
     (4,"Dan","Sales")],
    ["emp_id","name","dept"]
)

departments = spark.createDataFrame(
    [("IT","Engineering"),
     ("HR","Human Resources"),
     ("FIN","Finance")],
    ["dept","dept_name"]
)
```

---

## Inner Join

```python
employees.join(departments, "dept", "inner").show()
>>> employees.join(departments, "dept", "inner").show()
+----+------+-----+---------------+
|dept|emp_id| name|      dept_name|
+----+------+-----+---------------+
|  HR|     2|  Bob|Human Resources|
|  IT|     1|Alice|    Engineering|
|  IT|     3|Carol|    Engineering|
+----+------+-----+---------------+

```

Result:
- Only matching rows

---

## Left Join

```python
employees.join(departments, "dept", "left").show()
>>> employees.join(departments, "dept", "left").show()
+-----+------+-----+---------------+
| dept|emp_id| name|      dept_name|
+-----+------+-----+---------------+
|   IT|     1|Alice|    Engineering|
|   HR|     2|  Bob|Human Resources|
|   IT|     3|Carol|    Engineering|
|Sales|     4|  Dan|           NULL|
+-----+------+-----+---------------+
```

Result:
- All left rows, nulls for missing matches

---

## Right Join

```python
employees.join(departments, "dept", "right").show()
>>> employees.join(departments, "dept", "right").show()
+----+------+-----+---------------+
|dept|emp_id| name|      dept_name|
+----+------+-----+---------------+
|  IT|     3|Carol|    Engineering|
|  IT|     1|Alice|    Engineering|
|  HR|     2|  Bob|Human Resources|
| FIN|  NULL| NULL|        Finance|
+----+------+-----+---------------+

```

---

## Full Outer Join

```python
employees.join(departments, "dept", "outer").show()
+-----+------+-----+---------------+
| dept|emp_id| name|      dept_name|
+-----+------+-----+---------------+
|  FIN|  NULL| NULL|        Finance|
|   HR|     2|  Bob|Human Resources|
|   IT|     1|Alice|    Engineering|
|   IT|     3|Carol|    Engineering|
|Sales|     4|  Dan|           NULL|
+-----+------+-----+---------------+

```

---

## Cross Join

```python
employees.crossJoin(departments).show()
>>> employees.crossJoin(departments).show()
+------+-----+-----+----+---------------+
|emp_id| name| dept|dept|      dept_name|
+------+-----+-----+----+---------------+
|     1|Alice|   IT|  IT|    Engineering|
|     1|Alice|   IT|  HR|Human Resources|
|     1|Alice|   IT| FIN|        Finance|
|     2|  Bob|   HR|  IT|    Engineering|
|     2|  Bob|   HR|  HR|Human Resources|
|     2|  Bob|   HR| FIN|        Finance|
|     3|Carol|   IT|  IT|    Engineering|
|     3|Carol|   IT|  HR|Human Resources|
|     3|Carol|   IT| FIN|        Finance|
|     4|  Dan|Sales|  IT|    Engineering|
|     4|  Dan|Sales|  HR|Human Resources|
|     4|  Dan|Sales| FIN|        Finance|
+------+-----+-----+----+---------------+

```

---

## Join with Expressions

```python
emp.join(departments, emp.dept == departments.dept, "inner")
```

---

## Join on Multiple Columns

```python
df1.join(df2, ["id","date"], "inner")
```

---

## Semi Join

Returns left rows **where match exists**

```python
emp.join(departments, "dept", "left_semi").show()
```

---

## Anti Join

Returns left rows **where NO match exists**

```python
emp.join(departments, "dept", "left_anti").show()
```

---

## Broadcast Join

Useful for small DataFrames

```python
from pyspark.sql.functions import broadcast
emp.join(broadcast(departments), "dept", "inner")
```

---

## Avoiding Duplicate Columns

```python
df = emp.join(departments, emp.dept == departments.dept, "inner")         .select(emp.emp_id, emp.name, departments.dept_name)
```

---

## Join Performance Tips
- Broadcast small tables
- Filter before join
- Avoid cross join if possible
- Select required columns only
- Use explain() to inspect plan

---

## Teaching Notes
- Show join types visually
- Demonstrate left vs semi vs anti
- Show broadcast performance impact
- Practice with real messy data

---

## Exercises
1. Inner join employees and departments  
2. Show employees missing departments  
3. Show departments with no employees  
4. Broadcast join employees and departments  
5. Use semi join to filter employees  
6. Use anti join to find orphan employees  
7. Join on multiple keys  
8. Remove duplicate columns  

---

## Summary
- Joins combine rows across DataFrames  
- Many join types available in PySpark  
- Semi/anti enable advanced filtering  
- Broadcast join improves performance  
