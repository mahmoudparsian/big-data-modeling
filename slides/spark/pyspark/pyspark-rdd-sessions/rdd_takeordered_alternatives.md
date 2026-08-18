# Alternatives to `takeOrdered()` in PySpark

## 1) **RDD.top(n)**
- Similar to `takeOrdered(n)` but it always returns the **largest N elements**.
- You can control sorting with a **key function**.
- Main difference:  
  - `top(n)` → gets **highest values** (descending by default).  
  - `takeOrdered(n)` → gets **lowest values** (ascending by default, unless you flip with `-key`).  

### Example
```python
data = [10, 30, 20, 50, 40]
rdd = sc.parallelize(data)

print("Top 3:", rdd.top(3))  
# Output: [50, 40, 30]
```

---

## 2) **RDD.sortBy() + take(n)**
- You can explicitly sort by a key, then take the first few rows.
- Useful if you want **full control** over sorting logic.

### Example with `(name, salary)`
```python
# Smallest 3 salaries
smallest = rdd.sortBy(lambda x: x[1]).take(3)
print("Lowest 3 salaries:", smallest)

# Highest 3 salaries
highest = rdd.sortBy(lambda x: x[1], ascending=False).take(3)
print("Top 3 salaries:", highest)
```

---

## 3) **DataFrame API with orderBy() + limit()**
If you’ve moved to **DataFrames** (often easier for SQL-style queries), you can use `orderBy` + `limit`.

### Example
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([
    ("Alice", 95000),
    ("Bob", 120000),
    ("Carol", 70000)
], ["name", "salary"])

# Lowest 2 salaries
df.orderBy("salary").limit(2).show()

# Highest 2 salaries
df.orderBy(df.salary.desc()).limit(2).show()
```

---

## 🔑 Summary
- **`takeOrdered(n)`** → Best for *smallest N elements* (ascending).  
- **`top(n)`** → Best for *largest N elements* (descending).  
- **`sortBy() + take(n)`** → Flexible if you need explicit ascending/descending.  
- **DataFrame `orderBy() + limit()`** → Recommended for SQL/DataFrame workflows.
