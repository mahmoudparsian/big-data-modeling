# Module 14 – Spark SQL & DataFrame Query Optimization

## Overview
Optimizing SQL and DataFrame queries in PySpark is essential for:
- Faster execution
- Lower cost
- Reduced resource usage
- Scaling to large data volumes

This module covers:
- Explain plans
- Predicate pushdown
- Projection pruning
- Join optimization
- Broadcast strategies
- Cost-based optimizer (CBO)
- SQL/DataFrame best practices

---

## Explain Plans

Inspect query plan:
```python
df.explain()
```

Look for:
- `Exchange` (shuffle)
- `Project`
- `Filter`
- `BroadcastHashJoin`
- `SortMergeJoin`

Spark SQL:
```sql
EXPLAIN SELECT * FROM employees WHERE age > 30;
```

---

## Predicate Pushdown

Filters placed close to data source:
```python
df = spark.read.parquet("data").filter("age > 30")
```

Works best with:
- Parquet
- ORC
- Delta

Does **not** optimize:
- CSV
- JSON

---

## Projection Pruning

Drop columns early:
```python
df = df.select("id", "salary")
```

Reduces:
- Memory use
- Serialization cost
- Shuffle size

---

## Join Optimization

### Sort Merge Join (default)

Best when:
- Large datasets
- Join keys sorted

### Broadcast Hash Join (fast)

Force broadcast:
```python
from pyspark.sql.functions import broadcast

result = big.join(broadcast(small), "id")
```

Huge speed-up when small table < 10MB

---

## Adaptive Query Execution (AQE)

Enabled by default in new Spark:

Benefits:
- Dynamic join selection
- Skew mitigation
- Automatic optimization

Enable explicitly:
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

---

## Cost-Based Optimizer (CBO)

Collect statistics:
```python
spark.sql("ANALYZE TABLE employees COMPUTE STATISTICS")
```

Helps Spark choose:
- Join order
- Join type
- Broadcast strategy

---

## Common Anti-Patterns

### Bad pattern
```python
df.filter(df.age > 30)
df.filter(df.salary > 50000)
```

### Better
```python
df.filter((df.age > 30) & (df.salary > 50000))
```

Reduces scans + shuffles

---

## Sorting and Ordering

Avoid expensive sort:
```python
df.orderBy("age")
```

Alternative:
- `sample()`
- `approxQuantile()`

---

## Window Function Optimization

Avoid wide frames:
```python
rowsBetween(-sys.maxsize, sys.maxsize)
```

Use bounded frames:
```python
rowsBetween(-30, 0)
```

---

## Query Optimization Checklist

### Before join
- Filter
- Select only necessary columns
- Deduplicate keys

### Join
- Broadcast if possible
- Avoid cartesian joins

### After join
- Drop intermediate columns

### Overall
- Cache only reused data
- Avoid UDFs
- Prefer built-in functions

---

## SQL Best Practices

### Good
```sql
SELECT dept_id, AVG(salary)
FROM employees
GROUP BY dept_id;
```

### Avoid
```sql
SELECT *
FROM employees;
```

---

## Teaching Notes

- Have students compare performance of naive vs optimized queries
- Show aesthetic but high-impact optimizations (column pruning)
- Explain AQE visually via Spark UI

---

## Exercises

1. Force a broadcast join  
2. Compare join types with explain  
3. Use predicate pushdown with Parquet  
4. Measure effect of projection pruning  
5. Enable adaptive query execution  
6. Run CBO stats collection  
7. Optimize window function  
8. Identify anti-patterns  

---

## Summary

- Optimization = fewer shuffles, less data movement  
- Push filters down, prune columns early  
- Broadcast small tables  
- AQE + CBO make Spark smarter  
