# Module 13 – Caching, Persistence, and Storage Levels in PySpark

## Overview
Caching and persistence are critical performance tools in PySpark for:
- Avoiding repeated computation
- Improving iterative workloads
- Accelerating machine learning pipelines
- Speeding up interactive analysis

This module explains:
- When and why to cache
- Storage levels
- How Spark decides caching behavior
- Performance trade‑offs

---

## When to Cache a DataFrame

### Cache when:
- The DataFrame is reused multiple times
- Multiple actions are called on the same data
- Performing iterative algorithms
- Interactive SQL/analysis workloads

### Do NOT cache if:
- Data is used only once
- Dataset is extremely large
- Pipeline is linear (no reuse)
- There is insufficient memory

💡 Caching blindly can **hurt** performance.

---

## Cache vs Persist

### Cache
Short‑hand for:
```python
df.persist(StorageLevel.MEMORY_AND_DISK)
```

### Persist
Explicit storage policy:
```python
from pyspark.storagelevel import StorageLevel
df.persist(StorageLevel.MEMORY_ONLY)
```

---

## Storage Levels

| StorageLevel | Memory | Disk | Replication | Notes |
|--------------|--------|------|-------------|------|
| MEMORY_ONLY | ✓ | ✗ | ✗ | Fastest, may evict |
| MEMORY_ONLY_SER | ✓ | ✗ | ✗ | Serialized, smaller |
| MEMORY_AND_DISK | ✓ | ✓ | ✗ | Default, safe |
| MEMORY_AND_DISK_SER | ✓ | ✓ | ✗ | Lower memory |
| DISK_ONLY | ✗ | ✓ | ✗ | Slowest |
| OFF_HEAP | ✓ | ✗ | ✗ | Requires config |
| MEMORY_ONLY_2 | ✓ | ✗ | ✓ | Replicated |

---

## Enabling Cache

```python
df.cache()
df.count()
```

`.count()` forces evaluation.

---

## Persisting with Storage Level

```python
from pyspark.storagelevel import StorageLevel

df.persist(StorageLevel.MEMORY_ONLY)
df.count()
```

---

## Unpersisting Data

Free cached data:
```python
df.unpersist()
```

Destroy all cache:
```python
spark.catalog.clearCache()
```

Useful for memory pressure.

---

## Inspecting Cached Data

```python
spark.catalog.isCached("table")
```

Show cached tables:
```python
spark.catalog.listTables()
```

---

## Whole‑Stage Code Generation

Caching helps codegen by:
- Preventing re‑planning
- Reducing stages
- Improving scan locality

---

## Caching for Machine Learning

Important when:
- Training multiple models
- Doing hyperparameter search

Example:
```python
df.cache()
model1.fit(df)
model2.fit(df)
model3.fit(df)
```

---

## Caching SQL Views

```python
spark.sql("CACHE TABLE employees")
```

Uncache:
```python
spark.sql("UNCACHE TABLE employees")
```

---

## Performance Considerations

### Benefits
- Reduce recomputation
- Reduce shuffles
- Reduce scan costs

### Risks
- Memory pressure
- Evictions
- Spill to disk
- Extra overhead

---

## Best Practices

1. Cache only reused DataFrames  
2. Unpersist when done  
3. Prefer MEMORY_ONLY for small datasets  
4. Use MEMORY_AND_DISK for safer behavior  
5. Avoid caching streaming data  

---

## Teaching Notes

- Demonstrate cache vs no‑cache timing  
- Show Spark UI "Storage" tab  
- Teach when caching is harmful  
- Have students inspect memory usage  

---

## Exercises

1. Cache a DataFrame and measure performance  
2. Change storage levels and compare  
3. Train multiple ML models without/with cache  
4. Use cache on SQL views  
5. Simulate memory pressure  
6. Unpersist DataFrames and verify  
7. Cache only selective columns  
8. Use disk‑only storage and benchmark  

---

## Summary

- Caching accelerates repeated computation  
- Storage levels provide control over memory and resilience  
- Cache wisely — overuse can degrade performance  
- Unpersist to free memory when finished  
