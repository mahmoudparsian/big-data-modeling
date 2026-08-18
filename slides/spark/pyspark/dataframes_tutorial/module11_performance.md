# Module 11 – Performance Optimization in PySpark (Catalyst, Tungsten, and Best Practices)

## Overview
Performance optimization in PySpark revolves around:
- Understanding Spark's architecture
- Minimizing data movement
- Choosing optimal APIs
- Leveraging built-in optimizations

Topics covered:
- Catalyst optimizer
- Tungsten execution
- Partitions & shuffles
- Caching/persistence
- Broadcast joins
- Data serialization
- File formats & compression

---

## Catalyst Optimizer

Catalyst is Spark’s **query optimizer**.

It applies:
- Logical plan optimization
- Predicate pushdown
- Projection pruning
- Constant folding
- Join reordering

Example:

```python
df.filter(df.age > 30).select("name")
.explain()
```

Catalyst will prune unneeded columns.

---

## Tungsten Execution Engine

Tungsten focuses on:
- CPU efficiency
- Memory management
- Code generation

Benefits:
- Vectorized execution
- Less GC overhead
- Faster operations

---

## Wide vs Narrow Transformations

### Narrow
- map
- filter
- withColumn

Data stays local.

### Wide (cause shuffle)
- join
- groupBy
- orderBy

Data must move across cluster.

❗ Shuffles are expensive

---

## Minimizing Shuffles

### Strategy
- Repartition wisely
- Filter early
- Drop unnecessary columns
- Broadcast small tables
- Avoid wide operations when possible

Example:

```python
df = df.select("id","dept","salary")
```

---

## Partitioning

### Inspect partitions

```python
df.rdd.getNumPartitions()
```

### Repartition (shuffle)
```python
df.repartition(20)
```

### Coalesce (no shuffle)
```python
df.coalesce(4)
```

---

## Caching & Persistence

Cache DataFrame:
```python
df.cache()
df.count()
```

Persist with storage level:
```python
from pyspark.storagelevel import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)
```

When to cache:
- Reused DataFrames
- Repeated actions
- Model training loops

When NOT to cache:
- One-off transformations

---

## Broadcast Joins

Ideal when one side is small:

```python
from pyspark.sql.functions import broadcast

df = big.join(broadcast(small), "id")
```

Benefits:
- Avoids shuffle
- Huge speed-up

---

## Skewed Data

Symptoms:
- Long tail tasks
- Outliers in stage time

Fixes:
- Salting keys
- Repartitioning by key

Example:

```python
df.withColumn("skewed_key", concat(col("key"), rand()))
```

---

## Column Pruning

Select only required columns:

```python
df = df.select("id","salary")
```

Results in:
- Less serialization
- Faster operations
- Lower memory use

---

## Predicate Pushdown

Push filters to data source:

```python
spark.read.parquet("data").filter("age > 30")
```

---

## Choosing File Formats

Best options:
- Parquet
- ORC

Avoid:
- CSV
- JSON (for large data)

Benefits:
- Columnar storage
- Compression
- Predicate pushdown

---

## Compression

Recommended:
- Snappy
- Zstd

Avoid:
- Gzip (slow)

---

## Explain Plans

Always inspect plan:

```python
df.explain()
```

Look for:
- Exchange nodes (shuffle)
- BroadcastHashJoin vs SortMergeJoin
- Pruned columns

---

## Best Practices Checklist

### Code
- Avoid UDFs
- Use built-ins
- Filter early
- Drop unused columns

### Joins
- Broadcast small tables
- Avoid cartesian joins

### Data
- Use Parquet
- Repartition wisely

### Execution
- Cache reused DataFrames
- Avoid multiple actions on uncached data

---

## Teaching Notes
- Demo performance before/after optimization
- Show Spark UI DAG
- Explain lineage and fault tolerance
- Students should practice tuning small datasets

---

## Exercises
1. Inspect partition count  
2. Repartition a DataFrame  
3. Apply broadcast join  
4. Cache + measure performance  
5. Write DataFrame to Parquet and read back  
6. Identify shuffles in explain() output  
7. Fix skewed data  
8. Minimize columns before join  

---

## Summary
- Spark is fast, but optimization matters  
- Catalyst & Tungsten provide deep optimizations  
- Avoid shuffles, use broadcast, filter early  
- Choose Parquet, avoid CSV for large data  
- Cache wisely, not blindly  
