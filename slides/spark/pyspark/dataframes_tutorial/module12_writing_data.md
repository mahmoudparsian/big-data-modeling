# Module 12 – File Formats, Storage, and Data I/O in PySpark

## Overview
PySpark supports a variety of file formats for reading and writing data.  
Choosing the right format affects:
- Performance
- Storage footprint
- Query speed
- Compatibility with tools

This module covers:
- Supported formats
- Read/write APIs
- Columnar formats (Parquet, ORC)
- Compression
- Best practices

---

## Supported File Formats

### Common formats
| Format | Type | Notes |
|--------|------|------|
| CSV | text | human-readable, slow, no schema |
| JSON | text | flexible, slow, nested support |
| Parquet | columnar | fastest, compressed, schema |
| ORC | columnar | optimized for Hive |
| Avro | row | schema evolution |
| Delta Lake | transactional | ACID, CDC, time travel |

---

## Reading Data

### General syntax
```python
df = spark.read.format("parquet").load("path")
```

### CSV Example
```python
df = spark.read.csv("path", header=True, inferSchema=True)
```

### JSON Example
```python
df = spark.read.json("path")
```

### Parquet Example
```python
df = spark.read.parquet("path")
```

---

## Writing Data

### General syntax
```python
df.write.format("parquet").save("path")
```

### Write modes
| Mode | Behavior |
|------|----------|
| overwrite | replace data |
| append | add data |
| ignore | do nothing if exists |
| error | fail if exists |

```python
df.write.mode("overwrite").parquet("path")
```

---

## Columnar Formats (High Performance)

### Why Parquet / ORC?
- Columnar compression
- Predicate pushdown
- Faster scans
- Smaller files

Example:
```python
df.write.parquet("data.parquet")
df2 = spark.read.parquet("data.parquet")
```

---

## Compression

### Options
- snappy (default, fast)
- zstd (best compression)
- lz4 (fast)
- gzip (slow)

```python
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
```

---

## Partitioned Output

Useful for large datasets:

```python
df.write.partitionBy("country").parquet("path")
```

Benefits:
- Faster queries
- Reduced reads

Tradeoffs:
- Many small files if cardinality is high

---

## Coalescing Output Files

Avoid too many small files:

```python
df.coalesce(1).write.parquet("path")
```

---

## Bucketing (Advanced)

```python
df.write.bucketBy(8, "id").sortBy("id").saveAsTable("t")
```

Benefits:
- Improved joins
- Faster aggregations

---

## Reading Multiple Files

```python
df = spark.read.parquet("data/202*/")
```

Pattern matching supported!

---

## Loading from Cloud Storage

Examples:
```python
spark.read.parquet("s3a://bucket/data")
spark.read.parquet("gs://bucket/data")
spark.read.parquet("abfss://container@account.dfs.core.windows.net/data")
```

---

## SQL Integration

```python
df.createOrReplaceTempView("cats")
spark.sql("SELECT country, COUNT(*) FROM cats GROUP BY country")
```

---

## Performance Tips

1. Prefer Parquet  
2. Avoid CSV/JSON for big data  
3. Partition data by frequent filter keys  
4. Avoid too many small files  
5. Compress using snappy/zstd  

---

## Teaching Notes

- Show Parquet vs CSV performance difference  
- Use explain() to show pushdown  
- Students should examine file sizes  

---

## Exercises

1. Load CSV with header  
2. Write DataFrame as Parquet  
3. Compare file sizes CSV vs Parquet  
4. Partition by region  
5. Coalesce output to 4 files  
6. Load JSON nested data  
7. Benchmark filter performance on CSV vs Parquet  
8. Set compression to zstd  

---

## Summary

- File format choice matters  
- Parquet/ORC offer best performance  
- Write modes, partitioning, compression are key tools  
- Spark integrates with all storage systems  
