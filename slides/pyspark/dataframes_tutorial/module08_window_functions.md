# Module 8 – Window Functions in PySpark

## Overview
Window functions enable calculations across groups of rows without collapsing them into a single row.
They are essential for:
- Ranking
- Running totals
- Moving averages
- Percentiles
- Lag/lead comparisons
- Top-N analytics

---

## Importing Required Tools

```python
from pyspark.sql.window import Window
from pyspark.sql import functions as F
```

---

## Example Dataset

```python
df = spark.createDataFrame([
    ("Alice","IT",90000),
    ("Bob","IT",80000),
    ("Carol","HR",70000),
    ("Dan","HR",75000),
    ("Eve","HR",85000),
], ["name","dept","salary"])
```

---

## Defining a Window

```python
w = Window.partitionBy("dept").orderBy(F.col("salary").desc())
```

---

## Ranking Functions

### row_number()

```python
df.withColumn("rn", F.row_number().over(w)).show()
```

### rank()

```python
df.withColumn("rnk", F.rank().over(w)).show()
```

### dense_rank()

```python
df.withColumn("drank", F.dense_rank().over(w)).show()
```

---

## Top-N per Group

Example: Top 2 salaries per department

```python
(df.withColumn("rank", F.row_number().over(w))
   .filter("rank <= 2")
   .show())
```

---

## Cumulative Aggregations

### Running total

```python
df.withColumn("running_total", F.sum("salary").over(w)).show()
```

---

## Lag and Lead

### lag()

```python
df.withColumn("prev_salary", F.lag("salary",1).over(w)).show()
```

### lead()

```python
df.withColumn("next_salary", F.lead("salary",1).over(w)).show()
```

---

## Percent Rank

```python
df.withColumn("pct_rank", F.percent_rank().over(w)).show()
```

---

## ntile()

Split ordered rows into buckets:

```python
df.withColumn("bucket", F.ntile(3).over(w)).show()
```

---

## Window Without Ordering

Partition only:

```python
w2 = Window.partitionBy("dept")

df.withColumn("avg_dept", F.avg("salary").over(w2)).show()
```

Each row gets group average **without collapsing**.

---

## Practical Example: Compare Salary to Department Average

```python
w = Window.partitionBy("dept")
(df.withColumn("avg_sal", F.avg("salary").over(w))
   .withColumn("diff", F.col("salary") - F.col("avg_sal"))
   .show())
```

---

## Frame Specifications

Default frame:
- From unbounded preceding to current row

Custom frames:

```python
Window.orderBy("salary").rowsBetween(-1,1)
```

---

## Teaching Notes
- Window functions don’t reduce row count
- Partition defines grouping
- Order defines ranking or sequence
- Frames control aggregation ranges
- Excellent for analytics and ML features

---

## Exercises
1. Compute row_number per department  
2. Compute rank per department  
3. Show top 3 salaries per department  
4. Compute running total of salary per dept  
5. Compute lag/lead of salary  
6. Compute avg salary per dept without aggregation  
7. Compute difference from avg salary  
8. Use ntile to create quartiles  

---

## Summary
- Window functions preserve row structure  
- Useful for ranking, comparisons, and cumulative stats  
- Partition, order, and frame define behavior  
- Core tool for analytics and feature engineering  
