# Find Sum, Count, and Average Per Key: DataFrame Version

## 1. Introduction

This is the DataFrame companion to
[Find Sum, Count, and Average Per Key](find_sum_count_avg_per_key.md),
which solves the same problem with RDD reducers (`groupByKey()`,
`reduceByKey()`, `aggregateByKey()`, `combineByKey()`). It uses the same data,
so you can compare the two APIs side by side.

### The problem

**Input:** a DataFrame with two columns, `key` (string) and `value` (integer).

**Output:** one row per key, with the `sum`, `count`, and `average` of that key's values.

| Input (`key`, `value`) | Output (`key`, `sum`, `count`, `average`) |
|---|---|
| `('A', 2)`, `('A', 4)`, `('A', 9)` | `('A', 15, 3, 5.0)` |
| `('B', 6)`, `('B', 1)`, `('B', 2)` | `('B', 9, 3, 3.0)` |
| `('C', 1)`, `('C', 6)` | `('C', 7, 2, 3.5)` |

### RDD vs. DataFrame

With RDDs, you tell Spark **how** to compute the result: you write the functions
that build and merge `(sum, count)` pairs, then divide.

With DataFrames, you tell Spark **what** you want (`sum`, `count`, `avg` per
`key`) using built-in aggregate functions, and Spark's optimizer (Catalyst)
decides how to compute it. As section 7 shows, the plan it picks is the same
idea as `reduceByKey()`: partial `(sum, count)` per partition, then a shuffle,
then a final merge.

Solutions in this document:

| Section | Approach |
|---|---|
| 3 | `groupBy().agg()` with `sum()`, `count()`, `avg()` |
| 4 | `groupBy().agg()` with `sum()` and `count()`, then `average = sum / count` |
| 5 | Spark SQL: `GROUP BY` |
| 6 | Window functions: keep every row and add per-key `sum`, `count`, `average` |


## 2. Sample data

Start the PySpark shell:

~~~
spark-4.2.0 % ./bin/pyspark
...
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 4.2.0
      /_/

Using Python version 3.13.2
Spark context available as 'sc' (master = local[*], ...).
SparkSession available as 'spark'.
~~~

Create the DataFrame once; all solutions below reuse it. The schema
`"key STRING, value INT"` is given explicitly. Without it, Spark would infer
Python `int` values as `long` (`bigint`).

~~~python
>>> spark.version
'4.2.0'

>>> from pyspark.sql import functions as F

>>> pairs = [('A', 2), ('A', 4), ('A', 9),
...          ('B', 6), ('B', 1), ('B', 2),
...          ('C', 1), ('C', 6)]

>>> df = spark.createDataFrame(pairs, "key STRING, value INT")
>>> df.printSchema()
root
 |-- key: string (nullable = true)
 |-- value: integer (nullable = true)
<BLANKLINE>
>>> df.show()
+---+-----+
|key|value|
+---+-----+
|  A|    2|
|  A|    4|
|  A|    9|
|  B|    6|
|  B|    1|
|  B|    2|
|  C|    1|
|  C|    6|
+---+-----+
<BLANKLINE>
~~~

> **About `<BLANKLINE>`:** `printSchema()` and `show()` print an empty line at
> the end. In the shell you just see a blank line; `<BLANKLINE>` marks it here.

> **About `orderBy("key")`:** after a `groupBy()`, the order of the rows is not
> guaranteed. The examples use `orderBy("key")` so the output is always the same.


## 3. `groupBy().agg()` solution

`agg()` computes several aggregates in one pass over the data. `alias()`
gives each result column a readable name:

~~~python
>>> result = df.groupBy("key").agg(
...     F.sum("value").alias("sum"),
...     F.count("value").alias("count"),
...     F.avg("value").alias("average"))
>>> result.orderBy("key").show()
+---+---+-----+-------+
|key|sum|count|average|
+---+---+-----+-------+
|  A| 15|    3|    5.0|
|  B|  9|    3|    3.0|
|  C|  7|    2|    3.5|
+---+---+-----+-------+
<BLANKLINE>

>>> result.printSchema()
root
 |-- key: string (nullable = true)
 |-- sum: long (nullable = true)
 |-- count: long (nullable = false)
 |-- average: double (nullable = true)
<BLANKLINE>
~~~

Note the result types: `sum()` of an `int` column is a `long` (so it can't
overflow), `count()` is always a `long`, and `avg()` is always a `double`.

`F.mean()` is another name for `F.avg()`.

Without `alias()`, Spark names the columns after the expressions, which is
harder to work with:

~~~python
>>> df.groupBy("key").agg(F.sum("value"), F.count("value"), F.avg("value")).columns
['key', 'sum(value)', 'count(value)', 'avg(value)']
~~~


## 4. `sum` and `count` first, then `average = sum / count`

This mirrors the RDD solutions: aggregate `(sum, count)` per key, then
compute the average from them with `withColumn()`, the DataFrame equivalent
of `mapValues()`:

~~~python
>>> sum_count = df.groupBy("key").agg(
...     F.sum("value").alias("sum"),
...     F.count("value").alias("count"))

>>> sum_count_avg = sum_count.withColumn("average", F.col("sum") / F.col("count"))
>>> sum_count_avg.orderBy("key").show()
+---+---+-----+-------+
|key|sum|count|average|
+---+---+-----+-------+
|  A| 15|    3|    5.0|
|  B|  9|    3|    3.0|
|  C|  7|    2|    3.5|
+---+---+-----+-------+
<BLANKLINE>
~~~

In practice, prefer `F.avg()` (section 3). It's shorter and Spark computes
it the same way (see section 7). This version is useful when you need the
`sum` and `count` columns anyway.


## 5. Spark SQL solution

Register the DataFrame as a temporary view, then use plain SQL:

~~~python
>>> df.createOrReplaceTempView("pairs")

>>> spark.sql("""
...     SELECT key,
...            SUM(value)   AS sum,
...            COUNT(value) AS count,
...            AVG(value)   AS average
...     FROM pairs
...     GROUP BY key
...     ORDER BY key
... """).show()
+---+---+-----+-------+
|key|sum|count|average|
+---+---+-----+-------+
|  A| 15|    3|    5.0|
|  B|  9|    3|    3.0|
|  C|  7|    2|    3.5|
+---+---+-----+-------+
<BLANKLINE>
~~~

Spark SQL and the DataFrame API go through the same optimizer, so this
query runs exactly like the section 3 solution.


## 6. Window function solution: keep every row

`groupBy()` collapses each key to **one** row. Sometimes you want to keep
every original row and add the per-key statistics next to it (for example,
to compare each value to its key's average). A window partitioned by `key`
does that:

~~~python
>>> from pyspark.sql.window import Window

>>> w = Window.partitionBy("key")

>>> with_stats = (df
...     .withColumn("sum", F.sum("value").over(w))
...     .withColumn("count", F.count("value").over(w))
...     .withColumn("average", F.avg("value").over(w)))
>>> with_stats.orderBy("key", "value").show()
+---+-----+---+-----+-------+
|key|value|sum|count|average|
+---+-----+---+-----+-------+
|  A|    2| 15|    3|    5.0|
|  A|    4| 15|    3|    5.0|
|  A|    9| 15|    3|    5.0|
|  B|    1|  9|    3|    3.0|
|  B|    2|  9|    3|    3.0|
|  B|    6|  9|    3|    3.0|
|  C|    1|  7|    2|    3.5|
|  C|    6|  7|    2|    3.5|
+---+-----+---+-----+-------+
<BLANKLINE>
~~~

Note: a window function has no map-side combining. All rows of a key are
shuffled to the same partition, much like `groupByKey()` in the RDD version.
Use `groupBy()` when one row per key is all you need.


## 7. Under the hood: Spark builds a `reduceByKey()`-style plan

`explain()` shows the physical plan Spark runs for the section 3 solution:

```python
df.groupBy("key").agg(F.sum("value"), F.count("value"), F.avg("value")).explain()
```

Output (the `#` numbers and `plan_id` change from run to run):

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[key#0], functions=[sum(value#1), count(value#1), avg(value#1)])
   +- Exchange hashpartitioning(key#0, 200), ENSURE_REQUIREMENTS, [plan_id=238]
      +- HashAggregate(keys=[key#0], functions=[partial_sum(value#1), partial_count(value#1), partial_avg(value#1)])
         +- Scan ExistingRDD[key#0,value#1]
```

Read the plan from the bottom up:

1. **`Scan`**: read the rows.
2. **`HashAggregate` with `partial_...`**: in each partition, compute a
   partial result per key. This is map-side combining, the same idea as
   `reduceByKey()`, `aggregateByKey()`, and `combineByKey()`.
3. **`Exchange hashpartitioning(key)`**: the shuffle. Only the partial
   results cross the network, not every row.
4. **`HashAggregate`** (top): merge the partial results for each key into
   the final values.

And what is the partial result for `avg()`? `explain(mode="formatted")`
shows that `partial_avg` keeps a `(sum, count)` pair, exactly like our RDD
solutions:

```python
df.groupBy("key").agg(F.avg("value")).explain(mode="formatted")
```

Output (excerpt):

```
Functions [1]: [partial_avg(value#1)]
Aggregate Attributes [2]: [sum#6, count#7L]
Results [3]: [key#0, sum#8, count#9L]
```

So you never have to build the `(sum, count)` pairs yourself with
DataFrames, but it's good to know that this is what Spark does for you.


## 8. Nulls: `count(column)` vs. `count("*")`

The RDD version has no nulls, but real DataFrame columns often do. Aggregate
functions **skip nulls**. Let's add a row `('C', None)`:

~~~python
>>> df2 = spark.createDataFrame(pairs + [('C', None)], "key STRING, value INT")

>>> df2.groupBy("key").agg(
...     F.sum("value").alias("sum"),
...     F.count("value").alias("count"),
...     F.count("*").alias("rows"),
...     F.avg("value").alias("average")).orderBy("key").show()
+---+---+-----+----+-------+
|key|sum|count|rows|average|
+---+---+-----+----+-------+
|  A| 15|    3|   3|    5.0|
|  B|  9|    3|   3|    3.0|
|  C|  7|    2|   3|    3.5|
+---+---+-----+----+-------+
<BLANKLINE>
~~~

For key `C`, `count("value")` is `2` (non-null values) but `count("*")` is
`3` (all rows). `avg()` is `sum / count(value)`, so it's still `3.5`. If you
compute the average yourself (as in section 4), divide by `count("value")`,
not `count("*")`.


## 9. From DataFrame back to the RDD format

The RDD version's output is `RDD[(key, (sum, count, average))]`. To get the
same shape from the DataFrame result, use `df.rdd`, which gives an RDD of
`Row` objects:

~~~python
>>> result.orderBy("key").collect()
[Row(key='A', sum=15, count=3, average=5.0), Row(key='B', sum=9, count=3, average=3.0), Row(key='C', sum=7, count=2, average=3.5)]

>>> # Use row["count"], not row.count: count is also a method of Row (it's a tuple)
>>> rdd = result.rdd.map(lambda row: (row["key"], (row["sum"], row["count"], row["average"])))
>>> sorted(rdd.collect())
[('A', (15, 3, 5.0)), ('B', (9, 3, 3.0)), ('C', (7, 2, 3.5))]
~~~

This is the same answer as every solution in the
[RDD version](find_sum_count_avg_per_key.md).


## 10. Takeaways

| | RDD reducers | DataFrame `groupBy().agg()` |
|---|---|---|
| Style | you write *how* (combine functions) | you write *what* (`sum`, `count`, `avg`) |
| `(sum, count)` pairs | built by you | built by Spark (`partial_avg`) |
| Map-side combining | `reduceByKey()`, `aggregateByKey()`, `combineByKey()` (not `groupByKey()`) | automatic (`partial_...` aggregates) |
| Optimizer | none: runs your functions as written | Catalyst picks the plan |
| Nulls | you handle them | aggregate functions skip them |

* For aggregations like this one, the DataFrame API (or Spark SQL) is shorter,
  easier to read, and usually faster. Python lambdas in RDDs run in Python
  worker processes, while built-in DataFrame functions run in the JVM.
* Use `groupBy()` for one row per key; use a window when you need to keep
  every row next to its key's statistics.
* Name your result columns with `alias()`.
