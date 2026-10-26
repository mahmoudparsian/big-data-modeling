# Find Maximum or Average Salary, Then Filter: DataFrame Version

This is the DataFrame companion to
[PySpark Example: Using `reduceByKey()` to Find Maximum or Average Salary](reducebykey_max_avg_filter.md).
It solves the same steps with the same data, so you can compare the RDD and
DataFrame solutions side by side:

| Step | Goal | RDD version | DataFrame version |
|---|---|---|---|
| 1 | Max salary per name | `reduceByKey(lambda a, b: max(a, b))` | `groupBy("name").agg(F.max("salary"))` |
| 2 | Keep names whose max salary is at most 10,000 | `filter(lambda kv: kv[1] <= 10000)` | `.where(F.col("max_salary") <= 10000)` |
| 3 | Average salary per name, keep those at most 10,000 | `combineByKey()` + `mapValues()` + `filter()` | `groupBy("name").agg(F.avg("salary"))` + `.where(...)` |


## 1. Sample data

Start the PySpark shell (`./bin/pyspark`), then create the DataFrame:

~~~python
>>> spark.version
'4.2.0'

>>> from pyspark.sql import functions as F

>>> data = [("Alice", 8000),
...         ("Bob", 12000),
...         ("Alice", 10000),
...         ("Bob", 9000),
...         ("Carol", 9500)]

>>> df = spark.createDataFrame(data, "name STRING, salary INT")
>>> df.show()
+-----+------+
| name|salary|
+-----+------+
|Alice|  8000|
|  Bob| 12000|
|Alice| 10000|
|  Bob|  9000|
|Carol|  9500|
+-----+------+
<BLANKLINE>
~~~

> **About `<BLANKLINE>`:** `show()` prints an empty line at the end. In the
> shell you just see a blank line; `<BLANKLINE>` marks it here.

> **About `orderBy("name")`:** after a `groupBy()`, the order of the rows is
> not guaranteed. The examples use `orderBy("name")` so the output is always
> the same.


## 2. Step 1: Maximum salary per name

The RDD version needs a function (`lambda a, b: max(a, b)`); with DataFrames
you use the built-in aggregate function `F.max()`:

~~~python
>>> max_salary_by_name = df.groupBy("name").agg(F.max("salary").alias("max_salary"))
>>> max_salary_by_name.orderBy("name").show()
+-----+----------+
| name|max_salary|
+-----+----------+
|Alice|     10000|
|  Bob|     12000|
|Carol|      9500|
+-----+----------+
<BLANKLINE>
~~~

Like `reduceByKey()`, `F.max()` is computed in two phases: a partial max per
partition before the shuffle, then a final max after it. You don't write
any of that; Spark plans it for you.


## 3. Step 2: Keep names whose maximum salary is at most 10,000

The result of `groupBy().agg()` is just another DataFrame, so filter it
with `where()` (`filter()` is another name for the same method):

~~~python
>>> max_salary_le_10k = max_salary_by_name.where(F.col("max_salary") <= 10000)
>>> max_salary_le_10k.orderBy("name").show()
+-----+----------+
| name|max_salary|
+-----+----------+
|Alice|     10000|
|Carol|      9500|
+-----+----------+
<BLANKLINE>
~~~

`Bob` is dropped because his maximum salary is 12,000.


## 4. Step 3: Average salary per name, keep those at most 10,000

In the RDD version this takes three steps: `combineByKey()` to build
`(sum, count)` pairs, `mapValues()` to divide, and `filter()`. With
DataFrames, `F.avg()` does the first two for you:

~~~python
>>> avg_salary_by_name = df.groupBy("name").agg(F.avg("salary").alias("avg_salary"))
>>> avg_salary_by_name.orderBy("name").show()
+-----+----------+
| name|avg_salary|
+-----+----------+
|Alice|    9000.0|
|  Bob|   10500.0|
|Carol|    9500.0|
+-----+----------+
<BLANKLINE>

>>> avg_salary_le_10k = avg_salary_by_name.where(F.col("avg_salary") <= 10000)
>>> avg_salary_le_10k.orderBy("name").show()
+-----+----------+
| name|avg_salary|
+-----+----------+
|Alice|    9000.0|
|Carol|    9500.0|
+-----+----------+
<BLANKLINE>
~~~

`Bob` is dropped because his average salary is `(12000 + 9000) / 2 = 10500.0`.

Bonus: one `agg()` call can compute several aggregates in a single pass,
which would take a custom combiner with RDDs:

~~~python
>>> df.groupBy("name").agg(
...     F.max("salary").alias("max_salary"),
...     F.avg("salary").alias("avg_salary"),
...     F.count("salary").alias("count")).orderBy("name").show()
+-----+----------+----------+-----+
| name|max_salary|avg_salary|count|
+-----+----------+----------+-----+
|Alice|     10000|    9000.0|    2|
|  Bob|     12000|   10500.0|    2|
|Carol|      9500|    9500.0|    1|
+-----+----------+----------+-----+
<BLANKLINE>
~~~


## 5. Spark SQL: `HAVING` filters groups

In SQL, a filter on an aggregate goes in a `HAVING` clause, which runs
**after** `GROUP BY`:

~~~python
>>> df.createOrReplaceTempView("salaries")

>>> spark.sql("""
...     SELECT name, MAX(salary) AS max_salary
...     FROM salaries
...     GROUP BY name
...     HAVING MAX(salary) <= 10000
...     ORDER BY name
... """).show()
+-----+----------+
| name|max_salary|
+-----+----------+
|Alice|     10000|
|Carol|      9500|
+-----+----------+
<BLANKLINE>

>>> spark.sql("""
...     SELECT name, AVG(salary) AS avg_salary
...     FROM salaries
...     GROUP BY name
...     HAVING AVG(salary) <= 10000
...     ORDER BY name
... """).show()
+-----+----------+
| name|avg_salary|
+-----+----------+
|Alice|    9000.0|
|Carol|    9500.0|
+-----+----------+
<BLANKLINE>
~~~

`df.groupBy(...).agg(...).where(...)` in sections 3 and 4 is the DataFrame
equivalent of `GROUP BY ... HAVING ...`.


## 6. Common mistake: filtering *before* grouping

Where you put the filter matters. Filtering **before** `groupBy()` (SQL
`WHERE`) removes individual **rows**; filtering **after** (SQL `HAVING`)
removes whole **groups**. They answer different questions:

~~~python
>>> # WHERE: drop salaries above 10,000 first, then take the max of what's left
>>> (df.where(F.col("salary") <= 10000)
...    .groupBy("name").agg(F.max("salary").alias("max_salary"))
...    .orderBy("name").show())
+-----+----------+
| name|max_salary|
+-----+----------+
|Alice|     10000|
|  Bob|      9000|
|Carol|      9500|
+-----+----------+
<BLANKLINE>
~~~

`Bob` is still here, with a "max" of 9,000: only his 12,000 row was removed.
This answers "what is each person's highest salary that is at most 10,000?",
not step 2's question "which people never earned more than 10,000?".

The same is true in the RDD version: `rdd.filter(...)` before `reduceByKey()`
is not the same as `filter(...)` after it.


## 7. Back to the RDD format

The RDD version's results are lists of `(name, value)` tuples. To get the
same shape from a DataFrame, map the rows of `df.rdd`:

~~~python
>>> sorted(max_salary_le_10k.rdd.map(lambda row: (row["name"], row["max_salary"])).collect())
[('Alice', 10000), ('Carol', 9500)]

>>> sorted(avg_salary_le_10k.rdd.map(lambda row: (row["name"], row["avg_salary"])).collect())
[('Alice', 9000.0), ('Carol', 9500.0)]
~~~

These match the outputs in the [RDD version](reducebykey_max_avg_filter.md).


## 8. Summary

| Step | Goal | DataFrame API | Spark SQL |
|---|---|---|---|
| 1 | Max salary per name | `groupBy("name").agg(F.max("salary"))` | `SELECT name, MAX(salary) ... GROUP BY name` |
| 2 | Keep groups with max ≤ 10,000 | `.where(F.col("max_salary") <= 10000)` | `HAVING MAX(salary) <= 10000` |
| 3 | Average per name, keep ≤ 10,000 | `groupBy("name").agg(F.avg("salary")).where(...)` | `HAVING AVG(salary) <= 10000` |

* Built-in aggregate functions (`F.max()`, `F.avg()`, `F.count()`, ...)
  replace the reduce/combine functions you write with RDDs, and Spark still
  combines values in each partition before the shuffle.
* A filter **after** `groupBy().agg()` (SQL `HAVING`) drops groups; a filter
  **before** it (SQL `WHERE`) drops rows. Pick the one that matches your question.
* For the same problem with RDDs, see the
  [RDD version](reducebykey_max_avg_filter.md); for more on aggregating
  per key with DataFrames, see
  [Find Sum, Count, and Average Per Key: DataFrame Version](find_sum_count_avg_per_key_dataframe.md).
