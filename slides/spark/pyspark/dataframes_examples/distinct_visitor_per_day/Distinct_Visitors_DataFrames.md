# Distinct Visitors per Day Using PySpark DataFrames

Target: **Apache Spark 4.2.0**. Complete runnable solution:
[distinct_visitors_dataframe.py](distinct_visitors_dataframe.py).
See the [README](README.md) for installation, the 13 sample events, and checks.
This solution uses DataFrame expressions throughout, with no RDD conversion
and no Python UDF.

## 1. Problem

Count each user ID once per reporting date, even when visits repeat
across input files. The six rows in [partition_1.csv](data/partition_1.csv)
and seven in [partition_2.csv](data/partition_2.csv) have this distribution:

| Date | First file's users | Second file's users | Unique users globally |
|---|---|---|---|
| 2026-09-01 | U1, U2 | U2, U3 | U1, U2, U3 |
| 2026-09-02 | U1, U3 | U3, U4, U5 | U1, U3, U4, U5 |

Adding each file's distinct counts gives 4 and 5. The correct counts are
**3 and 4** because U2 and U3, respectively, occur in both files.

## 2. Load CSV with an Explicit Schema

```python
from pyspark.sql import functions as F, types as T

schema = T.StructType([
    T.StructField(name, T.StringType(), True)
    for name in ("event_id", "date", "user_id", "page")
])
events = (
    spark.read.schema(schema)
    .option("header", True)
    .option("mode", "FAILFAST")
    .csv(input_path)
)
```

All four columns are strings. This keeps IDs intact and avoids schema
inference. `date` is an already-normalized ISO date label; the lesson
performs no timestamp-to-date or time-zone conversion. Each CSV file
has the same header and column order.

`FAILFAST` requests failure on parser-detected malformed records; it is
not a complete schema or business-rule validator. The bundled data obeys
the contract in the README.

## 3. Select and Clean the Keys

```python
pairs = (
    events.select(
        F.trim("date").alias("date"),
        F.trim("user_id").alias("user_id"),
    )
    .where(
        F.col("date").isNotNull()
        & F.col("user_id").isNotNull()
        & (F.col("date") != "")
        & (F.col("user_id") != "")
    )
)
```

This projection discards event IDs and page paths, which do not affect
daily user counts. The sample retains 13 rows. Whitespace-only keys are
excluded; leading and trailing spaces on valid keys are removed.

Apply this policy before deduplication. In particular, `count_distinct`
ignores null user IDs, while counting rows after deduplication would
otherwise count a null-user row. Cleaning makes the two approaches
consistent. Empty strings are not SQL nulls, so filter them explicitly.

## 4. Remove Duplicate Date/User Pairs

```python
unique_pairs = pairs.dropDuplicates(["date", "user_id"])
```

The seven remaining rows, displayed in date/user order for teaching, are:

| date | user_id |
|---|---|
| 2026-09-01 | U1 |
| 2026-09-01 | U2 |
| 2026-09-01 | U3 |
| 2026-09-02 | U1 |
| 2026-09-02 | U3 |
| 2026-09-02 | U4 |
| 2026-09-02 | U5 |

The operation removes duplicates globally, not just inside each input
partition. It does not guarantee row order. See the
[Spark 4.2.0 dropDuplicates API](https://spark.apache.org/docs/4.2.0/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicates.html).

Do not use `events.dropDuplicates()` on all four original columns:
different event IDs or pages would keep repeated visits distinct.

## 5. Group by Date and Count

```python
result = unique_pairs.groupBy("date").agg(
    F.count("*").alias("distinct_visitors")
)
result.orderBy("date").show(truncate=False)
```

`count("*")` counts the already-deduplicated rows. In this cleaned table,
one row means one user on one date.

```text
+----------+-----------------+
|date      |distinct_visitors|
+----------+-----------------+
|2026-09-01|3                |
|2026-09-02|4                |
+----------+-----------------+
```

The result schema contains `date: string` and `distinct_visitors: long`.
Ordering is added for presentation, not for correctness.

## 6. Alternative: Exact count_distinct

The application also defines an equivalent function for comparison:

```python
result = pairs.groupBy("date").agg(
    F.count_distinct("user_id").alias("distinct_visitors")
)
```

This expresses the intent more compactly. It is an **exact** count; do
not replace it with `approx_count_distinct` when exact results are required.
The [Spark 4.2.0 count_distinct API](https://spark.apache.org/docs/4.2.0/api/python/reference/pyspark.sql/api/pyspark.sql.functions.count_distinct.html)
describes this aggregation. The check script verifies both forms agree
with each other and with the expected results.

## 7. Complete Core Function

`clean_pairs` in the executable contains Section 3's expressions. The
primary solution is:

```python
def distinct_visitors(events):
    unique_pairs = clean_pairs(events).dropDuplicates(["date", "user_id"])
    return unique_pairs.groupBy("date").agg(
        F.count("*").alias("distinct_visitors")
    )
```

The full script includes the Spark session, command-line arguments,
CSV loading, ordered display, and `spark.stop()` cleanup.

## 8. Run and Inspect the Plan

After the [setup](README.md#1-setup):

```text
"$SPARK_HOME/bin/spark-submit" --master "local[2]" distinct_visitors_dataframe.py
"$SPARK_HOME/bin/spark-submit" --master "local[2]" distinct_visitors_dataframe.py --explain
```

The second command calls `explain("formatted")` and then displays the
result. Look for scan, projection/filter, aggregation, and exchange
operators. Spark can optimize the logical operations, and adaptive query
execution can adjust the physical plan. Do not assume a fixed number of
jobs or one Spark job per DataFrame method.

Transformations are lazy; `show()` executes the computation.
The script sets `spark.sql.shuffle.partitions` to 2 for this tiny local
example. That is an initial shuffle setting, not a recommended cluster
value or a guarantee of final partition counts under adaptive execution.

For production output, use a distributed write, for example:

```python
result.write.mode("errorifexists").parquet(output_path)
```

Spark writes a directory of output files. The CLI provided here only
displays results and does not write or overwrite an output dataset.

## 9. Exercises and Answer Checks

1. Replace deduplication with `events.groupBy("date").count()`.
   Why do you get 6 and 7? You counted visits, not visitors.
2. Count distinct users for the whole period:
   `pairs.agg(F.count_distinct("user_id"))` gives 5, not 7.
3. Keep only dates with at least four visitors: filter `result` with
   `F.col("distinct_visitors") >= 4`. Only September 2 remains.
4. Count per page per day: retain page during projection, deduplicate
   `(date, page, user_id)`, then group by `(date, page)`.

Run `"$SPARK_HOME/bin/spark-submit" --master "local[2]" test_solutions.py` to verify both
exact DataFrame forms. A companion
[RDD solution](../../rdd_examples/distinct_visitor_per_day/Distinct_Visitors_RDD.md)
solves the same problem on the same inputs in its own sibling folder; its
test checks that solution independently, so the two are no longer
cross-checked against each other in one run.
