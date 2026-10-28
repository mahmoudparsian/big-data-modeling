# Package Tracking: DataFrame Version

## 1. Introduction

This is the DataFrame companion to
[A Real-Life Case for `reduceByKey()`: Package Tracking](reducebykey_package_tracking.md).
It uses the same scans, so you can compare the two APIs side by side.

**The job:** a delivery company scans every package many times. The "Track
your package" page shows the **current** status: the scan with the
**latest** timestamp. The scans arrive out of order.

| Input: one row per **scan** | Output: one row per **package** |
|---|---|
| `PKG-1001, 2026-10-21 06:05, Santa Clara, CA, out for delivery` | `PKG-1001, 2026-10-21 14:32, Santa Clara, CA, delivered` |
| `PKG-1001, 2026-10-20 08:15, San Jose, CA, picked up` | |
| `PKG-1001, 2026-10-21 14:32, Santa Clara, CA, delivered` | |
| `PKG-1001, 2026-10-20 19:40, Oakland, CA, in transit` | |

### From `reduceByKey()` to DataFrames

With RDDs, we wrote the business rule as a function and handed it to
`reduceByKey()`. With DataFrames, we pick a **built-in aggregate function**
that already does what the rule says:

| Business rule | RDD | DataFrame |
|---|---|---|
| Of two scans, keep the later one | `reduceByKey(later_scan)`, or `reduceByKey(max)` on `(time, location, status)` tuples | `max(struct(scan_time, location, status))`, or `max_by(column, scan_time)` |

The trick is the same in both APIs: put the timestamp **first** in a
tuple (RDD) or a struct (DataFrame), and the largest one is the latest scan.


## 2. Sample data

Start the PySpark shell (`./bin/pyspark`). Each scan is one row with four
columns. The timestamps arrive as text, so we convert them to a real
`timestamp` column with `to_timestamp()`:

~~~python
>>> spark.version
'4.2.0'

>>> from pyspark.sql import functions as F

>>> scans = [
...     ('PKG-1001', '2026-10-21 06:05', 'Santa Clara, CA', 'out for delivery'),
...     ('PKG-1002', '2026-10-22 03:10', 'Portland, OR',    'in transit'),
...     ('PKG-1001', '2026-10-20 08:15', 'San Jose, CA',    'picked up'),
...     ('PKG-1003', '2026-10-22 11:45', 'Austin, TX',      'picked up'),
...     ('PKG-1001', '2026-10-21 14:32', 'Santa Clara, CA', 'delivered'),
...     ('PKG-1002', '2026-10-21 09:00', 'Seattle, WA',     'picked up'),
...     ('PKG-1003', '2026-10-22 18:20', 'Dallas, TX',      'in transit'),
...     ('PKG-1001', '2026-10-20 19:40', 'Oakland, CA',     'in transit'),
... ]

>>> scans_df = (spark.createDataFrame(
...                 scans, "package_id STRING, scan_time STRING, location STRING, status STRING")
...             .withColumn("scan_time", F.to_timestamp("scan_time")))
>>> scans_df.printSchema()
root
 |-- package_id: string (nullable = true)
 |-- scan_time: timestamp (nullable = true)
 |-- location: string (nullable = true)
 |-- status: string (nullable = true)
<BLANKLINE>
>>> scans_df.show(truncate=False)
+----------+-------------------+---------------+----------------+
|package_id|scan_time          |location       |status          |
+----------+-------------------+---------------+----------------+
|PKG-1001  |2026-10-21 06:05:00|Santa Clara, CA|out for delivery|
|PKG-1002  |2026-10-22 03:10:00|Portland, OR   |in transit      |
|PKG-1001  |2026-10-20 08:15:00|San Jose, CA   |picked up       |
|PKG-1003  |2026-10-22 11:45:00|Austin, TX     |picked up       |
|PKG-1001  |2026-10-21 14:32:00|Santa Clara, CA|delivered       |
|PKG-1002  |2026-10-21 09:00:00|Seattle, WA    |picked up       |
|PKG-1003  |2026-10-22 18:20:00|Dallas, TX     |in transit      |
|PKG-1001  |2026-10-20 19:40:00|Oakland, CA    |in transit      |
+----------+-------------------+---------------+----------------+
<BLANKLINE>
~~~

> **About `<BLANKLINE>`:** `printSchema()` and `show()` print an empty line at
> the end. In the shell you just see a blank line; `<BLANKLINE>` marks it here.

> **About `orderBy()`:** after a `groupBy()`, the order of the rows is not
> guaranteed. The examples use `orderBy()` before `show()` so the output is
> always the same.


## 3. The DataFrame solution: `max()` of a struct

`F.struct()` packs several columns into one value, like a Python tuple.
Structs are compared field by field, starting with the first one, so the
**largest** `(scan_time, location, status)` struct is the **latest** scan.
`"latest.*"` unpacks the struct back into columns:

~~~python
>>> current = (scans_df
...            .groupBy("package_id")
...            .agg(F.max(F.struct("scan_time", "location", "status")).alias("latest"))
...            .select("package_id", "latest.*"))
>>> current.orderBy("package_id").show(truncate=False)
+----------+-------------------+---------------+----------+
|package_id|scan_time          |location       |status    |
+----------+-------------------+---------------+----------+
|PKG-1001  |2026-10-21 14:32:00|Santa Clara, CA|delivered |
|PKG-1002  |2026-10-22 03:10:00|Portland, OR   |in transit|
|PKG-1003  |2026-10-22 18:20:00|Dallas, TX     |in transit|
+----------+-------------------+---------------+----------+
<BLANKLINE>
~~~

This is the DataFrame twin of `scans_rdd.reduceByKey(max)`: one rule, "the
largest scan, with the time first".

### The same with `max_by()`

`F.max_by(col, ord)` returns the value of `col` from the row where `ord` is
largest. It reads almost like the business rule: "the status **at the
latest scan time**":

~~~python
>>> current2 = scans_df.groupBy("package_id").agg(
...     F.max("scan_time").alias("scan_time"),
...     F.max_by("location", "scan_time").alias("location"),
...     F.max_by("status", "scan_time").alias("status"))
>>> current2.orderBy("package_id").show(truncate=False)
+----------+-------------------+---------------+----------+
|package_id|scan_time          |location       |status    |
+----------+-------------------+---------------+----------+
|PKG-1001  |2026-10-21 14:32:00|Santa Clara, CA|delivered |
|PKG-1002  |2026-10-22 03:10:00|Portland, OR   |in transit|
|PKG-1003  |2026-10-22 18:20:00|Dallas, TX     |in transit|
+----------+-------------------+---------------+----------+
<BLANKLINE>
~~~

Which one to use? `max_by()` is easier to read when you need one or two
columns. The struct version is safer when two scans could have the **same**
timestamp: `max()` of a struct then compares the next field (the location),
so the answer never depends on the order of the rows. With `max_by()` and
a tie, Spark may return either row.


## 4. What Spark does with it: the `reduceByKey()` plan

We never wrote "reduce inside each partition first", but Spark does it
anyway. `explain()` prints the physical plan. We capture it as text and
look for two words:

~~~python
>>> import contextlib, io
>>> def plan_of(df):
...     buf = io.StringIO()
...     with contextlib.redirect_stdout(buf):
...         df.explain()
...     return buf.getvalue()
...

>>> plan = plan_of(current)
>>> "partial_max" in plan, "Exchange hashpartitioning" in plan
(True, True)
~~~

Read from the bottom up, the plan says:

1. `partial_max`: in each partition, keep only the latest scan of each
   package (what `later_scan` did inside a partition);
2. `Exchange hashpartitioning(package_id)`: the shuffle, which sends each
   package's partial results to one place;
3. `max`: merge the partial results into the final latest scan.

That's the `reduceByKey()` plan: only **one** scan per package per
partition crosses the network. The difference is who writes the rule: you
(RDD) or Spark's built-in `max()` (DataFrame).


## 5. Bonus: the status dashboard

How many packages are in each status right now? With RDDs this was a
second `reduceByKey()` (the word-count pattern). With DataFrames it is a
second `groupBy()`:

~~~python
>>> current.groupBy("status").count().orderBy("status").show()
+----------+-----+
|    status|count|
+----------+-----+
| delivered|    1|
|in transit|    2|
+----------+-----+
<BLANKLINE>
~~~


## 6. Other ways: a window function, and Spark SQL

### `row_number()` over a window

Another common way is to **rank** each package's scans from newest to
oldest, and keep rank 1:

~~~python
>>> from pyspark.sql import Window

>>> newest_first = Window.partitionBy("package_id").orderBy(F.desc("scan_time"))
>>> current3 = (scans_df
...             .withColumn("rank", F.row_number().over(newest_first))
...             .filter("rank = 1")
...             .drop("rank"))
>>> current3.orderBy("package_id").show(truncate=False)
+----------+-------------------+---------------+----------+
|package_id|scan_time          |location       |status    |
+----------+-------------------+---------------+----------+
|PKG-1001  |2026-10-21 14:32:00|Santa Clara, CA|delivered |
|PKG-1002  |2026-10-22 03:10:00|Portland, OR   |in transit|
|PKG-1003  |2026-10-22 18:20:00|Dallas, TX     |in transit|
+----------+-------------------+---------------+----------+
<BLANKLINE>
~~~

Same answer, and it keeps every column of the row without listing them.
But a window first **shuffles and sorts every scan** of a package, and only
then throws all of them away except one: the DataFrame version of
`groupByKey().mapValues(max)`. Prefer `max()`/`max_by()` when you only need
the latest row. The window version shines when you need the latest **N**
rows (`rank <= 3`).

### Spark SQL

~~~python
>>> scans_df.createOrReplaceTempView("scans")
>>> spark.sql("""
...     SELECT package_id,
...            max(scan_time)                AS scan_time,
...            max_by(location, scan_time)   AS location,
...            max_by(status, scan_time)     AS status
...     FROM scans
...     GROUP BY package_id
...     ORDER BY package_id
... """).show(truncate=False)
+----------+-------------------+---------------+----------+
|package_id|scan_time          |location       |status    |
+----------+-------------------+---------------+----------+
|PKG-1001  |2026-10-21 14:32:00|Santa Clara, CA|delivered |
|PKG-1002  |2026-10-22 03:10:00|Portland, OR   |in transit|
|PKG-1003  |2026-10-22 18:20:00|Dallas, TX     |in transit|
+----------+-------------------+---------------+----------+
<BLANKLINE>
~~~


## 7. A common mistake: the `max()` of every column

It is tempting to write "the latest scan" as "the `max()` of every column":

~~~python
>>> wrong = scans_df.groupBy("package_id").agg(
...     F.max("scan_time").alias("scan_time"),
...     F.max("location").alias("location"),      # WRONG
...     F.max("status").alias("status"))          # WRONG
>>> wrong.orderBy("package_id").show(truncate=False)
+----------+-------------------+---------------+---------+
|package_id|scan_time          |location       |status   |
+----------+-------------------+---------------+---------+
|PKG-1001  |2026-10-21 14:32:00|Santa Clara, CA|picked up|
|PKG-1002  |2026-10-22 03:10:00|Seattle, WA    |picked up|
|PKG-1003  |2026-10-22 18:20:00|Dallas, TX     |picked up|
+----------+-------------------+---------------+---------+
<BLANKLINE>
~~~

Every package is now "picked up", and `PKG-1002` is in Seattle at 03:10 on
10-22, a scan that never happened (at that time it was in Portland). Each
`max()` runs **on its own column**: `max("status")` is the alphabetically
largest status (`'picked up'` > `'out for delivery'` > `'in transit'` >
`'delivered'`), not the status of the latest scan. The three answers come
from three **different** rows, glued together.

Columns that must come from the **same row** need to travel together: in
one struct (section 3), with `max_by()`, or with a window (section 6). The
RDD version could not make this mistake, because a scan was one tuple, and
`later_scan` kept or dropped the whole tuple.

(A similar trap: `F.last("status")` returns the status of whatever row
happens to come last in each group. That is the DataFrame version of "the
last one wins", and the Spark documentation calls `first()` and `last()`
**non-deterministic** for exactly this reason.)


## 8. Takeaways

| Real-life case | RDD | DataFrame |
|---|---|---|
| [Package tracking](reducebykey_package_tracking_dataframe.md): the current scan per package | `reduceByKey(later_scan)` | `groupBy().agg(max(struct(time, ...)))` or `max_by(col, time)` |
| [Election-night vote totals](foldbykey_election_results_dataframe.md) per county | `foldByKey({}, add_tallies)` | `explode()` each tally into rows, then `groupBy().agg(sum())` |
| [Median home price](groupbykey_median_home_prices_dataframe.md) per zip code | `groupByKey()` + `mapValues(median)` | `groupBy().agg(median())` |
| [Star ratings](aggregatebykey_product_ratings_dataframe.md) per product | `aggregateByKey([0, 0, 0, 0, 0], add_review, merge_boxes)` | `groupBy().agg(count(when(stars == k, 1)))` for each star level |
| [Customer profiles](combinebykey_customer_profile_dataframe.md) from an order log | `combineByKey(start_profile, add_order, merge_profiles)` | `groupBy().agg(count(), sum(), min(), max(), mode())` |
| [Presorted mail](sortbykey_presorted_mail_dataframe.md) by ZIP code | `sortByKey(numPartitions=3)` | `repartitionByRange(3, "zip_code").sortWithinPartitions("zip_code")` |

Rules to remember from this example:

* "The value of X at the latest time" is `max_by(X, time)`, or `max()` of a
  struct that starts with the time.
* Separate `max()` calls on separate columns do **not** come from the same
  row. Keep a row's columns together in a struct.
* Built-in aggregates such as `max()` are combined per partition before the
  shuffle (`partial_max`), just like `reduceByKey()`.

A runnable version of this example is
[`reducebykey_package_tracking_dataframe.py`](reducebykey_package_tracking_dataframe.py)
(output of a real run: [`.log`](reducebykey_package_tracking_dataframe.log)).
