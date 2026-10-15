# PySpark DataFrame Practice Questions — Set 04: Answers

Solutions to the nine [Set 04 practice questions](QUESTIONS_04.md). Every session below
was run in the `bin/pyspark` shell (Spark 4.2.0), started from this folder. The output is
copied from those real runs.

Try the questions yourself first. There are often several correct solutions, and yours
doesn't need to match these exactly.

* [Q1 (Basic): Counting and summing per group](#q1-basic--counting-and-summing-per-group)
* [Q2 (Basic): Aggregating the whole table](#q2-basic--aggregating-the-whole-table)
* [Q3 (Basic): Grouping by two columns](#q3-basic--grouping-by-two-columns)
* [Q4 (Intermediate): Lists, sets, and conditional counts](#q4-intermediate--lists-sets-and-conditional-counts)
* [Q5 (Intermediate): Filtering before and after grouping](#q5-intermediate--filtering-before-and-after-grouping)
* [Q6 (Intermediate): Pivot tables](#q6-intermediate--pivot-tables)
* [Q7 (Intermediate+): Subtotals with rollup() and cube()](#q7-intermediate--subtotals-with-rollup-and-cube)
* [Q8 (Intermediate+): Medians, percentiles, and approximate counts](#q8-intermediate--medians-percentiles-and-approximate-counts)
* [Q9 (Intermediate+): Grouping by computed values, and the average trap](#q9-intermediate--grouping-by-computed-values-and-the-average-trap)

Setup, used by all the questions:

```python
>>> from pyspark.sql import functions as F
>>> trips_ddl = ("trip_id STRING, driver_id STRING, rider STRING, city STRING, "
...              "hour INT, distance_km DOUBLE, fare DOUBLE, rating INT")
>>> trips = spark.read.csv("data/trips.csv", header=True, schema=trips_ddl)
```

---

## Q1 (Basic) — Counting and summing per group

```python
>>> trips.groupBy("city").count().orderBy("city").show()
+--------+-----+
|    city|count|
+--------+-----+
| Oakland|    8|
|San Jose|   12|
+--------+-----+

>>> (trips.groupBy("city")
...       .agg(F.count("*").alias("trips"),
...            F.round(F.sum("fare"), 2).alias("revenue"),
...            F.round(F.avg("fare"), 2).alias("avg_fare"),
...            F.min("distance_km").alias("min_km"),
...            F.max("distance_km").alias("max_km"))
...       .orderBy("city")
...       .show())
+--------+-----+-------+--------+------+------+
|    city|trips|revenue|avg_fare|min_km|max_km|
+--------+-----+-------+--------+------+------+
| Oakland|    8| 168.25|   21.03|   3.1|  15.3|
|San Jose|   12|  308.0|   25.67|   2.2|  20.5|
+--------+-----+-------+--------+------+------+

>>> trips.groupBy("driver_id").agg(F.sum("fare").alias("revenue")).orderBy(F.desc("revenue")).show()
+---------+-------+
|driver_id|revenue|
+---------+-------+
|       d4| 185.25|
|       d1| 102.25|
|       d3|  99.75|
|       d2|   89.0|
+---------+-------+
```

**Notes:**

* **`groupBy(cols).agg(...)`** = "for each group, compute these summaries". It's the
  DataFrame version of `reduceByKey()`/`aggregateByKey()` from the RDD sets, and SQL's
  `GROUP BY`.
* `groupBy().count()` is a shortcut, and its result column is called `count`.
* One `agg()` computes **many** aggregates in one pass: here 5 per city. Compare
  [RDD Set 10, Q1](../rdd_practice_questions/ANSWERS_10.md#q1-basic--trips-fares-and-distances-per-city),
  where we carried a `(count, distance, fare)` tuple through `reduceByKey()` by hand. Spark
  now does that for us, and it still combines **locally** before the shuffle, just like
  `reduceByKey()`.
* The result of `groupBy()` has **no defined order**. Always `orderBy()` it when the order matters.

---

## Q2 (Basic) — Aggregating the whole table

```python
>>> trips.agg(F.count("*").alias("trips"),
...           F.sum("fare").alias("revenue"),
...           F.countDistinct("rider").alias("riders"),
...           F.countDistinct("driver_id", "city").alias("driver_cities")).show()
+-----+-------+------+-------------+
|trips|revenue|riders|driver_cities|
+-----+-------+------+-------------+
|   20| 476.25|     7|            7|
+-----+-------+------+-------------+

>>> trips.agg({"fare": "sum", "distance_km": "max"}).show()
+---------+----------------+
|sum(fare)|max(distance_km)|
+---------+----------------+
|   476.25|            20.5|
+---------+----------------+

>>> trips.agg(F.sum("fare")).first()[0]
476.25
```

**Notes:**

* **`agg()` without `groupBy()`** treats the whole table as **one group**, and returns one row.
* **`countDistinct(a, b)`** counts different **combinations**: 7 driver–city pairs.
* The **dict form** `agg({"col": "function"})` is short, but it can't rename the results,
  and it allows only one function per column. The `F.` functions with `alias()` are clearer.
* `.first()[0]` turns a one-row, one-column result into a plain Python number, to use in
  Python code.

---

## Q3 (Basic) — Grouping by two columns

```python
>>> (trips.groupBy("city", "driver_id")
...       .agg(F.count("*").alias("trips"), F.round(F.sum("fare"), 2).alias("revenue"))
...       .orderBy("city", "driver_id")
...       .show())
+--------+---------+-----+-------+
|    city|driver_id|trips|revenue|
+--------+---------+-----+-------+
| Oakland|       d1|    2|   30.0|
| Oakland|       d3|    3|   63.0|
| Oakland|       d4|    3|  75.25|
|San Jose|       d1|    4|  72.25|
|San Jose|       d2|    4|   89.0|
|San Jose|       d3|    3|  36.75|
|San Jose|       d4|    1|  110.0|
+--------+---------+-----+-------+

>>> trips.groupBy("city", "driver_id").count().count()
7
```

**Notes:**

* Grouping by **two columns** makes one group per **combination** that exists in the data:
  7 groups (`d2` never drove in Oakland, so there's no `(Oakland, d2)` group).
* This is [RDD Set 10, Q9](../rdd_practice_questions/ANSWERS_10.md#q9-intermediate--the-top-2-drivers-in-each-city)'s
  first step, `((city, driver), revenue)`, without having to build a tuple key.
* `.count().count()`: the first `count()` is the **per-group** count (a transformation that
  adds a column), and the second is the **action** that counts the rows (groups). They are
  different methods with the same name.

---

## Q4 (Intermediate) — Lists, sets, and conditional counts

```python
>>> (trips.groupBy("driver_id")
...       .agg(F.collect_list("rider").alias("riders"),
...            F.array_sort(F.collect_set("rider")).alias("unique_riders"),
...            F.count("rating").alias("rated"),
...            F.count_if(F.col("rating") == 5).alias("five_star"),
...            F.sum(F.when(F.col("rating") <= 2, 1).otherwise(0)).alias("low"))
...       .orderBy("driver_id")
...       .show(truncate=False))
+---------+------------------------+----------------+-----+---------+---+
|driver_id|riders                  |unique_riders   |rated|five_star|low|
+---------+------------------------+----------------+-----+---------+---+
|d1       |[r1, r3, r1, r2, r1, r6]|[r1, r2, r3, r6]|5    |3        |0  |
|d2       |[r2, r2, r2, r7]        |[r2, r7]        |3    |0        |0  |
|d3       |[r1, r5, r6, r1, r5, r1]|[r1, r5, r6]    |6    |4        |1  |
|d4       |[r4, r4, r3, r4]        |[r3, r4]        |3    |1        |1  |
+---------+------------------------+----------------+-----+---------+---+

>>> (trips.groupBy("city")
...       .agg(F.round(F.avg("rating"), 2).alias("avg_rating"), F.count("rating").alias("n"))
...       .orderBy("city")
...       .show())
+--------+----------+---+
|    city|avg_rating|  n|
+--------+----------+---+
| Oakland|       4.0|  6|
|San Jose|      4.09| 11|
+--------+----------+---+
```

**Notes:**

* **`collect_list()`** gathers **all** the values of a group into an array (duplicates
  kept); **`collect_set()`** keeps each value once. These are `groupByKey()`, and the
  `set` combiner from [RDD Set 06, Q5](../rdd_practice_questions/ANSWERS_06.md#q5-intermediate--unique-values-per-key-with-a-set).
  Like `groupByKey()`, they hold a whole group in memory, so use them for **small** groups.
* **The order inside a collected array is not guaranteed.** It depends on how the rows
  arrive from the partitions. `array_sort()` makes it stable, which is why `unique_riders`
  is sorted.
* **Conditional counts**, two ways:
  * `count_if(condition)` counts the rows where the condition is true;
  * `sum(when(condition, 1).otherwise(0))` works in every Spark version, and the same
    pattern with a value instead of `1` gives conditional **sums**.
* **`avg()` and `count(col)` skip `NULL`s**: the averages are over the 6 and 11 rated
  trips, not over 8 and 12 trips (as in Set 03, Q8).

---

## Q5 (Intermediate) — Filtering before and after grouping

```python
>>> ratings = (trips.filter(F.col("rating").isNotNull())
...                 .groupBy("driver_id")
...                 .agg(F.count("rating").alias("n"), F.round(F.avg("rating"), 2).alias("avg")))
>>> ratings.orderBy("driver_id").show()
+---------+---+----+
|driver_id|  n| avg|
+---------+---+----+
|       d1|  5| 4.6|
|       d2|  3|3.33|
|       d3|  6|4.33|
|       d4|  3|3.33|
+---------+---+----+

>>> ratings.filter((F.col("n") >= 3) & (F.col("avg") >= 4.5)).show()
+---------+---+---+
|driver_id|  n|avg|
+---------+---+---+
|       d1|  5|4.6|
+---------+---+---+

>>> (trips.filter("city = 'San Jose'")
...       .groupBy("driver_id")
...       .agg(F.sum("fare").alias("revenue"))
...       .filter("revenue > 80")
...       .orderBy("driver_id")
...       .show())
+---------+-------+
|driver_id|revenue|
+---------+-------+
|       d2|   89.0|
|       d4|  110.0|
+---------+-------+
```

**Notes:**

* **A `filter()` before `groupBy()`** chooses which **rows** go into the groups (SQL's
  `WHERE`). **A `filter()` after the aggregation** chooses which **groups** to keep, using
  the aggregated values (SQL's `HAVING`). The last example uses both: only San Jose trips,
  then only drivers with more than $80 of San Jose revenue.
* This is [RDD Set 10, Q4](../rdd_practice_questions/ANSWERS_10.md#q4-intermediate--driver-ratings)
  in a few lines, with the same answers: `d1` (5 ratings, 4.6) is the only driver with at
  least 3 ratings and an average of 4.5 or more.
* The `isNotNull()` filter isn't needed for `avg()` and `count("rating")`, which skip
  `NULL`s anyway. But it makes the intent clear, and it would matter for `count("*")`.
* **Careful with rounding in filters:** `avg` was rounded before the filter, so a true
  average of 4.496 would show as 4.5 and pass `>= 4.5`. To be exact, filter on the
  **unrounded** value, and round only for display.

---

## Q6 (Intermediate) — Pivot tables

```python
>>> trips.groupBy("driver_id").pivot("city").agg(F.round(F.sum("fare"), 2)).orderBy("driver_id").show()
+---------+-------+--------+
|driver_id|Oakland|San Jose|
+---------+-------+--------+
|       d1|   30.0|   72.25|
|       d2|   NULL|    89.0|
|       d3|   63.0|   36.75|
|       d4|  75.25|   110.0|
+---------+-------+--------+

>>> trips.groupBy("driver_id").pivot("city", ["San Jose", "Oakland"]).count().na.fill(0).orderBy("driver_id").show()
+---------+--------+-------+
|driver_id|San Jose|Oakland|
+---------+--------+-------+
|       d1|       4|      2|
|       d2|       4|      0|
|       d3|       3|      3|
|       d4|       1|      3|
+---------+--------+-------+

>>> period = (F.when(F.col("hour") < 12, "morning")
...            .when(F.col("hour") < 17, "afternoon")
...            .otherwise("evening"))
>>> (trips.withColumn("period", period)
...       .groupBy("city")
...       .pivot("period", ["morning", "afternoon", "evening"])
...       .count()
...       .orderBy("city")
...       .show())
+--------+-------+---------+-------+
|    city|morning|afternoon|evening|
+--------+-------+---------+-------+
| Oakland|      1|        1|      6|
|San Jose|      4|        1|      7|
+--------+-------+---------+-------+
```

**Notes:**

* **`pivot(col)`** turns the **values** of a column into **columns**: one per city here.
  The result is a cross-table, like a spreadsheet pivot table: rows = drivers,
  columns = cities, cells = revenue.
* A missing combination gives `NULL` (`d2` never drove in Oakland). For counts, `na.fill(0)`
  makes that a 0.
* **Give the values explicitly** (`pivot("city", ["San Jose", "Oakland"])`) when you can:
  * Spark doesn't need an extra job to find the distinct values first;
  * **you** choose the column order (here San Jose first, and morning–afternoon–evening in
    time order, not alphabetical);
  * the columns are always the same, even if a value is missing from today's data.
* `period` is a column **expression** stored in a Python variable (Set 02, Q1), so it can
  be reused. Q9 groups by it directly.

---

## Q7 (Intermediate+) — Subtotals with rollup() and cube()

```python
>>> (trips.rollup("city", "driver_id")
...       .agg(F.count("*").alias("trips"), F.round(F.sum("fare"), 2).alias("revenue"))
...       .orderBy(F.col("city").asc_nulls_last(), F.col("driver_id").asc_nulls_last())
...       .show())
+--------+---------+-----+-------+
|    city|driver_id|trips|revenue|
+--------+---------+-----+-------+
| Oakland|       d1|    2|   30.0|
| Oakland|       d3|    3|   63.0|
| Oakland|       d4|    3|  75.25|
| Oakland|     NULL|    8| 168.25|
|San Jose|       d1|    4|  72.25|
|San Jose|       d2|    4|   89.0|
|San Jose|       d3|    3|  36.75|
|San Jose|       d4|    1|  110.0|
|San Jose|     NULL|   12|  308.0|
|    NULL|     NULL|   20| 476.25|
+--------+---------+-----+-------+

>>> trips.rollup("city", "driver_id").count().count(), trips.cube("city", "driver_id").count().count()
(10, 14)

>>> (trips.cube("city", "driver_id")
...       .agg(F.count("*").alias("trips"), F.grouping_id().alias("gid"))
...       .filter("gid = 2")
...       .orderBy("driver_id")
...       .show())
+----+---------+-----+---+
|city|driver_id|trips|gid|
+----+---------+-----+---+
|NULL|       d1|    6|  2|
|NULL|       d2|    4|  2|
|NULL|       d3|    6|  2|
|NULL|       d4|    4|  2|
+----+---------+-----+---+
```

**Notes:**

* **`rollup(a, b)`** gives the normal `(a, b)` groups, **plus a subtotal per `a`**, **plus
  a grand total**: a report with subtotals, in one query. The `NULL` in a subtotal row means
  "all drivers" (or "all cities").
* **`cube(a, b)`** gives **every** combination of subtotals: per `(a, b)`, per `a`, per `b`,
  and the total. Here 7 + 2 + 4 + 1 = **14** rows, versus rollup's 7 + 2 + 1 = **10**.
* **`grouping_id()`** tells you which kind of row it is: a number whose bits are 1 for the
  columns that were "rolled up". With `(city, driver_id)`: `0` = a normal group,
  `1` = a city subtotal, `2` = a driver subtotal, `3` = the grand total. So `gid = 2` keeps
  the **per-driver** totals across both cities.
* **Why `grouping_id()` matters:** if the data itself had `NULL` cities, you couldn't tell a
  real `NULL` city from a "subtotal" `NULL`. `grouping_id()` (or `F.grouping("city")`) can.
* `asc_nulls_last()` puts each subtotal **after** its group's rows, like a printed report.

---

## Q8 (Intermediate+) — Medians, percentiles, and approximate counts

```python
>>> (trips.groupBy("city")
...       .agg(F.round(F.avg("fare"), 2).alias("mean"),
...            F.median("fare").alias("median"),
...            F.percentile_approx("fare", 0.5).alias("approx_median"),
...            F.percentile_approx("fare", [0.25, 0.75]).alias("quartiles"),
...            F.round(F.stddev("fare"), 2).alias("sd"))
...       .orderBy("city")
...       .show(truncate=False))
+--------+-----+------+-------------+-------------+-----+
|city    |mean |median|approx_median|quartiles    |sd   |
+--------+-----+------+-------------+-------------+-----+
|Oakland |21.03|20.625|20.25        |[12.0, 24.5] |8.83 |
|San Jose|25.67|15.5  |15.0         |[10.25, 28.0]|28.03|
+--------+-----+------+-------------+-------------+-----+

>>> trips.agg(F.countDistinct("rider"), F.approx_count_distinct("rider")).show()
+---------------------+----------------------------+
|count(DISTINCT rider)|approx_count_distinct(rider)|
+---------------------+----------------------------+
|                    7|                           7|
+---------------------+----------------------------+
```

**Notes:**

* **Mean vs. median:** San Jose's mean fare (25.67) is far above its median (15.50), and its
  standard deviation is 28: a few expensive trips pull the mean up. That's the $110
  outlier `t13` again ([RDD Set 10, Q6](../rdd_practice_questions/ANSWERS_10.md#q6-intermediate--suspicious-fares)).
  In Oakland, mean and median are close.
* **`median()`** is the exact median: with an even number of values, it averages the two
  middle ones (Oakland: (20.25 + 21.0) / 2 = 20.625).
  **`percentile_approx()`** returns an **actual value** from the data (20.25), close to the
  true percentile. It also takes a **list** of percentiles at once, like the quartiles here.
* **Why approximate?** An exact median needs all the values of a group sorted in one place:
  the `groupByKey()` problem from
  [RDD Set 02, Q5](../rdd_practice_questions/ANSWERS_02.md#q5-intermediate--groupbykey-vs-reducebykey).
  `percentile_approx()` uses a small summary (a "sketch") that can be combined like a
  reducer, so it scales to billions of rows. Its accuracy can be tuned with a third argument.
* **`approx_count_distinct()`** (the HyperLogLog algorithm) estimates the number of distinct
  values using a small fixed amount of memory. On 7 values it's exact; on a billion, it's
  typically within a few percent, and much faster than `countDistinct()`.

---

## Q9 (Intermediate+) — Grouping by computed values, and the average trap

```python
>>> (trips.groupBy("city")
...       .agg(F.round(F.avg(F.col("fare") / F.col("distance_km")), 3).alias("avg_of_ratios"),
...            F.round(F.sum("fare") / F.sum("distance_km"), 3).alias("ratio_of_sums"))
...       .orderBy("city")
...       .show())
+--------+-------------+-------------+
|    city|avg_of_ratios|ratio_of_sums|
+--------+-------------+-------------+
| Oakland|         2.66|        2.592|
|San Jose|        3.014|        3.225|
+--------+-------------+-------------+

>>> (trips.groupBy(period.alias("period"))
...       .agg(F.count("*").alias("trips"),
...            F.round(F.sum("fare"), 2).alias("revenue"),
...            F.round(F.sum("fare") / F.sum("distance_km"), 2).alias("per_km"))
...       .orderBy(F.desc("trips"))
...       .show())
+---------+-----+-------+------+
|   period|trips|revenue|per_km|
+---------+-----+-------+------+
|  evening|   13|  241.0|  2.62|
|  morning|    5|  178.0|  3.89|
|afternoon|    2|  57.25|  2.51|
+---------+-----+-------+------+

>>> (trips.groupBy((F.floor(F.col("fare") / 10) * 10).cast("int").alias("bucket"))
...       .count()
...       .orderBy("bucket")
...       .show())
+------+-----+
|bucket|count|
+------+-----+
|     0|    3|
|    10|    8|
|    20|    5|
|    30|    3|
|   110|    1|
+------+-----+
```

**Notes:**

* **Two different "average fare per km"s:**
  * **average of ratios**: compute fare/km for each trip, then average them. Every trip
    counts **equally**, whether it's 2 km or 20 km;
  * **ratio of sums**: total fare ÷ total km. Every **kilometer** counts equally. That's
    what a business usually means by "revenue per km", and it's the city rate used in
    [RDD Set 10, Q6](../rdd_practice_questions/ANSWERS_10.md#q6-intermediate--suspicious-fares)
    (2.592 and 3.225).

  They can differ a lot, and they can even rank the cities differently. Decide which
  question you're answering. This is the same family of trap as "the average of averages"
  in [RDD Set 04, Q9](../rdd_practice_questions/ANSWERS_04.md#q9-intermediate--when-reducebykey-gives-the-wrong-answer).
* **Group by an expression:** `groupBy()` accepts computed columns, not just column names.
  Here it groups by the time-of-day `period` from Q6, and by a **fare bucket**: `floor(fare /
  10) * 10` puts each fare into its $10 band. That's a **histogram**, the DataFrame version
  of [RDD Set 06, Q6](../rdd_practice_questions/ANSWERS_06.md#q6-intermediate--a-rating-histogram-per-movie).
* The histogram shows the outlier at a glance: everything is under $40 except one trip
  in the $110 bucket.
* The morning trips earn the most per km (3.89), partly because of that outlier: `t13` is
  a morning trip (8 a.m.).
