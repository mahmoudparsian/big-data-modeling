# PySpark DataFrame Practice Questions — Set 06: Answers

Solutions to the nine [Set 06 practice questions](QUESTIONS_06.md). Every session below
was run in the `bin/pyspark` shell (Spark 4.2.0), started from this folder. The output is
copied from those real runs.

Try the questions yourself first. There are often several correct solutions, and yours
doesn't need to match these exactly.

* [Q1 (Basic): Numbering rows within each group](#q1-basic--numbering-rows-within-each-group)
* [Q2 (Basic): row_number(), rank(), and dense_rank()](#q2-basic--row_number-rank-and-dense_rank)
* [Q3 (Basic): Group totals on every row](#q3-basic--group-totals-on-every-row)
* [Q4 (Intermediate): The top 2 drivers in each city](#q4-intermediate--the-top-2-drivers-in-each-city)
* [Q5 (Intermediate): Keeping the first payment per trip](#q5-intermediate--keeping-the-first-payment-per-trip)
* [Q6 (Intermediate): The previous and next trip, with lag() and lead()](#q6-intermediate--the-previous-and-next-trip-with-lag-and-lead)
* [Q7 (Intermediate+): Running totals, moving averages, and window frames](#q7-intermediate--running-totals-moving-averages-and-window-frames)
* [Q8 (Intermediate+): Quartiles and percentiles per group](#q8-intermediate--quartiles-and-percentiles-per-group)
* [Q9 (Intermediate+): The longest 5-star streak](#q9-intermediate--the-longest-5-star-streak)

Setup, used by all the questions:

```python
>>> from pyspark.sql import functions as F
>>> from pyspark.sql import Window as W
>>> trips_ddl = ("trip_id STRING, driver_id STRING, rider STRING, city STRING, "
...              "hour INT, distance_km DOUBLE, fare DOUBLE, rating INT")
>>> trips = spark.read.csv("data/trips.csv", header=True, schema=trips_ddl)
>>> payments = (spark.read.csv("data/payments.csv", header=True)
...                  .withColumn("paid_at", F.to_timestamp("paid_at"))
...                  .withColumn("amount", F.regexp_replace("amount", r"\$", "").cast("double")))
>>> rides = trips.join(payments.select("trip_id", "paid_at"), "trip_id")
```

`rides` is the trips with the time each one was paid, so that trips can be put in **time order**.

---

## Q1 (Basic) — Numbering rows within each group

```python
>>> by_fare = W.partitionBy("city").orderBy(F.desc("fare"))
>>> (trips.select("city", "trip_id", "fare", F.row_number().over(by_fare).alias("rn"))
...       .orderBy("city", "rn")
...       .show(6))
+-------+-------+-----+---+
|   city|trip_id| fare| rn|
+-------+-------+-----+---+
|Oakland|    t05|38.25|  1|
|Oakland|    t17| 25.0|  2|
|Oakland|    t08| 24.5|  3|
|Oakland|    t11| 21.0|  4|
|Oakland|    t18|20.25|  5|
|Oakland|    t15| 17.5|  6|
+-------+-------+-----+---+
only showing top 6 rows

>>> (trips.withColumn("rn", F.row_number().over(by_fare))
...       .filter("rn <= 2")
...       .select("city", "trip_id", "fare", "rn")
...       .orderBy("city", "rn")
...       .show())
+--------+-------+-----+---+
|    city|trip_id| fare| rn|
+--------+-------+-----+---+
| Oakland|    t05|38.25|  1|
| Oakland|    t17| 25.0|  2|
|San Jose|    t13|110.0|  1|
|San Jose|    t20| 36.0|  2|
+--------+-------+-----+---+
```

**Notes:**

* **A window function** computes a value for **each row** from a "window" of related rows,
  **without** collapsing them into one row per group, which is the big difference from
  `groupBy()`.
* **The window specification** says which rows and in what order:
  * `W.partitionBy("city")`: each city is its own window (this is not the same as RDD
    partitions; it means "per group");
  * `.orderBy(F.desc("fare"))`: the order of the rows **inside** each window.
* **`row_number()`** numbers the rows 1, 2, 3, ... in that order, **restarting** in every city.
* **Top N per group = `row_number()` + `filter(rn <= N)`.** The RDD sets needed
  `groupByKey()` with a `sorted()[:2]` or a bounded combiner for this
  ([RDD Set 02, Q9](../rdd_practice_questions/ANSWERS_02.md#q9-intermediate--top-2-students-per-class)).
* A window function must be used in `select()` or `withColumn()`. To filter on its result,
  add it as a column first, as here.

---

## Q2 (Basic) — row_number(), rank(), and dense_rank()

```python
>>> by_rating = W.partitionBy("city").orderBy(F.desc_nulls_last("rating"))
>>> by_rating_then_id = W.partitionBy("city").orderBy(F.desc_nulls_last("rating"), "trip_id")
>>> (trips.filter("city = 'Oakland'")
...       .select("trip_id", "rating",
...               F.row_number().over(by_rating_then_id).alias("row_number"),
...               F.rank().over(by_rating).alias("rank"),
...               F.dense_rank().over(by_rating).alias("dense_rank"))
...       .orderBy("row_number")
...       .show())
+-------+------+----------+----+----------+
|trip_id|rating|row_number|rank|dense_rank|
+-------+------+----------+----+----------+
|    t08|     5|         1|   1|         1|
|    t17|     5|         2|   1|         1|
|    t05|     4|         3|   3|         2|
|    t15|     4|         4|   3|         2|
|    t18|     4|         5|   3|         2|
|    t11|     2|         6|   6|         3|
|    t03|  NULL|         7|   7|         4|
|    t09|  NULL|         8|   7|         4|
+-------+------+----------+----+----------+
```

**Notes:**

* **The three functions differ only on ties:**
  * **`row_number()`**: always 1, 2, 3, ..., even for ties. Which tied row gets which number
    is **arbitrary** unless the order has a tie-breaker (here `trip_id`);
  * **`rank()`**: ties share a rank, and the next rank **skips** (1, 1, 3, ...), like
    sports results: two winners, and then third place;
  * **`dense_rank()`**: ties share a rank, with **no gaps** (1, 1, 2, ...).
* **Which one to use:** `row_number()` when you need **exactly** N rows per group (Q4, Q5);
  `rank()` or `dense_rank()` when tied rows should be treated **equally**, for example
  "every trip with the best rating".
* `desc_nulls_last()` keeps the unrated trips at the **end** of the ranking. They tie
  with each other.

---

## Q3 (Basic) — Group totals on every row

```python
>>> per_city = W.partitionBy("city")
>>> (trips.select("city", "trip_id", "fare",
...               F.round(F.sum("fare").over(per_city), 2).alias("city_total"),
...               F.round(F.col("fare") / F.sum("fare").over(per_city) * 100, 1).alias("pct_of_city"),
...               F.round(F.col("fare") - F.avg("fare").over(per_city), 2).alias("vs_city_avg"))
...       .orderBy("city", "trip_id")
...       .show(5))
+-------+-------+-----+----------+-----------+-----------+
|   city|trip_id| fare|city_total|pct_of_city|vs_city_avg|
+-------+-------+-----+----------+-----------+-----------+
|Oakland|    t03| 9.75|    168.25|        5.8|     -11.28|
|Oakland|    t05|38.25|    168.25|       22.7|      17.22|
|Oakland|    t08| 24.5|    168.25|       14.6|       3.47|
|Oakland|    t09| 12.0|    168.25|        7.1|      -9.03|
|Oakland|    t11| 21.0|    168.25|       12.5|      -0.03|
+-------+-------+-----+----------+-----------+-----------+
only showing top 5 rows

>>> per_driver = W.partitionBy("driver_id")
>>> (trips.select("driver_id", "trip_id", "fare",
...               F.count("*").over(per_driver).alias("driver_trips"),
...               F.max("fare").over(per_driver).alias("driver_max"))
...       .filter("driver_id = 'd2'")
...       .orderBy("trip_id")
...       .show())
+---------+-------+----+------------+----------+
|driver_id|trip_id|fare|driver_trips|driver_max|
+---------+-------+----+------------+----------+
|       d2|    t02|30.0|           4|      36.0|
|       d2|    t06| 8.0|           4|      36.0|
|       d2|    t16|15.0|           4|      36.0|
|       d2|    t20|36.0|           4|      36.0|
+---------+-------+----+------------+----------+
```

**Notes:**

* **Any aggregate function** (`sum`, `avg`, `count`, `max`, ...) can be used **over a
  window**. With only `partitionBy()` (no `orderBy()`), the window is the **whole group**,
  so every row of a city gets its city's total.
* That makes "each row **compared with** its group" easy: the share of the city's revenue,
  or the difference from the city's average. Without windows, you'd need a `groupBy()` and
  then a `join()` back to the trips.
* The rows are **not** collapsed: all 4 of `d2`'s trips are still there, each with `d2`'s
  totals next to it.

---

## Q4 (Intermediate) — The top 2 drivers in each city

```python
>>> revenue = trips.groupBy("city", "driver_id").agg(F.round(F.sum("fare"), 2).alias("revenue"))
>>> in_city = W.partitionBy("city").orderBy(F.desc("revenue"), "driver_id")
>>> top2 = (revenue
...         .withColumn("rank", F.row_number().over(in_city))
...         .withColumn("share", F.round(F.col("revenue") / F.sum("revenue").over(W.partitionBy("city")) * 100))
...         .filter("rank <= 2"))
>>> top2.orderBy("city", "rank").show()
+--------+---------+-------+----+-----+
|    city|driver_id|revenue|rank|share|
+--------+---------+-------+----+-----+
| Oakland|       d4|  75.25|   1| 45.0|
| Oakland|       d3|   63.0|   2| 37.0|
|San Jose|       d4|  110.0|   1| 36.0|
|San Jose|       d2|   89.0|   2| 29.0|
+--------+---------+-------+----+-----+
```

**Notes:**

* **First aggregate, then rank:** `groupBy()` makes one row per (city, driver), and a
  window over **those** rows ranks the drivers within each city.
* The **share** is computed **before** the filter, over **all** the drivers of the city. If
  you filtered first, the shares would only be of the top 2's revenue.
* This is [RDD Set 10, Q9](../rdd_practice_questions/ANSWERS_10.md#q9-intermediate--the-top-2-drivers-in-each-city),
  with the same answers (45%, 37%, 36%, 29%), but without writing a bounded top-2 combiner or
  broadcasting the city totals. Spark handles both.
* `driver_id` as the second sort key makes the ranking stable if two drivers ever earn
  exactly the same amount.

---

## Q5 (Intermediate) — Keeping the first payment per trip

```python
>>> dup = (spark.createDataFrame([
...     ("p01", "t01", "2026-10-05 08:00:00", "card", 17.40),
...     ("p02", "t02", "2026-10-06 09:13:00", "app", 33.00),
...     ("p02b", "t02", "2026-10-06 09:20:00", "app", 33.00),
...     ("p03", "t03", "2026-10-07 09:26:00", "cash", 9.75),
...     ("p03x", "t03", "2026-10-07 09:25:00", "cash", 9.75)],
...     "payment_id STRING, trip_id STRING, paid_at STRING, method STRING, amount DOUBLE")
...     .withColumn("paid_at", F.to_timestamp("paid_at")))
>>> first_first = W.partitionBy("trip_id").orderBy("paid_at", "payment_id")
>>> flagged = (dup.withColumn("n", F.row_number().over(first_first))
...               .withColumn("payments_for_trip", F.count("*").over(W.partitionBy("trip_id"))))
>>> flagged.orderBy("trip_id", "n").show()
+----------+-------+-------------------+------+------+---+-----------------+
|payment_id|trip_id|            paid_at|method|amount|  n|payments_for_trip|
+----------+-------+-------------------+------+------+---+-----------------+
|       p01|    t01|2026-10-05 08:00:00|  card|  17.4|  1|                1|
|       p02|    t02|2026-10-06 09:13:00|   app|  33.0|  1|                2|
|      p02b|    t02|2026-10-06 09:20:00|   app|  33.0|  2|                2|
|      p03x|    t03|2026-10-07 09:25:00|  cash|  9.75|  1|                2|
|       p03|    t03|2026-10-07 09:26:00|  cash|  9.75|  2|                2|
+----------+-------+-------------------+------+------+---+-----------------+

>>> flagged.filter("n = 1").drop("n", "payments_for_trip").orderBy("trip_id").show()
+----------+-------+-------------------+------+------+
|payment_id|trip_id|            paid_at|method|amount|
+----------+-------+-------------------+------+------+
|       p01|    t01|2026-10-05 08:00:00|  card|  17.4|
|       p02|    t02|2026-10-06 09:13:00|   app|  33.0|
|      p03x|    t03|2026-10-07 09:25:00|  cash|  9.75|
+----------+-------+-------------------+------+------+

>>> flagged.filter("n > 1").select("payment_id", "trip_id", "paid_at").orderBy("payment_id").show()
+----------+-------+-------------------+
|payment_id|trip_id|            paid_at|
+----------+-------+-------------------+
|      p02b|    t02|2026-10-06 09:20:00|
|       p03|    t03|2026-10-07 09:26:00|
+----------+-------+-------------------+
```

**Notes:**

* This is the **proper** fix for the double payments of
  [Set 03, Q6](ANSWERS_03.md#q6-intermediate--duplicate-payments). `dropDuplicates(["trip_id"])`
  kept an **arbitrary** payment, but `row_number()` over a window **you** order keeps a
  **chosen** one: here the **earliest** payment of each trip.
* **Choose the order on purpose.** For `t03`, the earliest payment is `p03x` (09:25), not
  `p03`. Ordering by `payment_id` would have kept the wrong one. `payment_id` is only the
  **tie-breaker**, for two payments at exactly the same time.
* **Don't just drop the extras, report them.** `n > 1` lists the later payments (possible
  double charges to refund), and `payments_for_trip` on every row shows which trips are affected.
* "Keep the **latest** row per key" is the same, with `orderBy(F.desc("paid_at"))`. That's
  a very common way to keep only the newest version of each record.

---

## Q6 (Intermediate) — The previous and next trip, with lag() and lead()

```python
>>> by_time = W.partitionBy("rider").orderBy("paid_at")
>>> history = (rides.select("rider", "trip_id", "paid_at", "fare",
...                         F.lag("paid_at").over(by_time).alias("prev_paid_at"),
...                         F.lag("fare").over(by_time).alias("prev_fare"),
...                         F.lead("trip_id").over(by_time).alias("next_trip"))
...                 .withColumn("hours_since_prev",
...                             F.round((F.unix_timestamp("paid_at") - F.unix_timestamp("prev_paid_at")) / 3600, 1)))
>>> (history.filter("rider = 'r1'")
...         .select("trip_id", "paid_at", "fare", "prev_fare", "hours_since_prev", "next_trip")
...         .orderBy("paid_at")
...         .show(truncate=False))
+-------+-------------------+-----+---------+----------------+---------+
|trip_id|paid_at            |fare |prev_fare|hours_since_prev|next_trip|
+-------+-------------------+-----+---------+----------------+---------+
|t01    |2026-10-05 08:00:00|14.5 |NULL     |NULL            |t04      |
|t04    |2026-10-08 12:39:00|19.0 |14.5     |76.7            |t19      |
|t19    |2026-10-09 20:54:00|7.5  |19.0     |32.3            |t12      |
|t12    |2026-10-09 21:23:00|10.25|7.5      |0.5             |t14      |
|t14    |2026-10-11 09:49:00|13.75|10.25    |36.4            |t07      |
|t07    |2026-10-11 18:18:00|16.0 |13.75    |8.5             |NULL     |
+-------+-------------------+-----+---------+----------------+---------+

>>> (history.groupBy("rider")
...         .agg(F.round(F.avg("hours_since_prev"), 1).alias("avg_gap_h"), F.count("*").alias("trips"))
...         .orderBy("rider")
...         .show())
+-----+---------+-----+
|rider|avg_gap_h|trips|
+-----+---------+-----+
|   r1|     30.9|    6|
|   r2|     34.6|    4|
|   r3|     71.2|    2|
|   r4|     33.1|    3|
|   r5|      1.5|    2|
|   r6|      0.5|    2|
|   r7|     NULL|    1|
+-----+---------+-----+
```

**Notes:**

* **`lag(col)`** takes the value from the **previous** row of the window, and **`lead(col)`**
  from the **next** one. "Previous" and "next" follow the window's `orderBy()`, here each
  rider's trips in **time** order (not `trip_id` order: `t19` comes before `t12`).
* The first row of each window has no previous row, so its `lag()` is `NULL`; the last row's
  `lead()` is `NULL` too. Both functions accept a default value, and a distance other than 1:
  `F.lag("fare", 2, 0.0)`.
* **Time between events:** `unix_timestamp()` turns a timestamp into seconds, so subtracting
  two of them and dividing by 3600 gives hours. Maya (`r1`) took two rides only **half an
  hour** apart on October 9.
* `avg()` skips the `NULL` gap of each rider's first trip, and `r7`, with a single trip, has no
  gap at all (`NULL`).
* This "compare with the previous row" pattern answers many questions: the change in price
  since the last order, the time since the last login, a session gap... It's very hard to do
  with RDDs, where rows have no order inside a group unless you sort a `groupByKey()` list
  yourself.

---

## Q7 (Intermediate+) — Running totals, moving averages, and window frames

```python
>>> daily = (rides.groupBy(F.to_date("paid_at").alias("day"))
...               .agg(F.count("*").alias("trips"), F.round(F.sum("fare"), 2).alias("revenue")))
>>> running = W.orderBy("day").rowsBetween(W.unboundedPreceding, W.currentRow)
>>> last_3 = W.orderBy("day").rowsBetween(-2, W.currentRow)
>>> (daily.select("day", "trips", "revenue",
...               F.round(F.sum("revenue").over(running), 2).alias("running"),
...               F.round(F.avg("revenue").over(last_3), 2).alias("avg_3d"))
...       .orderBy("day")
...       .show())
+----------+-----+-------+-------+------+
|       day|trips|revenue|running|avg_3d|
+----------+-----+-------+-------+------+
|2026-10-05|    3|   56.5|   56.5|  56.5|
|2026-10-06|    3|   57.0|  113.5| 56.75|
|2026-10-07|    3|  62.75| 176.25| 58.75|
|2026-10-08|    3|  60.25|  236.5|  60.0|
|2026-10-09|    3|   56.0|  292.5| 59.67|
|2026-10-10|    3|  154.0|  446.5| 90.08|
|2026-10-11|    2|  29.75| 476.25| 79.92|
+----------+-----+-------+-------+------+

>>> default_frame = W.orderBy("rating")
>>> rows_frame = W.orderBy("rating", "trip_id").rowsBetween(W.unboundedPreceding, W.currentRow)
>>> (trips.filter("rating IS NOT NULL")
...       .select("trip_id", "rating", "fare",
...               F.sum("fare").over(default_frame).alias("default_frame"),
...               F.sum("fare").over(rows_frame).alias("rows_frame"))
...       .orderBy("rating", "trip_id")
...       .show(7))
+-------+------+-----+-------------+----------+
|trip_id|rating| fare|default_frame|rows_frame|
+-------+------+-----+-------------+----------+
|    t13|     1|110.0|        110.0|     110.0|
|    t11|     2| 21.0|        131.0|     131.0|
|    t06|     3|  8.0|        175.0|     139.0|
|    t20|     3| 36.0|        175.0|     175.0|
|    t02|     4| 30.0|        309.0|     205.0|
|    t05|     4|38.25|        309.0|    243.25|
|    t10|     4| 28.0|        309.0|    271.25|
+-------+------+-----+-------------+----------+
only showing top 7 rows
```

**Notes:**

* **A window frame** chooses **which rows** of the window go into each row's calculation:
  * `rowsBetween(W.unboundedPreceding, W.currentRow)`: from the first row up to this one,
    a **running total**;
  * `rowsBetween(-2, W.currentRow)`: the 2 rows before and this one, a **3-day moving
    average**, which smooths out a single unusual day. The first two days have fewer rows to
    average.
* The revenue jumps on October 10, the day of the $110 trip `t13`. The moving average shows
  how much one outlier moves a trend.
* **The default-frame trap:** with `orderBy()` but no frame, Spark uses **`RANGE` between the
  first row and the current row**, where "range" means "all rows with the same `orderBy`
  value **count as one**". So `t06` and `t20` (both rated 3) both get **175.0**, the total
  **including each other**. With an explicit `rowsBetween(...)` frame and a tie-breaker, each
  row adds one trip at a time. A running total that "jumps" or repeats a value is the
  symptom. **Always give the frame explicitly** for running totals.
* `W.orderBy()` **without** `partitionBy()` puts **all** the rows in one window, so Spark
  moves everything to **one partition**, and it warns you: `WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.` That's fine
  for 7 days of totals, but not for a billion rows. Partition by something whenever you can.

---

## Q8 (Intermediate+) — Quartiles and percentiles per group

```python
>>> by_fare_in_city = W.partitionBy("city").orderBy("fare")
>>> (trips.select("city", "trip_id", "fare",
...               F.ntile(4).over(by_fare_in_city).alias("quartile"),
...               F.round(F.percent_rank().over(by_fare_in_city), 2).alias("pct_rank"),
...               F.round(F.cume_dist().over(by_fare_in_city), 2).alias("cume"))
...       .filter("city = 'Oakland'")
...       .orderBy("fare")
...       .show())
+-------+-------+-----+--------+--------+----+
|   city|trip_id| fare|quartile|pct_rank|cume|
+-------+-------+-----+--------+--------+----+
|Oakland|    t03| 9.75|       1|     0.0|0.13|
|Oakland|    t09| 12.0|       1|    0.14|0.25|
|Oakland|    t15| 17.5|       2|    0.29|0.38|
|Oakland|    t18|20.25|       2|    0.43| 0.5|
|Oakland|    t11| 21.0|       3|    0.57|0.63|
|Oakland|    t08| 24.5|       3|    0.71|0.75|
|Oakland|    t17| 25.0|       4|    0.86|0.88|
|Oakland|    t05|38.25|       4|     1.0| 1.0|
+-------+-------+-----+--------+--------+----+

>>> (trips.withColumn("q", F.ntile(4).over(by_fare_in_city))
...       .groupBy("city", "q")
...       .agg(F.count("*").alias("n"), F.min("fare").alias("from"), F.max("fare").alias("to"))
...       .orderBy("city", "q")
...       .show())
+--------+---+---+-----+-----+
|    city|  q|  n| from|   to|
+--------+---+---+-----+-----+
| Oakland|  1|  2| 9.75| 12.0|
| Oakland|  2|  2| 17.5|20.25|
| Oakland|  3|  2| 21.0| 24.5|
| Oakland|  4|  2| 25.0|38.25|
|San Jose|  1|  3|  7.5|10.25|
|San Jose|  2|  3|13.75| 15.0|
|San Jose|  3|  3| 16.0| 28.0|
|San Jose|  4|  3| 30.0|110.0|
+--------+---+---+-----+-----+
```

**Notes:**

* **`ntile(4)`** splits each window into 4 groups of (nearly) equal **size**: quartile 1 is
  the cheapest quarter of each city's trips. `ntile(10)` gives deciles, and so on.
* **`percent_rank()`** = (rank − 1) / (rows − 1), from 0 (the lowest) to 1 (the highest);
  **`cume_dist()`** = the fraction of rows **≤** this one, from 1/n to 1.
* Each **city** is ranked separately: a $25 trip is in Oakland's top quartile, but it would be
  in San Jose's third.
* The second query **combines** a window and a `groupBy()`: label each trip with its
  quartile, then summarize each quartile. San Jose's top quartile spans from $30 to $110, the
  outlier again.
* These are **ranks** within the data, not values. For "what fare is the 75th percentile?",
  use `percentile_approx()` ([Set 04, Q8](ANSWERS_04.md#q8-intermediate--medians-percentiles-and-approximate-counts)).

---

## Q9 (Intermediate+) — The longest 5-star streak

```python
>>> by_driver_time = W.partitionBy("driver_id").orderBy("paid_at")
>>> streaks = (rides
...     .withColumn("five", F.coalesce(F.col("rating") == 5, F.lit(False)))
...     .withColumn("rn", F.row_number().over(by_driver_time))
...     .withColumn("rn_same", F.row_number().over(W.partitionBy("driver_id", "five").orderBy("paid_at")))
...     .withColumn("grp", F.col("rn") - F.col("rn_same")))
>>> (streaks.filter("driver_id = 'd3'")
...         .select("trip_id", "paid_at", "rating", "five", "rn", "rn_same", "grp")
...         .orderBy("paid_at")
...         .show(truncate=False))
+-------+-------------------+------+-----+---+-------+---+
|trip_id|paid_at            |rating|five |rn |rn_same|grp|
+-------+-------------------+------+-----+---+-------+---+
|t15    |2026-10-05 17:02:00|4     |false|1  |1      |0  |
|t08    |2026-10-05 18:31:00|5     |true |2  |1      |1  |
|t04    |2026-10-08 12:39:00|5     |true |3  |2      |1  |
|t11    |2026-10-08 20:10:00|2     |false|4  |2      |2  |
|t19    |2026-10-09 20:54:00|5     |true |5  |3      |2  |
|t12    |2026-10-09 21:23:00|5     |true |6  |4      |2  |
+-------+-------------------+------+-----+---+-------+---+

>>> runs = (streaks.filter("five")
...                .groupBy("driver_id", "grp")
...                .agg(F.count("*").alias("streak"), F.min("paid_at").alias("start")))
>>> (runs.groupBy("driver_id")
...      .agg(F.max("streak").alias("longest_5_star_streak"))
...      .join(trips.select("driver_id").distinct(), "driver_id", "right")
...      .na.fill(0)
...      .orderBy("driver_id")
...      .show())
+---------+---------------------+
|driver_id|longest_5_star_streak|
+---------+---------------------+
|       d1|                    2|
|       d2|                    0|
|       d3|                    2|
|       d4|                    1|
+---------+---------------------+
```

**Notes:**

* **The problem:** a **streak** is a run of **consecutive** 5-star trips, in time order.
  `groupBy()` can count all of a driver's 5-star trips (4 for `d3`), but it can't see which
  ones are **next to each other**.
* **The "gaps and islands" trick:** number each driver's trips in time order (`rn`), and
  number them again **separately** for the 5-star and the other trips (`rn_same`). Inside a
  streak, both numbers go up by 1 at each step, so their **difference** (`grp`) stays the
  **same**. When the streak is broken, `rn` jumps ahead of `rn_same`, and `grp` changes. So
  every streak gets its own `grp` value: an "island".
  * For `d3`: the 5-star trips `t08` and `t04` have `grp` 1 (one streak of 2), and `t19` and
    `t12` have `grp` 2 (another streak of 2). `t11` (rated 2) breaks them apart.
* Then a normal `groupBy("driver_id", "grp")` measures each island, and `max()` finds the
  longest one per driver.
* **`coalesce(rating == 5, False)`** treats an unrated trip as "not 5 stars", so it breaks a
  streak. Without it, `rating == 5` would be `NULL`, and those trips would form their own
  `NULL` group.
* **The `right` join with all drivers** brings back `d2`, who has **no** 5-star trips and so no
  streak at all, as `0`. A report should list every driver, not only those with a result.
* The same trick finds runs of consecutive **days** a rider was active, or periods when a
  sensor stayed above a limit: problems that are very hard to solve without window functions.
