# PySpark DataFrame Practice Questions — Set 06: Window Functions

Nine practice questions, three at each level, meant to be solved **interactively** in
the `bin/pyspark` shell. This set is about **window functions**: calculations over a group
of related rows that, unlike `groupBy()`, **keep every row**. They answer questions such
as "the top 2 per group", "the first record per key", "compared with the previous row",
and "the running total".

A window function has two parts:

```python
from pyspark.sql import Window as W

w = W.partitionBy("city").orderBy(F.desc("fare"))     # which rows, and in what order
trips.withColumn("rn", F.row_number().over(w))        # the function, applied "over" the window
```

| # | Level | Topic | Input |
|---|---|---|---|
| [Q1](#q1-basic--numbering-rows-within-each-group) | Basic | Numbering rows within each group | [`trips.csv`](data/trips.csv) |
| [Q2](#q2-basic--row_number-rank-and-dense_rank) | Basic | `row_number()`, `rank()`, and `dense_rank()` | [`trips.csv`](data/trips.csv) |
| [Q3](#q3-basic--group-totals-on-every-row) | Basic | Group totals on every row | [`trips.csv`](data/trips.csv) |
| [Q4](#q4-intermediate--the-top-2-drivers-in-each-city) | Intermediate | The top 2 drivers in each city | [`trips.csv`](data/trips.csv) |
| [Q5](#q5-intermediate--keeping-the-first-payment-per-trip) | Intermediate | Keeping the first payment per trip | Python list |
| [Q6](#q6-intermediate--the-previous-and-next-trip-with-lag-and-lead) | Intermediate | The previous and next trip, with `lag()` and `lead()` | [`trips.csv`](data/trips.csv), [`payments.csv`](data/payments.csv) |
| [Q7](#q7-intermediate--running-totals-moving-averages-and-window-frames) | Intermediate+ | Running totals, moving averages, and window frames | [`trips.csv`](data/trips.csv), [`payments.csv`](data/payments.csv) |
| [Q8](#q8-intermediate--quartiles-and-percentiles-per-group) | Intermediate+ | Quartiles and percentiles per group | [`trips.csv`](data/trips.csv) |
| [Q9](#q9-intermediate--the-longest-5-star-streak) | Intermediate+ | The longest 5-star streak | [`trips.csv`](data/trips.csv), [`payments.csv`](data/payments.csv) |

The operations for this set:

* **Window specifications:** `W.partitionBy(...)`, `.orderBy(...)`,
  `.rowsBetween(start, end)`, `W.unboundedPreceding`, `W.currentRow`
* **Ranking functions:** `row_number()`, `rank()`, `dense_rank()`, `ntile(n)`,
  `percent_rank()`, `cume_dist()`
* **Offset functions:** `lag(col)`, `lead(col)`
* **Aggregates over a window:** `sum`, `avg`, `count`, `max`, ... followed by `.over(w)`

You may also use everything from Sets [01](QUESTIONS_01.md)–[05](QUESTIONS_05.md).

Try each question on your own before you open the [answers](ANSWERS_06.md).

## Files

| File | Purpose |
|---|---|
| [QUESTIONS_06.md](QUESTIONS_06.md) | The nine questions (this page) |
| [ANSWERS_06.md](ANSWERS_06.md) | Step-by-step solutions, with real output from Spark 4.2.0 |
| [data/trips.csv](data/trips.csv) | The 20 trips |
| [data/payments.csv](data/payments.csv) | One payment per trip, with the time it was paid (`paid_at`) |

## Getting started

Start `$SPARK_HOME/bin/pyspark` **from this folder**, as in
[Set 01](QUESTIONS_01.md#getting-started), do the questions **in order**, and start
with this setup:

```python
from pyspark.sql import functions as F
from pyspark.sql import Window as W

trips_ddl = ("trip_id STRING, driver_id STRING, rider STRING, city STRING, "
             "hour INT, distance_km DOUBLE, fare DOUBLE, rating INT")
trips = spark.read.csv("data/trips.csv", header=True, schema=trips_ddl)
payments = (spark.read.csv("data/payments.csv", header=True)
                 .withColumn("paid_at", F.to_timestamp("paid_at"))
                 .withColumn("amount", F.regexp_replace("amount", r"\$", "").cast("double")))
rides = trips.join(payments.select("trip_id", "paid_at"), "trip_id")
```

`rides` is the trips together with the time each one was paid, so that a rider's or a
driver's trips can be put in **time order**.

---

## Q1 (Basic) — Numbering rows within each group

**Tasks:**

1. Number the trips **within each city**, from the most expensive (1) to the cheapest, with
   `row_number()`. Show `city`, `trip_id`, `fare`, and the number (`rn`) for the first 6
   rows, sorted by city and `rn`.
2. Keep only the **2 most expensive trips of each city**.

**Expected output:**

```
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

+--------+-------+-----+---+
|    city|trip_id| fare| rn|
+--------+-------+-----+---+
| Oakland|    t05|38.25|  1|
| Oakland|    t17| 25.0|  2|
|San Jose|    t13|110.0|  1|
|San Jose|    t20| 36.0|  2|
+--------+-------+-----+---+
```

---

## Q2 (Basic) — row_number(), rank(), and dense_rank()

**Task:** for the **Oakland** trips, show `trip_id`, `rating`, and three rankings by rating
(highest first, unrated trips last):

* `row_number`: with `trip_id` as a tie-breaker;
* `rank` and `dense_rank`: without a tie-breaker.

Sort by `row_number`. How do the three functions treat ties?

**Expected output:**

```
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

---

## Q3 (Basic) — Group totals on every row

**Tasks:**

1. For every trip, show `city`, `trip_id`, `fare`, and:
   * `city_total`: the total fare of the trip's city (rounded to 2 decimals);
   * `pct_of_city`: the trip's share of that total, as a percentage (rounded to 1 decimal);
   * `vs_city_avg`: the fare minus the city's average fare (rounded to 2 decimals).

   Show 5 rows, sorted by city and trip.
2. For driver `d2`'s trips, show `driver_id`, `trip_id`, `fare`, the driver's number of
   trips (`driver_trips`), and the driver's highest fare (`driver_max`).

**Expected output:**

```
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

+---------+-------+----+------------+----------+
|driver_id|trip_id|fare|driver_trips|driver_max|
+---------+-------+----+------------+----------+
|       d2|    t02|30.0|           4|      36.0|
|       d2|    t06| 8.0|           4|      36.0|
|       d2|    t16|15.0|           4|      36.0|
|       d2|    t20|36.0|           4|      36.0|
+---------+-------+----+------------+----------+
```

**Hint:** a window with only `partitionBy()`, and no `orderBy()`, is the whole group.

---

## Q4 (Intermediate) — The top 2 drivers in each city

**Task:** compute each driver's revenue **per city** (rounded to 2 decimals). Then keep the
**top 2 drivers** of each city, with their `rank` (1 or 2) and their `share` of the city's
revenue (a whole percentage, as a `double`, of **all** the city's drivers).

**Expected output:**

```
+--------+---------+-------+----+-----+
|    city|driver_id|revenue|rank|share|
+--------+---------+-------+----+-----+
| Oakland|       d4|  75.25|   1| 45.0|
| Oakland|       d3|   63.0|   2| 37.0|
|San Jose|       d4|  110.0|   1| 36.0|
|San Jose|       d2|   89.0|   2| 29.0|
+--------+---------+-------+----+-----+
```

*Think about it:* compare with [RDD Set 10, Q9](../rdd_practice_questions/QUESTIONS_10.md#q9-intermediate--the-top-2-drivers-in-each-city).
Why must the share be computed **before** you keep only the top 2?

---

## Q5 (Intermediate) — Keeping the first payment per trip

**Input:** payments where two trips were paid **twice**:

```python
dup = (spark.createDataFrame([
    ("p01", "t01", "2026-10-05 08:00:00", "card", 17.40),
    ("p02", "t02", "2026-10-06 09:13:00", "app", 33.00),
    ("p02b", "t02", "2026-10-06 09:20:00", "app", 33.00),
    ("p03", "t03", "2026-10-07 09:26:00", "cash", 9.75),
    ("p03x", "t03", "2026-10-07 09:25:00", "cash", 9.75)],
    "payment_id STRING, trip_id STRING, paid_at STRING, method STRING, amount DOUBLE")
    .withColumn("paid_at", F.to_timestamp("paid_at")))
```

**Tasks:**

1. Number each trip's payments in **time order** (`n`), with `payment_id` as a tie-breaker,
   and add each trip's number of payments (`payments_for_trip`). Show all rows, sorted by
   trip and `n`.
2. Keep only the **first** payment of each trip.
3. List the **extra** payments (`payment_id`, `trip_id`, `paid_at`), the ones to refund.

**Expected output:**

```
+----------+-------+-------------------+------+------+---+-----------------+
|payment_id|trip_id|            paid_at|method|amount|  n|payments_for_trip|
+----------+-------+-------------------+------+------+---+-----------------+
|       p01|    t01|2026-10-05 08:00:00|  card|  17.4|  1|                1|
|       p02|    t02|2026-10-06 09:13:00|   app|  33.0|  1|                2|
|      p02b|    t02|2026-10-06 09:20:00|   app|  33.0|  2|                2|
|      p03x|    t03|2026-10-07 09:25:00|  cash|  9.75|  1|                2|
|       p03|    t03|2026-10-07 09:26:00|  cash|  9.75|  2|                2|
+----------+-------+-------------------+------+------+---+-----------------+

+----------+-------+-------------------+------+------+
|payment_id|trip_id|            paid_at|method|amount|
+----------+-------+-------------------+------+------+
|       p01|    t01|2026-10-05 08:00:00|  card|  17.4|
|       p02|    t02|2026-10-06 09:13:00|   app|  33.0|
|      p03x|    t03|2026-10-07 09:25:00|  cash|  9.75|
+----------+-------+-------------------+------+------+

+----------+-------+-------------------+
|payment_id|trip_id|            paid_at|
+----------+-------+-------------------+
|      p02b|    t02|2026-10-06 09:20:00|
|       p03|    t03|2026-10-07 09:26:00|
+----------+-------+-------------------+
```

*Think about it:* compare with `dropDuplicates(["trip_id"])` in
[Set 03, Q6](QUESTIONS_03.md#q6-intermediate--duplicate-payments). For trip `t03`, which
payment would you keep if you ordered by `payment_id`?

---

## Q6 (Intermediate) — The previous and next trip, with lag() and lead()

**Tasks:**

1. For each rider's trips **in time order** (`paid_at`), build `history` with: `rider`,
   `trip_id`, `paid_at`, `fare`, the **previous** trip's `paid_at` (`prev_paid_at`) and `fare`
   (`prev_fare`), the **next** trip's id (`next_trip`), and the hours since the previous trip
   (`hours_since_prev`, rounded to 1 decimal).
2. Show rider `r1`'s trips: `trip_id`, `paid_at`, `fare`, `prev_fare`, `hours_since_prev`, and
   `next_trip` (without cutting the values).
3. For each rider, show the average gap between trips in hours (`avg_gap_h`, rounded to
   1 decimal) and the number of trips.

**Expected output:**

```
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

**Hint:** `F.unix_timestamp(col)` turns a timestamp into seconds.

---

## Q7 (Intermediate+) — Running totals, moving averages, and window frames

**Tasks:**

1. From `rides`, compute the number of trips and the revenue (rounded to 2 decimals) per
   **day** (the date of `paid_at`), as `daily`. Then show each day with:
   * `running`: the **running total** of the revenue up to that day;
   * `avg_3d`: the **3-day moving average** of the revenue (that day and the 2 days before).

   (Round both to 2 decimals.)
2. For the **rated** trips, show `trip_id`, `rating`, `fare`, and two running totals of the fare:
   * `default_frame`: over `W.orderBy("rating")`, with **no** frame given;
   * `rows_frame`: over `W.orderBy("rating", "trip_id")` with
     `rowsBetween(W.unboundedPreceding, W.currentRow)`.

   Show 7 rows, sorted by rating and trip. Why do `t06` and `t20` get the **same**
   `default_frame` value?

**Expected output:**

```
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

---

## Q8 (Intermediate+) — Quartiles and percentiles per group

**Tasks:**

1. Within each city, ordered by fare, compute each trip's `quartile` (`ntile(4)`), its
   `pct_rank` (`percent_rank()`), and its `cume` (`cume_dist()`), the last two rounded to 2
   decimals. Show the **Oakland** trips, sorted by fare.
2. Label every trip with its quartile (`q`), and show, per city and quartile, the number of
   trips (`n`) and the lowest (`from`) and highest (`to`) fare.

**Expected output:**

```
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

---

## Q9 (Intermediate+) — The longest 5-star streak

A **streak** is a run of **consecutive** 5-star trips by the same driver, in time order. An
unrated trip breaks a streak.

**Tasks:**

1. In `rides`, add:
   * `five`: `True` if the trip was rated 5, otherwise `False` (also for unrated trips);
   * `rn`: the number of the trip among the driver's trips, in time order;
   * `rn_same`: the number of the trip among the driver's trips **with the same `five`
     value**, in time order;
   * `grp`: `rn - rn_same`.

   Show driver `d3`'s trips: `trip_id`, `paid_at`, `rating`, `five`, `rn`, `rn_same`, and
   `grp` (without cutting the values). What do you notice about `grp` within a streak?
2. Use `grp` to find each driver's **longest 5-star streak**. Make sure that **every** driver
   with trips is listed, with `0` for a driver who never got 5 stars.

**Expected output:**

```
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

+---------+---------------------+
|driver_id|longest_5_star_streak|
+---------+---------------------+
|       d1|                    2|
|       d2|                    0|
|       d3|                    2|
|       d4|                    1|
+---------+---------------------+
```

**Hint:** this is the "gaps and islands" technique. After task 1, the 5-star trips of one
streak share the same `(driver_id, grp)`, so a normal `groupBy()` can count them.
