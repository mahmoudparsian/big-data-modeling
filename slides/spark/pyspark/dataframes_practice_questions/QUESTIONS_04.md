# PySpark DataFrame Practice Questions — Set 04: Grouping and Aggregation

Nine practice questions, three at each level, meant to be solved **interactively** in
the `bin/pyspark` shell. This set is about **summarizing** data: counting, adding up,
and averaging, per group or over the whole table. It's the DataFrame version of the RDD
reducers (`reduceByKey()`, `aggregateByKey()`, `groupByKey()`), and of SQL's `GROUP BY`.

| # | Level | Topic | Input |
|---|---|---|---|
| [Q1](#q1-basic--counting-and-summing-per-group) | Basic | Counting and summing per group | [`data/trips.csv`](data/trips.csv) |
| [Q2](#q2-basic--aggregating-the-whole-table) | Basic | Aggregating the whole table | [`data/trips.csv`](data/trips.csv) |
| [Q3](#q3-basic--grouping-by-two-columns) | Basic | Grouping by two columns | [`data/trips.csv`](data/trips.csv) |
| [Q4](#q4-intermediate--lists-sets-and-conditional-counts) | Intermediate | Lists, sets, and conditional counts | [`data/trips.csv`](data/trips.csv) |
| [Q5](#q5-intermediate--filtering-before-and-after-grouping) | Intermediate | Filtering before and after grouping | [`data/trips.csv`](data/trips.csv) |
| [Q6](#q6-intermediate--pivot-tables) | Intermediate | Pivot tables | [`data/trips.csv`](data/trips.csv) |
| [Q7](#q7-intermediate--subtotals-with-rollup-and-cube) | Intermediate+ | Subtotals with `rollup()` and `cube()` | [`data/trips.csv`](data/trips.csv) |
| [Q8](#q8-intermediate--medians-percentiles-and-approximate-counts) | Intermediate+ | Medians, percentiles, and approximate counts | [`data/trips.csv`](data/trips.csv) |
| [Q9](#q9-intermediate--grouping-by-computed-values-and-the-average-trap) | Intermediate+ | Grouping by computed values, and the average trap | [`data/trips.csv`](data/trips.csv) |

The operations for this set:

* **Grouping:** `groupBy(...)`, `agg(...)`, `groupBy().count()`, `pivot()`, `rollup()`, `cube()`
* **Aggregate functions** (from `pyspark.sql.functions`): `count`, `count_if`,
  `countDistinct`, `approx_count_distinct`, `sum`, `avg`, `min`, `max`, `stddev`,
  `median`, `percentile_approx`, `collect_list`, `collect_set`, `grouping_id`

You may also use everything from Sets [01](QUESTIONS_01.md)–[03](QUESTIONS_03.md).

Try each question on your own before you open the [answers](ANSWERS_04.md).

## Files

| File | Purpose |
|---|---|
| [QUESTIONS_04.md](QUESTIONS_04.md) | The nine questions (this page) |
| [ANSWERS_04.md](ANSWERS_04.md) | Step-by-step solutions, with real output from Spark 4.2.0 |
| [data/trips.csv](data/trips.csv) | The 20 trips from Set 01 |

## Getting started

Start `$SPARK_HOME/bin/pyspark` **from this folder**, as in
[Set 01](QUESTIONS_01.md#getting-started), do the questions **in order**, and start
with this setup:

```python
from pyspark.sql import functions as F

trips_ddl = ("trip_id STRING, driver_id STRING, rider STRING, city STRING, "
             "hour INT, distance_km DOUBLE, fare DOUBLE, rating INT")
trips = spark.read.csv("data/trips.csv", header=True, schema=trips_ddl)
```

The result of a `groupBy()` comes in **no particular order**, so always end with an
`orderBy()` before you `show()` it.

---

## Q1 (Basic) — Counting and summing per group

**Tasks:**

1. Count the trips per city.
2. For each city, show in **one** `agg()`: the number of trips (`trips`), the total fare
   (`revenue`) and the average fare (`avg_fare`), both rounded to 2 decimals, and the
   shortest and longest distance (`min_km`, `max_km`).
3. Show the total fare per driver (`revenue`), **highest first**.

**Expected output:**

```
+--------+-----+
|    city|count|
+--------+-----+
| Oakland|    8|
|San Jose|   12|
+--------+-----+

+--------+-----+-------+--------+------+------+
|    city|trips|revenue|avg_fare|min_km|max_km|
+--------+-----+-------+--------+------+------+
| Oakland|    8| 168.25|   21.03|   3.1|  15.3|
|San Jose|   12|  308.0|   25.67|   2.2|  20.5|
+--------+-----+-------+--------+------+------+

+---------+-------+
|driver_id|revenue|
+---------+-------+
|       d4| 185.25|
|       d1| 102.25|
|       d3|  99.75|
|       d2|   89.0|
+---------+-------+
```

*Think about it:* compare task 2 with [RDD Set 10, Q1](../rdd_practice_questions/QUESTIONS_10.md#q1-basic--trips-fares-and-distances-per-city).
What did you **not** have to write this time?

---

## Q2 (Basic) — Aggregating the whole table

**Tasks:**

1. With **one** `agg()` and no `groupBy()`, show the number of trips, the total fare
   (`revenue`), the number of different riders (`riders`), and the number of different
   **(driver, city)** combinations (`driver_cities`).
2. Show the total fare and the longest distance using the **dict** form of `agg()`.
3. Get the total fare as a plain **Python number**.

**Expected output:**

```
+-----+-------+------+-------------+
|trips|revenue|riders|driver_cities|
+-----+-------+------+-------------+
|   20| 476.25|     7|            7|
+-----+-------+------+-------------+

+---------+----------------+
|sum(fare)|max(distance_km)|
+---------+----------------+
|   476.25|            20.5|
+---------+----------------+

476.25
```

---

## Q3 (Basic) — Grouping by two columns

**Tasks:**

1. For each **(city, driver)** combination, show the number of trips and the total fare
   (rounded to 2 decimals), sorted by city and driver.
2. How many (city, driver) groups are there? Why not 2 cities × 4 drivers = 8?

**Expected output:**

```
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

7
```

---

## Q4 (Intermediate) — Lists, sets, and conditional counts

**Tasks:**

1. For each driver, show (without cutting the values):
   * `riders`: the list of **all** their riders, in any order (duplicates kept);
   * `unique_riders`: the **sorted** list of their **different** riders;
   * `rated`: the number of trips with a rating;
   * `five_star`: the number of 5-star trips (use `count_if()`);
   * `low`: the number of trips rated 2 or lower (use `sum()` with `when()`).
2. For each city, show the average rating (rounded to 2 decimals) and the number of
   ratings. How many trips does each average include?

**Expected output:**

```
+---------+------------------------+----------------+-----+---------+---+
|driver_id|riders                  |unique_riders   |rated|five_star|low|
+---------+------------------------+----------------+-----+---------+---+
|d1       |[r1, r3, r1, r2, r1, r6]|[r1, r2, r3, r6]|5    |3        |0  |
|d2       |[r2, r2, r2, r7]        |[r2, r7]        |3    |0        |0  |
|d3       |[r1, r5, r6, r1, r5, r1]|[r1, r5, r6]    |6    |4        |1  |
|d4       |[r4, r4, r3, r4]        |[r3, r4]        |3    |1        |1  |
+---------+------------------------+----------------+-----+---------+---+

+--------+----------+---+
|    city|avg_rating|  n|
+--------+----------+---+
| Oakland|       4.0|  6|
|San Jose|      4.09| 11|
+--------+----------+---+
```

(Your `riders` lists may come out in a different order. Why is that allowed?)

---

## Q5 (Intermediate) — Filtering before and after grouping

**Tasks:**

1. For the **rated** trips, show each driver's number of ratings (`n`) and average rating
   (`avg`, rounded to 2 decimals). Call the result `ratings`.
2. Which drivers have **at least 3** ratings **and** an average of **4.5 or more**?
3. For **San Jose** trips only, show the drivers whose total fare (`revenue`) is **over 80**.

**Expected output:**

```
+---------+---+----+
|driver_id|  n| avg|
+---------+---+----+
|       d1|  5| 4.6|
|       d2|  3|3.33|
|       d3|  6|4.33|
|       d4|  3|3.33|
+---------+---+----+

+---------+---+---+
|driver_id|  n|avg|
+---------+---+---+
|       d1|  5|4.6|
+---------+---+---+

+---------+-------+
|driver_id|revenue|
+---------+-------+
|       d2|   89.0|
|       d4|  110.0|
+---------+-------+
```

*Think about it:* in task 3, which filter is SQL's `WHERE`, and which is `HAVING`?
Compare with [RDD Set 10, Q4](../rdd_practice_questions/QUESTIONS_10.md#q4-intermediate--driver-ratings).

---

## Q6 (Intermediate) — Pivot tables

**Tasks:**

1. Make a table with one row per **driver** and one column per **city**, holding the total
   fare (rounded to 2 decimals). What does `NULL` mean here?
2. Make a table of the **number of trips** per driver and city, with the city columns in
   the order **San Jose, Oakland**, and `0` instead of `NULL`.
3. Store this expression in a variable called `period`:

   ```python
   period = (F.when(F.col("hour") < 12, "morning")
              .when(F.col("hour") < 17, "afternoon")
              .otherwise("evening"))
   ```

   Add it as a column, and make a table of the number of trips per **city** (rows) and
   **period** (columns, in the order morning, afternoon, evening).

**Expected output:**

```
+---------+-------+--------+
|driver_id|Oakland|San Jose|
+---------+-------+--------+
|       d1|   30.0|   72.25|
|       d2|   NULL|    89.0|
|       d3|   63.0|   36.75|
|       d4|  75.25|   110.0|
+---------+-------+--------+

+---------+--------+-------+
|driver_id|San Jose|Oakland|
+---------+--------+-------+
|       d1|       4|      2|
|       d2|       4|      0|
|       d3|       3|      3|
|       d4|       1|      3|
+---------+--------+-------+

+--------+-------+---------+-------+
|    city|morning|afternoon|evening|
+--------+-------+---------+-------+
| Oakland|      1|        1|      6|
|San Jose|      4|        1|      7|
+--------+-------+---------+-------+
```

**Hint:** `pivot(col, [values])` lets you list the values, and so the column order, yourself.

---

## Q7 (Intermediate+) — Subtotals with rollup() and cube()

**Tasks:**

1. With `rollup("city", "driver_id")`, show the number of trips and the total fare
   (rounded to 2 decimals) per (city, driver), **plus** a subtotal per city **plus** a
   grand total. Sort so that each subtotal comes **after** its city's rows, and the grand
   total comes last.
2. How many rows do `rollup("city", "driver_id")` and `cube("city", "driver_id")` give?
   Why the difference?
3. With `cube()` and `grouping_id()`, show only the **per-driver** totals (all cities
   together).

**Expected output:**

```
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

(10, 14)
+----+---------+-----+---+
|city|driver_id|trips|gid|
+----+---------+-----+---+
|NULL|       d1|    6|  2|
|NULL|       d2|    4|  2|
|NULL|       d3|    6|  2|
|NULL|       d4|    4|  2|
+----+---------+-----+---+
```

**Hint:** `grouping_id()` is `0` for a normal group, and has a bit set for each column that
was rolled up. Try showing all the `cube()` rows with their `grouping_id()` first.

---

## Q8 (Intermediate+) — Medians, percentiles, and approximate counts

**Tasks:**

1. For each city, show (without cutting the values): the mean fare (rounded to 2 decimals),
   the exact **median** fare, the **approximate** median (`percentile_approx`), the
   approximate 25th and 75th percentiles as one list (`quartiles`), and the standard
   deviation (rounded to 2 decimals). What do the mean and the median tell you about San Jose?
2. Count the different riders exactly, and approximately.

**Expected output:**

```
+--------+-----+------+-------------+-------------+-----+
|city    |mean |median|approx_median|quartiles    |sd   |
+--------+-----+------+-------------+-------------+-----+
|Oakland |21.03|20.625|20.25        |[12.0, 24.5] |8.83 |
|San Jose|25.67|15.5  |15.0         |[10.25, 28.0]|28.03|
+--------+-----+------+-------------+-------------+-----+

+---------------------+----------------------------+
|count(DISTINCT rider)|approx_count_distinct(rider)|
+---------------------+----------------------------+
|                    7|                           7|
+---------------------+----------------------------+
```

*Think about it:* why would anyone want an **approximate** median, when an exact one exists?

---

## Q9 (Intermediate+) — Grouping by computed values, and the average trap

**Tasks:**

1. For each city, compute the "average fare per km" in **two** ways (rounded to 3 decimals):
   * `avg_of_ratios`: the average of each trip's fare ÷ distance;
   * `ratio_of_sums`: the total fare ÷ the total distance.

   Why are they different? Which one matches the city rates of
   [RDD Set 10, Q6](../rdd_practice_questions/QUESTIONS_10.md#q6-intermediate--suspicious-fares)?
2. **Group by the `period` expression** from Q6 (without adding it as a column first): show
   the number of trips, the total fare, and the fare per km (as a ratio of sums, rounded to 2
   decimals) per period, the busiest period first.
3. Make a **histogram** of the fares: group the trips into $10 bands (0, 10, 20, ...) with
   `floor(fare / 10) * 10` (as an `int`, called `bucket`), and count the trips per band.

**Expected output:**

```
+--------+-------------+-------------+
|    city|avg_of_ratios|ratio_of_sums|
+--------+-------------+-------------+
| Oakland|         2.66|        2.592|
|San Jose|        3.014|        3.225|
+--------+-------------+-------------+

+---------+-----+-------+------+
|   period|trips|revenue|per_km|
+---------+-----+-------+------+
|  evening|   13|  241.0|  2.62|
|  morning|    5|  178.0|  3.89|
|afternoon|    2|  57.25|  2.51|
+---------+-----+-------+------+

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

*Think about it:* what does the histogram tell you at a glance?
