# PySpark DataFrame Practice Questions — Set 05: Joins

Nine practice questions, three at each level, meant to be solved **interactively** in
the `bin/pyspark` shell. This set is about **combining tables**: trips with their drivers,
riders, and payments. It's the DataFrame version of
[RDD Set 05](../rdd_practice_questions/QUESTIONS_05.md), with more join types, and without
nested tuples.

| # | Level | Topic | Input |
|---|---|---|---|
| [Q1](#q1-basic--an-inner-join) | Basic | An inner join | [`trips.csv`](data/trips.csv), [`drivers.csv`](data/drivers.csv) |
| [Q2](#q2-basic--left-right-and-full-joins) | Basic | Left, right, and full joins | [`trips.csv`](data/trips.csv), [`drivers.csv`](data/drivers.csv) |
| [Q3](#q3-basic--different-key-names-and-ambiguous-columns) | Basic | Different key names, and ambiguous columns | [`trips.csv`](data/trips.csv), [`drivers.csv`](data/drivers.csv) |
| [Q4](#q4-intermediate--semi-joins-and-anti-joins) | Intermediate | Semi joins and anti joins | [`trips.csv`](data/trips.csv), [`drivers.csv`](data/drivers.csv), [`riders.json`](data/riders.json) |
| [Q5](#q5-intermediate--joining-four-tables-and-broadcast-joins) | Intermediate | Joining four tables, and broadcast joins | all four tables |
| [Q6](#q6-intermediate--joining-on-a-range) | Intermediate | Joining on a range | [`trips.csv`](data/trips.csv), Python lists |
| [Q7](#q7-intermediate--reconciling-two-lists-with-a-full-outer-join) | Intermediate+ | Reconciling two lists with a full outer join | [`payments.csv`](data/payments.csv), Python list |
| [Q8](#q8-intermediate--join-pitfalls-duplicate-keys-and-null-keys) | Intermediate+ | Join pitfalls: duplicate keys and `NULL` keys | [`trips.csv`](data/trips.csv), Python lists |
| [Q9](#q9-intermediate--a-self-join-riders-who-share-drivers) | Intermediate+ | A self-join: riders who share drivers | [`trips.csv`](data/trips.csv) |

The operations for this set:

* **`df1.join(df2, on, how)`**: `on` is a column name, a list of names, or a condition;
  `how` is `"inner"` (the default), `"left"`, `"right"`, `"full"`, `"left_semi"`, or
  `"left_anti"`
* **Helpers:** `alias()` (to name a DataFrame), `F.broadcast()` (a hint), `eqNullSafe()`,
  and `explain()` (to see the plan Spark chose)

You may also use everything from Sets [01](QUESTIONS_01.md)–[04](QUESTIONS_04.md).

Try each question on your own before you open the [answers](ANSWERS_05.md).

## Files

| File | Purpose |
|---|---|
| [QUESTIONS_05.md](QUESTIONS_05.md) | The nine questions (this page) |
| [ANSWERS_05.md](ANSWERS_05.md) | Step-by-step solutions, with real output from Spark 4.2.0 |
| [data/trips.csv](data/trips.csv) | 20 trips; the key columns are `trip_id`, `driver_id`, and `rider` |
| [data/drivers.csv](data/drivers.csv) | 5 drivers: `driver_id,name,car_type` (one has no trips) |
| [data/riders.json](data/riders.json) | 7 riders: `rider`, `name`, `home`, `tags`, ... |
| [data/payments.csv](data/payments.csv) | One payment per trip: `payment_id,trip_id,paid_at,method,amount` |

## Getting started

Start `$SPARK_HOME/bin/pyspark` **from this folder**, as in
[Set 01](QUESTIONS_01.md#getting-started), do the questions **in order**, and start
with this setup:

```python
from pyspark.sql import functions as F

trips_ddl = ("trip_id STRING, driver_id STRING, rider STRING, city STRING, "
             "hour INT, distance_km DOUBLE, fare DOUBLE, rating INT")
trips = spark.read.csv("data/trips.csv", header=True, schema=trips_ddl)
drivers = spark.read.csv("data/drivers.csv", header=True)
riders = spark.read.json("data/riders.json")
payments = (spark.read.csv("data/payments.csv", header=True)
                 .withColumn("paid_at", F.to_timestamp("paid_at"))
                 .withColumn("amount", F.regexp_replace("amount", r"\$", "").cast("double")))
```

---

## Q1 (Basic) — An inner join

**Tasks:**

1. Join `trips` with `drivers` on `driver_id`, as `joined`. Count the rows, and print the
   columns. How many `driver_id` columns are there?
2. Show `trip_id`, `driver_id`, `name`, `car_type`, and `fare` for the first 4 trips (by `trip_id`).
3. Show the total fare per **car type** (rounded to 2 decimals).

**Expected output:**

```
20
['driver_id', 'trip_id', 'rider', 'city', 'hour', 'distance_km', 'fare', 'rating', 'name', 'car_type']
+-------+---------+-----+--------+----+
|trip_id|driver_id| name|car_type|fare|
+-------+---------+-----+--------+----+
|    t01|       d1|  Ana|   sedan|14.5|
|    t02|       d2|  Ben|     suv|30.0|
|    t03|       d1|  Ana|   sedan|9.75|
|    t04|       d3|Chloe|   sedan|19.0|
+-------+---------+-----+--------+----+
only showing top 4 rows

+--------+-------+
|car_type|revenue|
+--------+-------+
|electric| 185.25|
|   sedan|  202.0|
|     suv|   89.0|
+--------+-------+
```

---

## Q2 (Basic) — Left, right, and full joins

**Tasks:**

1. With an **inner** join of `drivers` and `trips`, how many different drivers appear?
2. Now make a **left** join, `drivers` on the left, as `left`. Count its rows. Show the
   driver(s) without any trip.
3. From `left`, show each driver's name with their number of trips (`trips`) and their
   number of rows (`rows`). Why are they different for Eva?
4. Count the rows of `trips` **right**-joined with `drivers`, and of `trips` **full**-joined
   with `drivers`, as one tuple.

**Expected output:**

```
4
21
+---------+----+-------+
|driver_id|name|trip_id|
+---------+----+-------+
|       d5| Eva|   NULL|
+---------+----+-------+

+-----+-----+----+
| name|trips|rows|
+-----+-----+----+
|  Ana|    6|   6|
|  Ben|    4|   4|
|Chloe|    6|   6|
|  Dan|    4|   4|
|  Eva|    0|   1|
+-----+-----+----+

(21, 21)
```

---

## Q3 (Basic) — Different key names, and ambiguous columns

**Tasks:**

1. Rename `driver_id` to `id` in `drivers` (as `drivers_by_id`), and join it with `trips`
   using the **condition** `trips.driver_id == drivers_by_id.id`. Print the row count, and
   whether `id` is among the columns. Show `trip_id`, `driver_id`, `id`, and `name` for 2 trips.
2. Now join `trips` and `drivers` with the condition `trips.driver_id == drivers.driver_id`,
   as `same`. Print its columns. Then try to `select("driver_id")` from it, and print the
   error's type and the first 95 characters of its message.
3. Fix it with **aliases**: show `trip_id`, `driver_id`, and `name` for 2 trips.
4. Fix it again by **dropping** one of the two `driver_id` columns.

**Expected output:**

```
(20, True)
+-------+---------+---+----+
|trip_id|driver_id| id|name|
+-------+---------+---+----+
|    t01|       d1| d1| Ana|
|    t02|       d2| d2| Ben|
+-------+---------+---+----+
only showing top 2 rows

['trip_id', 'driver_id', 'rider', 'city', 'hour', 'distance_km', 'fare', 'rating', 'driver_id', 'name', 'car_type']
AnalysisException [AMBIGUOUS_REFERENCE] Reference `driver_id` is ambiguous, could be: [`driver_id`, `driver_id`].
+-------+---------+----+
|trip_id|driver_id|name|
+-------+---------+----+
|    t01|       d1| Ana|
|    t02|       d2| Ben|
+-------+---------+----+
only showing top 2 rows

+-------+---------+----+
|trip_id|driver_id|name|
+-------+---------+----+
|    t01|       d1| Ana|
|    t02|       d2| Ben|
+-------+---------+----+
only showing top 2 rows
```

**Hint:** `t, d = trips.alias("t"), drivers.alias("d")` lets you write `"t.driver_id"` and `"d.name"`.

---

## Q4 (Intermediate) — Semi joins and anti joins

**Tasks:**

1. Which drivers have **no** trips? Use an **anti** join.
2. Which drivers **have** trips? Use a **semi** join. Compare its row count with the row
   count of an inner join, as one tuple.
3. The **student** riders are those whose `tags` contain `"student"`. Show `trip_id`,
   `rider`, and `fare` of all the trips taken by students, using a semi join.
4. Which riders **never** took a trip in **Oakland**? Show their `rider` and `name`.

**Expected output:**

```
+---------+----+--------+
|driver_id|name|car_type|
+---------+----+--------+
|       d5| Eva|     suv|
+---------+----+--------+

+---------+-----+--------+
|driver_id| name|car_type|
+---------+-----+--------+
|       d1|  Ana|   sedan|
|       d2|  Ben|     suv|
|       d3|Chloe|   sedan|
|       d4|  Dan|electric|
+---------+-----+--------+

(4, 20)
+-------+-----+-----+
|trip_id|rider| fare|
+-------+-----+-----+
|    t01|   r1| 14.5|
|    t04|   r1| 19.0|
|    t07|   r1| 16.0|
|    t08|   r5| 24.5|
|    t12|   r1|10.25|
|    t14|   r1|13.75|
|    t15|   r5| 17.5|
|    t19|   r1|  7.5|
+-------+-----+-----+

+-----+----+
|rider|name|
+-----+----+
|   r1|Maya|
|   r2|Omar|
|   r7|Yuki|
+-----+----+
```

---

## Q5 (Intermediate) — Joining four tables, and broadcast joins

**Tasks:**

1. Build `full`: the trips joined with the drivers (only `driver_id`, `name` renamed to
   `driver`, and `car_type`), the riders (only `rider`, and `name` renamed to `rider_name`),
   and the payments (only `trip_id`, `method`, and `amount`). Add a `tip` column: the amount
   minus the fare, rounded to 2 decimals. Count the rows, and show `trip_id`, `driver`,
   `rider_name`, `fare`, `amount`, `tip`, and `method` for 5 trips.
2. Per payment method, show the number of trips, the total tips (rounded to 2 decimals), and
   the average tip as a **percentage** of the fare (rounded to 1 decimal).
3. **Look at the plan:** does `trips.join(drivers, "driver_id")` use a **broadcast** join? Set
   `spark.sql.autoBroadcastJoinThreshold` to `-1`, and check the plan again. Then, still with
   that setting, check the plan of a join with the hint `F.broadcast(drivers)`. Finally, `unset`
   the setting.

**Expected output:**

```
20
+-------+------+----------+-----+------+----+------+
|trip_id|driver|rider_name| fare|amount| tip|method|
+-------+------+----------+-----+------+----+------+
|    t01|   Ana|      Maya| 14.5|  17.4| 2.9|  card|
|    t02|   Ben|      Omar| 30.0|  33.0| 3.0|   app|
|    t03|   Ana|      Lena| 9.75|  9.75| 0.0|  cash|
|    t04| Chloe|      Maya| 19.0|  22.8| 3.8|  card|
|    t05|   Dan|      Kofi|38.25| 42.08|3.83|   app|
+-------+------+----------+-----+------+----+------+
only showing top 5 rows

+------+-----+-----+-----------+
|method|trips| tips|avg_tip_pct|
+------+-----+-----+-----------+
|   app|    8|24.51|       12.5|
|  card|    8|16.68|       15.0|
|  cash|    4|  0.0|        0.0|
+------+-----+-----+-----------+

True
True
True
```

(The three `True`s mean: `BroadcastHashJoin` is in the first plan, `SortMergeJoin` is in the
second, and `BroadcastHashJoin` is in the third.)

**Hint:** `df.explain()` **prints** the plan. To check it in code, you can capture what it prints:

```python
import io, contextlib

def plan_of(df):
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain()
    return buffer.getvalue()
```

---

## Q6 (Intermediate) — Joining on a range

**Input:** two small lookup tables:

```python
bands = spark.createDataFrame([("budget", 0.0, 15.0), ("standard", 15.0, 30.0), ("premium", 30.0, 1000.0)],
                              "band STRING, low DOUBLE, high DOUBLE")
surge = spark.createDataFrame([("morning rush", 7, 9, 1.5), ("evening rush", 17, 19, 1.3)],
                              "name STRING, start_hour INT, end_hour INT, multiplier DOUBLE")
```

**Tasks:**

1. Join each trip with the fare **band** it falls into: `low <= fare < high`. Count the rows,
   and show `trip_id`, `fare`, and `band` for 4 trips. Which band is the $30.00 trip in, and why?
2. Per band, show the number of trips and the total fare (rounded to 2 decimals), in the
   order budget, standard, premium.
3. Join each trip with the **surge** window its hour falls into (start and end hours
   included), **keeping** the trips outside every window. Show `trip_id`, `hour`, the window
   `name`, and the `surge_fare` (the fare × the multiplier, or × 1.0 outside the windows,
   rounded to 2 decimals) for 6 trips.
4. How many trips were in a surge window?

**Expected output:**

```
20
+-------+----+--------+
|trip_id|fare|    band|
+-------+----+--------+
|    t01|14.5|  budget|
|    t02|30.0| premium|
|    t03|9.75|  budget|
|    t04|19.0|standard|
+-------+----+--------+
only showing top 4 rows

+--------+-----+-------+
|    band|trips|revenue|
+--------+-----+-------+
|  budget|    7|  75.75|
|standard|    9| 186.25|
| premium|    4| 214.25|
+--------+-----+-------+

+-------+----+------------+----------+
|trip_id|hour|        name|surge_fare|
+-------+----+------------+----------+
|    t01|   8|morning rush|     21.75|
|    t02|   9|morning rush|      45.0|
|    t03|   9|morning rush|     14.63|
|    t04|  12|        NULL|      19.0|
|    t05|  13|        NULL|     38.25|
|    t06|  17|evening rush|      10.4|
+-------+----+------------+----------+
only showing top 6 rows

14
```

**Hint for task 2:** after `groupBy("band")`, there's no `low` column left to sort by. How
can you keep it?

---

## Q7 (Intermediate+) — Reconciling two lists with a full outer join

Our records say we were paid for trips `t01`–`t06`. The bank sent its own list. Do they agree?

**Input:**

```python
ours = payments.filter(F.col("trip_id") <= "t06").select("trip_id", F.col("amount").alias("our_amount"))
bank = spark.createDataFrame(
    [("t01", 17.40), ("t02", 33.00), ("t03", 9.75), ("t05", 42.08), ("t06", 9.80), ("t99", 15.00)],
    "trip_id STRING, bank_amount DOUBLE")
```

**Tasks:**

1. Join `ours` and `bank` so that **every** trip from **either** list appears. Add a `status`
   column: `"missing at bank"`, `"unknown to us"`, `"amount differs"`, or `"ok"`. Show all
   the rows, sorted by trip.
2. Count the trips per status.
3. Rename the key to `our_trip` in `ours` and to `bank_trip` in `bank`, and do the full join
   again with a **condition**. Make **one** `trip_id` column again, and show 3 rows.

**Expected output:**

```
+-------+----------+-----------+---------------+
|trip_id|our_amount|bank_amount|         status|
+-------+----------+-----------+---------------+
|    t01|      17.4|       17.4|             ok|
|    t02|      33.0|       33.0|             ok|
|    t03|      9.75|       9.75|             ok|
|    t04|      22.8|       NULL|missing at bank|
|    t05|     42.08|      42.08|             ok|
|    t06|       8.8|        9.8| amount differs|
|    t99|      NULL|       15.0|  unknown to us|
+-------+----------+-----------+---------------+

+---------------+-----+
|         status|count|
+---------------+-----+
| amount differs|    1|
|missing at bank|    1|
|             ok|    4|
|  unknown to us|    1|
+---------------+-----+

+-------+----------+-----------+
|trip_id|our_amount|bank_amount|
+-------+----------+-----------+
|    t01|      17.4|       17.4|
|    t02|      33.0|       33.0|
|    t03|      9.75|       9.75|
+-------+----------+-----------+
only showing top 3 rows
```

*Think about it:* in which order must the `status` conditions be checked, and why?

---

## Q8 (Intermediate+) — Join pitfalls: duplicate keys and NULL keys

**Tasks:**

1. Here's a "city information" table:

   ```python
   city_info = spark.createDataFrame(
       [("San Jose", "CA", "Pacific"), ("Oakland", "CA", "Pacific"), ("San Jose", "CR", "Central")],
       "city STRING, state STRING, tz STRING")
   ```

   Join `trips` with it on `city`. Print the row counts of `trips` and of the join, as one
   tuple. What happened? Write the check that would have warned you **before** the join.
2. Join these two tables on `k`. What happens to the rows with a `NULL` key?

   ```python
   k1 = spark.createDataFrame([("a", 1), (None, 2)], "k STRING, v1 INT")
   k2 = spark.createDataFrame([("a", 10), (None, 20)], "k STRING, v2 INT")
   ```

3. Join them again, so that the two `NULL` keys **do** match. Show `k`, `v1`, and `v2`.

**Expected output:**

```
(20, 32)
+--------+-----+
|    city|count|
+--------+-----+
|San Jose|    2|
+--------+-----+

+---+---+---+
|  k| v1| v2|
+---+---+---+
|  a|  1| 10|
+---+---+---+

+----+---+---+
|   k| v1| v2|
+----+---+---+
|   a|  1| 10|
|NULL|  2| 20|
+----+---+---+
```

---

## Q9 (Intermediate+) — A self-join: riders who share drivers

**Goal:** for every **pair of riders**, find how many **drivers** they have in common. This is
the DataFrame version of [RDD Set 05, Q9](../rdd_practice_questions/QUESTIONS_05.md#q9-intermediate--customers-who-buy-the-same-things-a-self-join).

**Tasks:**

1. Make `rides_with`: the **distinct** `(driver_id, rider)` pairs.
2. Join `rides_with` with **itself** on the driver, keeping each pair of **different** riders
   only **once**. Per pair `(rider_1, rider_2)`, compute the number of shared drivers
   (`shared_drivers`) and the sorted list of those drivers (`drivers`). Count the pairs.
3. Show all the pairs, the most shared drivers first (ties by `rider_1`, then `rider_2`),
   without cutting the values.

**Expected output:**

```
10
+-------+-------+--------------+--------+
|rider_1|rider_2|shared_drivers|drivers |
+-------+-------+--------------+--------+
|r1     |r6     |2             |[d1, d3]|
|r1     |r2     |1             |[d1]    |
|r1     |r3     |1             |[d1]    |
|r1     |r5     |1             |[d3]    |
|r2     |r3     |1             |[d1]    |
|r2     |r6     |1             |[d1]    |
|r2     |r7     |1             |[d2]    |
|r3     |r4     |1             |[d4]    |
|r3     |r6     |1             |[d1]    |
|r5     |r6     |1             |[d3]    |
+-------+-------+--------------+--------+
```

**Hint:** a table joined with itself needs **two aliases**, such as
`a, b = rides_with.alias("a"), rides_with.alias("b")`. How does the RDD version keep each pair once?
