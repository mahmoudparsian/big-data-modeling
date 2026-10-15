# PySpark DataFrame Practice Questions — Set 05: Answers

Solutions to the nine [Set 05 practice questions](QUESTIONS_05.md). Every session below
was run in the `bin/pyspark` shell (Spark 4.2.0), started from this folder. The output is
copied from those real runs.

Try the questions yourself first. There are often several correct solutions, and yours
doesn't need to match these exactly.

* [Q1 (Basic): An inner join](#q1-basic--an-inner-join)
* [Q2 (Basic): Left, right, and full joins](#q2-basic--left-right-and-full-joins)
* [Q3 (Basic): Different key names, and ambiguous columns](#q3-basic--different-key-names-and-ambiguous-columns)
* [Q4 (Intermediate): Semi joins and anti joins](#q4-intermediate--semi-joins-and-anti-joins)
* [Q5 (Intermediate): Joining four tables, and broadcast joins](#q5-intermediate--joining-four-tables-and-broadcast-joins)
* [Q6 (Intermediate): Joining on a range](#q6-intermediate--joining-on-a-range)
* [Q7 (Intermediate+): Reconciling two lists with a full outer join](#q7-intermediate--reconciling-two-lists-with-a-full-outer-join)
* [Q8 (Intermediate+): Join pitfalls: duplicate keys and NULL keys](#q8-intermediate--join-pitfalls-duplicate-keys-and-null-keys)
* [Q9 (Intermediate+): A self-join: riders who share drivers](#q9-intermediate--a-self-join-riders-who-share-drivers)

Setup, used by all the questions:

```python
>>> from pyspark.sql import functions as F
>>> trips_ddl = ("trip_id STRING, driver_id STRING, rider STRING, city STRING, "
...              "hour INT, distance_km DOUBLE, fare DOUBLE, rating INT")
>>> trips = spark.read.csv("data/trips.csv", header=True, schema=trips_ddl)
>>> drivers = spark.read.csv("data/drivers.csv", header=True)
>>> riders = spark.read.json("data/riders.json")
>>> payments = (spark.read.csv("data/payments.csv", header=True)
...                  .withColumn("paid_at", F.to_timestamp("paid_at"))
...                  .withColumn("amount", F.regexp_replace("amount", r"\$", "").cast("double")))
```

---

## Q1 (Basic) — An inner join

```python
>>> joined = trips.join(drivers, on="driver_id")
>>> joined.count()
20
>>> joined.columns
['driver_id', 'trip_id', 'rider', 'city', 'hour', 'distance_km', 'fare', 'rating', 'name', 'car_type']
>>> joined.select("trip_id", "driver_id", "name", "car_type", "fare").orderBy("trip_id").show(4)
+-------+---------+-----+--------+----+
|trip_id|driver_id| name|car_type|fare|
+-------+---------+-----+--------+----+
|    t01|       d1|  Ana|   sedan|14.5|
|    t02|       d2|  Ben|     suv|30.0|
|    t03|       d1|  Ana|   sedan|9.75|
|    t04|       d3|Chloe|   sedan|19.0|
+-------+---------+-----+--------+----+
only showing top 4 rows

>>> joined.groupBy("car_type").agg(F.round(F.sum("fare"), 2).alias("revenue")).orderBy("car_type").show()
+--------+-------+
|car_type|revenue|
+--------+-------+
|electric| 185.25|
|   sedan|  202.0|
|     suv|   89.0|
+--------+-------+
```

**Notes:**

* **`df1.join(df2, on="key")`** is an **inner join** (the default): it keeps the rows whose
  key appears on **both** sides.
* Joining **on a column name** (a string, or a list of names) keeps **one** copy of the
  key column, placed first. That's the easiest and safest way when the key has the same
  name on both sides.
* Each trip has exactly one driver, so the join still has 20 rows, now with the driver's
  `name` and `car_type`.
* The revenue per car type is
  [RDD Set 10, Q5](../rdd_practice_questions/ANSWERS_10.md#q5-intermediate--revenue-per-car-type-and-the-driver-with-no-trips),
  without having to read the nested `kv[1][1][1]` tuples.

---

## Q2 (Basic) — Left, right, and full joins

```python
>>> drivers.join(trips, "driver_id").select("name").distinct().count()
4

>>> left = drivers.join(trips, "driver_id", "left")
>>> left.count()
21
>>> left.filter(F.col("trip_id").isNull()).select("driver_id", "name", "trip_id").show()
+---------+----+-------+
|driver_id|name|trip_id|
+---------+----+-------+
|       d5| Eva|   NULL|
+---------+----+-------+

>>> left.groupBy("name").agg(F.count("trip_id").alias("trips"), F.count("*").alias("rows")).orderBy("name").show()
+-----+-----+----+
| name|trips|rows|
+-----+-----+----+
|  Ana|    6|   6|
|  Ben|    4|   4|
|Chloe|    6|   6|
|  Dan|    4|   4|
|  Eva|    0|   1|
+-----+-----+----+

>>> trips.join(drivers, "driver_id", "right").count(), trips.join(drivers, "driver_id", "full").count()
(21, 21)
```

**Notes:**

* **The join types**, with `drivers` on the left and `trips` on the right:

  | `how=` | Keeps | Here |
  |---|---|---|
  | `"inner"` (default) | keys on both sides | 4 drivers, 20 rows; Eva is gone |
  | `"left"` | every left row, with `NULL`s if there's no match | 21 rows: Eva with `NULL` trip columns |
  | `"right"` | every right row | `trips.join(drivers, ..., "right")` is the same as the left join above |
  | `"full"` | every row of both sides | 21 rows (every trip has a driver, so nothing extra) |

* **Counting with a left join:** `count("*")` counts **rows**, so Eva would get **1**,
  her `NULL` row. `count("trip_id")` skips `NULL`s, so she correctly gets **0** trips.
  This is the classic way to include "zero" groups in a report.
* Compare the RDD version ([RDD Set 05, Q5](../rdd_practice_questions/ANSWERS_05.md#q5-intermediate--data-quality-checks-with-outer-joins)),
  where `None` appeared inside nested tuples. Here it's just a `NULL` column.

---

## Q3 (Basic) — Different key names, and ambiguous columns

```python
>>> drivers_by_id = drivers.withColumnRenamed("driver_id", "id")
>>> j = trips.join(drivers_by_id, trips.driver_id == drivers_by_id.id)
>>> j.count(), "id" in j.columns
(20, True)
>>> j.select("trip_id", "driver_id", "id", "name").orderBy("trip_id").show(2)
+-------+---------+---+----+
|trip_id|driver_id| id|name|
+-------+---------+---+----+
|    t01|       d1| d1| Ana|
|    t02|       d2| d2| Ben|
+-------+---------+---+----+
only showing top 2 rows

>>> same = trips.join(drivers, trips.driver_id == drivers.driver_id)
>>> same.columns
['trip_id', 'driver_id', 'rider', 'city', 'hour', 'distance_km', 'fare', 'rating', 'driver_id', 'name', 'car_type']
>>> try:
...     same.select("driver_id")
... except Exception as e:
...     print(type(e).__name__, str(e)[:95])
...
AnalysisException [AMBIGUOUS_REFERENCE] Reference `driver_id` is ambiguous, could be: [`driver_id`, `driver_id`].

>>> t, d = trips.alias("t"), drivers.alias("d")
>>> (t.join(d, F.col("t.driver_id") == F.col("d.driver_id"))
...   .select("t.trip_id", "t.driver_id", "d.name")
...   .orderBy("trip_id")
...   .show(2))
+-------+---------+----+
|trip_id|driver_id|name|
+-------+---------+----+
|    t01|       d1| Ana|
|    t02|       d2| Ben|
+-------+---------+----+
only showing top 2 rows

>>> same.drop(drivers.driver_id).select("trip_id", "driver_id", "name").orderBy("trip_id").show(2)
+-------+---------+----+
|trip_id|driver_id|name|
+-------+---------+----+
|    t01|       d1| Ana|
|    t02|       d2| Ben|
+-------+---------+----+
only showing top 2 rows
```

**Notes:**

* **When the keys have different names**, join with a **condition**:
  `df1.col_a == df2.col_b`. Both key columns are kept.
* **The duplicate-column trap:** a condition join on two columns with the **same** name
  keeps **both** columns, so the result has two `driver_id`s, and any reference to
  `driver_id` is `AMBIGUOUS_REFERENCE`.
* **Three fixes:**
  1. join on the **name** instead (`on="driver_id"`, as in Q1), which keeps one copy;
  2. give each DataFrame an **alias** and say which one you mean: `"t.driver_id"` or
     `"d.name"`. This is needed when you join a table with **itself** (Q9);
  3. `drop()` one copy, referring to it through its DataFrame (`drivers.driver_id`).
* `t, d = trips.alias("t"), drivers.alias("d")` is ordinary Python: it assigns both
  aliases in one line.

---

## Q4 (Intermediate) — Semi joins and anti joins

```python
>>> drivers.join(trips, "driver_id", "left_anti").show()
+---------+----+--------+
|driver_id|name|car_type|
+---------+----+--------+
|       d5| Eva|     suv|
+---------+----+--------+

>>> drivers.join(trips, "driver_id", "left_semi").orderBy("driver_id").show()
+---------+-----+--------+
|driver_id| name|car_type|
+---------+-----+--------+
|       d1|  Ana|   sedan|
|       d2|  Ben|     suv|
|       d3|Chloe|   sedan|
|       d4|  Dan|electric|
+---------+-----+--------+

>>> drivers.join(trips, "driver_id", "left_semi").count(), drivers.join(trips, "driver_id").count()
(4, 20)

>>> students = riders.filter(F.array_contains("tags", "student")).select("rider")
>>> trips.join(students, "rider", "left_semi").select("trip_id", "rider", "fare").orderBy("trip_id").show()
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

>>> oakland_riders = trips.filter("city = 'Oakland'").select("rider").distinct()
>>> riders.join(oakland_riders, "rider", "left_anti").select("rider", "name").orderBy("rider").show()
+-----+----+
|rider|name|
+-----+----+
|   r1|Maya|
|   r2|Omar|
|   r7|Yuki|
+-----+----+
```

**Notes:**

* A **semi join** (`"left_semi"`) keeps the left rows that **have** a match, and an **anti
  join** (`"left_anti"`) keeps the left rows that **don't**. Both return **only the left
  columns**, and each left row **at most once**.
* **Semi join vs. inner join:** an inner join repeats a driver once **per trip** (20 rows);
  a semi join answers "which drivers have trips?" with each driver once (4 rows). It's a
  **filter** by another table, SQL's `WHERE EXISTS` / `IN (subquery)`.
* **Anti join** = "find what's missing", in one step: drivers with no trips (Eva), or
  riders who never rode in Oakland. In the RDD sets this took a `leftOuterJoin()` plus a
  `filter(... is None)`.
* Filtering by a list of **keys from another table**, like the student riders here, is a
  common use of semi joins.

---

## Q5 (Intermediate) — Joining four tables, and broadcast joins

```python
>>> full = (trips
...         .join(drivers.select("driver_id", F.col("name").alias("driver"), "car_type"), "driver_id")
...         .join(riders.select("rider", F.col("name").alias("rider_name")), "rider")
...         .join(payments.select("trip_id", "method", "amount"), "trip_id")
...         .withColumn("tip", F.round(F.col("amount") - F.col("fare"), 2)))
>>> full.count()
20
>>> full.select("trip_id", "driver", "rider_name", "fare", "amount", "tip", "method").orderBy("trip_id").show(5)
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

>>> (full.groupBy("method")
...      .agg(F.count("*").alias("trips"),
...           F.round(F.sum("tip"), 2).alias("tips"),
...           F.round(F.avg(F.col("tip") / F.col("fare") * 100), 1).alias("avg_tip_pct"))
...      .orderBy("method")
...      .show())
+------+-----+-----+-----------+
|method|trips| tips|avg_tip_pct|
+------+-----+-----+-----------+
|   app|    8|24.51|       12.5|
|  card|    8|16.68|       15.0|
|  cash|    4|  0.0|        0.0|
+------+-----+-----+-----------+

>>> import io, contextlib
>>> def plan_of(df):
...     buffer = io.StringIO()
...     with contextlib.redirect_stdout(buffer):
...         df.explain()
...     return buffer.getvalue()
...
>>> "BroadcastHashJoin" in plan_of(trips.join(drivers, "driver_id"))
True
>>> spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
>>> "SortMergeJoin" in plan_of(trips.join(drivers, "driver_id"))
True
>>> "BroadcastHashJoin" in plan_of(trips.join(F.broadcast(drivers), "driver_id"))
True
>>> spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
```

**Notes:**

* **Chained joins:** each `join()` adds one table. **Select and rename** the columns you
  need from each table first: both `drivers` and `riders` have a `name` column, and
  renaming them to `driver` and `rider_name` avoids ambiguous names (Q3).
* Each join uses its own key (`driver_id`, then `rider`, then `trip_id`), with no
  **re-keying** step in between, unlike the RDD
  [3-way join](../rdd_practice_questions/ANSWERS_05.md#q8-intermediate--revenue-per-city-and-category-a-3-way-join).
* **The tip** is the payment amount minus the fare. Card riders tip about 15% on average,
  app riders 12.5%, and cash riders 0% (the data records cash payments without tips).
* **Broadcast joins:** `explain()` prints the plan that Spark chose. With a small table like
  `drivers`, Spark **automatically** uses a **`BroadcastHashJoin`**: it sends the small
  table to every executor, so the big table **isn't shuffled**. That's the broadcast join
  you built by hand in [RDD Set 09, Q4](../rdd_practice_questions/ANSWERS_09.md#q4-intermediate--a-broadcast-join).
  * Spark broadcasts a table automatically when it's smaller than
    `spark.sql.autoBroadcastJoinThreshold` (10 MB by default). With the threshold turned
    off (`-1`), the same join becomes a **`SortMergeJoin`**, which shuffles and sorts both sides.
  * **`F.broadcast(df)`** is a **hint** that asks for a broadcast join anyway.
* `plan_of()` captures what `explain()` prints, so the code can check for a word in it.
  You can simply call `df.explain()` to read the whole plan. Set 09 looks at plans in more detail.

---

## Q6 (Intermediate) — Joining on a range

```python
>>> bands = spark.createDataFrame([("budget", 0.0, 15.0), ("standard", 15.0, 30.0), ("premium", 30.0, 1000.0)],
...                               "band STRING, low DOUBLE, high DOUBLE")
>>> with_band = trips.join(bands, (trips.fare >= bands.low) & (trips.fare < bands.high))
>>> with_band.count()
20
>>> with_band.select("trip_id", "fare", "band").orderBy("trip_id").show(4)
+-------+----+--------+
|trip_id|fare|    band|
+-------+----+--------+
|    t01|14.5|  budget|
|    t02|30.0| premium|
|    t03|9.75|  budget|
|    t04|19.0|standard|
+-------+----+--------+
only showing top 4 rows

>>> (with_band.groupBy("band", "low")
...           .agg(F.count("*").alias("trips"), F.round(F.sum("fare"), 2).alias("revenue"))
...           .orderBy("low")
...           .drop("low")
...           .show())
+--------+-----+-------+
|    band|trips|revenue|
+--------+-----+-------+
|  budget|    7|  75.75|
|standard|    9| 186.25|
| premium|    4| 214.25|
+--------+-----+-------+

>>> surge = spark.createDataFrame([("morning rush", 7, 9, 1.5), ("evening rush", 17, 19, 1.3)],
...                               "name STRING, start_hour INT, end_hour INT, multiplier DOUBLE")
>>> with_surge = trips.join(surge, trips.hour.between(surge.start_hour, surge.end_hour), "left")
>>> (with_surge.select("trip_id", "hour", "name",
...                    F.round(F.col("fare") * F.coalesce("multiplier", F.lit(1.0)), 2).alias("surge_fare"))
...            .orderBy("trip_id")
...            .show(6))
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

>>> with_surge.filter(F.col("name").isNotNull()).count()
14
```

**Notes:**

* A join condition doesn't have to be `==`. A **range join** (a *non-equi join*) matches
  each trip with the band its fare falls into, or the surge window its hour falls into.
  Here the small lookup tables are just Python lists.
* **Make the ranges not overlap.** `fare >= low AND fare < high` (half-open ranges) puts
  each fare in **exactly one** band. That's why the $30.00 trip `t02` is `premium`, not
  `standard`. With `<=` on both ends, it would match **two** bands, and appear **twice**
  (Q8).
* **A `left` join** keeps the trips outside every surge window (`NULL`), and `coalesce()`
  gives them the normal multiplier, `1.0`. With an inner join, those 6 trips would vanish.
* **`groupBy("band", "low")`** keeps `low` in the result so it can be used for sorting
  (budget → standard → premium, not alphabetically), and `drop()` hides it afterwards.
* Range joins can't use a hash table, because there's no single key to look up, so they
  are much more expensive than `==` joins on big tables. Keep one side small, so Spark can
  broadcast it.

---

## Q7 (Intermediate+) — Reconciling two lists with a full outer join

```python
>>> ours = payments.filter(F.col("trip_id") <= "t06").select("trip_id", F.col("amount").alias("our_amount"))
>>> bank = spark.createDataFrame(
...     [("t01", 17.40), ("t02", 33.00), ("t03", 9.75), ("t05", 42.08), ("t06", 9.80), ("t99", 15.00)],
...     "trip_id STRING, bank_amount DOUBLE")
>>> reconciled = ours.join(bank, "trip_id", "full")
>>> status = (F.when(F.col("bank_amount").isNull(), "missing at bank")
...            .when(F.col("our_amount").isNull(), "unknown to us")
...            .when(F.col("our_amount") != F.col("bank_amount"), "amount differs")
...            .otherwise("ok"))
>>> reconciled.withColumn("status", status).orderBy("trip_id").show()
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

>>> reconciled.withColumn("status", status).groupBy("status").count().orderBy("status").show()
+---------------+-----+
|         status|count|
+---------------+-----+
| amount differs|    1|
|missing at bank|    1|
|             ok|    4|
|  unknown to us|    1|
+---------------+-----+

>>> o = ours.withColumnRenamed("trip_id", "our_trip")
>>> b = bank.withColumnRenamed("trip_id", "bank_trip")
>>> (o.join(b, o.our_trip == b.bank_trip, "full")
...   .select(F.coalesce("our_trip", "bank_trip").alias("trip_id"), "our_amount", "bank_amount")
...   .orderBy("trip_id")
...   .show(3))
+-------+----------+-----------+
|trip_id|our_amount|bank_amount|
+-------+----------+-----------+
|    t01|      17.4|       17.4|
|    t02|      33.0|       33.0|
|    t03|      9.75|       9.75|
+-------+----------+-----------+
only showing top 3 rows
```

**Notes:**

* **Reconciliation** (checking that two systems agree) is a classic use of a **full outer
  join**: every key from **either** side shows up once, and a `NULL` on one side tells you
  what's missing there.
* **The four outcomes:** both sides agree (`ok`), we have it and the bank doesn't
  (`missing at bank`), the bank has it and we don't (`unknown to us`), or both have it with
  different amounts (`amount differs`). Here the bank charged $9.80 for `t06`, but our
  record says $8.80.
* **Order the checks:** the `NULL` checks come **before** `!=`, because `NULL != 9.8` is
  `NULL`, not `true` (Set 03, Q8). Without them, `t04` and `t99` would fall through to `"ok"`.
* **Joining on the name** (`"trip_id"`) gives **one** merged key column, filled in from
  whichever side has it. With a **condition** join on differently named keys, each side
  keeps its own key, `NULL` where it's missing, so you merge them yourself with
  `coalesce()`.
* Comparing money as `double`s works here, but real systems compare **cents** (integers)
  or `DECIMAL` values, to avoid rounding surprises.

---

## Q8 (Intermediate+) — Join pitfalls: duplicate keys and NULL keys

```python
>>> city_info = spark.createDataFrame(
...     [("San Jose", "CA", "Pacific"), ("Oakland", "CA", "Pacific"), ("San Jose", "CR", "Central")],
...     "city STRING, state STRING, tz STRING")
>>> trips.count(), trips.join(city_info, "city").count()
(20, 32)
>>> city_info.groupBy("city").count().filter("count > 1").show()
+--------+-----+
|    city|count|
+--------+-----+
|San Jose|    2|
+--------+-----+

>>> k1 = spark.createDataFrame([("a", 1), (None, 2)], "k STRING, v1 INT")
>>> k2 = spark.createDataFrame([("a", 10), (None, 20)], "k STRING, v2 INT")
>>> k1.join(k2, "k").show()
+---+---+---+
|  k| v1| v2|
+---+---+---+
|  a|  1| 10|
+---+---+---+

>>> k1.join(k2, k1.k.eqNullSafe(k2.k)).select(k1.k, "v1", "v2").orderBy("v1").show()
+----+---+---+
|   k| v1| v2|
+----+---+---+
|   a|  1| 10|
|NULL|  2| 20|
+----+---+---+
```

**Notes:**

* **Duplicate keys multiply rows.** There are two cities called San Jose in `city_info`
  (one in California, one in Costa Rica), so each of the 12 San Jose trips matches **two**
  rows: 20 trips became **32** rows. Every sum or count after this join is now wrong, and
  there's no error to warn you.
* This is the `m × n` rule from [RDD Set 05, Q6](../rdd_practice_questions/ANSWERS_05.md#q6-intermediate--predict-the-size-of-a-join).
  **Before joining**, check that the key is **unique** on the side that should have one row
  per key: `groupBy(key).count().filter("count > 1")` should be empty. **After joining**,
  compare the row counts.
* The fix here is a better key, `(city, state)`, or filtering `city_info` to the cities you
  actually serve.
* **`NULL` keys never match.** `NULL = NULL` is `NULL`, not `true`, so the rows with a `NULL`
  key vanish from an inner join, silently. Usually that's what you want, because a missing
  key isn't a real match. If two `NULL`s **should** match, join with **`eqNullSafe()`** (SQL's `<=>`).

---

## Q9 (Intermediate+) — A self-join: riders who share drivers

```python
>>> rides_with = trips.select("driver_id", "rider").distinct()
>>> a, b = rides_with.alias("a"), rides_with.alias("b")
>>> shared = (a.join(b, (F.col("a.driver_id") == F.col("b.driver_id")) & (F.col("a.rider") < F.col("b.rider")))
...            .groupBy(F.col("a.rider").alias("rider_1"), F.col("b.rider").alias("rider_2"))
...            .agg(F.count("*").alias("shared_drivers"),
...                 F.array_sort(F.collect_list("a.driver_id")).alias("drivers")))
>>> shared.count()
10
>>> shared.orderBy(F.desc("shared_drivers"), "rider_1", "rider_2").show(truncate=False)
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

**Notes:**

* A **self-join** joins a table **with itself**: here, each `(driver, rider)` pair with
  every other rider of the **same driver**. Both sides have the same column names, so
  **aliases** (`a` and `b`) are required (Q3).
* **`distinct()` first:** `r1` rode with `d1` three times, but that's **one** shared driver,
  not three.
* **`a.rider < b.rider`** keeps each pair **once**, and skips "pairs" of a rider with
  themselves, exactly as in [RDD Set 05, Q9](../rdd_practice_questions/ANSWERS_05.md#q9-intermediate--customers-who-buy-the-same-things-a-self-join).
  Putting it **in the join condition** (not in a later `filter()`) keeps the join's output small.
* `r1` and `r6` share **two** drivers (`d1` and `d3`), so they're the most "similar" riders:
  a starting point for a recommendation such as "riders like you also liked...".
* The RDD version needed a `join()`, a `filter()`, a `map()`, and a `reduceByKey()` over nested
  tuples. Here it's one readable query. The same **skew** warning applies, though: a
  driver with 1,000 riders makes about 500,000 pairs.
