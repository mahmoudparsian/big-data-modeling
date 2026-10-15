# PySpark DataFrame Practice Questions — Set 10: Answers

Solutions to the nine [Set 10 review questions](QUESTIONS_10.md). Every session below was
run in the `bin/pyspark` shell (Spark 4.2.0), started from this folder. The output is copied
from those real runs.

Each answer starts with the **toolbox** it uses, with links to the set where each tool was
introduced. There are many correct solutions; where another approach is just as good, the
notes mention it.

* [Q1 (Basic): A daily dashboard](#q1-basic--a-daily-dashboard)
* [Q2 (Basic): The driver roster](#q2-basic--the-driver-roster)
* [Q3 (Basic): The rider directory](#q3-basic--the-rider-directory)
* [Q4 (Intermediate): Riding away from home](#q4-intermediate--riding-away-from-home)
* [Q5 (Intermediate): Fare per km by time of day](#q5-intermediate--fare-per-km-by-time-of-day)
* [Q6 (Intermediate): The tip report](#q6-intermediate--the-tip-report)
* [Q7 (Intermediate+): Rider activity and consecutive days](#q7-intermediate--rider-activity-and-consecutive-days)
* [Q8 (Intermediate+): A data-quality report as a table](#q8-intermediate--a-data-quality-report-as-a-table)
* [Q9 (Intermediate+): Driver of the week](#q9-intermediate--driver-of-the-week)

Setup, shared by all the questions:

```python
>>> from pyspark.sql import functions as F
>>> from pyspark.sql import Window as W
>>> trips_ddl = ("trip_id STRING, driver_id STRING, rider STRING, city STRING, "
...              "hour INT, distance_km DOUBLE, fare DOUBLE, rating INT")
>>> trips = spark.read.csv("data/trips.csv", header=True, schema=trips_ddl)
>>> drivers = spark.read.csv("data/drivers.csv", header=True)
>>> riders = spark.read.json("data/riders.json")
>>> payments = (spark.read.csv("data/payments.csv", header=True)
...                  .withColumn("paid_at", F.to_timestamp("paid_at"))
...                  .withColumn("amount", F.regexp_replace("amount", r"\$", "").cast("double")))
>>> rides = trips.join(payments.select("trip_id", "paid_at", "method", "amount"), "trip_id")
```

---

## Q1 (Basic) — A daily dashboard

**Toolbox:** a join ([Set 05](ANSWERS_05.md)), `to_date()` ([Set 02, Q6](ANSWERS_02.md#q6-intermediate--dates-and-timestamps)),
`groupBy()` with several aggregates ([Set 04, Q1](ANSWERS_04.md#q1-basic--counting-and-summing-per-group)).

```python
>>> (rides.groupBy(F.to_date("paid_at").alias("day"))
...       .agg(F.count("*").alias("trips"),
...            F.round(F.sum("fare"), 2).alias("fares"),
...            F.round(F.sum("amount"), 2).alias("paid"),
...            F.round(F.avg("rating"), 2).alias("avg_rating"))
...       .orderBy("day")
...       .show())
+----------+-----+-----+-----+----------+
|       day|trips|fares| paid|avg_rating|
+----------+-----+-----+-----+----------+
|2026-10-05|    3| 56.5|61.15|      4.67|
|2026-10-06|    3| 57.0| 64.2|       4.0|
|2026-10-07|    3|62.75|70.85|       4.5|
|2026-10-08|    3|60.25|66.15|      3.67|
|2026-10-09|    3| 56.0|62.36|      4.67|
|2026-10-10|    3|154.0|158.4|      2.33|
|2026-10-11|    2|29.75|34.33|       5.0|
+----------+-----+-----+-----+----------+
```

**Notes:**

* The trips have no date; only the payments do. So the first step is a **join**, which
  `rides` already does.
* **Group by an expression:** `to_date("paid_at")` doesn't need its own column first.
* **The dashboard tells a story:** October 10 has the highest fares **and** the lowest average
  rating. That's the day of the $110, 1-star trip `t13`. A good dashboard makes such days stand out.
* `avg()` skips the unrated trips, so each day's average is over its **rated** trips only.

---

## Q2 (Basic) — The driver roster

**Toolbox:** aggregate first, then a **left join** ([Set 05, Q2](ANSWERS_05.md#q2-basic--left-right-and-full-joins));
`collect_set()` + `array_sort()` ([Set 04, Q4](ANSWERS_04.md#q4-intermediate--lists-sets-and-conditional-counts));
`coalesce()` for the missing values ([Set 02, Q4](ANSWERS_02.md#q4-intermediate--conditional-columns-with-when)).

```python
>>> per_driver = trips.groupBy("driver_id").agg(
...     F.count("*").alias("trips"),
...     F.array_sort(F.collect_set("city")).alias("cities"),
...     F.round(F.sum("fare"), 2).alias("revenue"))
>>> roster = (drivers.join(per_driver, "driver_id", "left")
...                  .select("driver_id", "name", "car_type",
...                          F.coalesce("trips", F.lit(0)).alias("trips"),
...                          F.coalesce("cities", F.array()).alias("cities"),
...                          F.coalesce("revenue", F.lit(0.0)).alias("revenue")))
>>> roster.orderBy("driver_id").show(truncate=False)
+---------+-----+--------+-----+-------------------+-------+
|driver_id|name |car_type|trips|cities             |revenue|
+---------+-----+--------+-----+-------------------+-------+
|d1       |Ana  |sedan   |6    |[Oakland, San Jose]|102.25 |
|d2       |Ben  |suv     |4    |[San Jose]         |89.0   |
|d3       |Chloe|sedan   |6    |[Oakland, San Jose]|99.75  |
|d4       |Dan  |electric|4    |[Oakland, San Jose]|185.25 |
|d5       |Eva  |suv     |0    |[]                 |0.0    |
+---------+-----+--------+-----+-------------------+-------+
```

**Notes:**

* **Start from the table that defines "every driver":** `drivers`, on the left of a left join.
  Starting from `trips` would lose Eva, who has no trips.
* **Aggregate before the join:** one row per driver joins cleanly, with one row in the result per
  driver ([RDD Set 05, Q4](../rdd_practice_questions/ANSWERS_05.md#q4-intermediate--orders-per-customer-and-per-city)'s
  "reduce before you join").
* **Choose the right "empty" value per type:** `0` trips, an empty list of cities, `0.0` revenue,
  rather than `NULL`, so that the report reads naturally, and later sums work.

---

## Q3 (Basic) — The rider directory

**Toolbox:** struct fields (`home.city`) and array functions ([Set 02, Q7](ANSWERS_02.md#q7-intermediate--structs-and-arrays));
a left join; sorting with a tie-breaker ([Set 03, Q2](ANSWERS_03.md#q2-basic--sorting-rows)).

```python
>>> spending = trips.groupBy("rider").agg(F.count("*").alias("trips"), F.round(F.sum("fare"), 2).alias("spent"))
>>> (riders.join(spending, "rider", "left")
...        .select("rider", "name",
...                F.col("home.city").alias("home_city"),
...                F.size(F.coalesce("tags", F.array())).alias("n_tags"),
...                F.coalesce(F.array_contains("tags", "student"), F.lit(False)).alias("student"),
...                "trips", "spent")
...        .orderBy(F.desc("spent"), "rider")
...        .show())
+-----+----+---------+------+-------+-----+------+
|rider|name|home_city|n_tags|student|trips| spent|
+-----+----+---------+------+-------+-----+------+
|   r3|Lena|  Oakland|     0|  false|    2|119.75|
|   r1|Maya| San Jose|     2|   true|    6|  81.0|
|   r2|Omar| San Jose|     1|  false|    4|  81.0|
|   r4|Kofi|  Oakland|     1|  false|    3| 75.25|
|   r5|Ines|  Oakland|     1|   true|    2|  42.0|
|   r6| Tom| San Jose|     0|  false|    2| 41.25|
|   r7|Yuki| San Jose|     2|  false|    1|  36.0|
+-----+----+---------+------+-------+-----+------+
```

**Notes:**

* **`NULL` arrays need care:** Tom has no `tags` at all. `coalesce(tags, array())` makes `size()`
  give **0** instead of `NULL`, and `coalesce(array_contains(...), False)` makes `student` a clean
  `false`.
* **Ties:** Maya and Omar both spent $81.00, so `rider` breaks the tie. Without it, their order
  could change from run to run.
* **Surprise:** the top spender is Lena, with only 2 trips, because one of them is the $110 trip
  `t13` ([RDD Set 10, Q6](../rdd_practice_questions/ANSWERS_10.md#q6-intermediate--suspicious-fares)).
  Before rewarding "top customers", check what drives the numbers.

---

## Q4 (Intermediate) — Riding away from home

**Toolbox:** a join on a **nested** field; a boolean column; `count_if()`
([Set 04, Q4](ANSWERS_04.md#q4-intermediate--lists-sets-and-conditional-counts)).

```python
>>> with_home = (trips.join(riders.select("rider", F.col("home.city").alias("home_city")), "rider")
...                   .withColumn("away", F.col("city") != F.col("home_city")))
>>> (with_home.groupBy("rider", "home_city")
...           .agg(F.count("*").alias("trips"), F.count_if("away").alias("away_trips"))
...           .withColumn("pct_away", F.round(F.col("away_trips") / F.col("trips") * 100))
...           .orderBy("rider")
...           .show())
+-----+---------+-----+----------+--------+
|rider|home_city|trips|away_trips|pct_away|
+-----+---------+-----+----------+--------+
|   r1| San Jose|    6|         0|     0.0|
|   r2| San Jose|    4|         0|     0.0|
|   r3|  Oakland|    2|         1|    50.0|
|   r4|  Oakland|    3|         0|     0.0|
|   r5|  Oakland|    2|         0|     0.0|
|   r6| San Jose|    2|         2|   100.0|
|   r7| San Jose|    1|         0|     0.0|
+-----+---------+-----+----------+--------+

>>> with_home.filter("away").select("trip_id", "rider", "home_city", "city").orderBy("trip_id").show()
+-------+-----+---------+--------+
|trip_id|rider|home_city|    city|
+-------+-----+---------+--------+
|    t11|   r6| San Jose| Oakland|
|    t13|   r3|  Oakland|San Jose|
|    t18|   r6| San Jose| Oakland|
+-------+-----+---------+--------+
```

**Notes:**

* Pull the nested `home.city` out **and rename it** before the join: both tables would otherwise
  have something called `city`, and the comparison would be ambiguous
  ([Set 05, Q3](ANSWERS_05.md#q3-basic--different-key-names-and-ambiguous-columns)).
* **A boolean column is a filter, and a count.** `filter("away")` keeps the away trips, and
  `count_if("away")` counts them.
* Tom (`r6`) lives in San Jose but **only** rides in Oakland: maybe he commutes, or his home
  address is out of date. Lena's only away trip is, once more, `t13`.

---

## Q5 (Intermediate) — Fare per km by time of day

**Toolbox:** a `when()` expression ([Set 02, Q4](ANSWERS_02.md#q4-intermediate--conditional-columns-with-when));
`pivot()` with fixed values ([Set 04, Q6](ANSWERS_04.md#q6-intermediate--pivot-tables)); the
**ratio of sums** ([Set 04, Q9](ANSWERS_04.md#q9-intermediate--grouping-by-computed-values-and-the-average-trap)).

```python
>>> period = (F.when(F.col("hour") < 12, "morning")
...            .when(F.col("hour") < 17, "afternoon")
...            .otherwise("evening"))
>>> (trips.withColumn("period", period)
...       .groupBy("city")
...       .pivot("period", ["morning", "afternoon", "evening"])
...       .agg(F.round(F.sum("fare") / F.sum("distance_km"), 2))
...       .orderBy("city")
...       .show())
+--------+-------+---------+-------+
|    city|morning|afternoon|evening|
+--------+-------+---------+-------+
| Oakland|   3.15|      2.5|   2.59|
|San Jose|   3.95|     2.53|   2.66|
+--------+-------+---------+-------+
```

**Notes:**

* **`pivot()` accepts any aggregate**, not just a count or a sum. Here each cell is the **total fare
  ÷ total km** of that city and period: the per-km price customers actually paid.
* Listing the periods keeps them in **time order** (not alphabetical), and keeps the columns stable.
* Mornings look more expensive in both cities, but San Jose's morning (3.95) includes `t13`, the
  outlier again. With so few trips, one trip can move a cell a lot, so add the **number of trips**
  per cell before drawing conclusions.

---

## Q6 (Intermediate) — The tip report

**Toolbox:** a filter, computed columns, top N with a tie-breaker ([Set 03, Q7](ANSWERS_03.md#q7-intermediate--top-n-and-paging)),
`groupBy()`.

```python
>>> tipped = (rides.filter(F.col("method") != "cash")
...                .withColumn("tip", F.round(F.col("amount") - F.col("fare"), 2))
...                .withColumn("tip_pct", F.round(F.col("tip") / F.col("fare") * 100, 1)))
>>> (tipped.orderBy(F.desc("tip_pct"), "trip_id")
...        .select("trip_id", "rider", "method", "fare", "tip", "tip_pct")
...        .show(3))
+-------+-----+------+----+---+-------+
|trip_id|rider|method|fare|tip|tip_pct|
+-------+-----+------+----+---+-------+
|    t01|   r1|  card|14.5|2.9|   20.0|
|    t04|   r1|  card|19.0|3.8|   20.0|
|    t07|   r1|   app|16.0|3.2|   20.0|
+-------+-----+------+----+---+-------+
only showing top 3 rows

>>> (tipped.groupBy("rider")
...        .agg(F.count("*").alias("tipped_trips"), F.round(F.avg("tip_pct"), 1).alias("avg_tip_pct"))
...        .orderBy(F.desc("avg_tip_pct"), "rider")
...        .show())
+-----+------------+-----------+
|rider|tipped_trips|avg_tip_pct|
+-----+------------+-----------+
|   r1|           6|       16.7|
|   r2|           4|       15.0|
|   r4|           3|       10.0|
|   r5|           1|       10.0|
|   r6|           1|       10.0|
|   r7|           1|       10.0|
+-----+------------+-----------+

>>> rides.filter("method = 'cash'").count()
4
```

**Notes:**

* **Exclude cash first:** the cash payments record no tip at all, so they would count as 0% tips,
  and drag every average down. A "0" that means "not recorded" is like the "0 for unrated"
  trap of [Set 03, Q8](ANSWERS_03.md#q8-intermediate--null-in-conditions).
* Several trips tie at a **20%** tip, so the top 3 depends on the tie-breaker (`trip_id`).
* **Lena (`r3`) is missing** from the per-rider table: both her trips were paid in cash.
  Missing from a report isn't the same as "tips 0%". Say so in the report.
* With windows, you could also rank the trips **within** each rider
  ([Set 06](ANSWERS_06.md)), for example "each rider's most generous trip".

---

## Q7 (Intermediate+) — Rider activity and consecutive days

**Toolbox:** `distinct()` days; `lag()` and `datediff()` ([Set 06, Q6](ANSWERS_06.md#q6-intermediate--the-previous-and-next-trip-with-lag-and-lead));
gaps and islands ([Set 06, Q9](ANSWERS_06.md#q9-intermediate--the-longest-5-star-streak)).

```python
>>> active = rides.select("rider", F.to_date("paid_at").alias("day")).distinct()
>>> by_day = W.partitionBy("rider").orderBy("day")
>>> (active.withColumn("gap", F.datediff("day", F.lag("day").over(by_day)))
...        .groupBy("rider")
...        .agg(F.min("day").alias("first"),
...             F.max("day").alias("last"),
...             F.count("*").alias("active_days"),
...             F.count_if(F.col("gap") == 1).alias("consecutive_pairs"))
...        .withColumn("span_days", F.datediff("last", "first") + 1)
...        .orderBy("rider")
...        .show())
+-----+----------+----------+-----------+-----------------+---------+
|rider|     first|      last|active_days|consecutive_pairs|span_days|
+-----+----------+----------+-----------+-----------------+---------+
|   r1|2026-10-05|2026-10-11|          4|                1|        7|
|   r2|2026-10-06|2026-10-10|          3|                1|        5|
|   r3|2026-10-07|2026-10-10|          2|                0|        4|
|   r4|2026-10-06|2026-10-09|          3|                1|        4|
|   r5|2026-10-05|2026-10-05|          1|                0|        1|
|   r6|2026-10-08|2026-10-08|          1|                0|        1|
|   r7|2026-10-10|2026-10-10|          1|                0|        1|
+-----+----------+----------+-----------+-----------------+---------+

>>> (active.withColumn("island", F.date_sub("day", F.row_number().over(by_day).cast("int")))
...        .groupBy("rider", "island")
...        .agg(F.count("*").alias("streak"), F.min("day").alias("from"), F.max("day").alias("to"))
...        .filter("streak > 1")
...        .drop("island")
...        .orderBy("rider", "from")
...        .show())
+-----+------+----------+----------+
|rider|streak|      from|        to|
+-----+------+----------+----------+
|   r1|     2|2026-10-08|2026-10-09|
|   r2|     2|2026-10-06|2026-10-07|
|   r4|     2|2026-10-06|2026-10-07|
+-----+------+----------+----------+
```

**Notes:**

* **`distinct()` first:** a rider with two trips on the same day (Maya on October 9) has **one**
  active day. Activity is about days, not trips.
* **`lag()` + `datediff()`** gives the gap since the previous active day; a gap of exactly 1 means two
  **consecutive** days.
* **Islands of consecutive days:** subtract the row number (in days) from each date. Inside a run of
  consecutive days, both go up by 1, so the result (`island`) stays the **same**. The same
  trick as Set 06's 5-star streaks, with dates instead of a second row number.
* `span_days` vs. `active_days` measures how **regular** a rider is: Maya was active on 4 of the 7
  days between her first and last ride.

---

## Q8 (Intermediate+) — A data-quality report as a table

**Toolbox:** read modes and the corrupt-record column ([Set 01, Q8](ANSWERS_01.md#q8-intermediate--bad-rows-in-a-csv-file));
rules as data ([Set 03, Q9](ANSWERS_03.md#q9-intermediate--a-rule-based-data-validation)); `try_cast`
and `try_to_timestamp` ([Set 02](ANSWERS_02.md)); a window count; `createDataFrame()` from Python
results ([Set 01, Q1](ANSWERS_01.md#q1-basic--a-dataframe-from-a-python-list)).

```python
>>> dirty = (spark.read.csv("data/trips_dirty.csv", header=True,
...                         schema=trips_ddl + ", _corrupt_record STRING",
...                         columnNameOfCorruptRecord="_corrupt_record")
...               .withColumn("copies", F.count("*").over(W.partitionBy("trip_id")))
...               .cache())
>>> raw_payments = spark.read.csv("data/payments_raw.csv", header=True)
>>> checks = [
...     ("trips_dirty", "unparseable row", dirty, F.col("_corrupt_record").isNotNull()),
...     ("trips_dirty", "unknown city", dirty, ~F.coalesce(F.col("city").isin("San Jose", "Oakland"), F.lit(False))),
...     ("trips_dirty", "non-positive distance", dirty, F.coalesce(F.col("distance_km") <= 0, F.lit(False))),
...     ("trips_dirty", "duplicated trip_id", dirty, F.col("copies") > 1),
...     ("payments_raw", "amount not a number", raw_payments,
...      F.regexp_replace("amount", r"[^0-9.]", "").try_cast("double").isNull()),
...     ("payments_raw", "non-standard date", raw_payments,
...      F.try_to_timestamp("paid_at", F.lit("yyyy-MM-dd HH:mm:ss")).isNull()),
...     ("payments_raw", "method needs cleaning", raw_payments, F.col("method") != F.lower(F.trim("method"))),
... ]
>>> results = [(table, check, df.filter(condition).count(), df.count())
...            for table, check, df, condition in checks]
>>> report = (spark.createDataFrame(results, "table STRING, check STRING, failed LONG, rows LONG")
...                .withColumn("pct", F.round(F.col("failed") / F.col("rows") * 100, 1)))
>>> report.show(truncate=False)
+------------+---------------------+------+----+----+
|table       |check                |failed|rows|pct |
+------------+---------------------+------+----+----+
|trips_dirty |unparseable row      |2     |27  |7.4 |
|trips_dirty |unknown city         |1     |27  |3.7 |
|trips_dirty |non-positive distance|1     |27  |3.7 |
|trips_dirty |duplicated trip_id   |2     |27  |7.4 |
|payments_raw|amount not a number  |2     |20  |10.0|
|payments_raw|non-standard date    |3     |20  |15.0|
|payments_raw|method needs cleaning|4     |20  |20.0|
+------------+---------------------+------+----+----+
```

**Notes:**

* **Checks as data:** each check is one tuple of (table, name, DataFrame, failing condition). A
  list comprehension runs them all, and adding a check is one line.
* **The report is itself a DataFrame**, built from the Python results with `createDataFrame()`. So
  it can be saved as Parquet every day (Set 09), compared over time, or turned into an alert when a
  percentage jumps.
* **Duplicates per row:** `count("*").over(W.partitionBy("trip_id"))` puts each id's number of copies
  on **every** row, so the check flags **both** copies of `t05`.
* The `coalesce(..., False)` guards make each condition `true` or `false`, never `NULL`, so that no
  failing row escapes the count (Set 03, Q9).
* Each check is a separate job, so `dirty` is **cached** first. This is a plain DataFrame `cache()`,
  with no Python UDF in the session (Set 08, Q6).

---

## Q9 (Intermediate+) — Driver of the week

**Toolbox:** a `groupBy()` with conditional counts; filters on aggregates (`HAVING`,
[Set 04, Q5](ANSWERS_04.md#q5-intermediate--filtering-before-and-after-grouping)); a ranking window
with tie-breakers ([Set 06, Q4](ANSWERS_06.md#q4-intermediate--the-top-2-drivers-in-each-city));
a join for the names.

```python
>>> per_city = (trips.groupBy("city", "driver_id")
...                  .agg(F.count("rating").alias("ratings"),
...                       F.round(F.avg("rating"), 2).alias("avg_rating"),
...                       F.count_if(F.col("rating") <= 2).alias("low"),
...                       F.round(F.sum("fare"), 2).alias("revenue")))
>>> per_city.orderBy("city", "driver_id").show()
+--------+---------+-------+----------+---+-------+
|    city|driver_id|ratings|avg_rating|low|revenue|
+--------+---------+-------+----------+---+-------+
| Oakland|       d1|      1|       4.0|  0|   30.0|
| Oakland|       d3|      3|      3.67|  1|   63.0|
| Oakland|       d4|      2|       4.5|  0|  75.25|
|San Jose|       d1|      4|      4.75|  0|  72.25|
|San Jose|       d2|      3|      3.33|  0|   89.0|
|San Jose|       d3|      3|       5.0|  0|  36.75|
|San Jose|       d4|      1|       1.0|  1|  110.0|
+--------+---------+-------+----------+---+-------+

>>> best_first = W.partitionBy("city").orderBy(F.desc("avg_rating"), F.desc("revenue"), "driver_id")
>>> (per_city.filter("ratings >= 2 AND low = 0")
...          .withColumn("rank", F.row_number().over(best_first))
...          .filter("rank = 1")
...          .join(drivers.select("driver_id", "name"), "driver_id")
...          .select("city", "driver_id", "name", "ratings", "avg_rating", "revenue")
...          .orderBy("city")
...          .show())
+--------+---------+-----+-------+----------+-------+
|    city|driver_id| name|ratings|avg_rating|revenue|
+--------+---------+-----+-------+----------+-------+
| Oakland|       d4|  Dan|      2|       4.5|  75.25|
|San Jose|       d3|Chloe|      3|       5.0|  36.75|
+--------+---------+-----+-------+----------+-------+
```

**Notes:**

* **Turn the business rule into steps:** (1) the facts per (city, driver); (2) the **eligibility**
  rules as a filter: at least 2 ratings, and no low (≤ 2) rating; (3) the **ranking** rule as a
  window order: best average rating, then the higher revenue, then the id; (4) keep rank 1, and
  add the names.
* **Eligibility matters:** in Oakland, Ana has a 4.0 average, but from only **one** rating, and Chloe
  has a 2-star trip, so the award goes to Dan. In San Jose, Dan, the biggest earner thanks to `t13`,
  is out because of that trip's 1-star rating, and Chloe's perfect 5.0 wins.
* **The rule decides the winner:** with the same eligibility rules but ranked by **revenue**, Ben
  would win San Jose; drop the eligibility rules too, and Dan would win **both** cities. Write the
  rule down explicitly, and make sure the people who will use the result agree with it.
* The same query as SQL is a good exercise: a CTE for `per_city`, and `QUALIFY ROW_NUMBER() OVER (...)
  = 1` ([Set 07, Q7](ANSWERS_07.md#q7-intermediate--window-functions-in-sql-and-qualify)).
