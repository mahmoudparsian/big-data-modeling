# PySpark DataFrame Practice Questions — Set 10: Mixed Review

Nine review questions, three at each level, meant to be solved **interactively** in the
`bin/pyspark` shell. Unlike Sets 01–09, this set doesn't tell you which operations to use.
**Choosing the right tools is part of the question**, as in an exam or a real project. Each
question is a small, realistic request about the ride-sharing data.

| # | Level | Question |
|---|---|---|
| [Q1](#q1-basic--a-daily-dashboard) | Basic | A daily dashboard |
| [Q2](#q2-basic--the-driver-roster) | Basic | The driver roster |
| [Q3](#q3-basic--the-rider-directory) | Basic | The rider directory |
| [Q4](#q4-intermediate--riding-away-from-home) | Intermediate | Riding away from home |
| [Q5](#q5-intermediate--fare-per-km-by-time-of-day) | Intermediate | Fare per km by time of day |
| [Q6](#q6-intermediate--the-tip-report) | Intermediate | The tip report |
| [Q7](#q7-intermediate--rider-activity-and-consecutive-days) | Intermediate+ | Rider activity and consecutive days |
| [Q8](#q8-intermediate--a-data-quality-report-as-a-table) | Intermediate+ | A data-quality report as a table |
| [Q9](#q9-intermediate--driver-of-the-week) | Intermediate+ | Driver of the week |

You may use **anything** from [Sets 01–09](README.md): reading files, column functions,
filters, `groupBy()`, joins, window functions, Spark SQL, and so on. (You won't need UDFs.) Each
question has a hidden **hint** that names the sets that help. Try without it first.

Try each question on your own before you open the [answers](ANSWERS_10.md). The answers show
one good solution, explain **why** those tools were chosen, and point out other good choices.
Compare them with [RDD Set 10](../rdd_practice_questions/QUESTIONS_10.md), which asks similar
questions of the same data with RDDs.

## Files

| File | Purpose |
|---|---|
| [QUESTIONS_10.md](QUESTIONS_10.md) | The nine questions (this page) |
| [ANSWERS_10.md](ANSWERS_10.md) | Solutions, with real output from Spark 4.2.0 |
| [data/trips.csv](data/trips.csv) | 20 trips: `trip_id,driver_id,rider,city,hour,distance_km,fare,rating` |
| [data/drivers.csv](data/drivers.csv) | 5 drivers: `driver_id,name,car_type` |
| [data/riders.json](data/riders.json) | 7 riders, with a nested `home` (`city`, `zip`) and a `tags` list |
| [data/payments.csv](data/payments.csv) | One payment per trip: `payment_id,trip_id,paid_at,method,amount` (amount as text like `$17.40`) |
| [data/trips_dirty.csv](data/trips_dirty.csv), [data/payments_raw.csv](data/payments_raw.csv) | The dirty versions of the trips and the payments, for Q8 |

## Getting started

Start `$SPARK_HOME/bin/pyspark` **from this folder**, as in
[Set 01](QUESTIONS_01.md#getting-started), do the questions **in order**, and start with this
setup, which every question uses:

```python
from pyspark.sql import functions as F
from pyspark.sql import Window as W

trips_ddl = ("trip_id STRING, driver_id STRING, rider STRING, city STRING, "
             "hour INT, distance_km DOUBLE, fare DOUBLE, rating INT")
trips = spark.read.csv("data/trips.csv", header=True, schema=trips_ddl)
drivers = spark.read.csv("data/drivers.csv", header=True)
riders = spark.read.json("data/riders.json")
payments = (spark.read.csv("data/payments.csv", header=True)
                 .withColumn("paid_at", F.to_timestamp("paid_at"))
                 .withColumn("amount", F.regexp_replace("amount", r"\$", "").cast("double")))
rides = trips.join(payments.select("trip_id", "paid_at", "method", "amount"), "trip_id")
```

`rides` is the trips together with their payment time, method, and paid amount (the fare plus
the tip).

---

## Q1 (Basic) — A daily dashboard

The operations team wants one line per **day** (the date of the payment): the number of trips,
the total fares (`fares`), the total paid amount (`paid`), and the average rating (`avg_rating`),
the last three rounded to 2 decimals, sorted by day. Which day stands out, and why?

**Expected output:**

```
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

<details>
<summary>Hint</summary>

Group by an expression: [Set 02, Q6](QUESTIONS_02.md#q6-intermediate--dates-and-timestamps) (dates)
and [Set 04, Q1](QUESTIONS_04.md#q1-basic--counting-and-summing-per-group) (several aggregates).

</details>

---

## Q2 (Basic) — The driver roster

HR wants a list of **every** driver: `driver_id`, `name`, `car_type`, the number of trips, the
**sorted list of cities** they drove in, and their total fares (`revenue`, rounded to 2 decimals).
A driver without trips must appear with `0` trips, an **empty** list, and `0.0` revenue.

**Expected output** (without cutting the values):

```
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

<details>
<summary>Hint</summary>

Aggregate the trips first, then a left join: [Set 05, Q2](QUESTIONS_05.md#q2-basic--left-right-and-full-joins),
[Set 04, Q4](QUESTIONS_04.md#q4-intermediate--lists-sets-and-conditional-counts) (`collect_set`),
and `coalesce()` for the missing values.

</details>

---

## Q3 (Basic) — The rider directory

Marketing wants one row per rider: `rider`, `name`, `home_city`, the number of tags (`n_tags`, `0`
if there are none), whether they're tagged `"student"` (`student`, `true` or `false`, never
`NULL`), their number of trips, and their total fares (`spent`, rounded to 2 decimals). Sort by
`spent` (highest first), then by `rider`. Who spent the most, and why?

**Expected output:**

```
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

<details>
<summary>Hint</summary>

Structs and arrays: [Set 02, Q7](QUESTIONS_02.md#q7-intermediate--structs-and-arrays). What do
`size()` and `array_contains()` return for Tom, who has no `tags` at all?

</details>

---

## Q4 (Intermediate) — Riding away from home

Which riders take trips **outside their home city**? For each rider, show `rider`, `home_city`,
the number of trips, the number of trips in another city (`away_trips`), and their percentage
(`pct_away`, rounded to a whole number). Then list the away trips themselves (`trip_id`, `rider`,
`home_city`, `city`).

**Expected output:**

```
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

+-------+-----+---------+--------+
|trip_id|rider|home_city|    city|
+-------+-----+---------+--------+
|    t11|   r6| San Jose| Oakland|
|    t13|   r3|  Oakland|San Jose|
|    t18|   r6| San Jose| Oakland|
+-------+-----+---------+--------+
```

<details>
<summary>Hint</summary>

Join the trips with the riders' **home city**, renamed first to avoid two `city` columns
([Set 05, Q3](QUESTIONS_05.md#q3-basic--different-key-names-and-ambiguous-columns)). A boolean
column can be filtered on, and counted with `count_if()`.

</details>

---

## Q5 (Intermediate) — Fare per km by time of day

Pricing wants a table with one row per **city** and one column per **time of day**: `morning`
(before 12), `afternoon` (before 17), and `evening`, in that order. Each cell is the **total fare
÷ total distance** of those trips, rounded to 2 decimals.

**Expected output:**

```
+--------+-------+---------+-------+
|    city|morning|afternoon|evening|
+--------+-------+---------+-------+
| Oakland|   3.15|      2.5|   2.59|
|San Jose|   3.95|     2.53|   2.66|
+--------+-------+---------+-------+
```

*Think about it:* should pricing raise morning fares in San Jose, based on this table?

<details>
<summary>Hint</summary>

A `when()` expression, and `pivot()` with a list of values:
[Set 04, Q6](QUESTIONS_04.md#q6-intermediate--pivot-tables). For "total ÷ total", see
[Set 04, Q9](QUESTIONS_04.md#q9-intermediate--grouping-by-computed-values-and-the-average-trap).

</details>

---

## Q6 (Intermediate) — The tip report

The tip is the paid amount minus the fare. **Cash payments don't record tips**, so leave them out.

1. Show the 3 trips with the highest tip **percentage** (`tip_pct` = tip ÷ fare × 100, rounded to 1
   decimal), with `trip_id`, `rider`, `method`, `fare`, `tip`, and `tip_pct`. Break ties by `trip_id`.
2. For each rider, show the number of tipped trips and the average tip percentage (rounded to 1
   decimal), the most generous first (ties by `rider`).
3. How many cash payments were left out? Which rider is **missing** from the table of task 2, and why?

**Expected output:**

```
+-------+-----+------+----+---+-------+
|trip_id|rider|method|fare|tip|tip_pct|
+-------+-----+------+----+---+-------+
|    t01|   r1|  card|14.5|2.9|   20.0|
|    t04|   r1|  card|19.0|3.8|   20.0|
|    t07|   r1|   app|16.0|3.2|   20.0|
+-------+-----+------+----+---+-------+
only showing top 3 rows

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

4
```

<details>
<summary>Hint</summary>

Filter first, then computed columns, then top N with a tie-breaker:
[Set 03, Q7](QUESTIONS_03.md#q7-intermediate--top-n-and-paging).

</details>

---

## Q7 (Intermediate+) — Rider activity and consecutive days

A rider is **active** on a day if they took at least one trip that day (by the payment date).

1. For each rider, show the first and last active day, the number of active days, the number of
   times they were active **two days in a row** (`consecutive_pairs`), and the number of days from
   the first to the last active day, both included (`span_days`).
2. List every **run** of consecutive active days longer than 1 day, as `rider`, `streak` (the
   number of days), `from`, and `to`.

**Expected output:**

```
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

+-----+------+----------+----------+
|rider|streak|      from|        to|
+-----+------+----------+----------+
|   r1|     2|2026-10-08|2026-10-09|
|   r2|     2|2026-10-06|2026-10-07|
|   r4|     2|2026-10-06|2026-10-07|
+-----+------+----------+----------+
```

<details>
<summary>Hint</summary>

Make the active days unique first. Then `lag()` and `datediff()`
([Set 06, Q6](QUESTIONS_06.md#q6-intermediate--the-previous-and-next-trip-with-lag-and-lead)), and
the gaps-and-islands trick ([Set 06, Q9](QUESTIONS_06.md#q9-intermediate--the-longest-5-star-streak)).
For dates, `date_sub(day, row_number)` stays the same within a run of consecutive days.

</details>

---

## Q8 (Intermediate+) — A data-quality report as a table

Build a **report DataFrame** with one row per check: `table`, `check`, the number of rows that
**fail** it (`failed`), the number of rows of the table (`rows`), and the failure percentage
(`pct`, rounded to 1 decimal). The checks, in this order:

| `table` | `check` | A row fails if... |
|---|---|---|
| `trips_dirty` | `unparseable row` | it has a corrupt record (read it as in [Set 01, Q8](QUESTIONS_01.md#q8-intermediate--bad-rows-in-a-csv-file)) |
| `trips_dirty` | `unknown city` | its city isn't `San Jose` or `Oakland` (or is missing) |
| `trips_dirty` | `non-positive distance` | its distance is 0 or less |
| `trips_dirty` | `duplicated trip_id` | its `trip_id` appears more than once (flag **every** copy) |
| `payments_raw` | `amount not a number` | its amount can't be cleaned into a number (as in Set 02, Q9) |
| `payments_raw` | `non-standard date` | `paid_at` isn't in the `yyyy-MM-dd HH:mm:ss` format |
| `payments_raw` | `method needs cleaning` | `method` changes when lowercased and trimmed |

**Expected output** (without cutting the values):

```
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

<details>
<summary>Hint</summary>

Keep the checks in a Python list of `(table, check, DataFrame, condition)` tuples
([Set 03, Q9](QUESTIONS_03.md#q9-intermediate--a-rule-based-data-validation)), count each one,
and turn the Python results into a DataFrame with `spark.createDataFrame()`. A window count over
`trip_id` puts the number of copies on every row.

</details>

---

## Q9 (Intermediate+) — Driver of the week

Each city names a **driver of the week**, by this rule:

* **Eligible:** at least **2** rated trips in that city, and **no** rating of 2 or lower there.
* **Winner:** the eligible driver with the highest **average rating** in that city; if tied, the
  higher **revenue** (total fares) in that city; if still tied, the smaller `driver_id`.

1. Show, per city and driver: the number of ratings, the average rating (rounded to 2 decimals), the
   number of low ratings (`low`, ≤ 2), and the revenue (rounded to 2 decimals).
2. Show each city's winner: `city`, `driver_id`, `name`, `ratings`, `avg_rating`, and `revenue`.

**Expected output:**

```
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

+--------+---------+-----+-------+----------+-------+
|    city|driver_id| name|ratings|avg_rating|revenue|
+--------+---------+-----+-------+----------+-------+
| Oakland|       d4|  Dan|      2|       4.5|  75.25|
|San Jose|       d3|Chloe|      3|       5.0|  36.75|
+--------+---------+-----+-------+----------+-------+
```

*Think about it:* who would win each city if the rule ranked by revenue instead? And with no
eligibility rules at all?

<details>
<summary>Hint</summary>

Facts per (city, driver) with conditional counts ([Set 04](QUESTIONS_04.md)), the eligibility rule
as a filter, and the ranking rule as a window order with `row_number()`
([Set 06, Q4](QUESTIONS_06.md#q4-intermediate--the-top-2-drivers-in-each-city)). In SQL,
`QUALIFY` does it in one statement ([Set 07, Q7](QUESTIONS_07.md#q7-intermediate--window-functions-in-sql-and-qualify)).

</details>
