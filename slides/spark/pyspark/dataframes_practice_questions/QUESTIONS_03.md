# PySpark DataFrame Practice Questions — Set 03: Filtering, Sorting, Nulls, and Duplicates

Nine practice questions, three at each level, meant to be solved **interactively** in
the `bin/pyspark` shell. This set is about **choosing rows**: which rows to keep, in what
order, and what to do with missing values and duplicates.

| # | Level | Topic | Input |
|---|---|---|---|
| [Q1](#q1-basic--filtering-rows) | Basic | Filtering rows | [`data/trips.csv`](data/trips.csv) |
| [Q2](#q2-basic--sorting-rows) | Basic | Sorting rows | [`data/trips.csv`](data/trips.csv) |
| [Q3](#q3-basic--distinct-values-and-duplicates) | Basic | Distinct values and duplicates | [`data/trips.csv`](data/trips.csv) |
| [Q4](#q4-intermediate--missing-values) | Intermediate | Missing values | [`data/trips.csv`](data/trips.csv), [`data/riders.json`](data/riders.json), Python list |
| [Q5](#q5-intermediate--filtering-text-and-lists) | Intermediate | Filtering text and lists | [`data/riders.json`](data/riders.json) |
| [Q6](#q6-intermediate--duplicate-payments) | Intermediate | Duplicate payments | Python list |
| [Q7](#q7-intermediate--top-n-and-paging) | Intermediate+ | Top N and paging | [`data/trips.csv`](data/trips.csv) |
| [Q8](#q8-intermediate--null-in-conditions) | Intermediate+ | `NULL` in conditions | [`data/trips.csv`](data/trips.csv), Python list |
| [Q9](#q9-intermediate--a-rule-based-data-validation) | Intermediate+ | A rule-based data validation | [`data/trips_dirty.csv`](data/trips_dirty.csv) |

The operations for this set:

* **Filtering:** `filter()` / `where()`, with `&`, `|`, `~`, `between()`, `isin()`,
  `isNull()`, `isNotNull()`, `eqNullSafe()`, `startswith()`, `endswith()`, `contains()`,
  `like()`, `ilike()`, `rlike()`, `array_contains()`
* **Sorting and limiting:** `orderBy()` / `sort()`, `asc()`, `desc()`,
  `asc_nulls_last()`, `desc_nulls_first()`, ..., `limit()`, `offset()`
* **Duplicates:** `distinct()`, `dropDuplicates()`
* **Missing values:** `na.drop()`, `na.fill()`, `na.replace()`, `coalesce()`

You may also use everything from [Set 01](QUESTIONS_01.md) and [Set 02](QUESTIONS_02.md).

Try each question on your own before you open the [answers](ANSWERS_03.md).

## Files

| File | Purpose |
|---|---|
| [QUESTIONS_03.md](QUESTIONS_03.md) | The nine questions (this page) |
| [ANSWERS_03.md](ANSWERS_03.md) | Step-by-step solutions, with real output from Spark 4.2.0 |
| [data/trips.csv](data/trips.csv) | The 20 trips from Set 01 |
| [data/riders.json](data/riders.json) | The 7 riders from Set 01 |
| [data/trips_dirty.csv](data/trips_dirty.csv) | The 20 trips, plus 6 bad rows and one duplicated row, for Q9 |

## Getting started

Start `$SPARK_HOME/bin/pyspark` **from this folder**, as in
[Set 01](QUESTIONS_01.md#getting-started), do the questions **in order**, and start
with this setup:

```python
from pyspark.sql import functions as F

trips_ddl = ("trip_id STRING, driver_id STRING, rider STRING, city STRING, "
             "hour INT, distance_km DOUBLE, fare DOUBLE, rating INT")
trips = spark.read.csv("data/trips.csv", header=True, schema=trips_ddl)
cols = ["trip_id", "city", "hour", "distance_km", "fare", "rating"]
```

`cols` is just a short list of columns to show, so that the tables fit on the screen.

---

## Q1 (Basic) — Filtering rows

**Tasks:**

1. Show (`cols` only) the trips with a fare **over 30**. Then count the same trips with
   `where()` and a **SQL string**.
2. Show the **Oakland** trips of **8 km or more**.
3. Count the trips that started **before 9** or at **20 or later**. Then count the trips
   that are **not** in San Jose.
4. Show `trip_id`, `driver_id`, and `hour` for the trips between **17 and 19** (inclusive)
   by driver **`d1` or `d3`**.
5. Try `trips.filter(F.col("city") == "Oakland" & F.col("fare") > 20)`, **without**
   parentheses around the comparisons. What happens? Print the error's type and message.

**Expected output:**

```
+-------+--------+----+-----------+-----+------+
|trip_id|    city|hour|distance_km| fare|rating|
+-------+--------+----+-----------+-----+------+
|    t05| Oakland|  13|       15.3|38.25|     4|
|    t13|San Jose|   8|       20.5|110.0|     1|
|    t20|San Jose|  22|       14.8| 36.0|     3|
+-------+--------+----+-----------+-----+------+

3
+-------+-------+----+-----------+-----+------+
|trip_id|   city|hour|distance_km| fare|rating|
+-------+-------+----+-----------+-----+------+
|    t05|Oakland|  13|       15.3|38.25|     4|
|    t08|Oakland|  18|        9.8| 24.5|     5|
|    t11|Oakland|  20|        8.0| 21.0|     2|
|    t17|Oakland|  18|       10.0| 25.0|     5|
+-------+-------+----+-----------+-----+------+

6
8
+-------+---------+----+
|trip_id|driver_id|hour|
+-------+---------+----+
|    t07|       d1|  18|
|    t08|       d3|  18|
|    t10|       d1|  19|
|    t15|       d3|  17|
|    t18|       d1|  19|
+-------+---------+----+

PySparkValueError [CANNOT_CONVERT_COLUMN_INTO_BOOL] Cannot convert column into bool: please use '&' for 'and', '|' for 'or', '~' for 'not' when building DataFrame boolean expressions.
```

---

## Q2 (Basic) — Sorting rows

**Tasks:** show (`cols` only, or as described):

1. The 3 **cheapest** trips, and then the 3 most **expensive** trips.
2. The first 5 trips sorted by `city`, and within each city by `fare`, **largest first**.
3. `trip_id` and `rating` of the first 4 trips sorted by `rating`. Where are the unrated trips?
4. The same, with the unrated trips **last**, and ties broken by `trip_id`.
5. The first 4 trips sorted by `rating`, **largest first**, but with the unrated trips
   **first**, and ties broken by `trip_id`.

**Expected output:**

```
+-------+--------+----+-----------+----+------+
|trip_id|    city|hour|distance_km|fare|rating|
+-------+--------+----+-----------+----+------+
|    t19|San Jose|  20|        2.2| 7.5|     5|
|    t06|San Jose|  17|        2.4| 8.0|     3|
|    t03| Oakland|   9|        3.1|9.75|  NULL|
+-------+--------+----+-----------+----+------+
only showing top 3 rows

+-------+--------+----+-----------+-----+------+
|trip_id|    city|hour|distance_km| fare|rating|
+-------+--------+----+-----------+-----+------+
|    t13|San Jose|   8|       20.5|110.0|     1|
|    t05| Oakland|  13|       15.3|38.25|     4|
|    t20|San Jose|  22|       14.8| 36.0|     3|
+-------+--------+----+-----------+-----+------+
only showing top 3 rows

+-------+-------+----+-----------+-----+------+
|trip_id|   city|hour|distance_km| fare|rating|
+-------+-------+----+-----------+-----+------+
|    t05|Oakland|  13|       15.3|38.25|     4|
|    t17|Oakland|  18|       10.0| 25.0|     5|
|    t08|Oakland|  18|        9.8| 24.5|     5|
|    t11|Oakland|  20|        8.0| 21.0|     2|
|    t18|Oakland|  19|        7.7|20.25|     4|
+-------+-------+----+-----------+-----+------+
only showing top 5 rows

+-------+------+
|trip_id|rating|
+-------+------+
|    t03|  NULL|
|    t09|  NULL|
|    t16|  NULL|
|    t13|     1|
+-------+------+
only showing top 4 rows

+-------+------+
|trip_id|rating|
+-------+------+
|    t13|     1|
|    t11|     2|
|    t06|     3|
|    t20|     3|
+-------+------+
only showing top 4 rows

+-------+------+
|trip_id|rating|
+-------+------+
|    t03|  NULL|
|    t09|  NULL|
|    t16|  NULL|
|    t01|     5|
+-------+------+
only showing top 4 rows
```

---

## Q3 (Basic) — Distinct values and duplicates

**Tasks:**

1. How many different cities are there?
2. Show every different `(driver_id, city)` combination, sorted.
3. How many different riders are there? Count them again with `dropDuplicates()` on the
   `rider` column. What's the difference between the two methods' **results** (not their
   counts)?
4. Are there any trips that appear **twice**, identical in every column?

**Expected output:**

```
2
+---------+--------+
|driver_id|    city|
+---------+--------+
|       d1| Oakland|
|       d1|San Jose|
|       d2|San Jose|
|       d3| Oakland|
|       d3|San Jose|
|       d4| Oakland|
|       d4|San Jose|
+---------+--------+

7
7
20
```

---

## Q4 (Intermediate) — Missing values

**Tasks:**

1. Count the trips **with** and **without** a rating.
2. Count the trips left after dropping the rows that have **any** missing value. Then
   count them again, looking **only** at the `fare` column.
3. Read `data/riders.json` as `riders`. Count the rows left by `na.drop()`, and by
   `na.drop(how="all")`. Who was dropped by the first one, and why might that be a mistake?
4. Replace the missing ratings with `0`, and show the three formerly unrated trips.
5. Replace `"San Jose"` with `"SJ"` and `"Oakland"` with `"OAK"` in the `city` column. Show
   3 rows.
6. For this small DataFrame, count the rows left by `na.drop()` with `how="any"`, with
   `how="all"`, with `thresh=2`, and with `subset=["k"]`, as one tuple:

   ```python
   messy = spark.createDataFrame(
       [("a", None, None), ("b", 1, None), (None, None, None), ("d", 4, 5.0)],
       "k STRING, x INT, y DOUBLE")
   ```

**Expected output:**

```
3
17
17
20
6
7
+-------+------+
|trip_id|rating|
+-------+------+
|    t03|     0|
|    t09|     0|
|    t16|     0|
+-------+------+

+-------+----+
|trip_id|city|
+-------+----+
|    t01|  SJ|
|    t02|  SJ|
|    t03| OAK|
+-------+----+
only showing top 3 rows

(1, 3, 2, 3)
```

*Think about it:* is filling the missing ratings with `0` a good idea? (Q8 comes back to this.)

---

## Q5 (Intermediate) — Filtering text and lists

**Input:** `riders` from Q4.

**Tasks:**

1. Show `rider` and `name` for the riders whose name **starts** with `O` or **ends** with `a`.
2. Count the riders whose name matches the SQL pattern `"i%"` with `like()`. Then show the
   names that match it with `ilike()`. Why are the results different?
3. Show `name` and `zip` for the riders whose zip code **starts with `9511` or `9512`**, using
   a regular expression.
4. Count the riders whose home city **contains** `"Jose"`.
5. Show `name` and `tags` of the riders tagged `"frequent"`.

**Expected output:**

```
+-----+----+
|rider|name|
+-----+----+
|   r1|Maya|
|   r2|Omar|
|   r3|Lena|
+-----+----+

0
+----+
|name|
+----+
|Ines|
+----+

+----+-----+
|name|  zip|
+----+-----+
|Maya|95112|
|Omar|95126|
| Tom|95110|
|Yuki|95113|
+----+-----+

4
+----+--------------------+
|name|                tags|
+----+--------------------+
|Maya| [student, frequent]|
|Omar|          [frequent]|
|Yuki|[business, frequent]|
+----+--------------------+
```

---

## Q6 (Intermediate) — Duplicate payments

**Input:** a payments table with **duplicates**:

```python
dup = spark.createDataFrame([
    ("p01", "t01", "2026-10-05 08:00:00", "card", 17.40),
    ("p01", "t01", "2026-10-05 08:00:00", "card", 17.40),
    ("p02", "t02", "2026-10-06 09:13:00", "app", 33.00),
    ("p02b", "t02", "2026-10-06 09:20:00", "app", 33.00),
    ("p03", "t03", "2026-10-07 09:26:00", "cash", 9.75),
    ("p03", "t03", "2026-10-07 09:26:00", "cash", 9.75),
    ("p03", "t03", "2026-10-07 09:26:00", "cash", 9.75)],
    "payment_id STRING, trip_id STRING, paid_at STRING, method STRING, amount DOUBLE")
```

**Tasks:**

1. Count the rows, the **distinct** rows, and the rows left by `dropDuplicates(["trip_id"])`,
   as one tuple. How many rows are **exact copies** of another row?
2. Show the distinct rows, sorted by `payment_id`. Which trip still has **two** payments?
   Are those two rows duplicates?
3. After removing the exact copies, list the trips that still have **more than one** payment,
   with their number of payments.
4. Why is `dropDuplicates(["trip_id"])` a dangerous way to "fix" this table?

**Expected output:**

```
(7, 4, 3)
3
+----------+-------+-------------------+------+------+
|payment_id|trip_id|            paid_at|method|amount|
+----------+-------+-------------------+------+------+
|       p01|    t01|2026-10-05 08:00:00|  card|  17.4|
|       p02|    t02|2026-10-06 09:13:00|   app|  33.0|
|      p02b|    t02|2026-10-06 09:20:00|   app|  33.0|
|       p03|    t03|2026-10-07 09:26:00|  cash|  9.75|
+----------+-------+-------------------+------+------+

+-------+-----+
|trip_id|count|
+-------+-----+
|    t02|    2|
+-------+-----+
```

**Hint for task 3:** `df.groupBy("trip_id").count()` counts the rows per trip, giving a
column called `count`. (Set 04 covers `groupBy()`.)

---

## Q7 (Intermediate+) — Top N and paging

**Tasks:**

1. Make `by_fare`: `trip_id` and `fare` of all trips, sorted by fare (**largest first**),
   with ties broken by `trip_id`. Show the **top 5**.
2. Show the **next 5** ("page 2").
3. Show `trip_id` and `rating` of the top 4 trips by rating (largest first, ties broken by
   `trip_id`). How many trips have the top rating? Why does the tie-breaker matter?
4. Get the ids of the top 3 trips by fare as a **Python list**, and the id of the
   **cheapest** trip as a Python string.

**Expected output:**

```
+-------+-----+
|trip_id| fare|
+-------+-----+
|    t13|110.0|
|    t05|38.25|
|    t20| 36.0|
|    t02| 30.0|
|    t10| 28.0|
+-------+-----+

+-------+-----+
|trip_id| fare|
+-------+-----+
|    t17| 25.0|
|    t08| 24.5|
|    t11| 21.0|
|    t18|20.25|
|    t04| 19.0|
+-------+-----+

+-------+------+
|trip_id|rating|
+-------+------+
|    t01|     5|
|    t04|     5|
|    t07|     5|
|    t08|     5|
+-------+------+

['t13', 't05', 't20']
't19'
```

**Hint:** `offset(n)` skips the first `n` rows.

---

## Q8 (Intermediate+) — NULL in conditions

**Tasks:**

1. Count the trips with a rating **equal** to 5, the trips with a rating **not equal** to 5,
   and all trips, as one tuple. Do the numbers add up? Why not?
2. Count the trips whose rating is "not 5" **including** the unrated trips, in three ways:
   with `isNull()`, with `eqNullSafe()`, and with the SQL condition
   `"rating IS DISTINCT FROM 5"`.
3. For this tiny DataFrame, show `r`, `r == 5` (as `eq`), `r != 5` (as `ne`), and
   `r.eqNullSafe(5)` (as `safe`):

   ```python
   spark.createDataFrame([(None,), (5,)], "r INT")
   ```

4. With one `select()`, show the number of rows, the number of ratings, the average
   rating, and the average rating if the missing ratings counted as `0`. Which average is
   "right"?

**Expected output:**

```
(8, 9, 20)
12
12
12
+----+----+-----+-----+
|   r|  eq|   ne| safe|
+----+----+-----+-----+
|NULL|NULL| NULL|false|
|   5|true|false| true|
+----+----+-----+-----+

+----+-------+------------------+----------+
|rows|ratings|               avg|avg_with_0|
+----+-------+------------------+----------+
|  20|     17|4.0588235294117645|      3.45|
+----+-------+------------------+----------+
```

---

## Q9 (Intermediate+) — A rule-based data validation

**Input:** [`data/trips_dirty.csv`](data/trips_dirty.csv): the 20 trips, plus 6 bad rows
and an **exact copy** of trip `t05`. Read it with the `trips_ddl` schema plus a
`_corrupt_record STRING` column (as in [Set 01, Q8](QUESTIONS_01.md#q8-intermediate--bad-rows-in-a-csv-file)),
and `cache()` it, as `dirty`.

**Tasks:**

1. Count the rows of `dirty`.
2. Write the **validation rules** as a Python dict of `{rule_name: condition}`:

   | Rule | A row passes if... |
   |---|---|
   | `parsed` | it has no corrupt record |
   | `known_city` | the city is `San Jose` or `Oakland` |
   | `positive_distance` | the distance is more than 0 |
   | `fare_present` | the fare is not missing |
   | `rating_1_to_5` | the rating is missing, or between 1 and 5 |

3. With a loop over the rules, print each rule's name, the number of rows that **fail** it,
   and the sorted ids of those trips. Count a rule as failed when it's `NULL`, too.
4. Keep only the rows that pass **all** the rules (combine the conditions with `&`, using
   `functools.reduce`), and drop `_corrupt_record`. Count them.
5. Remove the exact duplicates, and count again. Check that the result is **exactly**
   `trips`.

**Expected output:**

```
27
parsed 2 ['t21', 't22']
known_city 1 ['t23']
positive_distance 1 ['t24']
fare_present 2 ['t22', 't25']
rating_1_to_5 1 ['t26']
21
20
True
```

**Hints:** `reduce(lambda a, b: a & b, list_of_conditions)` joins any number of
conditions with `&`. `F.coalesce(condition, F.lit(False))` turns a `NULL` condition into
`False`.
