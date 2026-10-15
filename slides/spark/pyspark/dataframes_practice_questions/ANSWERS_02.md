# PySpark DataFrame Practice Questions — Set 02: Answers

Solutions to the nine [Set 02 practice questions](QUESTIONS_02.md). Every session below
was run in the `bin/pyspark` shell (Spark 4.2.0), started from this folder. The output is
copied from those real runs.

Try the questions yourself first. There are often several correct solutions, and yours
doesn't need to match these exactly.

* [Q1 (Basic): Selecting columns and expressions](#q1-basic--selecting-columns-and-expressions)
* [Q2 (Basic): Adding, replacing, renaming, and dropping columns](#q2-basic--adding-replacing-renaming-and-dropping-columns)
* [Q3 (Basic): Casting and rounding](#q3-basic--casting-and-rounding)
* [Q4 (Intermediate): Conditional columns with when()](#q4-intermediate--conditional-columns-with-when)
* [Q5 (Intermediate): String functions](#q5-intermediate--string-functions)
* [Q6 (Intermediate): Dates and timestamps](#q6-intermediate--dates-and-timestamps)
* [Q7 (Intermediate+): Structs and arrays](#q7-intermediate--structs-and-arrays)
* [Q8 (Intermediate+): Working on many columns at once](#q8-intermediate--working-on-many-columns-at-once)
* [Q9 (Intermediate+): Cleaning a messy payments file](#q9-intermediate--cleaning-a-messy-payments-file)

Setup, used by all the questions:

```python
>>> from pyspark.sql import functions as F
>>> trips_ddl = ("trip_id STRING, driver_id STRING, rider STRING, city STRING, "
...              "hour INT, distance_km DOUBLE, fare DOUBLE, rating INT")
>>> trips = spark.read.csv("data/trips.csv", header=True, schema=trips_ddl)
```

---

## Q1 (Basic) — Selecting columns and expressions

```python
>>> trips.select("trip_id", "fare").show(3)
+-------+----+
|trip_id|fare|
+-------+----+
|    t01|14.5|
|    t02|30.0|
|    t03|9.75|
+-------+----+
only showing top 3 rows

>>> trips.select(F.col("trip_id"), trips.city, trips["fare"]).show(3)
+-------+--------+----+
|trip_id|    city|fare|
+-------+--------+----+
|    t01|San Jose|14.5|
|    t02|San Jose|30.0|
|    t03| Oakland|9.75|
+-------+--------+----+
only showing top 3 rows

>>> trips.select("trip_id", (F.col("fare") / F.col("distance_km")).alias("per_km")).show(3)
+-------+------------------+
|trip_id|            per_km|
+-------+------------------+
|    t01|2.7884615384615383|
|    t02|               2.5|
|    t03|3.1451612903225805|
+-------+------------------+
only showing top 3 rows

>>> trips.select("trip_id", F.round(F.col("fare") / F.col("distance_km"), 2).alias("per_km")).show(3)
+-------+------+
|trip_id|per_km|
+-------+------+
|    t01|  2.79|
|    t02|   2.5|
|    t03|  3.15|
+-------+------+
only showing top 3 rows

>>> trips.selectExpr("trip_id", "round(fare / distance_km, 2) AS per_km", "fare * 100 AS cents").show(3)
+-------+------+------+
|trip_id|per_km| cents|
+-------+------+------+
|    t01|  2.79|1450.0|
|    t02|   2.5|3000.0|
|    t03|  3.15| 975.0|
+-------+------+------+
only showing top 3 rows

>>> F.col("fare") / 2
Column<'/(fare, 2)'>
```

**Notes:**

* **Four ways to name a column:** a string (`"fare"`), `F.col("fare")`, an attribute
  (`trips.fare`), and an index (`trips["fare"]`). Strings are the shortest; `F.col()`
  works without having a DataFrame variable at hand, which makes it the most common
  choice in reusable code.
* `select()` can compute **expressions**, not just pick columns. Arithmetic on columns
  (`/`, `*`, `+`, `-`) works row by row, like `map()` on an RDD, but Spark understands
  it, and can optimize it.
* **`alias()`** names the result. Without it, Spark invents a name like
  `(fare / distance_km)`.
* **`selectExpr()`** takes **SQL expressions** as strings. It's handy for quick
  calculations, and it's the same syntax as Spark SQL (Set 07).
* `F.col("fare") / 2` doesn't compute anything. It builds a **`Column` object**: a
  description of a calculation, which Spark runs later, for every row, when an action
  needs it. That's why you can store expressions in variables and reuse them.

---

## Q2 (Basic) — Adding, replacing, renaming, and dropping columns

```python
>>> with_extra = (trips.withColumn("per_km", F.round(F.col("fare") / F.col("distance_km"), 2))
...                    .withColumn("currency", F.lit("USD")))
>>> with_extra.columns
['trip_id', 'driver_id', 'rider', 'city', 'hour', 'distance_km', 'fare', 'rating', 'per_km', 'currency']

>>> with_extra.withColumn("fare", F.col("fare") * 2).select("trip_id", "fare").show(2)
+-------+----+
|trip_id|fare|
+-------+----+
|    t01|29.0|
|    t02|60.0|
+-------+----+
only showing top 2 rows

>>> with_extra.withColumnRenamed("fare", "fare_usd").drop("currency", "rider", "per_km").columns
['trip_id', 'driver_id', 'city', 'hour', 'distance_km', 'fare_usd', 'rating']

>>> both = trips.withColumns({"fare_cents": (F.col("fare") * 100).cast("int"),
...                           "is_long": F.col("distance_km") > 10})
>>> both.select("trip_id", "fare_cents", "is_long").show(3)
+-------+----------+-------+
|trip_id|fare_cents|is_long|
+-------+----------+-------+
|    t01|      1450|  false|
|    t02|      3000|   true|
|    t03|       975|  false|
+-------+----------+-------+
only showing top 3 rows

>>> trips.columns
['trip_id', 'driver_id', 'rider', 'city', 'hour', 'distance_km', 'fare', 'rating']
```

**Notes:**

* **`withColumn(name, expr)`** adds a new column at the end, or **replaces** a column
  that already has that name (as `fare` here).
* **`F.lit(value)`** turns a plain Python value into a column with the same value on
  every row. You need it wherever Spark expects a column, but you have a constant.
* `withColumnRenamed(old, new)` renames, and `drop(...)` removes one or more columns.
* **`withColumns({...})`** adds or replaces **several** columns in one step, with a dict.
* A comparison like `F.col("distance_km") > 10` is a `boolean` column.
* **DataFrames are immutable.** Every one of these methods returns a **new** DataFrame,
  and `trips` itself never changes, just like RDDs. Assign the result to a variable to keep it.

---

## Q3 (Basic) — Casting and rounding

```python
>>> trips.select("trip_id", "fare",
...              F.col("fare").cast("int").alias("fare_int"),
...              F.round("fare").alias("rounded"),
...              F.ceil("fare").alias("up"),
...              F.col("hour").cast("string").alias("hour_str")).show(3)
+-------+----+--------+-------+---+--------+
|trip_id|fare|fare_int|rounded| up|hour_str|
+-------+----+--------+-------+---+--------+
|    t01|14.5|      14|   15.0| 15|       8|
|    t02|30.0|      30|   30.0| 30|       9|
|    t03|9.75|       9|   10.0| 10|       9|
+-------+----+--------+-------+---+--------+
only showing top 3 rows

>>> words = spark.createDataFrame([("42",), ("3.5",), ("abc",), (None,)], "s STRING")
>>> words.select("s",
...              F.col("s").try_cast("int").alias("as_int"),
...              F.col("s").try_cast("double").alias("as_double")).show()
+----+------+---------+
|   s|as_int|as_double|
+----+------+---------+
|  42|    42|     42.0|
| 3.5|  NULL|      3.5|
| abc|  NULL|     NULL|
|NULL|  NULL|     NULL|
+----+------+---------+

>>> try:
...     words.select(F.col("s").cast("int")).show()
... except Exception as e:
...     print("CAST_INVALID_INPUT" in str(e), "try_cast" in str(e))
...
True True
```

**Notes:**

* **`cast(type)`** converts a column to another type: `"int"`, `"double"`, `"string"`,
  `"date"`... Casting a `double` to an `int` **cuts off** the decimals (`9.75` → `9`); it
  doesn't round.
* `F.round(col, n)` rounds to `n` decimals (0 by default); `F.ceil()` and `F.floor()`
  round up and down to whole numbers.
* **Spark 4 is strict about bad casts.** **ANSI mode** is on by default
  (`spark.sql.ansi.enabled = true`), so casting a string that isn't a valid number
  **fails the whole query** with a `CAST_INVALID_INPUT` error. (In Spark 3, it quietly
  returned `NULL`.) Note that even `"3.5"` fails for an `int`.
* In the shell, a failed query also prints a long **`ERROR` log message** (a block of
  JSON with the stack trace) before the Python exception. That's normal. The important
  part is the error class at the start: `[CAST_INVALID_INPUT]`.
* **`try_cast()`** is the safe version: an invalid value becomes `NULL` instead of an
  error. The error message itself suggests it. Use `cast()` when bad data **should** stop
  the job, and `try_cast()` when you'll handle the `NULL`s yourself (as in Q9).

---

## Q4 (Intermediate) — Conditional columns with when()

```python
>>> labeled = trips.select(
...     "trip_id", "distance_km", "rating",
...     F.when(F.col("distance_km") < 5, "short")
...      .when(F.col("distance_km") < 10, "medium")
...      .otherwise("long").alias("length"),
...     F.when(F.col("rating") >= 4, "happy")
...      .when(F.col("rating").isNull(), "not rated")
...      .otherwise("unhappy").alias("mood"),
...     F.coalesce(F.col("rating"), F.lit(0)).alias("rating_or_0"))
>>> labeled.show(6)
+-------+-----------+------+------+---------+-----------+
|trip_id|distance_km|rating|length|     mood|rating_or_0|
+-------+-----------+------+------+---------+-----------+
|    t01|        5.2|     5|medium|    happy|          5|
|    t02|       12.0|     4|  long|    happy|          4|
|    t03|        3.1|  NULL| short|not rated|          0|
|    t04|        7.5|     5|medium|    happy|          5|
|    t05|       15.3|     4|  long|    happy|          4|
|    t06|        2.4|     3| short|  unhappy|          3|
+-------+-----------+------+------+---------+-----------+
only showing top 6 rows

>>> no_otherwise = trips.select(F.when(F.col("rating") >= 4, "good").alias("label"))
>>> no_otherwise.filter("label IS NULL").count()
7
```

**Notes:**

* **`F.when(condition, value)`**, chained with more `.when()`s and ended with
  `.otherwise(value)`, is Spark's **if / elif / else**, or SQL's `CASE WHEN`. The
  conditions are checked **in order**, and the first true one wins. So `"medium"` only
  needs `< 10`, because the `< 5` trips were already taken.
* **Without `otherwise()`**, a row that matches no condition gets `NULL`: here the 4
  unhappy trips **and** the 3 unrated ones.
* **`NULL` in a condition is not "false"**: `rating >= 4` is `NULL` when the rating is
  `NULL`. That's why `"not rated"` needs its own `isNull()` test. Without it, the
  unrated trips would fall through to `"unhappy"`.
* **`F.coalesce(a, b, ...)`** returns the first value that isn't `NULL`. With a
  `F.lit()` default, it replaces missing values. (Set 03 covers `na.fill()`, which does
  this for whole DataFrames.)

---

## Q5 (Intermediate) — String functions

```python
>>> drivers = spark.read.csv("data/drivers.csv", header=True)
>>> drivers.select("driver_id",
...                F.upper("name").alias("upper"),
...                F.lower("car_type").alias("lower"),
...                F.length("name").alias("len"),
...                F.concat_ws(" / ", "name", "car_type").alias("label"),
...                F.concat(F.lit("ID-"), F.substring("driver_id", 2, 1)).alias("code"),
...                F.lpad(F.substring("driver_id", 2, 1), 3, "0").alias("padded")).show()
+---------+-----+--------+---+--------------+----+------+
|driver_id|upper|   lower|len|         label|code|padded|
+---------+-----+--------+---+--------------+----+------+
|       d1|  ANA|   sedan|  3|   Ana / sedan|ID-1|   001|
|       d2|  BEN|     suv|  3|     Ben / suv|ID-2|   002|
|       d3|CHLOE|   sedan|  5| Chloe / sedan|ID-3|   003|
|       d4|  DAN|electric|  3|Dan / electric|ID-4|   004|
|       d5|  EVA|     suv|  3|     Eva / suv|ID-5|   005|
+---------+-----+--------+---+--------------+----+------+

>>> payments = spark.read.csv("data/payments.csv", header=True)
>>> payments.dtypes
[('payment_id', 'string'), ('trip_id', 'string'), ('paid_at', 'string'), ('method', 'string'), ('amount', 'string')]
>>> payments.select("payment_id", "amount",
...                 F.regexp_replace("amount", r"\$", "").cast("double").alias("amount_usd")).show(3)
+----------+------+----------+
|payment_id|amount|amount_usd|
+----------+------+----------+
|       p01|$17.40|      17.4|
|       p02|$33.00|      33.0|
|       p03| $9.75|      9.75|
+----------+------+----------+
only showing top 3 rows

>>> riders = spark.read.json("data/riders.json")
>>> riders.select("name",
...               F.concat_ws(", ", "home.city", "home.zip").alias("address"),
...               F.split(F.col("home.city"), " ").alias("words")).show(3, truncate=False)
+----+---------------+-----------+
|name|address        |words      |
+----+---------------+-----------+
|Maya|San Jose, 95112|[San, Jose]|
|Omar|San Jose, 95126|[San, Jose]|
|Lena|Oakland, 94607 |[Oakland]  |
+----+---------------+-----------+
only showing top 3 rows
```

**Notes:**

* `pyspark.sql.functions` has a function for almost every string job: `upper`, `lower`,
  `length`, `trim`, `substring`, `concat`, `concat_ws`, `lpad`/`rpad`, `split`,
  `regexp_replace`, `regexp_extract`, ...
* **`substring(col, pos, len)` counts from 1**, not 0 (it's the SQL convention):
  `substring("d1", 2, 1)` is `"1"`.
* **`concat_ws(separator, ...)`** joins with a separator, and **skips `NULL`s**. Plain
  `concat()` returns `NULL` if **any** part is `NULL`.
* **The payments file stores amounts as text**, such as `"$17.40"`, so `amount` is a
  `string`. `regexp_replace()` removes the `$` (in a regular expression, `$` is special,
  so it's written `\$`), and `cast("double")` makes it a number.
* `split()` gives an **array** column. Q7 works with arrays.

---

## Q6 (Intermediate) — Dates and timestamps

```python
>>> pay = (payments.withColumn("paid_at", F.to_timestamp("paid_at"))
...                .withColumn("amount", F.regexp_replace("amount", r"\$", "").cast("double")))
>>> pay.dtypes
[('payment_id', 'string'), ('trip_id', 'string'), ('paid_at', 'timestamp'), ('method', 'string'), ('amount', 'double')]

>>> pay.select("payment_id", "paid_at",
...            F.to_date("paid_at").alias("day"),
...            F.date_format("paid_at", "EEE").alias("weekday"),
...            F.dayofweek("paid_at").alias("dow"),
...            F.hour("paid_at").alias("h"),
...            F.date_format("paid_at", "MMM d, h:mm a").alias("pretty")).show(5, truncate=False)
+----------+-------------------+----------+-------+---+---+---------------+
|payment_id|paid_at            |day       |weekday|dow|h  |pretty         |
+----------+-------------------+----------+-------+---+---+---------------+
|p01       |2026-10-05 08:00:00|2026-10-05|Mon    |2  |8  |Oct 5, 8:00 AM |
|p02       |2026-10-06 09:13:00|2026-10-06|Tue    |3  |9  |Oct 6, 9:13 AM |
|p03       |2026-10-07 09:26:00|2026-10-07|Wed    |4  |9  |Oct 7, 9:26 AM |
|p04       |2026-10-08 12:39:00|2026-10-08|Thu    |5  |12 |Oct 8, 12:39 PM|
|p05       |2026-10-09 13:52:00|2026-10-09|Fri    |6  |13 |Oct 9, 1:52 PM |
+----------+-------------------+----------+-------+---+---+---------------+
only showing top 5 rows

>>> pay.select(F.min("paid_at"), F.max("paid_at")).show(truncate=False)
+-------------------+-------------------+
|min(paid_at)       |max(paid_at)       |
+-------------------+-------------------+
|2026-10-05 08:00:00|2026-10-11 18:18:00|
+-------------------+-------------------+

>>> pay.select("payment_id",
...            F.datediff(F.lit("2026-10-31"), F.to_date("paid_at")).alias("days_to_end"),
...            F.date_add(F.to_date("paid_at"), 30).alias("due")).show(3)
+----------+-----------+----------+
|payment_id|days_to_end|       due|
+----------+-----------+----------+
|       p01|         26|2026-11-04|
|       p02|         25|2026-11-05|
|       p03|         24|2026-11-06|
+----------+-----------+----------+
only showing top 3 rows

>>> pay.filter(F.to_date("paid_at") == "2026-10-05").count()
3
>>> pay.filter(F.dayofweek("paid_at").isin(1, 7)).count()
5
```

**Notes:**

* **`to_timestamp()`** turns a string into a `timestamp` (a date **and** a time).
  `"2026-10-05 08:00:00"` is the standard format, so it needs no format pattern. Q9 reads
  other formats.
* **`to_date()`** keeps only the date part. **Extract parts** with `year`, `month`,
  `dayofmonth`, `hour`, `minute`, `dayofweek`, ...
* **`dayofweek()` counts from Sunday = 1** to Saturday = 7, so the weekend is `isin(1, 7)`.
  (October 5, 2026 is a Monday, so it's day 2.)
* **`date_format(col, pattern)`** turns a date into **text**, using pattern letters:
  `EEE` = short weekday name, `MMM` = short month name, `d` = day, `h` = hour 1–12,
  `mm` = minutes, `a` = AM/PM.
* **Date arithmetic:** `datediff(end, start)` gives the days between two dates, and
  `date_add(date, n)` adds days. Both work on **dates**, so the timestamps are converted
  with `to_date()` first.
* `min()` and `max()` work on timestamps too: the payments cover October 5–11, one week.
* Comparing a date column with the string `"2026-10-05"` works, because Spark converts
  the string to a date.

---

## Q7 (Intermediate+) — Structs and arrays

```python
>>> riders.select("rider",
...               F.size("tags").alias("n_tags"),
...               F.array_contains("tags", "student").alias("student"),
...               F.coalesce("tags", F.array()).alias("tags_or_empty")).show()
+-----+------+-------+--------------------+
|rider|n_tags|student|       tags_or_empty|
+-----+------+-------+--------------------+
|   r1|     2|   true| [student, frequent]|
|   r2|     1|  false|          [frequent]|
|   r3|     0|  false|                  []|
|   r4|     1|  false|          [business]|
|   r5|     1|   true|           [student]|
|   r6|  NULL|   NULL|                  []|
|   r7|     2|  false|[business, frequent]|
+-----+------+-------+--------------------+

>>> riders.select("rider", F.explode("tags").alias("tag")).show()
+-----+--------+
|rider|     tag|
+-----+--------+
|   r1| student|
|   r1|frequent|
|   r2|frequent|
|   r4|business|
|   r5| student|
|   r7|business|
|   r7|frequent|
+-----+--------+

>>> riders.select("rider", F.explode_outer("tags").alias("tag")).filter("rider IN ('r3', 'r6')").show()
+-----+----+
|rider| tag|
+-----+----+
|   r3|NULL|
|   r6|NULL|
+-----+----+

>>> riders.select("rider", F.struct(F.col("name"), F.col("member_since").alias("since")).alias("info")).printSchema()
root
 |-- rider: string (nullable = true)
 |-- info: struct (nullable = false)
 |    |-- name: string (nullable = true)
 |    |-- since: long (nullable = true)

>>> riders.select("rider", F.col("home").getField("zip").alias("zip"), F.col("home.city").alias("city")).show(2)
+-----+-----+--------+
|rider|  zip|    city|
+-----+-----+--------+
|   r1|95112|San Jose|
|   r2|95126|San Jose|
+-----+-----+--------+
only showing top 2 rows
```

**Notes:**

* **Array functions:** `size()` counts the elements, and `array_contains()` checks for a
  value. On a `NULL` array (`r6`), both give `NULL`, not `0` or `false`.
  `coalesce(tags, array())` replaces a `NULL` array with an empty one.
* **`explode()`** makes **one row per array element**: it's the DataFrame version of
  `flatMap()` ([RDD Set 02, Q1](../rdd_practice_questions/ANSWERS_02.md#q1-basic--map-vs-flatmap)).
  5 riders with tags became 7 rows. Riders with an empty or `NULL` array disappear.
* **`explode_outer()`** keeps them, with a `NULL` element, just as a left outer join keeps
  unmatched rows.
* **Structs:** `F.struct(...)` builds a struct column from other columns, and
  `col.getField("zip")` (or the dot syntax `"home.zip"`) reads a field from one.
* After `explode()`, the usual tools work on the tags: for example,
  `groupBy("tag").count()` (Set 04) counts the riders per tag.

---

## Q8 (Intermediate+) — Working on many columns at once

```python
>>> trips.toDF(*[c.upper() for c in trips.columns]).columns
['TRIP_ID', 'DRIVER_ID', 'RIDER', 'CITY', 'HOUR', 'DISTANCE_KM', 'FARE', 'RATING']

>>> def camel(name):
...     return "".join(word.title() for word in name.split("_"))
...
>>> trips.select([F.col(c).alias(camel(c)) for c in trips.columns]).columns
['TripId', 'DriverId', 'Rider', 'City', 'Hour', 'DistanceKm', 'Fare', 'Rating']

>>> doubles = [f.name for f in trips.schema.fields if f.dataType.simpleString() == "double"]
>>> doubles
['distance_km', 'fare']
>>> rounded = trips.select([F.round(c).alias(c) if c in doubles else F.col(c) for c in trips.columns])
>>> rounded.show(3)
+-------+---------+-----+--------+----+-----------+----+------+
|trip_id|driver_id|rider|    city|hour|distance_km|fare|rating|
+-------+---------+-----+--------+----+-----------+----+------+
|    t01|       d1|   r1|San Jose|   8|        5.0|15.0|     5|
|    t02|       d2|   r2|San Jose|   9|       12.0|30.0|     4|
|    t03|       d1|   r3| Oakland|   9|        3.0|10.0|  NULL|
+-------+---------+-----+--------+----+-----------+----+------+
only showing top 3 rows

>>> looped = trips
>>> for c in doubles:
...     looped = looped.withColumn(c, F.round(c))
...
>>> sorted(looped.collect()) == sorted(rounded.collect())
True
>>> trips.withColumns({c: F.round(c) for c in doubles}).columns == rounded.columns
True
```

**Notes:**

* **Columns are just Python data**: a list of names (`df.columns`) and a schema
  (`df.schema`). So a **list comprehension** can build a list of column expressions, and
  one `select()` applies them all. This is how real code handles tables with 50 or 500
  columns, without typing each name.
* `toDF(*new_names)` renames **all** the columns at once, in order. `select()` with
  `alias()` does the same, but it can also change some columns and keep others.
* The pattern `[transform(c) if condition else F.col(c) for c in df.columns]` changes some
  columns and **keeps the column order**.
* **Three ways, the same result:** one `select()`, a loop of `withColumn()`, or one
  `withColumns()` with a dict. For a few columns, they're equivalent. For **many**
  columns, prefer `select()` or `withColumns()`: every `withColumn()` call adds a step
  to the query plan, and hundreds of them can make Spark slow just to *plan* the query.

---

## Q9 (Intermediate+) — Cleaning a messy payments file

```python
>>> raw = spark.read.csv("data/payments_raw.csv", header=True)
>>> clean = raw.select(
...     "payment_id", "trip_id",
...     F.coalesce(F.try_to_timestamp("paid_at", F.lit("yyyy-MM-dd HH:mm:ss")),
...                F.try_to_timestamp("paid_at", F.lit("MM/dd/yyyy HH:mm"))).alias("paid_at"),
...     F.lower(F.trim("method")).alias("method"),
...     F.regexp_replace("amount", r"[^0-9.]", "").try_cast("double").alias("amount"),
...     F.col("amount").alias("raw_amount"))
>>> clean.show(truncate=False)
+----------+-------+-------------------+------+------+----------+
|payment_id|trip_id|paid_at            |method|amount|raw_amount|
+----------+-------+-------------------+------+------+----------+
|p01       |t01    |2026-10-05 08:00:00|card  |17.4  |$17.40    |
|p02       |t02    |2026-10-06 09:13:00|app   |33.0  |$33.00    |
|p03       |t03    |2026-10-07 09:26:00|cash  |9.75  |$9.75     |
|p04       |t04    |2026-10-08 12:39:00|card  |22.8  |$22.80    |
|p05       |t05    |2026-10-09 13:52:00|app   |42.08 |USD 42.08 |
|p06       |t06    |2026-10-10 17:05:00|card  |8.8   |$8.80     |
|p07       |t07    |2026-10-11 18:18:00|app   |19.2  |$19.20    |
|p08       |t08    |2026-10-05 18:31:00|cash  |24.5  |$24.50    |
|p09       |t09    |2026-10-06 19:44:00|card  |13.2  |$13.20    |
|p10       |t10    |2026-10-07 19:57:00|app   |33.6  |$ 33.60   |
|p11       |t11    |2026-10-08 20:10:00|card  |23.1  |$23.10    |
|p12       |t12    |2026-10-09 21:23:00|app   |11.28 |$11.28    |
|p13       |t13    |2026-10-10 08:36:00|cash  |NULL  |N/A       |
|p14       |t14    |2026-10-11 09:49:00|card  |15.13 |$15.13    |
|p15       |t15    |2026-10-05 17:02:00|app   |19.25 |$19.25    |
|p16       |t16    |2026-10-06 18:15:00|card  |18.0  |$18.00    |
|p17       |t17    |2026-10-07 18:28:00|app   |27.5  |$27.50    |
|p18       |t18    |2026-10-08 19:41:00|cash  |NULL  |NULL      |
|p19       |t19    |2026-10-09 20:54:00|card  |9.0   |$9.00     |
|p20       |t20    |2026-10-10 22:07:00|app   |39.6  |$39.60    |
+----------+-------+-------------------+------+------+----------+

>>> clean.filter("amount IS NULL").select("payment_id", "raw_amount").show()
+----------+----------+
|payment_id|raw_amount|
+----------+----------+
|       p13|       N/A|
|       p18|      NULL|
+----------+----------+

>>> good = payments.select("payment_id", "trip_id",
...                        F.to_timestamp("paid_at").alias("paid_at"), "method",
...                        F.regexp_replace("amount", r"\$", "").cast("double").alias("amount"))
>>> (sorted(clean.drop("raw_amount").filter("amount IS NOT NULL").collect())
...  == sorted(good.filter("payment_id NOT IN ('p13', 'p18')").collect()))
True
```

**Notes:**

* **Mixed date formats:** `F.try_to_timestamp(col, format)` returns `NULL` when the text
  doesn't match the format, instead of failing (like `try_cast`). Trying the formats one
  after the other inside **`coalesce()`** takes the first one that works: `p03`, `p09`,
  and `p16` used `MM/dd/yyyy HH:mm`.
* **Messy categories:** `lower(trim(...))` turns `" App"`, `"CARD"`, `"Card"`, and
  `"App "` into clean `app` / `card` values. Without it, a count per method would have 7
  "different" methods instead of 3.
* **Messy numbers:** `regexp_replace(amount, "[^0-9.]", "")` deletes **everything that
  isn't a digit or a dot**, so `$17.40`, `USD 42.08`, and `$ 33.60` all become plain
  numbers. Then **`try_cast("double")`** turns them into numbers, and the `N/A` (now an
  empty string) into `NULL` instead of an ANSI error (Q3).
* **Keep the raw value** (`raw_amount`) next to the cleaned one while you work. It makes
  the problem rows easy to inspect, and to report: two payments have no amount.
* **Check your cleaning against a known answer:** apart from the two missing amounts, the
  cleaned raw file is **exactly** the clean `payments.csv`. Every format problem was fixed,
  and nothing else changed.
* The whole cleaning is **one `select()`**, so Spark reads the file once and applies all
  the fixes in a single pass.
