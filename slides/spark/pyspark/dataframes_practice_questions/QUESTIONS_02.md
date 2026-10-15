# PySpark DataFrame Practice Questions — Set 02: Working with Columns

Nine practice questions, three at each level, meant to be solved **interactively** in
the `bin/pyspark` shell. This set is about **columns**: selecting them, computing new
ones, changing their types, and using Spark's built-in functions for numbers, strings,
dates, structs, and arrays.

| # | Level | Topic | Input |
|---|---|---|---|
| [Q1](#q1-basic--selecting-columns-and-expressions) | Basic | Selecting columns and expressions | [`data/trips.csv`](data/trips.csv) |
| [Q2](#q2-basic--adding-replacing-renaming-and-dropping-columns) | Basic | Adding, replacing, renaming, and dropping columns | [`data/trips.csv`](data/trips.csv) |
| [Q3](#q3-basic--casting-and-rounding) | Basic | Casting and rounding | [`data/trips.csv`](data/trips.csv), Python list |
| [Q4](#q4-intermediate--conditional-columns-with-when) | Intermediate | Conditional columns with `when()` | [`data/trips.csv`](data/trips.csv) |
| [Q5](#q5-intermediate--string-functions) | Intermediate | String functions | [`data/drivers.csv`](data/drivers.csv), [`data/payments.csv`](data/payments.csv), [`data/riders.json`](data/riders.json) |
| [Q6](#q6-intermediate--dates-and-timestamps) | Intermediate | Dates and timestamps | [`data/payments.csv`](data/payments.csv) |
| [Q7](#q7-intermediate--structs-and-arrays) | Intermediate+ | Structs and arrays | [`data/riders.json`](data/riders.json) |
| [Q8](#q8-intermediate--working-on-many-columns-at-once) | Intermediate+ | Working on many columns at once | [`data/trips.csv`](data/trips.csv) |
| [Q9](#q9-intermediate--cleaning-a-messy-payments-file) | Intermediate+ | Cleaning a messy payments file | [`data/payments_raw.csv`](data/payments_raw.csv) |

The operations for this set:

* **Choosing and computing columns:** `select()`, `selectExpr()`, `withColumn()`,
  `withColumns()`, `withColumnRenamed()`, `drop()`, `alias()`, `F.col()`, `F.lit()`
* **Types:** `cast()`, `try_cast()`
* **Built-in functions** from `pyspark.sql.functions` (imported as `F`): numbers (`round`,
  `ceil`), conditions (`when`/`otherwise`, `coalesce`), strings (`upper`, `lower`,
  `trim`, `length`, `substring`, `concat`, `concat_ws`, `lpad`, `split`,
  `regexp_replace`), dates (`to_timestamp`, `try_to_timestamp`, `to_date`,
  `date_format`, `dayofweek`, `hour`, `datediff`, `date_add`), and structs and arrays
  (`struct`, `getField`, `size`, `array_contains`, `explode`, `explode_outer`)

You may also use everything from [Set 01](QUESTIONS_01.md).

Try each question on your own before you open the [answers](ANSWERS_02.md).

## Files

| File | Purpose |
|---|---|
| [QUESTIONS_02.md](QUESTIONS_02.md) | The nine questions (this page) |
| [ANSWERS_02.md](ANSWERS_02.md) | Step-by-step solutions, with real output from Spark 4.2.0 |
| [data/trips.csv](data/trips.csv) | The 20 trips from Set 01 |
| [data/drivers.csv](data/drivers.csv) | 5 drivers: `driver_id,name,car_type` |
| [data/riders.json](data/riders.json) | 7 riders, with a nested `home` and a `tags` list (from Set 01) |
| [data/payments.csv](data/payments.csv) | One payment per trip: `payment_id,trip_id,paid_at,method,amount`, with the amount stored as text like `$17.40` |
| [data/payments_raw.csv](data/payments_raw.csv) | The same payments, with **messy** dates, methods, and amounts, for Q9 |

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

---

## Q1 (Basic) — Selecting columns and expressions

**Tasks:**

1. Show `trip_id` and `fare` for the first 3 trips.
2. Do the same with `trip_id`, `city`, and `fare`, naming each column in a **different**
   way: with `F.col()`, as an attribute of `trips`, and with `trips[...]`.
3. Show each trip's **fare per km** (fare ÷ distance), as a column called `per_km`.
   Then show it again, rounded to 2 decimals.
4. Do task 3 (rounded) again with `selectExpr()` and SQL expressions, and also add the
   fare in **cents**, as `cents`.
5. What does `F.col("fare") / 2` print, all by itself?

**Expected output:**

```
+-------+----+
|trip_id|fare|
+-------+----+
|    t01|14.5|
|    t02|30.0|
|    t03|9.75|
+-------+----+
only showing top 3 rows

+-------+--------+----+
|trip_id|    city|fare|
+-------+--------+----+
|    t01|San Jose|14.5|
|    t02|San Jose|30.0|
|    t03| Oakland|9.75|
+-------+--------+----+
only showing top 3 rows

+-------+------------------+
|trip_id|            per_km|
+-------+------------------+
|    t01|2.7884615384615383|
|    t02|               2.5|
|    t03|3.1451612903225805|
+-------+------------------+
only showing top 3 rows

+-------+------+
|trip_id|per_km|
+-------+------+
|    t01|  2.79|
|    t02|   2.5|
|    t03|  3.15|
+-------+------+
only showing top 3 rows

+-------+------+------+
|trip_id|per_km| cents|
+-------+------+------+
|    t01|  2.79|1450.0|
|    t02|   2.5|3000.0|
|    t03|  3.15| 975.0|
+-------+------+------+
only showing top 3 rows

Column<'/(fare, 2)'>
```

---

## Q2 (Basic) — Adding, replacing, renaming, and dropping columns

**Tasks:**

1. With `withColumn()`, add two columns to `trips`: `per_km` (the fare per km, rounded
   to 2 decimals) and `currency` (the text `"USD"` on every row). Call the result
   `with_extra`, and print its columns.
2. With `withColumn()`, **replace** the `fare` of `with_extra` with double the fare.
   Show `trip_id` and `fare` for 2 trips.
3. Starting from `with_extra`, rename `fare` to `fare_usd`, and drop `currency`, `rider`,
   and `per_km`. Print the columns.
4. With **one** `withColumns()` call, add `fare_cents` (the fare in cents, as an `int`)
   and `is_long` (`True` if the trip is longer than 10 km). Show `trip_id`, `fare_cents`,
   and `is_long` for 3 trips.
5. Print the columns of `trips`. Did any of the tasks above change it?

**Expected output:**

```
['trip_id', 'driver_id', 'rider', 'city', 'hour', 'distance_km', 'fare', 'rating', 'per_km', 'currency']
+-------+----+
|trip_id|fare|
+-------+----+
|    t01|29.0|
|    t02|60.0|
+-------+----+
only showing top 2 rows

['trip_id', 'driver_id', 'city', 'hour', 'distance_km', 'fare_usd', 'rating']
+-------+----------+-------+
|trip_id|fare_cents|is_long|
+-------+----------+-------+
|    t01|      1450|  false|
|    t02|      3000|   true|
|    t03|       975|  false|
+-------+----------+-------+
only showing top 3 rows

['trip_id', 'driver_id', 'rider', 'city', 'hour', 'distance_km', 'fare', 'rating']
```

**Hint:** a constant value must be turned into a column with `F.lit()`.

---

## Q3 (Basic) — Casting and rounding

**Tasks:**

1. For 3 trips, show `trip_id`, `fare`, the fare **cast** to an `int` (`fare_int`), the
   fare **rounded** (`rounded`), the fare rounded **up** (`up`), and the hour as a
   **string** (`hour_str`). What's the difference between casting and rounding?
2. Make this small DataFrame of strings:

   ```python
   words = spark.createDataFrame([("42",), ("3.5",), ("abc",), (None,)], "s STRING")
   ```

   Show `s`, together with `s` converted to an `int` (`as_int`) and to a `double`
   (`as_double`) with **`try_cast()`**.
3. Now convert `s` to an `int` with a plain **`cast()`**, and `show()` it. What happens?
   Catch the error, and print whether its message contains `"CAST_INVALID_INPUT"` and
   whether it contains `"try_cast"`.

**Expected output:**

```
+-------+----+--------+-------+---+--------+
|trip_id|fare|fare_int|rounded| up|hour_str|
+-------+----+--------+-------+---+--------+
|    t01|14.5|      14|   15.0| 15|       8|
|    t02|30.0|      30|   30.0| 30|       9|
|    t03|9.75|       9|   10.0| 10|       9|
+-------+----+--------+-------+---+--------+
only showing top 3 rows

+----+------+---------+
|   s|as_int|as_double|
+----+------+---------+
|  42|    42|     42.0|
| 3.5|  NULL|      3.5|
| abc|  NULL|     NULL|
|NULL|  NULL|     NULL|
+----+------+---------+

True True
```

*Think about it:* in Spark 3, task 3 gave `NULL`s instead of an error. Why is the new
behavior safer?

---

## Q4 (Intermediate) — Conditional columns with when()

**Tasks:**

1. Show the first 6 trips with `trip_id`, `distance_km`, `rating`, and three new columns:
   * `length`: `"short"` under 5 km, `"medium"` under 10 km, otherwise `"long"`;
   * `mood`: `"happy"` for a rating of 4 or more, `"not rated"` when there's no rating,
     otherwise `"unhappy"`;
   * `rating_or_0`: the rating, or `0` when there's no rating.
2. Make a `label` column that's `"good"` when the rating is 4 or more, **without** an
   `otherwise()`. How many rows have a `NULL` label? Why?

**Expected output:**

```
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

7
```

*Think about it:* what would `mood` be for the unrated trips if you left out the
`isNull()` test?

---

## Q5 (Intermediate) — String functions

**Tasks:**

1. Read [`data/drivers.csv`](data/drivers.csv) (with a header) as `drivers`. For every
   driver, show `driver_id` and these columns:
   * `upper`: the name in capitals; `lower`: the car type in lowercase; `len`: the
     length of the name;
   * `label`: the name and the car type, joined with `" / "`;
   * `code`: `"ID-"` followed by the driver's **number** (the character after the `d`);
   * `padded`: the driver's number, padded to 3 digits with zeros (`001`).
2. Read [`data/payments.csv`](data/payments.csv) (with a header) as `payments`, and print
   its `dtypes`. What type is `amount`, and why?
3. Show `payment_id`, `amount`, and the amount as a `double` called `amount_usd`, for 3
   payments.
4. For 3 riders (`data/riders.json`, as `riders`), show the name, the address as
   `"city, zip"`, and the words of the city name as a list (without cutting the values).

**Expected output:**

```
+---------+-----+--------+---+--------------+----+------+
|driver_id|upper|   lower|len|         label|code|padded|
+---------+-----+--------+---+--------------+----+------+
|       d1|  ANA|   sedan|  3|   Ana / sedan|ID-1|   001|
|       d2|  BEN|     suv|  3|     Ben / suv|ID-2|   002|
|       d3|CHLOE|   sedan|  5| Chloe / sedan|ID-3|   003|
|       d4|  DAN|electric|  3|Dan / electric|ID-4|   004|
|       d5|  EVA|     suv|  3|     Eva / suv|ID-5|   005|
+---------+-----+--------+---+--------------+----+------+

[('payment_id', 'string'), ('trip_id', 'string'), ('paid_at', 'string'), ('method', 'string'), ('amount', 'string')]
+----------+------+----------+
|payment_id|amount|amount_usd|
+----------+------+----------+
|       p01|$17.40|      17.4|
|       p02|$33.00|      33.0|
|       p03| $9.75|      9.75|
+----------+------+----------+
only showing top 3 rows

+----+---------------+-----------+
|name|address        |words      |
+----+---------------+-----------+
|Maya|San Jose, 95112|[San, Jose]|
|Omar|San Jose, 95126|[San, Jose]|
|Lena|Oakland, 94607 |[Oakland]  |
+----+---------------+-----------+
only showing top 3 rows
```

**Hints:** `substring(col, pos, len)` counts positions from **1**. To remove the `$`, use
`regexp_replace()` (in a regular expression, `$` must be written as `\$`).

---

## Q6 (Intermediate) — Dates and timestamps

**Tasks:**

1. From `payments`, make `pay`: `paid_at` converted to a **timestamp**, and `amount`
   converted to a `double` (as in Q5). Print its `dtypes`.
2. For 5 payments, show `payment_id`, `paid_at`, and these columns (without cutting the
   values): `day` (the date only), `weekday` (the short weekday name, like `Mon`), `dow`
   (the day-of-week number), `h` (the hour), and `pretty` (like `Oct 5, 8:00 AM`).
3. Show the earliest and the latest payment time.
4. For 3 payments, show the number of days from the payment date to `2026-10-31`
   (`days_to_end`), and the date 30 days after the payment (`due`).
5. How many payments were made on **October 5**? How many on a **weekend**?

**Expected output:**

```
[('payment_id', 'string'), ('trip_id', 'string'), ('paid_at', 'timestamp'), ('method', 'string'), ('amount', 'double')]
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

+-------------------+-------------------+
|min(paid_at)       |max(paid_at)       |
+-------------------+-------------------+
|2026-10-05 08:00:00|2026-10-11 18:18:00|
+-------------------+-------------------+

+----------+-----------+----------+
|payment_id|days_to_end|       due|
+----------+-----------+----------+
|       p01|         26|2026-11-04|
|       p02|         25|2026-11-05|
|       p03|         24|2026-11-06|
+----------+-----------+----------+
only showing top 3 rows

3
5
```

**Hints:** the format letters for `date_format()` are `EEE` (weekday name), `MMM` (month
name), `d` (day), `h` (hour 1–12), `mm` (minutes), and `a` (AM/PM). `dayofweek()` counts
from **Sunday = 1**.

---

## Q7 (Intermediate+) — Structs and arrays

**Input:** `riders` from Q5.

**Tasks:**

1. For every rider, show `rider`, the **number** of tags (`n_tags`), whether the tags
   contain `"student"` (`student`), and the tags with a missing list replaced by an
   **empty** list (`tags_or_empty`). What happens to `r6`'s `n_tags` and `student`?
2. Make **one row per tag**: `rider` and `tag`. Which riders disappear?
3. Do the same, but **keep** the riders without tags. Show just `r3` and `r6`.
4. Build a struct column `info` with the rider's `name` and `member_since` (renamed to
   `since`). Print the schema.
5. For 2 riders, show `rider`, the `zip` (with `getField()`), and the `city` (with the
   dot syntax).

**Expected output:**

```
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

+-----+----+
|rider| tag|
+-----+----+
|   r3|NULL|
|   r6|NULL|
+-----+----+

root
 |-- rider: string (nullable = true)
 |-- info: struct (nullable = false)
 |    |-- name: string (nullable = true)
 |    |-- since: long (nullable = true)

+-----+-----+--------+
|rider|  zip|    city|
+-----+-----+--------+
|   r1|95112|San Jose|
|   r2|95126|San Jose|
+-----+-----+--------+
only showing top 2 rows
```

*Think about it:* which RDD transformation does `explode()` remind you of?

---

## Q8 (Intermediate+) — Working on many columns at once

Real tables often have dozens of columns. Instead of typing every name, build the list
of columns with Python.

**Tasks:**

1. Rename **all** the columns of `trips` to UPPERCASE, in one step. Print the columns.
2. Write a function `camel(name)` that turns `"distance_km"` into `"DistanceKm"`. With
   one `select()`, rename all the columns with it. Print the columns.
3. Using the schema, find the `double` columns. Then, with **one** `select()`, round
   **only** those columns to whole numbers, keep the other columns unchanged and in the
   same order, and show 3 rows.
4. Do task 3 again with a **loop** of `withColumn()` calls, and check that you get the same
   rows. Then do it once more with **one** `withColumns()` call, and check that the
   columns are the same.

**Expected output:**

```
['TRIP_ID', 'DRIVER_ID', 'RIDER', 'CITY', 'HOUR', 'DISTANCE_KM', 'FARE', 'RATING']
['TripId', 'DriverId', 'Rider', 'City', 'Hour', 'DistanceKm', 'Fare', 'Rating']
['distance_km', 'fare']
+-------+---------+-----+--------+----+-----------+----+------+
|trip_id|driver_id|rider|    city|hour|distance_km|fare|rating|
+-------+---------+-----+--------+----+-----------+----+------+
|    t01|       d1|   r1|San Jose|   8|        5.0|15.0|     5|
|    t02|       d2|   r2|San Jose|   9|       12.0|30.0|     4|
|    t03|       d1|   r3| Oakland|   9|        3.0|10.0|  NULL|
+-------+---------+-----+--------+----+-----------+----+------+
only showing top 3 rows

True
True
```

*Think about it:* for a table with 500 columns, why is one `select()` better than 500
`withColumn()` calls?

---

## Q9 (Intermediate+) — Cleaning a messy payments file

**Input:** [`data/payments_raw.csv`](data/payments_raw.csv) (with a header), the same 20
payments as `payments.csv`, but with realistic problems:

* `paid_at`: most dates look like `2026-10-05 08:00:00`, but some look like `10/07/2026 09:26`;
* `method`: `card`, `app`, and `cash`, but sometimes in capitals or with extra spaces (`" App"`, `"CARD"`);
* `amount`: mostly like `$17.40`, but also `USD 42.08`, `$ 33.60`, `N/A`, and one empty value.

**Tasks:**

1. With **one** `select()`, build `clean` with these columns:
   * `payment_id` and `trip_id`, unchanged;
   * `paid_at`: a timestamp, whichever of the two formats the value uses;
   * `method`: lowercase, without extra spaces;
   * `amount`: a `double`, or `NULL` if there's no valid number;
   * `raw_amount`: the original amount text, kept for checking.

   Show all the rows (without cutting the values).
2. Show the payments whose amount couldn't be cleaned, with their `raw_amount`.
3. Check your work: apart from those, `clean` (without `raw_amount`) must contain
   **exactly** the same rows as the clean file `payments.csv`, with its `paid_at` as a
   timestamp and its `amount` as a `double`.

**Expected output:**

```
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

+----------+----------+
|payment_id|raw_amount|
+----------+----------+
|       p13|       N/A|
|       p18|      NULL|
+----------+----------+

True
```

**Hints:** `F.try_to_timestamp(col, F.lit(format))` returns `NULL` when the value doesn't
match the format, and `F.coalesce()` picks the first non-`NULL` value. The regular
expression `[^0-9.]` matches every character that **isn't** a digit or a dot.
