# PySpark DataFrame Practice Questions — Set 07: Spark SQL

Nine practice questions, three at each level, meant to be solved **interactively** in
the `bin/pyspark` shell. This set is about **Spark SQL**: running SQL queries on
DataFrames. Many questions are ones you already solved with the DataFrame API in Sets
02–06, so you can compare the two styles side by side.

```python
trips.createOrReplaceTempView("trips")                       # give the DataFrame a name for SQL
spark.sql("SELECT city, COUNT(*) FROM trips GROUP BY city")  # returns a DataFrame
```

| # | Level | Topic | Input |
|---|---|---|---|
| [Q1](#q1-basic--temporary-views-and-sparksql) | Basic | Temporary views and `spark.sql()` | all four tables |
| [Q2](#q2-basic--group-by-and-having-in-sql-and-with-the-dataframe-api) | Basic | `GROUP BY` and `HAVING`, in SQL and with the DataFrame API | [`trips.csv`](data/trips.csv) |
| [Q3](#q3-basic--sql-expressions-also-inside-dataframe-code) | Basic | SQL expressions, also inside DataFrame code | [`trips.csv`](data/trips.csv) |
| [Q4](#q4-intermediate--joins-in-sql) | Intermediate | Joins in SQL | [`trips.csv`](data/trips.csv), [`drivers.csv`](data/drivers.csv), [`riders.json`](data/riders.json) |
| [Q5](#q5-intermediate--subqueries-and-the-not-in-trap) | Intermediate | Subqueries, and the `NOT IN` trap | [`trips.csv`](data/trips.csv), [`drivers.csv`](data/drivers.csv), Python list |
| [Q6](#q6-intermediate--step-by-step-with-with-ctes) | Intermediate | Step by step with `WITH` (CTEs) | [`trips.csv`](data/trips.csv) |
| [Q7](#q7-intermediate--window-functions-in-sql-and-qualify) | Intermediate+ | Window functions in SQL, and `QUALIFY` | [`trips.csv`](data/trips.csv), [`payments.csv`](data/payments.csv) |
| [Q8](#q8-intermediate--parameters-dataframes-in-sql-and-view-lifetimes) | Intermediate+ | Parameters, DataFrames in SQL, and view lifetimes | [`trips.csv`](data/trips.csv) |
| [Q9](#q9-intermediate--a-whole-cleaning-and-report-pipeline-in-sql) | Intermediate+ | A whole cleaning-and-report pipeline in SQL | [`payments_raw.csv`](data/payments_raw.csv), [`trips.csv`](data/trips.csv) |

The operations for this set:

* **Views:** `df.createOrReplaceTempView(name)`, `df.createOrReplaceGlobalTempView(name)`,
  `spark.catalog.listTables()`, `spark.catalog.dropTempView(name)`
* **Queries:** `spark.sql(query)`, `spark.sql(query, args={...})`, and `spark.sql(query, df=...)`
* **SQL inside DataFrame code:** `F.expr("...")`, `filter("...")`, `selectExpr("...")`
* **SQL:** `SELECT`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `CASE WHEN`, `JOIN`
  (including `LEFT SEMI` and `LEFT ANTI`), subqueries (`IN`, `EXISTS`, scalar), `WITH`,
  `OVER (...)`, and `QUALIFY`

You may also use everything from Sets [01](QUESTIONS_01.md)–[06](QUESTIONS_06.md).

Try each question on your own before you open the [answers](ANSWERS_07.md).

## Files

| File | Purpose |
|---|---|
| [QUESTIONS_07.md](QUESTIONS_07.md) | The nine questions (this page) |
| [ANSWERS_07.md](ANSWERS_07.md) | Step-by-step solutions, with real output from Spark 4.2.0 |
| [data/trips.csv](data/trips.csv), [data/drivers.csv](data/drivers.csv), [data/riders.json](data/riders.json), [data/payments.csv](data/payments.csv) | The four tables of the earlier sets |
| [data/payments_raw.csv](data/payments_raw.csv) | The messy payments of Set 02, Q9, for Q9 |

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
payments = spark.read.csv("data/payments.csv", header=True)
```

**Tip:** write longer queries as **triple-quoted** strings, one clause per line:

```python
spark.sql("""
    SELECT city, COUNT(*) AS trips
    FROM trips
    GROUP BY city
    ORDER BY city""").show()
```

---

## Q1 (Basic) — Temporary views and spark.sql()

**Tasks:**

1. Make temporary views called `trips`, `drivers`, `riders`, and `payments` from the four
   DataFrames.
2. With SQL, show `trip_id`, `city`, and `fare` of the trips with a fare over 30, most
   expensive first. What **type** of object does `spark.sql()` return?
3. List the names of the views (sorted), with `spark.catalog.listTables()`.
4. Show the first 3 rows of `DESCRIBE trips`.

**Expected output:**

```
+-------+--------+-----+
|trip_id|    city| fare|
+-------+--------+-----+
|    t13|San Jose|110.0|
|    t05| Oakland|38.25|
|    t20|San Jose| 36.0|
+-------+--------+-----+

'DataFrame'
['drivers', 'payments', 'riders', 'trips']
+---------+---------+-------+
| col_name|data_type|comment|
+---------+---------+-------+
|  trip_id|   string|   NULL|
|driver_id|   string|   NULL|
|    rider|   string|   NULL|
+---------+---------+-------+
only showing top 3 rows
```

---

## Q2 (Basic) — GROUP BY and HAVING, in SQL and with the DataFrame API

**Tasks:**

1. With SQL, show for each city: the number of trips (`trips`), the total fare (`revenue`),
   and the average rating (`avg_rating`), both rounded to 2 decimals. Keep only the cities
   with **more than 5 trips**, sorted by city. Call the result `by_sql`.
2. Write the same query with the **DataFrame API**, as `by_api`, and check that both give the
   same rows.

**Expected output:**

```
+--------+-----+-------+----------+
|    city|trips|revenue|avg_rating|
+--------+-----+-------+----------+
| Oakland|    8| 168.25|       4.0|
|San Jose|   12|  308.0|      4.09|
+--------+-----+-------+----------+

True
```

*Think about it:* which DataFrame method does SQL's `HAVING` correspond to?

---

## Q3 (Basic) — SQL expressions, also inside DataFrame code

**Tasks:**

1. With SQL, show the first 4 trips (by `trip_id`) with `trip_id`, `fare`, and:
   * `length`: `"short"` under 5 km, `"medium"` under 10 km, otherwise `"long"` (with `CASE WHEN`);
   * `rating_or_0`: the rating, or 0 if there's none;
   * `fare_int`: the fare as an `INT`;
   * `city`: the city in capitals.
2. In **DataFrame** code, use `F.expr()` with the SQL expression `round(fare / distance_km, 2)`
   to show `trip_id` and `per_km` for 2 trips.
3. In DataFrame code, `filter()` with the **SQL string** `"city = 'Oakland' AND rating >= 5"`,
   and show the trip ids, sorted.
4. With SQL, show `try_cast('abc' AS INT)`, `try_cast('42' AS INT)`, and `try_divide(10, 0)`,
   as `a`, `b`, and `c`.

**Expected output:**

```
+-------+----+------+-----------+--------+--------+
|trip_id|fare|length|rating_or_0|fare_int|    city|
+-------+----+------+-----------+--------+--------+
|    t01|14.5|medium|          5|      14|SAN JOSE|
|    t02|30.0|  long|          4|      30|SAN JOSE|
|    t03|9.75| short|          0|       9| OAKLAND|
|    t04|19.0|medium|          5|      19|SAN JOSE|
+-------+----+------+-----------+--------+--------+

+-------+------+
|trip_id|per_km|
+-------+------+
|    t01|  2.79|
|    t02|   2.5|
+-------+------+
only showing top 2 rows

+-------+
|trip_id|
+-------+
|    t08|
|    t17|
+-------+

+----+---+----+
|   a|  b|   c|
+----+---+----+
|NULL| 42|NULL|
+----+---+----+
```

---

## Q4 (Intermediate) — Joins in SQL

**Tasks** (all in SQL, with table aliases):

1. Show the total fare per car type (rounded to 2 decimals), sorted.
2. Show every driver's name with their number of trips, **including** drivers with no trips.
3. Show the drivers with **no** trips, with a `LEFT ANTI JOIN`.
4. Show `rider` and `name` of the riders who took **at least one trip in Oakland**, with a
   `LEFT SEMI JOIN`.

**Expected output:**

```
+--------+-------+
|car_type|revenue|
+--------+-------+
|electric| 185.25|
|   sedan|  202.0|
|     suv|   89.0|
+--------+-------+

+-----+-----+
| name|trips|
+-----+-----+
|  Ana|    6|
|  Ben|    4|
|Chloe|    6|
|  Dan|    4|
|  Eva|    0|
+-----+-----+

+---------+----+--------+
|driver_id|name|car_type|
+---------+----+--------+
|       d5| Eva|     suv|
+---------+----+--------+

+-----+----+
|rider|name|
+-----+----+
|   r3|Lena|
|   r4|Kofi|
|   r5|Ines|
|   r6| Tom|
+-----+----+
```

---

## Q5 (Intermediate) — Subqueries, and the NOT IN trap

**Tasks:**

1. Show `trip_id`, `city`, and `fare` of the trips whose fare is **above the average fare of
   their own city**, sorted by city and fare (highest first). Use a **correlated scalar
   subquery**.
2. Show the names of the drivers who had a **1-star** trip, with `IN (subquery)`.
3. Show the names of the drivers who drove **at least once in Oakland**, with `EXISTS`.
4. Make a view `blocked` of blocked driver ids, from this DataFrame (note the `None`):

   ```python
   spark.createDataFrame([("d1",), (None,)], "driver_id STRING")
   ```

   Count the drivers whose id is `NOT IN` the blocked list. Is that the answer you expected?
   Then show the names of the drivers who are **not** blocked, correctly, with `NOT EXISTS`.

**Expected output:**

```
+-------+--------+-----+
|trip_id|    city| fare|
+-------+--------+-----+
|    t05| Oakland|38.25|
|    t17| Oakland| 25.0|
|    t08| Oakland| 24.5|
|    t13|San Jose|110.0|
|    t20|San Jose| 36.0|
|    t02|San Jose| 30.0|
|    t10|San Jose| 28.0|
+-------+--------+-----+

+----+
|name|
+----+
| Dan|
+----+

+-----+
| name|
+-----+
|  Ana|
|Chloe|
|  Dan|
+-----+

0
+-----+
| name|
+-----+
|  Ben|
|Chloe|
|  Dan|
|  Eva|
+-----+
```

---

## Q6 (Intermediate) — Step by step with WITH (CTEs)

**Task:** with **one** SQL query that uses two CTEs, find the **top 2 drivers of each city**
by revenue, with their `rank` and their `share` of the city's revenue (a whole percentage):

* the CTE `revenue`: each driver's revenue per city (rounded to 2 decimals);
* the CTE `ranked`: `revenue` plus the `rank` (`ROW_NUMBER()`, ties broken by `driver_id`) and
  the `share`, both computed with window functions (`OVER (PARTITION BY city ...)`);
* the final `SELECT`: the rows of `ranked` with `rank <= 2`, sorted by city and rank.

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

*Think about it:* this is [Set 06, Q4](QUESTIONS_06.md#q4-intermediate--the-top-2-drivers-in-each-city).
Which version do you find easier to read?

---

## Q7 (Intermediate+) — Window functions in SQL, and QUALIFY

**Tasks** (all in SQL):

1. Show `city`, `trip_id`, and `fare` of the 2 most expensive trips of each city, using
   **`QUALIFY`** (no CTE, no subquery), sorted by city and fare (highest first).
2. From the `payments` view, show per **day** (the date of `paid_at`): the number of payments,
   the total **paid amount** (the `amount` text without the `$`, as a `DOUBLE`, rounded to 2
   decimals), and the **running total** of the paid amounts (rounded to 2 decimals).
3. Show rider `r2`'s trips (`trip_id`, `rider`, `fare`) with the **previous** trip's fare, in
   `trip_id` order, using `LAG()`.

**Expected output:**

```
+--------+-------+-----+
|    city|trip_id| fare|
+--------+-------+-----+
| Oakland|    t05|38.25|
| Oakland|    t17| 25.0|
|San Jose|    t13|110.0|
|San Jose|    t20| 36.0|
+--------+-------+-----+

+----------+-----+-----+-------+
|       day|trips| paid|running|
+----------+-----+-----+-------+
|2026-10-05|    3|61.15|  61.15|
|2026-10-06|    3| 64.2| 125.35|
|2026-10-07|    3|70.85|  196.2|
|2026-10-08|    3|66.15| 262.35|
|2026-10-09|    3|62.36| 324.71|
|2026-10-10|    3|158.4| 483.11|
|2026-10-11|    2|34.33| 517.44|
+----------+-----+-----+-------+

+-------+-----+----+---------+
|trip_id|rider|fare|prev_fare|
+-------+-----+----+---------+
|    t02|   r2|30.0|     NULL|
|    t06|   r2| 8.0|     30.0|
|    t10|   r2|28.0|      8.0|
|    t16|   r2|15.0|     28.0|
+-------+-----+----+---------+
```

**Hint for task 2:** a window function can be applied to an aggregate:
`SUM(SUM(x)) OVER (ORDER BY ... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`.

---

## Q8 (Intermediate+) — Parameters, DataFrames in SQL, and view lifetimes

**Tasks:**

1. Show `trip_id` and `fare` of the trips in a given city above a given fare, using the
   **named parameters** `:city` and `:min_fare`, with `args={"city": "Oakland", "min_fare": 20}`.
2. **SQL injection:** set `evil = "Oakland' OR '1'='1"`. Count the trips with the query built
   as an **f-string**, `f"SELECT COUNT(*) FROM trips WHERE city = '{evil}'"`. Then count them
   with a **parameter** instead. Explain the two results.
3. Without creating a view, count the San Jose trips by passing the DataFrame `trips` into the
   query as `{t}` (with `t=trips`), and the city as a parameter.
4. Make a **global** temporary view `all_trips` of `trips`. Create a **new session** with
   `spark.newSession()`, and list **its** temporary views. Can it read `global_temp.all_trips`?
   Count its rows.
5. Drop the temporary view `blocked` from Q5. What does `dropTempView()` return?

**Expected output:**

```
+-------+-----+
|trip_id| fare|
+-------+-----+
|    t05|38.25|
|    t08| 24.5|
|    t11| 21.0|
|    t17| 25.0|
|    t18|20.25|
+-------+-----+

20
0
+---+
|  n|
+---+
| 12|
+---+

[]
20
True
```

---

## Q9 (Intermediate+) — A whole cleaning-and-report pipeline in SQL

**Input:** [`data/payments_raw.csv`](data/payments_raw.csv), the messy payments of
[Set 02, Q9](QUESTIONS_02.md#q9-intermediate--cleaning-a-messy-payments-file). Read it with a
header, and make a view `payments_raw`.

**Tasks:**

1. With **one** SQL query and two CTEs, build the tip report of
   [Set 05, Q5](QUESTIONS_05.md#q5-intermediate--joining-four-tables-and-broadcast-joins):
   * the CTE `clean`: the payments with `paid_at` as a timestamp (either format), `method` in
     lowercase without spaces, and `amount` as a `DOUBLE` (or `NULL` if invalid), using the
     same fixes as in Set 02, Q9;
   * the CTE `paid`: `clean` joined with `trips`, keeping only the payments **with** an amount,
     plus the `fare` and the `tip` (amount minus fare, rounded to 2 decimals);
   * the final `SELECT`: per method, the number of trips, the total tips (rounded to 2
     decimals), and the average tip percentage (rounded to 1 decimal).
2. List the payments whose amount could **not** be cleaned (`payment_id` and the raw `amount`).
3. Compare your report with Set 05, Q5. Why does `cash` have fewer trips here?

**Expected output:**

```
+------+-----+-----+-----------+
|method|trips| tips|avg_tip_pct|
+------+-----+-----+-----------+
|   app|    8|24.51|       12.5|
|  card|    8|16.68|       15.0|
|  cash|    2|  0.0|        0.0|
+------+-----+-----+-----------+

+----------+------+
|payment_id|amount|
+----------+------+
|       p13|   N/A|
|       p18|  NULL|
+----------+------+
```

**Hint:** the SQL functions have the same names as the `F.` functions:
`try_to_timestamp(col, 'format')`, `COALESCE`, `LOWER`, `TRIM`, `REGEXP_REPLACE`,
`try_cast(... AS DOUBLE)`.
