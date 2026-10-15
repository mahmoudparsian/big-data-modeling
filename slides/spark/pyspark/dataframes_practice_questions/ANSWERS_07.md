# PySpark DataFrame Practice Questions — Set 07: Answers

Solutions to the nine [Set 07 practice questions](QUESTIONS_07.md). Every session below
was run in the `bin/pyspark` shell (Spark 4.2.0), started from this folder. The output is
copied from those real runs.

Try the questions yourself first. There are often several correct solutions, and yours
doesn't need to match these exactly.

* [Q1 (Basic): Temporary views and spark.sql()](#q1-basic--temporary-views-and-sparksql)
* [Q2 (Basic): GROUP BY and HAVING, in SQL and with the DataFrame API](#q2-basic--group-by-and-having-in-sql-and-with-the-dataframe-api)
* [Q3 (Basic): SQL expressions, also inside DataFrame code](#q3-basic--sql-expressions-also-inside-dataframe-code)
* [Q4 (Intermediate): Joins in SQL](#q4-intermediate--joins-in-sql)
* [Q5 (Intermediate): Subqueries, and the NOT IN trap](#q5-intermediate--subqueries-and-the-not-in-trap)
* [Q6 (Intermediate): Step by step with WITH (CTEs)](#q6-intermediate--step-by-step-with-with-ctes)
* [Q7 (Intermediate+): Window functions in SQL, and QUALIFY](#q7-intermediate--window-functions-in-sql-and-qualify)
* [Q8 (Intermediate+): Parameters, DataFrames in SQL, and view lifetimes](#q8-intermediate--parameters-dataframes-in-sql-and-view-lifetimes)
* [Q9 (Intermediate+): A whole cleaning-and-report pipeline in SQL](#q9-intermediate--a-whole-cleaning-and-report-pipeline-in-sql)

Setup, used by all the questions:

```python
>>> from pyspark.sql import functions as F
>>> trips_ddl = ("trip_id STRING, driver_id STRING, rider STRING, city STRING, "
...              "hour INT, distance_km DOUBLE, fare DOUBLE, rating INT")
>>> trips = spark.read.csv("data/trips.csv", header=True, schema=trips_ddl)
>>> drivers = spark.read.csv("data/drivers.csv", header=True)
>>> riders = spark.read.json("data/riders.json")
>>> payments = spark.read.csv("data/payments.csv", header=True)
```

---

## Q1 (Basic) — Temporary views and spark.sql()

```python
>>> trips.createOrReplaceTempView("trips")
>>> drivers.createOrReplaceTempView("drivers")
>>> riders.createOrReplaceTempView("riders")
>>> payments.createOrReplaceTempView("payments")

>>> expensive = spark.sql("SELECT trip_id, city, fare FROM trips WHERE fare > 30 ORDER BY fare DESC")
>>> expensive.show()
+-------+--------+-----+
|trip_id|    city| fare|
+-------+--------+-----+
|    t13|San Jose|110.0|
|    t05| Oakland|38.25|
|    t20|San Jose| 36.0|
+-------+--------+-----+

>>> type(expensive).__name__
'DataFrame'
>>> sorted(t.name for t in spark.catalog.listTables())
['drivers', 'payments', 'riders', 'trips']
>>> spark.sql("DESCRIBE trips").show(3)
+---------+---------+-------+
| col_name|data_type|comment|
+---------+---------+-------+
|  trip_id|   string|   NULL|
|driver_id|   string|   NULL|
|    rider|   string|   NULL|
+---------+---------+-------+
only showing top 3 rows
```

**Notes:**

* **`df.createOrReplaceTempView("name")`** gives a DataFrame a **name** that SQL can use, like
  a table. It doesn't copy or store any data. The view is just a name for the DataFrame's
  recipe. ("Or replace" means that calling it again with the same name replaces the old view.)
* **`spark.sql(query)`** runs a SQL query and returns a **DataFrame**. So you can mix freely:
  call `.show()`, `.filter()`, `.join()`... on the result, or make a view of it for another query.
* SQL and the DataFrame API are **two ways to write the same thing**. Both go through the same
  optimizer (Catalyst), so there's no speed difference. Use whichever is clearer for the job,
  or for your team.
* `spark.catalog.listTables()` lists the views, and SQL commands like `SHOW TABLES` and
  `DESCRIBE trips` work too.

---

## Q2 (Basic) — GROUP BY and HAVING, in SQL and with the DataFrame API

```python
>>> by_sql = spark.sql("""
...     SELECT city,
...            COUNT(*) AS trips,
...            ROUND(SUM(fare), 2) AS revenue,
...            ROUND(AVG(rating), 2) AS avg_rating
...     FROM trips
...     GROUP BY city
...     HAVING COUNT(*) > 5
...     ORDER BY city""")
>>> by_sql.show()
+--------+-----+-------+----------+
|    city|trips|revenue|avg_rating|
+--------+-----+-------+----------+
| Oakland|    8| 168.25|       4.0|
|San Jose|   12|  308.0|      4.09|
+--------+-----+-------+----------+

>>> by_api = (trips.groupBy("city")
...                .agg(F.count("*").alias("trips"),
...                     F.round(F.sum("fare"), 2).alias("revenue"),
...                     F.round(F.avg("rating"), 2).alias("avg_rating"))
...                .filter("trips > 5")
...                .orderBy("city"))
>>> by_sql.collect() == by_api.collect()
True
```

**Notes:**

* Each SQL clause has a DataFrame method:

  | SQL | DataFrame API |
  |---|---|
  | `SELECT ... AS name` | `select(...)`, `alias("name")` |
  | `FROM trips` | the DataFrame `trips` |
  | `WHERE` (rows, before grouping) | `filter()` before `groupBy()` |
  | `GROUP BY city` | `groupBy("city")` |
  | aggregates (`COUNT`, `SUM`, ...) | `agg(F.count(...), ...)` |
  | `HAVING` (groups, after grouping) | `filter()` after `agg()` |
  | `ORDER BY` | `orderBy()` |
  | `LIMIT n` | `limit(n)` |

* SQL is written in this order (SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY), but it **runs**
  in the order FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY. The DataFrame API is
  written in the order it runs.
* A triple-quoted Python string (`"""..."""`) lets a query span several lines, which makes long
  SQL much easier to read.

---

## Q3 (Basic) — SQL expressions, also inside DataFrame code

```python
>>> spark.sql("""
...     SELECT trip_id, fare,
...            CASE WHEN distance_km < 5 THEN 'short'
...                 WHEN distance_km < 10 THEN 'medium'
...                 ELSE 'long' END AS length,
...            COALESCE(rating, 0) AS rating_or_0,
...            CAST(fare AS INT) AS fare_int,
...            UPPER(city) AS city
...     FROM trips
...     ORDER BY trip_id
...     LIMIT 4""").show()
+-------+----+------+-----------+--------+--------+
|trip_id|fare|length|rating_or_0|fare_int|    city|
+-------+----+------+-----------+--------+--------+
|    t01|14.5|medium|          5|      14|SAN JOSE|
|    t02|30.0|  long|          4|      30|SAN JOSE|
|    t03|9.75| short|          0|       9| OAKLAND|
|    t04|19.0|medium|          5|      19|SAN JOSE|
+-------+----+------+-----------+--------+--------+

>>> trips.select("trip_id", F.expr("round(fare / distance_km, 2) AS per_km")).show(2)
+-------+------+
|trip_id|per_km|
+-------+------+
|    t01|  2.79|
|    t02|   2.5|
+-------+------+
only showing top 2 rows

>>> trips.filter("city = 'Oakland' AND rating >= 5").select("trip_id").orderBy("trip_id").show()
+-------+
|trip_id|
+-------+
|    t08|
|    t17|
+-------+

>>> spark.sql("SELECT try_cast('abc' AS INT) AS a, try_cast('42' AS INT) AS b, try_divide(10, 0) AS c").show()
+----+---+----+
|   a|  b|   c|
+----+---+----+
|NULL| 42|NULL|
+----+---+----+
```

**Notes:**

* **`CASE WHEN ... THEN ... ELSE ... END`** is SQL's `F.when(...).otherwise(...)`
  ([Set 02, Q4](ANSWERS_02.md#q4-intermediate--conditional-columns-with-when)). Most `F.`
  functions have a SQL function with the same name: `ROUND`, `COALESCE`, `UPPER`,
  `REGEXP_REPLACE`, `DATE_FORMAT`, ...
* **SQL inside DataFrame code:** `F.expr("...")` turns a SQL expression into a `Column`, and
  `filter("...")`, `selectExpr("...")` accept SQL directly. Use it when the SQL is shorter or
  clearer, as for a compound condition.
* **The ANSI "try" functions** exist in SQL too: `try_cast()` and `try_divide()` return `NULL`
  instead of failing (dividing by zero is an error under ANSI mode).
* SQL keywords are case-insensitive (`SELECT` or `select`). Writing them in capitals is just a
  convention that makes queries easier to read.

---

## Q4 (Intermediate) — Joins in SQL

```python
>>> spark.sql("""
...     SELECT d.car_type, ROUND(SUM(t.fare), 2) AS revenue
...     FROM trips t JOIN drivers d ON t.driver_id = d.driver_id
...     GROUP BY d.car_type
...     ORDER BY d.car_type""").show()
+--------+-------+
|car_type|revenue|
+--------+-------+
|electric| 185.25|
|   sedan|  202.0|
|     suv|   89.0|
+--------+-------+

>>> spark.sql("""
...     SELECT d.name, COUNT(t.trip_id) AS trips
...     FROM drivers d LEFT JOIN trips t ON d.driver_id = t.driver_id
...     GROUP BY d.name
...     ORDER BY d.name""").show()
+-----+-----+
| name|trips|
+-----+-----+
|  Ana|    6|
|  Ben|    4|
|Chloe|    6|
|  Dan|    4|
|  Eva|    0|
+-----+-----+

>>> spark.sql("SELECT * FROM drivers d LEFT ANTI JOIN trips t ON d.driver_id = t.driver_id").show()
+---------+----+--------+
|driver_id|name|car_type|
+---------+----+--------+
|       d5| Eva|     suv|
+---------+----+--------+

>>> spark.sql("""
...     SELECT r.rider, r.name
...     FROM riders r LEFT SEMI JOIN trips t ON r.rider = t.rider AND t.city = 'Oakland'
...     ORDER BY r.rider""").show()
+-----+----+
|rider|name|
+-----+----+
|   r3|Lena|
|   r4|Kofi|
|   r5|Ines|
|   r6| Tom|
+-----+----+
```

**Notes:**

* **Table aliases** (`trips t`, `drivers d`) keep SQL joins short and unambiguous: `t.driver_id`
  vs. `d.driver_id`. In SQL, you always say which table a column comes from, so the "ambiguous
  column" problem of [Set 05, Q3](ANSWERS_05.md#q3-basic--different-key-names-and-ambiguous-columns)
  doesn't happen.
* All the DataFrame join types exist in SQL: `[INNER] JOIN`, `LEFT JOIN`, `RIGHT JOIN`,
  `FULL [OUTER] JOIN`, and Spark's `LEFT SEMI JOIN` and `LEFT ANTI JOIN`.
* **Conditions in `ON`:** putting `t.city = 'Oakland'` **in the join condition** of the semi join
  means "riders who have at least one Oakland trip". These are exactly the riders that the anti
  join of [Set 05, Q4](ANSWERS_05.md#q4-intermediate--semi-joins-and-anti-joins) left out.
* `COUNT(t.trip_id)` with a `LEFT JOIN` gives Eva 0 trips, the same trick as in Set 05, Q2.

---

## Q5 (Intermediate) — Subqueries, and the NOT IN trap

```python
>>> spark.sql("""
...     SELECT trip_id, city, fare FROM trips t
...     WHERE fare > (SELECT AVG(fare) FROM trips t2 WHERE t2.city = t.city)
...     ORDER BY city, fare DESC""").show()
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

>>> spark.sql("SELECT name FROM drivers WHERE driver_id IN (SELECT driver_id FROM trips WHERE rating = 1)").show()
+----+
|name|
+----+
| Dan|
+----+

>>> spark.sql("""
...     SELECT name FROM drivers d
...     WHERE EXISTS (SELECT 1 FROM trips t WHERE t.driver_id = d.driver_id AND t.city = 'Oakland')
...     ORDER BY name""").show()
+-----+
| name|
+-----+
|  Ana|
|Chloe|
|  Dan|
+-----+

>>> spark.createDataFrame([("d1",), (None,)], "driver_id STRING").createOrReplaceTempView("blocked")
>>> spark.sql("SELECT name FROM drivers WHERE driver_id NOT IN (SELECT driver_id FROM blocked)").count()
0
>>> spark.sql("""
...     SELECT name FROM drivers d
...     WHERE NOT EXISTS (SELECT 1 FROM blocked b WHERE b.driver_id = d.driver_id)
...     ORDER BY name""").show()
+-----+
| name|
+-----+
|  Ben|
|Chloe|
|  Dan|
|  Eva|
+-----+
```

**Notes:**

* **A subquery** is a query inside another query:
  * a **scalar** subquery returns one value, here each city's average fare. It's
    **correlated**: `t2.city = t.city` makes it run "for each outer row's city". This answers
    "trips above **their city's** average" in one statement (with DataFrames, you'd use a window
    average, as in [Set 06, Q3](ANSWERS_06.md#q3-basic--group-totals-on-every-row));
  * **`IN (subquery)`** and **`EXISTS (subquery)`** filter by another table, like a semi join.
    Spark actually **rewrites** them into semi joins.
* **The `NOT IN` trap:** the blocked list contains `d1` **and a `NULL`**. You'd expect 4 drivers
  (all but Ana), but you get **0**! `x NOT IN (d1, NULL)` means `x <> 'd1' AND x <> NULL`, and
  `x <> NULL` is `NULL` (unknown) for **every** `x` ([Set 03, Q8](ANSWERS_03.md#q8-intermediate--null-in-conditions)),
  so no row passes.
* **The fix: use `NOT EXISTS`** (or a `LEFT ANTI JOIN`), which compares row by row and simply
  finds no match for the `NULL`. Or filter the `NULL`s out of the subquery. This trap is
  famous in every SQL database, not just Spark.

---

## Q6 (Intermediate) — Step by step with WITH (CTEs)

```python
>>> spark.sql("""
...     WITH revenue AS (
...         SELECT city, driver_id, ROUND(SUM(fare), 2) AS revenue
...         FROM trips
...         GROUP BY city, driver_id
...     ),
...     ranked AS (
...         SELECT *,
...                ROW_NUMBER() OVER (PARTITION BY city ORDER BY revenue DESC, driver_id) AS rank,
...                ROUND(revenue / SUM(revenue) OVER (PARTITION BY city) * 100) AS share
...         FROM revenue
...     )
...     SELECT * FROM ranked WHERE rank <= 2 ORDER BY city, rank""").show()
+--------+---------+-------+----+-----+
|    city|driver_id|revenue|rank|share|
+--------+---------+-------+----+-----+
| Oakland|       d4|  75.25|   1| 45.0|
| Oakland|       d3|   63.0|   2| 37.0|
|San Jose|       d4|  110.0|   1| 36.0|
|San Jose|       d2|   89.0|   2| 29.0|
+--------+---------+-------+----+-----+
```

**Notes:**

* **A CTE** (common table expression), `WITH name AS (query)`, names an intermediate result,
  so a long query reads **top to bottom**, one step at a time: first the revenue per driver,
  then the ranking, then the filter. It's the SQL version of assigning DataFrames to Python
  variables.
* This is [Set 06, Q4](ANSWERS_06.md#q4-intermediate--the-top-2-drivers-in-each-city) in SQL,
  with the same answer, and it's
  [RDD Set 10, Q9](../rdd_practice_questions/ANSWERS_10.md#q9-intermediate--the-top-2-drivers-in-each-city)
  once more.
* **Window functions in SQL:** `function() OVER (PARTITION BY ... ORDER BY ...)` is exactly
  `F.function().over(W.partitionBy(...).orderBy(...))`.
* A CTE is computed as part of the query. It isn't stored or cached, so if you need the
  same intermediate result in several queries, make it a view, or `cache()` the DataFrame.

---

## Q7 (Intermediate+) — Window functions in SQL, and QUALIFY

```python
>>> spark.sql("""
...     SELECT city, trip_id, fare FROM trips
...     QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY fare DESC) <= 2
...     ORDER BY city, fare DESC""").show()
+--------+-------+-----+
|    city|trip_id| fare|
+--------+-------+-----+
| Oakland|    t05|38.25|
| Oakland|    t17| 25.0|
|San Jose|    t13|110.0|
|San Jose|    t20| 36.0|
+--------+-------+-----+

>>> spark.sql("""
...     SELECT DATE(TO_TIMESTAMP(paid_at)) AS day,
...            COUNT(*) AS trips,
...            ROUND(SUM(CAST(REGEXP_REPLACE(amount, '[$]', '') AS DOUBLE)), 2) AS paid,
...            ROUND(SUM(SUM(CAST(REGEXP_REPLACE(amount, '[$]', '') AS DOUBLE)))
...                  OVER (ORDER BY DATE(TO_TIMESTAMP(paid_at))
...                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS running
...     FROM payments
...     GROUP BY DATE(TO_TIMESTAMP(paid_at))
...     ORDER BY day""").show()
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

>>> spark.sql("""
...     SELECT trip_id, rider, fare,
...            LAG(fare) OVER (PARTITION BY rider ORDER BY trip_id) AS prev_fare
...     FROM trips WHERE rider = 'r2' ORDER BY trip_id""").show()
+-------+-----+----+---------+
|trip_id|rider|fare|prev_fare|
+-------+-----+----+---------+
|    t02|   r2|30.0|     NULL|
|    t06|   r2| 8.0|     30.0|
|    t10|   r2|28.0|      8.0|
|    t16|   r2|15.0|     28.0|
+-------+-----+----+---------+
```

**Notes:**

* **`QUALIFY`** filters on a **window function**, the way `HAVING` filters on an aggregate.
  The top 2 per city becomes **one** statement, without a CTE or a subquery. (Not all
  databases have `QUALIFY`, but Spark does.)
* **A window over an aggregate:** `SUM(SUM(amount)) OVER (...)` first sums the amounts **per
  day** (the inner `SUM`, from the `GROUP BY`), and then takes a running total of those daily
  sums (the outer `SUM ... OVER`). Window functions run **after** `GROUP BY`, so they can use
  its results.
* The frame is written in words: `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, which is
  `rowsBetween(W.unboundedPreceding, W.currentRow)`. The same default-frame trap applies
  ([Set 06, Q7](ANSWERS_06.md#q7-intermediate--running-totals-moving-averages-and-window-frames)),
  so give the frame explicitly.
* This counts the **paid amounts** (tips included), so the total is $517.44, more than the
  $476.25 of fares.
* Repeating `CAST(REGEXP_REPLACE(...))` three times is ugly and error-prone. A CTE that cleans
  the amount **once** (as in Q9) is the better way to write it.

---

## Q8 (Intermediate+) — Parameters, DataFrames in SQL, and view lifetimes

```python
>>> spark.sql("SELECT trip_id, fare FROM trips WHERE city = :city AND fare > :min_fare ORDER BY trip_id",
...           args={"city": "Oakland", "min_fare": 20}).show()
+-------+-----+
|trip_id| fare|
+-------+-----+
|    t05|38.25|
|    t08| 24.5|
|    t11| 21.0|
|    t17| 25.0|
|    t18|20.25|
+-------+-----+

>>> evil = "Oakland' OR '1'='1"
>>> spark.sql(f"SELECT COUNT(*) FROM trips WHERE city = '{evil}'").first()[0]
20
>>> spark.sql("SELECT COUNT(*) FROM trips WHERE city = :city", args={"city": evil}).first()[0]
0

>>> spark.sql("SELECT COUNT(*) AS n FROM {t} WHERE city = :city", args={"city": "San Jose"}, t=trips).show()
+---+
|  n|
+---+
| 12|
+---+

>>> trips.createOrReplaceGlobalTempView("all_trips")
>>> other = spark.newSession()
>>> [t.name for t in other.catalog.listTables()]
[]
>>> other.sql("SELECT COUNT(*) FROM global_temp.all_trips").first()[0]
20
>>> spark.catalog.dropTempView("blocked")
True
```

**Notes:**

* **Named parameters** (`:city`, `:min_fare`) with `args={...}` pass **values** into a query.
  Spark treats them strictly as values, never as SQL code.
* **Never build SQL by pasting user input into a string.** With the f-string, the "city"
  `Oakland' OR '1'='1` closes the quote and adds `OR '1'='1'`, which is always true, so the
  query returns **all 20 trips**. That's **SQL injection**, one of the most common security bugs.
  With a parameter, the same text is just a strange city name that matches **0** trips.
* **`{t}` with `t=trips`** puts a **DataFrame** into the query without creating a view first.
* **View lifetimes:**
  * a **temporary view** (`createOrReplaceTempView`) belongs to **one** `SparkSession`, and
    disappears when that session ends. The new session `other` sees **none** of our views;
  * a **global temporary view** (`createOrReplaceGlobalTempView`) is shared by all the sessions
    of the application, and is read with the `global_temp.` prefix;
  * `spark.catalog.dropTempView(name)` removes a view (it returns `True` if the view existed).
* Neither kind of view stores data. For tables that outlive the application, you'd write
  files (Set 09) or use a real catalog table (`saveAsTable`).

---

## Q9 (Intermediate+) — A whole cleaning-and-report pipeline in SQL

```python
>>> spark.read.csv("data/payments_raw.csv", header=True).createOrReplaceTempView("payments_raw")
>>> report = spark.sql("""
...     WITH clean AS (
...         SELECT payment_id, trip_id,
...                COALESCE(try_to_timestamp(paid_at, 'yyyy-MM-dd HH:mm:ss'),
...                         try_to_timestamp(paid_at, 'MM/dd/yyyy HH:mm')) AS paid_at,
...                LOWER(TRIM(method)) AS method,
...                try_cast(REGEXP_REPLACE(amount, '[^0-9.]', '') AS DOUBLE) AS amount
...         FROM payments_raw
...     ),
...     paid AS (
...         SELECT c.*, t.fare, ROUND(c.amount - t.fare, 2) AS tip
...         FROM clean c JOIN trips t ON c.trip_id = t.trip_id
...         WHERE c.amount IS NOT NULL
...     )
...     SELECT method,
...            COUNT(*) AS trips,
...            ROUND(SUM(tip), 2) AS tips,
...            ROUND(AVG(tip / fare * 100), 1) AS avg_tip_pct
...     FROM paid
...     GROUP BY method
...     ORDER BY method""")
>>> report.show()
+------+-----+-----+-----------+
|method|trips| tips|avg_tip_pct|
+------+-----+-----+-----------+
|   app|    8|24.51|       12.5|
|  card|    8|16.68|       15.0|
|  cash|    2|  0.0|        0.0|
+------+-----+-----+-----------+

>>> spark.sql("""
...     SELECT payment_id, amount FROM payments_raw
...     WHERE try_cast(REGEXP_REPLACE(amount, '[^0-9.]', '') AS DOUBLE) IS NULL
...     ORDER BY payment_id""").show()
+----------+------+
|payment_id|amount|
+----------+------+
|       p13|   N/A|
|       p18|  NULL|
+----------+------+
```

**Notes:**

* **One query, three steps:** clean the raw payments (the same fixes as
  [Set 02, Q9](ANSWERS_02.md#q9-intermediate--cleaning-a-messy-payments-file), written as SQL
  functions), join them with the trips, and summarize the tips per method (the report of
  [Set 05, Q5](ANSWERS_05.md#q5-intermediate--joining-four-tables-and-broadcast-joins)).
* **Compare with Set 05, Q5:** `app` and `card` match exactly (24.51 and 16.68). `cash` has only
  **2** trips here instead of 4, because the two payments with no valid amount (`p13` and `p18`)
  were both cash, and the `WHERE c.amount IS NOT NULL` filter drops them. The second query lists
  them, so nothing disappears silently.
* **SQL or DataFrames?** Both produce the same plan, so choose for **readability**:
  * SQL is often clearer for **set-based** logic (joins, grouping, CTEs, windows), and it's
    familiar to analysts;
  * the DataFrame API is better when the query is **built by code** (loops over columns, rules
    in a dict, as in [Set 03, Q9](ANSWERS_03.md#q9-intermediate--a-rule-based-data-validation)),
    and Python itself catches some mistakes, such as a misspelled function (`F.summ`),
    while SQL only finds them when it parses the query string.
  * In practice, you mix them: `spark.sql()` returns DataFrames, and DataFrames can be views.
