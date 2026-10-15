# PySpark DataFrame Practice Questions — Set 03: Answers

Solutions to the nine [Set 03 practice questions](QUESTIONS_03.md). Every session below
was run in the `bin/pyspark` shell (Spark 4.2.0), started from this folder. The output is
copied from those real runs.

Try the questions yourself first. There are often several correct solutions, and yours
doesn't need to match these exactly.

* [Q1 (Basic): Filtering rows](#q1-basic--filtering-rows)
* [Q2 (Basic): Sorting rows](#q2-basic--sorting-rows)
* [Q3 (Basic): Distinct values and duplicates](#q3-basic--distinct-values-and-duplicates)
* [Q4 (Intermediate): Missing values](#q4-intermediate--missing-values)
* [Q5 (Intermediate): Filtering text and lists](#q5-intermediate--filtering-text-and-lists)
* [Q6 (Intermediate): Duplicate payments](#q6-intermediate--duplicate-payments)
* [Q7 (Intermediate+): Top N and paging](#q7-intermediate--top-n-and-paging)
* [Q8 (Intermediate+): NULL in conditions](#q8-intermediate--null-in-conditions)
* [Q9 (Intermediate+): A rule-based data validation](#q9-intermediate--a-rule-based-data-validation)

Setup, used by all the questions:

```python
>>> from pyspark.sql import functions as F
>>> trips_ddl = ("trip_id STRING, driver_id STRING, rider STRING, city STRING, "
...              "hour INT, distance_km DOUBLE, fare DOUBLE, rating INT")
>>> trips = spark.read.csv("data/trips.csv", header=True, schema=trips_ddl)
>>> cols = ["trip_id", "city", "hour", "distance_km", "fare", "rating"]
```

---

## Q1 (Basic) — Filtering rows

```python
>>> trips.filter(F.col("fare") > 30).select(cols).show()
+-------+--------+----+-----------+-----+------+
|trip_id|    city|hour|distance_km| fare|rating|
+-------+--------+----+-----------+-----+------+
|    t05| Oakland|  13|       15.3|38.25|     4|
|    t13|San Jose|   8|       20.5|110.0|     1|
|    t20|San Jose|  22|       14.8| 36.0|     3|
+-------+--------+----+-----------+-----+------+

>>> trips.where("fare > 30").count()
3

>>> trips.filter((F.col("city") == "Oakland") & (F.col("distance_km") >= 8)).select(cols).show()
+-------+-------+----+-----------+-----+------+
|trip_id|   city|hour|distance_km| fare|rating|
+-------+-------+----+-----------+-----+------+
|    t05|Oakland|  13|       15.3|38.25|     4|
|    t08|Oakland|  18|        9.8| 24.5|     5|
|    t11|Oakland|  20|        8.0| 21.0|     2|
|    t17|Oakland|  18|       10.0| 25.0|     5|
+-------+-------+----+-----------+-----+------+

>>> trips.filter((F.col("hour") < 9) | (F.col("hour") >= 20)).count()
6
>>> trips.filter(~(F.col("city") == "San Jose")).count()
8

>>> (trips.filter(F.col("hour").between(17, 19) & F.col("driver_id").isin("d1", "d3"))
...       .select("trip_id", "driver_id", "hour")
...       .show())
+-------+---------+----+
|trip_id|driver_id|hour|
+-------+---------+----+
|    t07|       d1|  18|
|    t08|       d3|  18|
|    t10|       d1|  19|
|    t15|       d3|  17|
|    t18|       d1|  19|
+-------+---------+----+

>>> try:
...     trips.filter(F.col("city") == "Oakland" & F.col("fare") > 20)
... except Exception as e:
...     print(type(e).__name__, e)
...
PySparkValueError [CANNOT_CONVERT_COLUMN_INTO_BOOL] Cannot convert column into bool: please use '&' for 'and', '|' for 'or', '~' for 'not' when building DataFrame boolean expressions.
```

**Notes:**

* **`filter()` and `where()` are the same method**. Both keep the rows where the
  condition is true, and both accept a **column expression** or a **SQL string**.
* **Combine conditions with `&` (and), `|` (or), and `~` (not)**, not with Python's
  `and`, `or`, `not`. Python's keywords try to turn a `Column` into `True`/`False`, which is
  impossible, because the column has a different value on every row. That's the
  `CANNOT_CONVERT_COLUMN_INTO_BOOL` error.
* **Always put each comparison in parentheses.** In Python, `&` binds **more tightly**
  than `==` and `>`, so `F.col("city") == "Oakland" & F.col("fare") > 20` is read as
  `F.col("city") == ("Oakland" & F.col("fare")) > 20`, which fails with that same error.
* **`between(a, b)`** includes both ends (17 ≤ hour ≤ 19), and **`isin(...)`** tests for
  membership in a list.
* This is the DataFrame version of the RDD `filter(lambda t: ...)`. But here Spark *sees*
  the condition, so it can push it down to the data source (Set 09).

---

## Q2 (Basic) — Sorting rows

```python
>>> trips.orderBy("fare").select(cols).show(3)
+-------+--------+----+-----------+----+------+
|trip_id|    city|hour|distance_km|fare|rating|
+-------+--------+----+-----------+----+------+
|    t19|San Jose|  20|        2.2| 7.5|     5|
|    t06|San Jose|  17|        2.4| 8.0|     3|
|    t03| Oakland|   9|        3.1|9.75|  NULL|
+-------+--------+----+-----------+----+------+
only showing top 3 rows

>>> trips.orderBy(F.col("fare").desc()).select(cols).show(3)
+-------+--------+----+-----------+-----+------+
|trip_id|    city|hour|distance_km| fare|rating|
+-------+--------+----+-----------+-----+------+
|    t13|San Jose|   8|       20.5|110.0|     1|
|    t05| Oakland|  13|       15.3|38.25|     4|
|    t20|San Jose|  22|       14.8| 36.0|     3|
+-------+--------+----+-----------+-----+------+
only showing top 3 rows

>>> trips.orderBy("city", F.desc("fare")).select(cols).show(5)
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

>>> trips.orderBy("rating").select("trip_id", "rating").show(4)
+-------+------+
|trip_id|rating|
+-------+------+
|    t03|  NULL|
|    t09|  NULL|
|    t16|  NULL|
|    t13|     1|
+-------+------+
only showing top 4 rows

>>> trips.orderBy(F.col("rating").asc_nulls_last(), "trip_id").select("trip_id", "rating").show(4)
+-------+------+
|trip_id|rating|
+-------+------+
|    t13|     1|
|    t11|     2|
|    t06|     3|
|    t20|     3|
+-------+------+
only showing top 4 rows

>>> trips.orderBy(F.col("rating").desc_nulls_first(), "trip_id").select("trip_id", "rating").show(4)
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

**Notes:**

* `orderBy()` (or its twin, `sort()`) sorts **ascending** by default. For descending, use
  `F.col("x").desc()` or `F.desc("x")`.
* **Several sort keys** work like a phone book: first by `city` (A–Z), and **within**
  each city by `fare` (largest first).
* **Where do `NULL`s go?** Ascending puts `NULL`s **first** and descending puts them
  **last**, by default. To choose, use `asc_nulls_last()`, `asc_nulls_first()`,
  `desc_nulls_first()`, or `desc_nulls_last()`. A report of "worst ratings first" should
  start at `1`, not at the unrated trips.
* **Add a tie-breaker** (here `trip_id`) whenever the main key has ties. `t06` and `t20`
  both have a rating of 3. Without a tie-breaker, their order isn't guaranteed, which
  matters for "top N" questions (Q7).
* Sorting a big DataFrame is expensive: it's a full **shuffle** with **range
  partitioning**, just like `sortByKey()` in
  [RDD Set 08, Q8](../rdd_practice_questions/ANSWERS_08.md#q8-intermediate--custom-partitioning-and-range-partitioning).

---

## Q3 (Basic) — Distinct values and duplicates

```python
>>> trips.select("city").distinct().count()
2
>>> trips.select("driver_id", "city").distinct().orderBy("driver_id", "city").show()
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

>>> trips.select("rider").distinct().count()
7
>>> trips.dropDuplicates(["rider"]).count()
7
>>> trips.distinct().count()
20
```

**Notes:**

* **`distinct()`** removes rows that are **identical in every column**. To get the
  distinct values of some columns, `select()` them first. So `select("driver_id", "city")`
  followed by `distinct()` lists the 7 distinct driver–city pairs. This is
  [RDD Set 10, Q2](../rdd_practice_questions/ANSWERS_10.md#q2-basic--who-drives-where),
  in one line.
* **`dropDuplicates(["rider"])`** keeps **one whole row** per rider. All the columns stay,
  but **which** of a rider's trips is kept is **not defined**. Use it when any row will do;
  otherwise, choose the row deliberately (Q6, and window functions in [Set 06](QUESTIONS_06.md)).
* `trips.distinct().count()` is 20, so there are no fully duplicated trips.
* Like the RDD `distinct()`, these need a **shuffle** to bring equal rows together.

---

## Q4 (Intermediate) — Missing values

```python
>>> trips.filter(F.col("rating").isNull()).count()
3
>>> trips.filter(F.col("rating").isNotNull()).count()
17

>>> trips.na.drop().count()
17
>>> trips.na.drop(subset=["fare"]).count()
20

>>> riders = spark.read.json("data/riders.json")
>>> riders.na.drop().count()
6
>>> riders.na.drop(how="all").count()
7

>>> (trips.na.fill({"rating": 0})
...       .filter("trip_id IN ('t03', 't09', 't16')")
...       .select("trip_id", "rating")
...       .show())
+-------+------+
|trip_id|rating|
+-------+------+
|    t03|     0|
|    t09|     0|
|    t16|     0|
+-------+------+

>>> trips.na.replace({"San Jose": "SJ", "Oakland": "OAK"}, subset=["city"]).select("trip_id", "city").show(3)
+-------+----+
|trip_id|city|
+-------+----+
|    t01|  SJ|
|    t02|  SJ|
|    t03| OAK|
+-------+----+
only showing top 3 rows

>>> messy = spark.createDataFrame(
...     [("a", None, None), ("b", 1, None), (None, None, None), ("d", 4, 5.0)],
...     "k STRING, x INT, y DOUBLE")
>>> (messy.na.drop(how="any").count(), messy.na.drop(how="all").count(),
...  messy.na.drop(thresh=2).count(), messy.na.drop(subset=["k"]).count())
(1, 3, 2, 3)
```

**Notes:**

* **Testing for `NULL`** needs `isNull()` / `isNotNull()` (or `IS NULL` in SQL). You can't
  use `== None` (Q8 explains why).
* **`na.drop()`** (also written `dropna()`) removes rows with missing values:
  * `how="any"` (the default): drop a row if **any** column is `NULL`;
  * `how="all"`: drop it only if **all** the columns are `NULL`;
  * `thresh=n`: keep a row if it has **at least `n`** non-`NULL` values;
  * `subset=[...]`: look only at those columns.
* **Careful with the default:** `na.drop()` on `riders` drops `r6` just because he has no
  `tags`, which is probably not what you want. Decide **which** columns must be present,
  and use `subset`.
* **`na.fill()`** (or `fillna()`) replaces `NULL`s. A **dict** sets a value per column.
  It only fills columns of a matching type: `0` fills numeric columns, and `""` fills
  string columns.
* **Should you fill at all?** Filling the unrated trips with `0` makes them look like
  terrible trips, and pulls the average rating down (Q8). Fill when the value really means
  "zero" (for example, a missing tip); otherwise, keep the `NULL`, and let the functions
  skip it.
* **`na.replace()`** replaces specific **values**, not `NULL`s, in the chosen columns.

---

## Q5 (Intermediate) — Filtering text and lists

```python
>>> riders.filter(F.col("name").startswith("O") | F.col("name").endswith("a")).select("rider", "name").show()
+-----+----+
|rider|name|
+-----+----+
|   r1|Maya|
|   r2|Omar|
|   r3|Lena|
+-----+----+

>>> riders.filter(F.col("name").like("i%")).count()
0
>>> riders.filter(F.col("name").ilike("i%")).select("name").show()
+----+
|name|
+----+
|Ines|
+----+

>>> riders.filter(F.col("home.zip").rlike("^95(11|12)")).select("name", "home.zip").show()
+----+-----+
|name|  zip|
+----+-----+
|Maya|95112|
|Omar|95126|
| Tom|95110|
|Yuki|95113|
+----+-----+

>>> riders.filter(F.col("home.city").contains("Jose")).count()
4
>>> riders.filter(F.array_contains("tags", "frequent")).select("name", "tags").show()
+----+--------------------+
|name|                tags|
+----+--------------------+
|Maya| [student, frequent]|
|Omar|          [frequent]|
|Yuki|[business, frequent]|
+----+--------------------+
```

**Notes:**

* **Simple text tests:** `startswith()`, `endswith()`, and `contains()`.
* **`like(pattern)`** uses SQL wildcards: `%` = any number of characters, `_` = exactly one.
  It's **case-sensitive**, so `"i%"` doesn't match `Ines`. **`ilike()`** is the
  case-**insensitive** version.
* **`rlike(regex)`** uses a full **regular expression**: `^95(11|12)` means "starts with
  9511 or 9512". `95126` matches because it starts with `9512`.
* **Lists:** `array_contains(col, value)` keeps the rows whose array has the value. `r6`,
  with no `tags` at all, gives `NULL` here, so `filter()` drops it, which is correct.

---

## Q6 (Intermediate) — Duplicate payments

```python
>>> dup = spark.createDataFrame([
...     ("p01", "t01", "2026-10-05 08:00:00", "card", 17.40),
...     ("p01", "t01", "2026-10-05 08:00:00", "card", 17.40),
...     ("p02", "t02", "2026-10-06 09:13:00", "app", 33.00),
...     ("p02b", "t02", "2026-10-06 09:20:00", "app", 33.00),
...     ("p03", "t03", "2026-10-07 09:26:00", "cash", 9.75),
...     ("p03", "t03", "2026-10-07 09:26:00", "cash", 9.75),
...     ("p03", "t03", "2026-10-07 09:26:00", "cash", 9.75)],
...     "payment_id STRING, trip_id STRING, paid_at STRING, method STRING, amount DOUBLE")
>>> dup.count(), dup.distinct().count(), dup.dropDuplicates(["trip_id"]).count()
(7, 4, 3)
>>> dup.count() - dup.distinct().count()
3

>>> dup.distinct().orderBy("payment_id").show()
+----------+-------+-------------------+------+------+
|payment_id|trip_id|            paid_at|method|amount|
+----------+-------+-------------------+------+------+
|       p01|    t01|2026-10-05 08:00:00|  card|  17.4|
|       p02|    t02|2026-10-06 09:13:00|   app|  33.0|
|      p02b|    t02|2026-10-06 09:20:00|   app|  33.0|
|       p03|    t03|2026-10-07 09:26:00|  cash|  9.75|
+----------+-------+-------------------+------+------+

>>> dup.distinct().groupBy("trip_id").count().filter("count > 1").show()
+-------+-----+
|trip_id|count|
+-------+-----+
|    t02|    2|
+-------+-----+
```

**Notes:**

* There are **two kinds** of duplicates here, and they need different fixes:
  * **exact copies** of the same record (`p01` twice, `p03` three times), for example when a
    file was loaded twice. `distinct()` removes them safely: 7 rows → 4, so 3 copies were
    removed;
  * **two different records for the same thing**: trip `t02` was paid **twice**, 7 minutes
    apart (`p02` and `p02b`). That's not a copy. It's a real problem, maybe a double charge,
    and it needs a **decision**.
* **`dropDuplicates(["trip_id"])`** gives one row per trip, but it keeps an **arbitrary**
  one of `p02` and `p02b`. That hides the double charge instead of reporting it.
* **Find before you fix:** after removing the exact copies, `groupBy("trip_id").count()`
  with `count > 1` lists the trips that still have more than one payment.
  `groupBy()` is the subject of [Set 04](QUESTIONS_04.md).
* To keep a **chosen** row per key, such as the first payment of each trip, use a window
  function with `row_number()` ([Set 06, Q5](ANSWERS_06.md#q5-intermediate--keeping-the-first-payment-per-trip)).

---

## Q7 (Intermediate+) — Top N and paging

```python
>>> by_fare = trips.orderBy(F.desc("fare"), "trip_id").select("trip_id", "fare")
>>> by_fare.limit(5).show()
+-------+-----+
|trip_id| fare|
+-------+-----+
|    t13|110.0|
|    t05|38.25|
|    t20| 36.0|
|    t02| 30.0|
|    t10| 28.0|
+-------+-----+

>>> by_fare.offset(5).limit(5).show()
+-------+-----+
|trip_id| fare|
+-------+-----+
|    t17| 25.0|
|    t08| 24.5|
|    t11| 21.0|
|    t18|20.25|
|    t04| 19.0|
+-------+-----+

>>> trips.orderBy(F.desc("rating"), "trip_id").select("trip_id", "rating").limit(4).show()
+-------+------+
|trip_id|rating|
+-------+------+
|    t01|     5|
|    t04|     5|
|    t07|     5|
|    t08|     5|
+-------+------+

>>> [row.trip_id for row in by_fare.take(3)]
['t13', 't05', 't20']
>>> trips.orderBy("fare").first().trip_id
't19'
```

**Notes:**

* **Top N = sort + `limit(n)`.** Spark is smart about this: it doesn't fully sort all the
  data. Each partition keeps only its own top N, which is the same trick as
  [RDD Set 03, Q8](../rdd_practice_questions/ANSWERS_03.md#q8-intermediate--global-top-n).
* **Paging:** `offset(5).limit(5)` skips the first 5 rows and returns the next 5: page 2
  of the results. (`offset()` is new in Spark 3.4.)
* **Ties make "top N" ambiguous.** 8 trips have a rating of 5, so "the top 4 by rating"
  could be any 4 of them, unless a **tie-breaker** decides. With `trip_id` as the second
  key, the answer is the same on every run, and paging never shows a row twice or skips one.
* `limit()` returns a **DataFrame** (for more transformations), and `take(n)` and `first()`
  return **rows** to Python.

---

## Q8 (Intermediate+) — NULL in conditions

```python
>>> trips.filter(F.col("rating") == 5).count(), trips.filter(F.col("rating") != 5).count(), trips.count()
(8, 9, 20)
>>> trips.filter((F.col("rating") != 5) | F.col("rating").isNull()).count()
12
>>> trips.filter(~F.col("rating").eqNullSafe(5)).count()
12
>>> trips.filter("rating IS DISTINCT FROM 5").count()
12

>>> spark.createDataFrame([(None,), (5,)], "r INT").select(
...     "r",
...     (F.col("r") == 5).alias("eq"),
...     (F.col("r") != 5).alias("ne"),
...     F.col("r").eqNullSafe(5).alias("safe")).show()
+----+----+-----+-----+
|   r|  eq|   ne| safe|
+----+----+-----+-----+
|NULL|NULL| NULL|false|
|   5|true|false| true|
+----+----+-----+-----+

>>> trips.select(F.count("*").alias("rows"),
...              F.count("rating").alias("ratings"),
...              F.avg("rating").alias("avg"),
...              F.avg(F.coalesce("rating", F.lit(0))).alias("avg_with_0")).show()
+----+-------+------------------+----------+
|rows|ratings|               avg|avg_with_0|
+----+-------+------------------+----------+
|  20|     17|4.0588235294117645|      3.45|
+----+-------+------------------+----------+
```

**Notes:**

* **The puzzle:** 8 trips have a rating of 5, and 9 have a rating that isn't 5, but
  there are 20 trips. **3 trips are in neither group**: the unrated ones.
* **SQL uses three-valued logic**: a comparison with `NULL` is neither true nor false. It's
  **`NULL`** ("unknown"). `NULL == 5` is `NULL`, and so is `NULL != 5`. `filter()` keeps only
  the rows where the condition is **true**, so it drops the unknown ones **both times**.
* **The fixes**, when you want "not 5, including the unrated":
  * add `| F.col("rating").isNull()` explicitly;
  * use **`eqNullSafe()`**, the `NULL`-safe equality (written `<=>` in SQL). It treats
    `NULL` as a normal value, so it is never `NULL` itself: `NULL <=> 5` is `false`;
  * in SQL, `IS DISTINCT FROM` is the `NULL`-safe "not equal".
* This is also why `F.col("x") == None` never works: it's `NULL` on every row.
* **Aggregates skip `NULL`s:** `count("*")` counts rows (20), and `count("rating")` counts
  the non-`NULL` ratings (17). `avg("rating")` is the average of the **17 real ratings**.
  Filling the `NULL`s with `0` first (Q4) changes the answer from 4.06 to 3.45. Both are
  "correct", but they answer **different** questions.

---

## Q9 (Intermediate+) — A rule-based data validation

```python
>>> from functools import reduce
>>> dirty = spark.read.csv("data/trips_dirty.csv", header=True,
...                        schema=trips_ddl + ", _corrupt_record STRING",
...                        columnNameOfCorruptRecord="_corrupt_record").cache()
>>> dirty.count()
27

>>> rules = {
...     "parsed": F.col("_corrupt_record").isNull(),
...     "known_city": F.col("city").isin("San Jose", "Oakland"),
...     "positive_distance": F.col("distance_km") > 0,
...     "fare_present": F.col("fare").isNotNull(),
...     "rating_1_to_5": F.col("rating").isNull() | F.col("rating").between(1, 5),
... }
>>> for name, rule in rules.items():
...     failed = dirty.filter(~F.coalesce(rule, F.lit(False)))
...     print(name, failed.count(), sorted(r.trip_id for r in failed.collect()))
...
parsed 2 ['t21', 't22']
known_city 1 ['t23']
positive_distance 1 ['t24']
fare_present 2 ['t22', 't25']
rating_1_to_5 1 ['t26']

>>> all_rules = reduce(lambda a, b: a & b, [F.coalesce(r, F.lit(False)) for r in rules.values()])
>>> valid = dirty.filter(all_rules).drop("_corrupt_record")
>>> valid.count()
21
>>> clean = valid.distinct()
>>> clean.count()
20
>>> sorted(clean.collect()) == sorted(trips.collect())
True
```

**Notes:**

* **Rules as data:** each rule is a named **column expression** in a dict. Because rules
  are just Python values, the same loop can **report** each rule, and the **combined**
  filter can apply them all. Adding a rule is one line.
* **`reduce(lambda a, b: a & b, conditions)`** joins a list of conditions with `&`,
  however many there are: `c1 & c2 & c3 & ...`.
* **Why `coalesce(rule, False)`?** A rule can be `NULL` instead of `false`. For example,
  for a row with a **missing** distance, `distance_km > 0` is `NULL`. Then `~rule` is
  `NULL` too, so the row would be **missing from the failure report**, and the combined
  filter would drop it without saying why (Q8). `coalesce(..., False)` turns "unknown"
  into "failed", so a row passes only if every rule is **definitely true**.
* **The report** (which rules failed, how often, for which trips) is often as valuable as
  the clean data: it tells the data owners what to fix. `t22` failed two rules (it's
  missing two fields).
* **Order of steps:** apply the rules first (6 bad rows removed → 21), then remove the
  **exact** duplicates (the extra copy of `t05` → 20). The result is exactly the clean
  `trips.csv`.
* Each rule check is a separate job, so the raw data is **cached** first, and read
  only once.
