# PySpark DataFrame Practice Questions — Set 08: Answers

Solutions to the nine [Set 08 practice questions](QUESTIONS_08.md). Every session below
was run in the `bin/pyspark` shell (Spark 4.2.0, with pandas 2.2 and pyarrow 21), started
from this folder. The output is copied from those real runs.

Try the questions yourself first. There are often several correct solutions, and yours
doesn't need to match these exactly.

* [Q1 (Basic): A first Python UDF](#q1-basic--a-first-python-udf)
* [Q2 (Basic): The return type matters](#q2-basic--the-return-type-matters)
* [Q3 (Basic): UDFs and NULL](#q3-basic--udfs-and-null)
* [Q4 (Intermediate): Using a UDF in SQL](#q4-intermediate--using-a-udf-in-sql)
* [Q5 (Intermediate): A pandas UDF, and a rounding surprise](#q5-intermediate--a-pandas-udf-and-a-rounding-surprise)
* [Q6 (Intermediate): Why built-in functions are better](#q6-intermediate--why-built-in-functions-are-better)
* [Q7 (Intermediate+): A UDF that returns a struct](#q7-intermediate--a-udf-that-returns-a-struct)
* [Q8 (Intermediate+): pandas on whole groups](#q8-intermediate--pandas-on-whole-groups)
* [Q9 (Intermediate+): When a UDF is the right tool: fuzzy matching](#q9-intermediate--when-a-udf-is-the-right-tool-fuzzy-matching)

Setup, used by all the questions:

```python
>>> import io, contextlib
>>> import pandas as pd
>>> from pyspark.sql import functions as F
>>> from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
>>> trips_ddl = ("trip_id STRING, driver_id STRING, rider STRING, city STRING, "
...              "hour INT, distance_km DOUBLE, fare DOUBLE, rating INT")
>>> trips = spark.read.csv("data/trips.csv", header=True, schema=trips_ddl)
>>> def plan_of(df):
...     buffer = io.StringIO()
...     with contextlib.redirect_stdout(buffer):
...         df.explain()
...     return buffer.getvalue()
...
```

`plan_of()` is the helper from [Set 05, Q5](ANSWERS_05.md#q5-intermediate--joining-four-tables-and-broadcast-joins)
that captures what `explain()` prints.

---

## Q1 (Basic) — A first Python UDF

```python
>>> @F.udf(returnType=StringType())
... def stars(rating):
...     return "*" * rating if rating is not None else "not rated"
...
>>> trips.select("trip_id", "rating", stars("rating").alias("stars")).orderBy("trip_id").show(4)
+-------+------+---------+
|trip_id|rating|    stars|
+-------+------+---------+
|    t01|     5|    *****|
|    t02|     4|     ****|
|    t03|  NULL|not rated|
|    t04|     5|    *****|
+-------+------+---------+
only showing top 4 rows

>>> def period(hour):
...     if hour < 12:
...         return "morning"
...     if hour < 17:
...         return "afternoon"
...     return "evening"
...
>>> period_udf = F.udf(period, StringType())
>>> trips.groupBy(period_udf("hour").alias("period")).count().orderBy("period").show()
+---------+-----+
|   period|count|
+---------+-----+
|afternoon|    2|
|  evening|   13|
|  morning|    5|
+---------+-----+
```

**Notes:**

* **A UDF** (user-defined function) wraps an ordinary Python function so that it can be used
  on a column, like a built-in function. There are two ways to make one:
  * the **decorator** `@F.udf(returnType=...)` on a `def`;
  * **`F.udf(function, returnType)`**, which wraps a function that already exists, such as
    `period`, which you can still test as plain Python: `period(8)` is `'morning'`.
* The function gets **one row's values** (here one `rating`, or one `hour`) and returns one value.
* A UDF can be used anywhere a column expression can: in `select()`, `withColumn()`,
  `filter()`, and even `groupBy()`.
* Both of these could be written with built-in functions (`F.repeat()` and
  `F.when()`, as in [Set 04, Q6](ANSWERS_04.md#q6-intermediate--pivot-tables)). Q6 shows why
  you should prefer them when you can.

---

## Q2 (Basic) — The return type matters

```python
>>> half_int = F.udf(lambda x: x / 2, IntegerType())
>>> half_int_no_arrow = F.udf(lambda x: x / 2, IntegerType(), useArrow=False)
>>> half_double = F.udf(lambda x: x / 2, DoubleType())
>>> half_default = F.udf(lambda x: x / 2)
>>> trips.select("trip_id", "fare",
...              half_int("fare").alias("as_int"),
...              half_int_no_arrow("fare").alias("as_int_no_arrow"),
...              half_double("fare").alias("as_double"),
...              half_default("fare").alias("default")).show(3)
+-------+----+------+---------------+---------+-------+
|trip_id|fare|as_int|as_int_no_arrow|as_double|default|
+-------+----+------+---------------+---------+-------+
|    t01|14.5|     7|           NULL|     7.25|   7.25|
|    t02|30.0|    15|           NULL|     15.0|   15.0|
|    t03|9.75|     4|           NULL|    4.875|  4.875|
+-------+----+------+---------------+---------+-------+
only showing top 3 rows

>>> trips.select(half_default("fare").alias("d")).dtypes
[('d', 'string')]
```

**Notes:**

* **Spark can't see inside a Python function**, so it can't know what type the function returns.
  **You** declare the type, and Spark trusts you.
* **A wrong type silently changes your data:** `x / 2` returns a Python `float`, but `half_int`
  promised an `IntegerType`. There's **no error** in either case, but:
  * with Spark 4.2's default (Arrow-based Python UDFs, see Q6), the float is **converted**, and
    the decimals are cut off: 7.25 becomes **7**;
  * with `useArrow=False` (the classic mode), a value of the wrong type becomes **`NULL`**.

  Either way, the result is wrong without any warning. Always declare the type that the
  function **really** returns.
* **Without a return type**, the default is **`StringType`**. The values *look* right in
  `show()`, but the column is text (`'string'`): you can't add it up or compare it as numbers.

---

## Q3 (Basic) — UDFs and NULL

```python
>>> times_20 = F.udf(lambda r: r * 20, IntegerType())
>>> try:
...     trips.select(times_20("rating")).collect()
... except Exception as e:
...     print(type(e).__name__, "TypeError" in str(e), "NoneType" in str(e))
...
PythonException True True

>>> times_20_safe = F.udf(lambda r: r * 20 if r is not None else None, IntegerType())
>>> (trips.select("trip_id", "rating", times_20_safe("rating").alias("score"))
...       .filter("trip_id IN ('t01', 't03')")
...       .show())
+-------+------+-----+
|trip_id|rating|score|
+-------+------+-----+
|    t01|     5|  100|
|    t03|  NULL| NULL|
+-------+------+-----+
```

**Notes:**

* **A `NULL` arrives in Python as `None`.** `None * 20` raises a `TypeError`, and **one** bad row
  makes the **whole job fail**, with a `PythonException` that contains the Python error and its
  traceback.
* **Every UDF must handle `None`** for every argument that can be `NULL`. The usual pattern:
  `return None` when the input is `None`, so `NULL` in, `NULL` out, just like the built-in
  functions.
* The same goes for **any** Python exception in a UDF (a bad `int()` conversion, a missing dict
  key...): catch what you expect, and return `None` instead.
* Built-in functions already handle `NULL`. That's one more reason to prefer them (Q6).

---

## Q4 (Intermediate) — Using a UDF in SQL

```python
>>> stars_sql = spark.udf.register("stars", lambda r: "*" * r if r is not None else "not rated", StringType())
>>> trips.createOrReplaceTempView("trips")
>>> spark.sql("""
...     SELECT trip_id, stars(rating) AS stars
...     FROM trips WHERE city = 'Oakland'
...     ORDER BY trip_id LIMIT 3""").show()
+-------+---------+
|trip_id|    stars|
+-------+---------+
|    t03|not rated|
|    t05|     ****|
|    t08|    *****|
+-------+---------+

>>> trips.selectExpr("trip_id", "stars(rating) AS s").show(2)
+-------+-----+
|trip_id|    s|
+-------+-----+
|    t01|*****|
|    t02| ****|
+-------+-----+
only showing top 2 rows
```

**Notes:**

* **`spark.udf.register(name, function, returnType)`** makes a function callable **by name** in
  SQL ([Set 07](QUESTIONS_07.md)), and in SQL strings inside DataFrame code (`selectExpr()`,
  `F.expr()`, `filter("...")`).
* The registered UDF lives as long as the `SparkSession`, like a temporary view.
* `register()` also **returns** the UDF (here `stars_sql`), so the same function can be used
  in DataFrame code too: `trips.select(stars_sql("rating"))`.

---

## Q5 (Intermediate) — A pandas UDF, and a rounding surprise

```python
>>> @F.pandas_udf("double")
... def per_km_pandas(fare: pd.Series, km: pd.Series) -> pd.Series:
...     return (fare / km).round(2)
...
>>> @F.udf(DoubleType())
... def per_km_python(fare, km):
...     return round(fare / km, 2)
...
>>> compare = trips.select("trip_id", "fare", "distance_km",
...                        per_km_pandas("fare", "distance_km").alias("pandas"),
...                        per_km_python("fare", "distance_km").alias("python"),
...                        F.round(F.col("fare") / F.col("distance_km"), 2).alias("builtin"))
>>> compare.orderBy("trip_id").show(4)
+-------+----+-----------+------+------+-------+
|trip_id|fare|distance_km|pandas|python|builtin|
+-------+----+-----------+------+------+-------+
|    t01|14.5|        5.2|  2.79|  2.79|   2.79|
|    t02|30.0|       12.0|   2.5|   2.5|    2.5|
|    t03|9.75|        3.1|  3.15|  3.15|   3.15|
|    t04|19.0|        7.5|  2.53|  2.53|   2.53|
+-------+----+-----------+------+------+-------+
only showing top 4 rows

>>> compare.filter("pandas != builtin OR python != builtin").show()
+-------+----+-----------+------+------+-------+
|trip_id|fare|distance_km|pandas|python|builtin|
+-------+----+-----------+------+------+-------+
|    t11|21.0|        8.0|  2.62|  2.62|   2.63|
+-------+----+-----------+------+------+-------+
```

**Notes:**

* **A pandas UDF** (a "vectorized" UDF) gets a whole **batch of rows** at once, as a
  `pandas.Series` per column, and returns a `Series` of the same length. Spark sends the batches
  in the efficient **Apache Arrow** format, and pandas computes on whole columns with fast code,
  instead of Python looping over rows one by one.
* The **type hints** (`pd.Series -> pd.Series`) tell Spark what kind of pandas UDF it is, and
  the argument of `@F.pandas_udf("double")` is the return type.
* **The rounding surprise:** `t11` costs 21.0 / 8.0 = **2.625** per km. Python's `round()` and
  pandas' `.round()` round a tie like this to the **even** digit (2.62, "banker's rounding"),
  while Spark's `round()` rounds **half up** (2.63). The "same" code gives different answers in
  Python and in Spark. If exact rounding matters (money), decide on one rule, and use the same
  function everywhere. Spark's `F.bround()` rounds to even, like Python.

---

## Q6 (Intermediate) — Why built-in functions are better

```python
>>> python_plan = plan_of(trips.select(per_km_python("fare", "distance_km")))
>>> pandas_plan = plan_of(trips.select(per_km_pandas("fare", "distance_km")))
>>> builtin_plan = plan_of(trips.select(F.round(F.col("fare") / F.col("distance_km"), 2)))
>>> "ArrowEvalPython" in python_plan, "ArrowEvalPython" in pandas_plan, "EvalPython" in builtin_plan
(True, True, False)
>>> per_km_no_arrow = F.udf(lambda f, k: round(f / k, 2), DoubleType(), useArrow=False)
>>> "BatchEvalPython" in plan_of(trips.select(per_km_no_arrow("fare", "distance_km")))
True
>>> spark.conf.get("spark.sql.execution.pythonUDF.arrow.enabled")
'true'
```

A timing on 10 million generated rows, `spark.range(10_000_000)`, computing the sum of
`round(fare / km, 2)` four ways. Here is the best of 3 runs on one laptop. Your times will
differ, but the order is what matters:

```text
built-in function                 0.18 s
pandas UDF                        0.25 s
Python UDF (Arrow, the default)   1.24 s
Python UDF (useArrow=False)       1.62 s
```

**Notes:**

* **The plan shows where Python runs.** With a UDF, the plan has an extra **`...EvalPython`**
  step: Spark sends the data **out of the JVM** to Python worker processes, runs your function
  there, and sends the results back. A built-in function has no such step. It runs inside
  Spark's own engine, as fast compiled code.
* In Spark 4.2, plain Python UDFs are sent to Python with **Arrow** too, by default
  (`spark.sql.execution.pythonUDF.arrow.enabled` is `true`), so both UDFs show
  `ArrowEvalPython`. With `useArrow=False`, you get the older row-by-row `BatchEvalPython`.
* **The cost:** the Python UDF was about **7× slower** than the built-in function on the same
  data, and the pandas UDF in between. Moving data to Python and back is the price.
* **Spark can't optimize a UDF.** It's a "black box": Spark doesn't know what it does, so it can't
  push a filter through it, skip unused work, or reorder it.
* **So, the rule:** use a **built-in function** whenever one exists (there are hundreds:
  `pyspark.sql.functions`), then a **pandas UDF**, and a plain **Python UDF** only as a last resort.
* **A troubleshooting note:** in our Spark 4.2.0 test setup, Arrow-based **Python** UDFs
  sometimes **hung** (the job never finished, using no CPU) after a DataFrame had been
  `cache()`d in the same session. pandas UDFs, and Python UDFs with `useArrow=False`, were not
  affected. That's why this set never caches. If a Python UDF ever hangs for you, try
  `useArrow=False`.

---

## Q7 (Intermediate+) — A UDF that returns a struct

```python
>>> import re
>>> money = StructType([StructField("currency", StringType()), StructField("value", DoubleType())])
>>> @F.udf(money)
... def parse_money(text):
...     if text is None:
...         return None
...     m = re.match(r"^\s*(\$|USD)\s*([0-9]+(?:\.[0-9]+)?)\s*$", text)
...     if not m:
...         return None
...     return ("USD", float(m.group(2)))
...
>>> raw = spark.read.csv("data/payments_raw.csv", header=True)
>>> parsed = raw.select("payment_id", "amount", parse_money("amount").alias("money"))
>>> parsed.printSchema()
root
 |-- payment_id: string (nullable = true)
 |-- amount: string (nullable = true)
 |-- money: struct (nullable = true)
 |    |-- currency: string (nullable = true)
 |    |-- value: double (nullable = true)

>>> (parsed.select("payment_id", "amount", "money", "money.value")
...        .filter("payment_id IN ('p01', 'p05', 'p10', 'p13', 'p18')")
...        .orderBy("payment_id")
...        .show())
+----------+---------+------------+-----+
|payment_id|   amount|       money|value|
+----------+---------+------------+-----+
|       p01|   $17.40| {USD, 17.4}| 17.4|
|       p05|USD 42.08|{USD, 42.08}|42.08|
|       p10|  $ 33.60| {USD, 33.6}| 33.6|
|       p13|      N/A|        NULL| NULL|
|       p18|     NULL|        NULL| NULL|
+----------+---------+------------+-----+
```

**Notes:**

* **A UDF can return several values at once** as a **struct**: declare a `StructType`, and
  return a Python **tuple** (or a dict, or a `Row`) with one value per field. Then
  `"money.value"` reads a field, as in [Set 02, Q7](ANSWERS_02.md#q7-intermediate--structs-and-arrays).
  A UDF can also return an `ArrayType` (a Python list) or a `MapType` (a dict).
* The regular expression accepts `$` or `USD`, optional spaces, and a number; anything else
  (`N/A`) gives `None`, so `NULL`. This is a stricter version of the cleaning of
  [Set 02, Q9](ANSWERS_02.md#q9-intermediate--cleaning-a-messy-payments-file), which just deleted
  every non-digit character.
* **Could it be done without a UDF?** Yes: `F.regexp_extract()` can pull out both parts, and
  `F.struct()` can combine them. For a real pipeline, that would be faster. A UDF is
  attractive when the parsing logic gets complicated, or when it already exists as tested Python
  code.

---

## Q8 (Intermediate+) — pandas on whole groups

```python
>>> def zscore(pdf: pd.DataFrame) -> pd.DataFrame:
...     pdf = pdf.copy()
...     pdf["z"] = ((pdf["fare"] - pdf["fare"].mean()) / pdf["fare"].std(ddof=0)).round(2)
...     return pdf[["city", "trip_id", "fare", "z"]]
...
>>> with_z = trips.groupBy("city").applyInPandas(
...     zscore, schema="city STRING, trip_id STRING, fare DOUBLE, z DOUBLE")
>>> with_z.filter("abs(z) > 1.5").orderBy("city", "trip_id").show()
+--------+-------+-----+----+
|    city|trip_id| fare|   z|
+--------+-------+-----+----+
| Oakland|    t05|38.25|2.08|
|San Jose|    t13|110.0|3.14|
+--------+-------+-----+----+

>>> @F.pandas_udf("double")
... def rate(fare: pd.Series, km: pd.Series) -> float:
...     return round(float(fare.sum() / km.sum()), 3)
...
>>> trips.groupBy("city").agg(rate("fare", "distance_km").alias("rate")).orderBy("city").show()
+--------+-----+
|    city| rate|
+--------+-----+
| Oakland|2.592|
|San Jose|3.225|
+--------+-----+

>>> try:
...     trips.groupBy("city").agg(rate("fare", "distance_km"), F.count("*")).collect()
... except Exception as e:
...     print(type(e).__name__, str(e)[:60])
...
AnalysisException [INVALID_PANDAS_UDF_PLACEMENT] The group aggregate pandas UD

>>> def double_fares(batches):
...     for pdf in batches:
...         pdf["fare"] = pdf["fare"] * 2
...         yield pdf[["trip_id", "fare"]]
...
>>> trips.mapInPandas(double_fares, schema="trip_id STRING, fare DOUBLE").orderBy("trip_id").show(2)
+-------+----+
|trip_id|fare|
+-------+----+
|    t01|29.0|
|    t02|60.0|
+-------+----+
only showing top 2 rows
```

**Notes:**

* **`groupBy(...).applyInPandas(function, schema)`** gives your function **each group as a whole
  pandas DataFrame**, and puts the returned DataFrames back together. You can use the whole
  pandas library inside it, here a **z-score**: how many standard deviations each fare is from
  its city's mean. The two outliers stand out: `t13` (3.14) in San Jose, and `t05` (2.08) in Oakland.
  * **You must give the output schema**, because Spark can't guess what pandas will return.
  * Each group must **fit in memory** on one executor, as with `groupByKey()` in the RDD sets.
* **A grouped aggregate pandas UDF** (`Series -> float`) reduces each group's column to one
  value, and is used in `agg()`. Here it computes the "ratio of sums" rate of
  [Set 04, Q9](ANSWERS_04.md#q9-intermediate--grouping-by-computed-values-and-the-average-trap).
  It **can't be mixed** with built-in aggregates in the same `agg()`. Compute them separately,
  and join the results if you need both.
* **`mapInPandas(function, schema)`** gives your function an **iterator of pandas DataFrames**, one
  per batch, and is the DataFrame version of `mapPartitions()`
  ([RDD Set 03](../rdd_practice_questions/QUESTIONS_03.md)). It's the place for per-batch setup,
  such as loading a model once and scoring each batch.
* A z-score and this rate can both be written with **window functions** and built-in aggregates
  ([Set 06](QUESTIONS_06.md)). The pandas versions are worth it when the logic needs pandas, NumPy,
  or a machine-learning library.

---

## Q9 (Intermediate+) — When a UDF is the right tool: fuzzy matching

```python
>>> import difflib
>>> cities = spark.sparkContext.broadcast(["San Jose", "Oakland", "Palo Alto"])
>>> @F.udf(StringType())
... def fix_city(name):
...     if name is None:
...         return None
...     key = name.strip().lower()
...     aliases = {"sj": "San Jose", "oak": "Oakland"}
...     if key in aliases:
...         return aliases[key]
...     lowered = [c.lower() for c in cities.value]
...     match = difflib.get_close_matches(key, lowered, n=1, cutoff=0.6)
...     return cities.value[lowered.index(match[0])] if match else None
...
>>> messy = spark.createDataFrame(
...     [("San Jose",), (" san jose ",), ("SJ",), ("Oaklnd",), ("OAKLAND",),
...      ("Palo Alto",), ("paloalto",), ("Berkeley",), (None,)], "city STRING")
>>> messy.select("city", fix_city("city").alias("fixed")).show()
+----------+---------+
|      city|    fixed|
+----------+---------+
|  San Jose| San Jose|
| san jose | San Jose|
|        SJ| San Jose|
|    Oaklnd|  Oakland|
|   OAKLAND|  Oakland|
| Palo Alto|Palo Alto|
|  paloalto|Palo Alto|
|  Berkeley|     NULL|
|      NULL|     NULL|
+----------+---------+
```

**Notes:**

* **A real reason for a UDF:** matching **misspelled** names ("Oaklnd", "paloalto") to the
  closest known name is "fuzzy" logic, with no built-in Spark function for it. Python's standard
  library has `difflib.get_close_matches()`, which compares the strings and returns the best match
  above a similarity `cutoff`.
* **The function does the cheap work first:** `None` stays `None`, then exact aliases (`SJ`),
  then fuzzy matching. A name that matches nothing well enough (`Berkeley`) becomes `NULL`, so it
  can be **reviewed**, rather than forced into the wrong city.
* **The list of cities is a broadcast variable** ([RDD Set 09](../rdd_practice_questions/QUESTIONS_09.md)):
  it's sent to each executor once. For 3 cities it hardly matters, but a real reference list
  can have thousands of names.
* `difflib` is slow on big data (it compares the strings character by character). At scale, you'd
  normalize first with built-ins (`lower()`, `trim()`, `regexp_replace()`), so that most
  rows match **exactly** with a join, and run the fuzzy UDF only on the rows that are still
  unmatched.
* The same function could be a pandas UDF, using `Series.map(...)` on each batch. That
  saves the per-row transfer cost, but not the cost of `difflib` itself.
