# PySpark DataFrame Practice Questions — Set 08: User-Defined Functions (UDFs)

Nine practice questions, three at each level, meant to be solved **interactively** in
the `bin/pyspark` shell. This set is about **UDFs**: your own Python functions, used on
DataFrame columns. You'll write plain Python UDFs and pandas UDFs, see their traps (types,
`NULL`s, rounding, speed), and learn **when** a UDF is the right tool and when a built-in
function is better.

| # | Level | Topic | Input |
|---|---|---|---|
| [Q1](#q1-basic--a-first-python-udf) | Basic | A first Python UDF | [`trips.csv`](data/trips.csv) |
| [Q2](#q2-basic--the-return-type-matters) | Basic | The return type matters | [`trips.csv`](data/trips.csv) |
| [Q3](#q3-basic--udfs-and-null) | Basic | UDFs and `NULL` | [`trips.csv`](data/trips.csv) |
| [Q4](#q4-intermediate--using-a-udf-in-sql) | Intermediate | Using a UDF in SQL | [`trips.csv`](data/trips.csv) |
| [Q5](#q5-intermediate--a-pandas-udf-and-a-rounding-surprise) | Intermediate | A pandas UDF, and a rounding surprise | [`trips.csv`](data/trips.csv) |
| [Q6](#q6-intermediate--why-built-in-functions-are-better) | Intermediate | Why built-in functions are better | [`trips.csv`](data/trips.csv) |
| [Q7](#q7-intermediate--a-udf-that-returns-a-struct) | Intermediate+ | A UDF that returns a struct | [`payments_raw.csv`](data/payments_raw.csv) |
| [Q8](#q8-intermediate--pandas-on-whole-groups) | Intermediate+ | pandas on whole groups | [`trips.csv`](data/trips.csv) |
| [Q9](#q9-intermediate--when-a-udf-is-the-right-tool-fuzzy-matching) | Intermediate+ | When a UDF is the right tool: fuzzy matching | Python list |

The operations for this set:

* **Python UDFs:** `@F.udf(returnType=...)`, `F.udf(function, returnType)`, `useArrow=False`,
  and `spark.udf.register(name, function, returnType)` for SQL
* **pandas UDFs:** `@F.pandas_udf(returnType)` with type hints (`pd.Series -> pd.Series`, or
  `pd.Series -> float` for an aggregate)
* **pandas on groups and batches:** `groupBy(...).applyInPandas(function, schema)` and
  `mapInPandas(function, schema)`
* **Types:** `StringType`, `IntegerType`, `DoubleType`, `StructType`, `StructField` (from
  `pyspark.sql.types`)

You may also use everything from Sets [01](QUESTIONS_01.md)–[07](QUESTIONS_07.md).

Try each question on your own before you open the [answers](ANSWERS_08.md).

## Files

| File | Purpose |
|---|---|
| [QUESTIONS_08.md](QUESTIONS_08.md) | The nine questions (this page) |
| [ANSWERS_08.md](ANSWERS_08.md) | Step-by-step solutions, with real output from Spark 4.2.0 |
| [data/trips.csv](data/trips.csv) | The 20 trips |
| [data/payments_raw.csv](data/payments_raw.csv) | The messy payments of Set 02, Q9 |

## Getting started

pandas UDFs need the **pandas** and **pyarrow** Python packages, on the machine that runs
`pyspark` (the answers used pandas 2.2 and pyarrow 21). Check that they're installed:

```bash
python3 -c "import pandas, pyarrow; print(pandas.__version__, pyarrow.__version__)"
```

Then start `$SPARK_HOME/bin/pyspark` **from this folder**, as in
[Set 01](QUESTIONS_01.md#getting-started), do the questions **in order**, and start
with this setup:

```python
import io, contextlib
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField

trips_ddl = ("trip_id STRING, driver_id STRING, rider STRING, city STRING, "
             "hour INT, distance_km DOUBLE, fare DOUBLE, rating INT")
trips = spark.read.csv("data/trips.csv", header=True, schema=trips_ddl)

def plan_of(df):                       # what explain() prints, as a string (from Set 05, Q5)
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain()
    return buffer.getvalue()
```

**Don't `cache()` anything in this set.** In our Spark 4.2.0 test setup, Python UDFs sometimes
hung after a `cache()` in the same session (see the note in the answers to Q6).

---

## Q1 (Basic) — A first Python UDF

**Tasks:**

1. Write a UDF `stars` with the decorator `@F.udf(returnType=StringType())`. It turns a rating
   into that many `*` characters (`5` → `*****`), or `"not rated"` when there's no rating. Show
   `trip_id`, `rating`, and `stars` for 4 trips.
2. Write a plain Python function `period(hour)` that returns `"morning"` (before 12),
   `"afternoon"` (before 17), or `"evening"`. Wrap it with `F.udf()` as `period_udf`, and count
   the trips per period.

**Expected output:**

```
+-------+------+---------+
|trip_id|rating|    stars|
+-------+------+---------+
|    t01|     5|    *****|
|    t02|     4|     ****|
|    t03|  NULL|not rated|
|    t04|     5|    *****|
+-------+------+---------+
only showing top 4 rows

+---------+-----+
|   period|count|
+---------+-----+
|afternoon|    2|
|  evening|   13|
|  morning|    5|
+---------+-----+
```

---

## Q2 (Basic) — The return type matters

**Task:** the function `lambda x: x / 2` returns a Python `float`. Wrap it in four UDFs:

* `half_int`: declared as `IntegerType()`;
* `half_int_no_arrow`: declared as `IntegerType()`, with `useArrow=False`;
* `half_double`: declared as `DoubleType()`;
* `half_default`: with **no** return type.

Show `trip_id`, `fare`, and the four results (`as_int`, `as_int_no_arrow`, `as_double`,
`default`) for 3 trips. Then print the `dtypes` of a `select()` of `half_default`. What went wrong
with each wrong declaration? Was there an error message?

**Expected output:**

```
+-------+----+------+---------------+---------+-------+
|trip_id|fare|as_int|as_int_no_arrow|as_double|default|
+-------+----+------+---------------+---------+-------+
|    t01|14.5|     7|           NULL|     7.25|   7.25|
|    t02|30.0|    15|           NULL|     15.0|   15.0|
|    t03|9.75|     4|           NULL|    4.875|  4.875|
+-------+----+------+---------------+---------+-------+
only showing top 3 rows

[('d', 'string')]
```

---

## Q3 (Basic) — UDFs and NULL

**Tasks:**

1. Write a UDF `times_20` that multiplies the rating by 20 (`IntegerType`), **without** any
   special care. `collect()` it on all trips. Catch the error, and print its type, and whether
   its message contains `"TypeError"` and `"NoneType"`. What happened?
2. Fix it as `times_20_safe`, and show `trip_id`, `rating`, and the `score` for `t01` and `t03`.

**Expected output:**

```
PythonException True True
+-------+------+-----+
|trip_id|rating|score|
+-------+------+-----+
|    t01|     5|  100|
|    t03|  NULL| NULL|
+-------+------+-----+
```

---

## Q4 (Intermediate) — Using a UDF in SQL

**Tasks:**

1. **Register** the stars function from Q1 for SQL, under the name `stars`, with
   `spark.udf.register()`. Keep the returned UDF in a variable.
2. Make a temporary view `trips`, and with SQL, show `trip_id` and `stars(rating)` of the
   first 3 Oakland trips (by `trip_id`).
3. Use the registered function in DataFrame code, with `selectExpr()`, for 2 trips.

**Expected output:**

```
+-------+---------+
|trip_id|    stars|
+-------+---------+
|    t03|not rated|
|    t05|     ****|
|    t08|    *****|
+-------+---------+

+-------+-----+
|trip_id|    s|
+-------+-----+
|    t01|*****|
|    t02| ****|
+-------+-----+
only showing top 2 rows
```

---

## Q5 (Intermediate) — A pandas UDF, and a rounding surprise

**Tasks:**

1. Write a **pandas UDF** `per_km_pandas(fare, km)` that returns the fare per km, rounded to 2
   decimals, working on whole `pd.Series`.
2. Write the same calculation as a plain **Python UDF**, `per_km_python`.
3. Make `compare`: `trip_id`, `fare`, `distance_km`, and the fare per km computed three ways:
   with the pandas UDF (`pandas`), the Python UDF (`python`), and the **built-in** `F.round()`
   (`builtin`). Show 4 trips.
4. Show the rows where the three answers **don't** all agree. Why do they differ?

**Expected output:**

```
+-------+----+-----------+------+------+-------+
|trip_id|fare|distance_km|pandas|python|builtin|
+-------+----+-----------+------+------+-------+
|    t01|14.5|        5.2|  2.79|  2.79|   2.79|
|    t02|30.0|       12.0|   2.5|   2.5|    2.5|
|    t03|9.75|        3.1|  3.15|  3.15|   3.15|
|    t04|19.0|        7.5|  2.53|  2.53|   2.53|
+-------+----+-----------+------+------+-------+
only showing top 4 rows

+-------+----+-----------+------+------+-------+
|trip_id|fare|distance_km|pandas|python|builtin|
+-------+----+-----------+------+------+-------+
|    t11|21.0|        8.0|  2.62|  2.62|   2.63|
+-------+----+-----------+------+------+-------+
```

**Hint:** compute 21.0 / 8.0 yourself. How should 2.625 be rounded to 2 decimals?

---

## Q6 (Intermediate) — Why built-in functions are better

**Tasks:**

1. With `plan_of()`, check whether the plans of three `select()`s contain `"ArrowEvalPython"`
   (or `"EvalPython"`): one with `per_km_python`, one with `per_km_pandas`, and one with the
   built-in `F.round()`. Print the three results as a tuple.
2. Make the Python UDF again with `useArrow=False`, and check whether its plan contains
   `"BatchEvalPython"`.
3. Print the setting `spark.sql.execution.pythonUDF.arrow.enabled`.
4. **(Optional, slower.)** Time the sum of the fare per km over
   `spark.range(10_000_000)` (make the columns with
   `(F.col("id") % 100 + 1).cast("double")` and `(F.col("id") % 37 + 1).cast("double")`),
   computed with the built-in function, the pandas UDF, and the Python UDF, using `time.time()`.
   Which is fastest?

**Expected output** (tasks 1–3):

```
(True, True, False)
True
'true'
```

*Think about it:* what does the `EvalPython` step in a plan mean for your data?

---

## Q7 (Intermediate+) — A UDF that returns a struct

**Input:** [`data/payments_raw.csv`](data/payments_raw.csv) (with a header), whose `amount`
column holds text like `$17.40`, `USD 42.08`, `$ 33.60`, `N/A`, or nothing.

**Tasks:**

1. Write a UDF `parse_money(text)` that returns a **struct** with two fields, `currency`
   (`STRING`) and `value` (`DOUBLE`). For text that is `$` or `USD`, optional spaces, and a number,
   it returns `("USD", the number)`. For anything else (or `None`), it returns `None`.
2. Apply it to `amount`, as a column `money`, and print the schema.
3. Show `payment_id`, `amount`, `money`, and `money.value` for the payments `p01`, `p05`, `p10`,
   `p13`, and `p18`.

**Expected output:**

```
root
 |-- payment_id: string (nullable = true)
 |-- amount: string (nullable = true)
 |-- money: struct (nullable = true)
 |    |-- currency: string (nullable = true)
 |    |-- value: double (nullable = true)

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

**Hint:** the regular expression `^\s*(\$|USD)\s*([0-9]+(?:\.[0-9]+)?)\s*$` matches the valid
amounts, and its second group is the number.

---

## Q8 (Intermediate+) — pandas on whole groups

**Tasks:**

1. Write a pandas function `zscore(pdf)` that receives **one city's trips** as a pandas
   DataFrame, and adds a column `z`: the fare's z-score within the city,
   `(fare − mean) / standard deviation` (use `std(ddof=0)`), rounded to 2 decimals. It returns
   `city`, `trip_id`, `fare`, and `z`. Apply it with `groupBy("city").applyInPandas(...)`, and show
   the trips whose `z` is more than 1.5 away from 0.
2. Write a **grouped aggregate** pandas UDF `rate(fare, km)` (`pd.Series, pd.Series -> float`) that
   returns the total fare ÷ the total distance, rounded to 3 decimals. Show it per city.
3. Try to use `rate(...)` **together** with `F.count("*")` in the same `agg()`. Print the error's
   type and the first 60 characters of its message.
4. With `mapInPandas()`, double every fare, returning `trip_id` and `fare`. Show 2 trips.

**Expected output:**

```
+--------+-------+-----+----+
|    city|trip_id| fare|   z|
+--------+-------+-----+----+
| Oakland|    t05|38.25|2.08|
|San Jose|    t13|110.0|3.14|
+--------+-------+-----+----+

+--------+-----+
|    city| rate|
+--------+-----+
| Oakland|2.592|
|San Jose|3.225|
+--------+-----+

AnalysisException [INVALID_PANDAS_UDF_PLACEMENT] The group aggregate pandas UD
+-------+----+
|trip_id|fare|
+-------+----+
|    t01|29.0|
|    t02|60.0|
+-------+----+
only showing top 2 rows
```

---

## Q9 (Intermediate+) — When a UDF is the right tool: fuzzy matching

Some data has **misspelled** values that no exact rule can fix. Here are some city names
typed by hand:

```python
messy = spark.createDataFrame(
    [("San Jose",), (" san jose ",), ("SJ",), ("Oaklnd",), ("OAKLAND",),
     ("Palo Alto",), ("paloalto",), ("Berkeley",), (None,)], "city STRING")
```

**Task:** write a UDF `fix_city(name)` that returns the correct city name from the list
`["San Jose", "Oakland", "Palo Alto"]`, or `None`:

* `None` stays `None`;
* the names are compared after `strip()` and `lower()`;
* the aliases `sj` and `oak` mean San Jose and Oakland;
* otherwise, use Python's `difflib.get_close_matches(name, candidates, n=1, cutoff=0.6)` to find
  the closest name, or `None` if nothing is close enough.

Send the list of cities to the executors as a **broadcast variable**. Show `city` and the
fixed name (`fixed`) for every row.

**Expected output:**

```
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

*Think about it:* why is this a good use of a UDF, when Q1's UDFs were not? How would you make it
fast for a billion rows?
