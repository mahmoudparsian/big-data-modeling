# PySpark DataFrame Practice Questions — Set 01: Creating and Inspecting DataFrames

Nine practice questions, three at each level, meant to be solved **interactively** in
the `bin/pyspark` shell. This set is about **getting data into a DataFrame**, and
**looking at it**: its rows, its columns, and its schema.

| # | Level | Topic | Input |
|---|---|---|---|
| [Q1](#q1-basic--a-dataframe-from-a-python-list) | Basic | A DataFrame from a Python list | Python list |
| [Q2](#q2-basic--giving-a-dataframe-a-schema) | Basic | Giving a DataFrame a schema | Python list |
| [Q3](#q3-basic--rows-dicts-and-getting-data-back) | Basic | `Row`s, dicts, and getting data back | Python lists |
| [Q4](#q4-intermediate--reading-csv-files-with-and-without-a-header) | Intermediate | Reading CSV files, with and without a header | [`data/trips.csv`](data/trips.csv), [`data/trips_no_header.csv`](data/trips_no_header.csv) |
| [Q5](#q5-intermediate--a-schema-for-the-csv-file-and-summary-statistics) | Intermediate | A schema for the CSV file, and summary statistics | [`data/trips.csv`](data/trips.csv) |
| [Q6](#q6-intermediate--nested-json-data) | Intermediate | Nested JSON data | [`data/riders.json`](data/riders.json) |
| [Q7](#q7-intermediate--from-rdd-to-dataframe-and-back) | Intermediate+ | From RDD to DataFrame and back | [`trips.txt`](../rdd_practice_questions/data/trips.txt) from the RDD sets |
| [Q8](#q8-intermediate--bad-rows-in-a-csv-file) | Intermediate+ | Bad rows in a CSV file | [`data/trips_raw.csv`](data/trips_raw.csv) |
| [Q9](#q9-intermediate--profiling-a-dataframe) | Intermediate+ | Profiling a DataFrame | [`data/trips.csv`](data/trips.csv) |

The operations for this set:

* **Creating:** `spark.createDataFrame(data, schema)`, `rdd.toDF(names)`,
  `spark.read.csv(...)`, `spark.read.json(...)`
* **Schemas:** a DDL string like `"name STRING, age INT"`, or a `StructType` of `StructField`s
* **Looking:** `show()`, `printSchema()`, `columns`, `dtypes`, `schema`, `count()`,
  `describe()`, `summary()`, `isEmpty()`
* **Getting rows back:** `collect()`, `take(n)`, `head(n)`, `first()`, and `df.rdd`

The data set is the ride-sharing data from
[RDD Set 10](../rdd_practice_questions/QUESTIONS_10.md), now as CSV and JSON files with
**headers and field names**, so you can compare the two APIs.

Try each question on your own before you open the [answers](ANSWERS_01.md).

## Files

| File | Purpose |
|---|---|
| [QUESTIONS_01.md](QUESTIONS_01.md) | The nine questions (this page) |
| [ANSWERS_01.md](ANSWERS_01.md) | Step-by-step solutions, with real output from Spark 4.2.0 |
| [data/trips.csv](data/trips.csv) | 20 trips: `trip_id,driver_id,rider,city,hour,distance_km,fare,rating` (an empty rating means "not rated") |
| [data/trips_no_header.csv](data/trips_no_header.csv) | The same 20 trips, **without** a header line, for Q4 |
| [data/riders.json](data/riders.json) | 7 riders, as JSON Lines, with a nested `home` object and a `tags` list |
| [data/trips_raw.csv](data/trips_raw.csv) | The 20 trips plus 2 **bad** rows, for Q8 |

## Getting started

Open a terminal **in this folder** (so the relative paths `data/...` work), then start
the shell:

```bash
$SPARK_HOME/bin/pyspark
```

The shell already gives you `spark` (the `SparkSession`, the entry point for DataFrames)
and `sc` (the `SparkContext`, for RDDs). Check the version:

```python
>>> spark.version
'4.2.0'
```

Tips:

* Do the questions **in order**, in one shell session. Later questions reuse DataFrames
  (like `trips`) from earlier ones.
* `show()` prints at most 20 rows, and cuts long values to 20 characters. Use
  `show(n)` for fewer rows, and `show(truncate=False)` for full values.
* Like RDDs, DataFrames are **lazy**: reading a file or adding a transformation does
  nothing until an **action** (`show()`, `count()`, `collect()`, ...) runs.

---

## Q1 (Basic) — A DataFrame from a Python list

**Input:**

```python
data = [("Ana", "sedan", 2019), ("Ben", "suv", 2021), ("Chloe", "sedan", 2020)]
```

**Tasks:**

1. Create a DataFrame `drivers` from `data`, with the column names `name`, `car`, and
   `since`. Show it.
2. Print its schema. What type did Spark choose for `since`?
3. Print its number of rows, its column names, and its `dtypes`.

**Expected output:**

```
+-----+-----+-----+
| name|  car|since|
+-----+-----+-----+
|  Ana|sedan| 2019|
|  Ben|  suv| 2021|
|Chloe|sedan| 2020|
+-----+-----+-----+

root
 |-- name: string (nullable = true)
 |-- car: string (nullable = true)
 |-- since: long (nullable = true)

3
['name', 'car', 'since']
[('name', 'string'), ('car', 'string'), ('since', 'bigint')]
```

---

## Q2 (Basic) — Giving a DataFrame a schema

**Input:**

```python
data = [("Ana", "sedan", 2019), ("Ben", None, 2021)]
```

**Tasks:**

1. Create a DataFrame from `data` with the schema given as a **DDL string**:
   `"name STRING, car STRING, since INT"`. Print the schema and show the data.
2. Create the same DataFrame with a **`StructType`** schema in which `name` is **not
   nullable**. Print the schema.
3. Now try to create a DataFrame with the `StructType` schema from the row
   `(None, "suv", 2022)`. What happens? Print the error's type and message.

**Expected output:**

```
root
 |-- name: string (nullable = true)
 |-- car: string (nullable = true)
 |-- since: integer (nullable = true)

+----+-----+-----+
|name|  car|since|
+----+-----+-----+
| Ana|sedan| 2019|
| Ben| NULL| 2021|
+----+-----+-----+

root
 |-- name: string (nullable = false)
 |-- car: string (nullable = true)
 |-- since: integer (nullable = true)

PySparkValueError [FIELD_NOT_NULLABLE_WITH_NAME] field name: This field is not nullable, but got None.
```

**Hint:** `from pyspark.sql.types import StructType, StructField, StringType, IntegerType`

---

## Q3 (Basic) — Rows, dicts, and getting data back

**Tasks:**

1. Create a DataFrame `from_rows` from a list of two `Row`s:
   `Row(name="Ana", car="sedan", since=2019)` and `Row(name="Ben", car="suv", since=2021)`.
   Print its schema. (`from pyspark.sql import Row`)
2. Create a DataFrame from a list of two **dicts**,
   `{"name": "Ana", "since": 2019}` and `{"name": "Ben", "since": 2021}`, and show it.
3. `collect()` the rows of `from_rows`. From the first row, get the name (as an
   attribute), the car (as a key), and `since` (by position), as one tuple. Then turn the
   first row into a dict.
4. Compare `first()`, `take(1)`, and `head(2)`.

**Expected output:**

```
root
 |-- name: string (nullable = true)
 |-- car: string (nullable = true)
 |-- since: long (nullable = true)

+----+-----+
|name|since|
+----+-----+
| Ana| 2019|
| Ben| 2021|
+----+-----+

[Row(name='Ana', car='sedan', since=2019), Row(name='Ben', car='suv', since=2021)]
('Ana', 'sedan', 2019)
{'name': 'Ana', 'car': 'sedan', 'since': 2019}
Row(name='Ana', car='sedan', since=2019)
[Row(name='Ana', car='sedan', since=2019)]
[Row(name='Ana', car='sedan', since=2019), Row(name='Ben', car='suv', since=2021)]
```

---

## Q4 (Intermediate) — Reading CSV files, with and without a header

CSV files come in two kinds: with a **header line** (the column names) and without one.
Spark can't tell them apart, so **you** must tell it which kind you're reading.

### Part A: a file **with** a header line

**Input:** [`data/trips.csv`](data/trips.csv), which starts like this:

```
trip_id,driver_id,rider,city,hour,distance_km,fare,rating
t01,d1,r1,San Jose,8,5.2,14.50,5
t02,d2,r2,San Jose,9,12.0,30.00,4
t03,d1,r3,Oakland,9,3.1,9.75,
...
```

**Tasks:**

1. Read it with `spark.read.csv(path)` and **no options**. Print the schema, show 3 rows,
   and count the rows. What went wrong?
2. Read it again with `header=True`. Print the `dtypes`, and count the rows.
3. Read it again with `header=True, inferSchema=True`, as `trips`. Print the schema and
   show 5 rows.
4. How many trips have **no rating**?

**Expected output:**

```
root
 |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
 |-- _c3: string (nullable = true)
 |-- _c4: string (nullable = true)
 |-- _c5: string (nullable = true)
 |-- _c6: string (nullable = true)
 |-- _c7: string (nullable = true)

+-------+---------+-----+--------+----+-----------+-----+------+
|    _c0|      _c1|  _c2|     _c3| _c4|        _c5|  _c6|   _c7|
+-------+---------+-----+--------+----+-----------+-----+------+
|trip_id|driver_id|rider|    city|hour|distance_km| fare|rating|
|    t01|       d1|   r1|San Jose|   8|        5.2|14.50|     5|
|    t02|       d2|   r2|San Jose|   9|       12.0|30.00|     4|
+-------+---------+-----+--------+----+-----------+-----+------+
only showing top 3 rows
21
[('trip_id', 'string'), ('driver_id', 'string'), ('rider', 'string'), ('city', 'string'), ('hour', 'string'), ('distance_km', 'string'), ('fare', 'string'), ('rating', 'string')]
20
root
 |-- trip_id: string (nullable = true)
 |-- driver_id: string (nullable = true)
 |-- rider: string (nullable = true)
 |-- city: string (nullable = true)
 |-- hour: integer (nullable = true)
 |-- distance_km: double (nullable = true)
 |-- fare: double (nullable = true)
 |-- rating: integer (nullable = true)

+-------+---------+-----+--------+----+-----------+-----+------+
|trip_id|driver_id|rider|    city|hour|distance_km| fare|rating|
+-------+---------+-----+--------+----+-----------+-----+------+
|    t01|       d1|   r1|San Jose|   8|        5.2| 14.5|     5|
|    t02|       d2|   r2|San Jose|   9|       12.0| 30.0|     4|
|    t03|       d1|   r3| Oakland|   9|        3.1| 9.75|  NULL|
|    t04|       d3|   r1|San Jose|  12|        7.5| 19.0|     5|
|    t05|       d4|   r4| Oakland|  13|       15.3|38.25|     4|
+-------+---------+-----+--------+----+-----------+-----+------+
only showing top 5 rows
3
```

**Hint for task 4:** `trips.filter("rating IS NULL")` keeps the rows where the condition
(written in SQL) is true.

### Part B: a file **without** a header line

**Input:** [`data/trips_no_header.csv`](data/trips_no_header.csv): the same 20 trips,
but the file starts directly with the data:

```
t01,d1,r1,San Jose,8,5.2,14.50,5
t02,d2,r2,San Jose,9,12.0,30.00,4
...
```

**Tasks:**

5. Read it with `inferSchema=True` (and no `header` option), as `no_header`. Print the
   schema, and show 3 rows. What are the column names?
6. Give the columns their real names with `toDF()`, using this list, and print the `dtypes`:

   ```python
   names = ["trip_id", "driver_id", "rider", "city", "hour", "distance_km", "fare", "rating"]
   ```

7. Read the file again, this time with an **explicit schema** (a DDL string with the same
   names, and the types `STRING`, `STRING`, `STRING`, `STRING`, `INT`, `DOUBLE`, `DOUBLE`,
   `INT`). Count the rows, and check that it's **exactly** the same data as `trips` from
   Part A. (Hint: compare `sorted(df.collect())` of both.)
8. What happens if you read this file with `header=True` by mistake? Print the column
   names, and count the rows.

**Expected output:**

```
root
 |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
 |-- _c3: string (nullable = true)
 |-- _c4: integer (nullable = true)
 |-- _c5: double (nullable = true)
 |-- _c6: double (nullable = true)
 |-- _c7: integer (nullable = true)

+---+---+---+--------+---+----+----+----+
|_c0|_c1|_c2|     _c3|_c4| _c5| _c6| _c7|
+---+---+---+--------+---+----+----+----+
|t01| d1| r1|San Jose|  8| 5.2|14.5|   5|
|t02| d2| r2|San Jose|  9|12.0|30.0|   4|
|t03| d1| r3| Oakland|  9| 3.1|9.75|NULL|
+---+---+---+--------+---+----+----+----+
only showing top 3 rows
[('trip_id', 'string'), ('driver_id', 'string'), ('rider', 'string'), ('city', 'string'), ('hour', 'int'), ('distance_km', 'double'), ('fare', 'double'), ('rating', 'int')]
20
True
['t01', 'd1', 'r1', 'San Jose', '8', '5.2', '14.50', '5']
19
```

*Think about it:* which of the four ways of reading in this question are **wrong** for
their file, and how would you notice?

---

## Q5 (Intermediate) — A schema for the CSV file, and summary statistics

**Tasks:**

1. Read [`data/trips.csv`](data/trips.csv) again as `trips`, this time with
   `header=True` and an **explicit schema**, given as this DDL string:

   ```python
   trips_ddl = ("trip_id STRING, driver_id STRING, rider STRING, city STRING, "
                "hour INT, distance_km DOUBLE, fare DOUBLE, rating INT")
   ```

   Print the `dtypes`.
2. Use `describe()` to show the summary statistics of `distance_km`, `fare`, and `rating`.
   Why is the `rating` count different?
3. Use `summary()` to show the count, min, 25%, 50%, 75%, and max of `distance_km` and
   `fare` only.

**Expected output:**

```
[('trip_id', 'string'), ('driver_id', 'string'), ('rider', 'string'), ('city', 'string'), ('hour', 'int'), ('distance_km', 'double'), ('fare', 'double'), ('rating', 'int')]
+-------+-----------------+------------------+------------------+
|summary|      distance_km|              fare|            rating|
+-------+-----------------+------------------+------------------+
|  count|               20|                20|                17|
|   mean|             8.02|           23.8125|4.0588235294117645|
| stddev|4.830342692430403|22.112879522791093|1.1974237050483696|
|    min|              2.2|               7.5|                 1|
|    max|             20.5|             110.0|                 5|
+-------+-----------------+------------------+------------------+

+-------+-----------+-----+
|summary|distance_km| fare|
+-------+-----------+-----+
|  count|         20|   20|
|    min|        2.2|  7.5|
|    25%|        4.4| 12.0|
|    50%|        6.6| 17.5|
|    75%|       10.0| 25.0|
|    max|       20.5|110.0|
+-------+-----------+-----+
```

*Think about it:* why is an explicit schema better than `inferSchema=True` for a real
data pipeline? And what do the mean and the median (50%) of `fare` tell you?

---

## Q6 (Intermediate) — Nested JSON data

**Input:** [`data/riders.json`](data/riders.json), in **JSON Lines** format (one JSON
object per line):

```
{"rider": "r1", "name": "Maya", "member_since": 2021, "home": {"city": "San Jose", "zip": "95112"}, "tags": ["student", "frequent"]}
...
```

**Tasks:**

1. Read it with `spark.read.json()` as `riders`, and print the schema. What are the
   types of `home` and `tags`?
2. Show all the rows, **without** cutting the values.
3. Show each rider's id, home city, and zip code.
4. Look at the `tags` of `r3` and `r6`. What's the difference?

**Expected output:**

```
root
 |-- home: struct (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- zip: string (nullable = true)
 |-- member_since: long (nullable = true)
 |-- name: string (nullable = true)
 |-- rider: string (nullable = true)
 |-- tags: array (nullable = true)
 |    |-- element: string (containsNull = true)

+-----------------+------------+----+-----+--------------------+
|home             |member_since|name|rider|tags                |
+-----------------+------------+----+-----+--------------------+
|{San Jose, 95112}|2021        |Maya|r1   |[student, frequent] |
|{San Jose, 95126}|2019        |Omar|r2   |[frequent]          |
|{Oakland, 94607} |2024        |Lena|r3   |[]                  |
|{Oakland, 94610} |2022        |Kofi|r4   |[business]          |
|{Oakland, 94612} |2023        |Ines|r5   |[student]           |
|{San Jose, 95110}|2020        |Tom |r6   |NULL                |
|{San Jose, 95113}|2025        |Yuki|r7   |[business, frequent]|
+-----------------+------------+----+-----+--------------------+

+-----+--------+-----+
|rider|    city|  zip|
+-----+--------+-----+
|   r1|San Jose|95112|
|   r2|San Jose|95126|
|   r3| Oakland|94607|
|   r4| Oakland|94610|
|   r5| Oakland|94612|
|   r6|San Jose|95110|
|   r7|San Jose|95113|
+-----+--------+-----+
```

**Hint for task 3:** `select()` can reach into a struct with a dot: `"home.city"`.

---

## Q7 (Intermediate+) — From RDD to DataFrame and back

**Input:** the RDD sets' trip file, which has **no header**:
[`../rdd_practice_questions/data/trips.txt`](../rdd_practice_questions/data/trips.txt).

**Tasks:**

1. Read it with `sc.textFile(path, 2)`, and turn each line into a tuple
   `(trip_id, driver_id, city, fare)`, with the fare as a `float`.
2. Turn that RDD into a DataFrame with `toDF()` and the column names `trip_id`,
   `driver_id`, `city`, and `fare`. Print the schema, and show 3 rows.
3. Turn the same RDD into a DataFrame with `spark.createDataFrame()` and a DDL schema,
   and print its `dtypes`.
4. Go the other way: get `trips.rdd` (with `trips` from Q5). What type is it? What does
   its first element look like?
5. Use **RDD** operations on `trips.rdd` to compute the total fare per city, sorted.
   Compare with [RDD Set 10, Q1](../rdd_practice_questions/QUESTIONS_10.md#q1-basic--trips-fares-and-distances-per-city).

**Expected output:**

```
root
 |-- trip_id: string (nullable = true)
 |-- driver_id: string (nullable = true)
 |-- city: string (nullable = true)
 |-- fare: double (nullable = true)

+-------+---------+--------+----+
|trip_id|driver_id|    city|fare|
+-------+---------+--------+----+
|    t01|       d1|San Jose|14.5|
|    t02|       d2|San Jose|30.0|
|    t03|       d1| Oakland|9.75|
+-------+---------+--------+----+
only showing top 3 rows
[('trip_id', 'string'), ('driver_id', 'string'), ('city', 'string'), ('fare', 'double')]
'RDD'
Row(trip_id='t01', driver_id='d1', rider='r1', city='San Jose', hour=8, distance_km=5.2, fare=14.5, rating=5)
[('Oakland', 168.25), ('San Jose', 308.0)]
```

---

## Q8 (Intermediate+) — Bad rows in a CSV file

**Input:** [`data/trips_raw.csv`](data/trips_raw.csv): the 20 trips, plus two bad rows:

```
t21,d2,r2,Oakland,eleven,4.0,12.00,3        <- the hour isn't a number
t22,d1,r1,San Jose,14,6.1                   <- only 6 fields instead of 8
```

Use the `trips_ddl` schema from Q5 for every read.

**Tasks:**

1. Read the file in the default `PERMISSIVE` mode, with an extra column
   `_corrupt_record STRING` added to the schema and the option
   `columnNameOfCorruptRecord="_corrupt_record"`. `cache()` it. Count the rows, and show
   `trip_id`, `hour`, `fare`, and `_corrupt_record` for the bad rows only (without cutting
   the values).
2. Read the file with `mode="DROPMALFORMED"`. Count the rows. Then count them with
   `len(df.collect())`, and with `df.cache().count()`. Can you explain the difference?
3. Read the file with `mode="FAILFAST"`. Does `count()` fail? Does `collect()` fail?
   (Catch the error, and print whether its message contains
   `"Malformed records are detected"`.)

**Expected output:**

```
22
+-------+----+----+------------------------------------+
|trip_id|hour|fare|_corrupt_record                     |
+-------+----+----+------------------------------------+
|t21    |NULL|12.0|t21,d2,r2,Oakland,eleven,4.0,12.00,3|
|t22    |14  |NULL|t22,d1,r1,San Jose,14,6.1           |
+-------+----+----+------------------------------------+

22
20
20
22
True
```

---

## Q9 (Intermediate+) — Profiling a DataFrame

When you get a new data set, the first job is to **profile** it: which columns does it
have, which are numeric, how many values are missing, how many are different?

**Input:** `trips` from Q5.

**Tasks:**

1. Using `trips.schema`, list the names of the **numeric** columns (`int` or `double`).
   Then list `(name, type, nullable)` for the first 3 fields.
2. With **one** `select()`, count the `NULL`s in **every** column.
3. With **one** `select()`, count the **different** values in every column.
4. Use `isEmpty()` to check whether `trips` has any rows, and whether any trip has a
   fare over 1000.

**Expected output:**

```
['hour', 'distance_km', 'fare', 'rating']
[('trip_id', 'string', True), ('driver_id', 'string', True), ('rider', 'string', True)]
+-------+---------+-----+----+----+-----------+----+------+
|trip_id|driver_id|rider|city|hour|distance_km|fare|rating|
+-------+---------+-----+----+----+-----------+----+------+
|      0|        0|    0|   0|   0|          0|   0|     3|
+-------+---------+-----+----+----+-----------+----+------+

+-------+---------+-----+----+----+-----------+----+------+
|trip_id|driver_id|rider|city|hour|distance_km|fare|rating|
+-------+---------+-----+----+----+-----------+----+------+
|     20|        4|    7|   2|  10|         20|  20|     5|
+-------+---------+-----+----+----+-----------+----+------+

(False, True)
```

**Hints:** each field of `trips.schema.fields` has `.name`, `.dataType`, and `.nullable`,
and `.dataType.simpleString()` gives the type's short name. For tasks 2 and 3, use
`from pyspark.sql import functions as F`, and build a **list** of column expressions with
a list comprehension, such as `[F.countDistinct(c).alias(c) for c in trips.columns]`.
To count nulls, `F.count(F.when(F.col(c).isNull(), 1))` works, because `count()` skips
`NULL`s.
