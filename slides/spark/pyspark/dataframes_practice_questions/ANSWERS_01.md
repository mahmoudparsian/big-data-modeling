# PySpark DataFrame Practice Questions — Set 01: Answers

Solutions to the nine [Set 01 practice questions](QUESTIONS_01.md). Every session
below was run in the `bin/pyspark` shell (Spark 4.2.0), started from this folder. The
output is copied from those real runs.

Try the questions yourself first. There are often several correct solutions, and
yours doesn't need to match these exactly.

* [Q1 (Basic): A DataFrame from a Python list](#q1-basic--a-dataframe-from-a-python-list)
* [Q2 (Basic): Giving a DataFrame a schema](#q2-basic--giving-a-dataframe-a-schema)
* [Q3 (Basic): Rows, dicts, and getting data back](#q3-basic--rows-dicts-and-getting-data-back)
* [Q4 (Intermediate): Reading CSV files, with and without a header](#q4-intermediate--reading-csv-files-with-and-without-a-header)
* [Q5 (Intermediate): A schema for the CSV file, and summary statistics](#q5-intermediate--a-schema-for-the-csv-file-and-summary-statistics)
* [Q6 (Intermediate): Nested JSON data](#q6-intermediate--nested-json-data)
* [Q7 (Intermediate+): From RDD to DataFrame and back](#q7-intermediate--from-rdd-to-dataframe-and-back)
* [Q8 (Intermediate+): Bad rows in a CSV file](#q8-intermediate--bad-rows-in-a-csv-file)
* [Q9 (Intermediate+): Profiling a DataFrame](#q9-intermediate--profiling-a-dataframe)

---

## Q1 (Basic) — A DataFrame from a Python list

```python
>>> drivers = spark.createDataFrame(
...     [("Ana", "sedan", 2019), ("Ben", "suv", 2021), ("Chloe", "sedan", 2020)],
...     ["name", "car", "since"])
>>> drivers.show()
+-----+-----+-----+
| name|  car|since|
+-----+-----+-----+
|  Ana|sedan| 2019|
|  Ben|  suv| 2021|
|Chloe|sedan| 2020|
+-----+-----+-----+

>>> drivers.printSchema()
root
 |-- name: string (nullable = true)
 |-- car: string (nullable = true)
 |-- since: long (nullable = true)

>>> drivers.count()
3
>>> drivers.columns
['name', 'car', 'since']
>>> drivers.dtypes
[('name', 'string'), ('car', 'string'), ('since', 'bigint')]
```

**Notes:**

* A **DataFrame** is a table: an RDD of rows **plus a schema**, which gives every
  column a **name** and a **type**. The schema is what lets Spark optimize your
  queries. An RDD of tuples has no idea what's inside each tuple.
* `spark` (the `SparkSession`) is the entry point for DataFrames, as `sc` is for RDDs.
  The `bin/pyspark` shell creates both for you.
* `show()` prints the first 20 rows as a table. `printSchema()` prints the schema as a
  tree. `columns` and `dtypes` give the same information as Python lists.
* **Spark guessed the types** from the data. A Python `int` becomes a `long` (a 64-bit
  integer, called `bigint` in SQL), because Python integers can be big. `printSchema()`
  says `long`, and `dtypes` uses the SQL name, `bigint`. They're the same type.
* `nullable = true` means the column may contain `NULL` (Python's `None`).

---

## Q2 (Basic) — Giving a DataFrame a schema

```python
>>> with_ddl = spark.createDataFrame([("Ana", "sedan", 2019), ("Ben", None, 2021)],
...                                  "name STRING, car STRING, since INT")
>>> with_ddl.printSchema()
root
 |-- name: string (nullable = true)
 |-- car: string (nullable = true)
 |-- since: integer (nullable = true)

>>> with_ddl.show()
+----+-----+-----+
|name|  car|since|
+----+-----+-----+
| Ana|sedan| 2019|
| Ben| NULL| 2021|
+----+-----+-----+

>>> from pyspark.sql.types import StructType, StructField, StringType, IntegerType
>>> schema = StructType([
...     StructField("name", StringType(), nullable=False),
...     StructField("car", StringType(), nullable=True),
...     StructField("since", IntegerType(), nullable=True)])
>>> with_struct = spark.createDataFrame([("Ana", "sedan", 2019), ("Ben", None, 2021)], schema)
>>> with_struct.printSchema()
root
 |-- name: string (nullable = false)
 |-- car: string (nullable = true)
 |-- since: integer (nullable = true)

>>> try:
...     spark.createDataFrame([(None, "suv", 2022)], schema)
... except Exception as e:
...     print(type(e).__name__, e)
...
PySparkValueError [FIELD_NOT_NULLABLE_WITH_NAME] field name: This field is not nullable, but got None.
```

**Notes:**

* There are two ways to write a schema:
  * a **DDL string**, `"name STRING, car STRING, since INT"`: short and readable, the
    same syntax as SQL's `CREATE TABLE`;
  * a **`StructType`** of `StructField(name, type, nullable)`: longer, but it can set
    every detail, such as `nullable=False`, and it can be built by code.
* With a schema, **you** choose the types: `since` is now an `integer` (32-bit), not the
  `long` that Spark guessed in Q1.
* Python's `None` becomes `NULL` in the table.
* `nullable=False` is a **promise** that the column never holds `NULL`. Spark checks it
  when creating the DataFrame from Python data, and refuses the `None`.

---

## Q3 (Basic) — Rows, dicts, and getting data back

```python
>>> from pyspark.sql import Row
>>> from_rows = spark.createDataFrame([Row(name="Ana", car="sedan", since=2019),
...                                    Row(name="Ben", car="suv", since=2021)])
>>> from_rows.printSchema()
root
 |-- name: string (nullable = true)
 |-- car: string (nullable = true)
 |-- since: long (nullable = true)

>>> from_dicts = spark.createDataFrame([{"name": "Ana", "since": 2019},
...                                     {"name": "Ben", "since": 2021}])
>>> from_dicts.show()
+----+-----+
|name|since|
+----+-----+
| Ana| 2019|
| Ben| 2021|
+----+-----+

>>> rows = from_rows.collect()
>>> rows
[Row(name='Ana', car='sedan', since=2019), Row(name='Ben', car='suv', since=2021)]
>>> rows[0].name, rows[0]["car"], rows[0][2]
('Ana', 'sedan', 2019)
>>> rows[0].asDict()
{'name': 'Ana', 'car': 'sedan', 'since': 2019}

>>> from_rows.first()
Row(name='Ana', car='sedan', since=2019)
>>> from_rows.take(1)
[Row(name='Ana', car='sedan', since=2019)]
>>> from_rows.head(2)
[Row(name='Ana', car='sedan', since=2019), Row(name='Ben', car='suv', since=2021)]
```

**Notes:**

* A **`Row`** is like a named tuple. It's what every DataFrame row looks like in Python.
  With `Row`s or dicts, the column names come from the field names or the keys, so you
  don't have to list them separately.
* Three ways to read a field: by name as an attribute (`row.name`), by name as a key
  (`row["car"]`), or by position (`row[2]`). `asDict()` turns a row into a plain dict.
* **Getting data back to the driver:**
  * `collect()` returns **all** rows as a list. It's safe only for **small** results;
  * `take(n)` and `head(n)` return the first `n` rows as a list;
  * `first()` returns just the first row (not in a list).
* `show()` **prints** rows; `collect()` and `take()` **return** them, so you can use them
  in Python code.

---

## Q4 (Intermediate) — Reading CSV files, with and without a header

**Part A: a file with a header line** ([`data/trips.csv`](data/trips.csv))

```python
>>> raw = spark.read.csv("data/trips.csv")
>>> raw.printSchema()
root
 |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
 |-- _c3: string (nullable = true)
 |-- _c4: string (nullable = true)
 |-- _c5: string (nullable = true)
 |-- _c6: string (nullable = true)
 |-- _c7: string (nullable = true)

>>> raw.show(3)
+-------+---------+-----+--------+----+-----------+-----+------+
|    _c0|      _c1|  _c2|     _c3| _c4|        _c5|  _c6|   _c7|
+-------+---------+-----+--------+----+-----------+-----+------+
|trip_id|driver_id|rider|    city|hour|distance_km| fare|rating|
|    t01|       d1|   r1|San Jose|   8|        5.2|14.50|     5|
|    t02|       d2|   r2|San Jose|   9|       12.0|30.00|     4|
+-------+---------+-----+--------+----+-----------+-----+------+
only showing top 3 rows
>>> raw.count()
21

>>> with_header = spark.read.csv("data/trips.csv", header=True)
>>> with_header.dtypes
[('trip_id', 'string'), ('driver_id', 'string'), ('rider', 'string'), ('city', 'string'), ('hour', 'string'), ('distance_km', 'string'), ('fare', 'string'), ('rating', 'string')]
>>> with_header.count()
20

>>> trips = spark.read.csv("data/trips.csv", header=True, inferSchema=True)
>>> trips.printSchema()
root
 |-- trip_id: string (nullable = true)
 |-- driver_id: string (nullable = true)
 |-- rider: string (nullable = true)
 |-- city: string (nullable = true)
 |-- hour: integer (nullable = true)
 |-- distance_km: double (nullable = true)
 |-- fare: double (nullable = true)
 |-- rating: integer (nullable = true)

>>> trips.show(5)
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
>>> trips.filter("rating IS NULL").count()
3
```

**Part B: a file without a header line** ([`data/trips_no_header.csv`](data/trips_no_header.csv))

```python
>>> no_header = spark.read.csv("data/trips_no_header.csv", inferSchema=True)
>>> no_header.printSchema()
root
 |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
 |-- _c3: string (nullable = true)
 |-- _c4: integer (nullable = true)
 |-- _c5: double (nullable = true)
 |-- _c6: double (nullable = true)
 |-- _c7: integer (nullable = true)

>>> no_header.show(3)
+---+---+---+--------+---+----+----+----+
|_c0|_c1|_c2|     _c3|_c4| _c5| _c6| _c7|
+---+---+---+--------+---+----+----+----+
|t01| d1| r1|San Jose|  8| 5.2|14.5|   5|
|t02| d2| r2|San Jose|  9|12.0|30.0|   4|
|t03| d1| r3| Oakland|  9| 3.1|9.75|NULL|
+---+---+---+--------+---+----+----+----+
only showing top 3 rows

>>> names = ["trip_id", "driver_id", "rider", "city", "hour", "distance_km", "fare", "rating"]
>>> no_header.toDF(*names).dtypes
[('trip_id', 'string'), ('driver_id', 'string'), ('rider', 'string'), ('city', 'string'), ('hour', 'int'), ('distance_km', 'double'), ('fare', 'double'), ('rating', 'int')]

>>> with_schema = spark.read.csv(
...     "data/trips_no_header.csv",
...     schema="trip_id STRING, driver_id STRING, rider STRING, city STRING, "
...            "hour INT, distance_km DOUBLE, fare DOUBLE, rating INT")
>>> with_schema.count()
20
>>> sorted(with_schema.collect()) == sorted(trips.collect())
True

>>> wrong = spark.read.csv("data/trips_no_header.csv", header=True, inferSchema=True)
>>> wrong.columns
['t01', 'd1', 'r1', 'San Jose', '8', '5.2', '14.50', '5']
>>> wrong.count()
19
```

**Notes:**

**Part A, a file with a header:**

* **No options:** Spark doesn't know the first line is a header. It names the columns
  `_c0`, `_c1`, ..., treats the header as a data row (21 rows), and reads **everything
  as strings**.
* **`header=True`:** the first line gives the column names (20 rows), but every column
  is still a `string`. You can't add up a string column of fares correctly.
* **`inferSchema=True`:** Spark reads the data an **extra time** to guess each column's
  type: `integer`, `double`, or `string`. The empty ratings became `NULL`.
* Compare with the RDD version ([RDD Set 10](../rdd_practice_questions/ANSWERS_10.md)),
  where we wrote a `parse()` function by hand to split the lines, convert the numbers,
  and turn an empty rating into `None`. The CSV reader does all of that for us.
* `filter("rating IS NULL")` uses a SQL condition as a string. [Set 03](QUESTIONS_03.md)
  covers filtering in detail.

**Part B, a file without a header:**

* By default, `header` is `False`, which is **right** for this file: all 20 lines are
  data. But the columns get the made-up names `_c0` ... `_c7`. (With `inferSchema=True`,
  the types are still guessed correctly.)
* There are two ways to give the columns real names:
  * **`toDF(*names)`** renames all the columns at once, in order. The `*` unpacks the list
    into separate arguments;
  * an **explicit schema** gives names **and** types in one step, with no extra pass to
    guess the types. This is the best way for a file without a header. The result is
    exactly the same data as `trips` from Part A.
* **The opposite mistake:** `header=True` on a file **without** a header turns the
  **first trip** into the column names (`t01`, `d1`, ...), and that trip is lost from the
  data: 19 rows instead of 20. Spark can't tell a header from a data line, so **you** have
  to know which kind of file you have. Look at its first lines before reading it.

  | File | Read with | Result |
  |---|---|---|
  | with a header | `header=True` | ✅ the names come from the header |
  | with a header | no `header` | ❌ the header becomes a data row, and all the columns are strings |
  | without a header | `schema=...` (or `toDF(*names)`) | ✅ you supply the names |
  | without a header | `header=True` | ❌ the first data row becomes the header, and is lost |

---

## Q5 (Intermediate) — A schema for the CSV file, and summary statistics

```python
>>> trips_ddl = ("trip_id STRING, driver_id STRING, rider STRING, city STRING, "
...              "hour INT, distance_km DOUBLE, fare DOUBLE, rating INT")
>>> trips = spark.read.csv("data/trips.csv", header=True, schema=trips_ddl)
>>> trips.dtypes
[('trip_id', 'string'), ('driver_id', 'string'), ('rider', 'string'), ('city', 'string'), ('hour', 'int'), ('distance_km', 'double'), ('fare', 'double'), ('rating', 'int')]

>>> trips.describe("distance_km", "fare", "rating").show()
+-------+-----------------+------------------+------------------+
|summary|      distance_km|              fare|            rating|
+-------+-----------------+------------------+------------------+
|  count|               20|                20|                17|
|   mean|             8.02|           23.8125|4.0588235294117645|
| stddev|4.830342692430403|22.112879522791093|1.1974237050483696|
|    min|              2.2|               7.5|                 1|
|    max|             20.5|             110.0|                 5|
+-------+-----------------+------------------+------------------+

>>> (trips.summary("count", "min", "25%", "50%", "75%", "max")
...       .select("summary", "distance_km", "fare")
...       .show())
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

**Notes:**

* **An explicit schema is the best choice for real data.** It's faster (no extra pass
  over the file to guess types), and **safer**: the types don't change when the data
  changes. With `inferSchema=True`, a column of whole numbers becomes an `integer`
  today, and a `double` tomorrow, when one value has a decimal point.
* The schema is the same DDL string syntax as in Q2. The column names come from the
  schema, and `header=True` just tells Spark to skip the first line.
* **`describe()`** gives count, mean, standard deviation, min, and max. Note that the
  `rating` count is **17**: `NULL`s are skipped, like the 3 unrated trips.
* **`summary()`** can also give **percentiles** (`25%`, `50%` = the median, `75%`).
  Spark computes these **approximately**, which is fast even on huge tables.
* The mean fare (23.81) is much larger than the median (17.50), and the max is 110.
  That's a sign of an **outlier**, the suspicious trip from RDD Set 10, Q6.

---

## Q6 (Intermediate) — Nested JSON data

```python
>>> riders = spark.read.json("data/riders.json")
>>> riders.printSchema()
root
 |-- home: struct (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- zip: string (nullable = true)
 |-- member_since: long (nullable = true)
 |-- name: string (nullable = true)
 |-- rider: string (nullable = true)
 |-- tags: array (nullable = true)
 |    |-- element: string (containsNull = true)

>>> riders.show(truncate=False)
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

>>> riders.select("rider", "home.city", "home.zip").show()
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

**Notes:**

* `spark.read.json()` reads **JSON Lines**: one JSON object per line. Spark infers the
  schema by reading the data, including **nested** types:
  * `home` is a **`struct`**, a column with named fields inside;
  * `tags` is an **`array`** of strings.
* The JSON reader puts the columns in **alphabetical** order (`home`, `member_since`,
  `name`, ...), not in the order of the file. Use `select()` to choose an order.
* `"home.city"` reaches **into** a struct. The result column is just called `city`.
* `show(truncate=False)` prints full values. By default, `show()` cuts every value
  to 20 characters.
* **`[]` vs. `NULL`:** Lena's `tags` is an **empty list** (the key is in the file with
  no tags), while Tom's is `NULL` (the key is **missing** from his line). They mean
  different things, and they behave differently in queries. Q9 counts the nulls.
* Integers in JSON become `long`, as with Python ints in Q1.

---

## Q7 (Intermediate+) — From RDD to DataFrame and back

```python
>>> rdd = (sc.textFile("../rdd_practice_questions/data/trips.txt", 2)
...          .map(lambda line: line.split(","))
...          .map(lambda f: (f[0], f[1], f[3], float(f[6]))))
>>> from_rdd = rdd.toDF(["trip_id", "driver_id", "city", "fare"])
>>> from_rdd.printSchema()
root
 |-- trip_id: string (nullable = true)
 |-- driver_id: string (nullable = true)
 |-- city: string (nullable = true)
 |-- fare: double (nullable = true)

>>> from_rdd.show(3)
+-------+---------+--------+----+
|trip_id|driver_id|    city|fare|
+-------+---------+--------+----+
|    t01|       d1|San Jose|14.5|
|    t02|       d2|San Jose|30.0|
|    t03|       d1| Oakland|9.75|
+-------+---------+--------+----+
only showing top 3 rows
>>> spark.createDataFrame(rdd, "trip_id STRING, driver_id STRING, city STRING, fare DOUBLE").dtypes
[('trip_id', 'string'), ('driver_id', 'string'), ('city', 'string'), ('fare', 'double')]

>>> back = trips.rdd
>>> type(back).__name__
'RDD'
>>> back.first()
Row(trip_id='t01', driver_id='d1', rider='r1', city='San Jose', hour=8, distance_km=5.2, fare=14.5, rating=5)
>>> back.map(lambda r: (r.city, r.fare)).reduceByKey(lambda a, b: a + b).sortByKey().collect()
[('Oakland', 168.25), ('San Jose', 308.0)]
```

**Notes:**

* **RDD → DataFrame:** `rdd.toDF(column_names)` guesses the types from the data, and
  `spark.createDataFrame(rdd, schema)` uses the schema you give. Both need an RDD of
  **tuples** (or `Row`s): one tuple per row, one item per column.
* **DataFrame → RDD:** `df.rdd` is an RDD of **`Row`** objects, so all the RDD
  operations from the [RDD sets](../rdd_practice_questions/README.md) work on it. Here
  `reduceByKey()` gives the same fare totals per city as
  [RDD Set 10, Q1](../rdd_practice_questions/ANSWERS_10.md#q1-basic--trips-fares-and-distances-per-city).
* The input path `../rdd_practice_questions/data/trips.txt` is the RDD sets' file without a
  header, read from this folder.
* **When to go back to RDDs:** rarely. The DataFrame API covers almost everything, and
  it's usually much faster, because Spark understands the columns and can optimize the
  whole query. Converting to an RDD gives up that optimization, so use it only for
  logic that really needs plain Python on each row.

---

## Q8 (Intermediate+) — Bad rows in a CSV file

```python
>>> permissive = spark.read.csv(
...     "data/trips_raw.csv", header=True,
...     schema=trips_ddl + ", _corrupt_record STRING",
...     columnNameOfCorruptRecord="_corrupt_record").cache()
>>> permissive.count()
22
>>> (permissive.filter("_corrupt_record IS NOT NULL")
...            .select("trip_id", "hour", "fare", "_corrupt_record")
...            .show(truncate=False))
+-------+----+----+------------------------------------+
|trip_id|hour|fare|_corrupt_record                     |
+-------+----+----+------------------------------------+
|t21    |NULL|12.0|t21,d2,r2,Oakland,eleven,4.0,12.00,3|
|t22    |14  |NULL|t22,d1,r1,San Jose,14,6.1           |
+-------+----+----+------------------------------------+

>>> dropped = spark.read.csv("data/trips_raw.csv", header=True, schema=trips_ddl,
...                          mode="DROPMALFORMED")
>>> dropped.count()
22
>>> len(dropped.collect())
20
>>> dropped.cache().count()
20

>>> strict = spark.read.csv("data/trips_raw.csv", header=True, schema=trips_ddl,
...                         mode="FAILFAST")
>>> strict.count()
22
>>> try:
...     strict.collect()
... except Exception as e:
...     print("Malformed records are detected" in str(e))
...
True
```

**Notes:**

* The raw file has 22 trips: the 20 good ones, `t21` (the hour is `eleven`, which isn't
  an `INT`), and `t22` (only 6 fields instead of 8).
* **The three read modes:**

  | Mode | What happens to a bad row |
  |---|---|
  | `PERMISSIVE` (the default) | **kept**: the bad fields become `NULL`, and the whole raw line can be saved in a "corrupt record" column |
  | `DROPMALFORMED` | **dropped** |
  | `FAILFAST` | the job **fails** with an error |

* **The corrupt-record column** must be in the schema (as a `STRING`), and it holds the
  original line for each bad row, which is perfect for a "bad rows" report. Note that
  `t22`'s missing fields became `NULL` (`fare`, `rating`), while its good fields
  (`hour` = 14) were still read.
* **The `count()` trap:** `dropped.count()` says **22**, and `strict.count()` doesn't fail
  at all! A `count()` doesn't need any **column values**, so Spark doesn't parse the
  fields, and never notices the bad rows. Only when the rows are really parsed (by
  `collect()`, or by a `cache()` that stores every column) do you get 20 rows, or the
  `FAILFAST` error.
* **The lesson:** don't check a read mode with a bare `count()`. Count the rows of a
  `cache()`d DataFrame, or count the `_corrupt_record` rows in `PERMISSIVE` mode, which
  is the most useful mode in practice: nothing is lost, and the bad rows are easy to find.
* The reason is CSV **column pruning**: Spark parses only the columns a query needs.
  It's controlled by `spark.sql.csv.parser.columnPruning.enabled`, which is `true` by
  default.

---

## Q9 (Intermediate+) — Profiling a DataFrame

```python
>>> [f.name for f in trips.schema.fields if f.dataType.simpleString() in ("int", "double")]
['hour', 'distance_km', 'fare', 'rating']
>>> [(f.name, f.dataType.simpleString(), f.nullable) for f in trips.schema.fields][:3]
[('trip_id', 'string', True), ('driver_id', 'string', True), ('rider', 'string', True)]

>>> from pyspark.sql import functions as F
>>> trips.select([F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in trips.columns]).show()
+-------+---------+-----+----+----+-----------+----+------+
|trip_id|driver_id|rider|city|hour|distance_km|fare|rating|
+-------+---------+-----+----+----+-----------+----+------+
|      0|        0|    0|   0|   0|          0|   0|     3|
+-------+---------+-----+----+----+-----------+----+------+

>>> trips.select([F.countDistinct(c).alias(c) for c in trips.columns]).show()
+-------+---------+-----+----+----+-----------+----+------+
|trip_id|driver_id|rider|city|hour|distance_km|fare|rating|
+-------+---------+-----+----+----+-----------+----+------+
|     20|        4|    7|   2|  10|         20|  20|     5|
+-------+---------+-----+----+----+-----------+----+------+

>>> trips.isEmpty(), trips.filter("fare > 1000").isEmpty()
(False, True)
```

**Notes:**

* **`df.schema`** is a `StructType`: a list of `StructField`s, each with a `name`, a
  `dataType`, and `nullable`. Because it's a normal Python object, code can use it, for
  example to find all the **numeric** columns of any DataFrame.
* **A profile in one pass:** a list comprehension builds **one expression per column**, and
  one `select()` computes all of them together. This works for a table with 8 columns or 800.
  * `F.count(F.when(F.col(c).isNull(), 1))` counts the `NULL`s: `when()` gives `1` for a
    null and `NULL` otherwise, and `count()` skips `NULL`s;
  * `F.countDistinct(c)` counts the different values.
* `pyspark.sql.functions` (usually imported as `F`) holds Spark's hundreds of built-in
  column functions. [Set 02](QUESTIONS_02.md) starts using it properly.
* What the profile says: `trip_id` is unique (20 different values in 20 rows), so it's a
  good **key**; `rating` is the only column with nulls; and there are 4 drivers, 7 riders,
  and 2 cities.
* `isEmpty()` is the cheap way to ask "are there any rows?". It stops at the first row,
  while `count() == 0` counts them all.
