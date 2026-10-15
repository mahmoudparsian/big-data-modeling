# PySpark DataFrame Practice Questions — Set 09: Reading, Writing, and Performance

Nine practice questions, three at each level, meant to be solved **interactively** in
the `bin/pyspark` shell. This set is about **storing** DataFrames (file formats, save modes,
partitioned folders, the number of files) and about **reading Spark's plans** to see why a
query is fast or slow (pushdown, pruning, caching, adaptive execution, join strategies).

| # | Level | Topic | Input |
|---|---|---|---|
| [Q1](#q1-basic--writing-and-reading-back-a-csv-file) | Basic | Writing and reading back a CSV file | [`trips.csv`](data/trips.csv) |
| [Q2](#q2-basic--parquet-and-json) | Basic | Parquet and JSON | [`trips.csv`](data/trips.csv), `spark.range()` |
| [Q3](#q3-basic--save-modes) | Basic | Save modes | [`trips.csv`](data/trips.csv) |
| [Q4](#q4-intermediate--partitioned-output-and-partition-pruning) | Intermediate | Partitioned output, and partition pruning | [`trips.csv`](data/trips.csv) |
| [Q5](#q5-intermediate--how-many-output-files) | Intermediate | How many output files? | [`trips.csv`](data/trips.csv) |
| [Q6](#q6-intermediate--reading-the-plan-pushdown-and-pruning) | Intermediate | Reading the plan: pushdown and pruning | [`trips.csv`](data/trips.csv) |
| [Q7](#q7-intermediate--caching-dataframes) | Intermediate+ | Caching DataFrames | [`trips.csv`](data/trips.csv) |
| [Q8](#q8-intermediate--adaptive-query-execution-and-join-strategies) | Intermediate+ | Adaptive query execution and join strategies | [`trips.csv`](data/trips.csv), [`drivers.csv`](data/drivers.csv) |
| [Q9](#q9-intermediate--an-end-to-end-etl-job) | Intermediate+ | An end-to-end ETL job | [`payments_raw.csv`](data/payments_raw.csv), [`trips.csv`](data/trips.csv) |

The operations for this set:

* **Writing:** `df.write.csv()`, `.json()`, `.parquet()`, with `.mode(...)`, `.partitionBy(...)`,
  and options like `header=True`
* **Reading:** `spark.read.csv()`, `.json()`, `.parquet()`, and the `basePath` option
* **Files and partitions:** `repartition()`, `coalesce()`, `df.rdd.getNumPartitions()`
* **Performance:** `explain()`, `cache()`, `unpersist()`, `storageLevel`, `spark.conf.get()` and
  `spark.conf.set()`

You may also use everything from Sets [01](QUESTIONS_01.md)–[08](QUESTIONS_08.md).

Try each question on your own before you open the [answers](ANSWERS_09.md).

## Files

| File | Purpose |
|---|---|
| [QUESTIONS_09.md](QUESTIONS_09.md) | The nine questions (this page) |
| [ANSWERS_09.md](ANSWERS_09.md) | Step-by-step solutions, with real output from Spark 4.2.0 |
| [data/trips.csv](data/trips.csv), [data/drivers.csv](data/drivers.csv) | The trips and drivers |
| [data/payments_raw.csv](data/payments_raw.csv) | The messy payments of Set 02, Q9 |
| `output/` | Created by **your** answers; it's listed in [`.gitignore`](.gitignore), so it never gets into git |

## Getting started

Start `$SPARK_HOME/bin/pyspark` **from this folder**, as in
[Set 01](QUESTIONS_01.md#getting-started), do the questions **in order**, and start with this
setup. It also defines three small helpers:

```python
import io, contextlib, os, glob, re, shutil
from pyspark.sql import functions as F

trips_ddl = ("trip_id STRING, driver_id STRING, rider STRING, city STRING, "
             "hour INT, distance_km DOUBLE, fare DOUBLE, rating INT")
trips = spark.read.csv("data/trips.csv", header=True, schema=trips_ddl)

def plan_of(df):                 # what explain() prints, as a string (from Set 05, Q5)
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain()
    return buffer.getvalue()

def parts(path):                 # the number of data files Spark wrote into a folder
    return len(glob.glob(path + "/part-*"))

def size(path):                  # their total size, in bytes
    return sum(os.path.getsize(f) for f in glob.glob(path + "/part-*"))

shutil.rmtree("output", ignore_errors=True)    # start with an empty output folder
```

Spark writes **folders**, not single files, so after each write, look inside with your file
browser or `ls -R output/`.

---

## Q1 (Basic) — Writing and reading back a CSV file

**Tasks:**

1. Write `trips` as CSV, with a header, to `output/trips_csv`. Is there a `_SUCCESS` file in the
   folder? How many `part-` files?
2. Read the folder back (with a header and `trips_ddl`). Count the rows, and check that the data
   is the same as `trips`.
3. Write `trips` to the same path again. What happens? Print the error's type and the first 21
   characters of its message.

**Expected output:**

```
(True, 1)
(20, True)
AnalysisException [PATH_ALREADY_EXISTS]
```

---

## Q2 (Basic) — Parquet and JSON

**Tasks:**

1. Write `trips` as **Parquet** to `output/trips_parquet`. Read it back **without** any schema.
   Are the `dtypes` the same as `trips`'s?
2. Write `trips` as **JSON** to `output/trips_json`. Print the `dtypes` of the data read back,
   and the first line of its `part-` file.
3. Compare the sizes of the CSV, Parquet, and JSON outputs.
4. Make a bigger DataFrame:

   ```python
   big = spark.range(1_000_000, numPartitions=4).select(
       (F.col("id") % 100).alias("a"), (F.col("id") % 7).cast("string").alias("b"))
   ```

   Write it as CSV (`output/big_csv`) and as Parquet (`output/big_parquet`), and compare the
   sizes. What changed?

**Expected output:**

```
True
[('city', 'string'), ('distance_km', 'double'), ('driver_id', 'string'), ('fare', 'double'), ('hour', 'bigint'), ('rating', 'bigint'), ('rider', 'string'), ('trip_id', 'string')]
{"trip_id":"t01","driver_id":"d1","rider":"r1","city":"San Jose","hour":8,"distance_km":5.2,"fare":14.5,"rating":5}
(711, 2752, 2303)
(4900000, 89140)
```

(Your Parquet sizes may differ a little with other versions.)

---

## Q3 (Basic) — Save modes

**Tasks:** for `output/trips_parquet` from Q2:

1. Write `trips` there again with mode `"append"`, and count the rows read back.
2. Write `trips` there with mode `"overwrite"`, and count again.
3. Write just **one** row (`trips.limit(1)`) there with mode `"ignore"`, and count again.

**Expected output:**

```
40
20
20
```

*Think about it:* what would happen if a daily job using `"append"` ran twice by mistake?

---

## Q4 (Intermediate) — Partitioned output, and partition pruning

**Tasks:**

1. Write `trips` as Parquet to `output/by_city`, with `partitionBy("city")` (and mode
   `"overwrite"`). List the sub-folders whose name starts with `city=`.
2. Read `output/by_city` back as `by_city`. What is its **last** column? Count the rows.
3. Filter `by_city` on `city == "Oakland"`. Check that its plan has a `PartitionFilters` entry,
   and that `Oakland` appears in it.
4. Read the sub-folder `output/by_city/city=Oakland` directly, and print its columns. Then read
   it again with the option `basePath` set to `output/by_city`, and print its **last** column.

**Expected output:**

```
['city=Oakland', 'city=San Jose']
('city', 20)
(True, True)
['trip_id', 'driver_id', 'rider', 'hour', 'distance_km', 'fare', 'rating']
'city'
```

**Hint for task 3:** `plan.split("PartitionFilters")[1].split("]")[0]` is the text of the
partition filters.

---

## Q5 (Intermediate) — How many output files?

**Tasks:**

1. How many partitions do `trips` and `trips.repartition(4)` have?
2. Write `trips.repartition(4)` as Parquet to `output/four`, and count the `part-` files. Then
   write `trips.coalesce(1)` as CSV (with a header) to `output/one`, and count them.
3. Write `trips.repartition(3)`, partitioned by city, to `output/by_city_3`, and show the number of
   `part-` files in **each** city folder, as a dict.
4. Do the same with `trips.repartition("city")` instead, to `output/by_city_1`. What changed, and why?

**Expected output:**

```
(1, 4)
4
1
{'city=Oakland': 2, 'city=San Jose': 3}
{'city=Oakland': 1, 'city=San Jose': 1}
```

---

## Q6 (Intermediate) — Reading the plan: pushdown and pruning

**Tasks:**

1. Read `output/trips_parquet`, keep the trips with a fare **over 30**, and select only `trip_id`
   and `fare`, as `query`. In its plan, find (with `re.findall()`) the `PushedFilters: [...]` and
   the `ReadSchema: ...` entries. What do they mean?
2. Write the same query on the **CSV** file `data/trips.csv` (with `trips_ddl`), and find its
   `ReadSchema`. If both plans look alike, why is Parquet still much faster on big data?
3. Count the rows of `query`.

**Expected output:**

```
['PushedFilters: [IsNotNull(fare), GreaterThan(fare,30.0)]']
['ReadSchema: struct<trip_id:string,fare:double>']
['ReadSchema: struct<trip_id:string,fare:double>']
3
```

**Hint:** `re.findall(r"PushedFilters: \[[^\]]*\]", plan)` and `re.findall(r"ReadSchema: [^\n]*", plan)`.

---

## Q7 (Intermediate+) — Caching DataFrames

**Tasks:**

1. Compute the total fare per city as `revenue`, and **cache** it. Print `is_cached` and the
   storage level. How is it different from the RDD default of
   [RDD Set 08, Q3](../rdd_practice_questions/QUESTIONS_08.md#q3-basic--cache-compute-once-use-many-times)?
2. Check that the cached DataFrame's plan reads from an `InMemoryTableScan`.
3. Collect it, sorted by city.
4. `unpersist()` it, and print `is_cached` again.

**Expected output:**

```
True
Disk Memory Deserialized 1x Replicated
True
[Row(city='Oakland', revenue=168.25), Row(city='San Jose', revenue=308.0)]
False
```

---

## Q8 (Intermediate+) — Adaptive query execution and join strategies

**Tasks:**

1. Print the settings `spark.sql.adaptive.enabled` and `spark.sql.shuffle.partitions`.
2. Count the trips per city, as `counts`. Check that its plan contains
   `"AdaptiveSparkPlan isFinalPlan=false"`. How many partitions does the result have?
3. Turn **off** `spark.sql.adaptive.enabled`, and count the partitions of the same query again.
   Then turn it back **on**.
4. Read `data/drivers.csv` (with a header) as `drivers`. Does the plan of `trips.join(drivers,
   "driver_id")` use a `BroadcastHashJoin`?

**Expected output:**

```
('true', '200')
True
1
200
True
```

*Think about it:* what are the 200 partitions in task 3, and why does adaptive query execution
reduce them to 1?

---

## Q9 (Intermediate+) — An end-to-end ETL job

**Goal:** turn the messy payments file into a clean, **partitioned Parquet table**, and query it.

**Tasks:**

1. **Extract and transform:** read `data/payments_raw.csv` (with a header), and clean it as in
   [Set 02, Q9](QUESTIONS_02.md#q9-intermediate--cleaning-a-messy-payments-file): `payment_id`,
   `trip_id`, `paid_at` (a timestamp, from either format), `method` (lowercase, trimmed), and
   `amount` (a `double`, or `NULL`). Join it with `trips` (`trip_id`, `city`, `fare`), and add
   `tip` (amount − fare, rounded to 2 decimals) and `day` (the date of `paid_at`), as `paid_trips`.
2. **Load:** write `paid_trips` as Parquet to `output/paid_trips`, partitioned by `city` and
   `day`, with **one file per folder**. List the `day=` folders inside `city=Oakland`.
3. Read the table back as `lake`, and print its schema.
4. Show `trip_id`, `method`, `amount`, and `tip` of the **San Jose** payments of **October 10**.
   Check that both values appear in the plan's `PartitionFilters`.
5. Check that `lake` has the same `(trip_id, amount)` rows as `paid_trips`. Then delete the
   `output` folder.

**Expected output:**

```
['day=2026-10-05', 'day=2026-10-06', 'day=2026-10-07', 'day=2026-10-08', 'day=2026-10-09']
root
 |-- trip_id: string (nullable = true)
 |-- payment_id: string (nullable = true)
 |-- paid_at: timestamp (nullable = true)
 |-- method: string (nullable = true)
 |-- amount: double (nullable = true)
 |-- fare: double (nullable = true)
 |-- tip: double (nullable = true)
 |-- city: string (nullable = true)
 |-- day: date (nullable = true)

+-------+------+------+----+
|trip_id|method|amount| tip|
+-------+------+------+----+
|    t06|  card|   8.8| 0.8|
|    t13|  cash|  NULL|NULL|
|    t20|   app|  39.6| 3.6|
+-------+------+------+----+

(True, True)
True
False
```

(The last `False` is `os.path.exists("output")` after deleting it.)
