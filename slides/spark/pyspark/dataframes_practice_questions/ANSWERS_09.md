# PySpark DataFrame Practice Questions — Set 09: Answers

Solutions to the nine [Set 09 practice questions](QUESTIONS_09.md). Every session below
was run in the `bin/pyspark` shell (Spark 4.2.0), started from this folder. The output is
copied from those real runs.

Try the questions yourself first. There are often several correct solutions, and yours
doesn't need to match these exactly.

* [Q1 (Basic): Writing and reading back a CSV file](#q1-basic--writing-and-reading-back-a-csv-file)
* [Q2 (Basic): Parquet and JSON](#q2-basic--parquet-and-json)
* [Q3 (Basic): Save modes](#q3-basic--save-modes)
* [Q4 (Intermediate): Partitioned output, and partition pruning](#q4-intermediate--partitioned-output-and-partition-pruning)
* [Q5 (Intermediate): How many output files?](#q5-intermediate--how-many-output-files)
* [Q6 (Intermediate): Reading the plan: pushdown and pruning](#q6-intermediate--reading-the-plan-pushdown-and-pruning)
* [Q7 (Intermediate+): Caching DataFrames](#q7-intermediate--caching-dataframes)
* [Q8 (Intermediate+): Adaptive query execution and join strategies](#q8-intermediate--adaptive-query-execution-and-join-strategies)
* [Q9 (Intermediate+): An end-to-end ETL job](#q9-intermediate--an-end-to-end-etl-job)

Setup, used by all the questions:

```python
>>> import io, contextlib, os, glob, re, shutil
>>> from pyspark.sql import functions as F
>>> trips_ddl = ("trip_id STRING, driver_id STRING, rider STRING, city STRING, "
...              "hour INT, distance_km DOUBLE, fare DOUBLE, rating INT")
>>> trips = spark.read.csv("data/trips.csv", header=True, schema=trips_ddl)
>>> def plan_of(df):
...     buffer = io.StringIO()
...     with contextlib.redirect_stdout(buffer):
...         df.explain()
...     return buffer.getvalue()
...
>>> def parts(path):
...     return len(glob.glob(path + "/part-*"))
...
>>> def size(path):
...     return sum(os.path.getsize(f) for f in glob.glob(path + "/part-*"))
...
>>> shutil.rmtree("output", ignore_errors=True)
```

Everything is written under the folder `output/`, which is listed in this folder's
`.gitignore`, so your files never end up in git. The setup deletes it first, so that each
session starts clean.

---

## Q1 (Basic) — Writing and reading back a CSV file

```python
>>> trips.write.csv("output/trips_csv", header=True)
>>> "_SUCCESS" in os.listdir("output/trips_csv"), parts("output/trips_csv")
(True, 1)

>>> back = spark.read.csv("output/trips_csv", header=True, schema=trips_ddl)
>>> back.count(), sorted(back.collect()) == sorted(trips.collect())
(20, True)

>>> try:
...     trips.write.csv("output/trips_csv", header=True)
... except Exception as e:
...     print(type(e).__name__, str(e)[:21])
...
AnalysisException [PATH_ALREADY_EXISTS]
```

**Notes:**

* **`df.write.csv(path)`** writes a **folder**, not a single file. Each partition of the
  DataFrame becomes one `part-...csv` file inside it (here 1, because the data is small). An
  empty `_SUCCESS` file marks that the whole job finished. If it's missing, the output is
  incomplete. The `.crc` files are checksums.
* **Reading a folder** reads all its `part-` files together, so the data comes back exactly as
  it was, as long as you give the same schema (Set 01, Q5).
* **By default, Spark refuses to overwrite** an existing path (`PATH_ALREADY_EXISTS`), which
  protects you from losing data by accident. Q3 shows the other save modes.
* `trips.write` is a **`DataFrameWriter`**. Writing is an **action**: it runs the whole query
  right away.

---

## Q2 (Basic) — Parquet and JSON

```python
>>> trips.write.parquet("output/trips_parquet")
>>> spark.read.parquet("output/trips_parquet").dtypes == trips.dtypes
True

>>> trips.write.json("output/trips_json")
>>> spark.read.json("output/trips_json").dtypes
[('city', 'string'), ('distance_km', 'double'), ('driver_id', 'string'), ('fare', 'double'), ('hour', 'bigint'), ('rating', 'bigint'), ('rider', 'string'), ('trip_id', 'string')]
>>> print(open(glob.glob("output/trips_json/part-*.json")[0]).readline().strip())
{"trip_id":"t01","driver_id":"d1","rider":"r1","city":"San Jose","hour":8,"distance_km":5.2,"fare":14.5,"rating":5}

>>> size("output/trips_csv"), size("output/trips_parquet"), size("output/trips_json")
(711, 2752, 2303)

>>> big = spark.range(1_000_000, numPartitions=4).select(
...     (F.col("id") % 100).alias("a"), (F.col("id") % 7).cast("string").alias("b"))
>>> big.write.csv("output/big_csv")
>>> big.write.parquet("output/big_parquet")
>>> size("output/big_csv"), size("output/big_parquet")
(4900000, 89140)
```

**Notes:**

* **Parquet** is the standard file format for big data:
  * it **stores the schema** inside the files, so the types come back exactly as written, with
    no `inferSchema` and no DDL string needed;
  * it's **columnar**: the values of each column are stored together, so Spark can read only the
    columns a query needs (Q6), and similar values compress very well;
  * it stores **statistics** (min and max per column, per block), so Spark can skip blocks that
    can't match a filter.
* **JSON** writes one object per line (JSON Lines), and the reader guesses the types again: the
  `int` columns come back as `bigint`, and the columns are in alphabetical order (Set 01, Q6).
* **Size:** for 20 rows, Parquet is **bigger** than CSV (2,752 vs. 711 bytes), because its
  metadata costs more than the data. For **1 million** rows, Parquet is about **55× smaller**
  (89 KB vs. 4.9 MB), because each column compresses well. Columnar formats shine on real data volumes.
  (The exact Parquet sizes can differ a little with other Spark or Parquet versions.)
* **Use CSV and JSON to exchange data** with other tools and people, and **Parquet** (or table
  formats built on it, like Delta Lake and Apache Iceberg) **inside** your data pipelines.

---

## Q3 (Basic) — Save modes

```python
>>> trips.write.mode("append").parquet("output/trips_parquet")
>>> spark.read.parquet("output/trips_parquet").count()
40
>>> trips.write.mode("overwrite").parquet("output/trips_parquet")
>>> spark.read.parquet("output/trips_parquet").count()
20
>>> trips.limit(1).write.mode("ignore").parquet("output/trips_parquet")
>>> spark.read.parquet("output/trips_parquet").count()
20
```

**Notes:**

* **The four save modes**, for when the path already exists:

  | `mode(...)` | What happens |
  |---|---|
  | `"error"` / `"errorifexists"` (the default) | the write **fails** (Q1) |
  | `"append"` | the new files are **added** next to the old ones |
  | `"overwrite"` | the old data is **deleted**, then the new data is written |
  | `"ignore"` | **nothing** is written; the old data stays |

* **`append` can duplicate data:** running the same job twice doubles the rows (40). Real
  pipelines either overwrite, or append only new data (for example, a new date partition, Q9).
* **`overwrite` deletes first**, so if the job fails halfway, the old data is already gone.
  Table formats like Delta Lake make overwrites safe (atomic).

---

## Q4 (Intermediate) — Partitioned output, and partition pruning

```python
>>> trips.write.mode("overwrite").partitionBy("city").parquet("output/by_city")
>>> sorted(d for d in os.listdir("output/by_city") if d.startswith("city="))
['city=Oakland', 'city=San Jose']

>>> by_city = spark.read.parquet("output/by_city")
>>> by_city.columns[-1], by_city.count()
('city', 20)

>>> oakland = by_city.filter(F.col("city") == "Oakland")
>>> plan = plan_of(oakland)
>>> "PartitionFilters" in plan, "Oakland" in plan.split("PartitionFilters")[1].split("]")[0]
(True, True)

>>> spark.read.parquet("output/by_city/city=Oakland").columns
['trip_id', 'driver_id', 'rider', 'hour', 'distance_km', 'fare', 'rating']
>>> spark.read.option("basePath", "output/by_city").parquet("output/by_city/city=Oakland").columns[-1]
'city'
```

**Notes:**

* **`partitionBy("city")`** writes one **sub-folder per value**: `city=Oakland/` and
  `city=San Jose/`. The `city` column is **not** stored inside the files; its value is in the
  folder name.
* **Reading the top folder** brings `city` back as a column (at the end), rebuilt from the folder
  names.
* **Partition pruning:** a filter on the partition column becomes a **`PartitionFilters`**
  entry in the plan, and Spark reads **only** the `city=Oakland` folder. The San Jose files
  aren't even opened. For data partitioned by date, "yesterday's data" reads one folder out of
  thousands.
* Reading a sub-folder directly loses the `city` column, unless you tell Spark where the table
  starts with the `basePath` option.
* **Choose the partition column carefully:** a column you **often filter on**, with **not too
  many** values (dates, countries, cities). Partitioning by a unique id would make one tiny
  folder per row: the "small files" problem.
* This is Spark's file-level version of the RDD `partitionBy()` from
  [RDD Set 08](../rdd_practice_questions/QUESTIONS_08.md): related data stored together, so
  that work can skip what it doesn't need.

---

## Q5 (Intermediate) — How many output files?

```python
>>> trips.rdd.getNumPartitions(), trips.repartition(4).rdd.getNumPartitions()
(1, 4)
>>> trips.repartition(4).write.mode("overwrite").parquet("output/four")
>>> parts("output/four")
4
>>> trips.coalesce(1).write.mode("overwrite").csv("output/one", header=True)
>>> parts("output/one")
1

>>> trips.repartition(3).write.mode("overwrite").partitionBy("city").parquet("output/by_city_3")
>>> {d: parts("output/by_city_3/" + d) for d in sorted(os.listdir("output/by_city_3")) if d.startswith("city=")}
{'city=Oakland': 2, 'city=San Jose': 3}
>>> trips.repartition("city").write.mode("overwrite").partitionBy("city").parquet("output/by_city_1")
>>> {d: parts("output/by_city_1/" + d) for d in sorted(os.listdir("output/by_city_1")) if d.startswith("city=")}
{'city=Oakland': 1, 'city=San Jose': 1}
```

**Notes:**

* **One output file per partition** of the DataFrame (and, with `partitionBy()`, per partition
  **per folder**). So you control the number of files by controlling the number of partitions:
  * **`repartition(n)`** reshuffles into `n` balanced partitions, so `n` files;
  * **`coalesce(n)`** merges partitions without a shuffle, which is the cheap way to get **fewer**
    files. `coalesce(1)` gives a single file, handy for a small report, but then **one** task
    writes everything, so don't do it for big data.
  * These are the DataFrame versions of [RDD Set 08, Q1](../rdd_practice_questions/ANSWERS_08.md#q1-basic--coalesce-and-repartition).
* **With `partitionBy()`,** each of the 3 partitions can hold rows of **both** cities, so each one
  writes a file into **each** city folder (up to 3 × 2 files). **`repartition("city")`** first puts
  each city's rows into **one** partition, so each folder gets exactly **one** file. That's the
  usual way to avoid many small files when writing partitioned data.
* **Aim for files of roughly 100 MB to 1 GB.** Thousands of tiny files make reading slow (each
  file has a fixed cost to open), and a few huge files limit parallelism.

---

## Q6 (Intermediate) — Reading the plan: pushdown and pruning

```python
>>> query = spark.read.parquet("output/trips_parquet").filter(F.col("fare") > 30).select("trip_id", "fare")
>>> plan = plan_of(query)
>>> re.findall(r"PushedFilters: \[[^\]]*\]", plan)
['PushedFilters: [IsNotNull(fare), GreaterThan(fare,30.0)]']
>>> re.findall(r"ReadSchema: [^\n]*", plan)
['ReadSchema: struct<trip_id:string,fare:double>']

>>> csv_query = spark.read.csv("data/trips.csv", header=True, schema=trips_ddl).filter("fare > 30").select("trip_id", "fare")
>>> re.findall(r"ReadSchema: [^\n]*", plan_of(csv_query))
['ReadSchema: struct<trip_id:string,fare:double>']
>>> query.count()
3
```

**Notes:**

* **`df.explain()`** prints the **physical plan**: what Spark will actually do. Read it from the
  bottom (the data source) to the top (the result). You can also try `df.explain("formatted")`
  for a more readable layout.
* **Predicate pushdown:** `PushedFilters` shows that the filter `fare > 30` was **pushed down**
  into the file reader. Spark didn't write the filter *after* reading; the reader applies it
  *while* reading.
* **Column pruning:** `ReadSchema` shows that only the **2 columns** the query uses are read, not
  all 8. You wrote `filter()` before `select()`, and Spark reordered and optimized it anyway. This
  is the **Catalyst** optimizer.
* **Why Parquet benefits most:** both formats show the same pushed filters and read schema, but
  a CSV reader must still **read and split every line** of the file. A Parquet reader reads only
  the chosen **columns** from disk, and uses the per-block min/max **statistics** to skip whole
  blocks where `fare` can't be over 30. On big data, that's the difference between reading 100 GB
  and reading 1 GB.
* None of this works through a Python UDF (Set 08, Q6): Spark can't push a filter into code it
  can't see.

---

## Q7 (Intermediate+) — Caching DataFrames

```python
>>> revenue = trips.groupBy("city").agg(F.sum("fare").alias("revenue"))
>>> cached = revenue.cache()
>>> cached.is_cached
True
>>> print(cached.storageLevel)
Disk Memory Deserialized 1x Replicated
>>> "InMemoryTableScan" in plan_of(cached)
True
>>> cached.orderBy("city").collect()
[Row(city='Oakland', revenue=168.25), Row(city='San Jose', revenue=308.0)]
>>> cached.unpersist().is_cached
False
```

**Notes:**

* **`cache()` works on DataFrames as on RDDs** ([RDD Set 08, Q3](../rdd_practice_questions/ANSWERS_08.md#q3-basic--cache-compute-once-use-many-times)):
  the first action computes and stores the result, and later actions reuse it. It's lazy, and
  `unpersist()` frees it.
* **The default storage level differs:** for DataFrames it's **`MEMORY_AND_DISK`** (it spills to
  disk if memory is full), stored in Spark's compact **columnar** in-memory format. For RDDs, it
  was `MEMORY_ONLY`.
* After caching, the plan reads from an **`InMemoryTableScan`** instead of the file.
* **When to cache:** a DataFrame that is used by **several** actions, and is expensive to
  compute: the same rule as for RDDs ([RDD Set 08, Q9](../rdd_practice_questions/ANSWERS_08.md#q9-intermediate--where-to-put-cache)).
  Don't cache a DataFrame that is used once, or one that is just a simple read of a Parquet file.
  That file is already fast to read.
* Remember the warning of [Set 08, Q6](ANSWERS_08.md#q6-intermediate--why-built-in-functions-are-better):
  in our test setup, Python UDFs could hang after a `cache()` in the same session.

---

## Q8 (Intermediate+) — Adaptive query execution and join strategies

```python
>>> spark.conf.get("spark.sql.adaptive.enabled"), spark.conf.get("spark.sql.shuffle.partitions")
('true', '200')

>>> counts = trips.groupBy("city").count()
>>> "AdaptiveSparkPlan isFinalPlan=false" in plan_of(counts)
True
>>> counts.rdd.getNumPartitions()
1

>>> spark.conf.set("spark.sql.adaptive.enabled", "false")
>>> trips.groupBy("city").count().rdd.getNumPartitions()
200
>>> spark.conf.set("spark.sql.adaptive.enabled", "true")

>>> drivers = spark.read.csv("data/drivers.csv", header=True)
>>> "BroadcastHashJoin" in plan_of(trips.join(drivers, "driver_id"))
True
```

**Notes:**

* **Every shuffle** (`groupBy()`, `join()`, `orderBy()`, `distinct()`...) splits its output into
  `spark.sql.shuffle.partitions` partitions, **200** by default. For 20 trips, that would mean
  200 tasks, nearly all of them empty. **Without AQE**, that's exactly what you get.
* **Adaptive query execution (AQE)**, on by default since Spark 3.2, **re-plans the query while it
  runs**, using the real sizes it measures after each shuffle:
  * it **coalesces** small shuffle partitions: here the 200 become **1**;
  * it can **switch a join to a broadcast join** when one side turns out to be small;
  * it **splits skewed partitions**: the hot-key problem of
    [RDD Set 08, Q7](../rdd_practice_questions/ANSWERS_08.md#q7-intermediate--a-skewed-key-and-salting),
    handled automatically for joins.
* **`AdaptiveSparkPlan isFinalPlan=false`** in `explain()` means the plan shown is the **initial**
  one, which AQE may still change at run time. After an action, the Spark UI shows the final plan.
* **Join strategies:** a small table (here `drivers`) is joined with a **`BroadcastHashJoin`**,
  with no shuffle of the big table ([Set 05, Q5](ANSWERS_05.md#q5-intermediate--joining-four-tables-and-broadcast-joins)).
  Two big tables use a **`SortMergeJoin`**, which shuffles and sorts both sides.
* With AQE on, you rarely need to tune `spark.sql.shuffle.partitions` by hand. Set it high enough
  for your biggest data, and let AQE shrink it.

---

## Q9 (Intermediate+) — An end-to-end ETL job

```python
>>> raw = spark.read.csv("data/payments_raw.csv", header=True)
>>> clean = raw.select(
...     "payment_id", "trip_id",
...     F.coalesce(F.try_to_timestamp("paid_at", F.lit("yyyy-MM-dd HH:mm:ss")),
...                F.try_to_timestamp("paid_at", F.lit("MM/dd/yyyy HH:mm"))).alias("paid_at"),
...     F.lower(F.trim("method")).alias("method"),
...     F.regexp_replace("amount", r"[^0-9.]", "").try_cast("double").alias("amount"))
>>> paid_trips = (clean.join(trips.select("trip_id", "city", "fare"), "trip_id")
...                    .withColumn("tip", F.round(F.col("amount") - F.col("fare"), 2))
...                    .withColumn("day", F.to_date("paid_at")))
>>> (paid_trips.repartition("city", "day")
...            .write.mode("overwrite")
...            .partitionBy("city", "day")
...            .parquet("output/paid_trips"))

>>> sorted(d for d in os.listdir("output/paid_trips/city=Oakland") if d.startswith("day="))
['day=2026-10-05', 'day=2026-10-06', 'day=2026-10-07', 'day=2026-10-08', 'day=2026-10-09']

>>> lake = spark.read.parquet("output/paid_trips")
>>> lake.printSchema()
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

>>> one_day = lake.filter((F.col("city") == "San Jose") & (F.col("day") == "2026-10-10"))
>>> one_day.select("trip_id", "method", "amount", "tip").orderBy("trip_id").show()
+-------+------+------+----+
|trip_id|method|amount| tip|
+-------+------+------+----+
|    t06|  card|   8.8| 0.8|
|    t13|  cash|  NULL|NULL|
|    t20|   app|  39.6| 3.6|
+-------+------+------+----+

>>> partition_filters = re.findall(r"PartitionFilters: \[[^\]]*\]", plan_of(one_day))[0]
>>> "San Jose" in partition_filters, "2026-10-10" in partition_filters
(True, True)
>>> sorted(lake.select("trip_id", "amount").collect()) == sorted(paid_trips.select("trip_id", "amount").collect())
True

>>> shutil.rmtree("output")
>>> os.path.exists("output")
False
```

**Notes:**

* **ETL** = **E**xtract (read the raw CSV), **T**ransform (clean it as in
  [Set 02, Q9](ANSWERS_02.md#q9-intermediate--cleaning-a-messy-payments-file), join it with the
  trips, add the tip), and **L**oad (write it to its final place). This is the everyday work of a
  data engineer.
* **The output is a small "data lake" table:** Parquet files, partitioned by **city** and **day**
  (`city=Oakland/day=2026-10-05/...`). Each partition column adds one folder level.
* **Reading it back**, the types are exact (`timestamp`, `date`, `double`), with no schema to
  write, and `city` and `day` come back from the folder names.
* **Partition pruning** on both columns: the plan's `PartitionFilters` contain both `San Jose` and
  `2026-10-10`, so the query reads **one folder**. That's why tables are partitioned by date.
* **`repartition("city", "day")` before writing** gives exactly one file per folder (Q5).
* `t13`'s payment had no valid amount (`N/A` in the raw file), so its `amount` and `tip` are `NULL`.
  The ETL keeps the row, so the problem stays visible downstream, instead of silently dropping it.
* **The check at the end** compares the written-and-read-back data with the in-memory result.
  Writing a few such checks into a pipeline catches problems before anyone uses the data.
