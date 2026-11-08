# PySpark DataFrame Transformations: Exam Review

A review of the most important **DataFrame transformations** (and the actions that go
with them). It is the companion of the
[RDD Transformations Exam Review](../rdd_transformations_review/README.md): it uses
**the same customers and orders**, in the same order of topics, so you can put the two
side by side and see how each RDD idea is written with DataFrames:

1. **Column operations** (the DataFrame "mappers"): `select`, `withColumn`, `explode`, ...
2. **Filters**: `filter`, `dropna`, `distinct`, `dropDuplicates`, ...
3. **Aggregations** (the DataFrame "reducers"): `groupBy`, `agg`, `pivot`, window functions, ...
4. **Joins**: `join` with every `how=`, semi and anti joins, set operations, ...

Each operation starts with a small **scenario** (the business problem), then shows
**how it works** and a **worked example**, and ends with **notes**: the things exam
questions like to test. Every section links to the matching RDD section.

Every example was run in the `bin/pyspark` shell (Spark 4.3.0). The output is copied
from those real runs. Type the examples yourself: predict each output **before** you
press Enter. That is exactly what an exam asks you to do.

## Contents

* [Part 0: The basics you need first](#part-0-the-basics-you-need-first)
* [Part 1: Column operations (the "mappers")](#part-1-column-operations-the-mappers)
  * [1.1 `select()`: the DataFrame `map()`](#11-select-the-dataframe-map) ·
    [1.2 `withColumn()`, `withColumnRenamed()`, `drop()`](#12-withcolumn-withcolumnrenamed-drop) ·
    [1.3 `when()` / `otherwise()`: if/else for columns](#13-when--otherwise-ifelse-for-columns) ·
    [1.4 `split()` + `explode()`: the DataFrame `flatMap()`](#14-split--explode-the-dataframe-flatmap) ·
    [1.5 Types: `cast()` and `try_cast()`](#15-types-cast-and-try_cast)
* [Part 2: Filters](#part-2-filters)
  * [2.1 `filter()` / `where()`](#21-filter--where) ·
    [2.2 Missing values: `isNull()`, `dropna()`, `fillna()`](#22-missing-values-isnull-dropna-fillna) ·
    [2.3 `distinct()` and `dropDuplicates()`](#23-distinct-and-dropduplicates) ·
    [2.4 Filtering with a lookup table: `isin()` and `left_semi`](#24-filtering-with-a-lookup-table-isin-and-left_semi) ·
    [2.5 `limit()` and `sample()`](#25-limit-and-sample) ·
    [2.6 Top-N: `orderBy()` + `limit()`, `first()`, `take()`](#26-top-n-orderby--limit-first-take)
* [Part 3: Aggregations (the "reducers")](#part-3-aggregations-the-reducers)
  * [3.1 Whole-table aggregates: `agg()`](#31-whole-table-aggregates-agg) ·
    [3.2 `groupBy().agg()`: the DataFrame `reduceByKey()`](#32-groupbyagg-the-dataframe-reducebykey) ·
    [3.3 Averages: no `(sum, count)` pairs needed](#33-averages-no-sum-count-pairs-needed) ·
    [3.4 Counting: `count("*")`, `count(col)`, `countDistinct()`](#34-counting-count-countcol-countdistinct) ·
    [3.5 `collect_list()` / `collect_set()`: the DataFrame `groupByKey()`](#35-collect_list--collect_set-the-dataframe-groupbykey) ·
    [3.6 Filtering after aggregation (SQL `HAVING`)](#36-filtering-after-aggregation-sql-having) ·
    [3.7 `NULL` values in aggregates](#37-null-values-in-aggregates) ·
    [3.8 `pivot()`: a table of key × key](#38-pivot-a-table-of-key--key) ·
    [3.9 Window functions: aggregate without collapsing rows](#39-window-functions-aggregate-without-collapsing-rows) ·
    [3.10 Sorting: `orderBy()`](#310-sorting-orderby) ·
    [3.11 Which aggregation should I use?](#311-which-aggregation-should-i-use)
* [Part 4: Joins](#part-4-joins)
  * [4.1 Inner join](#41-inner-join) ·
    [4.2 Left outer join](#42-left-outer-join) ·
    [4.3 Right and full outer joins](#43-right-and-full-outer-joins) ·
    [4.4 Anti join: rows without a match](#44-anti-join-rows-without-a-match) ·
    [4.5 Semi join: rows with a match](#45-semi-join-rows-with-a-match) ·
    [4.6 Joining on columns with different names (re-keying)](#46-joining-on-columns-with-different-names-re-keying) ·
    [4.7 Self-join](#47-self-join) ·
    [4.8 Broadcast join](#48-broadcast-join) ·
    [4.9 `crossJoin()`: every pair](#49-crossjoin-every-pair) ·
    [4.10 Set operations: `unionByName()`, `intersect()`, `subtract()`](#410-set-operations-unionbyname-intersect-subtract)
* [Part 5: Putting it all together](#part-5-putting-it-all-together)
* [Part 6: Cheat sheet](#part-6-cheat-sheet)
* [Part 7: Exam traps](#part-7-exam-traps)
* [Part 8: Test yourself](#part-8-test-yourself)

---

## Part 0: The basics you need first

**DataFrame**: a distributed table with **named, typed columns**. Spark knows its
structure (the **schema**), so it can optimize your query before running it (the
*Catalyst* optimizer). Under the hood a DataFrame is still an RDD (of `Row` objects):
`df.rdd` gives it to you.

* A **transformation** (`select`, `filter`, `groupBy().agg()`, `join`, ...) returns a
  **new DataFrame**. It is **lazy**: nothing runs yet. Spark only builds a **plan**.
* An **action** (`show`, `count`, `collect`, `first`, `take`, `write...`) makes Spark
  optimize the plan and run the job.
* You describe **what** you want with **column expressions** (`F.col("amount") * 2`),
  not Python functions. Spark runs them inside the JVM, which is much faster than a
  Python `lambda`.
* **Narrow** transformations (`select`, `filter`, `withColumn`, ...) work inside each
  partition. **Wide** transformations (`groupBy().agg()`, `join`, `distinct`,
  `orderBy`, ...) need a **shuffle**, exactly like `reduceByKey()` or `join()` on RDDs.

In the `pyspark` shell, `spark` (the `SparkSession`) already exists. We import the
functions module as `F` (the usual style: `F.sum`, `F.max`, `F.round` would otherwise
hide Python's own `sum`, `max`, `round`):

```python
>>> from pyspark.sql import functions as F
>>> from pyspark.sql import Window
```

### The data used throughout

The same small online store as in the RDD review: four customers and seven orders,
with the same two deliberate gaps. Customer `c4` (Dana) has **no orders**, and order
`o6` belongs to a customer `c5` who is **not in the customer list**.

```python
>>> customers = spark.createDataFrame([
...     ("c1", "Alice", "San Jose"),
...     ("c2", "Bob",   "Oakland"),
...     ("c3", "Chen",  "San Jose"),
...     ("c4", "Dana",  "Fresno"),
... ], "customer_id STRING, name STRING, city STRING")

>>> orders = spark.createDataFrame([
...     ("o1", "c1", "books", 20),
...     ("o2", "c2", "games", 35),
...     ("o3", "c1", "games", 15),
...     ("o4", "c3", "books", 40),
...     ("o5", "c1", "books", 25),
...     ("o6", "c5", "music", 10),
...     ("o7", "c2", "books", 30),
... ], "order_id STRING, customer_id STRING, category STRING, amount INT")

>>> orders.show()
+--------+-----------+--------+------+
|order_id|customer_id|category|amount|
+--------+-----------+--------+------+
|      o1|         c1|   books|    20|
|      o2|         c2|   games|    35|
|      o3|         c1|   games|    15|
|      o4|         c3|   books|    40|
|      o5|         c1|   books|    25|
|      o6|         c5|   music|    10|
|      o7|         c2|   books|    30|
+--------+-----------+--------+------+
>>> orders.printSchema()
root
 |-- order_id: string (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- category: string (nullable = true)
 |-- amount: integer (nullable = true)
```

The second argument is the **schema** as a short DDL string (`name TYPE, ...`). Without
it, Spark would guess the types (and call integer columns `long`).

Laziness in action:

```python
>>> big = orders.filter(F.col("amount") >= 25)   # transformation: nothing runs yet
>>> type(big).__name__                            # just a plan for a new DataFrame
'DataFrame'
>>> big.count()                                   # action: now Spark runs the job
4
```

**Notes:**

* Like an RDD, a DataFrame is **immutable**: `filter()` returns a new DataFrame and
  `orders` does not change.
* `show()` prints at most 20 rows; `show(n)` prints `n`. It is an **action**.
* After a shuffle, row order is not guaranteed. The examples below end with
  `orderBy(...)` before `show()` to get a stable answer, just as the RDD review used
  `sorted(...)`.
* RDD version: [Part 0](../rdd_transformations_review/README.md#part-0-the-basics-you-need-first).

---

## Part 1: Column operations (the "mappers")

With RDDs, you changed each element with a Python function. With DataFrames, you
describe the new **columns**. All of these are **narrow**: no shuffle.

| DataFrame | Does | RDD counterpart |
|---|---|---|
| `select(cols)` | build each output row from expressions | `map()` |
| `withColumn(name, expr)` | add or replace one column | `map()` that adds a field |
| `when().otherwise()` | if/else inside an expression | `if` inside a `map()` lambda |
| `explode(array)` | one output row per array element | `flatMap()` |
| `cast` / `try_cast` | change a column's type | `int(...)` in a parse function |

### 1.1 `select()`: the DataFrame `map()`

**Scenario:** the payment system wants each order's amount in **cents**, next to the
order id.

**How it works:** `df.select(...)` builds every output row from the expressions you
list. **N rows in → N rows out**, like `map()`. A column can be named in several ways:
`"amount"`, `F.col("amount")`, `orders.amount`, `orders["amount"]`. Use `.alias()` to
name a computed column.

```python
>>> orders.select("order_id", (F.col("amount") * 100).alias("cents")).show()
+--------+-----+
|order_id|cents|
+--------+-----+
|      o1| 2000|
|      o2| 3500|
|      o3| 1500|
|      o4| 4000|
|      o5| 2500|
|      o6| 1000|
|      o7| 3000|
+--------+-----+

>>> orders.rdd.map(lambda r: (r.order_id, r.amount * 100)).take(3)   # the RDD way
[('o1', 2000), ('o2', 3500), ('o3', 1500)]
```

`selectExpr()` lets you write each column as a SQL expression:

```python
>>> orders.selectExpr("order_id", "amount * 100 AS cents", "upper(category) AS cat").show(3)
+--------+-----+-----+
|order_id|cents|  cat|
+--------+-----+-----+
|      o1| 2000|BOOKS|
|      o2| 3500|GAMES|
|      o3| 1500|GAMES|
+--------+-----+-----+
only showing top 3 rows
```

**Notes:**

* `select()` keeps **only** the columns you list. To keep everything and add one, use
  `withColumn()` (next section), or `select("*", new_expr)`.
* An expression without `.alias()` gets an automatic name like `(amount * 100)`.
* `df.rdd` gives an RDD of `Row` objects: `r.amount` or `r["amount"]` reads a field.
* RDD version: [1.1 `map()`](../rdd_transformations_review/README.md#11-map).

### 1.2 `withColumn()`, `withColumnRenamed()`, `drop()`

**Scenario:** the finance team wants a `tax` column (9%) and a `total`, and prefers the
name `amount_usd`.

**How it works:** `withColumn(name, expr)` adds a column, or **replaces** it if the name
already exists. Each call returns a new DataFrame, so you chain them.

```python
>>> with_tax = (orders
...     .withColumn("tax", F.round(F.col("amount") * 0.09, 2))
...     .withColumn("total", F.col("amount") + F.col("tax"))
...     .withColumnRenamed("amount", "amount_usd")
...     .drop("category"))
>>> with_tax.show()
+--------+-----------+----------+----+-----+
|order_id|customer_id|amount_usd| tax|total|
+--------+-----------+----------+----+-----+
|      o1|         c1|        20| 1.8| 21.8|
|      o2|         c2|        35|3.15|38.15|
|      o3|         c1|        15|1.35|16.35|
|      o4|         c3|        40| 3.6| 43.6|
|      o5|         c1|        25|2.25|27.25|
|      o6|         c5|        10| 0.9| 10.9|
|      o7|         c2|        30| 2.7| 32.7|
+--------+-----------+----------+----+-----+
>>> orders.columns          # the original is unchanged
['order_id', 'customer_id', 'category', 'amount']
```

**Notes:**

* `withColumn("amount", ...)` with an **existing** name replaces that column (no error).
* `withColumns({"a": expr1, "b": expr2})` adds several at once.
* `drop()` of a column that does not exist is silently ignored.

### 1.3 `when()` / `otherwise()`: if/else for columns

**Scenario:** marketing labels each order **small** (< 20), **medium** (< 30) or
**large**.

**How it works:** `F.when(condition, value)` can be chained; the first true condition
wins; `otherwise()` gives the default. Without `otherwise()`, unmatched rows get `NULL`.

```python
>>> orders.select("order_id", "amount",
...     F.when(F.col("amount") < 20, "small")
...      .when(F.col("amount") < 30, "medium")
...      .otherwise("large").alias("size")).show()
+--------+------+------+
|order_id|amount|  size|
+--------+------+------+
|      o1|    20|medium|
|      o2|    35| large|
|      o3|    15| small|
|      o4|    40| large|
|      o5|    25|medium|
|      o6|    10| small|
|      o7|    30| large|
+--------+------+------+
```

**Notes:**

* The order of `when()` calls matters: `amount < 30` is also true for 15, but the
  first condition already matched.
* `F.lit(value)` turns a Python value into a column, e.g. `F.lit("USD").alias("currency")`.

### 1.4 `split()` + `explode()`: the DataFrame `flatMap()`

**Scenario:** customers leave short text reviews. Marketing wants to know which words
appear most: one row **per word**, not per review.

**How it works:** `F.split(col, pattern)` turns a string into an **array**;
`F.explode(array)` produces **one row per element**. A row with an empty array produces
**no** row, exactly like `flatMap()` with an empty list.

```python
>>> reviews = spark.createDataFrame(
...     [("great book",), ("too long",), ("",), ("great great game",)], "text STRING")

>>> reviews.select(F.split("text", " ").alias("words"), F.size(F.split("text", " ")).alias("n")).show()
+--------------------+---+
|               words|  n|
+--------------------+---+
|       [great, book]|  2|
|         [too, long]|  2|
|                  []|  1|
|[great, great, game]|  3|
+--------------------+---+

>>> words = reviews.select(F.explode(F.split("text", " ")).alias("word"))
>>> words.filter(F.col("word") != "").groupBy("word").count().orderBy(F.desc("count"), "word").show()
+-----+-----+
| word|count|
+-----+-----+
|great|    3|
| book|    1|
| game|    1|
| long|    1|
|  too|    1|
+-----+-----+
```

**Notes:**

* Splitting the empty string `""` gives `[""]`, an array with **one empty string**, not
  an empty array: look at `n = 1` (`show()` prints it as `[]`, which hides the empty
  string). That is why we filter `word != ""`. Python's `"".split()` gives `[]`.
* `F.explode_outer()` keeps rows whose array is empty or `NULL` (with a `NULL` value).
* `F.posexplode()` also returns each element's position.
* RDD version: [1.2 `flatMap()`](../rdd_transformations_review/README.md#12-flatmap).

### 1.5 Types: `cast()` and `try_cast()`

**Scenario:** the order log arrives as text, so `amount` is a **string**. Before you
can sum it, it must become a number.

**How it works:** `F.col("x").cast("int")` converts a column. In Spark 4, **ANSI mode**
is on by default: casting text that is not a number (`"abc"`) **fails the whole job**.
`try_cast("int")` turns bad values into `NULL` instead.

```python
>>> raw = spark.createDataFrame([("o1", "20"), ("o2", "abc"), ("o3", "15")],
...                             "order_id STRING, amount STRING")
>>> raw.printSchema()
root
 |-- order_id: string (nullable = true)
 |-- amount: string (nullable = true)
>>> raw.select("order_id", F.col("amount").try_cast("int").alias("amount")).show()
+--------+------+
|order_id|amount|
+--------+------+
|      o1|    20|
|      o2|  NULL|
|      o3|    15|
+--------+------+
```

**Notes:**

* Strings don't add up: `F.sum()` of a string column does not do what you expect.
  Convert first.
* With RDDs you wrote `int(...)` inside a parse function, in a `try` block. With
  DataFrames, `try_cast` does the "parse or give up" part for you.
* RDD version: [1.2 `flatMap()`, parse-or-drop](../rdd_transformations_review/README.md#12-flatmap).

---

## Part 2: Filters

### 2.1 `filter()` / `where()`

**Scenario:** the warehouse needs every **books** order of **25 or more**.

**How it works:** `df.filter(condition)` (alias `where`) keeps the rows where the
condition is true. Combine conditions with `&` (and), `|` (or), `~` (not), and put
**each condition in parentheses**.

```python
>>> orders.filter((F.col("category") == "books") & (F.col("amount") >= 25)).show()
+--------+-----------+--------+------+
|order_id|customer_id|category|amount|
+--------+-----------+--------+------+
|      o4|         c3|   books|    40|
|      o5|         c1|   books|    25|
|      o7|         c2|   books|    30|
+--------+-----------+--------+------+
>>> orders.where("category = 'books' AND amount >= 25").count()     # SQL string, same rows
3
>>> orders.filter(F.col("customer_id").isin("c2", "c3")).select("order_id").orderBy("order_id").show()
+--------+
|order_id|
+--------+
|      o2|
|      o4|
|      o7|
+--------+
```

**Notes:**

* Python's `and` / `or` / `not` do **not** work on columns (`ValueError: Cannot convert
  column into bool`). Use `&`, `|`, `~`.
* Without parentheses, `F.col("a") == 1 & F.col("b") == 2` is read as
  `F.col("a") == (1 & F.col("b")) == 2`: an error or a wrong result.
* `filter()` is narrow: no shuffle.
* RDD version: [2.1 `filter()`](../rdd_transformations_review/README.md#21-filter).

### 2.2 Missing values: `isNull()`, `dropna()`, `fillna()`

**Scenario:** a second order file has gaps: one order has no amount, one has no
category. Decide what to do with each.

```python
>>> messy = spark.createDataFrame([
...     ("o8", "c1", "books", 12),
...     ("o9", "c3", None,    18),
...     ("o10", "c2", "games", None),
... ], "order_id STRING, customer_id STRING, category STRING, amount INT")

>>> messy.filter(F.col("amount").isNull()).show()
+--------+-----------+--------+------+
|order_id|customer_id|category|amount|
+--------+-----------+--------+------+
|     o10|         c2|   games|  NULL|
+--------+-----------+--------+------+
>>> messy.dropna(subset=["amount"]).show()
+--------+-----------+--------+------+
|order_id|customer_id|category|amount|
+--------+-----------+--------+------+
|      o8|         c1|   books|    12|
|      o9|         c3|    NULL|    18|
+--------+-----------+--------+------+
>>> messy.fillna({"category": "unknown", "amount": 0}).show()
+--------+-----------+--------+------+
|order_id|customer_id|category|amount|
+--------+-----------+--------+------+
|      o8|         c1|   books|    12|
|      o9|         c3| unknown|    18|
|     o10|         c2|   games|     0|
+--------+-----------+--------+------+
```

**Notes:**

* Test for missing values with `isNull()` / `isNotNull()`. `F.col("x") == None` does
  **not** work: comparing with `NULL` gives `NULL`, which `filter()` treats as false.
* `dropna()` with no arguments drops a row if **any** column is `NULL`.
* `fillna()` with a dict fills each column with its own value.
* RDD version: [2.2 Cleaning dirty data](../rdd_transformations_review/README.md#22-cleaning-dirty-data).

### 2.3 `distinct()` and `dropDuplicates()`

**Scenario:** which categories does the store sell? And: keep just **one order per
customer** (any one) for a survey.

```python
>>> orders.select("category").distinct().orderBy("category").show()
+--------+
|category|
+--------+
|   books|
|   games|
|   music|
+--------+
>>> orders.dropDuplicates(["customer_id"]).count()
4
```

**Notes:**

* `distinct()` compares **whole rows**; `dropDuplicates(["col"])` compares only the
  listed columns and keeps **one** of the matching rows (which one is not guaranteed).
* Both are **wide** (shuffle), like RDD `distinct()`.
* RDD version: [2.3 `distinct()`](../rdd_transformations_review/README.md#23-distinct).

### 2.4 Filtering with a lookup table: `isin()` and `left_semi`

**Scenario:** keep only the orders of customers on the VIP list.

**How it works:** for a short list in Python, `isin()`. For a list that is itself a
DataFrame (maybe large), a **left semi join** (Part 4.5): it keeps the left rows that have
a match, and adds no columns.

```python
>>> vip_ids = ["c1", "c3"]
>>> orders.filter(F.col("customer_id").isin(vip_ids)).count()
4

>>> vips = spark.createDataFrame([("c1",), ("c3",)], "customer_id STRING")
>>> orders.join(vips, "customer_id", "left_semi").orderBy("order_id").show()
+-----------+--------+--------+------+
|customer_id|order_id|category|amount|
+-----------+--------+--------+------+
|         c1|      o1|   books|    20|
|         c1|      o3|   games|    15|
|         c3|      o4|   books|    40|
|         c1|      o5|   books|    25|
+-----------+--------+--------+------+
```

**Notes:**

* With RDDs, you broadcast a Python `set` and tested `x in vip.value` inside
  `filter()`. A semi join with a small DataFrame is automatically broadcast by Spark.
* RDD version: [2.4 Filtering with a lookup set](../rdd_transformations_review/README.md#24-filtering-with-a-lookup-set-broadcast).

### 2.5 `limit()` and `sample()`

```python
>>> orders.limit(2).show()
+--------+-----------+--------+------+
|order_id|customer_id|category|amount|
+--------+-----------+--------+------+
|      o1|         c1|   books|    20|
|      o2|         c2|   games|    35|
+--------+-----------+--------+------+
>>> spark.range(0, 1000, 1, 4).sample(fraction=0.1, seed=42).count()
111
```

**Notes:**

* `limit(n)` is a **transformation** (returns a DataFrame); `take(n)` and `head(n)` are
  **actions** (return a list of `Row`s).
* `sample()` returns **about** the fraction, not exactly. The same seed on the same data
  and partitions gives the same sample.
* RDD version: [2.5 `sample()`](../rdd_transformations_review/README.md#25-sample).

### 2.6 Top-N: `orderBy()` + `limit()`, `first()`, `take()`

**Scenario:** the three biggest orders.

```python
>>> orders.orderBy(F.desc("amount")).limit(3).show()
+--------+-----------+--------+------+
|order_id|customer_id|category|amount|
+--------+-----------+--------+------+
|      o4|         c3|   books|    40|
|      o2|         c2|   games|    35|
|      o7|         c2|   books|    30|
+--------+-----------+--------+------+
>>> orders.orderBy(F.desc("amount")).first()
Row(order_id='o4', customer_id='c3', category='books', amount=40)
>>> orders.take(2)                      # first 2 rows in the DataFrame's order: NOT the largest
[Row(order_id='o1', customer_id='c1', category='books', amount=20), Row(order_id='o2', customer_id='c2', category='games', amount=35)]
>>> orders.first()["amount"]
20
```

**Notes:**

* `take(n)` is **not** "top n". There is no `top()` for DataFrames: sort, then
  `limit()` / `take()`.
* `first()` returns a `Row`. Read a field with `row["amount"]` or `row.amount`.
* Sort descending with `F.desc("col")`, `F.col("col").desc()`, or
  `orderBy("col", ascending=False)`.
* RDD version: [2.6 Top-N](../rdd_transformations_review/README.md#26-top-n-take-top-takeordered).

---

## Part 3: Aggregations (the "reducers")

With RDDs, you built `(key, value)` pairs and chose among `reduceByKey`,
`aggregateByKey`, `combineByKey`, ... With DataFrames, almost every case is

```text
df.groupBy("key column(s)").agg(F.sum(...), F.avg(...), F.count(...), ...)
```

Spark picks the efficient plan for you: like `reduceByKey()`, it combines partial
results on each partition **before** the shuffle.

### 3.1 Whole-table aggregates: `agg()`

**Scenario:** total revenue, number of orders, average, smallest and largest order.

**How it works:** `df.agg(...)` collapses the whole DataFrame into **one row**.

```python
>>> orders.agg(
...     F.sum("amount").alias("total"),
...     F.count("*").alias("orders"),
...     F.avg("amount").alias("avg"),
...     F.min("amount").alias("min"),
...     F.max("amount").alias("max"),
... ).show()
+-----+------+----+---+---+
|total|orders| avg|min|max|
+-----+------+----+---+---+
|  175|     7|25.0| 10| 40|
+-----+------+----+---+---+

>>> orders.agg(F.sum("amount")).first()[0]          # the number itself
175
>>> orders.rdd.map(lambda r: r.amount).reduce(lambda a, b: a + b)   # the RDD way
175
```

**Notes:**

* The result of `agg()` is a **DataFrame**, not a number. `first()[0]` gets the value.
* RDD version: [3.1 `reduce()`](../rdd_transformations_review/README.md#31-reduce),
  [3.2 `fold()` and `aggregate()`](../rdd_transformations_review/README.md#32-fold-and-aggregate).

### 3.2 `groupBy().agg()`: the DataFrame `reduceByKey()`

**Scenario:** total spent **per customer**.

```python
>>> orders.groupBy("customer_id").agg(F.sum("amount").alias("total")).orderBy("customer_id").show()
+-----------+-----+
|customer_id|total|
+-----------+-----+
|         c1|   60|
|         c2|   65|
|         c3|   40|
|         c5|   10|
+-----------+-----+

>>> sorted(orders.rdd.map(lambda r: (r.customer_id, r.amount))       # the RDD way
...                  .reduceByKey(lambda a, b: a + b).collect())
[('c1', 60), ('c2', 65), ('c3', 40), ('c5', 10)]
```

Several results in one pass, and grouping by **two** columns (with RDDs, the key would
be a tuple):

```python
>>> (orders.groupBy("customer_id")
...     .agg(F.count("*").alias("orders"),
...          F.sum("amount").alias("total"),
...          F.max("amount").alias("biggest"))
...     .orderBy("customer_id").show())
+-----------+------+-----+-------+
|customer_id|orders|total|biggest|
+-----------+------+-----+-------+
|         c1|     3|   60|     25|
|         c2|     2|   65|     35|
|         c3|     1|   40|     40|
|         c5|     1|   10|     10|
+-----------+------+-----+-------+

>>> orders.groupBy("customer_id", "category").agg(F.sum("amount").alias("total")) \
...     .orderBy("customer_id", "category").show()
+-----------+--------+-----+
|customer_id|category|total|
+-----------+--------+-----+
|         c1|   books|   45|
|         c1|   games|   15|
|         c2|   books|   30|
|         c2|   games|   35|
|         c3|   books|   40|
|         c5|   music|   10|
+-----------+--------+-----+
```

**Notes:**

* Shortcuts exist: `groupBy("k").count()`, `groupBy("k").sum("amount")`. They name the
  column `count` / `sum(amount)`; `agg()` + `alias()` gives better names.
* `groupBy()` alone does nothing useful: it returns a `GroupedData` object, and you must
  call `agg()` (or a shortcut) on it.
* RDD version: [3.3 `reduceByKey()`](../rdd_transformations_review/README.md#33-reducebykey).

### 3.3 Averages: no `(sum, count)` pairs needed

**Scenario:** average order amount per category.

**How it works:** with RDDs, an average needed `aggregateByKey()` or `combineByKey()`
with `(sum, count)` pairs, because an average of averages is wrong. With DataFrames, it
is just `F.avg()`.

```python
>>> orders.groupBy("category").agg(F.round(F.avg("amount"), 2).alias("avg")).orderBy("category").show()
+--------+-----+
|category|  avg|
+--------+-----+
|   books|28.75|
|   games| 25.0|
|   music| 10.0|
+--------+-----+
```

**Notes:**

* Spark computes the average correctly across partitions (it keeps a sum and a count
  internally, exactly what you did by hand).
* RDD version: [3.6 `aggregateByKey()`](../rdd_transformations_review/README.md#36-aggregatebykey),
  [3.7 `combineByKey()`](../rdd_transformations_review/README.md#37-combinebykey).

### 3.4 Counting: `count("*")`, `count(col)`, `countDistinct()`

**Scenario:** per city of the customer list: how many customers? And in the orders:
how many orders, and how many **different** customers ordered each category?

```python
>>> customers.groupBy("city").count().orderBy("city").show()
+--------+-----+
|    city|count|
+--------+-----+
|  Fresno|    1|
| Oakland|    1|
|San Jose|    2|
+--------+-----+

>>> orders.groupBy("category").agg(
...     F.count("*").alias("orders"),
...     F.countDistinct("customer_id").alias("customers"),
... ).orderBy("category").show()
+--------+------+---------+
|category|orders|customers|
+--------+------+---------+
|   books|     4|        3|
|   games|     2|        2|
|   music|     1|        1|
+--------+------+---------+
```

**Notes:**

* `F.count("*")` counts rows; `F.count("col")` counts rows where `col` is **not
  `NULL`**; `F.countDistinct("col")` counts different non-`NULL` values.
* RDD version: [3.8 `countByKey()`, `countByValue()`](../rdd_transformations_review/README.md#38-countbykey-countbyvalue-collectasmap).

### 3.5 `collect_list()` / `collect_set()`: the DataFrame `groupByKey()`

**Scenario:** list each customer's order ids.

```python
>>> (orders.groupBy("customer_id")
...     .agg(F.array_sort(F.collect_list("order_id")).alias("orders"),
...          F.array_sort(F.collect_set("category")).alias("categories"))
...     .orderBy("customer_id").show(truncate=False))
+-----------+------------+--------------+
|customer_id|orders      |categories    |
+-----------+------------+--------------+
|c1         |[o1, o3, o5]|[books, games]|
|c2         |[o2, o7]    |[books, games]|
|c3         |[o4]        |[books]       |
|c5         |[o6]        |[music]       |
+-----------+------------+--------------+
```

**Notes:**

* `collect_list` keeps duplicates; `collect_set` removes them. The order inside the
  array is **not guaranteed**: wrap it in `F.array_sort()` when order matters.
* Like `groupByKey()`, this moves **every value** to one place. Prefer a real
  aggregate (`sum`, `count`, ...) when that is all you need.
* RDD version: [3.4 `groupByKey()`](../rdd_transformations_review/README.md#34-groupbykey).

### 3.6 Filtering after aggregation (SQL `HAVING`)

**Scenario:** which customers spent **more than 50** in total?

**How it works:** `filter()` **before** `groupBy()` chooses the rows that take part (SQL
`WHERE`); `filter()` **after** `agg()` chooses the groups (SQL `HAVING`).

```python
>>> (orders.groupBy("customer_id").agg(F.sum("amount").alias("total"))
...     .filter(F.col("total") > 50).orderBy("customer_id").show())
+-----------+-----+
|customer_id|total|
+-----------+-----+
|         c1|   60|
|         c2|   65|
+-----------+-----+

>>> (orders.filter(F.col("category") == "books")           # WHERE: books only
...     .groupBy("customer_id").agg(F.sum("amount").alias("books_total"))
...     .orderBy("customer_id").show())
+-----------+-----------+
|customer_id|books_total|
+-----------+-----------+
|         c1|         45|
|         c2|         30|
|         c3|         40|
+-----------+-----------+
```

### 3.7 `NULL` values in aggregates

**Scenario:** the messy file from 2.2: what is the total and the average amount?

```python
>>> messy.agg(F.count("*").alias("rows"), F.count("amount").alias("amounts"),
...           F.sum("amount").alias("total"), F.avg("amount").alias("avg")).show()
+----+-------+-----+----+
|rows|amounts|total| avg|
+----+-------+-----+----+
|   3|      2|   30|15.0|
+----+-------+-----+----+
```

**Notes:**

* Aggregates **skip `NULL`s**: the average is `(12 + 18) / 2 = 15.0`, not `30 / 3`.
  If "missing" should count as 0, use `F.coalesce(F.col("amount"), F.lit(0))` first.
* `count("*")` counts every row, `count("amount")` only the non-`NULL` amounts.

### 3.8 `pivot()`: a table of key × key

**Scenario:** spending per customer (rows) and category (columns).

```python
>>> (orders.groupBy("customer_id")
...     .pivot("category", ["books", "games", "music"])
...     .agg(F.sum("amount"))
...     .orderBy("customer_id").show())
+-----------+-----+-----+-----+
|customer_id|books|games|music|
+-----------+-----+-----+-----+
|         c1|   45|   15| NULL|
|         c2|   30|   35| NULL|
|         c3|   40| NULL| NULL|
|         c5| NULL| NULL|   10|
+-----------+-----+-----+-----+
```

**Notes:**

* A missing combination is `NULL`; add `.fillna(0)` for zeros.
* Listing the values (`["books", "games", "music"]`) fixes the column order and saves
  Spark a job to find them.

### 3.9 Window functions: aggregate without collapsing rows

**Scenario:** for each order, show the customer's total next to it; and find each
customer's **biggest** order.

**How it works:** `F.sum("amount").over(Window.partitionBy("customer_id"))` computes the
sum per customer but **keeps every row**. With an `orderBy`, a window can rank rows:
`row_number()` numbers them 1, 2, 3, ... inside each group, so `row_number() == 1` is
"top-1 per group".

```python
>>> per_customer = Window.partitionBy("customer_id")
>>> (orders.withColumn("customer_total", F.sum("amount").over(per_customer))
...     .withColumn("pct", F.round(F.col("amount") / F.col("customer_total") * 100, 1))
...     .orderBy("customer_id", "order_id").show())
+--------+-----------+--------+------+--------------+-----+
|order_id|customer_id|category|amount|customer_total|  pct|
+--------+-----------+--------+------+--------------+-----+
|      o1|         c1|   books|    20|            60| 33.3|
|      o3|         c1|   games|    15|            60| 25.0|
|      o5|         c1|   books|    25|            60| 41.7|
|      o2|         c2|   games|    35|            65| 53.8|
|      o7|         c2|   books|    30|            65| 46.2|
|      o4|         c3|   books|    40|            40|100.0|
|      o6|         c5|   music|    10|            10|100.0|
+--------+-----------+--------+------+--------------+-----+

>>> biggest_first = Window.partitionBy("customer_id").orderBy(F.desc("amount"), "order_id")
>>> (orders.withColumn("rn", F.row_number().over(biggest_first))
...     .filter(F.col("rn") == 1).orderBy("customer_id").show())
+--------+-----------+--------+------+---+
|order_id|customer_id|category|amount| rn|
+--------+-----------+--------+------+---+
|      o5|         c1|   books|    25|  1|
|      o2|         c2|   games|    35|  1|
|      o4|         c3|   books|    40|  1|
|      o6|         c5|   music|    10|  1|
+--------+-----------+--------+------+---+
```

**Notes:**

* `groupBy` gives one row **per group**; a window gives one row **per input row**.
* With RDDs, "top-1 per key" meant `reduceByKey(max)` or `groupByKey()` and sorting in
  Python.
* `rank()` and `dense_rank()` give ties the same number; `row_number()` never does.
* Running totals: `.over(window.rowsBetween(Window.unboundedPreceding, Window.currentRow))`.

### 3.10 Sorting: `orderBy()`

```python
>>> orders.orderBy("category", F.desc("amount")).select("category", "amount", "order_id").show()
+--------+------+--------+
|category|amount|order_id|
+--------+------+--------+
|   books|    40|      o4|
|   books|    30|      o7|
|   books|    25|      o5|
|   books|    20|      o1|
|   games|    35|      o2|
|   games|    15|      o3|
|   music|    10|      o6|
+--------+------+--------+
```

**Notes:**

* `orderBy()` (alias `sort()`) is **wide**: it shuffles to sort globally.
* Several columns sort by the first, then break ties with the next.
* `F.col("x").asc_nulls_last()` / `desc_nulls_first()` decide where `NULL`s go.
* RDD version: [3.9 `sortByKey()` and `sortBy()`](../rdd_transformations_review/README.md#39-sortbykey-and-sortby).

### 3.11 Which aggregation should I use?

| You want | DataFrame | RDD |
|---|---|---|
| one number for the whole data | `df.agg(F.sum(...))` | `reduce`, `fold`, `aggregate` |
| sum / count / min / max per key | `groupBy(k).agg(...)` | `reduceByKey` |
| average per key | `groupBy(k).agg(F.avg(...))` | `aggregateByKey` / `combineByKey` with `(sum, count)` |
| all values per key | `groupBy(k).agg(F.collect_list(...))` | `groupByKey` |
| keep only some groups | `agg(...)` then `filter(...)` | reduce, then `filter` |
| a key × key table | `groupBy(k1).pivot(k2).agg(...)` | (by hand) |
| per-row result that needs the group | window function `.over(...)` | `groupByKey` + Python |

---

## Part 4: Joins

`a.join(b, on, how)` combines two DataFrames on equal values. No re-shaping into
`(key, value)` pairs is needed: name the key column.

The two inputs for this part are `customers` and `orders` from Part 0.

| `how=` | Keeps | Missing side becomes | RDD counterpart |
|---|---|---|---|
| `"inner"` (default) | matches only | (row dropped) | `join()` |
| `"left"` | all left rows | `NULL` columns | `leftOuterJoin()` |
| `"right"` | all right rows | `NULL` columns | `rightOuterJoin()` |
| `"full"` | all rows of both | `NULL` on either side | `fullOuterJoin()` |
| `"left_semi"` | left rows **with** a match, left columns only | | (filter by keys) |
| `"left_anti"` | left rows **without** a match, left columns only | | `subtractByKey()` |
| `crossJoin()` | every pair | | `cartesian()` |

### 4.1 Inner join

**Scenario:** the order list should show the **customer's name** next to each amount.

```python
>>> joined = orders.join(customers, on="customer_id", how="inner")
>>> joined.select("order_id", "customer_id", "name", "amount").orderBy("order_id").show()
+--------+-----------+-----+------+
|order_id|customer_id| name|amount|
+--------+-----------+-----+------+
|      o1|         c1|Alice|    20|
|      o2|         c2|  Bob|    35|
|      o3|         c1|Alice|    15|
|      o4|         c3| Chen|    40|
|      o5|         c1|Alice|    25|
|      o7|         c2|  Bob|    30|
+--------+-----------+-----+------+
>>> joined.count()
6

>>> joined.groupBy("name").agg(F.sum("amount").alias("total")).orderBy("name").show()
+-----+-----+
| name|total|
+-----+-----+
|Alice|   60|
|  Bob|   65|
| Chen|   40|
+-----+-----+
```

**Notes:**

* Dana (`c4`, no orders) and order `o6` (`c5`, unknown customer) are **silently
  dropped**: an inner join keeps only matching keys.
* **Size rule:** a key with `m` rows on the left and `n` on the right gives `m × n`
  rows. `c1`: 3 orders × 1 customer = 3 rows. Duplicate keys on **both** sides can
  make the result explode.
* `on="customer_id"` (a column **name**) keeps **one** `customer_id` column. Compare
  4.6.
* RDD version: [4.1 `join()`](../rdd_transformations_review/README.md#41-join).

### 4.2 Left outer join

**Scenario:** customer service wants **every** customer, including those who never
ordered, to send them a coupon.

```python
>>> left = customers.join(orders, "customer_id", "left")
>>> left.select("customer_id", "name", "order_id", "amount").orderBy("customer_id", "order_id").show()
+-----------+-----+--------+------+
|customer_id| name|order_id|amount|
+-----------+-----+--------+------+
|         c1|Alice|      o1|    20|
|         c1|Alice|      o3|    15|
|         c1|Alice|      o5|    25|
|         c2|  Bob|      o2|    35|
|         c2|  Bob|      o7|    30|
|         c3| Chen|      o4|    40|
|         c4| Dana|    NULL|  NULL|
+-----------+-----+--------+------+

>>> left.filter(F.col("order_id").isNull()).select("name").show()
+----+
|name|
+----+
|Dana|
+----+
```

**Notes:**

* Every **left** row survives; columns from the right are `NULL` where there is no
  match.
* "Left join, then keep the rows where a right column is `NULL`" finds the missing
  matches. The **anti join** (4.5) does the same in one step.
* `NULL` in arithmetic gives `NULL`; in `sum()` it is skipped. Use
  `F.coalesce(col, F.lit(0))` to treat it as 0.
* RDD version: [4.2 `leftOuterJoin()`](../rdd_transformations_review/README.md#42-leftouterjoin).

### 4.3 Right and full outer joins

**Scenario:** accounting asks: are there orders whose customer does **not exist**? And
they want one report with **everything** from both sides.

```python
>>> (customers.join(orders, "customer_id", "right")
...     .filter(F.col("name").isNull()).select("order_id", "customer_id", "amount").show())
+--------+-----------+------+
|order_id|customer_id|amount|
+--------+-----------+------+
|      o6|         c5|    10|
+--------+-----------+------+

>>> full = customers.join(orders, "customer_id", "full")
>>> full.select("customer_id", "name", "order_id").orderBy("customer_id", "order_id").show()
+-----------+-----+--------+
|customer_id| name|order_id|
+-----------+-----+--------+
|         c1|Alice|      o1|
|         c1|Alice|      o3|
|         c1|Alice|      o5|
|         c2|  Bob|      o2|
|         c2|  Bob|      o7|
|         c3| Chen|      o4|
|         c4| Dana|    NULL|
|         c5| NULL|      o6|
+-----------+-----+--------+
```

**Notes:**

* `a.join(b, k, "right")` is the same as `b.join(a, k, "left")` (columns in a different
  order). Most people write the left version.
* When you join on a column **name**, the full join fills `customer_id` from whichever
  side has it (`c4` and `c5` both appear).
* RDD version: [4.3 `rightOuterJoin()` and `fullOuterJoin()`](../rdd_transformations_review/README.md#43-rightouterjoin-and-fullouterjoin).

### 4.4 Anti join: rows without a match

**Scenario:** which customers have **no** orders? Which orders have an **unknown**
customer?

```python
>>> customers.join(orders, "customer_id", "left_anti").show()
+-----------+----+------+
|customer_id|name|  city|
+-----------+----+------+
|         c4|Dana|Fresno|
+-----------+----+------+
>>> orders.join(customers, "customer_id", "left_anti").show()
+-----------+--------+--------+------+
|customer_id|order_id|category|amount|
+-----------+--------+--------+------+
|         c5|      o6|   music|    10|
+-----------+--------+--------+------+
```

**Notes:**

* `left_anti` returns only **left** columns and adds nothing from the right.
* This is the standard **data-quality check** after loading data.
* RDD version: [4.4 `subtractByKey()`](../rdd_transformations_review/README.md#44-subtractbykey).

### 4.5 Semi join: rows with a match

**Scenario:** which customers have **at least one** order? List each customer once.

```python
>>> customers.join(orders, "customer_id", "left_semi").orderBy("customer_id").show()
+-----------+-----+--------+
|customer_id| name|    city|
+-----------+-----+--------+
|         c1|Alice|San Jose|
|         c2|  Bob| Oakland|
|         c3| Chen|San Jose|
+-----------+-----+--------+
>>> customers.join(orders, "customer_id", "inner").count()        # compare: one row per order
6
```

**Notes:**

* `left_semi` keeps each left row **at most once**, however many matches it has, and
  adds no right columns. It is SQL's `WHERE EXISTS` / `IN (subquery)`.

### 4.6 Joining on columns with different names (re-keying)

**Scenario:** a `cities` table calls its key `cust` instead of `customer_id`.

**How it works:** give a **condition** instead of a name. Both key columns are then
kept, so drop one (or use aliases).

```python
>>> regions = spark.createDataFrame(
...     [("San Jose", "South Bay"), ("Oakland", "East Bay"), ("Fresno", "Central Valley")],
...     "town STRING, region STRING")

>>> (customers.join(regions, customers.city == regions.town, "left")
...     .drop("town").orderBy("customer_id").show())
+-----------+-----+--------+--------------+
|customer_id| name|    city|        region|
+-----------+-----+--------+--------------+
|         c1|Alice|San Jose|     South Bay|
|         c2|  Bob| Oakland|      East Bay|
|         c3| Chen|San Jose|     South Bay|
|         c4| Dana|  Fresno|Central Valley|
+-----------+-----+--------+--------------+
```

**Notes:**

* With a condition, **both** key columns stay. If they have the **same** name (e.g.
  `orders.customer_id == customers.customer_id`), `select("customer_id")` fails with
  `AMBIGUOUS_REFERENCE`: drop one copy (`.drop(customers.customer_id)`) or use aliases.
* With RDDs, you re-keyed with `map()` so the join field became element `[0]`. With
  DataFrames you just write the condition.
* RDD version: [4.6 Re-keying](../rdd_transformations_review/README.md#46-re-keying-joining-on-a-different-field).

### 4.7 Self-join

**Scenario:** which customers live in the **same city**?

**How it works:** join a DataFrame with itself under two **aliases**, so every column
name can be qualified.

```python
>>> c1 = customers.alias("a")
>>> c2 = customers.alias("b")
>>> (c1.join(c2, (F.col("a.city") == F.col("b.city")) & (F.col("a.customer_id") < F.col("b.customer_id")))
...    .select(F.col("a.name").alias("name_1"), F.col("b.name").alias("name_2"), F.col("a.city"))
...    .show())
+------+------+--------+
|name_1|name_2|    city|
+------+------+--------+
| Alice|  Chen|San Jose|
+------+------+--------+
```

**Notes:**

* `a.customer_id < b.customer_id` removes pairs of a customer with **themselves** and
  lists each pair **once** (not both Alice–Chen and Chen–Alice).
* RDD version: [4.7 Self-join](../rdd_transformations_review/README.md#47-self-join).

### 4.8 Broadcast join

**Scenario:** `orders` is huge, `customers` is small. Avoid shuffling the orders.

**How it works:** `F.broadcast(small_df)` sends a full copy of the small table to every
executor, so each partition of the big table is joined **locally**, with no shuffle of
the big table. Spark does this automatically for tables under
`spark.sql.autoBroadcastJoinThreshold` (10 MB by default).

```python
>>> orders.join(F.broadcast(customers), "customer_id").count()
6
```

**Notes:**

* The physical plan (`.explain()`) then shows `BroadcastHashJoin` instead of
  `SortMergeJoin`.
* The RDD version was a broadcast variable holding a dict, used inside `map()`.
* RDD version: [4.8 Broadcast (map-side) join](../rdd_transformations_review/README.md#48-broadcast-map-side-join).

### 4.9 `crossJoin()`: every pair

**Scenario:** a coupon for **every** customer × **every** category.

```python
>>> cats = orders.select("category").distinct()
>>> grid = customers.select("name").crossJoin(cats)
>>> grid.count()
12
>>> grid.orderBy("name", "category").show(4)
+-----+--------+
| name|category|
+-----+--------+
|Alice|   books|
|Alice|   games|
|Alice|   music|
|  Bob|   books|
+-----+--------+
only showing top 4 rows
```

**Notes:**

* 4 customers × 3 categories = 12 rows. On big tables a cross join explodes: use it
  only on small ones.
* RDD version: [4.9 `cartesian()`](../rdd_transformations_review/README.md#49-cartesian).

### 4.10 Set operations: `unionByName()`, `intersect()`, `subtract()`

**Scenario:** customers who bought **books**, customers who bought **games**: who
bought both? Books but not games? Either?

```python
>>> books = orders.filter(F.col("category") == "books").select("customer_id")
>>> games = orders.filter(F.col("category") == "games").select("customer_id")

>>> books.intersect(games).orderBy("customer_id").show()
+-----------+
|customer_id|
+-----------+
|         c1|
|         c2|
+-----------+
>>> books.subtract(games).orderBy("customer_id").show()
+-----------+
|customer_id|
+-----------+
|         c3|
+-----------+
>>> books.unionByName(games).count(), books.unionByName(games).distinct().count()
(6, 3)
```

The **trap**: `union()` matches columns by **position**, not by name:

```python
>>> a = spark.createDataFrame([("c1", "Alice")], "customer_id STRING, name STRING")
>>> b = spark.createDataFrame([("Bob", "c2")], "name STRING, customer_id STRING")
>>> a.union(b).show()
+-----------+-----+
|customer_id| name|
+-----------+-----+
|         c1|Alice|
|        Bob|   c2|
+-----------+-----+
>>> a.unionByName(b).show()
+-----------+-----+
|customer_id| name|
+-----------+-----+
|         c1|Alice|
|         c2|  Bob|
+-----------+-----+
```

**Notes:**

* `union()` / `unionByName()` keep **duplicates** (like RDD `union()` and SQL
  `UNION ALL`); add `.distinct()` to remove them.
* `intersect()` and `subtract()` remove duplicates; `exceptAll()` / `intersectAll()`
  keep them.
* Set operations compare **whole rows**: `select()` the columns that matter first.
* RDD version: [4.10 Set operations](../rdd_transformations_review/README.md#410-set-operations-union-intersection-subtract).

---

## Part 5: Putting it all together

**Scenario:** from the raw order file (with a header and a bad line), find the total
revenue **per city**, biggest first. Same problem and same answer as
[Part 5 of the RDD review](../rdd_transformations_review/README.md#part-5-putting-it-all-together).

`spark.read.csv()` can read an RDD of text lines (or a file path):

```python
>>> raw_lines = sc.parallelize([
...     "order_id,customer_id,category,amount",
...     "o1,c1,books,20", "o2,c2,games,35", "o3,c1,games,15",
...     "o4,c3,books,40", "o5,c1,books,25", "o6,c5,music,10",
...     "o7,c2,books,30", "o8,c3,books,oops",
... ], 2)

>>> city_revenue = (spark.read.csv(raw_lines, header=True)          # header line -> column names
...     .withColumn("amount", F.col("amount").try_cast("int"))    # "oops" -> NULL
...     .dropna(subset=["amount"])                                 # drop the bad line
...     .groupBy("customer_id").agg(F.sum("amount").alias("total"))  # total per customer
...     .join(customers, "customer_id")                            # inner: drops c5
...     .groupBy("city").agg(F.sum("total").alias("revenue"))      # total per city
...     .orderBy(F.desc("revenue"), "city"))
>>> city_revenue.show()
+--------+-------+
|    city|revenue|
+--------+-------+
|San Jose|    100|
| Oakland|     65|
+--------+-------+
>>> [tuple(r) for r in city_revenue.collect()]
[('San Jose', 100), ('Oakland', 65)]
```

**Notes:**

* Without `inferSchema` or a schema, every CSV column is a **string**; that is why
  `try_cast` is needed (and it also catches `"oops"`).
* Aggregating per customer **before** the join is the same deliberate choice as in the
  RDD version (less data to join). Spark's optimizer would often find good plans
  anyway, but the habit is right.
* Nothing ran until `show()`: everything before it only built the plan.

---

## Part 6: Cheat sheet

`T` = transformation (returns a DataFrame, lazy). `A` = action (returns a value, runs
a job). Wide = causes a shuffle.

| Operation | T/A | Wide? | Does | RDD counterpart |
|---|---|---|---|---|
| `select(...)` / `selectExpr(...)` | T | no | build each row from expressions | `map` |
| `withColumn(n, e)` / `withColumns({...})` | T | no | add / replace column(s) | `map` |
| `withColumnRenamed(a, b)` / `drop(c)` | T | no | rename / remove columns | `map` |
| `F.when(c, v).otherwise(w)` | expr | | if/else | `if` in a lambda |
| `F.explode(F.split(c, p))` | expr | | one row per element | `flatMap` |
| `F.col(c).try_cast(t)` | expr | | convert, bad → `NULL` | `int()` in `try` |
| `filter(cond)` / `where(cond)` | T | no | keep matching rows | `filter` |
| `dropna(...)` / `fillna(...)` | T | no | drop / fill `NULL`s | `filter` / `map` |
| `distinct()` / `dropDuplicates([...])` | T | **yes** | remove duplicates | `distinct` |
| `limit(n)` / `sample(f, seed)` | T | no | first n / random subset | `take` / `sample` |
| `orderBy(...)` / `sort(...)` | T | **yes** | sort | `sortBy` / `sortByKey` |
| `agg(...)` | T | **yes** | one row for the whole data | `reduce` / `aggregate` |
| `groupBy(k).agg(...)` | T | **yes** | one row per group | `reduceByKey` / `aggregateByKey` |
| `F.collect_list` / `F.collect_set` | expr | | values into an array | `groupByKey` |
| `groupBy(k).pivot(c).agg(...)` | T | **yes** | key × key table | (by hand) |
| `F.xxx().over(window)` | expr | **yes** | per-row group result, ranking | `groupByKey` + Python |
| `join(b, on, "inner")` | T | **yes**\* | matching rows | `join` |
| `join(b, on, "left" / "right" / "full")` | T | **yes**\* | keep unmatched rows, `NULL`s | `leftOuterJoin` / ... |
| `join(b, on, "left_anti")` | T | **yes**\* | left rows without a match | `subtractByKey` |
| `join(b, on, "left_semi")` | T | **yes**\* | left rows with a match | (filter by keys) |
| `crossJoin(b)` | T | **yes** | every pair | `cartesian` |
| `unionByName(b)` | T | no | rows of both, duplicates kept | `union` |
| `intersect(b)` / `subtract(b)` | T | **yes** | set operations | `intersection` / `subtract` |
| `show(n)` | A | | print rows | `take` + print |
| `count()` | A | | number of rows | `count` |
| `collect()` / `take(n)` / `first()` | A | | `Row` objects | same names |
| `write.parquet(p)` / `write.csv(p)` | A | | save | `saveAsTextFile` |

\* No shuffle of the big table when the small one is **broadcast**.

---

## Part 7: Exam traps

1. **Transformations are lazy.** Nothing runs until an action (`show`, `count`,
   `collect`, `write`). Errors in an expression (a wrong column name) appear when the
   DataFrame is built; errors in the **data** (a bad cast) appear only at the action.
2. **Use `&`, `|`, `~` with parentheses**, never `and` / `or` / `not`.
3. **`== None` does not find missing values.** Use `isNull()` / `isNotNull()`.
4. **ANSI mode (Spark 4):** `cast("int")` of bad text **fails the job**; use `try_cast`.
   CSV columns are strings unless you give a schema or `inferSchema=True`.
5. **`select()` keeps only what you list.** Use `withColumn()` or `select("*", ...)` to
   keep the rest.
6. **`withColumn()` with an existing name replaces the column** silently.
7. **`split("")` gives `[""]`, not `[]`**: `explode` then yields one empty word.
8. **`explode()` drops rows whose array is empty or `NULL`**; `explode_outer()` keeps them.
9. **`groupBy()` alone is not a result**: call `agg()` / `count()` / ... on it.
10. **`agg()` returns a DataFrame**, not a number: use `first()[0]`.
11. **Aggregates skip `NULL`s** (`avg` of 12, 18, `NULL` is 15.0); `count("*")` does not.
12. **Filter before `groupBy` = WHERE, after `agg` = HAVING.** They answer different
    questions.
13. **Group order is not guaranteed**: end with `orderBy()`, with a tie-breaker.
14. **Inner joins drop unmatched rows silently**; `m × n` rows per key.
15. **Joining on a condition keeps both key columns** (ambiguous names); joining on a
    column name keeps one.
16. **`left_semi` / `left_anti` return only left columns.**
17. **`union()` matches columns by position**: use `unionByName()`. Union keeps
    duplicates.
18. **`take(n)` is not top-n**: sort first. **`collect()` brings all rows to the driver.**

---

## Part 8: Test yourself

Predict the output, then open the answer. All of them use the data from this page.

**Q1.** How many rows does this return, and why?

```text
reviews.select(F.explode(F.split("text", " "))).count()
```

<details><summary>Answer</summary>

```python
>>> reviews.select(F.explode(F.split("text", " "))).count()
8
```

`"great book"` gives 2, `"too long"` gives 2, `""` gives **1** (an empty string: `split`
returns `[""]`), `"great great game"` gives 3. That is 8, not 7 as with RDD
`flatMap(lambda r: r.split())`.

</details>

**Q2.** Number of orders per category, sorted by category, using `orders`, `groupBy()`
and `count()`.

<details><summary>Answer</summary>

```python
>>> orders.groupBy("category").count().orderBy("category").show()
+--------+-----+
|category|count|
+--------+-----+
|   books|    4|
|   games|    2|
|   music|    1|
+--------+-----+
```

</details>

**Q3.** What is the difference between these two results?

```text
orders.filter(F.col("amount") > 20).groupBy("customer_id").count()
orders.groupBy("customer_id").count().filter(F.col("count") > 1)
```

<details><summary>Answer</summary>

```python
>>> orders.filter(F.col("amount") > 20).groupBy("customer_id").count().orderBy("customer_id").show()
+-----------+-----+
|customer_id|count|
+-----------+-----+
|         c1|    1|
|         c2|    2|
|         c3|    1|
+-----------+-----+
>>> orders.groupBy("customer_id").count().filter(F.col("count") > 1).orderBy("customer_id").show()
+-----------+-----+
|customer_id|count|
+-----------+-----+
|         c1|    3|
|         c2|    2|
+-----------+-----+
```

The first filters **orders** (WHERE): it counts, per customer, the orders over 20. The
second filters **groups** (HAVING): customers with more than one order.

</details>

**Q4.** What is the average amount of the `messy` DataFrame (from 2.2), and why is it
not 10?

<details><summary>Answer</summary>

```python
>>> messy.agg(F.avg("amount")).first()[0]
15.0
```

`avg` skips the `NULL`: `(12 + 18) / 2 = 15.0`. Treating the missing amount as 0 would
give `30 / 3 = 10.0`:

```python
>>> messy.agg(F.avg(F.coalesce(F.col("amount"), F.lit(0)))).first()[0]
10.0
```

</details>

**Q5.** Each customer's **name** and **number of orders**, including customers with
zero orders.

<details><summary>Answer</summary>

```python
>>> (customers.join(orders, "customer_id", "left")
...     .groupBy("name").agg(F.count("order_id").alias("orders"))
...     .orderBy("name").show())
+-----+------+
| name|orders|
+-----+------+
|Alice|     3|
|  Bob|     2|
| Chen|     1|
| Dana|     0|
+-----+------+
```

`F.count("order_id")` skips the `NULL` that the left join produced for Dana, so she
gets 0. `F.count("*")` would wrongly give her 1.

</details>

**Q6.** How many rows does `orders.join(customers, "customer_id", "full")` have?

<details><summary>Answer</summary>

```python
>>> orders.join(customers, "customer_id", "full").count()
8
```

6 matched order rows (`o1`–`o5`, `o7`), plus `o6` (unknown customer `c5`), plus Dana
(`c4`, no orders) = 8.

</details>

**Q7.** Which customers ordered from **more than one** category?

<details><summary>Answer</summary>

```python
>>> (orders.groupBy("customer_id").agg(F.countDistinct("category").alias("n"))
...     .filter(F.col("n") > 1).orderBy("customer_id").show())
+-----------+---+
|customer_id|  n|
+-----------+---+
|         c1|  2|
|         c2|  2|
+-----------+---+
```

</details>

**Q8.** For each city of the customers, the **total** amount ordered, with cities that
have no orders shown as 0. Sort by city.

<details><summary>Answer</summary>

```python
>>> (customers.join(orders, "customer_id", "left")
...     .groupBy("city").agg(F.coalesce(F.sum("amount"), F.lit(0)).alias("total"))
...     .orderBy("city").show())
+--------+-----+
|    city|total|
+--------+-----+
|  Fresno|    0|
| Oakland|   65|
|San Jose|  100|
+--------+-----+
```

Fresno's only customer (Dana) has no orders: `sum` of only `NULL`s is `NULL`, and
`coalesce` turns it into 0.

</details>

**Q9.** Each customer's **second** biggest order (customers with only one order don't
appear).

<details><summary>Answer</summary>

```python
>>> w = Window.partitionBy("customer_id").orderBy(F.desc("amount"), "order_id")
>>> (orders.withColumn("rn", F.row_number().over(w))
...     .filter(F.col("rn") == 2).select("customer_id", "order_id", "amount")
...     .orderBy("customer_id").show())
+-----------+--------+------+
|customer_id|order_id|amount|
+-----------+--------+------+
|         c1|      o1|    20|
|         c2|      o7|    30|
+-----------+--------+------+
```

</details>

**Q10.** Which of these cause a shuffle?
`select`, `filter`, `groupBy().agg()`, `withColumn`, `join` (two large tables),
`unionByName`, `distinct`, `explode`, `orderBy`

<details><summary>Answer</summary>

`groupBy().agg()`, `join`, `distinct`, `orderBy`. The others (`select`, `filter`,
`withColumn`, `unionByName`, `explode`) are narrow. (A join with a small, **broadcast**
table does not shuffle the big one; see [4.8](#48-broadcast-join).)

</details>

---

## Want more practice?

* [`../rdd_transformations_review/`](../rdd_transformations_review): the RDD version of
  this review, same data, same order of topics.
* [`../dataframes_practice_questions/`](../dataframes_practice_questions): 10 sets of
  DataFrame practice questions with full answers (Set 10 is an exam-style mixed review).
* [`../dataframes_notebooks/`](../dataframes_notebooks): five marimo notebooks that teach
  DataFrames step by step, starting from RDDs.
