# Printing Presorted Mail: DataFrame Version

## 1. Introduction

This is the DataFrame companion to
[A Real-Life Case for `sortByKey()`: Printing Presorted Mail](sortbykey_presorted_mail.md).
It uses the same statements, so you can compare the two APIs side by side.

**The job:** a credit-card company prints a paper statement for every
customer. The post office gives a discount for mail that arrives **in ZIP
code order**, and the print shop has **3 printers**, each printing one
**contiguous range** of ZIP codes (printer 1 the lowest, printer 3 the
highest).

| Input: statements, in billing order | Output: the **same** statements, in ZIP code order, in 3 ranges |
|---|---|
| `95050, Chen, 120.00` | printer 1: `02138 Murphy`, `10001 Okafor`, ... |
| `10001, Okafor, 45.10` | printer 2: ..., `95050 Chen`, ... |
| `98101, Larsen, 9.50` | printer 3: `98101 Larsen`, `98101 Andersen` |
| ... | |

### From `sortByKey()` to DataFrames

`sortByKey(numPartitions=3)` did two things at once: it **sorted** the
records, and it split them into **3 contiguous key ranges**, one per
partition. With DataFrames these are two separate questions, and each has
its own method:

| What the print shop needs | RDD | DataFrame |
|---|---|---|
| All statements in ZIP code order | `sortByKey()` | `orderBy("zip_code")` |
| ... split into 3 ZIP ranges, one per printer | `sortByKey(numPartitions=3)` | `repartitionByRange(3, "zip_code").sortWithinPartitions("zip_code")` |


## 2. Sample data

Start the PySpark shell (`./bin/pyspark`). ZIP codes are 5-character
**strings**, so string order is ZIP code order (and `'02138'` keeps its
leading zero):

~~~python
>>> spark.version
'4.2.0'

>>> from pyspark.sql import functions as F

>>> statements = [
...     ('95050', 'Chen',       120.00),
...     ('10001', 'Okafor',      45.10),
...     ('98101', 'Larsen',       9.50),
...     ('02138', 'Murphy',     310.25),
...     ('60601', 'Kowalski',    88.00),
...     ('95050', 'Alvarez',     64.75),
...     ('30301', 'Washington', 150.00),
...     ('94105', 'Nakamura',   210.40),
...     ('10001', 'Bernstein',   19.99),
...     ('73301', 'Hernandez',   75.00),
...     ('95050', 'Baker',       33.30),
...     ('98101', 'Andersen',    99.00),
... ]

>>> statements_df = spark.createDataFrame(
...     statements, "zip_code STRING, customer STRING, amount_due DOUBLE")
>>> statements_df.printSchema()
root
 |-- zip_code: string (nullable = true)
 |-- customer: string (nullable = true)
 |-- amount_due: double (nullable = true)
<BLANKLINE>
>>> statements_df.count()
12
~~~

(The ZIP codes are real: Cambridge MA, New York, Atlanta, Chicago, Austin,
San Francisco, Santa Clara, Seattle. The customers and amounts are made up.)

> **About `<BLANKLINE>`:** `printSchema()` and `show()` print an empty line at
> the end. In the shell you just see a blank line; `<BLANKLINE>` marks it here.


## 3. All statements in ZIP code order: `orderBy()`

~~~python
>>> by_zip = statements_df.orderBy("zip_code")
>>> [row.zip_code for row in by_zip.collect()]
['02138', '10001', '10001', '30301', '60601', '73301', '94105', '95050', '95050', '95050', '98101', '98101']
~~~

Like `sortByKey()`, `orderBy()` is a **distributed** sort: Spark samples the
keys, sends each row to the partition for its key range (a shuffle with a
*range* partitioner), and sorts each partition in parallel.

As with `sortByKey()`, only the key is sorted. `Okafor` and `Bernstein`
(both in `10001`) are in **no particular order**, and that order can change
from one run to the next. (It did while this page was being tested, which
is why we only print the ZIP codes here.) When the order of ties matters,
add a tie-breaker column, as in the next section.

But how many printers did we get?

~~~python
>>> by_zip.rdd.getNumPartitions()
1
~~~

**One.** `orderBy()` doesn't take a number of partitions. Its shuffle uses
`spark.sql.shuffle.partitions` (200 by default), and then Adaptive Query
Execution (AQE) notices the data is tiny and **merges** the partitions,
here into a single one. That's a sensible choice for speed, but it is
Spark's choice, not the print shop's. With millions of statements you
would get some other number. `orderBy()` promises the **order**, not the
**partitions**.


## 4. One ZIP range per printer: `repartitionByRange()`

When the partitions themselves matter (here, one per printer), say so:

* `repartitionByRange(3, "zip_code")`: split the rows into exactly **3
  contiguous ranges** of ZIP codes (the shuffle part of `sortByKey()`);
* `sortWithinPartitions("zip_code", "customer")`: sort **each** range on its
  own, with no further shuffle (the second part of `sortByKey()`). We add
  `customer` as a tie-breaker, so each printer's stack has a fixed order
  (by name within a ZIP code) on every run.

`F.spark_partition_id()` tells us which partition each row is in. That's
the printer number (plus 1):

~~~python
>>> batches = (statements_df
...            .repartitionByRange(3, "zip_code")
...            .sortWithinPartitions("zip_code", "customer"))
>>> batches.rdd.getNumPartitions()
3

>>> printed = batches.withColumn("printer", F.spark_partition_id() + 1)
>>> printed.show()
+--------+----------+----------+-------+
|zip_code|  customer|amount_due|printer|
+--------+----------+----------+-------+
|   02138|    Murphy|    310.25|      1|
|   10001| Bernstein|     19.99|      1|
|   10001|    Okafor|      45.1|      1|
|   30301|Washington|     150.0|      1|
|   60601|  Kowalski|      88.0|      2|
|   73301| Hernandez|      75.0|      2|
|   94105|  Nakamura|     210.4|      2|
|   95050|   Alvarez|     64.75|      2|
|   95050|     Baker|      33.3|      2|
|   95050|      Chen|     120.0|      2|
|   98101|  Andersen|      99.0|      3|
|   98101|    Larsen|       9.5|      3|
+--------+----------+----------+-------+
<BLANKLINE>

>>> (printed.groupBy("printer")
...         .agg(F.count("*").alias("statements"),
...              F.min("zip_code").alias("from_zip"),
...              F.max("zip_code").alias("to_zip"))
...         .orderBy("printer").show())
+-------+----------+--------+------+
|printer|statements|from_zip|to_zip|
+-------+----------+--------+------+
|      1|         4|   02138| 30301|
|      2|         6|   60601| 95050|
|      3|         2|   98101| 98101|
+-------+----------+--------+------+
<BLANKLINE>
~~~

Three contiguous ranges (the range split uses `zip_code` only, so a ZIP
code is never split between two printers), in order, and each one sorted.
Written with
`batches.write.csv(...)`, they would become three files, `part-00000`,
`part-00001`, `part-00002`, one per printer.

The batches are not the same as in the RDD version (`5, 5, 2` there,
`4, 6, 2` here). Both are correct: they are just two ways of picking the
boundaries from the same sorted keys. PySpark's `sortByKey()` picks the keys
at 1/3 and 2/3 of the sorted sample (`'60601'` and `'95050'`). Spark SQL's
range partitioner aims for about 12 / 3 = 4 rows per range: it closes range
1 after `30301` (4 rows), and range 2 after `95050`, because all three
`95050` statements must stay together.


## 5. Sorting by more than one field, descending, and top N

### Within a ZIP code, by customer name

`orderBy()` takes several columns: sort by the first, break ties with the
next (the RDD version needed a tuple key for this). Unlike section 3, this
order is the same on every run:

~~~python
>>> statements_df.orderBy("zip_code", "customer").show()
+--------+----------+----------+
|zip_code|  customer|amount_due|
+--------+----------+----------+
|   02138|    Murphy|    310.25|
|   10001| Bernstein|     19.99|
|   10001|    Okafor|      45.1|
|   30301|Washington|     150.0|
|   60601|  Kowalski|      88.0|
|   73301| Hernandez|      75.0|
|   94105|  Nakamura|     210.4|
|   95050|   Alvarez|     64.75|
|   95050|     Baker|      33.3|
|   95050|      Chen|     120.0|
|   98101|  Andersen|      99.0|
|   98101|    Larsen|       9.5|
+--------+----------+----------+
<BLANKLINE>
~~~

The printer batches in section 4 did the same inside each range, while
keeping the range split on ZIP code only.

### The largest amounts due: `orderBy(desc(...)).limit(n)`

The collections department wants the 3 largest amounts due. With RDDs that
was `takeOrdered()`. With DataFrames, write the sort and the limit, and
Spark plans them together:

~~~python
>>> top3 = statements_df.orderBy(F.desc("amount_due")).limit(3)
>>> top3.show()
+--------+----------+----------+
|zip_code|  customer|amount_due|
+--------+----------+----------+
|   02138|    Murphy|    310.25|
|   94105|  Nakamura|     210.4|
|   30301|Washington|     150.0|
+--------+----------+----------+
<BLANKLINE>

>>> import contextlib, io
>>> def plan_of(df):
...     buf = io.StringIO()
...     with contextlib.redirect_stdout(buf):
...         df.explain()
...     return buf.getvalue()
...
>>> "TakeOrderedAndProject" in plan_of(top3)
True
~~~

`TakeOrderedAndProject` is `takeOrdered()`: each partition keeps only its own
top 3, and Spark never sorts the whole table. So `orderBy().limit(n)` is
cheap even on huge data.

### Spark SQL

~~~python
>>> statements_df.createOrReplaceTempView("statements")
>>> spark.sql("""
...     SELECT zip_code, customer, amount_due
...     FROM statements
...     ORDER BY amount_due DESC
...     LIMIT 3
... """).show()
+--------+----------+----------+
|zip_code|  customer|amount_due|
+--------+----------+----------+
|   02138|    Murphy|    310.25|
|   94105|  Nakamura|     210.4|
|   30301|Washington|     150.0|
+--------+----------+----------+
<BLANKLINE>
~~~


## 6. A common mistake: letting Spark guess the column types

Statements usually arrive as a **CSV file**. A common habit is to read it
with `inferSchema=True` and let Spark guess the types. Here is our data as
CSV text (`spark.read.csv()` also accepts an RDD of CSV lines, so we don't
need a file):

~~~python
>>> lines = ["zip_code,customer,amount_due"] + \
...         [f"{z},{c},{a:.2f}" for z, c, a in statements]
>>> lines[:3]
['zip_code,customer,amount_due', '95050,Chen,120.00', '10001,Okafor,45.10']

>>> guessed = spark.read.csv(spark.sparkContext.parallelize(lines),
...                          header=True, inferSchema=True)
>>> guessed.printSchema()
root
 |-- zip_code: integer (nullable = true)
 |-- customer: string (nullable = true)
 |-- amount_due: double (nullable = true)
<BLANKLINE>
>>> guessed.orderBy("zip_code", "customer").show(3)
+--------+---------+----------+
|zip_code| customer|amount_due|
+--------+---------+----------+
|    2138|   Murphy|    310.25|
|   10001|Bernstein|     19.99|
|   10001|   Okafor|      45.1|
+--------+---------+----------+
only showing top 3 rows
~~~

Spark saw only digits in `zip_code` and guessed **integer**. The order is
still right (all ZIP codes have 5 digits), but `02138` became `2138`, and
Mr. Murphy's statement would be printed with a 4-digit ZIP code that the
post office can't deliver. A ZIP code is a **label** that happens to use
digits, not a number.

The opposite mistake: read the file **without** `inferSchema`, and every
column is a string, including the amounts. Now "largest amount first" goes
wrong:

~~~python
>>> as_text = spark.read.csv(spark.sparkContext.parallelize(lines), header=True)
>>> as_text.orderBy(F.desc("amount_due")).show(5)
+--------+---------+----------+
|zip_code| customer|amount_due|
+--------+---------+----------+
|   98101| Andersen|     99.00|
|   98101|   Larsen|      9.50|
|   60601| Kowalski|     88.00|
|   73301|Hernandez|     75.00|
|   95050|  Alvarez|     64.75|
+--------+---------+----------+
only showing top 5 rows
~~~

Strings are compared character by character, and `'9'` is the largest first
character, so `'99.00'` and `'9.50'` come first and the real largest amount,
`'310.25'`, is nowhere in the top 5.

The fix for both: **give the schema yourself**, with the type that has the
order you want:

~~~python
>>> typed = spark.read.csv(spark.sparkContext.parallelize(lines), header=True,
...                        schema="zip_code STRING, customer STRING, amount_due DOUBLE")
>>> typed.orderBy("zip_code", "customer").show(3)
+--------+---------+----------+
|zip_code| customer|amount_due|
+--------+---------+----------+
|   02138|   Murphy|    310.25|
|   10001|Bernstein|     19.99|
|   10001|   Okafor|      45.1|
+--------+---------+----------+
only showing top 3 rows

>>> typed.orderBy(F.desc("amount_due")).select("amount_due").limit(5).collect() == \
...     statements_df.orderBy(F.desc("amount_due")).select("amount_due").limit(5).collect()
True
~~~


## 7. Takeaways

| Real-life case | RDD | DataFrame |
|---|---|---|
| [Package tracking](reducebykey_package_tracking_dataframe.md): the current scan per package | `reduceByKey(later_scan)` | `groupBy().agg(max(struct(time, ...)))` or `max_by(col, time)` |
| [Election-night vote totals](foldbykey_election_results_dataframe.md) per county | `foldByKey({}, add_tallies)` | `explode()` each tally into rows, then `groupBy().agg(sum())` |
| [Median home price](groupbykey_median_home_prices_dataframe.md) per zip code | `groupByKey()` + `mapValues(median)` | `groupBy().agg(median())` |
| [Star ratings](aggregatebykey_product_ratings_dataframe.md) per product | `aggregateByKey([0, 0, 0, 0, 0], add_review, merge_boxes)` | `groupBy().agg(count(when(stars == k, 1)))` for each star level |
| [Customer profiles](combinebykey_customer_profile_dataframe.md) from an order log | `combineByKey(start_profile, add_order, merge_profiles)` | `groupBy().agg(count(), sum(), min(), max(), mode())` |
| [Presorted mail](sortbykey_presorted_mail_dataframe.md) by ZIP code | `sortByKey(numPartitions=3)` | `repartitionByRange(3, "zip_code").sortWithinPartitions("zip_code")` |

Rules to remember from this example:

* `orderBy()` promises the **order** of the rows, not the number of
  partitions (AQE may merge them).
* When the partitions themselves matter (one file per printer), use
  `repartitionByRange(n, key)` + `sortWithinPartitions(key)`.
* `orderBy(...).limit(n)` is the DataFrame `takeOrdered()`: it never sorts
  the whole table.
* Give CSV files an explicit schema: ZIP codes, phone numbers and account
  numbers are strings; amounts are numbers.

A runnable version of this example is
[`sortbykey_presorted_mail_dataframe.py`](sortbykey_presorted_mail_dataframe.py)
(output of a real run: [`.log`](sortbykey_presorted_mail_dataframe.log)).
