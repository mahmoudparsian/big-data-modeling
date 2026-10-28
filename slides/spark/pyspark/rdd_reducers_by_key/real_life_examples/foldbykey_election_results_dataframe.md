# Election-Night Vote Totals: DataFrame Version

## 1. Introduction

This is the DataFrame companion to
[A Real-Life Case for `foldByKey()`: Election-Night Vote Totals](foldbykey_election_results.md).
It uses the same precinct reports, so you can compare the two APIs side by
side. (All vote counts are made up.)

**The job:** every precinct sends its **tally**, `{candidate: votes}`, and the
election office publishes a running total for every county:

```
Santa Clara   Patel 1,385   Lee 1,235   Rivera 348   write-in 4
```

| Input: one row per **precinct** | Output: the county's tally |
|---|---|
| `Santa Clara, {Lee: 410, Patel: 385, Rivera: 120}` | `Santa Clara, {Lee: 1235, Patel: 1385, Rivera: 348, write-in: 4}` |
| `Santa Clara, {Lee: 530, Patel: 760, Rivera: 140}` | |
| `Santa Clara, {Lee: 295, Patel: 240, Rivera: 88, write-in: 4}` | |

### From `foldByKey()` to DataFrames

With RDDs, a tally was a Python dictionary, and we wrote `add_tallies` to add
two dictionaries candidate by candidate, starting from the empty tally `{}`.

DataFrames have no built-in aggregate that "adds two dictionaries". Instead,
the DataFrame way is to **change the shape of the data**: turn each tally into
one row per candidate, `(county, candidate, votes)`. Then "add the tallies
candidate by candidate" is just a `sum()` per `(county, candidate)`:

| Business rule | RDD | DataFrame |
|---|---|---|
| The empty tally, before any votes are counted | `zeroValue = {}` | nothing to write: a `(county, candidate)` with no rows has no votes |
| Add two tallies, candidate by candidate | `add_tallies` | `explode()` the tally into rows, then `groupBy("county", "candidate").agg(sum("votes"))` |

This is a general rule for DataFrames: a **long** table (one row per fact)
is easier to aggregate than a column of dictionaries.


## 2. Sample data

Start the PySpark shell (`./bin/pyspark`). The tally column is a
`MAP<STRING, INT>`, Spark's type for a dictionary:

~~~python
>>> spark.version
'4.2.0'

>>> from pyspark.sql import functions as F

>>> reports = [
...     ('Santa Clara', {'Lee': 410, 'Patel': 385, 'Rivera': 120}),
...     ('Alameda',     {'Lee': 700, 'Patel': 640, 'Rivera': 300}),
...     ('Santa Clara', {'Lee': 530, 'Patel': 760, 'Rivera': 140}),
...     ('Alameda',     {'Lee': 510, 'Patel': 560, 'Rivera': 210, 'write-in': 7}),
...     ('Santa Clara', {'Lee': 295, 'Patel': 240, 'Rivera': 88, 'write-in': 4}),
...     ('San Mateo',   {'Lee': 220, 'Patel': 260, 'Rivera': 95}),
...     ('San Mateo',   {'Lee': 180, 'Patel': 150, 'Rivera': 60}),
...     ('San Mateo',   {'Lee': 140, 'Patel': 300}),
... ]

>>> reports_df = spark.createDataFrame(reports, "county STRING, tally MAP<STRING, INT>")
>>> reports_df.printSchema()
root
 |-- county: string (nullable = true)
 |-- tally: map (nullable = true)
 |    |-- key: string
 |    |-- value: integer (valueContainsNull = true)
<BLANKLINE>
>>> reports_df.count()
8
~~~

> **About `<BLANKLINE>`:** `printSchema()` and `show()` print an empty line at
> the end. In the shell you just see a blank line; `<BLANKLINE>` marks it here.

> **About `orderBy()`:** after a `groupBy()`, the order of the rows is not
> guaranteed. The examples use `orderBy()` before `show()` so the output is
> always the same. (We don't `show()` the raw map column: Spark doesn't
> promise an order for the entries of a map either.)


## 3. Step 1: one row per candidate per precinct, with `explode()`

`F.explode()` on a map column turns **each entry** of the map into its own
row, with two new columns: the key and the value:

~~~python
>>> votes = reports_df.select("county", F.explode("tally").alias("candidate", "votes"))
>>> votes.printSchema()
root
 |-- county: string (nullable = true)
 |-- candidate: string (nullable = false)
 |-- votes: integer (nullable = true)
<BLANKLINE>
>>> votes.count()
25

>>> votes.filter("county = 'Santa Clara'").orderBy("candidate", "votes").show()
+-----------+---------+-----+
|     county|candidate|votes|
+-----------+---------+-----+
|Santa Clara|      Lee|  295|
|Santa Clara|      Lee|  410|
|Santa Clara|      Lee|  530|
|Santa Clara|    Patel|  240|
|Santa Clara|    Patel|  385|
|Santa Clara|    Patel|  760|
|Santa Clara|   Rivera|   88|
|Santa Clara|   Rivera|  120|
|Santa Clara|   Rivera|  140|
|Santa Clara| write-in|    4|
+-----------+---------+-----+
<BLANKLINE>
~~~

8 precinct reports became 25 rows: one per `(precinct, candidate)` pair. A
candidate who got no votes in a precinct (the `write-in` in most of them)
simply has **no row**. That is the DataFrame's "empty tally": nothing.


## 4. Step 2: add up the votes, with `groupBy().agg(sum())`

~~~python
>>> county_totals = (votes.groupBy("county", "candidate")
...                       .agg(F.sum("votes").alias("votes")))
>>> county_totals.orderBy("county", F.desc("votes")).show()
+-----------+---------+-----+
|     county|candidate|votes|
+-----------+---------+-----+
|    Alameda|      Lee| 1210|
|    Alameda|    Patel| 1200|
|    Alameda|   Rivera|  510|
|    Alameda| write-in|    7|
|  San Mateo|    Patel|  710|
|  San Mateo|      Lee|  540|
|  San Mateo|   Rivera|  155|
|Santa Clara|    Patel| 1385|
|Santa Clara|      Lee| 1235|
|Santa Clara|   Rivera|  348|
|Santa Clara| write-in|    4|
+-----------+---------+-----+
<BLANKLINE>
~~~

These are the same numbers as the RDD version
(`Santa Clara: Lee 1235, Patel 1385, Rivera 348, write-in 4`), with the
candidates in TV order (most votes first).

`sum()` is combined per partition before the shuffle (`partial_sum`), just
like `foldByKey()`. And its "zero" is built in and always correct: no one can
pass it a wrong starting value (compare section 8 of the RDD version, where
a `{'Lee': 100}` zero rigged the election).

### The TV view: one row per county, with `pivot()`

The TV graphic shows one line per county, one column per candidate.
`pivot()` turns the values of `candidate` into columns. Listing the values
(`candidates`) makes the column order fixed, and saves Spark an extra job
to find them:

~~~python
>>> candidates = ['Lee', 'Patel', 'Rivera', 'write-in']
>>> tv = votes.groupBy("county").pivot("candidate", candidates).agg(F.sum("votes"))
>>> tv.orderBy("county").show()
+-----------+----+-----+------+--------+
|     county| Lee|Patel|Rivera|write-in|
+-----------+----+-----+------+--------+
|    Alameda|1210| 1200|   510|       7|
|  San Mateo| 540|  710|   155|    NULL|
|Santa Clara|1235| 1385|   348|       4|
+-----------+----+-----+------+--------+
<BLANKLINE>
~~~

Note the `NULL`: San Mateo had no `write-in` rows, so its sum is `NULL`, not
`0`. Section 7 shows why that matters.


## 5. Bonus: the statewide total

With RDDs, the statewide total was `RDD.fold()` with the same zero and the
same function. With DataFrames, it is a `groupBy()` on fewer columns:

~~~python
>>> votes.groupBy("candidate").agg(F.sum("votes").alias("votes")).orderBy(F.desc("votes")).show()
+---------+-----+
|candidate|votes|
+---------+-----+
|    Patel| 3295|
|      Lee| 2985|
|   Rivera| 1013|
| write-in|   11|
+---------+-----+
<BLANKLINE>
~~~

`rollup()` computes **every level at once**: per `(county, candidate)`, per
county (all candidates), and the grand total. The `NULL`s mark the "all"
rows:

~~~python
>>> levels = votes.rollup("county", "candidate").agg(F.sum("votes").alias("votes"))
>>> levels.filter(F.col("candidate").isNull()).orderBy(F.col("county").asc_nulls_last()).show()
+-----------+---------+-----+
|     county|candidate|votes|
+-----------+---------+-----+
|    Alameda|     NULL| 2927|
|  San Mateo|     NULL| 1405|
|Santa Clara|     NULL| 2972|
|       NULL|     NULL| 7304|
+-----------+---------+-----+
<BLANKLINE>
~~~

That's the number of votes counted in each county, and `7304` in the whole
state.


## 6. Can we keep the dictionaries? `aggregate()` over a list of maps

Yes, and it is the most literal translation of `foldByKey()`: collect each
county's tallies into a list, then **fold** the list, starting from an
empty map, with a function that adds two maps key by key:

~~~python
>>> empty_tally = F.create_map().cast("map<string,int>")      # {}

>>> def add_tallies(t1, t2):                                  # Column version of add_tallies
...     return F.map_zip_with(t1, t2,
...                           lambda k, v1, v2: F.coalesce(v1, F.lit(0)) + F.coalesce(v2, F.lit(0)))
...

>>> folded = reports_df.groupBy("county").agg(
...     F.aggregate(F.collect_list("tally"), empty_tally, add_tallies).alias("tally"))

>>> (folded.select("county", F.explode("tally").alias("candidate", "votes"))
...        .orderBy("county", "candidate")
...        .collect()
...  == county_totals.orderBy("county", "candidate").collect())
True
~~~

`F.aggregate(array, zero, merge)` is Python's `functools.reduce(merge, array,
zero)` for an array column, and `map_zip_with()` combines two maps key by
key (a key missing from one map gives `NULL` there, hence the `coalesce()`).

It works, but compare it with section 4:

* `collect_list()` first gathers **every** tally of a county in one place:
  that's `groupByKey()`, not `foldByKey()`. Every tally crosses the network
  and no votes are added before the shuffle.
* The code is much harder to read than `groupBy().agg(sum())`.

Keep dictionaries for **storing** data if you like. To **aggregate** them,
`explode()` them into rows first.


## 7. A common mistake: `NULL` is not zero

The TV station also wants the total number of votes counted in each county.
Adding the four columns of the TV view looks right:

~~~python
>>> tv.withColumn("total", F.col("Lee") + F.col("Patel") + F.col("Rivera") + F.col("write-in")) \
...   .orderBy("county").show()
+-----------+----+-----+------+--------+-----+
|     county| Lee|Patel|Rivera|write-in|total|
+-----------+----+-----+------+--------+-----+
|    Alameda|1210| 1200|   510|       7| 2927|
|  San Mateo| 540|  710|   155|    NULL| NULL|
|Santa Clara|1235| 1385|   348|       4| 2972|
+-----------+----+-----+------+--------+-----+
<BLANKLINE>
~~~

San Mateo's total is `NULL`: in SQL, **anything + `NULL` is `NULL`**. San
Mateo had no write-in votes, so its `write-in` sum is `NULL` ("no rows"),
and one `NULL` erased 1,405 real votes.

This is the DataFrame side of the `zeroValue` question. In `foldByKey()`, we
had to **say** what "empty" means (`{}`). In a DataFrame, an empty group is
`NULL`, and when the business means "0 votes", we must say so ourselves
with `fillna(0)` (or `F.coalesce(col, F.lit(0))`):

~~~python
>>> tv.fillna(0) \
...   .withColumn("total", F.col("Lee") + F.col("Patel") + F.col("Rivera") + F.col("write-in")) \
...   .orderBy("county").show()
+-----------+----+-----+------+--------+-----+
|     county| Lee|Patel|Rivera|write-in|total|
+-----------+----+-----+------+--------+-----+
|    Alameda|1210| 1200|   510|       7| 2927|
|  San Mateo| 540|  710|   155|       0| 1405|
|Santa Clara|1235| 1385|   348|       4| 2972|
+-----------+----+-----+------+--------+-----+
<BLANKLINE>
~~~

The totals now match the `rollup()` in section 5. (Better still: compute
totals from the **long** table, `votes.groupBy("county").agg(sum("votes"))`,
where a missing candidate is simply a missing row and no `NULL` ever appears.)


## 8. Takeaways

| Real-life case | RDD | DataFrame |
|---|---|---|
| [Package tracking](reducebykey_package_tracking_dataframe.md): the current scan per package | `reduceByKey(later_scan)` | `groupBy().agg(max(struct(time, ...)))` or `max_by(col, time)` |
| [Election-night vote totals](foldbykey_election_results_dataframe.md) per county | `foldByKey({}, add_tallies)` | `explode()` each tally into rows, then `groupBy().agg(sum())` |
| [Median home price](groupbykey_median_home_prices_dataframe.md) per zip code | `groupByKey()` + `mapValues(median)` | `groupBy().agg(median())` |
| [Star ratings](aggregatebykey_product_ratings_dataframe.md) per product | `aggregateByKey([0, 0, 0, 0, 0], add_review, merge_boxes)` | `groupBy().agg(count(when(stars == k, 1)))` for each star level |
| [Customer profiles](combinebykey_customer_profile_dataframe.md) from an order log | `combineByKey(start_profile, add_order, merge_profiles)` | `groupBy().agg(count(), sum(), min(), max(), mode())` |
| [Presorted mail](sortbykey_presorted_mail_dataframe.md) by ZIP code | `sortByKey(numPartitions=3)` | `repartitionByRange(3, "zip_code").sortWithinPartitions("zip_code")` |

Rules to remember from this example:

* To aggregate a column of dictionaries, `explode()` it into one row per
  entry first; then it is a plain `groupBy().agg(sum())`.
* `rollup()` gives the per-county and the statewide totals in one query.
* An empty group is `NULL`, not `0`, and `NULL` + anything is `NULL`. When
  the business means 0, say so with `fillna(0)` or `coalesce()`.

A runnable version of this example is
[`foldbykey_election_results_dataframe.py`](foldbykey_election_results_dataframe.py)
(output of a real run: [`.log`](foldbykey_election_results_dataframe.log)).
