# Median Home Price per Zip Code: DataFrame Version

## 1. Introduction

This is the DataFrame companion to
[A Real-Life Case for `groupByKey()`: Median Home Price per Zip Code](groupbykey_median_home_prices.md).
It uses the same home sales, so you can compare the two APIs side by side.

**The job:** a real-estate website shows, for every zip code, the number of
homes sold and the **median** price (the middle price, which one mansion
can't distort the way it distorts the average):

```
95050 (Santa Clara)   5 homes sold   median price: $1,310K
```

| Input: one row per **sale** | Output: one row per **zip code** |
|---|---|
| `95050, 1250`, `95050, 9800`, `95050, 1400`, `95050, 1310`, `95050, 1180` | `95050, 5, 1310` |

### From `groupByKey()` to DataFrames

The RDD version made one point: a median needs **all** the values of a key
(partial medians cannot be merged), so we collected each zip code's prices
with `groupByKey()` and computed the median ourselves.

DataFrames have a built-in `median()` aggregate, so we don't collect the
prices by hand. But it can't escape the math: internally, it too keeps the
prices of each group (section 4).

| Step | RDD | DataFrame |
|---|---|---|
| 1. Collect all sales of each zip code | `groupByKey()` | done for you inside `median()` (or by hand: `collect_list()`) |
| 2. Sort each pile and take the middle | `mapValues(median)` | `median()` |


## 2. Sample data

Start the PySpark shell (`./bin/pyspark`). Prices are in thousands of
dollars:

~~~python
>>> spark.version
'4.2.0'

>>> from pyspark.sql import functions as F

>>> sales = [
...     ('95050', 1250), ('95014', 2900), ('95050', 9800),
...     ('95112',  980), ('95050', 1400), ('95014', 3100),
...     ('95050', 1310), ('95112', 1050), ('95014', 2750),
...     ('95050', 1180), ('95112', 1120), ('95014', 3300),
... ]

>>> sales_df = spark.createDataFrame(sales, "zip_code STRING, price INT")
>>> sales_df.printSchema()
root
 |-- zip_code: string (nullable = true)
 |-- price: integer (nullable = true)
<BLANKLINE>
>>> sales_df.count()
12
~~~

Zip codes are strings (a zip code is a label, and `'02138'` must keep its
leading zero). `95050` = Santa Clara, `95014` = Cupertino, `95112` = San Jose.

> **About `<BLANKLINE>`:** `printSchema()` and `show()` print an empty line at
> the end. In the shell you just see a blank line; `<BLANKLINE>` marks it here.

> **About `orderBy()`:** after a `groupBy()`, the order of the rows is not
> guaranteed. The examples use `orderBy()` before `show()` so the output is
> always the same.


## 3. The DataFrame solution: `groupBy().agg(median())`

~~~python
>>> summary = sales_df.groupBy("zip_code").agg(
...     F.count("*").alias("homes_sold"),
...     F.median("price").alias("median_price"))
>>> summary.orderBy("zip_code").show()
+--------+----------+------------+
|zip_code|homes_sold|median_price|
+--------+----------+------------+
|   95014|         4|      3000.0|
|   95050|         5|      1310.0|
|   95112|         3|      1050.0|
+--------+----------+------------+
<BLANKLINE>
~~~

`95014` has 4 sales (an even number), so its median is the average of the
two middle prices: `(2900 + 3100) / 2 = 3000.0`, exactly as in the RDD
version. (`median()` always returns a `double`, so `1310` shows as `1310.0`.)

And the same report the website shows:

~~~python
>>> names = {'95014': 'Cupertino', '95050': 'Santa Clara', '95112': 'San Jose'}
>>> for row in summary.orderBy("zip_code").collect():
...     print(f"{row.zip_code} ({names[row.zip_code]})   {row.homes_sold} homes sold   "
...           f"median price: ${row.median_price:,.0f}K")
...
95014 (Cupertino)   4 homes sold   median price: $3,000K
95050 (Santa Clara)   5 homes sold   median price: $1,310K
95112 (San Jose)   3 homes sold   median price: $1,050K
~~~

### Average vs. median, side by side

~~~python
>>> sales_df.groupBy("zip_code").agg(
...     F.avg("price").alias("average"),
...     F.median("price").alias("median")).orderBy("zip_code").show()
+--------+-------+------+
|zip_code|average|median|
+--------+-------+------+
|   95014| 3012.5|3000.0|
|   95050| 2988.0|1310.0|
|   95112| 1050.0|1050.0|
+--------+-------+------+
<BLANKLINE>
~~~

The one $9,800K mansion in `95050` more than doubles the average; the median
ignores it.


## 4. What `median()` does inside: still "all the values"

`median()` is a short name for `percentile(price, 0.5)`, and the plan shows
it:

~~~python
>>> import contextlib, io
>>> def plan_of(df):
...     buf = io.StringIO()
...     with contextlib.redirect_stdout(buf):
...         df.explain()
...     return buf.getvalue()
...

>>> plan = plan_of(summary)
>>> "partial_percentile" in plan, "Exchange hashpartitioning" in plan
(True, True)
~~~

So there **is** a partial step before the shuffle. But what does a "partial
median" look like? Section 5 of the RDD version showed that a partial median
(one number) cannot be merged. Spark doesn't try: its partial result for
each zip code is a table of **every distinct price with how many times it
occurs** (an `OpenHashMap[value, count]` in Spark's `PercentileBase` class).
Merging two of these tables is easy (add the counts), and the median is
computed only at the very end, from the complete table.

In other words, `median()` is `groupByKey()` with one saving: a price that
occurs 1,000 times is stored once, with a count of 1,000. Each group still
has to fit in memory, so the RDD rule still applies: fine for home sales per
zip code, not for billions of values per key.

### `groupByKey()` by hand: `collect_list()`

When you need the whole list of values, and not just a median, collect it
with `collect_list()`, the DataFrame version of `groupByKey()`:

~~~python
>>> grouped = sales_df.groupBy("zip_code").agg(
...     F.array_sort(F.collect_list("price")).alias("prices"))
>>> grouped.orderBy("zip_code").show(truncate=False)
+--------+------------------------------+
|zip_code|prices                        |
+--------+------------------------------+
|95014   |[2750, 2900, 3100, 3300]      |
|95050   |[1180, 1250, 1310, 1400, 9800]|
|95112   |[980, 1050, 1120]             |
+--------+------------------------------+
<BLANKLINE>
~~~

That's Step 1 of the RDD version, with the prices sorted. (`collect_list()`
gives no guaranteed order, so we sort with `array_sort()`.) It is useful
when a job needs every value of a group, for example to find web sessions
in each user's clicks. For a median, just use `median()`.


## 5. Spark SQL

In SQL, `median()` is available, and so is the standard SQL way of writing
a median, `percentile_cont(0.5) WITHIN GROUP (ORDER BY ...)`:

~~~python
>>> sales_df.createOrReplaceTempView("sales")
>>> spark.sql("""
...     SELECT zip_code,
...            count(*)                                         AS homes_sold,
...            median(price)                                    AS median_price,
...            percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS percentile_cont
...     FROM sales
...     GROUP BY zip_code
...     ORDER BY zip_code
... """).show()
+--------+----------+------------+---------------+
|zip_code|homes_sold|median_price|percentile_cont|
+--------+----------+------------+---------------+
|   95014|         4|      3000.0|         3000.0|
|   95050|         5|      1310.0|         1310.0|
|   95112|         3|      1050.0|         1050.0|
+--------+----------+------------+---------------+
<BLANKLINE>
~~~


## 6. A common mistake: `percentile_approx()` is not the median

Many tutorials compute a median with `percentile_approx(col, 0.5)`. Look at
`95014`:

~~~python
>>> sales_df.groupBy("zip_code").agg(
...     F.median("price").alias("median"),
...     F.percentile_approx("price", 0.5).alias("percentile_approx")).orderBy("zip_code").show()
+--------+------+-----------------+
|zip_code|median|percentile_approx|
+--------+------+-----------------+
|   95014|3000.0|             2900|
|   95050|1310.0|             1310|
|   95112|1050.0|             1050|
+--------+------+-----------------+
<BLANKLINE>
~~~

For the odd groups the answers agree, but for `95014` (4 sales),
`percentile_approx()` says `2900`, not `3000.0`. Two reasons:

1. **It never averages.** It returns an **actual value** from the data: the
   smallest price such that at least half of the prices are less than or
   equal to it. With 4 prices, that is the second one, `2900`.
2. **It is approximate.** It keeps a small, fixed-size **summary** of each
   group (a "sketch"), not every value. For large groups, the answer is
   close to the true percentile, within an error you control with its
   `accuracy` argument (default `10000`).

The second point is also its strength. Because the sketch has a fixed size,
two sketches can be merged like partial sums, so `percentile_approx()`
works on groups of **any** size, with no risk of running out of memory.
That's the escape hatch the RDD version mentioned. So:

| Use ... | When ... |
|---|---|
| `median()` (or `percentile(col, 0.5)`) | you need the **exact** median, and each group fits in memory (home sales per zip code) |
| `percentile_approx(col, 0.5)` | the groups are huge (response times of a web server: billions of values), and a close answer is good enough |

Always say which one you used: a real-estate report that writes "median"
and shows `2900` for Cupertino is off by $100K.


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

* `median()` is exact. Inside, it keeps every distinct value of a group
  (with its count), so each group must fit in memory, just like
  `groupByKey()`.
* `percentile_approx()` is approximate and returns an actual data value
  (no averaging), but it works on groups of any size.
* `collect_list()` is the DataFrame `groupByKey()`: use it when a job really
  needs every value of a group.

A runnable version of this example is
[`groupbykey_median_home_prices_dataframe.py`](groupbykey_median_home_prices_dataframe.py)
(output of a real run: [`.log`](groupbykey_median_home_prices_dataframe.log)).
