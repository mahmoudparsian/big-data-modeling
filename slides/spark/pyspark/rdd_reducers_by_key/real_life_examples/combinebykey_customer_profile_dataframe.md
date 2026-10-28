# Building Customer Profiles: DataFrame Version

## 1. Introduction

This is the DataFrame companion to
[A Real-Life Case for `combineByKey()`: Building Customer Profiles](combinebykey_customer_profile.md).
It uses the same orders, so you can compare the two APIs side by side.

**The job:** the marketing team of an online coffee shop wants a **profile**
for every customer: how many orders they placed, how much they spent, the
dates of their first and last order, and their **favorite product** (the one
they bought most often). The order log is not sorted by date.

| Input: one row per **order** | Output: one row per **customer** |
|---|---|
| `alice, 2026-01-05, coffee, 12.0` | `alice, 4 orders, 63.0, 2026-01-05, 2026-03-15, coffee` |
| `alice, 2026-02-11, mug, 15.0` | |
| `alice, 2026-01-20, coffee, 12.0` | |
| `alice, 2026-03-15, coffee, 24.0` | |

### From `combineByKey()` to DataFrames

With RDDs, a profile was one dictionary, and we wrote the three
`combineByKey()` functions to start, add to, and merge it. With DataFrames,
each field of the profile is its **own column**, computed by its own
built-in aggregate. Each aggregate already knows how to start, add one
value, and merge two partial results:

| Profile field | RDD (inside `start_profile` / `add_order` / `merge_profiles`) | DataFrame |
|---|---|---|
| number of orders | `1`, then `+= 1`, then `+=` | `count("*")` |
| total spent | `amount`, then `+= amount`, then `+=` | `sum("amount")` |
| first order | `date`, then `min(...)`, then `min(...)` | `min("order_date")` |
| last order | `date`, then `max(...)`, then `max(...)` | `max("order_date")` |
| favorite product | a `{product: count}` dictionary, then the top count | `mode("product")` |

The RDD version chose `combineByKey()` over `aggregateByKey()` because an
empty profile would need fake dates (a first order in the year 9999). The
DataFrame aggregates don't have that problem: `min()` and `max()` start from
`NULL` ("no value yet"), and the first real date replaces it. That is
`createCombiner`'s idea, "the first real order starts the profile", built
into every aggregate.


## 2. Sample data

Start the PySpark shell (`./bin/pyspark`). The dates arrive as text; we
convert them to a real `date` column with `to_date()`:

~~~python
>>> spark.version
'4.2.0'

>>> from pyspark.sql import functions as F

>>> orders = [
...     ('alice', '2026-01-05', 'coffee',  12.0),
...     ('bob',   '2026-01-07', 'tea',      8.0),
...     ('alice', '2026-02-11', 'mug',     15.0),
...     ('alice', '2026-01-20', 'coffee',  12.0),
...     ('bob',   '2026-03-02', 'tea',      8.0),
...     ('alice', '2026-03-15', 'coffee',  24.0),
...     ('carol', '2026-02-28', 'teapot',  30.0),
...     ('bob',   '2026-02-14', 'cookies',  5.0),
... ]

>>> orders_df = (spark.createDataFrame(
...                  orders, "customer STRING, order_date STRING, product STRING, amount DOUBLE")
...              .withColumn("order_date", F.to_date("order_date")))
>>> orders_df.printSchema()
root
 |-- customer: string (nullable = true)
 |-- order_date: date (nullable = true)
 |-- product: string (nullable = true)
 |-- amount: double (nullable = true)
<BLANKLINE>
>>> orders_df.show()
+--------+----------+-------+------+
|customer|order_date|product|amount|
+--------+----------+-------+------+
|   alice|2026-01-05| coffee|  12.0|
|     bob|2026-01-07|    tea|   8.0|
|   alice|2026-02-11|    mug|  15.0|
|   alice|2026-01-20| coffee|  12.0|
|     bob|2026-03-02|    tea|   8.0|
|   alice|2026-03-15| coffee|  24.0|
|   carol|2026-02-28| teapot|  30.0|
|     bob|2026-02-14|cookies|   5.0|
+--------+----------+-------+------+
<BLANKLINE>
~~~

> **About `<BLANKLINE>`:** `printSchema()` and `show()` print an empty line at
> the end. In the shell you just see a blank line; `<BLANKLINE>` marks it here.

> **About `orderBy()`:** after a `groupBy()`, the order of the rows is not
> guaranteed. The examples use `orderBy()` before `show()` so the output is
> always the same.


## 3. The DataFrame solution: one `agg()`, one column per field

~~~python
>>> profiles = orders_df.groupBy("customer").agg(
...     F.count("*").alias("orders"),
...     F.sum("amount").alias("total"),
...     F.min("order_date").alias("first_order"),
...     F.max("order_date").alias("last_order"),
...     F.mode("product", deterministic=True).alias("favorite"))
>>> profiles.orderBy("customer").show()
+--------+------+-----+-----------+----------+--------+
|customer|orders|total|first_order|last_order|favorite|
+--------+------+-----+-----------+----------+--------+
|   alice|     4| 63.0| 2026-01-05|2026-03-15|  coffee|
|     bob|     3| 21.0| 2026-01-07|2026-03-02|     tea|
|   carol|     1| 30.0| 2026-02-28|2026-02-28|  teapot|
+--------+------+-----+-----------+----------+--------+
<BLANKLINE>
~~~

The same profiles as the RDD version, in one statement that reads like the
marketing team's request. `F.mode()` is the **most frequent value** of a
column: `coffee` for alice (3 times, `mug` once), `tea` for bob (2 times).
`deterministic=True` decides ties: if two products were bought equally
often, it returns the **lowest** value (the alphabetically first product),
the same tie rule as the RDD version's `favorite()`. Without it, `mode()`
may return any of the tied values.

And the "We miss you!" list: customers with no order since March 1st.
Because `last_order` is a real `date`, we compare it with a date:

~~~python
>>> (profiles.filter(F.col("last_order") < F.lit("2026-03-01").cast("date"))
...          .select("customer").orderBy("customer").show())
+--------+
|customer|
+--------+
|   carol|
+--------+
<BLANKLINE>
~~~


## 4. The favorite product, with a tie-breaking rule

`mode(..., deterministic=True)` breaks ties alphabetically. But the
marketing team might want a different rule: for example, on a tie, the
product the customer bought **most recently**, or the one that brings the
most money. `mode()` can't express that.

For any tie rule, compute the favorite in two steps: count each customer's
orders per product, then keep the top product per customer with a window
that orders by count (descending), then by the tie rule. Here the tie rule
is the product name, so we can check the result against `mode()`:

~~~python
>>> from pyspark.sql import Window

>>> per_product = orders_df.groupBy("customer", "product").agg(F.count("*").alias("times"))
>>> per_product.orderBy("customer", "product").show()
+--------+-------+-----+
|customer|product|times|
+--------+-------+-----+
|   alice| coffee|    3|
|   alice|    mug|    1|
|     bob|cookies|    1|
|     bob|    tea|    2|
|   carol| teapot|    1|
+--------+-------+-----+
<BLANKLINE>

>>> top_first = Window.partitionBy("customer").orderBy(F.desc("times"), "product")
>>> favorites = (per_product
...              .withColumn("rank", F.row_number().over(top_first))
...              .filter("rank = 1")
...              .select("customer", F.col("product").alias("favorite")))
>>> favorites.orderBy("customer").show()
+--------+--------+
|customer|favorite|
+--------+--------+
|   alice|  coffee|
|     bob|     tea|
|   carol|  teapot|
+--------+--------+
<BLANKLINE>
~~~

`per_product` is the `products` dictionary of the RDD profile
(`{'coffee': 3, 'mug': 1}`), stored as rows. To use another tie rule,
change only the window's `orderBy()`. Join `favorites` to the other profile
columns to get the full profile:

~~~python
>>> profiles2 = (orders_df.groupBy("customer").agg(
...                  F.count("*").alias("orders"),
...                  F.sum("amount").alias("total"),
...                  F.min("order_date").alias("first_order"),
...                  F.max("order_date").alias("last_order"))
...              .join(favorites, "customer"))
>>> profiles2.orderBy("customer").collect() == profiles.orderBy("customer").collect()
True
~~~


## 5. Spark SQL

~~~python
>>> orders_df.createOrReplaceTempView("orders")
>>> spark.sql("""
...     SELECT customer,
...            count(*)        AS orders,
...            sum(amount)     AS total,
...            min(order_date) AS first_order,
...            max(order_date) AS last_order,
...            mode(product, true) AS favorite
...     FROM orders
...     GROUP BY customer
...     ORDER BY customer
... """).show()
+--------+------+-----+-----------+----------+--------+
|customer|orders|total|first_order|last_order|favorite|
+--------+------+-----+-----------+----------+--------+
|   alice|     4| 63.0| 2026-01-05|2026-03-15|  coffee|
|     bob|     3| 21.0| 2026-01-07|2026-03-02|     tea|
|   carol|     1| 30.0| 2026-02-28|2026-02-28|  teapot|
+--------+------+-----+-----------+----------+--------+
<BLANKLINE>
~~~


## 6. A common mistake: `max()` (or `first()`) as the "favorite"

Looking for a short way to put "the favorite product" in the `agg()`,
someone writes `max("product")`:

~~~python
>>> orders_df.groupBy("customer").agg(
...     F.max("product").alias("favorite")).orderBy("customer").show()
+--------+--------+
|customer|favorite|
+--------+--------+
|   alice|     mug|
|     bob|     tea|
|   carol|  teapot|
+--------+--------+
<BLANKLINE>
~~~

alice bought coffee 3 times and a mug once, yet her coupon is for **mugs**.
`max()` of a string column is the **alphabetically last** value (`'mug'` >
`'coffee'`), which has nothing to do with how often it was bought. bob's
`tea` is right only by luck: `'tea'` > `'cookies'` alphabetically too.

`F.first("product")` is no better: it returns whichever order happens to
come first in each group, and the Spark documentation calls it
**non-deterministic** for that reason. Mistakes like these are dangerous
because they **look right** on most rows. "Most often" is a count: use
`mode()`, or count per product as in section 4.


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

* A profile with many fields is **one** `agg()` with one aggregate per field.
  Spark computes all of them in one pass, combined per partition before the
  shuffle, like `combineByKey()`.
* `min()` and `max()` start from `NULL`, so there is no need for fake "empty"
  dates.
* "Most often" is `mode()`; `deterministic=True` breaks ties by the lowest
  value. For any other tie rule, count per value and rank with a window.
* `max()` of a string is alphabetical, and `first()` is arbitrary: neither
  means "favorite".

A runnable version of this example is
[`combinebykey_customer_profile_dataframe.py`](combinebykey_customer_profile_dataframe.py)
(output of a real run: [`.log`](combinebykey_customer_profile_dataframe.log)).
