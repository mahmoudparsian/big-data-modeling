# Star Ratings on Product Pages: DataFrame Version

## 1. Introduction

This is the DataFrame companion to
[A Real-Life Case for `aggregateByKey()`: Star Ratings on Product Pages](aggregatebykey_product_ratings.md).
It uses the same reviews, so you can compare the two APIs side by side.

**The job:** an online store shows a **rating box** on every product page:
how many reviews gave 5 stars, 4 stars, ..., 1 star, plus the number of
reviews and the average rating:

```
grinder   4.25 out of 5   (4 reviews)
  5 stars | ##   | 2
  4 stars | #    | 1
  3 stars | #    | 1
  2 stars |      | 0
  1 stars |      | 0
```

| Input: one row per **review** | Output: one row per **product** |
|---|---|
| `grinder, 5`, `grinder, 4`, `grinder, 5`, `grinder, 3` | `grinder: 1 star 0, 2 stars 0, 3 stars 1, 4 stars 1, 5 stars 2, 4 reviews, average 4.25` |

### From `aggregateByKey()` to DataFrames

With RDDs, the rating box was a list of 5 counters, and we gave
`aggregateByKey()` three things: the empty box, "add one review", and "merge
two boxes". With DataFrames, each of the 5 counters is its **own column**,
and each column is one built-in aggregate: "count the reviews with `k`
stars". Spark already knows how to start, add to, and merge a count:

| Business rule | RDD | DataFrame |
|---|---|---|
| 1. The empty rating box | `zeroValue = [0, 0, 0, 0, 0]` | built into `count()`: a count starts at 0 |
| 2. Add one review to a box | `add_review`: `box[stars - 1] += 1` | `count(when(stars == k, 1))`, one column per star level `k` |
| 3. Merge two boxes | `merge_boxes`: add position by position | built into `count()`: partial counts are added after the shuffle |

The average no longer has to be computed from the box: `avg("stars")` does it.


## 2. Sample data

Start the PySpark shell (`./bin/pyspark`):

~~~python
>>> spark.version
'4.2.0'

>>> from pyspark.sql import functions as F

>>> reviews = [
...     ('grinder', 5), ('kettle', 4), ('grinder', 4), ('mug', 5),
...     ('grinder', 5), ('kettle', 2), ('grinder', 3), ('kettle', 4),
... ]

>>> reviews_df = spark.createDataFrame(reviews, "product STRING, stars INT")
>>> reviews_df.printSchema()
root
 |-- product: string (nullable = true)
 |-- stars: integer (nullable = true)
<BLANKLINE>
>>> reviews_df.count()
8
~~~

> **About `<BLANKLINE>`:** `printSchema()` and `show()` print an empty line at
> the end. In the shell you just see a blank line; `<BLANKLINE>` marks it here.

> **About `orderBy()`:** after a `groupBy()`, the order of the rows is not
> guaranteed. The examples use `orderBy()` before `show()` so the output is
> always the same.


## 3. The DataFrame solution: one `count(when(...))` per star level

`F.when(condition, 1)` is `1` when the condition holds and `NULL` otherwise,
and `F.count()` counts only the non-`NULL` values. So
`count(when(stars == 3, 1))` is "the number of 3-star reviews". A list
comprehension builds the five columns:

~~~python
>>> star_columns = [F.count(F.when(F.col("stars") == k, 1)).alias(f"stars_{k}")
...                 for k in range(1, 6)]

>>> boxes = reviews_df.groupBy("product").agg(
...     *star_columns,
...     F.count("*").alias("reviews"),
...     F.round(F.avg("stars"), 2).alias("average"))
>>> boxes.orderBy("product").show()
+-------+-------+-------+-------+-------+-------+-------+-------+
|product|stars_1|stars_2|stars_3|stars_4|stars_5|reviews|average|
+-------+-------+-------+-------+-------+-------+-------+-------+
|grinder|      0|      0|      1|      1|      2|      4|   4.25|
| kettle|      0|      1|      0|      2|      0|      3|   3.33|
|    mug|      0|      0|      0|      0|      1|      1|    5.0|
+-------+-------+-------+-------+-------+-------+-------+-------+
<BLANKLINE>
~~~

Columns `stars_1` to `stars_5` are the RDD box `[0, 0, 1, 1, 2]` for
`grinder`, and the numbers match the RDD version. All of it in **one** pass
over the reviews, combined per partition before the shuffle, like
`aggregateByKey()`.

And the rating box exactly as the product page shows it:

~~~python
>>> def show_box(row):
...     print(f"{row.product}   {row.average} out of 5   ({row.reviews} reviews)")
...     for k in range(5, 0, -1):
...         n = row[f"stars_{k}"]
...         print(f"  {k} stars | {'#' * n:<4} | {n}")
...

>>> for row in boxes.orderBy("product").collect():
...     show_box(row)
...
grinder   4.25 out of 5   (4 reviews)
  5 stars | ##   | 2
  4 stars | #    | 1
  3 stars | #    | 1
  2 stars |      | 0
  1 stars |      | 0
kettle   3.33 out of 5   (3 reviews)
  5 stars |      | 0
  4 stars | ##   | 2
  3 stars |      | 0
  2 stars | #    | 1
  1 stars |      | 0
mug   5.0 out of 5   (1 reviews)
  5 stars | #    | 1
  4 stars |      | 0
  3 stars |      | 0
  2 stars |      | 0
  1 stars |      | 0
~~~


## 4. The empty box, for a product with no reviews

As in the RDD version, the catalog has a `teapot` that nobody has reviewed
yet. A left join keeps it, but every column that came from `boxes` is
`NULL`:

~~~python
>>> catalog = spark.createDataFrame(
...     [('grinder', 'Coffee grinder'), ('kettle', 'Electric kettle'),
...      ('mug', 'Travel mug'), ('teapot', 'Glass teapot')],
...     "product STRING, name STRING")

>>> pages = catalog.join(boxes, "product", "left")
>>> pages.orderBy("product").show()
+-------+---------------+-------+-------+-------+-------+-------+-------+-------+
|product|           name|stars_1|stars_2|stars_3|stars_4|stars_5|reviews|average|
+-------+---------------+-------+-------+-------+-------+-------+-------+-------+
|grinder| Coffee grinder|      0|      0|      1|      1|      2|      4|   4.25|
| kettle|Electric kettle|      0|      1|      0|      2|      0|      3|   3.33|
|    mug|     Travel mug|      0|      0|      0|      0|      1|      1|    5.0|
| teapot|   Glass teapot|   NULL|   NULL|   NULL|   NULL|   NULL|   NULL|   NULL|
+-------+---------------+-------+-------+-------+-------+-------+-------+-------+
<BLANKLINE>
~~~

The business meaning of the counters is clear: no reviews means **0** of
each. So we fill the counter columns, and only those, with `0`. The empty
box `[0, 0, 0, 0, 0]` of the RDD version reappears:

~~~python
>>> counters = [f"stars_{k}" for k in range(1, 6)] + ["reviews"]
>>> pages = pages.fillna(0, subset=counters)
>>> pages.orderBy("product").show()
+-------+---------------+-------+-------+-------+-------+-------+-------+-------+
|product|           name|stars_1|stars_2|stars_3|stars_4|stars_5|reviews|average|
+-------+---------------+-------+-------+-------+-------+-------+-------+-------+
|grinder| Coffee grinder|      0|      0|      1|      1|      2|      4|   4.25|
| kettle|Electric kettle|      0|      1|      0|      2|      0|      3|   3.33|
|    mug|     Travel mug|      0|      0|      0|      0|      1|      1|    5.0|
| teapot|   Glass teapot|      0|      0|      0|      0|      0|      0|   NULL|
+-------+---------------+-------+-------+-------+-------+-------+-------+-------+
<BLANKLINE>
~~~

Why not `fillna(0)` for **all** columns? Because the teapot's average would
become `0.0`, which the page would show as "0 out of 5": the worst possible
rating, for a product that no one has rated. An average of no reviews is
**undefined**, and `NULL` says exactly that; the page can show "No reviews
yet". (In the RDD version, `summary()` would have divided by zero for an
empty box: the same question, a different symptom.)


## 5. Other ways: `pivot()`, and Spark SQL

### `pivot()`

`pivot("stars", [1, 2, 3, 4, 5])` turns each star level into a column, and
`count()` counts the reviews in each cell:

~~~python
>>> pivoted = reviews_df.groupBy("product").pivot("stars", [1, 2, 3, 4, 5]).count()
>>> pivoted.fillna(0).orderBy("product").show()
+-------+---+---+---+---+---+
|product|  1|  2|  3|  4|  5|
+-------+---+---+---+---+---+
|grinder|  0|  0|  1|  1|  2|
| kettle|  0|  1|  0|  2|  0|
|    mug|  0|  0|  0|  0|  1|
+-------+---+---+---+---+---+
<BLANKLINE>
~~~

The same five counters, in a shorter query. But note the `fillna(0)`: a
pivot cell with no rows is `NULL`, not `0`. Section 6 shows what goes wrong
when the list of values is left out.

### Spark SQL

In SQL, `count_if(condition)` counts the rows where a condition holds:

~~~python
>>> reviews_df.createOrReplaceTempView("reviews")
>>> spark.sql("""
...     SELECT product,
...            count_if(stars = 1) AS stars_1,
...            count_if(stars = 2) AS stars_2,
...            count_if(stars = 3) AS stars_3,
...            count_if(stars = 4) AS stars_4,
...            count_if(stars = 5) AS stars_5,
...            count(*)            AS reviews,
...            round(avg(stars), 2) AS average
...     FROM reviews
...     GROUP BY product
...     ORDER BY product
... """).show()
+-------+-------+-------+-------+-------+-------+-------+-------+
|product|stars_1|stars_2|stars_3|stars_4|stars_5|reviews|average|
+-------+-------+-------+-------+-------+-------+-------+-------+
|grinder|      0|      0|      1|      1|      2|      4|   4.25|
| kettle|      0|      1|      0|      2|      0|      3|   3.33|
|    mug|      0|      0|      0|      0|      1|      1|    5.0|
+-------+-------+-------+-------+-------+-------+-------+-------+
<BLANKLINE>
~~~


## 6. A common mistake: `pivot()` without the list of values

Without a list of values, `pivot()` runs an extra job to find the distinct
values of `stars` **in the data**, and makes one column for each:

~~~python
>>> reviews_df.groupBy("product").pivot("stars").count().orderBy("product").show()
+-------+----+----+----+----+
|product|   2|   3|   4|   5|
+-------+----+----+----+----+
|grinder|NULL|   1|   1|   2|
| kettle|   1|NULL|   2|NULL|
|    mug|NULL|NULL|NULL|   1|
+-------+----+----+----+----+
<BLANKLINE>
~~~

Two problems:

1. **The 1-star column is gone.** No one gave a 1-star review in this data,
   so Spark never saw the value `1`. The rating box would have 4 bars, and
   code that reads `row["1"]` would fail. Tomorrow, the first 1-star review
   would silently add a new column. The shape of the report would depend on
   the data.
2. **Empty cells are `NULL`**, not `0`. The page would print
   `"2 stars | NULL"`.

The 5 star levels are part of the **business**, not of the data: a rating
box always has 5 bars. That's the same fact the RDD version expressed with
its 5-counter `zeroValue`. Say it in the code: list the values
(`pivot("stars", [1, 2, 3, 4, 5])`) and fill the counters with `0`, or build
the five columns yourself with `count(when(...))` as in section 3, which
gives `0` (not `NULL`) for an empty cell.


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

* "Count the rows where X holds" is `count(when(X, 1))` (or `count_if(X)` in
  SQL). Several of them in one `agg()` build a whole histogram in one pass.
* `pivot()` without a list of values makes columns only for the values that
  happen to be in the data. When the columns are fixed by the business, list
  them.
* After a left join or a pivot, fill **counts** with `0`, but leave values
  that don't exist (like the average of no reviews) as `NULL`.

A runnable version of this example is
[`aggregatebykey_product_ratings_dataframe.py`](aggregatebykey_product_ratings_dataframe.py)
(output of a real run: [`.log`](aggregatebykey_product_ratings_dataframe.log)).
