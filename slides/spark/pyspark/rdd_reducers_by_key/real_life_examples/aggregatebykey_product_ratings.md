# A Real-Life Case for `aggregateByKey()`: Star Ratings on Product Pages

## 1. The scenario

An online store shows a **rating box** on every product page, like this:

```
grinder   4.25 out of 5   (4 reviews)
  5 stars | ##   | 2
  4 stars | #    | 1
  3 stars | #    | 1
  2 stars |      | 0
  1 stars |      | 0
```

Every night, the store recomputes the rating box of every product from the
log of all reviews. Each review is a `(product, stars)` pair, where `stars` is
`1` to `5`. One product's reviews are spread over many partitions.

For example, here are **all four** reviews of the `grinder` in the log, and
the **one** rating-box record we want for it:

| Input: one record per **review** | Output: one record per **product** |
|---|---|
| `('grinder', 5)` | `('grinder', [0, 0, 1, 1, 2])` |
| `('grinder', 4)` | |
| `('grinder', 5)` | |
| `('grinder', 3)` | |

The output is a list of 5 counters, one per star level: position `0` counts
the 1-star reviews, position `1` the 2-star reviews, ..., position `4` the
5-star reviews. Everything in the rating box comes from these 5 counters:

| Rating box field | Value for `grinder` | How it is computed |
|---|---|---|
| the 5 bars | `[0, 0, 1, 1, 2]` | count each star level: one 3, one 4, two 5s |
| number of reviews | `4` | add the counters: `0 + 0 + 1 + 1 + 2 = 4` |
| average rating | `4.25` | `(1·0 + 2·0 + 3·1 + 4·1 + 5·2) / 4 = 17 / 4 = 4.25` |

So many input records (one per review) are **reduced** to one output record
(one per product), with the product as the key. As in the
[customer profile](combinebykey_customer_profile.md) example, the **value**
(one review: a number `1..5`) and the **result** (a list of 5 counters) are
different types. But this time there is something new: the result has a
natural **empty** value.


## 2. The empty rating box is real

A brand-new product, with no reviews yet, still has a rating box on its
page: **0 reviews, five empty bars**:

```
[0, 0, 0, 0, 0]
```

This is not a trick or a made-up value. It's what customers actually see.
And every product's rating box starts this way and fills up one review at a
time. Ask the web team how the box is built, and they describe:

1. **Start from an empty box**: `[0, 0, 0, 0, 0]`.
2. **Add one review**: add 1 to the counter of its star level.
3. If two servers each counted part of the reviews, **merge their two
   boxes** by adding the counters position by position.

These are **exactly** the three arguments of `aggregateByKey()`:

| Business rule | `aggregateByKey()` argument | Type |
|---|---|---|
| 1. The empty rating box | `zeroValue` | `U` |
| 2. Add one review to a box | `seqFunc` | `(U, V) → U` |
| 3. Merge two boxes | `combFunc` | `(U, U) → U` |


## 3. Sample data

Start the PySpark shell (`./bin/pyspark`). We use **2 partitions** so the
output is the same on every machine:

~~~python
>>> sc.version
'4.2.0'

>>> reviews = [
...     ('grinder', 5), ('kettle', 4), ('grinder', 4), ('mug', 5),
...     ('grinder', 5), ('kettle', 2), ('grinder', 3), ('kettle', 4),
... ]

>>> # reviews_rdd : RDD[(product, stars)]
>>> reviews_rdd = sc.parallelize(reviews, 2)
>>> reviews_rdd.glom().collect()
[[('grinder', 5), ('kettle', 4), ('grinder', 4), ('mug', 5)], [('grinder', 5), ('kettle', 2), ('grinder', 3), ('kettle', 4)]]
~~~

`grinder` and `kettle` have reviews in **both** partitions; `mug` only in one.


## 4. The `aggregateByKey()` solution

~~~python
>>> # Rule 1: the empty rating box (zeroValue)
>>> empty_box = [0, 0, 0, 0, 0]

>>> # Rule 2: add one review to a box ((U, V) -> U)
>>> def add_review(box, stars):
...     box[stars - 1] += 1           # 1 star -> position 0, ..., 5 stars -> position 4
...     return box
...

>>> # Rule 3: merge two boxes of the same product ((U, U) -> U)
>>> def merge_boxes(box1, box2):
...     for i in range(5):
...         box1[i] += box2[i]
...     return box1
...

>>> # boxes : RDD[(product, [count of 1 star, ..., count of 5 stars])]
>>> boxes = reviews_rdd.aggregateByKey(empty_box, add_review, merge_boxes)
>>> sorted(boxes.collect())
[('grinder', [0, 0, 1, 1, 2]), ('kettle', [0, 1, 0, 2, 0]), ('mug', [0, 0, 0, 0, 1])]
~~~

That's the whole aggregation. Now compute the number of reviews and the
average rating from each box:

~~~python
>>> def summary(box):
...     num_reviews = sum(box)
...     total_stars = sum((i + 1) * n for i, n in enumerate(box))
...     return (num_reviews, round(total_stars / num_reviews, 2))
...

>>> # (number of reviews, average rating)
>>> sorted(boxes.mapValues(summary).collect())
[('grinder', (4, 4.25)), ('kettle', (3, 3.33)), ('mug', (1, 5.0))]
~~~

And print the rating box exactly as the product page shows it:

~~~python
>>> def show(product, box):
...     num_reviews, average = summary(box)
...     print(f"{product}   {average} out of 5   ({num_reviews} reviews)")
...     for stars in range(5, 0, -1):
...         n = box[stars - 1]
...         print(f"  {stars} stars | {'#' * n:<4} | {n}")
...

>>> for product, box in sorted(boxes.collect()):
...     show(product, box)
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


## 5. Step by step, for `grinder`

`grinder` has 2 reviews in partition 0 and 2 reviews in partition 1. **Each
partition starts from its own empty box**:

| Step | Partition 0 | Partition 1 |
|---|---|---|
| start (`zeroValue`) | `[0, 0, 0, 0, 0]` | `[0, 0, 0, 0, 0]` |
| `add_review` | `+ 5 stars → [0, 0, 0, 0, 1]` | `+ 5 stars → [0, 0, 0, 0, 1]` |
| `add_review` | `+ 4 stars → [0, 0, 0, 1, 1]` | `+ 3 stars → [0, 0, 1, 0, 1]` |
| `merge_boxes` (after the shuffle) | `[0, 0, 0, 1, 1] + [0, 0, 1, 0, 1] → [0, 0, 1, 1, 2]` | |

Note that `add_review` and `merge_boxes` change `box` **in place**. That
is safe even though every key in every partition starts from the **same**
`empty_box` object. PySpark gives each one its own copy
(`copy.deepcopy(zeroValue)`, in `$SPARK_HOME/python/pyspark/core/rdd.py`),
so our `empty_box` itself never changes:

~~~python
>>> empty_box
[0, 0, 0, 0, 0]
~~~


## 6. The empty box shows up in the final report, too

The review log only has products that **have** reviews. But the store's
catalog also has a `teapot` that nobody has reviewed yet, and its page needs
a rating box too. It is the same empty box we already have:

~~~python
>>> catalog = sc.parallelize([('grinder', 'Coffee grinder'), ('kettle', 'Electric kettle'),
...                           ('mug', 'Travel mug'), ('teapot', 'Glass teapot')], 2)

>>> # (product, (name, box or None))  ->  (product, (name, box))
>>> pages = (catalog.leftOuterJoin(boxes)
...                 .mapValues(lambda v: (v[0], v[1] if v[1] is not None else empty_box)))
>>> for product, (name, box) in sorted(pages.collect()):
...     print(f"{name:16} {box}")
...
Coffee grinder   [0, 0, 1, 1, 2]
Electric kettle  [0, 1, 0, 2, 0]
Travel mug       [0, 0, 0, 0, 1]
Glass teapot     [0, 0, 0, 0, 0]
~~~

The `zeroValue` of `aggregateByKey()` is the **same** value the business
uses for "no data yet". That is the sign that `aggregateByKey()` is the
right reducer.


## 7. Why not `combineByKey()` or `map()` + `reduceByKey()`?

### `combineByKey()`

`combineByKey()` has no `zeroValue`. Instead, its `createCombiner` must build
a box from the **first** review of a product:

~~~python
>>> def first_review(stars):
...     box = [0, 0, 0, 0, 0]    # an empty box ...
...     box[stars - 1] += 1      # ... plus one review: the same code as add_review!
...     return box
...
>>> boxes2 = reviews_rdd.combineByKey(first_review, add_review, merge_boxes)
>>> sorted(boxes2.collect()) == sorted(boxes.collect())
True
~~~

Same answer, but `first_review` is just "an empty box, then `add_review`":
we wrote the same rule twice. When an empty result exists, `aggregateByKey()`
lets you say it once. In fact, this is exactly how PySpark implements
`aggregateByKey()`:

```python
# $SPARK_HOME/python/pyspark/core/rdd.py (simplified)
def aggregateByKey(self, zeroValue, seqFunc, combFunc, ...):
    def createZero():
        return copy.deepcopy(zeroValue)
    return self.combineByKey(lambda v: seqFunc(createZero(), v), seqFunc, combFunc, ...)
```

(Compare with the [customer profile](combinebykey_customer_profile.md)
example, where the result has a first and last order date. An empty profile
would need fake dates, so `combineByKey()` was the better fit there.)

### `map()` + `reduceByKey()`

`reduceByKey()` needs boxes on **both** sides, so every single review must
first become a box holding just that one review:

~~~python
>>> def one_review_box(stars):
...     box = [0, 0, 0, 0, 0]
...     box[stars - 1] = 1
...     return box
...
>>> one_review_boxes = reviews_rdd.mapValues(one_review_box)
>>> one_review_boxes.take(2)
[('grinder', [0, 0, 0, 0, 1]), ('kettle', [0, 0, 0, 1, 0])]
>>> sorted(one_review_boxes.reduceByKey(merge_boxes).collect()) == sorted(boxes.collect())
True
~~~

Again the same answer, but the natural rule, "add one review: `+1` to one
counter", is gone. Every review is dressed up as a 5-counter box, and then
boxes are added to boxes, 5 numbers at a time.


## 8. A common mistake: a `zeroValue` that is not empty

The `zeroValue` is used **once per key per partition**, not once per key. So it
must be truly empty. Suppose someone wants to be generous and gives every
product a "free" 5-star review to start with:

~~~python
>>> generous_box = [0, 0, 0, 0, 1]
>>> sorted(reviews_rdd.aggregateByKey(generous_box, add_review, merge_boxes).collect())
[('grinder', [0, 0, 1, 1, 4]), ('kettle', [0, 1, 0, 2, 2]), ('mug', [0, 0, 0, 0, 2])]
~~~

`mug` got **one** free 5-star review (its reviews are in one partition), but
`grinder` and `kettle` got **two** (their reviews are in two partitions).
The result depends on how Spark happened to split the data, which is a bug
that is very hard to find. Business rules like "add a bonus" belong
**after** the aggregation (in `mapValues()`), never in the `zeroValue`.


## 9. Takeaways

| Use ... | When ... | Example |
|---|---|---|
| `reduceByKey()` | the value and the result are the same kind of thing, and one rule combines any two of them in any order | [current scan](reducebykey_package_tracking.md) per package |
| `foldByKey()` | same as `reduceByKey()`, **and** there is a natural empty value of that same type | [vote tally](foldbykey_election_results.md) per county |
| `aggregateByKey()` | the result is different from the value, and has a natural empty value | star-rating box per product (this example) |
| `combineByKey()` | the result is different from the value, with no sensible empty value; the first value starts it | [customer profile](combinebykey_customer_profile.md) |
| `groupByKey()` | the answer needs **all** the values of a key together, and each group is small enough | [median home price](groupbykey_median_home_prices.md) per zip code |

Other real-life jobs with the same shape as the rating box (a fixed set of
counters that all start at zero):

* **Exam grade distribution** per course: counts of A, B, C, D, F (zero:
  `[0, 0, 0, 0, 0]`).
* **Store traffic by hour** per store: visitors in each hour of the day
  (zero: 24 zeros).
* **Survey results** per question: how many people picked each answer
  (zero: one `0` per answer choice).
* **Monthly spending** per customer: dollars spent in each month of the year
  (zero: 12 zeros).

Two rules to remember:

* The `zeroValue` must be truly **empty** (neutral): merging it with any
  result must not change that result. Spark may use it many times per key.
* `seqFunc` and `combFunc` may change and return their first argument. That
  is safe even for a mutable `zeroValue` such as a list, because PySpark
  makes a fresh copy of the `zeroValue` for every key in every partition.

For the basics of all four reducers on a simpler problem, see
[Find Sum, Count, and Average Per Key](../find_sum_count_avg_per_key.md).

The same case with DataFrames (built-in aggregates, Spark SQL, and the
DataFrame-specific mistakes): [DataFrame version](aggregatebykey_product_ratings_dataframe.md).
