# A Real-Life Case for `groupByKey()`: Median Home Price per Zip Code

## 1. The scenario

A real-estate website shows a summary for every zip code:

```
95050 (Santa Clara)   5 homes sold   median price: $1,310K
```

Every night it recomputes this summary from the log of all home sales. Each
sale is a `(zip_code, price)` pair, with the price in thousands of dollars.
One zip code's sales are spread over many partitions.

For example, here are **all five** sales in zip code `95050`, and the **one**
summary record we want for it:

| Input: one record per **sale** | Output: one record per **zip code** |
|---|---|
| `('95050', 1250)` | `('95050', (5, 1310))` |
| `('95050', 9800)` | |
| `('95050', 1400)` | |
| `('95050', 1310)` | |
| `('95050', 1180)` | |

| Output field | Value | How it is computed from the 5 sales |
|---|---|---|
| number of sales | `5` | count the sales |
| median price | `1310` | sort the prices: `1180, 1250, `**`1310`**`, 1400, 9800`, and take the **middle** one |

### Why the median and not the average?

One of the five homes is a mansion that sold for $9,800K. Look at what it
does to the average:

| | Value | What a buyer learns |
|---|---|---|
| average | `(1250 + 9800 + 1400 + 1310 + 1180) / 5 = 2988` | "Homes here cost about $3M." **Wrong:** 4 of the 5 homes sold for less than $1.5M. |
| median | `1310` | "Half the homes sold for less than $1,310K, half for more." **Right.** |

One unusual sale pulls the average far away from a typical home. The
median ignores how extreme the extreme values are. That's why real-estate
reports, salary surveys, and household-income statistics all use the
median.

**If the number of values is even**, there are two middle values and the
median is their average. For example, the median of `2750, 2900, 3100, 3300`
is `(2900 + 3100) / 2 = 3000.0`.


## 2. Why this job needs *all* the values

In the [customer profile](combinebykey_customer_profile.md) and
[rating box](aggregatebykey_product_ratings.md) examples, we kept a small
summary per key, and we could:

* **add** one value to the summary (one more order, one more review), and
* **merge** two summaries from two partitions into one.

Try that with the median. Suppose a partition has seen the prices
`1250, 9800, 1400` (median `1400`), and the next price is `1310`. What is the
new median? You cannot tell from `1400` alone. You need the other prices
again. And merging two partial medians doesn't work either (section 5 shows
this with real data).

The only "summary" that is enough to compute a median is **the whole list of
prices**. Collecting the whole list of values for each key is exactly what
`groupByKey()` does:

| Step | What a person does | Spark |
|---|---|---|
| 1. Collect | put all sales of each zip code in one pile | `groupByKey()` |
| 2. Compute | sort each pile and take the middle | `mapValues(median)` |


## 3. Sample data

Start the PySpark shell (`./bin/pyspark`). We use **2 partitions** so the
output is the same on every machine:

~~~python
>>> sc.version
'4.2.0'

>>> # (zip_code, price in $1000s)
>>> sales = [
...     ('95050', 1250), ('95014', 2900), ('95050', 9800),
...     ('95112',  980), ('95050', 1400), ('95014', 3100),
...     ('95050', 1310), ('95112', 1050), ('95014', 2750),
...     ('95050', 1180), ('95112', 1120), ('95014', 3300),
... ]

>>> # sales_rdd : RDD[(zip_code, price)]
>>> sales_rdd = sc.parallelize(sales, 2)
>>> for i, part in enumerate(sales_rdd.glom().collect()):
...     print("partition", i, ":", part)
...
partition 0 : [('95050', 1250), ('95014', 2900), ('95050', 9800), ('95112', 980), ('95050', 1400), ('95014', 3100)]
partition 1 : [('95050', 1310), ('95112', 1050), ('95014', 2750), ('95050', 1180), ('95112', 1120), ('95014', 3300)]
~~~

Every zip code has sales in **both** partitions.
(`95050` = Santa Clara, `95014` = Cupertino, `95112` = San Jose.)


## 4. The `groupByKey()` solution

Step 1: collect all prices of each zip code:

~~~python
>>> # grouped : RDD[(zip_code, Iterable[price])]
>>> grouped = sales_rdd.groupByKey()

>>> # sorted() is only for display: the order of values in a group is not guaranteed
>>> for zip_code, prices in sorted(grouped.collect()):
...     print(zip_code, sorted(prices))
...
95014 [2750, 2900, 3100, 3300]
95050 [1180, 1250, 1310, 1400, 9800]
95112 [980, 1050, 1120]
~~~

Step 2: compute the summary from each complete list:

~~~python
>>> def median(values):
...     s = sorted(values)
...     n = len(s)
...     middle = n // 2
...     if n % 2 == 1:                            # odd: the middle value
...         return s[middle]
...     return (s[middle - 1] + s[middle]) / 2    # even: average of the two middle values
...

>>> # summary : RDD[(zip_code, (number of sales, median price))]
>>> summary = grouped.mapValues(lambda prices: (len(prices), median(prices)))
>>> sorted(summary.collect())
[('95014', (4, 3000.0)), ('95050', (5, 1310)), ('95112', (3, 1050))]
~~~

And the same report the website shows:

~~~python
>>> names = {'95014': 'Cupertino', '95050': 'Santa Clara', '95112': 'San Jose'}
>>> for zip_code, (n, med) in sorted(summary.collect()):
...     print(f"{zip_code} ({names[zip_code]})   {n} homes sold   median price: ${med:,.0f}K")
...
95014 (Cupertino)   4 homes sold   median price: $3,000K
95050 (Santa Clara)   5 homes sold   median price: $1,310K
95112 (San Jose)   3 homes sold   median price: $1,050K
~~~

For comparison, here is the (misleading) average, side by side with the
median:

~~~python
>>> for zip_code, prices in sorted(grouped.collect()):
...     prices = list(prices)
...     print(zip_code, "average:", sum(prices) / len(prices), "  median:", median(prices))
...
95014 average: 3012.5   median: 3000.0
95050 average: 2988.0   median: 1310
95112 average: 1050.0   median: 1050
~~~

When there's no extreme value (`95014`, `95112`), the average and the median
are close. The one mansion in `95050` more than doubles the average.


## 5. Why partial medians cannot be merged

Could we save the shuffle by computing a median **inside each partition**
first, and then combining the partial medians (the way `reduceByKey()`
combines partial sums)? Let's try it for `95050`:

~~~python
>>> def medians_in_partition(records):
...     prices = {}
...     for zip_code, price in records:
...         prices.setdefault(zip_code, []).append(price)
...     return [(z, (sorted(p), median(p))) for z, p in prices.items()]
...
>>> partial = sales_rdd.mapPartitions(medians_in_partition)
>>> partial.filter(lambda kv: kv[0] == '95050').collect()
[('95050', ([1250, 1400, 9800], 1400)), ('95050', ([1180, 1310], 1245.0))]

>>> median([1400, 1245.0])        # "median of the partial medians"
1322.5
>>> median([1250, 9800, 1400, 1310, 1180])     # the true median
1310
~~~

The partial medians (`1400` and `1245.0`) do not have enough information to
recover the true median `1310`. And the wrong answer `1322.5` depends on how
Spark happened to split the data into partitions. Compare this with sums:
the sum of partial sums is **always** the true sum. That's why `reduceByKey()`
works for sums and counts, but **not** for medians.


## 6. Why not `reduceByKey()`, `aggregateByKey()`, or `combineByKey()`?

Since the only summary that works is the whole list of prices, any of the
other reducers would have to build that list. Here it is with
`combineByKey()`:

~~~python
>>> def start_list(price):
...     return [price]
...
>>> def add_price(prices, price):
...     prices.append(price)
...     return prices
...
>>> def merge_lists(prices1, prices2):
...     prices1.extend(prices2)
...     return prices1
...
>>> lists = sales_rdd.combineByKey(start_list, add_price, merge_lists)
>>> sorted(lists.mapValues(lambda prices: (len(prices), median(prices))).collect())
[('95014', (4, 3000.0)), ('95050', (5, 1310)), ('95112', (3, 1050))]
~~~

Same answer, but we just rewrote `groupByKey()` by hand. PySpark's own
`groupByKey()` is built from these very same three functions
(`$SPARK_HOME/python/pyspark/core/rdd.py`):

```python
# inside RDD.groupByKey() (simplified)
def createCombiner(x):
    return [x]

def mergeValue(xs, x):
    xs.append(x)
    return xs

def mergeCombiners(a, b):
    a.extend(b)
    return a
```

When the job really needs every value, say so: `groupByKey()` is shorter,
and anyone reading the code knows immediately that each key gets its full
list of values.


## 7. The price of `groupByKey()`, and when it is acceptable

`groupByKey()` is expensive:

* **every** value crosses the network in the shuffle (here, every sale), and
* all values of one key must fit in the memory of **one** executor.

For this job that is fine: even a large zip code has only a few thousand
home sales per year, so every group is small. It would **not** be fine to
group, say, every page view of a website by country: some keys would hold
billions of values.

So, before reaching for `groupByKey()`, ask:

1. **Can the answer be built from a small summary that can be added to and
   merged?** (count, sum, average, minimum, maximum, top-N, a histogram, a
   set of distinct values ...) Then use `reduceByKey()`, `aggregateByKey()`,
   or `combineByKey()`. See
   [groupByKey vs reduceByKey](../groupbykey_vs_reducebykey_guide.md).
2. **Does the answer really need all values of a key together** (a median, or
   anything that depends on the order of the values), **and is each group
   small enough** to fit in memory? Then `groupByKey()` is the right tool.

(For a median over very large groups, DataFrames offer
`percentile_approx()`, which returns an approximate median without
collecting every value of a group in one place.)


## 8. Takeaways

| Use ... | When ... | Example |
|---|---|---|
| `reduceByKey()` | the value and the result are the same kind of thing | total sales per zip code |
| `aggregateByKey()` | the result has a natural empty value | [star-rating box](aggregatebykey_product_ratings.md) per product |
| `combineByKey()` | the result has no sensible empty value; the first value starts it | [customer profile](combinebykey_customer_profile.md) |
| `groupByKey()` | the answer needs **all** the values of a key together, and each group is small enough | median home price per zip code |

Other real-life jobs that need all values of a key:

* **Median delivery time** per restaurant (a few very late deliveries make the
  average useless).
* **Web sessions** per user: sort a user's clicks by time and start a new
  session after every gap of more than 30 minutes. The answer depends on the
  **order** of the clicks, so no small summary works.
* **Duplicate payment detection** per credit card: sort a card's
  transactions by time and flag two identical charges within one minute.
* **Percentiles** (for example, the 90th-percentile response time) per web
  server.

For the basics of all four reducers on a simpler problem, see
[Find Sum, Count, and Average Per Key](../find_sum_count_avg_per_key.md).
