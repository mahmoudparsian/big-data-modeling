# A Real-Life Case for `reduceByKey()`: Package Tracking

## 1. The scenario

A delivery company scans every package many times on its way: when it is
picked up, at every warehouse, when it goes out for delivery, and when it is
delivered. The "Track your package" web page shows the **current** status:

```
PKG-1001   delivered   Santa Clara, CA   (2026-10-21 14:32)
```

Every scan is a `(package_id, (timestamp, location, status))` pair. Scanners
upload their scans in batches, so the log is **not** in time order, and one
package's scans are spread over many partitions.

For example, here are **all four** scans of package `PKG-1001` in the log (in
the order they arrive), and the **one** record we want for it:

| Input: one record per **scan** | Output: one record per **package** |
|---|---|
| `('PKG-1001', ('2026-10-21 06:05', 'Santa Clara, CA', 'out for delivery'))` | `('PKG-1001', ('2026-10-21 14:32', 'Santa Clara, CA', 'delivered'))` |
| `('PKG-1001', ('2026-10-20 08:15', 'San Jose, CA', 'picked up'))` | |
| `('PKG-1001', ('2026-10-21 14:32', 'Santa Clara, CA', 'delivered'))` | |
| `('PKG-1001', ('2026-10-20 19:40', 'Oakland, CA', 'in transit'))` | |

| Output field | Value | How it is computed from the 4 scans |
|---|---|---|
| the current scan | `('2026-10-21 14:32', 'Santa Clara, CA', 'delivered')` | the scan with the **latest** timestamp: `14:32` on `10-21` is later than `06:05` on `10-21`, `19:40` on `10-20`, and `08:15` on `10-20` |

Notice that the last scan to **arrive** in the log (`in transit`, Oakland) is
**not** the current status. The package was delivered the next day. We must
compare timestamps, not trust the arrival order.


## 2. Why this is a job for `reduceByKey()`

Look at the input and the output: the result for a package is **one scan**,
exactly the same kind of thing as each input value. Nothing new has to be
built: no profile, no rating box, no list.

Ask the customer-service team how they find the current status from a pile of
scan slips, and they describe **one** rule:

> Take two scans, keep the **later** one. Repeat until one scan is left.

That one rule is all `reduceByKey()` needs. It also works no matter how the
pile is split up: if two people each find the latest scan in their half of
the pile, the later of their two answers is the latest scan overall. Spark
relies on exactly this to reduce each partition first and then merge the
partial results.

| Business rule | `reduceByKey()` argument | Type |
|---|---|---|
| Of two scans, keep the later one | `func` | `(V, V) → V` |


## 3. Sample data

Start the PySpark shell (`./bin/pyspark`). We use **2 partitions** so the
output is the same on every machine:

~~~python
>>> sc.version
'4.2.0'

>>> # (package_id, (timestamp, location, status))
>>> scans = [
...     ('PKG-1001', ('2026-10-21 06:05', 'Santa Clara, CA', 'out for delivery')),
...     ('PKG-1002', ('2026-10-22 03:10', 'Portland, OR',    'in transit')),
...     ('PKG-1001', ('2026-10-20 08:15', 'San Jose, CA',    'picked up')),
...     ('PKG-1003', ('2026-10-22 11:45', 'Austin, TX',      'picked up')),
...     ('PKG-1001', ('2026-10-21 14:32', 'Santa Clara, CA', 'delivered')),
...     ('PKG-1002', ('2026-10-21 09:00', 'Seattle, WA',     'picked up')),
...     ('PKG-1003', ('2026-10-22 18:20', 'Dallas, TX',      'in transit')),
...     ('PKG-1001', ('2026-10-20 19:40', 'Oakland, CA',     'in transit')),
... ]

>>> # scans_rdd : RDD[(package_id, (timestamp, location, status))]
>>> scans_rdd = sc.parallelize(scans, 2)
>>> for i, part in enumerate(scans_rdd.glom().collect()):
...     print("partition", i)
...     for scan in part:
...         print("   ", scan)
...
partition 0
    ('PKG-1001', ('2026-10-21 06:05', 'Santa Clara, CA', 'out for delivery'))
    ('PKG-1002', ('2026-10-22 03:10', 'Portland, OR', 'in transit'))
    ('PKG-1001', ('2026-10-20 08:15', 'San Jose, CA', 'picked up'))
    ('PKG-1003', ('2026-10-22 11:45', 'Austin, TX', 'picked up'))
partition 1
    ('PKG-1001', ('2026-10-21 14:32', 'Santa Clara, CA', 'delivered'))
    ('PKG-1002', ('2026-10-21 09:00', 'Seattle, WA', 'picked up'))
    ('PKG-1003', ('2026-10-22 18:20', 'Dallas, TX', 'in transit'))
    ('PKG-1001', ('2026-10-20 19:40', 'Oakland, CA', 'in transit'))
~~~

Every package has scans in **both** partitions. Timestamps are written as
`'YYYY-MM-DD HH:MM'` strings, so comparing two of them as strings gives the
correct time order.


## 4. The `reduceByKey()` solution

~~~python
>>> # Of two scans, keep the later one: (V, V) -> V
>>> def later_scan(scan1, scan2):
...     return scan1 if scan1[0] >= scan2[0] else scan2    # scan[0] is the timestamp
...

>>> # current : RDD[(package_id, (timestamp, location, status))]
>>> current = scans_rdd.reduceByKey(later_scan)
>>> for package, (time, location, status) in sorted(current.collect()):
...     print(f"{package}   {status:<16} {location:<16} ({time})")
...
PKG-1001   delivered        Santa Clara, CA  (2026-10-21 14:32)
PKG-1002   in transit       Portland, OR     (2026-10-22 03:10)
PKG-1003   in transit       Dallas, TX       (2026-10-22 18:20)
~~~

That's the whole job: **one** function, one line of logic.

**An even shorter version.** Python compares tuples element by element,
starting with the first one. Since each scan is a tuple that **starts** with
the timestamp, the built-in `max()` already picks the later scan:

~~~python
>>> max(('2026-10-20 19:40', 'Oakland, CA', 'in transit'),
...     ('2026-10-21 14:32', 'Santa Clara, CA', 'delivered'))
('2026-10-21 14:32', 'Santa Clara, CA', 'delivered')

>>> sorted(scans_rdd.reduceByKey(max).collect()) == sorted(current.collect())
True
~~~

(A bonus of `max()`: if two scans of a package ever had the same timestamp,
it would break the tie by comparing the location, then the status, so the
answer never depends on the order of the scans.)


## 5. Step by step, for `PKG-1001`

`PKG-1001` has 2 scans in each partition. `reduceByKey()` first reduces
**inside** each partition, then merges the partial results after the
shuffle:

| Step | Partition 0 | Partition 1 |
|---|---|---|
| scans | `10-21 06:05 out for delivery`, `10-20 08:15 picked up` | `10-21 14:32 delivered`, `10-20 19:40 in transit` |
| `later_scan` inside the partition | keep `10-21 06:05 out for delivery` | keep `10-21 14:32 delivered` |
| `later_scan` after the shuffle | `10-21 06:05 out for delivery` vs `10-21 14:32 delivered` → **`10-21 14:32 delivered`** | |

Only **one** scan per package per partition crosses the network, not every
scan. Compare this with the [median](groupbykey_median_home_prices.md),
where partial results could **not** be merged: here the later of two partial
"latest scans" is always the true latest scan.


## 6. Bonus: a status dashboard, with `reduceByKey()` again

The operations team also wants a dashboard: how many packages are in each
status right now? That's the classic word-count pattern, another
`reduceByKey()`, applied to the current statuses:

~~~python
>>> # (status, 1) for every package, then add the 1s per status
>>> dashboard = (current
...              .map(lambda kv: (kv[1][2], 1))     # kv[1][2] is the status
...              .reduceByKey(lambda a, b: a + b))
>>> sorted(dashboard.collect())
[('delivered', 1), ('in transit', 2)]
~~~

Here again the result (a count) is the same type as the values (counts of 1),
and addition can be done in any order and in any partition.


## 7. Why not `groupByKey()`, `aggregateByKey()`, or `combineByKey()`?

All three give the same answer, but each one makes the job harder.

### `groupByKey()`: ships every scan, keeps them all, uses one

~~~python
>>> current2 = scans_rdd.groupByKey().mapValues(max)
>>> sorted(current2.collect()) == sorted(current.collect())
True
~~~

Every scan of every package crosses the network and is held in memory, just
to throw all of them away except one. A real package is scanned 10 to 20
times, and a delivery company handles millions of packages a day. With
`reduceByKey()`, each partition sends at most **one** scan per package.

### `aggregateByKey()`: needs a fake "empty" scan

~~~python
>>> no_scan = ('0000-00-00 00:00', '', '')     # "earlier than every real scan"
>>> current3 = scans_rdd.aggregateByKey(no_scan, later_scan, later_scan)
>>> sorted(current3.collect()) == sorted(current.collect())
True
~~~

A scan from the year 0000, at no location, with no status, is not something
that exists in the business. We had to invent it only because
`aggregateByKey()` requires a `zeroValue`. And `seqFunc` and `combFunc` are the
**same** function, a sign that the value and the result are the same type.

### `combineByKey()`: three functions, two of them identical, one doing nothing

~~~python
>>> current4 = scans_rdd.combineByKey(lambda scan: scan, later_scan, later_scan)
>>> sorted(current4.collect()) == sorted(current.collect())
True
~~~

`createCombiner` returns the scan unchanged, and `mergeValue` and
`mergeCombiners` are both `later_scan`. That is **exactly** how PySpark
defines `reduceByKey()` (`$SPARK_HOME/python/pyspark/core/rdd.py`):

```python
def reduceByKey(self, func, numPartitions=None, partitionFunc=portable_hash):
    return self.combineByKey(lambda x: x, func, func, numPartitions, partitionFunc)
```

So when the value and the result are the same type, `reduceByKey()` is
`combineByKey()` with the boring parts filled in for you.


## 8. A common mistake: "the last one wins"

It is tempting to write "keep the newest record" as "keep the second
argument":

```python
last_wins = lambda scan1, scan2: scan2      # WRONG
```

This assumes the scans arrive in time order. They don't, and Spark doesn't
promise any order either: inside a partition, values are combined in the
order they happen to be stored, and after the shuffle, partial results are
merged in whatever order they arrive from the network. A function given to
`reduceByKey()` must give the same answer for **every** order (it must be
**associative** and **commutative**).

You can see the problem without Spark, with Python's `functools.reduce()` on
the four scans of `PKG-1001` in two different orders:

~~~python
>>> from functools import reduce
>>> pkg_1001 = [scan for package, scan in scans if package == 'PKG-1001']
>>> def last_wins(scan1, scan2):
...     return scan2
...

>>> reduce(last_wins, pkg_1001)              # in log order
('2026-10-20 19:40', 'Oakland, CA', 'in transit')
>>> reduce(last_wins, reversed(pkg_1001))    # same scans, another order
('2026-10-21 06:05', 'Santa Clara, CA', 'out for delivery')

>>> reduce(later_scan, pkg_1001)             # correct, in any order
('2026-10-21 14:32', 'Santa Clara, CA', 'delivered')
>>> reduce(later_scan, reversed(pkg_1001))
('2026-10-21 14:32', 'Santa Clara, CA', 'delivered')
~~~

`last_wins` tells the customer their package is "in transit" in Oakland (or
"out for delivery", depending on the order), when it was delivered a day
ago. `later_scan` compares the timestamps, so the order doesn't matter.


## 9. Takeaways

| Use ... | When ... | Example |
|---|---|---|
| `reduceByKey()` | the value and the result are the same kind of thing, and one rule combines any two of them in any order | current scan per package |
| `foldByKey()` | same as `reduceByKey()`, **and** there is a natural empty value of that same type | [vote tally](foldbykey_election_results.md) per county |
| `aggregateByKey()` | the result is different from the value, and has a natural empty value | [star-rating box](aggregatebykey_product_ratings.md) per product |
| `combineByKey()` | the result is different from the value, with no sensible empty value; the first value starts it | [customer profile](combinebykey_customer_profile.md) |
| `groupByKey()` | the answer needs **all** the values of a key together, and each group is small enough | [median home price](groupbykey_median_home_prices.md) per zip code |

Other real-life jobs with the same shape (one rule, `(V, V) → V`):

* **Highest bid** per auction item (`max`).
* **Latest price** per stock ticker (keep the later quote, like `later_scan`).
* **Latest version** of each customer record, when a database sends every
  change as a new record (keep the record with the highest version number).
* **Total minutes streamed** per artist, to pay royalties (`+`).
* **First login** per user (`min` of the login times).

One rule to remember: the function given to `reduceByKey()` must give the
same answer no matter in which order, or in which groups, Spark combines the
values: `max`, `min`, `+`, "keep the later timestamp" are all fine; "keep the
second one" is not.

For the basics of all four reducers on a simpler problem, see
[Find Sum, Count, and Average Per Key](../find_sum_count_avg_per_key.md).

The same case with DataFrames (built-in aggregates, Spark SQL, and the
DataFrame-specific mistakes): [DataFrame version](reducebykey_package_tracking_dataframe.md).
