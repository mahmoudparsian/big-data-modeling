# PySpark RDD Practice Questions — Set 06: Answers

Solutions to the nine [Set 06 practice questions](QUESTIONS_06.md). Every
session below was run in the `bin/pyspark` shell (Spark 4.2.0), started from
this folder. The output is copied from those real runs.

Try the questions yourself first. There are often several correct
solutions, and yours doesn't need to match these exactly.

* [Q1 (Basic): Sum and product per key](#q1-basic--sum-and-product-per-key)
* [Q2 (Basic): Count per key](#q2-basic--count-per-key)
* [Q3 (Basic): Max and min per key: choosing the zero value](#q3-basic--max-and-min-per-key-choosing-the-zero-value)
* [Q4 (Intermediate): Average per key](#q4-intermediate--average-per-key)
* [Q5 (Intermediate): Unique values per key with a set](#q5-intermediate--unique-values-per-key-with-a-set)
* [Q6 (Intermediate): A rating histogram per movie](#q6-intermediate--a-rating-histogram-per-movie)
* [Q7 (Intermediate+): The zero-value trap](#q7-intermediate--the-zero-value-trap)
* [Q8 (Intermediate+): A full report per key in one pass](#q8-intermediate--a-full-report-per-key-in-one-pass)
* [Q9 (Intermediate+): Implement aggregateByKey() yourself](#q9-intermediate--implement-aggregatebykey-yourself)

---

## Q1 (Basic) — Sum and product per key

```python
>>> sales = sc.parallelize([("apple", 3), ("banana", 2), ("apple", 5),
...                         ("cherry", 7), ("banana", 4), ("apple", 1)], 2)

>>> sales.aggregateByKey(0, lambda acc, v: acc + v, lambda a, b: a + b).sortByKey().collect()
[('apple', 9), ('banana', 6), ('cherry', 7)]
>>> sales.reduceByKey(lambda a, b: a + b).sortByKey().collect()
[('apple', 9), ('banana', 6), ('cherry', 7)]

>>> sales.aggregateByKey(1, lambda acc, v: acc * v, lambda a, b: a * b).sortByKey().collect()
[('apple', 15), ('banana', 8), ('cherry', 7)]
```

**Notes:**

* The zero value must be the **neutral element** of the operation: the value that
  changes nothing. That's `0` for adding (`x + 0 = x`) and `1` for multiplying
  (`x × 1 = x`). With `0` as the zero for a product, every result would be 0.
* When the result has the **same type** as the values, `seqFunc` and `combFunc`
  are the same function, and `aggregateByKey(zero, f, f)` gives the same answer
  as `reduceByKey(f)`.

---

## Q2 (Basic) — Count per key

```python
>>> pets = sc.parallelize([("cat", "tom"), ("dog", "rex"), ("cat", "kit"),
...                        ("fish", "nemo"), ("dog", "max"), ("cat", "zoe")], 2)

>>> pets.aggregateByKey(0, lambda count, name: count + 1, lambda a, b: a + b).sortByKey().collect()
[('cat', 3), ('dog', 2), ('fish', 1)]

>>> pets.aggregateByKey(0, lambda total, name: total + len(name), lambda a, b: a + b).sortByKey().collect()
[('cat', 9), ('dog', 6), ('fish', 4)]
```

**Notes:**

* The values are **names** (strings), but the result is a **count** (an `int`). That's
  why the two functions differ:
  * `seqFunc(count, name)` combines an `int` with a `str`: it adds 1 (or `len(name)`);
  * `combFunc(a, b)` combines two `int`s: it adds them.
* With `reduceByKey()` you'd need `mapValues(lambda name: 1)` first. Here the
  conversion happens inside `seqFunc`.

---

## Q3 (Basic) — Max and min per key: choosing the zero value

```python
>>> sales.aggregateByKey(float("-inf"), max, max).sortByKey().collect()
[('apple', 5), ('banana', 4), ('cherry', 7)]

>>> (sales.aggregateByKey((float("inf"), float("-inf")),
...                       lambda mm, v: (min(mm[0], v), max(mm[1], v)),
...                       lambda a, b: (min(a[0], b[0]), max(a[1], b[1])))
...       .sortByKey()
...       .collect())
[('apple', (1, 5)), ('banana', (2, 4)), ('cherry', (7, 7))]

>>> neg = sc.parallelize([("t", -5), ("t", -2), ("u", -9)], 2)
>>> sales.aggregateByKey(0, max, max).sortByKey().collect()
[('apple', 5), ('banana', 4), ('cherry', 7)]
>>> neg.aggregateByKey(0, max, max).sortByKey().collect()
[('t', 0), ('u', 0)]
>>> neg.aggregateByKey(float("-inf"), max, max).sortByKey().collect()
[('t', -2), ('u', -9)]
```

**Notes:**

* The neutral element for `max` is **−∞**: `max(x, −∞) = x` for every `x`. For
  `min` it's **+∞**. So the zero value for `(min, max)` is `(inf, -inf)`.
* `0` **looks** like it works on `sales`, because all the values are positive. On
  `neg`, every maximum becomes `0`, a value that **isn't even in the data**. Test
  with negative numbers, and pick a zero that is neutral for **all** possible values.
* The output keeps `int`s (`5`, not `5.0`): `max(-inf, 3)` returns `3` itself.
  The `float` zero never appears in the result unless a key has no values, which
  can't happen here.

---

## Q4 (Intermediate) — Average per key

```python
>>> readings = (sc.textFile("data/sensors.txt", 2)
...               .map(lambda line: line.split(","))
...               .map(lambda f: (f[0], int(f[1]))))

>>> sum_count = readings.aggregateByKey(
...     (0, 0),
...     lambda acc, v: (acc[0] + v, acc[1] + 1),
...     lambda a, b: (a[0] + b[0], a[1] + b[1]))
>>> sum_count.sortByKey().collect()
[('s1', (84, 4)), ('s2', (120, 4)), ('s3', (68, 4))]
>>> sum_count.mapValues(lambda acc: acc[0] / acc[1]).sortByKey().collect()
[('s1', 21.0), ('s2', 30.0), ('s3', 17.0)]
```

**Notes:**

* Compared with `combineByKey()` in Set 04, `createCombiner(v) = (v, 1)` is
  replaced by the zero value `(0, 0)`. The first value then goes through
  `seqFunc` like all the others: `seqFunc((0, 0), v) = (v, 1)`. Same result, one function fewer.
* **`aggregateByKey()` is a shortcut for `combineByKey()`.** This is its whole
  implementation in PySpark (`pyspark/core/rdd.py`, Spark 4.2.0):

  ```python
  def createZero():
      return copy.deepcopy(zeroValue)

  return self.combineByKey(
      lambda v: seqFunc(createZero(), v), seqFunc, combFunc, numPartitions, partitionFunc
  )
  ```

  So `createCombiner(v)` is just "**`seqFunc` applied to a fresh copy of the zero
  value**". Q7 and Q9 follow from these few lines.

---

## Q5 (Intermediate) — Unique values per key with a set

```python
>>> logs = sc.textFile("data/web_logs.txt", 2).map(lambda line: line.split(","))
>>> def add(s, v):
...     s.add(v)
...     return s
...
>>> def union_sets(s1, s2):
...     s1 |= s2
...     return s1
...

>>> pages = logs.map(lambda f: (f[1], f[2])).aggregateByKey(set(), add, union_sets)
>>> pages.mapValues(sorted).sortByKey().collect()
[('alex', ['/cart', '/home']), ('bob', ['/checkout', '/home']), ('carol', ['/cart', '/home', '/login']), ('dana', ['/cart', '/home']), ('erin', ['/home', '/search']), ('frank', ['/login'])]

>>> date_user = logs.filter(lambda f: f[3] == "200").map(lambda f: (f[0], f[1]))
>>> date_user.aggregateByKey(set(), add, union_sets).mapValues(len).sortByKey().collect()
[('2026-10-01', 2), ('2026-10-02', 4), ('2026-10-03', 1)]
```

**Notes:**

* `add` changes the set in place and returns it, which avoids creating a new set
  for every value.
* **Why the users don't share one set:** as the source code in Q4 shows,
  `aggregateByKey()` makes a **deep copy** of the zero value (`copy.deepcopy`)
  every time it starts a new key in a partition. So each key gets its own fresh,
  empty set. Q9 shows what happens without that copy.
* Same answer as the `combineByKey()` version in
  [Set 04, Q6](ANSWERS_04.md#q6-intermediate--unique-values-per-key), with the
  `to_set` function replaced by the zero value `set()`.

---

## Q6 (Intermediate) — A rating histogram per movie

```python
>>> ratings = (sc.textFile("data/movie_ratings.txt", 2)
...              .map(lambda line: line.split(","))
...              .map(lambda f: (f[1], int(f[2]))))

>>> def add_rating(hist, rating):
...     hist[rating - 1] += 1
...     return hist
...
>>> def add_hists(h1, h2):
...     return [a + b for a, b in zip(h1, h2)]
...
>>> hist = ratings.aggregateByKey([0, 0, 0, 0, 0], add_rating, add_hists)
>>> hist.sortByKey().collect()
[('Coco', [0, 0, 1, 0, 1]), ('Inception', [0, 0, 1, 2, 1]), ('Titanic', [1, 1, 0, 1, 0]), ('Up', [0, 0, 0, 1, 2])]

>>> def average(h):
...     return round(sum((i + 1) * c for i, c in enumerate(h)) / sum(h), 2)
...
>>> hist.mapValues(average).sortByKey().collect()
[('Coco', 4.0), ('Inception', 4.0), ('Titanic', 2.33), ('Up', 4.67)]

>>> hist.mapValues(lambda h: h.index(max(h)) + 1).sortByKey().collect()
[('Coco', 3), ('Inception', 4), ('Titanic', 1), ('Up', 5)]
```

**Notes:**

* A histogram is a **fixed-size** accumulator: 5 numbers, however many ratings a
  movie has. That's the ideal case for a reducer: a billion ratings per movie still
  give only 5 numbers per movie per partition.
* Position `rating - 1` holds the count for that rating, so a 1-star rating
  adds to `hist[0]` and a 5-star rating to `hist[4]`.
* `add_hists` adds two histograms position by position.
* Once you have the histogram, you can compute **many** statistics from it
  without looking at the data again: the count (`sum(h)`), the average, the most
  common rating, the median, the share of 5-star ratings...
* `h.index(max(h))` returns the **first** position with the highest count, so ties
  go to the lower rating: Coco has one 3 and one 5, and gets 3.

---

## Q7 (Intermediate+) — The zero-value trap

```python
>>> data = [("a", 1), ("a", 2), ("b", 3), ("a", 4)]

>>> r1 = sc.parallelize(data, 1)
>>> r1.glom().collect()
[[('a', 1), ('a', 2), ('b', 3), ('a', 4)]]
>>> r1.aggregateByKey(10, lambda acc, v: acc + v, lambda x, y: x + y).sortByKey().collect()
[('a', 17), ('b', 13)]

>>> r2 = sc.parallelize(data, 2)
>>> r2.glom().collect()
[[('a', 1), ('a', 2)], [('b', 3), ('a', 4)]]
>>> r2.aggregateByKey(10, lambda acc, v: acc + v, lambda x, y: x + y).sortByKey().collect()
[('a', 27), ('b', 13)]

>>> r4 = sc.parallelize(data, 4)
>>> r4.glom().collect()
[[('a', 1)], [('a', 2)], [('b', 3)], [('a', 4)]]
>>> r4.aggregateByKey(10, lambda acc, v: acc + v, lambda x, y: x + y).sortByKey().collect()
[('a', 37), ('b', 13)]
```

The fix: use a **neutral** zero value, and add the bonus **once**, at the end:

```python
>>> r4.aggregateByKey(0, lambda acc, v: acc + v, lambda x, y: x + y).mapValues(lambda t: t + 10).sortByKey().collect()
[('a', 17), ('b', 13)]
```

**Notes:**

* **The zero value is used once per key, per partition**, not once per key. In the
  source code in Q4, `createZero()` is called by `createCombiner`, which runs the
  first time a key shows up **in each partition**.
  * 1 partition: `a` is in 1 partition → `10 + 1 + 2 + 4 = 17`;
  * 2 partitions: `a` is in 2 partitions → `(10 + 1 + 2) + (10 + 4) = 27`;
  * 4 partitions: `a` is in 3 partitions → `(10 + 1) + (10 + 2) + (10 + 4) = 37`.
  * `b` is always in exactly one partition, so it always gets 13. A test with
    only `b` would never find the bug.
* **The rule:** the zero value must be **neutral** for `seqFunc`
  (`seqFunc(zero, v)` must act like "just `v`"). Then it doesn't matter how many
  times it's used. `0` for sums, `1` for products, `-inf` for max, `set()` for
  set union, `[0, 0, 0, 0, 0]` for a histogram.
* A non-neutral zero gives an answer that **depends on the partitioning**, which can
  change when the data grows, the cluster changes, or someone repartitions. It's one
  of the hardest bugs to notice, because small tests often run on one partition.
* Compare with [Set 04, Q9](ANSWERS_04.md#q9-intermediate--when-reducebykey-gives-the-wrong-answer):
  there the **function** wasn't safe; here the **zero value** isn't.

---

## Q8 (Intermediate+) — A full report per key in one pass

```python
>>> items = (sc.textFile("data/sales.txt", 2)
...            .map(lambda line: line.split(","))
...            .map(lambda f: (f[0], (f[1], float(f[2])))))

>>> def seq(acc, item):
...     total, n, products, top = acc
...     product, price = item
...     products.add(product)
...     if top is None or price > top[1]:
...         top = item
...     return (total + price, n + 1, products, top)
...
>>> def comb(a, b):
...     top = a[3]
...     if top is None or (b[3] is not None and b[3][1] > top[1]):
...         top = b[3]
...     return (a[0] + b[0], a[1] + b[1], a[2] | b[2], top)
...
>>> report = items.aggregateByKey((0.0, 0, set(), None), seq, comb)
>>> (report.mapValues(lambda r: (round(r[0], 2), r[1], sorted(r[2]), r[3]))
...        .sortByKey()
...        .collect())
[('S1', (13.75, 4, ['apple', 'bread', 'milk'], ('milk', 4.0))), ('S2', (9.85, 3, ['apple', 'banana', 'eggs'], ('eggs', 5.1))), ('S3', (8.0, 3, ['banana', 'bread', 'milk'], ('milk', 4.0)))]
```

**Notes:**

* Each part of the accumulator has its **own neutral zero** and its own merge rule:

  | Part | Zero | `seq` (add one item) | `comb` (merge two reports) |
  |---|---|---|---|
  | total | `0.0` | `+ price` | `+` |
  | count | `0` | `+ 1` | `+` |
  | products | `set()` | `add(product)` | union `\|` |
  | most expensive | `None` | keep the pricier item | keep the pricier one |

* `None` is the neutral value for "most expensive item": "no item yet". Both
  functions check for it, because a report from a partition can still have
  `None` here, if it never saw an item for that key. (It can't actually happen in
  this code, but checking makes the functions correct on their own.)
* Rounding happens **at the end**, in `mapValues()`. Rounding inside `seq` would
  pile up rounding errors.
* If two items tie for the highest price, this code keeps whichever arrives
  first, so the answer could change between runs. Add a tie-break (like the
  product name) when it matters, as in Set 02, Q9.
* **One pass, one shuffle**, four answers. Computing them separately would read
  and shuffle the data four times.

---

## Q9 (Intermediate+) — Implement aggregateByKey() yourself

A first try, which puts the zero value straight into `createCombiner`:

```python
>>> def my_aggregate_by_key_buggy(rdd, zero, seq_func, comb_func):
...     return rdd.combineByKey(lambda v: seq_func(zero, v), seq_func, comb_func)
...
>>> my_aggregate_by_key_buggy(sales, 0, lambda a, v: a + v, lambda a, b: a + b).sortByKey().collect()
[('apple', 9), ('banana', 6), ('cherry', 7)]
```

It works for numbers. Now with a list as the zero value:

```python
>>> def append(lst, v):
...     lst.append(v)
...     return lst
...
>>> def extend(l1, l2):
...     l1.extend(l2)
...     return l1
...
>>> pets.glom().collect()
[[('cat', 'tom'), ('dog', 'rex'), ('cat', 'kit')], [('fish', 'nemo'), ('dog', 'max'), ('cat', 'zoe')]]
>>> pets.aggregateByKey([], append, extend).mapValues(sorted).sortByKey().collect()
[('cat', ['kit', 'tom', 'zoe']), ('dog', ['max', 'rex']), ('fish', ['nemo'])]
>>> my_aggregate_by_key_buggy(pets, [], append, extend).mapValues(sorted).sortByKey().collect()
[('cat', ['kit', 'max', 'nemo', 'rex', 'tom', 'zoe']), ('dog', ['kit', 'max', 'nemo', 'rex', 'tom', 'zoe']), ('fish', ['max', 'nemo', 'zoe'])]
```

The fix: give **every** new key its own **copy** of the zero value:

```python
>>> import copy
>>> def my_aggregate_by_key(rdd, zero, seq_func, comb_func):
...     return rdd.combineByKey(lambda v: seq_func(copy.deepcopy(zero), v), seq_func, comb_func)
...
>>> my_aggregate_by_key(sales, 0, lambda a, v: a + v, lambda a, b: a + b).sortByKey().collect()
[('apple', 9), ('banana', 6), ('cherry', 7)]
>>> my_aggregate_by_key(pets, [], append, extend).mapValues(sorted).sortByKey().collect()
[('cat', ['kit', 'tom', 'zoe']), ('dog', ['max', 'rex']), ('fish', ['nemo'])]
```

**Notes:**

* **The bug:** `seq_func(zero, v)` uses the **same** list object for every key.
  `append` changes that list, so in partition 0, `cat` and `dog` both start from
  one shared list, and it collects `tom`, `rex`, and `kit`. Partition 1's keys share
  another list. After the merges, every key ends up with the wrong names. (The exact
  wrong result depends on how Spark ships the data, so don't try to predict it.
  The point is that it's wrong.)
* With a number as the zero value, the bug doesn't show: `a + v` makes a **new**
  number and never changes `0`. That's why the first test passed. **Mutable zero
  values (lists, sets, dicts) are where it shows up.**
* `copy.deepcopy(zero)` gives each key a brand-new list. This is exactly the
  `createZero()` function in PySpark's own `aggregateByKey()` (see Q4), so the
  fixed version **is** the real implementation.
* That is also why Q5 and Q8 can safely change the zero `set()` in place.
