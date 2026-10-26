# Find Sum, Count, and Average Per Key

## 1. Introduction

This document shows how to solve one classic problem with the four
PySpark reducers for `(key, value)` RDDs:

* `groupByKey()`
* `reduceByKey()`
* `aggregateByKey()`
* `combineByKey()`

Solving the *same* problem four ways makes it easy to compare them.

### The problem

**Input:** `source_rdd : RDD[(String, Integer)]`, a set of `(key, value)` pairs.

**Output:** `RDD[(String, (Integer, Integer, Float))]`, where for every
key the value is the triplet `(sum, count, average)` of that key's values.

| Input (`key`, `value`) | Output (`key`, (`sum`, `count`, `average`)) |
|---|---|
| `('A', 2)`, `('A', 4)`, `('A', 9)` | `('A', (15, 3, 5.0))` |
| `('B', 6)`, `('B', 1)`, `('B', 2)` | `('B', (9, 3, 3.0))` |
| `('C', 1)`, `('C', 6)` | `('C', (7, 2, 3.5))` |

**Key idea:** an average is **not** associative (`avg(avg(a, b), c) != avg(a, b, c)`),
so you cannot reduce averages directly. Instead, reduce a `(sum, count)` pair per
key (addition **is** associative and commutative), then compute
`average = sum / count` once at the end, with `mapValues()`.

### The four reducers at a glance

| Reducer | Signature | Value types |
|---|---|---|
| `groupByKey()` | `groupByKey()` | `RDD[(K, V)] → RDD[(K, Iterable[V])]` |
| `reduceByKey()` | `reduceByKey(func)` | `RDD[(K, V)] → RDD[(K, V)]` (output type = input type) |
| `aggregateByKey()` | `aggregateByKey(zeroValue, seqFunc, combFunc)` | `RDD[(K, V)] → RDD[(K, U)]` (`U` can differ from `V`) |
| `combineByKey()` | `combineByKey(createCombiner, mergeValue, mergeCombiners)` | `RDD[(K, V)] → RDD[(K, C)]` (`C` can differ from `V`) |

1. **`groupByKey()`** groups all values for each key into a single sequence.
   Every `(key, value)` pair is sent across the network (no map-side combining).

   ```python
   # source_rdd : RDD[(K, V)]
   # target_rdd : RDD[(K, Iterable[V])]
   target_rdd = source_rdd.groupByKey()
   ```

2. **`reduceByKey(func)`** merges the values for each key using an
   **associative** and **commutative** function `func: (V, V) → V`.
   Values are combined locally in each partition *before* the shuffle.
   The output value type must be the **same** as the input value type.

   ```python
   # source_rdd : RDD[(K, V)]
   # target_rdd : RDD[(K, V)]
   target_rdd = source_rdd.reduceByKey(func)
   ```

3. **`aggregateByKey(zeroValue, seqFunc, combFunc)`** aggregates the values of
   each key, starting from a neutral `zeroValue`. The result type `U` can be
   **different** from the value type `V`.

   ```python
   # source_rdd : RDD[(K, V)]
   # target_rdd : RDD[(K, U)]
   # zeroValue : U              -- initial (neutral) value, per key per partition
   # seqFunc   : (U, V) -> U    -- merge a value V into an accumulator U (within a partition)
   # combFunc  : (U, U) -> U    -- merge two accumulators (across partitions)
   target_rdd = source_rdd.aggregateByKey(zeroValue, seqFunc, combFunc)
   ```

4. **`combineByKey(createCombiner, mergeValue, mergeCombiners)`** is the most
   general of the four. It's like `aggregateByKey()`, but instead of a fixed
   `zeroValue`, a **function** builds the initial combiner from the first value
   seen for a key in each partition. (`reduceByKey()` and `aggregateByKey()` are
   both implemented on top of `combineByKey()`.)

   ```python
   # source_rdd : RDD[(K, V)]
   # target_rdd : RDD[(K, C)]
   # createCombiner : V -> C       -- turn the first V of a key (per partition) into a C
   # mergeValue     : (C, V) -> C  -- merge another V into a C (within a partition)
   # mergeCombiners : (C, C) -> C  -- merge two Cs (across partitions)
   target_rdd = source_rdd.combineByKey(createCombiner, mergeValue, mergeCombiners)
   ```


## 2. Sample data (shared by all solutions)

Start the PySpark shell:

~~~
spark-4.2.0 % ./bin/pyspark
...
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 4.2.0
      /_/

Using Python version 3.13.2
Spark context available as 'sc' (master = local[*], ...).
SparkSession available as 'spark'.
~~~

Create the source RDD once; all four solutions below reuse it.
We ask for **2 partitions** explicitly so the output is the same on every
machine, and so we can see how each reducer works inside a partition:

~~~python
>>> sc.version
'4.2.0'

>>> pairs = [('A', 2), ('A', 4), ('A', 9),
...          ('B', 6), ('B', 1), ('B', 2),
...          ('C', 1), ('C', 6)]

>>> # rdd : RDD[(String, Integer)]
>>> rdd = sc.parallelize(pairs, 2)
>>> rdd.count()
8
>>> rdd.getNumPartitions()
2

>>> # glom() shows the elements of each partition as a list
>>> for i, part in enumerate(rdd.glom().collect()):
...     print("partition", i, ":", part)
...
partition 0 : [('A', 2), ('A', 4), ('A', 9), ('B', 6)]
partition 1 : [('B', 1), ('B', 2), ('C', 1), ('C', 6)]
~~~

Note that key `'B'` lives in **both** partitions, while `'A'` and `'C'`
each live in only one. Keep this in mind for `aggregateByKey()` and
`combineByKey()` below.

> **About `sorted()`:** after a shuffle, the order of the keys in `collect()`
> is not guaranteed. The examples use `sorted(... .collect())` so the output
> is always the same and easy to read.


## 3. `groupByKey()` solution

Group all values of a key together, then compute the triplet from the list
of values with `mapValues()`:

~~~python
>>> # grouped : RDD[(String, Iterable[Integer])]
>>> grouped = rdd.groupByKey()
>>> sorted(grouped.mapValues(lambda values: list(values)).collect())
[('A', [2, 4, 9]), ('B', [6, 1, 2]), ('C', [1, 6])]

>>> # sum_count_avg : RDD[(String, (Integer, Integer, Float))]
>>> sum_count_avg = grouped.mapValues(
...     lambda values: (sum(values), len(values), sum(values) / len(values)))
>>> sorted(sum_count_avg.collect())
[('A', (15, 3, 5.0)), ('B', (9, 3, 3.0)), ('C', (7, 2, 3.5))]
~~~

* **Pros:** simple and readable.
* **Cons:** every single `(key, value)` pair is shuffled across the network,
and *all* values of a key must fit in one executor's memory. For a key
with millions of values, this can cause an `OutOfMemoryError`.


## 4. `reduceByKey()` solution

`reduceByKey()` requires the output value type to be the same as the input
value type. We want `(sum, count)`, not a single integer, so we first
**map** each value `v` to the pair `(v, 1)`:

~~~python
>>> # rdd2 : RDD[(String, (Integer, Integer))]
>>> # rdd2 : RDD[(key, (sum, count))]
>>> rdd2 = rdd.mapValues(lambda v: (v, 1))
>>> rdd2.collect()
[('A', (2, 1)), ('A', (4, 1)), ('A', (9, 1)), ('B', (6, 1)), ('B', (1, 1)), ('B', (2, 1)), ('C', (1, 1)), ('C', (6, 1))]

>>> # x = (sum1, count1)
>>> # y = (sum2, count2)
>>> # Adding pairs element-wise is associative and commutative
>>> sum_count = rdd2.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
>>> sorted(sum_count.collect())
[('A', (15, 3)), ('B', (9, 3)), ('C', (7, 2))]

>>> # sum_count_avg : RDD[(String, (Integer, Integer, Float))]
>>> sum_count_avg = sum_count.mapValues(lambda v: (v[0], v[1], v[0] / v[1]))
>>> sorted(sum_count_avg.collect())
[('A', (15, 3, 5.0)), ('B', (9, 3, 3.0)), ('C', (7, 2, 3.5))]
~~~

* **Pros:** values are combined locally in each partition *before* the
shuffle (map-side combining), so only one `(sum, count)` per key per
partition crosses the network.
* **Cons:** needs an extra `mapValues()` step to convert `V` into `(V, 1)`,
which creates a new tuple for every input element.


## 5. `aggregateByKey()` solution

`aggregateByKey()` lets the accumulator type `U = (sum, count)` differ from
the value type `V = Integer`, so no extra `mapValues()` is needed before
the reduction. We supply three things:

| Argument | Our choice | Meaning |
|---|---|---|
| `zeroValue` | `(0, 0)` | starting `(sum, count)` for a key, in each partition |
| `seqFunc(acc, v)` | `(acc[0] + v, acc[1] + 1)` | add one value `v` into the accumulator (within a partition) |
| `combFunc(acc1, acc2)` | `(acc1[0] + acc2[0], acc1[1] + acc2[1])` | merge two accumulators (across partitions) |

~~~python
>>> # acc = (sum, count), v = Integer
>>> def seq_func(acc, v):
...     return (acc[0] + v, acc[1] + 1)
...
>>> # acc1, acc2 = (sum, count)
>>> def comb_func(acc1, acc2):
...     return (acc1[0] + acc2[0], acc1[1] + acc2[1])
...

>>> # sum_count : RDD[(String, (Integer, Integer))]
>>> sum_count = rdd.aggregateByKey((0, 0), seq_func, comb_func)
>>> sorted(sum_count.collect())
[('A', (15, 3)), ('B', (9, 3)), ('C', (7, 2))]

>>> # sum_count_avg : RDD[(String, (Integer, Integer, Float))]
>>> sum_count_avg = sum_count.mapValues(lambda v: (v[0], v[1], v[0] / v[1]))
>>> sorted(sum_count_avg.collect())
[('A', (15, 3, 5.0)), ('B', (9, 3, 3.0)), ('C', (7, 2, 3.5))]
~~~

Step by step, for key `'B'` (which lives in both partitions):

| Step | Partition 0 | Partition 1 |
|---|---|---|
| start | `(0, 0)` | `(0, 0)` |
| `seqFunc` | `+ 6 → (6, 1)` | `+ 1 → (1, 1)`, `+ 2 → (3, 2)` |
| `combFunc` (after the shuffle) | `(6, 1) + (3, 2) → (9, 3)` | |

### Common mistake: a `zeroValue` that is not neutral

`zeroValue` is used **once per key per partition**, not once per key. So it
must be a neutral element (`0` for sum, `0` for count). Watch what happens
with a "wrong" `zeroValue` of `(1, 0)`:

~~~python
>>> sorted(rdd.aggregateByKey((1, 0), seq_func, comb_func).collect())
[('A', (16, 3)), ('B', (11, 3)), ('C', (8, 2))]
~~~

`'A'` and `'C'` live in one partition each, so their sums are off by `1`.
But `'B'` lives in two partitions, so its sum is off by `2`. The error
depends on how the data is partitioned, which makes this bug hard to find.


## 6. `combineByKey()` solution

`combineByKey()` is like `aggregateByKey()`, but instead of a fixed
`zeroValue`, the **first** value of a key in each partition is turned into a
combiner by `createCombiner`:

| Argument | Our choice | Meaning |
|---|---|---|
| `createCombiner(v)` | `(v, 1)` | first value `v` of a key in a partition → `(sum, count)` |
| `mergeValue(c, v)` | `(c[0] + v, c[1] + 1)` | add another value `v` into combiner `c` (within a partition) |
| `mergeCombiners(c1, c2)` | `(c1[0] + c2[0], c1[1] + c2[1])` | merge two combiners (across partitions) |

~~~python
>>> # v = Integer  ->  C = (sum, count)
>>> def create_combiner(v):
...     return (v, 1)
...
>>> # c = (sum, count), v = Integer
>>> def merge_value(c, v):
...     return (c[0] + v, c[1] + 1)
...
>>> # c1, c2 = (sum, count)
>>> def merge_combiners(c1, c2):
...     return (c1[0] + c2[0], c1[1] + c2[1])
...

>>> # sum_count : RDD[(String, (Integer, Integer))]
>>> sum_count = rdd.combineByKey(create_combiner, merge_value, merge_combiners)
>>> sorted(sum_count.collect())
[('A', (15, 3)), ('B', (9, 3)), ('C', (7, 2))]

>>> # sum_count_avg : RDD[(String, (Integer, Integer, Float))]
>>> sum_count_avg = sum_count.mapValues(lambda c: (c[0], c[1], c[0] / c[1]))
>>> sorted(sum_count_avg.collect())
[('A', (15, 3, 5.0)), ('B', (9, 3, 3.0)), ('C', (7, 2, 3.5))]
~~~

Step by step, for key `'B'`:

| Step | Partition 0 | Partition 1 |
|---|---|---|
| `createCombiner` (first value) | `6 → (6, 1)` | `1 → (1, 1)` |
| `mergeValue` (other values) | (no more values) | `+ 2 → (3, 2)` |
| `mergeCombiners` (after the shuffle) | `(6, 1) + (3, 2) → (9, 3)` | |

The same solution, written with lambda expressions:

~~~python
>>> sum_count = rdd.combineByKey(
...     lambda v: (v, 1),
...     lambda c, v: (c[0] + v, c[1] + 1),
...     lambda c1, c2: (c1[0] + c2[0], c1[1] + c2[1]))
>>> sorted(sum_count.collect())
[('A', (15, 3)), ('B', (9, 3)), ('C', (7, 2))]
~~~


## 7. Comparison and takeaways

| | `groupByKey()` | `reduceByKey()` | `aggregateByKey()` | `combineByKey()` |
|---|---|---|---|---|
| Map-side combining (before the shuffle) | No | Yes | Yes | Yes |
| What is shuffled per key | every value | one `(sum, count)` per partition | one `(sum, count)` per partition | one `(sum, count)` per partition |
| Output type can differ from `V` | yes (`Iterable[V]`) | no | yes | yes |
| Extra `mapValues()` before reducing | No | Yes: `v → (v, 1)` | No | No |
| How the accumulator starts | (n/a) | the first value | fixed `zeroValue` | `createCombiner(first value)` |
| Number of functions to write | 0 | 1 | 2 (+ `zeroValue`) | 3 |

* All four give the same answer, and all four compute the final
  `average = sum / count` with a `mapValues()` at the end.
* Prefer `reduceByKey()`, `aggregateByKey()`, or `combineByKey()` over
  `groupByKey()` for aggregations: they shuffle much less data and do not
  need all values of a key in memory at once. Use `groupByKey()` only when
  you really need every value (see
  [groupByKey vs reduceByKey](groupbykey_vs_reducebykey_guide.md)).
* Use `aggregateByKey()` when there is a natural neutral "zero"
  (like `(0, 0)` here); use `combineByKey()` when the starting combiner
  should be built from the first value (for example, when there is no
  natural zero, such as finding a per-key minimum and maximum).
* The functions must be **associative and commutative**: Spark does not
  promise any order for combining values inside a partition or for
  merging combiners across partitions.

For a more formal treatment of these four transformations, see
[PySpark (Key, Value) RDD Transformations](key_value_reducer_transformations.md).

For the same problem solved with DataFrames and Spark SQL, see the
[DataFrame version](find_sum_count_avg_per_key_dataframe.md).
