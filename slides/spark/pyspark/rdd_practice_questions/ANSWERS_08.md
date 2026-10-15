# PySpark RDD Practice Questions — Set 08: Answers

Solutions to the nine [Set 08 practice questions](QUESTIONS_08.md). Every
session below was run in the `bin/pyspark` shell (Spark 4.2.0), started from
this folder. The output is copied from those real runs.

Try the questions yourself first. There are often several correct
solutions, and yours doesn't need to match these exactly.

* [Q1 (Basic): coalesce() and repartition()](#q1-basic--coalesce-and-repartition)
* [Q2 (Basic): partitionBy(): choosing where each key goes](#q2-basic--partitionby-choosing-where-each-key-goes)
* [Q3 (Basic): cache(): compute once, use many times](#q3-basic--cache-compute-once-use-many-times)
* [Q4 (Intermediate): Which transformations keep the partitioner?](#q4-intermediate--which-transformations-keep-the-partitioner)
* [Q5 (Intermediate): Joins without a shuffle](#q5-intermediate--joins-without-a-shuffle)
* [Q6 (Intermediate): Shrinking after a filter: coalesce() or repartition()?](#q6-intermediate--shrinking-after-a-filter-coalesce-or-repartition)
* [Q7 (Intermediate+): A skewed key, and salting](#q7-intermediate--a-skewed-key-and-salting)
* [Q8 (Intermediate+): Custom partitioning and range partitioning](#q8-intermediate--custom-partitioning-and-range-partitioning)
* [Q9 (Intermediate+): Where to put cache()](#q9-intermediate--where-to-put-cache)

The `stages_run()` helper from the [questions](QUESTIONS_08.md#getting-started)
is used in Q4 and Q5:

```python
>>> import itertools
>>> _job_counter = itertools.count()
>>> def stages_run(action):
...     group = "job-%d" % next(_job_counter)
...     sc.setJobGroup(group, group)
...     action()
...     tracker = sc.statusTracker()
...     ran = 0
...     for job_id in tracker.getJobIdsForGroup(group):
...         for stage_id in tracker.getJobInfo(job_id).stageIds:
...             info = tracker.getStageInfo(stage_id)
...             if info is not None and info.numCompletedTasks > 0:
...                 ran += 1
...     sc.setJobGroup("", "")
...     return ran
...
```

---

## Q1 (Basic) — coalesce() and repartition()

```python
>>> nums = sc.parallelize(range(12), 4)
>>> nums.glom().collect()
[[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10, 11]]
>>> nums.coalesce(2).glom().collect()
[[0, 1, 2, 3, 4, 5], [6, 7, 8, 9, 10, 11]]
>>> nums.coalesce(8).getNumPartitions()
4

>>> big = sc.parallelize(range(1000), 4)
>>> big.glom().map(len).collect()
[250, 250, 250, 250]
>>> big.repartition(8).glom().map(len).collect()
[130, 120, 140, 120, 120, 120, 130, 120]
>>> big.coalesce(2).glom().map(len).collect()
[500, 500]
>>> big.repartition(8).count(), big.coalesce(2).count()
(1000, 1000)
```

**Notes:**

* **`coalesce(n)` merges neighboring partitions, without a shuffle.** Partitions 0+1
  and 2+3 simply become one partition each. Nothing is re-sorted or sent to another
  machine (on a cluster, Spark tries to merge partitions that are already on the
  same machine). That makes it **cheap**, but it can only **reduce** the number of
  partitions: `coalesce(8)` on 4 partitions leaves 4.
* **`repartition(n)` does a full shuffle.** Every element can move to any partition,
  so it can **increase** or decrease the number of partitions, and it spreads the
  data roughly evenly. That makes it **expensive**: all the data crosses the network.
* In PySpark, `repartition(n)` is literally `coalesce(n, shuffle=True)`.
* Neither one changes the **data**, only how it's split up (both counts are 1000).

---

## Q2 (Basic) — partitionBy(): choosing where each key goes

```python
>>> pairs = sc.parallelize([(k, k * 10) for k in range(10)], 2)
>>> pairs.glom().collect()
[[(0, 0), (1, 10), (2, 20), (3, 30), (4, 40)], [(5, 50), (6, 60), (7, 70), (8, 80), (9, 90)]]

>>> by3 = pairs.partitionBy(3)
>>> by3.glom().collect()
[[(0, 0), (3, 30), (6, 60), (9, 90)], [(1, 10), (4, 40), (7, 70)], [(2, 20), (5, 50), (8, 80)]]

>>> halves = pairs.partitionBy(2, lambda k: 0 if k < 5 else 1)
>>> halves.glom().collect()
[[(0, 0), (1, 10), (2, 20), (3, 30), (4, 40)], [(5, 50), (6, 60), (7, 70), (8, 80), (9, 90)]]

>>> evens_odds = pairs.partitionBy(2, lambda k: k % 2)
>>> evens_odds.glom().collect()
[[(0, 0), (2, 20), (4, 40), (6, 60), (8, 80)], [(1, 10), (3, 30), (5, 50), (7, 70), (9, 90)]]
```

**Notes:**

* `partitionBy(n)` works on **pair RDDs** and puts each element in partition
  `hash(key) % n`. A Python `int` hashes to itself, so key `k` goes to `k % 3`.
  For strings and tuples the hash isn't easy to predict by hand, but it's always
  the same for the same key. (Spark fixes Python's hash seed on the workers, using
  `PYTHONHASHSEED=0`, so that every worker agrees.)
* The important guarantee: **all the elements with the same key end up in the same
  partition.** That is exactly what `reduceByKey()`, `groupByKey()`, and `join()`
  need, so they all start with a `partitionBy()` internally. That's their shuffle.
* `partitionBy(n, f)` lets you choose the partition **yourself**: `f(key)` returns the
  partition number (Spark uses `f(key) % n`). Q8 uses this for a real grouping.
* The partitions don't have to be the same size. The halves are 5 + 5 here, but
  with a skewed key they won't be (Q7).

---

## Q3 (Basic) — cache(): compute once, use many times

```python
>>> calls = sc.accumulator(0)
>>> def slow_double(x):
...     calls.add(1)
...     return x * 2
...
>>> doubled = sc.parallelize(range(100), 4).map(slow_double)
>>> doubled.count()
100
>>> doubled.count()
100
>>> calls.value
200

>>> calls = sc.accumulator(0)
>>> cached = sc.parallelize(range(100), 4).map(slow_double).cache()
>>> cached.is_cached
True
>>> calls.value
0
>>> cached.count()
100
>>> calls.value
100
>>> cached.count()
100
>>> cached.sum()
9900
>>> calls.value
100
>>> print(cached.getStorageLevel())
Memory Serialized 1x Replicated

>>> cached = cached.unpersist()
>>> cached.is_cached
False
>>> cached.count()
100
>>> calls.value
200
```

**Notes:**

* **An RDD is a recipe, not a result.** Without `cache()`, every action runs the whole
  recipe again from the start. Two `count()`s run `slow_double` 200 times for 100 elements.
* **`cache()` keeps the computed partitions in memory** after the first action that
  computes them. After that, `count()` and `sum()` read the stored partitions:
  `slow_double` still ran only 100 times.
* **`cache()` is lazy.** Right after `cache()`, nothing has been computed or stored
  yet (`calls.value` is 0). The **first action** pays the full cost, and the later
  actions are cheap.
* `cache()` is `persist()` with the default storage level `MEMORY_ONLY`. PySpark
  always stores Python objects **serialized** (pickled), hence "Memory Serialized".
  Other levels, like `StorageLevel.MEMORY_AND_DISK`, can spill to disk when memory is full.
* **`unpersist()`** frees the memory. Then the RDD is a plain recipe again, and the
  next action recomputes it (100 more calls → 200).
* If a cached partition gets lost (an executor dies, or memory runs out), Spark
  quietly **recomputes** it from the recipe. Caching is an optimization; it never
  changes the answer.

---

## Q4 (Intermediate) — Which transformations keep the partitioner?

```python
>>> print(pairs.partitioner)
None
>>> by3.partitioner is not None, by3.partitioner.numPartitions
(True, 3)

>>> by3.mapValues(lambda v: v + 1).partitioner == by3.partitioner
True
>>> by3.filter(lambda kv: kv[1] > 20).partitioner == by3.partitioner
True
>>> print(by3.map(lambda kv: (kv[0], kv[1] + 1)).partitioner)
None
>>> by3.partitionBy(3) is by3
True

>>> stages_run(lambda: by3.mapValues(lambda v: v + 1).reduceByKey(lambda a, b: a + b).count())
1
>>> stages_run(lambda: by3.map(lambda kv: (kv[0], kv[1] + 1)).reduceByKey(lambda a, b: a + b).count())
2
```

**Notes:**

* A **partitioner** is the RDD's promise: "the elements are placed by
  `hash(key) % n`". A plain `parallelize()` makes no such promise (`None`).
  `partitionBy()` does.
* **`mapValues()` and `filter()` keep the promise**: they can't change any key, so
  each element is still in the right partition.
* **`map()` loses it.** Spark can't know that your function leaves the key alone. You
  *could* return a different key, so it assumes you did.
* The payoff: `reduceByKey()` on an RDD that is already partitioned **by the same
  partitioner skips the shuffle**. PySpark's `partitionBy()` starts with
  `if self.partitioner == partitioner: return self`, and that's why `by3.partitionBy(3)
  is by3`. So the job runs in **1 stage** with `mapValues()`, but **2 stages** with
  `map()`, because the shuffle has to happen again.
* (`by3`'s own `partitionBy()` shuffle ran once already, in Q2. Spark keeps
  shuffle output and **skips** stages it has already run, so `stages_run()` counts
  only the work that's new in each job.)
* **Lesson:** when you only change the **values** of a pair RDD, use `mapValues()`,
  not `map()`. It reads better *and* it can save a shuffle.

---

## Q5 (Intermediate) — Joins without a shuffle

```python
>>> left = sc.parallelize([(k, k * 10) for k in range(10)], 2)
>>> right = sc.parallelize([(k, str(k)) for k in range(0, 10, 2)], 2)

>>> stages_run(lambda: left.join(right).count())
2

>>> left_p = left.partitionBy(3).cache()
>>> right_p = right.partitionBy(3).cache()
>>> left_p.count(), right_p.count()
(10, 5)
>>> stages_run(lambda: left_p.join(right_p).count())
1
>>> stages_run(lambda: left_p.join(right).count())
2

>>> sorted(left_p.join(right_p).collect())
[(0, (0, '0')), (2, (20, '2')), (4, (40, '4')), (6, (60, '6')), (8, (80, '8'))]
```

**Notes:**

* A normal join takes **2 stages**: stage 1 reads both RDDs and shuffles them by key,
  and stage 2 joins the matching keys.
* If **both** RDDs already have the **same partitioner**, key `k` is in the same
  partition number on both sides, so partition *i* of the result only needs
  partition *i* of each side. There's **no shuffle**: 1 stage.
* If only **one** side is partitioned, both sides get shuffled again (2 stages).
  Co-partitioning only pays off when it's done on **both** sides, in the same way.
* `partitionBy()` is itself a shuffle, so it's **not** free. It pays off when you
  **reuse** the partitioned RDD: partition once, `cache()` it, and then join it, group
  it, or reduce it many times. A typical case is a big table that you join with many
  small lookups, or iterative algorithms like PageRank, which join the same
  `links` RDD on every iteration.
* Without `cache()`, `left_p` would be recomputed for every action. Spark can often
  reuse the shuffle output it wrote earlier, but `cache()` makes the reuse
  reliable, and skips even re-reading the shuffled data.
* You can see the stages yourself in the Spark web UI at
  [http://localhost:4040](http://localhost:4040) while the shell is running (Jobs tab).

---

## Q6 (Intermediate) — Shrinking after a filter: coalesce() or repartition()?

```python
>>> data = sc.parallelize(range(1000), 8)
>>> few = data.filter(lambda x: x < 300)
>>> few.glom().map(len).collect()
[125, 125, 50, 0, 0, 0, 0, 0]

>>> few.coalesce(2).glom().map(len).collect()
[300, 0]
>>> few.repartition(2).glom().map(len).collect()
[150, 150]

>>> tiny = sc.parallelize(range(12), 4)
>>> tiny.repartition(4).glom().map(len).collect()
[3, 6, 0, 3]
```

**Notes:**

* **A `filter()` keeps the partitions** (all 8 of them), even if most become **empty**.
  Many tiny or empty partitions waste time: each one is still a task.
* **`coalesce(2)` merges neighbors**: partitions 0–3 become one, and 4–7 the other.
  The first 4 hold all 300 elements and the last 4 hold none, so the result is
  `[300, 0]`. It's cheap (no shuffle), but here it's completely **unbalanced**.
  One task does all the work.
* **`repartition(2)` shuffles** and spreads the elements evenly: `[150, 150]`.
* **Rule of thumb:**
  * the partitions are roughly **balanced** and you want fewer, for example before
    writing output files → `coalesce()`, which is cheap;
  * the partitions are **unbalanced** (after a selective filter, or skewed data),
    or you want **more** partitions → `repartition()`, which costs a shuffle but
    balances the work.
* **Small-data surprise:** `repartition()` on 12 elements gives `[3, 6, 0, 3]`, which is
  not balanced at all. PySpark moves elements in **batches of up to 10**, and each
  batch goes to one partition. Here, each source partition (3 elements) is one batch.
  With real data sizes (thousands of elements per partition), this evens out, as
  above. Don't judge `repartition()` on toy data.

---

## Q7 (Intermediate+) — A skewed key, and salting

```python
>>> events = [(0, 1)] * 1000 + [(k, 1) for k in (1, 2, 3) for _ in range(10)]
>>> ev = sc.parallelize(events, 4)
>>> ev.count()
1030

>>> ev.partitionBy(4).glom().map(len).collect()
[1000, 10, 10, 10]
>>> (ev.groupByKey(4)
...    .mapPartitions(lambda it: [sum(len(vs) for k, vs in it)])
...    .collect())
[1000, 10, 10, 10]

>>> def add_salt(it):
...     for i, (k, v) in enumerate(it):
...         yield ((k, i % 4), v)
...
>>> salted = ev.mapPartitions(add_salt)
>>> salted.take(3)
[((0, 0), 1), ((0, 1), 1), ((0, 2), 1)]
>>> salted.partitionBy(4).glom().map(len).collect()
[257, 257, 260, 256]

>>> step1 = salted.reduceByKey(lambda a, b: a + b)
>>> sorted(step1.collect())[:5]
[((0, 0), 253), ((0, 1), 249), ((0, 2), 249), ((0, 3), 249), ((1, 0), 2)]
>>> step2 = step1.map(lambda kv: (kv[0][0], kv[1])).reduceByKey(lambda a, b: a + b)
>>> step2.sortByKey().collect()
[(0, 1000), (1, 10), (2, 10), (3, 10)]
```

**Notes:**

* **Skew:** key `0` has 1,000 of the 1,030 records. Since all the values of one key go to
  **one** partition, that partition gets 1,000 elements and the others 10. On a
  cluster, one task works ~100× longer than the rest, and the whole job waits for it.
  If the key is big enough, that task runs out of memory.
* This hurts most for `groupByKey()` and `join()`, which really do need all the values
  of a key in one place. `reduceByKey()` suffers less, because it combines locally
  first, but its final merge for the hot key still happens in one task.
* **Salting** splits a hot key into several "sub-keys": `(0, 0)`, `(0, 1)`, `(0, 2)`,
  `(0, 3)`. Each sub-key can land in a different partition, so the work is spread out:
  `[257, 257, 260, 256]`.
* Salting needs **two steps**: aggregate by the salted key (each sub-key gets a
  partial count, about 250), then **remove the salt** and aggregate again (4 partial
  counts per key, which is tiny). The final answer is the same.
* This only works for operations that can be **split and recombined** (sums, counts,
  max...: the same rule as for `reduceByKey()` in Set 04). Here the salt is
  `i % 4`, a counter inside each partition (`mapPartitions()` from Set 03). A random
  salt also works.
* In practice, you'd salt only the keys you **know** are hot, and leave the others alone.

---

## Q8 (Intermediate+) — Custom partitioning and range partitioning

```python
>>> by_city = (sc.textFile("data/shop_customers.txt", 2)
...              .map(lambda line: line.split(","))
...              .map(lambda f: (f[2], f[1])))
>>> region = {"San Jose": 0, "Austin": 1, "Boston": 2}
>>> per_city = by_city.partitionBy(3, lambda city: region[city])
>>> per_city.glom().collect()
[[('San Jose', 'Ann'), ('San Jose', 'Cara')], [('Austin', 'Bob'), ('Austin', 'Eli')], [('Boston', 'Dev')]]

>>> def summary(index, it):
...     names = sorted(name for city, name in it)
...     yield (index, len(names), names)
...
>>> per_city.mapPartitionsWithIndex(summary).collect()
[(0, 2, ['Ann', 'Cara']), (1, 2, ['Bob', 'Eli']), (2, 1, ['Dev'])]

>>> scores = sc.parallelize([72, 95, 61, 88, 79, 99, 54, 83, 90, 67,
...                          76, 92, 58, 85, 70, 97, 63, 81, 74, 89], 4)
>>> ranked = scores.map(lambda s: (s, None)).sortByKey(numPartitions=3)
>>> ranked.map(lambda kv: kv[0]).glom().collect()
[[54, 58, 61, 63, 67, 70, 72], [74, 76, 79, 81, 83, 85, 88], [89, 90, 92, 95, 97, 99]]
```

**Notes:**

* **Custom partitioning:** the function `lambda city: region[city]` decides the
  partition, so each city gets **its own** partition. That's useful when each partition
  becomes one output file ("one file per region"), or when a later
  `mapPartitions()` should see one whole group at a time, as `summary` does here.
  A key that's missing from `region` would raise a `KeyError`, so use
  `region.get(city, 0)` or similar if unknown keys are possible.
* **Range partitioning:** `sortByKey()` doesn't hash the keys. It **samples** them,
  chooses boundaries (here: up to 72, up to 88, and the rest), and sends each key to the partition for
  its **range**. Then each partition is sorted on its own. Reading partition 0, then 1,
  then 2 gives the fully sorted result. That's how a distributed sort works: no
  single machine ever sorts everything.
* That's also why `sortByKey()` started an extra sampling job in
  [Set 04, Q7](ANSWERS_04.md#q7-intermediate--predict-the-combinebykey-calls).
* Because the boundaries come from a sample, the range partitions are only **roughly**
  equal (7, 7, 6 here). PySpark uses a fixed sampling seed, so the boundaries are the
  same on every run for the same data.

---

## Q9 (Intermediate+) — Where to put cache()

```python
>>> parses = sc.accumulator(0)
>>> def parse(line):
...     parses.add(1)
...     sensor, temp = line.split(",")
...     return (sensor, int(temp))
...

>>> raw = sc.textFile("data/sensors.txt", 2)
>>> readings = raw.map(parse)
>>> readings.count(), readings.filter(lambda kv: kv[1] > 25).count(), readings.reduceByKey(max).count()
(12, 4, 3)
>>> parses.value
36

>>> parses = sc.accumulator(0)
>>> raw = sc.textFile("data/sensors.txt", 2).cache()
>>> readings = raw.map(parse)
>>> readings.count(), readings.filter(lambda kv: kv[1] > 25).count(), readings.reduceByKey(max).count()
(12, 4, 3)
>>> parses.value
36

>>> parses = sc.accumulator(0)
>>> raw = sc.textFile("data/sensors.txt", 2)
>>> readings = raw.map(parse).cache()
>>> readings.count(), readings.filter(lambda kv: kv[1] > 25).count(), readings.reduceByKey(max).count()
(12, 4, 3)
>>> parses.value
12

>>> parses = sc.accumulator(0)
>>> readings = sc.textFile("data/sensors.txt", 2).map(parse).cache()
>>> readings.glom().map(len).collect()
[7, 5]
>>> parses.value
12
>>> readings = readings.unpersist()

>>> parses = sc.accumulator(0)
>>> readings = sc.textFile("data/sensors.txt", 2).map(parse).cache()
>>> readings.take(1)
[('s1', 20)]
>>> parses.value
7
>>> readings.count()
12
>>> parses.value
12
```

**Notes:**

* **No cache:** 3 actions × 12 lines = **36** parses. Every action re-reads the file
  and re-parses every line.
* **Caching the wrong RDD:** `raw.cache()` saves the file **reading**, but `parse` still
  runs in every action, because `readings` (the result of `parse`) isn't cached.
  Still **36**.
* **Caching the right RDD:** cache the RDD that is **reused**, right **after** the
  expensive work. `readings.cache()` parses each line once: **12**. All three
  actions start from the cached `readings`.
* **A partial first action:** `take(1)` only needs the first partition, so it parses
  and caches only that partition's **7** lines. The next action (`count()`) uses the
  cached partition, and parses only the other **5**, for 12 in total. Each partition
  is cached the first time an action computes it, independently of the others.
* **When to cache:** an RDD is used by **2 or more actions** (or by every iteration
  of a loop), **and** computing it is expensive (parsing, joins, shuffles). Don't cache
  everything: memory is limited, and caching an RDD that's used once just adds work.
  Call `unpersist()` when you're done.
* The same rule applies to DataFrames (`df.cache()`).
