# PySpark RDD Practice Questions — Set 08: Partitioning and Caching

Nine practice questions, three at each level, meant to be solved
**interactively** in the `bin/pyspark` shell. This set is about **how** an RDD's
data is split up and stored, which decides how fast your job runs:

| Operation | What it does | Shuffle? |
|---|---|---|
| `rdd.coalesce(n)` | **merges** neighboring partitions into `n` (it can only reduce the number) | **no** |
| `rdd.repartition(n)` | redistributes all elements **evenly** into `n` partitions (more or fewer) | **yes** |
| `pairs.partitionBy(n, f)` | puts each `(key, value)` in partition `f(key) % n` (default `f` = hash) | **yes** |
| `rdd.cache()` | keeps the computed partitions in **memory**, for the next actions to reuse | — |

Related helpers: `getNumPartitions()`, `glom()` (from [Set 03](QUESTIONS_03.md)),
`rdd.partitioner`, `rdd.is_cached`, `rdd.getStorageLevel()`, `rdd.unpersist()`,
and `sc.accumulator()` to count work.

| # | Level | Topic | Input |
|---|---|---|---|
| [Q1](#q1-basic--coalesce-and-repartition) | Basic | `coalesce()` and `repartition()` | Python ranges |
| [Q2](#q2-basic--partitionby-choosing-where-each-key-goes) | Basic | `partitionBy()`: choosing where each key goes | Python list |
| [Q3](#q3-basic--cache-compute-once-use-many-times) | Basic | `cache()`: compute once, use many times | Python range |
| [Q4](#q4-intermediate--which-transformations-keep-the-partitioner) | Intermediate | Which transformations keep the partitioner? | Python list |
| [Q5](#q5-intermediate--joins-without-a-shuffle) | Intermediate | Joins without a shuffle | Python lists |
| [Q6](#q6-intermediate--shrinking-after-a-filter-coalesce-or-repartition) | Intermediate | Shrinking after a filter: `coalesce()` or `repartition()`? | Python ranges |
| [Q7](#q7-intermediate--a-skewed-key-and-salting) | Intermediate+ | A skewed key, and salting | Python list |
| [Q8](#q8-intermediate--custom-partitioning-and-range-partitioning) | Intermediate+ | Custom partitioning and range partitioning | [`data/shop_customers.txt`](data/shop_customers.txt), Python list |
| [Q9](#q9-intermediate--where-to-put-cache) | Intermediate+ | Where to put `cache()` | [`data/sensors.txt`](data/sensors.txt) |

You may also use everything from the earlier sets.

Try each question on your own before you open the
[answers](ANSWERS_08.md).

## Files

| File | Purpose |
|---|---|
| [QUESTIONS_08.md](QUESTIONS_08.md) | The nine questions (this page) |
| [ANSWERS_08.md](ANSWERS_08.md) | Step-by-step solutions, with real output from Spark 4.2.0 |
| [data/shop_customers.txt](data/shop_customers.txt) | Input for Q8 (also used in Set 05): `customer_id,name,city` |
| [data/sensors.txt](data/sensors.txt) | Input for Q9 (also used in Sets 04 and 06): `sensor_id,temperature` |

## Getting started

Start `$SPARK_HOME/bin/pyspark` **from this folder**, as in
[Set 01](QUESTIONS_01.md#getting-started). As in Sets 03–07, **always give the
number of partitions**, so your output matches the expected answers. Do the
questions **in order**, in one shell session. Q4 and Q5 reuse RDDs from Q2.

Q4 and Q5 ask **how many stages** a job runs. Every shuffle starts a new stage, so
fewer stages means fewer shuffles. Paste this helper into the shell first. It runs
an action and returns the number of stages that actually ran:

```python
import itertools
_job_counter = itertools.count()

def stages_run(action):
    group = "job-%d" % next(_job_counter)
    sc.setJobGroup(group, group)
    action()
    tracker = sc.statusTracker()
    ran = 0
    for job_id in tracker.getJobIdsForGroup(group):
        for stage_id in tracker.getJobInfo(job_id).stageIds:
            info = tracker.getStageInfo(stage_id)
            if info is not None and info.numCompletedTasks > 0:
                ran += 1
    sc.setJobGroup("", "")
    return ran
```

For example, `stages_run(lambda: rdd.count())` returns `1`. You can also watch
the jobs and stages in the Spark web UI at
[http://localhost:4040](http://localhost:4040) while the shell is running.

---

## Q1 (Basic) — coalesce() and repartition()

**Input:**

```python
nums = sc.parallelize(range(12), 4)
big = sc.parallelize(range(1000), 4)
```

**Tasks:**

1. Show the partitions of `nums`. Then show the partitions of `nums.coalesce(2)`.
   Which partitions were merged?
2. How many partitions does `nums.coalesce(8)` have?
3. Show the **size** of each partition of `big`, of `big.repartition(8)`, and of
   `big.coalesce(2)`. (Hint: `glom().map(len)`.)
4. Check that `repartition()` and `coalesce()` don't change the data: count both.

**Expected output:**

```
[[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10, 11]]
[[0, 1, 2, 3, 4, 5], [6, 7, 8, 9, 10, 11]]
4
[250, 250, 250, 250]
[130, 120, 140, 120, 120, 120, 130, 120]
[500, 500]
(1000, 1000)
```

---

## Q2 (Basic) — partitionBy(): choosing where each key goes

**Input:**

```python
pairs = sc.parallelize([(k, k * 10) for k in range(10)], 2)
```

**Tasks:**

1. Show the partitions of `pairs`.
2. Make `by3 = pairs.partitionBy(3)`. **Predict** which keys end up together, then
   show the partitions. (For an `int` key, the hash is the number itself.)
3. With your own partition function, put keys `0`–`4` in partition 0 and `5`–`9`
   in partition 1.
4. With your own partition function, put the even keys in partition 0 and the odd
   keys in partition 1.

**Expected output:**

```
[[(0, 0), (1, 10), (2, 20), (3, 30), (4, 40)], [(5, 50), (6, 60), (7, 70), (8, 80), (9, 90)]]
[[(0, 0), (3, 30), (6, 60), (9, 90)], [(1, 10), (4, 40), (7, 70)], [(2, 20), (5, 50), (8, 80)]]
[[(0, 0), (1, 10), (2, 20), (3, 30), (4, 40)], [(5, 50), (6, 60), (7, 70), (8, 80), (9, 90)]]
[[(0, 0), (2, 20), (4, 40), (6, 60), (8, 80)], [(1, 10), (3, 30), (5, 50), (7, 70), (9, 90)]]
```

---

## Q3 (Basic) — cache(): compute once, use many times

We count how many times a "slow" function really runs:

```python
calls = sc.accumulator(0)

def slow_double(x):
    calls.add(1)
    return x * 2
```

**Tasks:**

1. Make `doubled = sc.parallelize(range(100), 4).map(slow_double)`. Call
   `doubled.count()` **twice**, then print `calls.value`.
2. Reset the counter. Make the same RDD, but with `.cache()` at the end. Print
   `is_cached` and `calls.value` **before** any action.
3. Call `count()`, print `calls.value`, then call `count()` and `sum()`, and print
   `calls.value` again.
4. Print the storage level.
5. `unpersist()` the RDD (keep the returned RDD), check `is_cached`, call `count()`
   once more, and print `calls.value`.

**Expected output:**

```
100
100
200
True
0
100
100
100
9900
100
Memory Serialized 1x Replicated
False
100
200
```

---

## Q4 (Intermediate) — Which transformations keep the partitioner?

**Input:** `pairs` and `by3` from Q2.

**Tasks:**

1. Print the `partitioner` of `pairs`. Then check that `by3` has one, and print its
   number of partitions.
2. Does the result of each of these still have **the same partitioner** as `by3`?
   * `by3.mapValues(lambda v: v + 1)`
   * `by3.filter(lambda kv: kv[1] > 20)`
   * `by3.map(lambda kv: (kv[0], kv[1] + 1))`, which keeps the same keys!
3. What does `by3.partitionBy(3)` return?
4. With `stages_run()`, compare the stages of
   `by3.mapValues(lambda v: v + 1).reduceByKey(lambda a, b: a + b).count()` and the
   same job with the `map()` from task 2 instead of `mapValues()`.

**Expected output:**

```
None
(True, 3)
True
True
None
True
1
2
```

(Task 2 prints `True`, `True`, and then the `partitioner` of the `map()` result.
Task 3 prints whether `by3.partitionBy(3) is by3`.)

---

## Q5 (Intermediate) — Joins without a shuffle

**Input:**

```python
left = sc.parallelize([(k, k * 10) for k in range(10)], 2)
right = sc.parallelize([(k, str(k)) for k in range(0, 10, 2)], 2)
```

**Tasks:**

1. How many stages does `left.join(right).count()` run?
2. Partition **both** RDDs with `partitionBy(3)`, and `cache()` them. Run a `count()`
   on each so they get computed and cached.
3. How many stages does the join of the two **partitioned** RDDs run?
4. How many stages if only **one** side is partitioned?
5. Check the join result, sorted.

**Expected output:**

```
2
(10, 5)
1
2
[(0, (0, '0')), (2, (20, '2')), (4, (40, '4')), (6, (60, '6')), (8, (80, '8'))]
```

*Think about it:* `partitionBy()` is a shuffle itself. So when is it worth it?

---

## Q6 (Intermediate) — Shrinking after a filter: coalesce() or repartition()?

**Input:**

```python
data = sc.parallelize(range(1000), 8)
few = data.filter(lambda x: x < 300)
```

**Tasks:**

1. Show the size of each partition of `few`.
2. You want only 2 partitions. Show the partition sizes after `coalesce(2)` and
   after `repartition(2)`. Which is better here, and why?
3. Show the partition sizes of `sc.parallelize(range(12), 4).repartition(4)`. Is that
   what you expected?

**Expected output:**

```
[125, 125, 50, 0, 0, 0, 0, 0]
[300, 0]
[150, 150]
[3, 6, 0, 3]
```

---

## Q7 (Intermediate+) — A skewed key, and salting

**Input:** 1,030 events: 1,000 for key `0`, and 10 each for keys `1`, `2`, and `3`:

```python
events = [(0, 1)] * 1000 + [(k, 1) for k in (1, 2, 3) for _ in range(10)]
ev = sc.parallelize(events, 4)
```

**Tasks:**

1. Count the events. Show the partition sizes after `partitionBy(4)`.
2. After `groupByKey(4)`, show the **number of values** in each partition. (Hint: a
   `mapPartitions()` that adds up `len(values)`.)
3. **Salt** the keys: with `mapPartitions()`, turn each `(k, v)` into `((k, i % 4), v)`,
   where `i` counts the elements of the partition (0, 1, 2, ...). Show the first 3
   salted elements, and the partition sizes after `partitionBy(4)`.
4. Count the events per key in **two steps**: first `reduceByKey()` on the salted
   keys (show the first 5 sorted results), then remove the salt and `reduceByKey()`
   again (show all, sorted by key).

**Expected output:**

```
1030
[1000, 10, 10, 10]
[1000, 10, 10, 10]
[((0, 0), 1), ((0, 1), 1), ((0, 2), 1)]
[257, 257, 260, 256]
[((0, 0), 253), ((0, 1), 249), ((0, 2), 249), ((0, 3), 249), ((1, 0), 2)]
[(0, 1000), (1, 10), (2, 10), (3, 10)]
```

---

## Q8 (Intermediate+) — Custom partitioning and range partitioning

**Input:** [`data/shop_customers.txt`](data/shop_customers.txt) (read with **2**
partitions, as `customer_id,name,city`), and 20 exam scores:

```python
scores = sc.parallelize([72, 95, 61, 88, 79, 99, 54, 83, 90, 67,
                         76, 92, 58, 85, 70, 97, 63, 81, 74, 89], 4)
```

**Tasks:**

1. Make `(city, name)` pairs. Partition them so that **each city has its own
   partition**: `San Jose` → 0, `Austin` → 1, `Boston` → 2. Show the partitions.
2. With `mapPartitionsWithIndex()`, return one `(partition_index, number_of_customers,
   sorted_names)` summary per partition.
3. Sort the scores with `sortByKey(numPartitions=3)` (make them keys first), and show
   the **partitions** of the result (just the scores). What does each partition hold?

**Expected output:**

```
[[('San Jose', 'Ann'), ('San Jose', 'Cara')], [('Austin', 'Bob'), ('Austin', 'Eli')], [('Boston', 'Dev')]]
[(0, 2, ['Ann', 'Cara']), (1, 2, ['Bob', 'Eli']), (2, 1, ['Dev'])]
[[54, 58, 61, 63, 67, 70, 72], [74, 76, 79, 81, 83, 85, 88], [89, 90, 92, 95, 97, 99]]
```

---

## Q9 (Intermediate+) — Where to put cache()

**Input:** [`data/sensors.txt`](data/sensors.txt), read with **2** partitions. We
count how many times each line is parsed:

```python
parses = sc.accumulator(0)

def parse(line):
    parses.add(1)
    sensor, temp = line.split(",")
    return (sensor, int(temp))
```

Each version below runs the same **3 actions** on `readings`:
`readings.count()`, `readings.filter(lambda kv: kv[1] > 25).count()`, and
`readings.reduceByKey(max).count()`. Reset `parses` before each version.

**Tasks:**

1. **No cache:** `readings = sc.textFile(...).map(parse)`. How many parses?
2. **Cache the file:** `raw = sc.textFile(...).cache()`, then `readings = raw.map(parse)`.
   How many parses?
3. **Cache the parsed data:** `readings = sc.textFile(...).map(parse).cache()`. How many parses?
4. With a fresh cached `readings`, show the partition sizes. (Then `unpersist()` it.)
5. With another fresh cached `readings`, call `take(1)` and print `parses.value`.
   Then call `count()` and print `parses.value` again. Explain both numbers.

**Expected output:**

```
(12, 4, 3)
36
(12, 4, 3)
36
(12, 4, 3)
12
[7, 5]
12
[('s1', 20)]
7
12
12
```

*Think about it:* which RDD should you cache, in general?
