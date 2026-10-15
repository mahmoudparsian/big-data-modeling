# PySpark RDD Practice Questions — Set 04: Answers

Solutions to the nine [Set 04 practice questions](QUESTIONS_04.md). Every
session below was run in the `bin/pyspark` shell (Spark 4.2.0), started from
this folder. The output is copied from those real runs.

Try the questions yourself first. There are often several correct
solutions, and yours doesn't need to match these exactly.

* [Q1 (Basic): Sum and max per key, three ways](#q1-basic--sum-and-max-per-key-three-ways)
* [Q2 (Basic): Count per key, three ways](#q2-basic--count-per-key-three-ways)
* [Q3 (Basic): List of values per key, three ways](#q3-basic--list-of-values-per-key-three-ways)
* [Q4 (Intermediate): Average per key with combineByKey()](#q4-intermediate--average-per-key-with-combinebykey)
* [Q5 (Intermediate): Min, max, mean, and standard deviation in one pass](#q5-intermediate--min-max-mean-and-standard-deviation-in-one-pass)
* [Q6 (Intermediate): Unique values per key](#q6-intermediate--unique-values-per-key)
* [Q7 (Intermediate+): Predict the combineByKey() calls](#q7-intermediate--predict-the-combinebykey-calls)
* [Q8 (Intermediate+): Top 2 per key without groupByKey()](#q8-intermediate--top-2-per-key-without-groupbykey)
* [Q9 (Intermediate+): When reduceByKey() gives the wrong answer](#q9-intermediate--when-reducebykey-gives-the-wrong-answer)

---

## Q1 (Basic) — Sum and max per key, three ways

```python
>>> sales = sc.parallelize([("apple", 3), ("banana", 2), ("apple", 5),
...                         ("cherry", 7), ("banana", 4), ("apple", 1)], 2)
>>> sales.glom().collect()
[[('apple', 3), ('banana', 2), ('apple', 5)], [('cherry', 7), ('banana', 4), ('apple', 1)]]

>>> sales.reduceByKey(lambda a, b: a + b).sortByKey().collect()
[('apple', 9), ('banana', 6), ('cherry', 7)]
>>> sales.groupByKey().mapValues(sum).sortByKey().collect()
[('apple', 9), ('banana', 6), ('cherry', 7)]
>>> sales.combineByKey(lambda v: v,
...                    lambda c, v: c + v,
...                    lambda c1, c2: c1 + c2).sortByKey().collect()
[('apple', 9), ('banana', 6), ('cherry', 7)]

>>> sales.reduceByKey(max).sortByKey().collect()
[('apple', 5), ('banana', 4), ('cherry', 7)]
>>> sales.groupByKey().mapValues(max).sortByKey().collect()
[('apple', 5), ('banana', 4), ('cherry', 7)]
>>> sales.combineByKey(lambda v: v, max, max).sortByKey().collect()
[('apple', 5), ('banana', 4), ('cherry', 7)]
```

**Notes:**

* When the combiner is the **same type** as the values (a number here),
  `combineByKey()` doesn't add anything. `createCombiner` just returns the
  value, and the other two functions are the same as the `reduceByKey()` function.
  In fact, **`reduceByKey(f)` is `combineByKey(lambda v: v, f, f)`**.
* For apple: partition 0 combines `3 + 5 = 8`, partition 1 has `1`. After the
  shuffle, `mergeCombiners(8, 1) = 9`. `groupByKey()` instead ships all three
  values, `3`, `5`, and `1`, and adds them up afterwards.

---

## Q2 (Basic) — Count per key, three ways

```python
>>> pets = sc.parallelize([("cat", "tom"), ("dog", "rex"), ("cat", "kit"),
...                        ("fish", "nemo"), ("dog", "max"), ("cat", "zoe")], 2)

>>> pets.mapValues(lambda name: 1).reduceByKey(lambda a, b: a + b).sortByKey().collect()
[('cat', 3), ('dog', 2), ('fish', 1)]
>>> pets.groupByKey().mapValues(len).sortByKey().collect()
[('cat', 3), ('dog', 2), ('fish', 1)]
>>> pets.combineByKey(lambda name: 1,
...                   lambda count, name: count + 1,
...                   lambda c1, c2: c1 + c2).sortByKey().collect()
[('cat', 3), ('dog', 2), ('fish', 1)]
```

**Notes:**

* `reduceByKey(f)` must return the **same type** as the values. You can't add
  names to get a count, so first you have to `mapValues()` every name to a `1`.
* `combineByKey()` has no such rule. The combiner (a count, an `int`) can be a
  **different type** than the values (names, `str`):
  * `createCombiner`: the first name seen → count 1;
  * `mergeValue`: another name → count + 1 (the name itself is ignored);
  * `mergeCombiners`: two counts → their sum.
* That's the main reason `combineByKey()` exists: **the value type and the result
  type can be different**.

---

## Q3 (Basic) — List of values per key, three ways

```python
>>> pets.groupByKey().mapValues(sorted).sortByKey().collect()
[('cat', ['kit', 'tom', 'zoe']), ('dog', ['max', 'rex']), ('fish', ['nemo'])]

>>> def to_list(v):
...     return [v]
...
>>> def append(c, v):
...     c.append(v)
...     return c
...
>>> def extend(c1, c2):
...     c1.extend(c2)
...     return c1
...
>>> pets.combineByKey(to_list, append, extend).mapValues(sorted).sortByKey().collect()
[('cat', ['kit', 'tom', 'zoe']), ('dog', ['max', 'rex']), ('fish', ['nemo'])]

>>> (pets.mapValues(lambda v: [v])
...      .reduceByKey(lambda a, b: a + b)
...      .mapValues(sorted)
...      .sortByKey()
...      .collect())
[('cat', ['kit', 'tom', 'zoe']), ('dog', ['max', 'rex']), ('fish', ['nemo'])]
```

**Notes:**

* The `combineByKey()` version is **exactly how PySpark implements
  `groupByKey()`**. Its source code (`pyspark/core/rdd.py`) defines the same three
  functions: `[x]`, `xs.append(x)`, and `a.extend(b)`, and passes them to
  `combineByKey()`.
* **Use `groupByKey()`** when you really want all the values: it's the
  simplest, and Spark optimizes it for exactly this job.
* **The `reduceByKey()` version is the slowest.** `a + b` on lists builds a **new**
  list every time, copying both lists, so a key with *n* values costs about *n²*
  copying steps. `append` and `extend` change the list in place instead.
* Changing the first argument in place and returning it, as `append()` and
  `extend()` do, is safe. Each combiner is owned by a single task, and it saves
  creating a new list each time. Spark's own `groupByKey()` does it, as shown above.
* Collecting **all** the values of a key doesn't shrink the data at all, so all
  three versions shuffle every value. The reducers only save work when the
  combiner is **smaller** than the values (a sum, count, max, set, top-N, ...).

---

## Q4 (Intermediate) — Average per key with combineByKey()

```python
>>> readings = (sc.textFile("data/sensors.txt", 2)
...               .map(lambda line: line.split(","))
...               .map(lambda f: (f[0], int(f[1]))))
>>> readings.count()
12

>>> sum_count = readings.combineByKey(
...     lambda v: (v, 1),
...     lambda c, v: (c[0] + v, c[1] + 1),
...     lambda c1, c2: (c1[0] + c2[0], c1[1] + c2[1]))
>>> sum_count.sortByKey().collect()
[('s1', (84, 4)), ('s2', (120, 4)), ('s3', (68, 4))]

>>> sum_count.mapValues(lambda c: c[0] / c[1]).sortByKey().collect()
[('s1', 21.0), ('s2', 30.0), ('s3', 17.0)]

>>> (readings.mapValues(lambda v: (v, 1))
...          .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
...          .mapValues(lambda c: c[0] / c[1])
...          .sortByKey()
...          .collect())
[('s1', 21.0), ('s2', 30.0), ('s3', 17.0)]

>>> readings.groupByKey().mapValues(lambda vs: sum(vs) / len(vs)).sortByKey().collect()
[('s1', 21.0), ('s2', 30.0), ('s3', 17.0)]
```

**Notes:**

* This is the **classic `combineByKey()` example**. The values are `int`s, and the
  combiner is a `(sum, count)` tuple:
  * `createCombiner(v)` → `(v, 1)`
  * `mergeValue((s, n), v)` → `(s + v, n + 1)`
  * `mergeCombiners((s1, n1), (s2, n2))` → `(s1 + s2, n1 + n2)`
* The `reduceByKey()` version does the same work, but it needs a `mapValues()`
  first to turn every value into a `(v, 1)` tuple, because `reduceByKey()` can't
  change the type. With `combineByKey()`, the tuple is only created **once per key per partition**.
* `groupByKey()` gives the right answer too, but it ships every reading across the
  network. The other two ship one small tuple per key per partition.

---

## Q5 (Intermediate) — Min, max, mean, and standard deviation in one pass

```python
>>> def create(v):
...     return (v, v, 1, v, v * v)
...
>>> def merge_value(c, v):
...     return (min(c[0], v), max(c[1], v), c[2] + 1, c[3] + v, c[4] + v * v)
...
>>> def merge_combiners(a, b):
...     return (min(a[0], b[0]), max(a[1], b[1]), a[2] + b[2], a[3] + b[3], a[4] + b[4])
...
>>> stats = readings.combineByKey(create, merge_value, merge_combiners)
>>> stats.sortByKey().collect()
[('s1', (18, 24, 4, 84, 1784)), ('s2', (27, 33, 4, 120, 3620)), ('s3', (15, 20, 4, 68, 1170))]

>>> import math
>>> def finish(c):
...     lo, hi, n, total, total_sq = c
...     mean = total / n
...     return (lo, hi, n, mean, round(math.sqrt(total_sq / n - mean * mean), 3))
...
>>> stats.mapValues(finish).sortByKey().collect()
[('s1', (18, 24, 4, 21.0, 2.236)), ('s2', (27, 33, 4, 30.0, 2.236)), ('s3', (15, 20, 4, 17.0, 1.871))]
```

**Notes:**

* Each part of the combiner can be combined **on its own**: min with min, max
  with max, and counts and sums by adding. So the whole 5-tuple can be combined
  too. This is the same idea as the `(min, max, sum, count)` tuple in
  [Set 01, Q8](ANSWERS_01.md#q8-intermediate--temperature-statistics-with-dirty-data),
  but now with `combineByKey()`, so there is no need for an extra `mapValues()`.
* A standard deviation needs the mean first, so you might think you need two
  passes. You don't: `variance = E[x²] − (E[x])²`, and `E[x²]` is just
  `sum_of_squares / count`. So the 5 numbers are enough.
* With very large numbers and many values, the `E[x²] − (E[x])²` formula can
  lose precision. Spark's built-in `stats()` uses a more careful method
  (the `StatCounter` class, which updates the mean and variance step by step), but with
  the same idea: a small combiner that can be merged.

---

## Q6 (Intermediate) — Unique values per key

```python
>>> logs = sc.textFile("data/web_logs.txt", 2).map(lambda line: line.split(","))
>>> date_user = logs.filter(lambda f: f[3] == "200").map(lambda f: (f[0], f[1]))

>>> def to_set(v):
...     return {v}
...
>>> def add_to_set(s, v):
...     s.add(v)
...     return s
...
>>> def union_sets(s1, s2):
...     s1 |= s2
...     return s1
...
>>> users_per_day = date_user.combineByKey(to_set, add_to_set, union_sets)
>>> users_per_day.mapValues(sorted).sortByKey().collect()
[('2026-10-01', ['alex', 'bob']), ('2026-10-02', ['alex', 'bob', 'dana', 'erin']), ('2026-10-03', ['carol'])]
>>> users_per_day.mapValues(len).sortByKey().collect()
[('2026-10-01', 2), ('2026-10-02', 4), ('2026-10-03', 1)]

>>> date_user.groupByKey().mapValues(lambda users: len(set(users))).sortByKey().collect()
[('2026-10-01', 2), ('2026-10-02', 4), ('2026-10-03', 1)]

>>> pages_per_user = (logs.map(lambda f: (f[1], f[2]))
...                       .combineByKey(to_set, add_to_set, union_sets)
...                       .mapValues(sorted)
...                       .sortByKey())
>>> pages_per_user.collect()
[('alex', ['/cart', '/home']), ('bob', ['/checkout', '/home']), ('carol', ['/cart', '/home', '/login']), ('dana', ['/cart', '/home']), ('erin', ['/home', '/search']), ('frank', ['/login'])]
```

**Notes:**

* A **`set`** combiner removes duplicates **inside each partition, before the
  shuffle**. alex's two visits on 10-01 become one entry before anything crosses the
  network. `groupByKey()` ships both visits, and removes the duplicate afterwards.
* Set 01 solved the same problem with `distinct()` + `reduceByKey()`. That's often
  even better when there are many unique users per key, because `distinct()` spreads
  the pairs across partitions, while a set combiner has to hold **all** the unique
  users of one key in memory.
* The same three small functions solve task 4 too. Write them once, and reuse
  them for any "unique values per key" problem.

---

## Q7 (Intermediate+) — Predict the combineByKey() calls

```python
>>> pairs = [("a", 1), ("b", 1), ("a", 1), ("a", 1), ("b", 1), ("c", 1)]
>>> n_create = sc.accumulator(0)
>>> n_merge_value = sc.accumulator(0)
>>> n_merge_combiners = sc.accumulator(0)
>>> def create(v):
...     n_create.add(1)
...     return v
...
>>> def merge_value(c, v):
...     n_merge_value.add(1)
...     return c + v
...
>>> def merge_combiners(c1, c2):
...     n_merge_combiners.add(1)
...     return c1 + c2
...

>>> rdd = sc.parallelize(pairs, 2)
>>> rdd.glom().collect()
[[('a', 1), ('b', 1), ('a', 1)], [('a', 1), ('b', 1), ('c', 1)]]
>>> sorted(rdd.combineByKey(create, merge_value, merge_combiners).collect())
[('a', 3), ('b', 2), ('c', 1)]
>>> (n_create.value, n_merge_value.value, n_merge_combiners.value)
(5, 1, 2)

>>> n_create = sc.accumulator(0)
>>> n_merge_value = sc.accumulator(0)
>>> n_merge_combiners = sc.accumulator(0)
>>> sorted(sc.parallelize(pairs, 1).combineByKey(create, merge_value, merge_combiners).collect())
[('a', 3), ('b', 2), ('c', 1)]
>>> (n_create.value, n_merge_value.value, n_merge_combiners.value)
(3, 3, 0)

>>> n_create = sc.accumulator(0)
>>> n_merge_value = sc.accumulator(0)
>>> n_merge_combiners = sc.accumulator(0)
>>> sorted(sc.parallelize(pairs, 6).combineByKey(create, merge_value, merge_combiners).collect())
[('a', 3), ('b', 2), ('c', 1)]
>>> (n_create.value, n_merge_value.value, n_merge_combiners.value)
(6, 0, 3)
```

**Notes, with 2 partitions** `[a, b, a]` and `[a, b, c]`:

| Function | When it's called | Calls |
|---|---|---|
| `createCombiner` | first time a key is seen **in a partition**: a, b in partition 0; a, b, c in partition 1 | **5** |
| `mergeValue` | each later value of that key **in the same partition**: the second `a` in partition 0 | **1** |
| `mergeCombiners` | each extra combiner of a key **after the shuffle**: `a` and `b` came from both partitions, `c` from only one | **2** |

* **1 partition** (3, 3, 0): one combiner per key, all other values merged
  locally, and nothing to merge after the shuffle.
* **6 partitions** (6, 0, 3): each element is alone in its partition, so every
  value creates its own combiner, and all the combining happens after the shuffle:
  2 merges for `a` (3 combiners), 1 for `b`, 0 for `c`.
* The more values per key in each partition, the more work `mergeValue` does
  **before** the shuffle. That's exactly where reducers save network traffic.
* **Why not `sortByKey()`?** `sortByKey()` first runs an **extra job** to sample
  the keys and choose the sort ranges. That job runs the reduce side of
  `combineByKey()` again, so `mergeCombiners` is counted more than once. We saw 6
  instead of 2. The answer doesn't change, but the counts do. This is the same
  accumulator caution as in
  [Set 03, Q3](ANSWERS_03.md#q3-basic--expensive-setup-once-per-partition).

---

## Q8 (Intermediate+) — Top 2 per key without groupByKey()

```python
>>> rows = (sc.textFile("data/class_scores.txt", 2)
...           .map(lambda line: line.split(","))
...           .map(lambda f: (f[0], (f[1], int(f[2])))))

>>> def keep_top2(students):
...     return sorted(students, key=lambda s: (-s[1], s[0]))[:2]
...
>>> top2 = rows.combineByKey(
...     lambda s: [s],
...     lambda c, s: keep_top2(c + [s]),
...     lambda c1, c2: keep_top2(c1 + c2))
>>> top2.sortByKey().collect()
[('A', [('ben', 92), ('dee', 92)]), ('B', [('gus', 95), ('fay', 81)]), ('C', [('ivy', 85), ('kim', 85)])]

>>> rows.groupByKey().mapValues(keep_top2).sortByKey().collect()
[('A', [('ben', 92), ('dee', 92)]), ('B', [('gus', 95), ('fay', 81)]), ('C', [('ivy', 85), ('kim', 85)])]
```

**Notes:**

* The combiner is a list of **at most 2** students. `mergeValue` adds one student
  and keeps the best 2; `mergeCombiners` joins two lists (at most 4 students) and
  keeps the best 2.
* **Memory:** for a class of 10 million students, the `groupByKey()` solution
  must hold all 10 million `(student, score)` pairs of that class in one task's
  memory, and ship them all across the network. The `combineByKey()` solution
  never holds more than 4, and ships at most 2 per partition.
* It's correct for the same reason as the global top-N in
  [Set 03, Q8](ANSWERS_03.md#q8-intermediate--global-top-n): anyone in the
  overall top 2 is also in the top 2 of their own partition.
* The `(-score, name)` sort key gives the same winner whatever order the values
  arrive in. Without the tie-break, `ben` and `dee` (both 92) could swap places
  from run to run.

---

## Q9 (Intermediate+) — When reduceByKey() gives the wrong answer

```python
>>> pairs = [("a", 10), ("a", 20), ("a", 60), ("b", 5), ("b", 5), ("b", 50)]

>>> one = sc.parallelize(pairs, 1)
>>> three = sc.parallelize(pairs, 3)
>>> one.reduceByKey(lambda x, y: (x + y) / 2).sortByKey().collect()
[('a', 37.5), ('b', 27.5)]
>>> three.glom().collect()
[[('a', 10), ('a', 20)], [('a', 60), ('b', 5)], [('b', 5), ('b', 50)]]
>>> three.reduceByKey(lambda x, y: (x + y) / 2).sortByKey().collect()
[('a', 37.5), ('b', 16.25)]

>>> one.reduceByKey(lambda x, y: x - y).sortByKey().collect()
[('a', -70), ('b', -50)]
>>> three.reduceByKey(lambda x, y: x - y).sortByKey().collect()
[('a', -70), ('b', 50)]

>>> (three.combineByKey(lambda v: (v, 1),
...                     lambda c, v: (c[0] + v, c[1] + 1),
...                     lambda c1, c2: (c1[0] + c2[0], c1[1] + c2[1]))
...       .mapValues(lambda c: c[0] / c[1])
...       .sortByKey()
...       .collect())
[('a', 30.0), ('b', 20.0)]
```

**Notes:**

* **"Average of two" is wrong even on 1 partition**: `((10 + 20) / 2 + 60) / 2 = 37.5`,
  not 30. Averaging is not **associative**, so the grouping changes the answer.
  With 3 partitions, `b` is grouped differently, so the answer changes again
  (`5` and `(5 + 50) / 2 = 27.5` → `16.25`): **the result depends on the partitioning**.
* **Subtraction** is neither associative nor commutative. With 3 partitions,
  `b` is `5` in one partition and `5 − 50 = −45` in another, and the merge
  `5 − (−45) = 50` depends on the order the two partial results arrive in. On a
  real cluster it could just as well be `−45 − 5 = −50`. A function like this
  can give **different answers on different runs**.
* Task 4, which functions are safe for `reduceByKey()`:

  | Function | Associative? | Commutative? | Safe? |
  |---|---|---|---|
  | `x + y` (numbers) | yes | yes | ✅ |
  | `x * y` | yes | yes | ✅ |
  | `max(x, y)` | yes | yes | ✅ |
  | `x - y` | no | no | ❌ |
  | `(x + y) / 2` | no | yes | ❌ |
  | `x + y` (joining strings) | yes | **no** | ❌, the order of the parts can change |

* **The fix** is to reduce something that *is* associative and commutative, like
  `(sum, count)`, and compute the average **at the end**, as in Q4.
* Small print: adding **floats** isn't perfectly associative either, because of
  rounding. A float sum can differ in its last digits between partitionings. That
  is usually fine, but don't test float results with `==`.
