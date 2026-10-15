# PySpark RDD Practice Questions — Set 04: Reducers

Nine practice questions, three at each level, meant to be solved
**interactively** in the `bin/pyspark` shell. This set is only about the
three main **reducers** for `(key, value)` RDDs:

| Reducer | What you give it | What it does |
|---|---|---|
| `groupByKey()` | nothing | Collects **all** the values of each key into one `ResultIterable`. |
| `reduceByKey(f)` | `f(v1, v2) -> v` | Combines two values into one value **of the same type**, again and again, until one is left per key. |
| `combineByKey(create, merge_value, merge_combiners)` | 3 functions | The most general one. The result (the **combiner**) can be a **different type** than the values. |

`combineByKey()` takes three functions:

* `createCombiner(v)`: turns the **first** value of a key, *in a partition*,
  into a combiner.
* `mergeValue(c, v)`: adds **another** value from the same partition into the
  combiner.
* `mergeCombiners(c1, c2)`: merges two combiners for the same key that came from
  **different partitions** (after the shuffle).

| # | Level | Topic | Input |
|---|---|---|---|
| [Q1](#q1-basic--sum-and-max-per-key-three-ways) | Basic | Sum and max per key, three ways | Python list |
| [Q2](#q2-basic--count-per-key-three-ways) | Basic | Count per key, three ways | Python list |
| [Q3](#q3-basic--list-of-values-per-key-three-ways) | Basic | List of values per key, three ways | Python list |
| [Q4](#q4-intermediate--average-per-key-with-combinebykey) | Intermediate | Average per key with `combineByKey()` | [`data/sensors.txt`](data/sensors.txt) |
| [Q5](#q5-intermediate--min-max-mean-and-standard-deviation-in-one-pass) | Intermediate | Min, max, mean, and standard deviation in one pass | [`data/sensors.txt`](data/sensors.txt) |
| [Q6](#q6-intermediate--unique-values-per-key) | Intermediate | Unique values per key | [`data/web_logs.txt`](data/web_logs.txt) |
| [Q7](#q7-intermediate--predict-the-combinebykey-calls) | Intermediate+ | Predict the `combineByKey()` calls | Python list |
| [Q8](#q8-intermediate--top-2-per-key-without-groupbykey) | Intermediate+ | Top 2 per key without `groupByKey()` | [`data/class_scores.txt`](data/class_scores.txt) |
| [Q9](#q9-intermediate--when-reducebykey-gives-the-wrong-answer) | Intermediate+ | When `reduceByKey()` gives the wrong answer | Python list |

Use the **three reducers above**, plus these helpers to prepare and show the
data: `sc.parallelize()`, `sc.textFile()`, `map()`, `mapValues()`, `filter()`,
`sortByKey()`, `glom()`, `collect()`, `count()`, and `sc.accumulator()` (from
[Set 03](QUESTIONS_03.md)).

Try each question on your own before you open the
[answers](ANSWERS_04.md).

## Files

| File | Purpose |
|---|---|
| [QUESTIONS_04.md](QUESTIONS_04.md) | The nine questions (this page) |
| [ANSWERS_04.md](ANSWERS_04.md) | Step-by-step solutions, with real output from Spark 4.2.0 |
| [data/sensors.txt](data/sensors.txt) | Input for Q4 and Q5: `sensor_id,temperature` |
| [data/web_logs.txt](data/web_logs.txt) | Input for Q6 (also used in Sets 01 and 02): `date,user,url,http_status` |
| [data/class_scores.txt](data/class_scores.txt) | Input for Q8 (also used in Set 02): `class,student,score` |

## Getting started

Start `$SPARK_HOME/bin/pyspark` **from this folder**, as in
[Set 01](QUESTIONS_01.md#getting-started). As in Set 03, **always give the
number of partitions** (`sc.parallelize(data, 2)`, `sc.textFile(path, 2)`), so
your output matches the expected answers.

---

## Q1 (Basic) — Sum and max per key, three ways

**Input:**

```python
sales = sc.parallelize([("apple", 3), ("banana", 2), ("apple", 5),
                        ("cherry", 7), ("banana", 4), ("apple", 1)], 2)
```

**Tasks:**

1. Show the partitions with `glom()`.
2. Find the **total per fruit** three times: with `reduceByKey()`, with
   `groupByKey()`, and with `combineByKey()`. Sort each result by key.
3. Do the same for the **largest value per fruit**.

**Expected output:**

```
[[('apple', 3), ('banana', 2), ('apple', 5)], [('cherry', 7), ('banana', 4), ('apple', 1)]]
[('apple', 9), ('banana', 6), ('cherry', 7)]
[('apple', 9), ('banana', 6), ('cherry', 7)]
[('apple', 9), ('banana', 6), ('cherry', 7)]
[('apple', 5), ('banana', 4), ('cherry', 7)]
[('apple', 5), ('banana', 4), ('cherry', 7)]
[('apple', 5), ('banana', 4), ('cherry', 7)]
```

---

## Q2 (Basic) — Count per key, three ways

**Input** (a list of `(species, pet_name)` pairs):

```python
pets = sc.parallelize([("cat", "tom"), ("dog", "rex"), ("cat", "kit"),
                       ("fish", "nemo"), ("dog", "max"), ("cat", "zoe")], 2)
```

**Tasks:** find the **number of pets per species** three ways: with
`reduceByKey()`, with `groupByKey()`, and with `combineByKey()`. Sort each result by key.

**Expected output:**

```
[('cat', 3), ('dog', 2), ('fish', 1)]
[('cat', 3), ('dog', 2), ('fish', 1)]
[('cat', 3), ('dog', 2), ('fish', 1)]
```

**Hint:** the values here are names (strings), but the counts are numbers.
`reduceByKey()` needs a `mapValues()` first. `combineByKey()` doesn't. Why not?

---

## Q3 (Basic) — List of values per key, three ways

**Input:** the same `pets` RDD as in Q2.

**Tasks:** for each species, find the **sorted list of pet names**, three ways:

1. With `groupByKey()`.
2. With `combineByKey()`, building a Python list yourself.
3. With `reduceByKey()`. (Hint: turn each value into a one-element list first.)

**Expected output** (all three are the same):

```
[('cat', ['kit', 'tom', 'zoe']), ('dog', ['max', 'rex']), ('fish', ['nemo'])]
[('cat', ['kit', 'tom', 'zoe']), ('dog', ['max', 'rex']), ('fish', ['nemo'])]
[('cat', ['kit', 'tom', 'zoe']), ('dog', ['max', 'rex']), ('fish', ['nemo'])]
```

*Think about it:* all three give the same answer. Which one would you use, and
which one is the slowest?

---

## Q4 (Intermediate) — Average per key with combineByKey()

**Input:** [`data/sensors.txt`](data/sensors.txt), read with **2** partitions.
Each line is one temperature reading, as `sensor_id,temperature`:

```
s1,20
s2,31
s1,22
...
```

**Tasks:**

1. Read the file into `(sensor, temperature)` pairs, with the temperature as an
   `int`. Count them.
2. Use `combineByKey()` to build a `(sum, count)` combiner for each sensor.
3. Turn the combiners into the **average per sensor**.
4. Check your answer with `reduceByKey()` and with `groupByKey()`.

**Expected output:**

```
12
[('s1', (84, 4)), ('s2', (120, 4)), ('s3', (68, 4))]
[('s1', 21.0), ('s2', 30.0), ('s3', 17.0)]
[('s1', 21.0), ('s2', 30.0), ('s3', 17.0)]
[('s1', 21.0), ('s2', 30.0), ('s3', 17.0)]
```

---

## Q5 (Intermediate) — Min, max, mean, and standard deviation in one pass

**Input:** the same `(sensor, temperature)` pairs as in Q4.

**Tasks:**

1. With **one** `combineByKey()`, build a combiner
   `(min, max, count, sum, sum_of_squares)` for each sensor.
2. Use `mapValues()` to turn it into `(min, max, count, mean, std_dev)`, with
   the standard deviation rounded to 3 decimals.

**Hint:** the (population) standard deviation is `sqrt(sum_of_squares / count − mean²)`.
You don't need to keep the values, just these 5 numbers.

**Expected output:**

```
[('s1', (18, 24, 4, 84, 1784)), ('s2', (27, 33, 4, 120, 3620)), ('s3', (15, 20, 4, 68, 1170))]
[('s1', (18, 24, 4, 21.0, 2.236)), ('s2', (27, 33, 4, 30.0, 2.236)), ('s3', (15, 20, 4, 17.0, 1.871))]
```

---

## Q6 (Intermediate) — Unique values per key

**Input:** [`data/web_logs.txt`](data/web_logs.txt), read with **2** partitions,
as `date,user,url,http_status` (the same file as in Sets 01 and 02).

**Tasks:**

1. For the successful requests (status `200`), use `combineByKey()` with a
   Python **`set`** as the combiner to find the **unique users per day**. Show them
   as sorted lists.
2. Turn that into the **number of distinct visitors per day**. (You should get
   the same answer as [Set 01, Q7](QUESTIONS_01.md#q7-intermediate--distinct-visitors-per-day).)
3. Get the same counts with `groupByKey()`.
4. Using **all** the requests, find the sorted list of **unique pages** each user visited.

**Expected output:**

```
[('2026-10-01', ['alex', 'bob']), ('2026-10-02', ['alex', 'bob', 'dana', 'erin']), ('2026-10-03', ['carol'])]
[('2026-10-01', 2), ('2026-10-02', 4), ('2026-10-03', 1)]
[('2026-10-01', 2), ('2026-10-02', 4), ('2026-10-03', 1)]
[('alex', ['/cart', '/home']), ('bob', ['/checkout', '/home']), ('carol', ['/cart', '/home', '/login']), ('dana', ['/cart', '/home']), ('erin', ['/home', '/search']), ('frank', ['/login'])]
```

---

## Q7 (Intermediate+) — Predict the combineByKey() calls

**Input:**

```python
pairs = [("a", 1), ("b", 1), ("a", 1), ("a", 1), ("b", 1), ("c", 1)]
```

We count the calls of each of the three functions with accumulators:

```python
n_create = sc.accumulator(0)
n_merge_value = sc.accumulator(0)
n_merge_combiners = sc.accumulator(0)

def create(v):
    n_create.add(1)
    return v

def merge_value(c, v):
    n_merge_value.add(1)
    return c + v

def merge_combiners(c1, c2):
    n_merge_combiners.add(1)
    return c1 + c2
```

**Tasks:**

1. Make `rdd = sc.parallelize(pairs, 2)` and show its partitions.
2. **Before you run anything**, predict how many times each of the three functions
   will be called by `rdd.combineByKey(create, merge_value, merge_combiners)`.
3. Run it (use `sorted(... .collect())`, **not** `sortByKey()`), then print the three counts as a tuple.
4. Reset the counters and repeat with **1** partition, and then with **6** partitions.
   Predict first each time.

**Expected output:**

```
[[('a', 1), ('b', 1), ('a', 1)], [('a', 1), ('b', 1), ('c', 1)]]
[('a', 3), ('b', 2), ('c', 1)]
(5, 1, 2)
[('a', 3), ('b', 2), ('c', 1)]
(3, 3, 0)
[('a', 3), ('b', 2), ('c', 1)]
(6, 0, 3)
```

*Think about it:* why does the problem say not to use `sortByKey()` here? (Try it
and look at the counts.)

---

## Q8 (Intermediate+) — Top 2 per key without groupByKey()

**Input:** [`data/class_scores.txt`](data/class_scores.txt), read with **2**
partitions, as `class,student,score` (the same file as
[Set 02, Q9](QUESTIONS_02.md#q9-intermediate--top-2-students-per-class)).

**Tasks:**

1. Read the file into `(class, (student, score))` pairs.
2. With `combineByKey()`, find the **top 2 students per class** (highest score
   first, ties in alphabetical order). The combiner must **never hold more than
   2 students**.
3. Check your answer against a `groupByKey()` solution.

**Expected output:**

```
[('A', [('ben', 92), ('dee', 92)]), ('B', [('gus', 95), ('fay', 81)]), ('C', [('ivy', 85), ('kim', 85)])]
[('A', [('ben', 92), ('dee', 92)]), ('B', [('gus', 95), ('fay', 81)]), ('C', [('ivy', 85), ('kim', 85)])]
```

*Think about it:* a class with 10 million students: how much memory does each
solution need for that one key?

---

## Q9 (Intermediate+) — When reduceByKey() gives the wrong answer

`reduceByKey(f)` combines values in **whatever grouping the partitions give it**.
It's only correct if `f` doesn't care about grouping (**associative**) or order
(**commutative**).

**Input:**

```python
pairs = [("a", 10), ("a", 20), ("a", 60), ("b", 5), ("b", 5), ("b", 50)]
```

The correct averages are `a = 30.0` and `b = 20.0`.

**Tasks:**

1. Try to compute the average with `reduceByKey(lambda x, y: (x + y) / 2)` on
   `sc.parallelize(pairs, 1)`, then on `sc.parallelize(pairs, 3)`. Show the
   partitions of the 3-partition RDD. Are the answers right? Are they even the same?
2. Try `reduceByKey(lambda x, y: x - y)` with 1 and with 3 partitions.
3. Compute the correct averages with `combineByKey()`, on 3 partitions.
4. For each of these functions, say whether it's safe for `reduceByKey()`:
   `x + y`, `x * y`, `max(x, y)`, `x - y`, `(x + y) / 2`, and string joining `x + y`.

**Expected output** (tasks 1–3):

```
[('a', 37.5), ('b', 27.5)]
[[('a', 10), ('a', 20)], [('a', 60), ('b', 5)], [('b', 5), ('b', 50)]]
[('a', 37.5), ('b', 16.25)]
[('a', -70), ('b', -50)]
[('a', -70), ('b', 50)]
[('a', 30.0), ('b', 20.0)]
```
