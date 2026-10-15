# PySpark RDD Practice Questions — Set 06: aggregateByKey()

Nine practice questions, three at each level, meant to be solved
**interactively** in the `bin/pyspark` shell. This set is about one reducer,
`aggregateByKey()`:

```python
rdd.aggregateByKey(zeroValue, seqFunc, combFunc)
```

* `zeroValue`: the **starting value** of the result (the "accumulator") for a key.
* `seqFunc(acc, v)`: adds **one value** `v` into an accumulator, within a partition.
* `combFunc(acc1, acc2)`: merges two accumulators for the same key from
  **different partitions**.

Like `combineByKey()` (from [Set 04](QUESTIONS_04.md)), the result type can
differ from the value type. The difference is that instead of a
`createCombiner()` function, you give a **starting value**. Most of the time
that's simpler, but the starting value has a trap of its own (Q7).

| # | Level | Topic | Input |
|---|---|---|---|
| [Q1](#q1-basic--sum-and-product-per-key) | Basic | Sum and product per key | Python list |
| [Q2](#q2-basic--count-per-key) | Basic | Count per key | Python list |
| [Q3](#q3-basic--max-and-min-per-key-choosing-the-zero-value) | Basic | Max and min per key: choosing the zero value | Python lists |
| [Q4](#q4-intermediate--average-per-key) | Intermediate | Average per key | [`data/sensors.txt`](data/sensors.txt) |
| [Q5](#q5-intermediate--unique-values-per-key-with-a-set) | Intermediate | Unique values per key with a set | [`data/web_logs.txt`](data/web_logs.txt) |
| [Q6](#q6-intermediate--a-rating-histogram-per-movie) | Intermediate | A rating histogram per movie | [`data/movie_ratings.txt`](data/movie_ratings.txt) |
| [Q7](#q7-intermediate--the-zero-value-trap) | Intermediate+ | The zero-value trap | Python list |
| [Q8](#q8-intermediate--a-full-report-per-key-in-one-pass) | Intermediate+ | A full report per key in one pass | [`data/sales.txt`](data/sales.txt) |
| [Q9](#q9-intermediate--implement-aggregatebykey-yourself) | Intermediate+ | Implement `aggregateByKey()` yourself | Python list |

Use **`aggregateByKey()`**, plus helpers from the earlier sets to prepare, check,
and show the data: `sc.parallelize()`, `sc.textFile()`, `map()`, `mapValues()`,
`filter()`, `reduceByKey()`, `combineByKey()`, `sortByKey()`, `glom()`,
`collect()`, `count()`.

Try each question on your own before you open the
[answers](ANSWERS_06.md).

## Files

| File | Purpose |
|---|---|
| [QUESTIONS_06.md](QUESTIONS_06.md) | The nine questions (this page) |
| [ANSWERS_06.md](ANSWERS_06.md) | Step-by-step solutions, with real output from Spark 4.2.0 |
| [data/sensors.txt](data/sensors.txt) | Input for Q4 (also used in Set 04): `sensor_id,temperature` |
| [data/web_logs.txt](data/web_logs.txt) | Input for Q5 (also used in Sets 01, 02, 04): `date,user,url,http_status` |
| [data/movie_ratings.txt](data/movie_ratings.txt) | Input for Q6 (also used in Set 01): `user,movie,rating` |
| [data/sales.txt](data/sales.txt) | Input for Q8 (also used in Set 01): `store,product,price` |

## Getting started

Start `$SPARK_HOME/bin/pyspark` **from this folder**, as in
[Set 01](QUESTIONS_01.md#getting-started). As in Sets 03 and 04, **always give
the number of partitions** (`sc.parallelize(data, 2)`, `sc.textFile(path, 2)`).
In this set it really matters: Q7 shows an answer that **changes** with the number
of partitions.

---

## Q1 (Basic) — Sum and product per key

**Input:**

```python
sales = sc.parallelize([("apple", 3), ("banana", 2), ("apple", 5),
                        ("cherry", 7), ("banana", 4), ("apple", 1)], 2)
```

**Tasks:**

1. Find the **total per fruit** with `aggregateByKey()`, sorted by key.
2. Check it with `reduceByKey()`.
3. Find the **product** of the values per fruit (e.g. apple: 3 × 5 × 1).
   What zero value do you need?

**Expected output:**

```
[('apple', 9), ('banana', 6), ('cherry', 7)]
[('apple', 9), ('banana', 6), ('cherry', 7)]
[('apple', 15), ('banana', 8), ('cherry', 7)]
```

---

## Q2 (Basic) — Count per key

**Input** (a list of `(species, pet_name)` pairs):

```python
pets = sc.parallelize([("cat", "tom"), ("dog", "rex"), ("cat", "kit"),
                       ("fish", "nemo"), ("dog", "max"), ("cat", "zoe")], 2)
```

**Tasks:**

1. Find the **number of pets per species** with `aggregateByKey()`, without a
   `mapValues()` first.
2. Find the **total number of letters** in the pet names of each species
   (cat: `tom` + `kit` + `zoe` = 9 letters).

**Expected output:**

```
[('cat', 3), ('dog', 2), ('fish', 1)]
[('cat', 9), ('dog', 6), ('fish', 4)]
```

*Think about it:* in task 1, `seqFunc` and `combFunc` are **different** functions.
Why can't they be the same, as they are in Q1?

---

## Q3 (Basic) — Max and min per key: choosing the zero value

**Input:** the `sales` RDD from Q1, and an RDD with **negative** values:

```python
neg = sc.parallelize([("t", -5), ("t", -2), ("u", -9)], 2)
```

**Tasks:**

1. Find the **largest value per fruit** in `sales` with `aggregateByKey()`.
2. Find the **(smallest, largest)** value per fruit in one `aggregateByKey()`.
3. Someone uses `0` as the zero value for the maximum. Try
   `aggregateByKey(0, max, max)` on `sales`, and then on `neg`. What goes wrong?
   Fix it.

**Expected output:**

```
[('apple', 5), ('banana', 4), ('cherry', 7)]
[('apple', (1, 5)), ('banana', (2, 4)), ('cherry', (7, 7))]
[('apple', 5), ('banana', 4), ('cherry', 7)]
[('t', 0), ('u', 0)]
[('t', -2), ('u', -9)]
```

**Hint:** Python's `float("-inf")` is smaller than every number, and
`float("inf")` is larger than every number.

---

## Q4 (Intermediate) — Average per key

**Input:** [`data/sensors.txt`](data/sensors.txt), read with **2** partitions, as
`(sensor, temperature)` pairs with the temperature as an `int` (the same data as
[Set 04, Q4](QUESTIONS_04.md#q4-intermediate--average-per-key-with-combinebykey)).

**Tasks:**

1. Use `aggregateByKey()` with the zero value `(0, 0)` to build a `(sum, count)`
   pair for each sensor.
2. Turn it into the **average per sensor**.

**Expected output:**

```
[('s1', (84, 4)), ('s2', (120, 4)), ('s3', (68, 4))]
[('s1', 21.0), ('s2', 30.0), ('s3', 17.0)]
```

*Think about it:* compare your solution with the `combineByKey()` solution in
Set 04. Which do you find easier to read?

---

## Q5 (Intermediate) — Unique values per key with a set

**Input:** [`data/web_logs.txt`](data/web_logs.txt), read with **2** partitions, as
`date,user,url,http_status`.

**Tasks:**

1. Use `aggregateByKey()` with the zero value `set()` to find the **unique pages**
   each user visited (all requests). Show them as sorted lists, by user.
2. For the successful requests (status `200`), find the **number of distinct
   visitors per day**, again with a `set()` zero value.

**Expected output:**

```
[('alex', ['/cart', '/home']), ('bob', ['/checkout', '/home']), ('carol', ['/cart', '/home', '/login']), ('dana', ['/cart', '/home']), ('erin', ['/home', '/search']), ('frank', ['/login'])]
[('2026-10-01', 2), ('2026-10-02', 4), ('2026-10-03', 1)]
```

*Think about it:* your `seqFunc` probably **changes** the set (`s.add(v)`). But the
same `set()` object is the zero value for **every** key. Why don't all the users
end up sharing one set? (Q9 answers this.)

---

## Q6 (Intermediate) — A rating histogram per movie

**Input:** [`data/movie_ratings.txt`](data/movie_ratings.txt), read with **2**
partitions, as `user,movie,rating` with ratings from 1 to 5.

**Tasks:**

1. For each movie, build a **histogram** of its ratings: a list of 5 counts, where
   position 0 is the number of 1-star ratings, position 1 the number of 2-star
   ratings, and so on. Use the zero value `[0, 0, 0, 0, 0]`.
2. From each histogram, compute the **average rating**, rounded to 2 decimals.
3. From each histogram, find the **most common rating**. If two ratings are tied,
   take the lower one.

**Expected output:**

```
[('Coco', [0, 0, 1, 0, 1]), ('Inception', [0, 0, 1, 2, 1]), ('Titanic', [1, 1, 0, 1, 0]), ('Up', [0, 0, 0, 1, 2])]
[('Coco', 4.0), ('Inception', 4.0), ('Titanic', 2.33), ('Up', 4.67)]
[('Coco', 3), ('Inception', 4), ('Titanic', 1), ('Up', 5)]
```

---

## Q7 (Intermediate+) — The zero-value trap

**Input:**

```python
data = [("a", 1), ("a", 2), ("b", 3), ("a", 4)]
```

A student wants "the total per key, **plus a bonus of 10**", and writes:

```python
rdd.aggregateByKey(10, lambda acc, v: acc + v, lambda x, y: x + y)
```

**Tasks:**

1. What answer does the student **expect**? (`a` = 1 + 2 + 4 + 10 = 17, `b` = 13.)
2. Run it on `sc.parallelize(data, n)` for `n` = 1, 2, and 4. For each `n`, show
   the partitions and the result.
3. Explain the results. **How many times** is the zero value used for each key?
4. What must be true of a zero value for `aggregateByKey()` to give the same answer
   on any number of partitions? Fix the student's code.

**Expected output** (one partition list and one result for each `n`):

```
[[('a', 1), ('a', 2), ('b', 3), ('a', 4)]]
[('a', 17), ('b', 13)]
[[('a', 1), ('a', 2)], [('b', 3), ('a', 4)]]
[('a', 27), ('b', 13)]
[[('a', 1)], [('a', 2)], [('b', 3)], [('a', 4)]]
[('a', 37), ('b', 13)]
```

---

## Q8 (Intermediate+) — A full report per key in one pass

**Input:** [`data/sales.txt`](data/sales.txt), read with **2** partitions. Each line
is `store,product,price`. Read it into `(store, (product, price))` pairs, with the
price as a `float`.

**Task:** with **one** `aggregateByKey()`, build this report for each store:

* the **total** sales (rounded to 2 decimals in the final output);
* the **number of items** sold;
* the **sorted list of different products** sold;
* the **most expensive item** sold, as `(product, price)`.

**Expected output:**

```
[('S1', (13.75, 4, ['apple', 'bread', 'milk'], ('milk', 4.0))), ('S2', (9.85, 3, ['apple', 'banana', 'eggs'], ('eggs', 5.1))), ('S3', (8.0, 3, ['banana', 'bread', 'milk'], ('milk', 4.0)))]
```

**Hint:** use a 4-part tuple as the accumulator, with the zero value
`(0.0, 0, set(), None)`. `None` means "no item yet". `seqFunc` adds one item, and
`combFunc` merges two reports, each part with its own rule.

---

## Q9 (Intermediate+) — Implement aggregateByKey() yourself

Suppose `aggregateByKey()` was not available, but `combineByKey()` was.

**Tasks:**

1. Write `my_aggregate_by_key(rdd, zero, seq_func, comb_func)` using
   `combineByKey()`. Test it on Q1: the total per fruit in `sales`.
2. Now test it with a **mutable** zero value: build the list of names per species
   in `pets` (from Q2) with the zero value `[]`, `append` as `seq_func`, and `extend`
   as `comb_func`. Compare with the built-in `aggregateByKey()`.
3. If your answers differ in task 2, find the bug and fix it.

**Expected output** (task 1; then for task 2, the built-in `aggregateByKey()`
and a **correct** `my_aggregate_by_key()`):

```
[('apple', 9), ('banana', 6), ('cherry', 7)]
[('cat', ['kit', 'tom', 'zoe']), ('dog', ['max', 'rex']), ('fish', ['nemo'])]
[('cat', ['kit', 'tom', 'zoe']), ('dog', ['max', 'rex']), ('fish', ['nemo'])]
```

**Hint:** where does the zero value go in `combineByKey()`? And what happens
if every key **starts from the same list object**?
