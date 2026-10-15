# PySpark RDD Practice Questions — Set 07: Set Operations

Nine practice questions, three at each level, meant to be solved
**interactively** in the `bin/pyspark` shell. This set is about combining two
RDDs **as collections of elements**:

| Operation | Result | Duplicates | Shuffle? |
|---|---|---|---|
| `a.union(b)` | every element of `a`, then every element of `b` | **kept** | **no** |
| `a.intersection(b)` | elements that are in **both** `a` and `b` | **removed** | yes |
| `a.subtract(b)` | elements of `a` that are **not** in `b` | **kept** (from `a`) | yes |

The three operations don't treat duplicates the same way, and that difference
comes up again and again in this set.

| # | Level | Topic | Input |
|---|---|---|---|
| [Q1](#q1-basic--union) | Basic | `union()` | Python lists |
| [Q2](#q2-basic--intersection) | Basic | `intersection()` | Python lists |
| [Q3](#q3-basic--subtract) | Basic | `subtract()` | Python lists |
| [Q4](#q4-intermediate--who-takes-which-course) | Intermediate | Who takes which course? | [`data/enrollments.txt`](data/enrollments.txt) |
| [Q5](#q5-intermediate--customer-retention-from-one-month-to-the-next) | Intermediate | Customer retention from one month to the next | [`data/visitors_sep.txt`](data/visitors_sep.txt), [`data/visitors_oct.txt`](data/visitors_oct.txt) |
| [Q6](#q6-intermediate--set-operations-on-key-value-pairs) | Intermediate | Set operations on `(key, value)` pairs | Python lists |
| [Q7](#q7-intermediate--implement-intersection-and-subtract-yourself) | Intermediate+ | Implement `intersection()` and `subtract()` yourself | Python lists |
| [Q8](#q8-intermediate--document-similarity-jaccard) | Intermediate+ | Document similarity (Jaccard) | [`data/documents.txt`](data/documents.txt) |
| [Q9](#q9-intermediate--when-duplicates-matter-bag-operations) | Intermediate+ | When duplicates matter: "bag" operations | Python lists |

Use **`union()`, `intersection()`, and `subtract()`**, plus everything from the
earlier sets: `map()`, `flatMap()`, `filter()`, `distinct()`, `mapValues()`,
`reduceByKey()`, `sortByKey()`, `join()`, `leftOuterJoin()`, `cogroup()`, `glom()`,
`getNumPartitions()`, `collect()`, `count()`.

Try each question on your own before you open the
[answers](ANSWERS_07.md).

## Files

| File | Purpose |
|---|---|
| [QUESTIONS_07.md](QUESTIONS_07.md) | The nine questions (this page) |
| [ANSWERS_07.md](ANSWERS_07.md) | Step-by-step solutions, with real output from Spark 4.2.0 |
| [data/enrollments.txt](data/enrollments.txt) | Input for Q4 (also used in Set 02): `student:course1,course2,...` |
| [data/visitors_sep.txt](data/visitors_sep.txt) | Input for Q5: one customer id per visit in September |
| [data/visitors_oct.txt](data/visitors_oct.txt) | Input for Q5: one customer id per visit in October |
| [data/documents.txt](data/documents.txt) | Input for Q8 (also used in Set 02): `doc_id\|text` |

## Getting started

Start `$SPARK_HOME/bin/pyspark` **from this folder**, as in
[Set 01](QUESTIONS_01.md#getting-started), and give the number of partitions
explicitly, as in the earlier sets.

The order of the results of `intersection()` and `subtract()` is not fixed, so
use Python's `sorted(rdd.collect())` to compare with the expected answers.

---

## Q1 (Basic) — union()

**Input:**

```python
a = sc.parallelize([1, 2, 3, 4, 4], 2)
b = sc.parallelize([3, 4, 5, 6], 3)
```

**Tasks:**

1. Compute `a.union(b)`. Collect it, and count it.
2. How many partitions does the union have? Show them with `glom()`.
3. Find the **unique** elements of the union, sorted.
4. Is `b.union(a)` the same as `a.union(b)`? Compare them sorted.

**Expected output:**

```
[1, 2, 3, 4, 4, 3, 4, 5, 6]
9
5
[[1, 2], [3, 4, 4], [3], [4], [5, 6]]
[1, 2, 3, 4, 5, 6]
True
```

*Think about it:* the union has exactly `2 + 3` partitions, holding the same
elements as before. What does that tell you about how much work `union()` does?

---

## Q2 (Basic) — intersection()

**Input:** the same `a` and `b` as in Q1.

**Tasks:**

1. Find the elements that are in **both** `a` and `b`, sorted. (`4` appears twice in `a`.
   How many times does it appear in the result?)
2. Compute `a.intersection(a)`, sorted. What does it equal?
3. Intersect `a` with `sc.parallelize([9, 10], 2)`.

**Expected output:**

```
[3, 4]
[1, 2, 3, 4]
[]
```

---

## Q3 (Basic) — subtract()

**Input:** the same `a` and `b` as in Q1, plus:

```python
dup = sc.parallelize([1, 1, 2, 2, 7], 2)
two = sc.parallelize([2], 1)
```

**Tasks:**

1. Compute `a.subtract(b)` and `b.subtract(a)`, each sorted. Are they the same?
2. Compute `dup.subtract(two)`, sorted. How many `1`s are left? How many `2`s?
3. The **symmetric difference** is the set of elements in **exactly one** of the two RDDs.
   Compute it for `a` and `b`, sorted.

**Expected output:**

```
[1, 2]
[5, 6]
[1, 1, 7]
[1, 2, 5, 6]
```

---

## Q4 (Intermediate) — Who takes which course?

**Input:** [`data/enrollments.txt`](data/enrollments.txt), read with **2** partitions,
as `student:course1,course2,...` (the same file as Set 02, Q6).

**Tasks:**

1. Make an RDD of the students who take `math`, and one of the students who take
   `art`. Show both, sorted.
2. Which students take **both** math and art?
3. Which students take **math or art** (or both)? List each student once.
4. Which students take math but **not** art?
5. Which students take **exactly one** of the two?
6. Which students take **neither**?

**Expected output:**

```
['alice', 'bob', 'carol', 'frank']
['alice', 'carol', 'erin']
['alice', 'carol']
['alice', 'bob', 'carol', 'erin', 'frank']
['bob', 'frank']
['bob', 'erin', 'frank']
['dan']
```

---

## Q5 (Intermediate) — Customer retention from one month to the next

**Input:** [`data/visitors_sep.txt`](data/visitors_sep.txt) and
[`data/visitors_oct.txt`](data/visitors_oct.txt), each read with **2** partitions.
Each line is the customer id of **one visit**, so a customer who visited twice
appears twice.

**Tasks:**

1. Count the visits in each month. Then count the **unique** customers in each month.
2. **Retained** customers visited in **both** months. Who are they?
3. **Churned** customers visited in September but **not** in October. Who are they?
4. **New** customers visited in October but **not** in September. Who are they?
5. How many unique customers visited **at all** over the two months?
6. What's the **retention rate**: retained customers ÷ September's unique customers,
   as a percentage?
7. What do you get for task 3 if you forget to remove the duplicates first, and
   subtract the raw visit RDDs?

**Expected output:**

```
8
8
6
6
['c1', 'c2', 'c4']
['c3', 'c5', 'c6']
['c7', 'c8', 'c9']
9
50%
['c3', 'c5', 'c6']
```

*Think about it:* task 7 gives the same answer as task 3 here. Is that always
true? What about `intersection()` on the raw visits?

---

## Q6 (Intermediate) — Set operations on (key, value) pairs

**Input:** the stock of two warehouses, as `(item, quantity)` pairs:

```python
wh1 = sc.parallelize([("apple", 5), ("pear", 3), ("kiwi", 2)], 2)
wh2 = sc.parallelize([("apple", 3), ("pear", 3), ("plum", 4)], 2)
```

**Tasks:**

1. Find the **total stock per item** over both warehouses.
2. Compute `wh1.intersection(wh2)`. What does it really find?
3. Compute `wh1.subtract(wh2)`, sorted. Why is `apple` in the result?
4. Which **items** are in warehouse 1 but not in warehouse 2? Which items are in both?
5. For the items **only** in warehouse 1, show `(item, quantity)`.

**Expected output:**

```
[('apple', 8), ('kiwi', 2), ('pear', 6), ('plum', 4)]
[('pear', 3)]
[('apple', 5), ('kiwi', 2)]
['kiwi']
['apple', 'pear']
[('kiwi', 2)]
```

---

## Q7 (Intermediate+) — Implement intersection() and subtract() yourself

Suppose `intersection()` and `subtract()` were not available, but `cogroup()` was.

**Tasks:**

1. Write `my_intersection(x, y)` using `map()`, `cogroup()`, `filter()`, and `map()`.
   Compare it with `a.intersection(b)` on `a` and `b` from Q1.
2. Write `my_subtract(x, y)`. Compare it with `a.subtract(b)`.
3. Your `my_subtract` must keep duplicates the way `subtract()` does. Check it on
   `dup` and `two` from Q3, and show the result.
4. Check that `my_intersection` matches `intersection()` on the **raw** September
   and October visits from Q5, with all their duplicates.

**Expected output** (in tasks 1 and 2, mine first, then the built-in one):

```
[3, 4]
[3, 4]
[1, 2]
[1, 2]
True
[1, 1, 7]
True
```

**Hint:** as in [Set 02, Q7](QUESTIONS_02.md#q7-intermediate--implement-distinct-yourself),
make every element a key: `(x, None)`. After `cogroup()`, each element has a list of
its copies in `x` and a list of its copies in `y`.

---

## Q8 (Intermediate+) — Document similarity (Jaccard)

The **Jaccard similarity** of two sets `A` and `B` is `|A ∩ B| / |A ∪ B|`: the
number of shared items divided by the number of items in either set. It goes from
`0` (nothing shared) to `1` (identical sets).

**Input:** [`data/documents.txt`](data/documents.txt), read with **2** partitions,
as `doc_id|text`.

**Tasks:**

1. Write a function `words(doc_id)` that returns an RDD of the **unique** words of
   one document. Show the sorted words of `d1`.
2. For `d1` and `d2`, show the shared words (sorted), the number of shared words,
   the number of words in either document, and their Jaccard similarity (3 decimals).
3. Compute the Jaccard similarity for **every pair** of documents.
4. Which pairs of documents are the **most similar**?

**Expected output:**

```
['big', 'data', 'makes', 'simple', 'spark']
['big', 'data']
2
8
0.25
[(('d1', 'd2'), 0.25), (('d1', 'd3'), 0.111), (('d1', 'd4'), 0.222), (('d2', 'd3'), 0.25), (('d2', 'd4'), 0.1), (('d3', 'd4'), 0.1)]
[(('d1', 'd2'), 0.25), (('d2', 'd3'), 0.25)]
```

---

## Q9 (Intermediate+) — When duplicates matter: "bag" operations

A **bag** (or *multiset*) is like a set, but it counts how many times each element
appears. Think of the items in two shopping baskets.

**Input:**

```python
bag1 = sc.parallelize(["x", "x", "x", "y", "z", "z"], 2)
bag2 = sc.parallelize(["x", "z", "z", "z", "w"], 2)
```

**Tasks:**

1. Count `bag1.union(bag2)`.
2. Compute `bag1.intersection(bag2)` and `bag1.subtract(bag2)`, sorted.
3. Count each element in `bag1` and in `bag2`, as `(element, count)` pairs, sorted.
4. **Bag intersection**: each element as many times as it appears in **both** bags,
   that is, `min(count in bag1, count in bag2)` times. Compute it, sorted.
5. **Bag difference**: each element of `bag1` as many times as it appears in `bag1`
   **beyond** its count in `bag2`, that is, `max(0, count in bag1 − count in bag2)` times.
   Compute it, sorted.
6. Check tasks 4 and 5 with Python's `collections.Counter`, which supports `&` and `-`
   for bags.

**Expected output:**

```
11
['x', 'z']
['y']
[('x', 3), ('y', 1), ('z', 2)]
[('w', 1), ('x', 1), ('z', 3)]
['x', 'z', 'z']
['x', 'x', 'y']
['x', 'z', 'z']
['x', 'x', 'y']
```

**Hint:** after counting, the two bags are small `(element, count)` RDDs. Which join
from [Set 05](QUESTIONS_05.md) keeps the elements that are only in `bag1`? Then use
`flatMap()` to repeat each element the right number of times.
