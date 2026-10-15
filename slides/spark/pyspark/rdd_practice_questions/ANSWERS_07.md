# PySpark RDD Practice Questions — Set 07: Answers

Solutions to the nine [Set 07 practice questions](QUESTIONS_07.md). Every
session below was run in the `bin/pyspark` shell (Spark 4.2.0), started from
this folder. The output is copied from those real runs.

Try the questions yourself first. There are often several correct
solutions, and yours doesn't need to match these exactly.

* [Q1 (Basic): union()](#q1-basic--union)
* [Q2 (Basic): intersection()](#q2-basic--intersection)
* [Q3 (Basic): subtract()](#q3-basic--subtract)
* [Q4 (Intermediate): Who takes which course?](#q4-intermediate--who-takes-which-course)
* [Q5 (Intermediate): Customer retention from one month to the next](#q5-intermediate--customer-retention-from-one-month-to-the-next)
* [Q6 (Intermediate): Set operations on (key, value) pairs](#q6-intermediate--set-operations-on-key-value-pairs)
* [Q7 (Intermediate+): Implement intersection() and subtract() yourself](#q7-intermediate--implement-intersection-and-subtract-yourself)
* [Q8 (Intermediate+): Document similarity (Jaccard)](#q8-intermediate--document-similarity-jaccard)
* [Q9 (Intermediate+): When duplicates matter: "bag" operations](#q9-intermediate--when-duplicates-matter-bag-operations)

---

## Q1 (Basic) — union()

```python
>>> a = sc.parallelize([1, 2, 3, 4, 4], 2)
>>> b = sc.parallelize([3, 4, 5, 6], 3)

>>> u = a.union(b)
>>> u.collect()
[1, 2, 3, 4, 4, 3, 4, 5, 6]
>>> u.count()
9

>>> u.getNumPartitions()
5
>>> u.glom().collect()
[[1, 2], [3, 4, 4], [3], [4], [5, 6]]

>>> sorted(u.distinct().collect())
[1, 2, 3, 4, 5, 6]

>>> sorted(b.union(a).collect()) == sorted(a.union(b).collect())
True
```

**Notes:**

* `union()` **keeps every element**, duplicates included: `3` and `4` appear in both
  RDDs, and `4` three times in total. It's like SQL's `UNION ALL`, not `UNION`.
  For a true set union (each element once), add `distinct()`.
* **`union()` doesn't move any data.** The new RDD simply *lists* the partitions of
  `a` (2) followed by those of `b` (3), as they are. There's no shuffle, so `union()`
  is very cheap, even for huge RDDs.
* That's also why the output keeps the original order: `a`'s elements first, then `b`'s.
* `b.union(a)` has the same elements, in a different order. As a **collection** it's
  the same; as a list it isn't.

---

## Q2 (Basic) — intersection()

```python
>>> sorted(a.intersection(b).collect())
[3, 4]
>>> sorted(a.intersection(a).collect())
[1, 2, 3, 4]
>>> a.intersection(sc.parallelize([9, 10], 2)).collect()
[]
```

**Notes:**

* `intersection()` **removes duplicates**: `4` appears twice in `a`, but **once** in
  the result. It returns each shared element once, like a real set intersection.
* So `a.intersection(a)` is the same as `a.distinct()`.
* Unlike `union()`, `intersection()` **shuffles** both RDDs. It has to bring equal
  elements from both sides together. Q7 shows how.

---

## Q3 (Basic) — subtract()

```python
>>> sorted(a.subtract(b).collect())
[1, 2]
>>> sorted(b.subtract(a).collect())
[5, 6]

>>> dup = sc.parallelize([1, 1, 2, 2, 7], 2)
>>> two = sc.parallelize([2], 1)
>>> sorted(dup.subtract(two).collect())
[1, 1, 7]

>>> symmetric = a.subtract(b).union(b.subtract(a))
>>> sorted(symmetric.collect())
[1, 2, 5, 6]
```

**Notes:**

* `a.subtract(b)` keeps the elements of `a` that **don't appear anywhere** in `b`.
  Order matters: `a − b` ≠ `b − a`.
* **Duplicates in `a` are kept**: both `1`s survive. But **all** copies of an element
  that's in `b` are removed. One `2` in `two` removes both `2`s from `dup`. So
  `subtract()` is "remove every element that appears in `b`", **not** "take away
  one copy for each copy in `b`". Q9 builds that second version.
* The symmetric difference is `(a − b) ∪ (b − a)`. Here, `union()` is safe without a
  `distinct()`: the two halves can't share an element, since one half has no
  elements of `b`, and the other only elements of `b`.

---

## Q4 (Intermediate) — Who takes which course?

```python
>>> enrolled = (sc.textFile("data/enrollments.txt", 2)
...               .map(lambda line: line.split(":"))
...               .map(lambda f: (f[0], f[1].split(","))))
>>> math = enrolled.filter(lambda kv: "math" in kv[1]).map(lambda kv: kv[0])
>>> art = enrolled.filter(lambda kv: "art" in kv[1]).map(lambda kv: kv[0])
>>> sorted(math.collect())
['alice', 'bob', 'carol', 'frank']
>>> sorted(art.collect())
['alice', 'carol', 'erin']

>>> sorted(math.intersection(art).collect())
['alice', 'carol']
>>> sorted(math.union(art).distinct().collect())
['alice', 'bob', 'carol', 'erin', 'frank']
>>> sorted(math.subtract(art).collect())
['bob', 'frank']
>>> sorted(math.subtract(art).union(art.subtract(math)).collect())
['bob', 'erin', 'frank']

>>> students = enrolled.map(lambda kv: kv[0])
>>> sorted(students.subtract(math.union(art)).collect())
['dan']
```

**Notes:**

* Each English phrase maps to one set operation:

  | Question | Set operation |
  |---|---|
  | both math **and** art | `math.intersection(art)` |
  | math **or** art | `math.union(art).distinct()` |
  | math but **not** art | `math.subtract(art)` |
  | **exactly one** | `(math − art) ∪ (art − math)` |
  | **neither** | `students − (math ∪ art)` |

* Without `distinct()`, "math or art" would list `alice` and `carol` twice,
  because they're in both RDDs.
* "Neither" doesn't need `distinct()` on `math.union(art)`: for `subtract()`, it
  doesn't matter how often an element appears in the **right-hand** RDD.

---

## Q5 (Intermediate) — Customer retention from one month to the next

```python
>>> sep_visits = sc.textFile("data/visitors_sep.txt", 2)
>>> oct_visits = sc.textFile("data/visitors_oct.txt", 2)
>>> sep_visits.count()
8
>>> oct_visits.count()
8
>>> sep_customers = sep_visits.distinct()
>>> oct_customers = oct_visits.distinct()
>>> sep_customers.count()
6
>>> oct_customers.count()
6

>>> retained = sep_customers.intersection(oct_customers)
>>> sorted(retained.collect())
['c1', 'c2', 'c4']
>>> sorted(sep_customers.subtract(oct_customers).collect())
['c3', 'c5', 'c6']
>>> sorted(oct_customers.subtract(sep_customers).collect())
['c7', 'c8', 'c9']

>>> sep_customers.union(oct_customers).distinct().count()
9
>>> print(f"{retained.count() / sep_customers.count():.0%}")
50%

>>> sorted(sep_visits.subtract(oct_visits).collect())
['c3', 'c5', 'c6']
```

**Notes:**

* This is a common business report: of September's 6 customers, 3 came back
  (**50% retention**), 3 were lost, and 3 new ones arrived in October.
* **Remove duplicates first.** The raw files count **visits**, not customers.
  `sep_visits.count()` is 8, but there are only 6 customers.
* Task 7 happens to give the same answer, because each churned customer
  visited only **once** in September. If `c3` had visited twice, `sep_visits.subtract(oct_visits)`
  would list `c3` **twice**, since `subtract()` keeps the left side's duplicates.
  `intersection()` on the raw visits would still be right, because it always removes
  duplicates, but relying on that is fragile. Calling `distinct()` first makes the
  intent clear.
* `{x:.0%}` is Python formatting: multiply by 100, no decimals, add `%`.

---

## Q6 (Intermediate) — Set operations on (key, value) pairs

```python
>>> wh1 = sc.parallelize([("apple", 5), ("pear", 3), ("kiwi", 2)], 2)
>>> wh2 = sc.parallelize([("apple", 3), ("pear", 3), ("plum", 4)], 2)

>>> wh1.union(wh2).reduceByKey(lambda a, b: a + b).sortByKey().collect()
[('apple', 8), ('kiwi', 2), ('pear', 6), ('plum', 4)]

>>> wh1.intersection(wh2).collect()
[('pear', 3)]
>>> sorted(wh1.subtract(wh2).collect())
[('apple', 5), ('kiwi', 2)]

>>> items1 = wh1.map(lambda kv: kv[0])
>>> items2 = wh2.map(lambda kv: kv[0])
>>> sorted(items1.subtract(items2).collect())
['kiwi']
>>> sorted(items1.intersection(items2).collect())
['apple', 'pear']

>>> (items1.subtract(items2)
...        .map(lambda item: (item, None))
...        .join(wh1)
...        .map(lambda kv: (kv[0], kv[1][1]))
...        .collect())
[('kiwi', 2)]
```

**Notes:**

* **`union()` + `reduceByKey()`** is the standard way to **merge** two pair RDDs
  that describe the same kind of thing: here, stock from two warehouses.
* Set operations compare **whole elements**. For pairs, that means the key **and**
  the value. `("apple", 5)` and `("apple", 3)` are different elements, so:
  * `intersection()` finds only `("pear", 3)`, the one item with the **same
    quantity** in both. That's not "items in both warehouses";
  * `subtract()` keeps `("apple", 5)`, even though warehouse 2 also has apples.
* To compare by **key only**, keep just the keys first (task 4). To get the values
  back afterwards, join the result with the original RDD (task 5).
* Spark also has `subtractByKey()`, which does tasks 4 and 5 in one step. This is
  how `subtract()` itself is implemented. See Q7.

---

## Q7 (Intermediate+) — Implement intersection() and subtract() yourself

```python
>>> def my_intersection(x, y):
...     return (x.map(lambda v: (v, None))
...              .cogroup(y.map(lambda v: (v, None)))
...              .filter(lambda kv: len(kv[1][0]) > 0 and len(kv[1][1]) > 0)
...              .map(lambda kv: kv[0]))
...
>>> sorted(my_intersection(a, b).collect())
[3, 4]
>>> sorted(a.intersection(b).collect())
[3, 4]

>>> def my_subtract(x, y):
...     return (x.map(lambda v: (v, None))
...              .cogroup(y.map(lambda v: (v, None)))
...              .filter(lambda kv: len(kv[1][1]) == 0)
...              .flatMap(lambda kv: [kv[0]] * len(kv[1][0])))
...
>>> sorted(my_subtract(a, b).collect())
[1, 2]
>>> sorted(a.subtract(b).collect())
[1, 2]

>>> sorted(my_subtract(dup, two).collect()) == sorted(dup.subtract(two).collect())
True
>>> sorted(my_subtract(dup, two).collect())
[1, 1, 7]

>>> sorted(my_intersection(sep_visits, oct_visits).collect()) == sorted(sep_visits.intersection(oct_visits).collect())
True
```

**Notes:**

* After `cogroup()`, each distinct element `v` has two lists: its copies in `x`, and its
  copies in `y`. Then:
  * **intersection**: keep `v` if **both** lists are non-empty, and output it
    **once**. That's why `intersection()` removes duplicates;
  * **subtract**: keep `v` if the `y` list is **empty**, and output it **as many times
    as it appears in `x`**. That's why `subtract()` keeps the left side's duplicates.
  * An element that's only in `y` has an empty `x` list, so `my_subtract` outputs it
    0 times, which is correct.
* This is how PySpark does it. From `pyspark/core/rdd.py` (Spark 4.2.0):

  ```python
  # intersection()
  return (
      self.map(lambda v: (v, None))
      .cogroup(other.map(lambda v: (v, None)))
      .filter(lambda k_vs: all(k_vs[1]))
      .keys()
  )

  # subtract() calls subtractByKey(), which does:
  return (
      self.cogroup(other, numPartitions)
      .filter(filter_func)               # keep keys with left values and no right values
      .flatMapValues(lambda x: x[0])     # one output per left value
  )
  ```

  `all(k_vs[1])` is a short way of writing "both lists are non-empty".
* Both are built on `cogroup()`, so both **shuffle both RDDs**. `union()` is the only
  one of the three that doesn't shuffle.

---

## Q8 (Intermediate+) — Document similarity (Jaccard)

```python
>>> docs = sc.textFile("data/documents.txt", 2).map(lambda line: line.split("|"))
>>> def words(doc_id):
...     return (docs.filter(lambda d: d[0] == doc_id)
...                 .flatMap(lambda d: d[1].split())
...                 .distinct())
...
>>> sorted(words("d1").collect())
['big', 'data', 'makes', 'simple', 'spark']

>>> w1 = words("d1")
>>> w2 = words("d2")
>>> sorted(w1.intersection(w2).collect())
['big', 'data']
>>> shared = w1.intersection(w2).count()
>>> either = w1.union(w2).distinct().count()
>>> shared
2
>>> either
8
>>> round(shared / either, 3)
0.25

>>> ids = sorted(docs.map(lambda d: d[0]).collect())
>>> results = []
>>> for i in range(len(ids)):
...     for j in range(i + 1, len(ids)):
...         A, B = words(ids[i]), words(ids[j])
...         n_shared = A.intersection(B).count()
...         n_either = A.union(B).distinct().count()
...         results.append(((ids[i], ids[j]), round(n_shared / n_either, 3)))
...
>>> results
[(('d1', 'd2'), 0.25), (('d1', 'd3'), 0.111), (('d1', 'd4'), 0.222), (('d2', 'd3'), 0.25), (('d2', 'd4'), 0.1), (('d3', 'd4'), 0.1)]

>>> best = max(score for pair, score in results)
>>> [r for r in results if r[1] == best]
[(('d1', 'd2'), 0.25), (('d2', 'd3'), 0.25)]
```

**Notes:**

* `distinct()` inside `words()` makes each document a real **set** of words. `d2`
  contains `big` twice, and without `distinct()`, `words("d2").count()` would count it twice.
* `|A ∪ B|` needs `union()` **plus** `distinct()`, for the words in both documents.
  A shortcut is `|A| + |B| − |A ∩ B|`, which avoids one shuffle.
* `d1` and `d2` share `big` and `data`, and `d2` and `d3` share `a` and `cluster`.
  Both pairs score `2 / 8 = 0.25`. Note that `a` counts as much as `cluster`. Real
  systems usually remove **stop words** (`a`, `and`, `the`...) first.
* **This doesn't scale.** The `for` loop runs on the driver and starts several Spark
  jobs **per pair**. With 4 documents that's 6 pairs, but with 1 million documents
  it's 500 billion pairs. At scale, you'd build the inverted index from
  [Set 02, Q8](ANSWERS_02.md#q8-intermediate--build-an-inverted-index), find the
  document pairs that share at least one word with a self-join (as in
  [Set 05, Q9](ANSWERS_05.md#q9-intermediate--customers-who-buy-the-same-things-a-self-join)),
  and count the shared words with `reduceByKey()`, all in one job. The set operations
  here show **what** Jaccard measures; the join version is **how** you'd compute it
  for many documents.

---

## Q9 (Intermediate+) — When duplicates matter: "bag" operations

```python
>>> bag1 = sc.parallelize(["x", "x", "x", "y", "z", "z"], 2)
>>> bag2 = sc.parallelize(["x", "z", "z", "z", "w"], 2)

>>> bag1.union(bag2).count()
11
>>> sorted(bag1.intersection(bag2).collect())
['x', 'z']
>>> sorted(bag1.subtract(bag2).collect())
['y']

>>> count_1 = bag1.map(lambda e: (e, 1)).reduceByKey(lambda a, b: a + b)
>>> count_2 = bag2.map(lambda e: (e, 1)).reduceByKey(lambda a, b: a + b)
>>> count_1.sortByKey().collect()
[('x', 3), ('y', 1), ('z', 2)]
>>> count_2.sortByKey().collect()
[('w', 1), ('x', 1), ('z', 3)]

>>> bag_intersection = count_1.join(count_2).flatMap(lambda kv: [kv[0]] * min(kv[1]))
>>> sorted(bag_intersection.collect())
['x', 'z', 'z']

>>> bag_difference = (count_1.leftOuterJoin(count_2)
...                          .flatMap(lambda kv: [kv[0]] * max(0, kv[1][0] - (kv[1][1] or 0))))
>>> sorted(bag_difference.collect())
['x', 'x', 'y']

>>> from collections import Counter
>>> c1, c2 = Counter(bag1.collect()), Counter(bag2.collect())
>>> sorted((c1 & c2).elements())
['x', 'z', 'z']
>>> sorted((c1 - c2).elements())
['x', 'x', 'y']
```

**Notes:**

* How many copies of each element end up in the result
  (`bag1` has `x` ×3, `y` ×1, `z` ×2; `bag2` has `x` ×1, `z` ×3, `w` ×1):

  | Operation | `x` | `y` | `z` | `w` |
  |---|---|---|---|---|
  | `bag1.union(bag2)` | 4 | 1 | 5 | 1 |
  | `bag1.intersection(bag2)` | 1 | 0 | 1 | 0 |
  | `bag1.subtract(bag2)` | 0 | 1 | 0 | 0 |
  | bag intersection, `min(m, n)` | 1 | 0 | 2 | 0 |
  | bag difference, `max(0, m − n)` | 2 | 1 | 0 | 0 |

  `union()` **adds** the counts, `intersection()` gives **1** for every shared element,
  and `subtract()` gives **0** for every element that `bag2` has at all.
* **Bag semantics** matter when the duplicates **mean something**: items in a basket,
  units in stock, word counts. "I have 3 x's and you took 1" should leave 2,
  not 0.
* The recipe: **count** each element, combine the counts with a **join**, then
  `flatMap()` each element back out the right number of times.
  * Bag intersection uses an inner `join()`, since an element must be in both bags;
  * bag difference uses `leftOuterJoin()`: `y` is only in `bag1`, and `None` means a
    count of 0 (`kv[1][1] or 0`).
* `[e] * n` is a list with `n` copies of `e`. For `n = 0` it's `[]`, so `flatMap()`
  outputs nothing, which is how `z` disappears from the bag difference.
* Working with **counts** instead of copies also scales: an element that appears a
  million times becomes **one** `(element, 1000000)` pair before the join.
