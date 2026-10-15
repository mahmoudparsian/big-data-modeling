# PySpark RDD Practice Questions — Set 03: Answers

Solutions to the nine [Set 03 practice questions](QUESTIONS_03.md). Every
session below was run in the `bin/pyspark` shell (Spark 4.2.0), started from
this folder. The output is copied from those real runs.

Try the questions yourself first. There are often several correct
solutions, and yours doesn't need to match these exactly.

* [Q1 (Basic): Seeing the partitions](#q1-basic--seeing-the-partitions)
* [Q2 (Basic): mapPartitions() can do what map, filter, and flatMap do](#q2-basic--mappartitions-can-do-what-map-filter-and-flatmap-do)
* [Q3 (Basic): Expensive setup, once per partition](#q3-basic--expensive-setup-once-per-partition)
* [Q4 (Intermediate): Word count with local combining](#q4-intermediate--word-count-with-local-combining)
* [Q5 (Intermediate): One-pass statistics, and empty partitions](#q5-intermediate--one-pass-statistics-and-empty-partitions)
* [Q6 (Intermediate): Parsing a real CSV file](#q6-intermediate--parsing-a-real-csv-file)
* [Q7 (Intermediate+): Calling a batch API](#q7-intermediate--calling-a-batch-api)
* [Q8 (Intermediate+): Global top-N](#q8-intermediate--global-top-n)
* [Q9 (Intermediate+): Running total across partitions](#q9-intermediate--running-total-across-partitions)

---

## Q1 (Basic) — Seeing the partitions

```python
>>> rdd = sc.parallelize(range(1, 13), 3)
>>> rdd.getNumPartitions()
3
>>> rdd.glom().collect()
[[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]

>>> rdd.mapPartitions(lambda it: [sum(it)]).collect()
[10, 26, 42]
>>> rdd.mapPartitions(lambda it: [len(list(it))]).collect()
[4, 4, 4]

>>> sum(rdd.mapPartitions(lambda it: [sum(it)]).collect())
78
```

**Notes:**

* The function gets an **iterator**, not a list. `sum(it)` works on an
  iterator, but `len(it)` doesn't, which is why task 3 uses `len(list(it))`.
* The function must **return an iterable**. Spark loops over what you return,
  and each item becomes one element of the new RDD. `[sum(it)]` is a list with
  one item, so each partition produces one element.
* Returning `sum(it)` (a plain `int`) fails when you run an action:

  ```
  TypeError: 'int' object is not iterable
  ```

* An iterator can be read **only once**. After `sum(it)`, the iterator is used
  up, so if you need the values twice, save them first with `values = list(it)`.

---

## Q2 (Basic) — mapPartitions() can do what map, filter, and flatMap do

```python
>>> nums = sc.parallelize(range(1, 11), 3)
>>> nums.glom().collect()
[[1, 2, 3], [4, 5, 6], [7, 8, 9, 10]]

>>> def squares(it):
...     for x in it:
...         yield x * x
...
>>> nums.mapPartitions(squares).collect()
[1, 4, 9, 16, 25, 36, 49, 64, 81, 100]
>>> nums.mapPartitions(squares).collect() == nums.map(lambda x: x * x).collect()
True

>>> def evens(it):
...     for x in it:
...         if x % 2 == 0:
...             yield x
...
>>> nums.mapPartitions(evens).collect()
[2, 4, 6, 8, 10]

>>> def min_max(it):
...     values = list(it)
...     yield (min(values), max(values))
...
>>> nums.mapPartitions(min_max).collect()
[(1, 3), (4, 6), (7, 10)]

>>> def plus_minus(it):
...     for x in it:
...         yield x
...         yield -x
...
>>> nums.mapPartitions(plus_minus).count()
20
```

**Notes:**

* A function with `yield` is a **generator**. Each `yield` produces one
  output element, so the function can produce as many or as few elements as
  you like: one per input (`squares`, like `map`), some of them (`evens`, like
  `filter`), one per partition (`min_max`), or several per input (`plus_minus`,
  like `flatMap`).
* So `mapPartitions()` is the most general of these transformations. Use it when
  you need something that `map`/`filter`/`flatMap` can't give you: setup, state,
  or a whole-partition view. Q3–Q9 show those cases.
* A generator also handles **one element at a time**, so a partition doesn't
  need to fit in memory. `min_max` breaks that rule with `list(it)`, and Q5
  shows the safer one-pass version.

---

## Q3 (Basic) — Expensive setup, once per partition

```python
>>> codes = sc.parallelize(["US", "FR", "JP", "US", "DE", "FR",
...                         "JP", "US", "IN", "DE", "FR", "US"], 3)
>>> setups = sc.accumulator(0)
>>> def load_country_names():
...     setups.add(1)
...     return {"US": "United States", "FR": "France", "JP": "Japan",
...             "DE": "Germany", "IN": "India"}
...

>>> names1 = codes.map(lambda c: load_country_names()[c])
>>> names1.collect()
['United States', 'France', 'Japan', 'United States', 'Germany', 'France', 'Japan', 'United States', 'India', 'Germany', 'France', 'United States']
>>> setups.value
12

>>> setups = sc.accumulator(0)
>>> def to_names(it):
...     lookup = load_country_names()
...     for c in it:
...         yield lookup[c]
...
>>> names2 = codes.mapPartitions(to_names)
>>> names2.collect()
['United States', 'France', 'Japan', 'United States', 'Germany', 'France', 'Japan', 'United States', 'India', 'Germany', 'France', 'United States']
>>> setups.value
3

>>> names1.collect() == names2.collect()
True
>>> setups.value
6
```

**Notes:**

* With `map()`, the lambda runs once per element, so the table is loaded **12
  times**. With `mapPartitions()`, `to_names` runs once per partition, so the table is
  loaded **3 times**, and then reused for all the elements in that partition.
* With 1 billion elements in 1,000 partitions, that's **1,000,000,000 loads
  vs. 1,000**. This is the #1 reason to use `mapPartitions()`: opening database
  connections, HTTP clients, loading ML models, compiling big regular expressions, etc.
* Why not just load the table once on the driver and use it in a `map()`
  lambda? For a small dictionary, that works (Spark ships a copy with each
  task). But many resources, like a database connection, **can't be shipped**,
  and must be created where the task runs, on the executor.
* **Accumulator caution:** an accumulator updated inside a transformation is
  increased **every time** the RDD is recomputed. The comparison line runs both
  RDDs again, so `setups.value` grows from 3 to **6**: `names2` loaded the table
  3 more times. (`names1` ran again too, but it still adds to the *first*
  accumulator. PySpark packs up an RDD's function the first time it runs a job
  with it, so `names1` keeps the counter it started with.) Read an accumulator
  right after the one action you want to measure.

---

## Q4 (Intermediate) — Word count with local combining

```python
>>> lines = sc.textFile("data/quotes.txt", 2)
>>> words = (lines.flatMap(lambda line: line.split())
...               .map(lambda w: w.lower().strip(".,!?;:")))

>>> pairs1 = words.map(lambda w: (w, 1))
>>> pairs1.count()
40

>>> def local_counts(it):
...     counts = {}
...     for w in it:
...         counts[w] = counts.get(w, 0) + 1
...     return counts.items()
...
>>> pairs2 = words.mapPartitions(local_counts)
>>> pairs2.count()
23
>>> pairs2.glom().map(len).collect()
[14, 9]

>>> r1 = pairs1.reduceByKey(lambda a, b: a + b).sortByKey().collect()
>>> r2 = pairs2.reduceByKey(lambda a, b: a + b).sortByKey().collect()
>>> r1 == r2
True
>>> r2[:5]
[('and', 2), ('big', 4), ('data', 5), ('fast', 1), ('fun', 1)]
```

**Notes:**

* This is **in-mapper combining**: each partition counts its own words first,
  so each word appears **at most once per partition**. 40 pairs went down to 23
  (14 different words in partition 0, 9 in partition 1).
* With real text the savings are huge. A word like "the" may appear millions
  of times in a partition but becomes **one** `("the", n)` pair.
* `reduceByKey()` already does some local combining by itself. Doing it in
  `mapPartitions()` makes it **explicit** and skips creating all the
  `(word, 1)` tuples in the first place. In MapReduce, this is the same idea as a
  combiner, done inside the mapper.
* `counts.items()` is iterable, so it's a valid return value. The dictionary
  holds only **distinct** words, so it stays small even for a big partition.

---

## Q5 (Intermediate) — One-pass statistics, and empty partitions

```python
>>> amounts = sc.textFile("data/transactions.txt", 2).map(lambda line: int(line.split(",")[1]))
>>> amounts.glom().collect()
[[20, 15, 35, 50, 5, 10, 40], [25, 45, 60, 30, 100]]

>>> def stats(it):
...     lo = hi = None
...     total = 0
...     n = 0
...     for x in it:
...         lo = x if lo is None else min(lo, x)
...         hi = x if hi is None else max(hi, x)
...         total += x
...         n += 1
...     if n > 0:
...         yield (lo, hi, total, n)
...
>>> parts = amounts.mapPartitions(stats).collect()
>>> parts
[(5, 50, 175, 7), (25, 100, 260, 5)]

>>> lo = min(p[0] for p in parts)
>>> hi = max(p[1] for p in parts)
>>> total = sum(p[2] for p in parts)
>>> n = sum(p[3] for p in parts)
>>> (lo, hi, total, n, round(total / n, 2))
(5, 100, 435, 12, 36.25)

>>> small = sc.parallelize([5, 8, 1], 4)
>>> small.glom().collect()
[[], [5], [8], [1]]
>>> small.mapPartitions(stats).collect()
[(5, 5, 5, 1), (8, 8, 8, 1), (1, 1, 1, 1)]
```

**Notes:**

* **One pass:** `stats` looks at each element once and keeps only 4 variables,
  so it works the same for 10 elements or 10 billion. It never builds a list.
* The results per partition are **tiny** (one tuple each), so combining them on
  the driver with plain Python is cheap.
* **Empty partitions happen**: with 3 elements in 4 partitions, one partition
  has nothing. They also show up after a `filter()` that removes everything
  in a partition. The `min_max` function from Q2 crashes on them:

  ```
  ValueError: min() iterable argument is empty
  ```

  The `if n > 0:` check makes `stats` yield **nothing** for an empty partition,
  so it simply adds no element to the result.

---

## Q6 (Intermediate) — Parsing a real CSV file

```python
>>> raw = sc.textFile("data/customers.csv", 2)
>>> raw.count()
7
>>> raw.glom().map(len).collect()
[4, 3]
>>> raw.map(lambda line: len(line.split(","))).collect()
[4, 5, 5, 5, 4, 6, 4]

>>> import csv
>>> def parse(index, it):
...     if index == 0:
...         next(it, None)
...     for f in csv.reader(it):
...         yield (int(f[0]), f[1], f[2], float(f[3]))
...
>>> customers = raw.mapPartitionsWithIndex(parse)
>>> customers.count()
6
>>> customers.take(2)
[(101, 'Smith, John', 'San Jose', 250.5), (102, 'Maria Garcia', 'Austin, TX', 100.0)]

>>> customers.filter(lambda c: c[3] > 100).map(lambda c: c[1]).collect()
['Smith, John', 'Chen, Wei']
>>> customers.map(lambda c: c[2]).collect()
['San Jose', 'Austin, TX', 'Boston', 'Seattle', 'Portland, OR', 'Denver']
```

**Notes:**

* A plain `split(",")` breaks `"Smith, John"` into two fields, so lines have 4, 5,
  or even 6 fields. A real CSV parser understands quotes.
* `csv.reader()` takes **any iterator of lines**, and `mapPartitions()` hands you
  exactly that. You create one reader per partition and stream every line through it.
* Skipping the header: `textFile()` splits the file in order, so the header is
  the first line of **partition 0**. `next(it, None)` reads and throws away one line.
  The `None` default keeps it from failing if that partition is empty.
* Another common way is `raw.filter(lambda line: line != header)`, with
  `header = raw.first()`. That also works, but it checks every line, and it
  drops any data line that happens to be identical to the header.
* For real projects, the DataFrame reader (`spark.read.csv(..., header=True)`)
  handles all of this for you. This question shows *how* such a reader can work
  on RDDs.

---

## Q7 (Intermediate+) — Calling a batch API

```python
>>> items = sc.parallelize(["a", "b", "c", "d", "e", "f",
...                         "g", "h", "i", "j", "k", "l"], 2)
>>> calls = sc.accumulator(0)
>>> def api_lookup_batch(keys):
...     calls.add(1)
...     return [k.upper() for k in keys]
...

>>> items.map(lambda k: api_lookup_batch([k])[0]).collect()
['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L']
>>> calls.value
12

>>> from itertools import islice
>>> calls = sc.accumulator(0)
>>> def in_batches(it, size=4):
...     while True:
...         batch = list(islice(it, size))
...         if not batch:
...             break
...         for result in api_lookup_batch(batch):
...             yield result
...
>>> items.mapPartitions(in_batches).collect()
['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L']
>>> calls.value
4

>>> items.glom().map(len).collect()
[6, 6]
```

**Notes:**

* Each partition has 6 items, so it sends a batch of 4 and then a batch of 2:
  **2 calls per partition × 2 partitions = 4 calls**, instead of 12.
* Batching is impossible with `map()`, which only ever sees **one** element.
  `mapPartitions()` can group elements because it sees the whole partition.
* Only one batch is in memory at a time. `islice` takes the next 4 elements
  and leaves the rest of the iterator untouched, so even a huge partition works.
* The batch size is a trade-off: bigger batches mean fewer calls, but each call
  is larger. Real APIs usually set a maximum batch size, often 100 or 1,000.

---

## Q8 (Intermediate+) — Global top-N

```python
>>> import heapq
>>> scores = sc.parallelize([72, 95, 61, 88, 79, 99, 54, 83, 90, 67,
...                          76, 92, 58, 85, 70, 97, 63, 81, 74, 89], 4)
>>> scores.glom().collect()
[[72, 95, 61, 88, 79], [99, 54, 83, 90, 67], [76, 92, 58, 85, 70], [97, 63, 81, 74, 89]]

>>> local_top = scores.mapPartitions(lambda it: heapq.nlargest(3, it))
>>> local_top.glom().collect()
[[95, 88, 79], [99, 90, 83], [92, 85, 76], [97, 89, 81]]

>>> candidates = local_top.collect()
>>> len(candidates)
12
>>> heapq.nlargest(3, candidates)
[99, 97, 95]

>>> sorted(scores.collect(), reverse=True)[:3]
[99, 97, 95]
```

**Notes:**

* **Why it's always correct:** if a score is in the overall top 3, then at most
  2 scores anywhere are bigger, so at most 2 scores **in its own partition** are
  bigger. That puts it in its partition's top 3.
* Only N numbers per partition reach the driver. With 1,000 partitions of 1
  million scores each, that's **3,000 numbers instead of 1 billion**.
* `heapq.nlargest(3, it)` reads the iterator once and keeps only 3 items in
  memory, so it doesn't need to sort the whole partition.
* This is exactly how PySpark's own `rdd.top(n)` is implemented in
  `pyspark/core/rdd.py`:

  ```python
  def topIterator(iterator):
      yield heapq.nlargest(num, iterator, key=key)

  def merge(a, b):
      return heapq.nlargest(num, a + b, key=key)

  return self.mapPartitions(topIterator).reduce(merge)
  ```

  So `scores.top(3)` gives the same answer. Now you know how it works.

---

## Q9 (Intermediate+) — Running total across partitions

```python
>>> nums = sc.parallelize(range(1, 11), 3)

>>> def partition_sum(index, it):
...     yield (index, sum(it))
...
>>> sums = nums.mapPartitionsWithIndex(partition_sum).collect()
>>> sums
[(0, 6), (1, 15), (2, 34)]

>>> offsets = {}
>>> running = 0
>>> for index, s in sums:
...     offsets[index] = running
...     running += s
...
>>> offsets
{0: 0, 1: 6, 2: 21}

>>> def running_total(index, it):
...     total = offsets[index]
...     for x in it:
...         total += x
...         yield total
...
>>> nums.mapPartitionsWithIndex(running_total).collect()
[1, 3, 6, 10, 15, 21, 28, 36, 45, 55]
>>> nums.mapPartitionsWithIndex(running_total).glom().collect()
[[1, 3, 6], [10, 15, 21], [28, 36, 45, 55]]
```

**Notes:**

* **Why not `map()`?** A running total needs **state**: the total so far. A
  `map()` function sees one element and remembers nothing between calls, but
  the local variable `total` in `running_total` stays alive across the whole
  partition.
* **Why two passes?** Partition 1 needs the sum of partition 0 before it can
  start, and the partitions run **at the same time** on different machines.
  Pass 1 computes a tiny result (one number per partition), the driver turns it
  into offsets, and pass 2 runs fully in parallel again.
* `offsets` is a small dict on the driver. Spark ships a copy of it with each
  task, because `running_total` uses it.
* This works because `parallelize()` and `textFile()` keep the elements **in
  order** across partitions. After a shuffle, like `reduceByKey()`, the order
  is lost, and a running total would need a `sortByKey()` first.
