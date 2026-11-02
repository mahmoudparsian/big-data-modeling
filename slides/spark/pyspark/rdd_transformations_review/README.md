# PySpark RDD Transformations: Exam Review

A review of the most important **RDD transformations** (and the actions that go
with them), grouped the way you will use them:

1. **Mappers**: change each element (`map`, `flatMap`, `mapValues`, ...)
2. **Filters**: keep some elements and drop the rest (`filter`, `distinct`, ...)
3. **Reducers**: combine many elements into fewer (`reduceByKey`, `groupByKey`, ...)
4. **Joins**: combine two RDDs by key (`join`, `leftOuterJoin`, `cogroup`, ...)

Each operation starts with a small **scenario** (the business problem),
then shows **how it works** and a **worked example**, and ends with
**notes**: the things exam questions like to test.

Every example was run in the `bin/pyspark` shell (Spark 4.3.0). The output is
copied from those real runs. Type the examples yourself: predict each output
**before** you press Enter. That is exactly what an exam asks you to do.

## Contents

* [Part 0: The basics you need first](#part-0-the-basics-you-need-first)
* [Part 1: Mappers](#part-1-mappers)
  * [1.1 `map()`](#11-map) ·
    [1.2 `flatMap()`](#12-flatmap) ·
    [1.3 `mapValues()`](#13-mapvalues) ·
    [1.4 `flatMapValues()`](#14-flatmapvalues) ·
    [1.5 `keyBy()`, `keys()`, `values()`](#15-keyby-keys-values) ·
    [1.6 `mapPartitions()`](#16-mappartitions) ·
    [1.7 `mapPartitionsWithIndex()`](#17-mappartitionswithindex)
* [Part 2: Filters](#part-2-filters)
  * [2.1 `filter()`](#21-filter) ·
    [2.2 Cleaning dirty data](#22-cleaning-dirty-data) ·
    [2.3 `distinct()`](#23-distinct) ·
    [2.4 Filtering with a lookup set (broadcast)](#24-filtering-with-a-lookup-set-broadcast) ·
    [2.5 `sample()`](#25-sample) ·
    [2.6 Top-N: `take()`, `top()`, `takeOrdered()`](#26-top-n-take-top-takeordered)
* [Part 3: Reducers](#part-3-reducers)
  * [3.1 `reduce()`](#31-reduce) ·
    [3.2 `fold()` and `aggregate()`](#32-fold-and-aggregate) ·
    [3.3 `reduceByKey()`](#33-reducebykey) ·
    [3.4 `groupByKey()`](#34-groupbykey) ·
    [3.5 `foldByKey()`](#35-foldbykey) ·
    [3.6 `aggregateByKey()`](#36-aggregatebykey) ·
    [3.7 `combineByKey()`](#37-combinebykey) ·
    [3.8 `countByKey()`, `countByValue()`, `collectAsMap()`](#38-countbykey-countbyvalue-collectasmap) ·
    [3.9 `sortByKey()` and `sortBy()`](#39-sortbykey-and-sortby) ·
    [3.10 Which reducer should I use?](#310-which-reducer-should-i-use)
* [Part 4: Joins](#part-4-joins)
  * [4.1 `join()`](#41-join) ·
    [4.2 `leftOuterJoin()`](#42-leftouterjoin) ·
    [4.3 `rightOuterJoin()` and `fullOuterJoin()`](#43-rightouterjoin-and-fullouterjoin) ·
    [4.4 `subtractByKey()`](#44-subtractbykey) ·
    [4.5 `cogroup()`](#45-cogroup) ·
    [4.6 Re-keying: joining on a different field](#46-re-keying-joining-on-a-different-field) ·
    [4.7 Self-join](#47-self-join) ·
    [4.8 Broadcast (map-side) join](#48-broadcast-map-side-join) ·
    [4.9 `cartesian()`](#49-cartesian) ·
    [4.10 Set operations: `union()`, `intersection()`, `subtract()`](#410-set-operations-union-intersection-subtract)
* [Part 5: Putting it all together](#part-5-putting-it-all-together)
* [Part 6: Cheat sheet](#part-6-cheat-sheet)
* [Part 7: Exam traps](#part-7-exam-traps)
* [Part 8: Test yourself](#part-8-test-yourself)

---

## Part 0: The basics you need first

**RDD** (Resilient Distributed Dataset): an immutable collection of elements,
split into **partitions** that are processed in parallel.

* A **transformation** (`map`, `filter`, `reduceByKey`, ...) returns a **new RDD**.
  It is **lazy**: nothing runs yet. Spark only records the step in the RDD's
  **lineage** (the recipe for building it).
* An **action** (`collect`, `count`, `take`, `reduce`, `saveAsTextFile`, ...)
  returns a **value to the driver** (or writes a file). It is what makes Spark
  actually run the job.
* A **pair RDD** is an RDD of 2-tuples `(key, value)`. All the `...ByKey()`
  operations and all the joins need a pair RDD. Element `[0]` is the key, `[1]` is
  the value.
* A **narrow** transformation (`map`, `filter`, `flatMap`, `mapValues`, ...)
  works inside each partition. A **wide** transformation (`reduceByKey`,
  `groupByKey`, `join`, `distinct`, `sortByKey`, ...) must move data between
  partitions so that equal keys meet. This move is the **shuffle**, and it is the
  expensive part of a Spark job.

### The data used throughout

A small online store has four customers and seven orders. Note the two
deliberate gaps: customer `c4` (Dana) has **no orders**, and order `o6` belongs to
a customer `c5` who is **not in the customer list**.

```python
>>> customers = sc.parallelize([
...     ("c1", "Alice", "San Jose"),
...     ("c2", "Bob",   "Oakland"),
...     ("c3", "Chen",  "San Jose"),
...     ("c4", "Dana",  "Fresno"),
... ], 2)

>>> orders = sc.parallelize([     # (order_id, customer_id, category, amount)
...     ("o1", "c1", "books", 20),
...     ("o2", "c2", "games", 35),
...     ("o3", "c1", "games", 15),
...     ("o4", "c3", "books", 40),
...     ("o5", "c1", "books", 25),
...     ("o6", "c5", "music", 10),
...     ("o7", "c2", "books", 30),
... ], 2)
```

The `2` asks for 2 partitions. `glom()` turns each partition into a list, so
you can see how the elements are split:

```python
>>> orders.getNumPartitions()
2
>>> orders.map(lambda o: o[0]).glom().collect()
[['o1', 'o2', 'o3'], ['o4', 'o5', 'o6', 'o7']]
```

Laziness in action:

```python
>>> big = orders.filter(lambda o: o[3] >= 25)   # transformation: nothing runs yet
>>> type(big).__name__                          # just a recipe for a new RDD
'PipelinedRDD'
>>> big.count()                                 # action: now Spark runs the job
4
```

**Notes:**

* Each action re-runs the whole lineage from the start. If you will use an RDD in
  several actions, call `rdd.cache()` once so the second action reuses the result.
* `collect()` brings **every** element to the driver. It is fine on these tiny
  examples, but on real data use `take(n)`, `count()` or `saveAsTextFile()`.
* The order of `collect()` output follows the partitions. After a shuffle that order
  is not something to rely on, so the examples below use `sorted(...)` to
  get a stable answer.

---

## Part 1: Mappers

Mappers transform elements. They are all **narrow**: no shuffle.

| Operation | One input element gives... | Works on |
|---|---|---|
| `map(f)` | exactly **1** output element | any RDD |
| `flatMap(f)` | **0, 1 or many** output elements | any RDD |
| `mapValues(f)` | exactly 1, **key unchanged** | pair RDD |
| `flatMapValues(f)` | 0, 1 or many, **key unchanged** | pair RDD |
| `mapPartitions(f)` | `f` sees a **whole partition** at once | any RDD |

### 1.1 `map()`

**Scenario:** the store's order log arrives as text lines like `"o1,c1,books,20"`.
Before you can compute anything, each line must become a tuple with a real number
for the amount.

**How it works:** `rdd.map(f)` calls `f` on every element and keeps whatever `f`
returns. **N elements in → N elements out**, always.

```python
>>> lines = sc.parallelize(["o1,c1,books,20", "o2,c2,games,35", "o3,c1,games,15"], 2)

>>> lines.map(lambda line: line.split(",")).collect()
[['o1', 'c1', 'books', '20'], ['o2', 'c2', 'games', '35'], ['o3', 'c1', 'games', '15']]

>>> def parse(line):
...     oid, cid, category, amount = line.split(",")
...     return (oid, cid, category, int(amount))
...
>>> parsed = lines.map(parse)
>>> parsed.collect()
[('o1', 'c1', 'books', 20), ('o2', 'c2', 'games', 35), ('o3', 'c1', 'games', 15)]
```

The most common `map()` of all: build a **pair RDD** `(key, value)` to prepare for a
reducer or a join.

```python
>>> amounts = orders.map(lambda o: (o[1], o[3]))     # (customer_id, amount)
>>> amounts.collect()
[('c1', 20), ('c2', 35), ('c1', 15), ('c3', 40), ('c1', 25), ('c5', 10), ('c2', 30)]
>>> orders.count(), amounts.count()
(7, 7)
```

**Notes:**

* After `split(",")` every field is still a **string**: `'20'`, not `20`. Summing
  strings concatenates them (`'20' + '35'` is `'2035'`). Convert with `int()` /
  `float()` in your parse function.
* `map()` with `split()` gives a **list per line** (nested lists), not a flat list of
  words. That is what `flatMap()` is for.
* A named function (`def parse`) is easier to read and test than a long `lambda`.

### 1.2 `flatMap()`

**Scenario:** customers leave short text reviews. Marketing wants to know which
words appear most. You need one element **per word**, not one per review.

**How it works:** `rdd.flatMap(f)`: `f` returns a **list (or any iterable)** for
each element, and Spark **flattens** all those lists into one RDD.
**N elements in → any number out** (an empty list gives nothing).

```python
>>> reviews = sc.parallelize(["great book", "too long", "", "great great game"], 2)

>>> reviews.map(lambda r: r.split()).collect()
[['great', 'book'], ['too', 'long'], [], ['great', 'great', 'game']]

>>> reviews.flatMap(lambda r: r.split()).collect()
['great', 'book', 'too', 'long', 'great', 'great', 'game']

>>> reviews.map(lambda r: r.split()).count(), reviews.flatMap(lambda r: r.split()).count()
(4, 7)
```

`flatMap()` can also **parse and drop bad records in one step**: return `[x]` to
keep a record, `[]` to drop it.

```python
>>> raw = sc.parallelize(["o1,20", "o2,abc", "o3,15", "o4"], 2)
>>> def parse_or_drop(line):
...     try:
...         oid, amount = line.split(",")
...         return [(oid, int(amount))]
...     except ValueError:          # wrong number of fields, or not a number
...         return []
...
>>> raw.flatMap(parse_or_drop).collect()
[('o1', 20), ('o3', 15)]
```

**Notes:**

* The empty review `""` gives `[]` in `map()` (one element, an empty list) but
  **nothing** in `flatMap()`.
* Trap: a **string** is iterable too. If `f` returns a string, you get its
  **characters**:

```python
>>> sc.parallelize(["ab", "cd"], 1).flatMap(lambda s: s.upper()).collect()
['A', 'B', 'C', 'D']
```

### 1.3 `mapValues()`

**Scenario:** the payment system wants every order amount in **cents** instead of
dollars. The customer key must stay the same.

**How it works:** on a pair RDD, `rdd.mapValues(f)` applies `f` to the **value**
only. The key is passed through untouched.

```python
>>> amounts.mapValues(lambda dollars: dollars * 100).collect()
[('c1', 2000), ('c2', 3500), ('c1', 1500), ('c3', 4000), ('c1', 2500), ('c5', 1000), ('c2', 3000)]

>>> amounts.map(lambda kv: (kv[0], kv[1] * 100)).collect()      # same result with map()
[('c1', 2000), ('c2', 3500), ('c1', 1500), ('c3', 4000), ('c1', 2500), ('c5', 1000), ('c2', 3000)]
```

So why have `mapValues()` at all? Because Spark knows the keys did not change, so
it **keeps the partitioner**. If the RDD was already hash-partitioned by key (by
`partitionBy()`, or by a `reduceByKey()`), a later `reduceByKey()` or `join()` on the
same key can skip a shuffle. After `map()`, Spark has to assume the keys may have
changed:

```python
>>> p = amounts.partitionBy(2)
>>> p.mapValues(lambda v: v * 100).partitioner == p.partitioner
True
>>> p.map(lambda kv: (kv[0], kv[1] * 100)).partitioner is None
True
```

**Notes:**

* `mapValues(f)`: `f` receives **only the value**, so write `lambda v: ...`, not
  `lambda kv: ...`.
* If you need the key to compute the new value, use `map()`.

### 1.4 `flatMapValues()`

**Scenario:** a movie catalog stores genres as one string, `"animation|comedy"`. To
count movies per genre, you need one `(movie, genre)` pair per genre.

**How it works:** `rdd.flatMapValues(f)`: `f` turns one value into a list of values;
each one is paired with the **same key**.

```python
>>> movies = sc.parallelize([("Up", "animation|comedy"),
...                          ("Heat", "crime"),
...                          ("Coco", "animation|music|family")], 2)

>>> movie_genre = movies.flatMapValues(lambda g: g.split("|"))
>>> movie_genre.collect()
[('Up', 'animation'), ('Up', 'comedy'), ('Heat', 'crime'), ('Coco', 'animation'), ('Coco', 'music'), ('Coco', 'family')]

>>> sorted(movie_genre.map(lambda kv: (kv[1], 1)).reduceByKey(lambda a, b: a + b).collect())
[('animation', 2), ('comedy', 1), ('crime', 1), ('family', 1), ('music', 1)]
```

**Notes:**

* `flatMapValues()` is to `mapValues()` what `flatMap()` is to `map()`.
* The second step swaps the pair to `(genre, 1)` so that the genre becomes the key
  (see [3.3 `reduceByKey()`](#33-reducebykey)).

### 1.5 `keyBy()`, `keys()`, `values()`

**Scenario:** you want to group whole order records by customer, keeping the full
record as the value.

```python
>>> by_customer = orders.keyBy(lambda o: o[1])        # key = f(element), value = element
>>> by_customer.take(2)
[('c1', ('o1', 'c1', 'books', 20)), ('c2', ('o2', 'c2', 'games', 35))]

>>> amounts.keys().collect()
['c1', 'c2', 'c1', 'c3', 'c1', 'c5', 'c2']
>>> amounts.values().sum()
175
```

**Notes:**

* `rdd.keyBy(f)` is the same as `rdd.map(lambda x: (f(x), x))`.
* `keys()` keeps **duplicates**: add `.distinct()` to get each customer once.
* `values().sum()`: `sum()` is an action that returns a number.

### 1.6 `mapPartitions()`

**Scenario:** the store reports in euros. The exchange rate comes from an external
service, and every call to it is slow. With `map()`, you would call it **once per
order**. With `mapPartitions()`, you call it **once per partition**.

**How it works:** `rdd.mapPartitions(f)`: `f` receives an **iterator over all the
elements of one partition** and must **return (or `yield`) an iterable** of output
elements. It is called once per partition.

An accumulator (a counter the tasks can add to) shows how many times the
"expensive" setup runs:

```python
>>> setup_calls = sc.accumulator(0)
>>> def get_usd_to_eur_rate():
...     setup_calls.add(1)          # imagine: a slow web-service call
...     return 0.9
...

>>> def convert_partition(rows):
...     rate = get_usd_to_eur_rate()          # ONCE per partition
...     for cid, usd in rows:
...         yield (cid, round(usd * rate, 2))
...
>>> amounts.mapPartitions(convert_partition).collect()
[('c1', 18.0), ('c2', 31.5), ('c1', 13.5), ('c3', 36.0), ('c1', 22.5), ('c5', 9.0), ('c2', 27.0)]
>>> setup_calls.value
2

>>> setup_calls = sc.accumulator(0)
>>> amounts.map(lambda kv: (kv[0], round(kv[1] * get_usd_to_eur_rate(), 2))).count()
7
>>> setup_calls.value                         # ONCE per element
7
```

Second use: **local aggregation**. Each partition returns one `(sum, count)` pair
instead of all its elements:

```python
>>> def sum_and_count(rows):
...     total, n = 0, 0
...     for cid, amount in rows:
...         total += amount
...         n += 1
...     yield (total, n)
...
>>> amounts.mapPartitions(sum_and_count).collect()
[(70, 3), (105, 4)]
```

The trap: `f` must return an **iterable of elements**. If you `return (total, n)`,
Spark treats the tuple as **two elements**:

```python
>>> def wrong_sum_and_count(rows):
...     total, n = 0, 0
...     for cid, amount in rows:
...         total += amount
...         n += 1
...     return (total, n)
...
>>> amounts.mapPartitions(wrong_sum_and_count).collect()
[70, 3, 105, 4]
```

**Notes:**

* Use `mapPartitions()` when there is **per-partition setup** (a database
  connection, a model, a parser) or when you want to **pre-aggregate** inside a
  partition.
* `rows` is an **iterator**: you can loop over it only once.
* Use `yield` (a generator), or return a list such as `[(total, n)]`.

### 1.7 `mapPartitionsWithIndex()`

**Scenario:** a CSV file has a header line `cid,name,city`. The header is the first
line of the file, so it is in **partition 0**. Drop it there, and only there.

**How it works:** like `mapPartitions()`, but `f(index, rows)` also receives the
partition number.

```python
>>> raw_customers = sc.parallelize(["cid,name,city",
...                                 "c1,Alice,San Jose",
...                                 "c2,Bob,Oakland",
...                                 "c3,Chen,San Jose"], 2)

>>> raw_customers.mapPartitionsWithIndex(lambda i, rows: [(i, r) for r in rows]).collect()
[(0, 'cid,name,city'), (0, 'c1,Alice,San Jose'), (1, 'c2,Bob,Oakland'), (1, 'c3,Chen,San Jose')]

>>> def drop_header(index, rows):
...     rows = iter(rows)
...     if index == 0:
...         next(rows, None)      # skip the first line of partition 0
...     return rows
...
>>> raw_customers.mapPartitionsWithIndex(drop_header).collect()
['c1,Alice,San Jose', 'c2,Bob,Oakland', 'c3,Chen,San Jose']
```

**Notes:**

* The simpler way to drop a header is
  `header = rdd.first(); rdd.filter(lambda line: line != header)`
  (see [2.2](#22-cleaning-dirty-data)). It needs an extra job for `first()`, and it
  would also remove any data line identical to the header.
* The first line of a file read with `sc.textFile()` is always in partition 0.

---

## Part 2: Filters

Filters decide **which elements survive**. They never change an element.

### 2.1 `filter()`

**Scenario:** the marketing team wants every **books** order of **$25 or more**.

**How it works:** `rdd.filter(f)` keeps the elements for which `f(element)` is
`True`. **N in → at most N out**. Narrow: no shuffle.

```python
>>> orders.filter(lambda o: o[2] == "books" and o[3] >= 25).collect()
[('o4', 'c3', 'books', 40), ('o5', 'c1', 'books', 25), ('o7', 'c2', 'books', 30)]

>>> amounts.filter(lambda kv: kv[0] == "c1").collect()      # filter on the key
[('c1', 20), ('c1', 15), ('c1', 25)]

>>> orders.filter(lambda o: o[3] > 1000).collect()           # no match: empty, not an error
[]
```

**Notes:**

* `f` must return `True`/`False` (or something truthy/falsy). It does not
  transform the element: to keep only a field, follow with `map()`.
* Filter **early**: every element you drop before a shuffle is data that does not
  travel over the network.
* After a selective filter, many partitions can be almost empty. `coalesce(n)`
  reduces the number of partitions without a full shuffle.

### 2.2 Cleaning dirty data

**Scenario:** a temperature sensor file has a header, a blank line, a missing
value (`NA`) and a line with a field missing. Keep only the valid readings.

```python
>>> raw_temps = sc.parallelize(["sensor,temp", "s1,21.5", "", "s2,NA",
...                             "s3,19.0", "s1", "s2,23.5"], 2)

>>> header = raw_temps.first()
>>> header
'sensor,temp'

>>> def is_valid(line):
...     fields = line.split(",")
...     if len(fields) != 2:
...         return False
...     try:
...         float(fields[1])
...         return True
...     except ValueError:
...         return False
...
>>> clean = raw_temps.filter(lambda line: line != header).filter(is_valid)
>>> clean.collect()
['s1,21.5', 's3,19.0', 's2,23.5']

>>> clean.map(lambda line: float(line.split(",")[1])).max()
23.5
```

**Notes:**

* Two chained `filter()` calls are the same as one filter with `and`. Spark
  **pipelines** narrow steps, so the data is still read only once.
* `first()` is an action: it runs a (small) job and returns the header to the
  driver.
* Alternative: the `flatMap()` "parse or drop" pattern from
  [1.2](#12-flatmap) cleans and parses in a single step.

### 2.3 `distinct()`

**Scenario:** the website logged every page visit. How many **different**
visitors came?

**How it works:** `rdd.distinct()` removes duplicate elements. It is **wide**: equal
elements may sit in different partitions, so they must be shuffled together.

```python
>>> visits = sc.parallelize(["ann", "bob", "ann", "cid", "bob", "ann"], 2)
>>> sorted(visits.distinct().collect())
['ann', 'bob', 'cid']
>>> visits.count(), visits.distinct().count()
(6, 3)

>>> sorted(orders.map(lambda o: o[1]).distinct().collect())   # customers who ordered
['c1', 'c2', 'c3', 'c5']
```

**Notes:**

* PySpark implements it with a reducer:
  `rdd.map(lambda x: (x, None)).reduceByKey(lambda x, _: x).map(lambda kv: kv[0])`.
* On tuples, the **whole tuple** must match: `("c1", 20)` and `("c1", 15)` are
  different.
* Elements must be **hashable**: a list cannot be used, but a tuple can.

### 2.4 Filtering with a lookup set (broadcast)

**Scenario:** the store has a short list of VIP customers. Keep only the VIP
orders.

**How it works:** send the small set to every executor **once** with a **broadcast
variable**, then `filter()` against it.

```python
>>> vip = sc.broadcast({"c1", "c3"})
>>> orders.filter(lambda o: o[1] in vip.value).map(lambda o: o[0]).collect()
['o1', 'o3', 'o4', 'o5']
```

**Notes:**

* Read the broadcast value with `.value`.
* A Python `set` lookup is fast (`in` on a list scans the whole list).
* This is a **filter-style join** with no shuffle. When the "lookup list" is itself
  a big RDD, use [`join()`](#41-join) or [`subtractByKey()`](#44-subtractbykey)
  instead.

### 2.5 `sample()`

**Scenario:** the data science team wants about **10%** of the data to test a model
quickly.

**How it works:** `rdd.sample(withReplacement, fraction, seed)`. Each element is
kept with probability `fraction`.

```python
>>> nums = sc.parallelize(range(100), 4)
>>> s1 = nums.sample(False, 0.1, seed=42)
>>> s1.count()                     # about 10% ... but not exactly 10
15
>>> s1.collect() == nums.sample(False, 0.1, seed=42).collect()   # same seed, same sample
True
```

**Notes:**

* `fraction` is a **probability**, not an exact size: here 10% of 100 elements
  gave **15**. For an exact number, use the
  `takeSample(withReplacement, num, seed)` action.
* With a fixed `seed` (and the same partitions) the sample is repeatable.

### 2.6 Top-N: `take()`, `top()`, `takeOrdered()`

**Scenario:** show the 3 biggest orders, and the 2 smallest.

These are **actions** (they return a Python list), but they are the usual way to
"filter down to the top N".

```python
>>> orders.map(lambda o: o[3]).top(3)                     # largest first
[40, 35, 30]
>>> orders.top(2, key=lambda o: o[3])
[('o4', 'c3', 'books', 40), ('o2', 'c2', 'games', 35)]
>>> orders.takeOrdered(2, key=lambda o: o[3])             # smallest first
[('o6', 'c5', 'music', 10), ('o3', 'c1', 'games', 15)]
>>> orders.take(2)                                        # the first 2, no ordering
[('o1', 'c1', 'books', 20), ('o2', 'c2', 'games', 35)]
```

**Notes:**

* `take(n)` gives the **first n** elements in partition order, not the largest.
* `top(n)` = largest first; `takeOrdered(n)` = smallest first. Both accept `key=`.
  `takeOrdered(n, key=lambda x: -x)` gives the largest first.
* All three return at most `n` elements to the driver, so they are safe on big data
  (unlike `collect()`).

---

## Part 3: Reducers

Reducers combine many values into fewer. Two families:

* **Actions on a whole RDD** (`reduce`, `fold`, `aggregate`): return **one value**
  to the driver.
* **Transformations per key** (`reduceByKey`, `groupByKey`, `foldByKey`,
  `aggregateByKey`, `combineByKey`): return a **new pair RDD** with **one element per
  key**. They are **wide** (shuffle).

### 3.1 `reduce()`

**Scenario:** what is the store's **total revenue**, and its **largest** order?

**How it works:** `rdd.reduce(f)`: `f(a, b)` combines two values into one of the
**same type**. Spark reduces inside each partition, then combines the partition
results.

```python
>>> revenue = orders.map(lambda o: o[3])
>>> revenue.reduce(lambda a, b: a + b)
175
>>> revenue.reduce(lambda a, b: a if a > b else b)
40
```

The function must be **associative** and **commutative**, because Spark decides how
to group the values. Subtraction is neither, so the answer depends on the number
of partitions:

```python
>>> sc.parallelize([10, 1, 2, 3], 1).reduce(lambda a, b: a - b)    # 10-1-2-3
4
>>> sc.parallelize([10, 1, 2, 3], 2).reduce(lambda a, b: a - b)    # (10-1) - (2-3)
10
```

And an empty RDD has nothing to reduce:

```python
>>> sc.parallelize([], 2).reduce(lambda a, b: a + b)
Traceback (most recent call last):
  ...
ValueError: Can not reduce() empty RDD
```

**Notes:**

* `reduce()` is an **action**: the result is a Python value, not an RDD.
* The output type equals the input type. To compute an **average**, you need a
  `(sum, count)` pair: use `aggregate()` (next section), or `map()` to pairs first.

### 3.2 `fold()` and `aggregate()`

**`fold(zero, f)`** is `reduce()` with a starting value, so it also works on an
empty RDD. The trap: the zero is used **once per partition, plus once more** when
the partition results are combined. So it must be a **neutral** value (`0` for
`+`, `1` for `*`, `[]` for list concatenation).

```python
>>> revenue.fold(0, lambda a, b: a + b)
175
>>> sc.parallelize([], 2).fold(0, lambda a, b: a + b)
0
>>> sc.parallelize([1, 2, 3], 2).fold(10, lambda a, b: a + b)      # 6 + 10*(2+1)
36
```

**`aggregate(zero, seqOp, combOp)`** can return a **different type** than the
elements. **Scenario:** the **average** order amount, in one pass.

* `seqOp(acc, x)`: add one element `x` into the accumulator (inside a partition).
* `combOp(acc1, acc2)`: merge two accumulators (from two partitions).

```python
>>> total, n = revenue.aggregate(
...     (0, 0),                                       # zero: (sum, count)
...     lambda acc, x: (acc[0] + x, acc[1] + 1),      # seqOp
...     lambda a, b: (a[0] + b[0], a[1] + b[1]))      # combOp
>>> total, n, total / n
(175, 7, 25.0)
```

**Notes:**

* `reduce` → same type, no zero. `fold` → same type, with a zero.
  `aggregate` → **any** result type, with a zero and two functions.
* Their per-key versions are `reduceByKey`, `foldByKey` and `aggregateByKey`.

### 3.3 `reduceByKey()`

**Scenario:** how much has **each customer** spent in total?

**How it works:** on a pair RDD, `rdd.reduceByKey(f)` combines all the values of the
**same key** with `f(a, b)`. It first reduces **inside each partition** (a
"map-side combine", like a MapReduce combiner), then shuffles only one partial result
per key per partition.

```python
>>> spent = amounts.reduceByKey(lambda a, b: a + b)
>>> sorted(spent.collect())
[('c1', 60), ('c2', 65), ('c3', 40), ('c5', 10)]

>>> sorted(amounts.reduceByKey(max).collect())        # largest order per customer
[('c1', 25), ('c2', 35), ('c3', 40), ('c5', 10)]
```

The classic **word count**: `flatMap` → `map` to `(word, 1)` → `reduceByKey`:

```python
>>> counts = (reviews.flatMap(lambda r: r.split())
...                  .map(lambda w: (w, 1))
...                  .reduceByKey(lambda a, b: a + b))
>>> sorted(counts.collect())
[('book', 1), ('game', 1), ('great', 3), ('long', 1), ('too', 1)]
```

**Notes:**

* `f` must be associative and commutative, and its result has the **same type**
  as the values (here `int + int → int`).
* `(k, 1)` + `reduceByKey(add)` is the standard **count per key**.
* Prefer `reduceByKey()` over `groupByKey()` whenever the reduction can be done in
  pairs (sum, count, min, max): far less data is shuffled.

### 3.4 `groupByKey()`

**Scenario:** what is the **median** order amount per customer? A median cannot be
computed two values at a time: you need **all** of a customer's amounts together.

**How it works:** `rdd.groupByKey()` collects all the values of each key into one
iterable: `(key, [v1, v2, ...])`. There is **no** map-side combine: every single
value crosses the network.

```python
>>> grouped = amounts.groupByKey()
>>> type(grouped.values().first()).__name__
'ResultIterable'
>>> sorted(grouped.mapValues(lambda vs: sorted(vs)).collect())
[('c1', [15, 20, 25]), ('c2', [30, 35]), ('c3', [40]), ('c5', [10])]

>>> import statistics
>>> sorted(grouped.mapValues(statistics.median).collect())
[('c1', 20), ('c2', 32.5), ('c3', 40), ('c5', 10)]
```

It can do anything `reduceByKey()` can, but at a higher cost:

```python
>>> sorted(grouped.mapValues(sum).collect()) == sorted(spent.collect())
True
```

**Notes:**

* The values come back as a `ResultIterable`. Use `mapValues(list)` to see them,
  and don't rely on their **order** (sort if order matters).
* A key with millions of values must fit in **one executor's memory**: a skewed key
  can crash the job.
* Use it when you truly need all the values: median, percentiles, sorting values
  inside a key, building a full list.

### 3.5 `foldByKey()`

**Scenario:** total revenue **per category**, with an explicit zero.

**How it works:** `rdd.foldByKey(zero, f)` is `reduceByKey(f)` with a starting value.
The zero is used **once per key per partition**.

```python
>>> cat_amounts = orders.map(lambda o: (o[2], o[3]))      # (category, amount)
>>> sorted(cat_amounts.foldByKey(0, lambda a, b: a + b).collect())
[('books', 115), ('games', 50), ('music', 10)]

>>> sorted(cat_amounts.foldByKey(100, lambda a, b: a + b).collect())   # WRONG zero
[('books', 315), ('games', 150), ('music', 110)]
```

**Notes:**

* With zero `100`: `books` appears in **both** partitions, so it gets `+100` twice
  (`115 + 200`); `games` and `music` appear in one partition each (`+100`). The result
  depends on the partitioning, which is a sure sign of a bug.
* Rule: the zero must not change the result (`0` for sum, `1` for product).

### 3.6 `aggregateByKey()`

**Scenario:** the **average** order amount **per category**. An average is not
"reduce two values into one value": you need a running **(sum, count)** per key.

**How it works:** `rdd.aggregateByKey(zero, seqFunc, combFunc)`, the per-key
version of `aggregate()`:

* `zero`: the starting accumulator, used once per key per partition.
* `seqFunc(acc, v)`: add one value into the accumulator (inside a partition).
* `combFunc(acc1, acc2)`: merge two accumulators (after the shuffle).

```python
>>> sum_count = cat_amounts.aggregateByKey(
...     (0, 0),
...     lambda acc, v: (acc[0] + v, acc[1] + 1),
...     lambda a, b: (a[0] + b[0], a[1] + b[1]))
>>> sorted(sum_count.collect())
[('books', (115, 4)), ('games', (50, 2)), ('music', (10, 1))]
>>> sorted(sum_count.mapValues(lambda p: p[0] / p[1]).collect())
[('books', 28.75), ('games', 25.0), ('music', 10.0)]
```

Second scenario: the **smallest and largest** order per customer, in one pass:

```python
>>> min_max = amounts.aggregateByKey(
...     (float("inf"), float("-inf")),
...     lambda acc, v: (min(acc[0], v), max(acc[1], v)),
...     lambda a, b: (min(a[0], b[0]), max(a[1], b[1])))
>>> sorted(min_max.collect())
[('c1', (15, 25)), ('c2', (30, 35)), ('c3', (40, 40)), ('c5', (10, 10))]
```

Third scenario: the **set of categories** each customer bought from:

```python
>>> cust_cat = orders.map(lambda o: (o[1], o[2]))
>>> cats = cust_cat.aggregateByKey(set(),
...                                lambda s, v: s | {v},
...                                lambda s1, s2: s1 | s2)
>>> sorted(cats.mapValues(sorted).collect())
[('c1', ['books', 'games']), ('c2', ['books', 'games']), ('c3', ['books']), ('c5', ['music'])]
```

**Notes:**

* The result type (a tuple, a set) **differs** from the value type (an int, a
  string). That is the whole point of `aggregateByKey()`; `reduceByKey()` cannot do it
  directly.
* The zero must be **neutral** for `seqFunc` (`(0, 0)`, `inf`/`-inf`, an empty set).
* Divide **at the end** (`mapValues`), never inside the reducer: an average of
  averages is wrong when the groups have different sizes.

### 3.7 `combineByKey()`

**Scenario:** the **average** amount **per customer**, with the most general
reducer. (`reduceByKey`, `foldByKey`, `aggregateByKey` and `groupByKey` are all
built on top of it.)

**How it works:** `rdd.combineByKey(createCombiner, mergeValue, mergeCombiners)`:

* `createCombiner(v)`: the **first** value of a key in a partition becomes an
  accumulator. (No zero value needed.)
* `mergeValue(acc, v)`: add another value of the same key, same partition.
* `mergeCombiners(acc1, acc2)`: merge accumulators from different partitions.

```python
>>> avg_by_customer = amounts.combineByKey(
...     lambda v: (v, 1),                               # createCombiner
...     lambda acc, v: (acc[0] + v, acc[1] + 1),        # mergeValue
...     lambda a, b: (a[0] + b[0], a[1] + b[1]))        # mergeCombiners
>>> sorted(avg_by_customer.collect())
[('c1', (60, 3)), ('c2', (65, 2)), ('c3', (40, 1)), ('c5', (10, 1))]
>>> sorted(avg_by_customer.mapValues(lambda p: round(p[0] / p[1], 2)).collect())
[('c1', 20.0), ('c2', 32.5), ('c3', 40.0), ('c5', 10.0)]
```

How each of the others is a special case:

| Reducer | Is the same as `combineByKey(...)` with |
|---|---|
| `reduceByKey(f)` | `lambda v: v`, `f`, `f` |
| `foldByKey(zero, f)` | `lambda v: f(zero, v)`, `f`, `f` |
| `aggregateByKey(zero, seq, comb)` | `lambda v: seq(zero, v)`, `seq`, `comb` |
| `groupByKey()` | `lambda v: [v]`, `lambda acc, v: acc + [v]`, `lambda a, b: a + b` (no map-side combine) |

```python
>>> as_lists = amounts.combineByKey(lambda v: [v],
...                                 lambda acc, v: acc + [v],
...                                 lambda a, b: a + b)
>>> sorted(as_lists.mapValues(sorted).collect())
[('c1', [15, 20, 25]), ('c2', [30, 35]), ('c3', [40]), ('c5', [10])]
```

**Notes:**

* `createCombiner` is called once **per key per partition**, not once per key: a key
  found in 2 partitions gets 2 accumulators, merged by `mergeCombiners`.
* Choose `combineByKey()` when the first value itself should start the accumulator
  (no natural zero), or on exams that ask for "the most general" reducer.

### 3.8 `countByKey()`, `countByValue()`, `collectAsMap()`

**Scenario:** quick counts for a dashboard. These are **actions**: they return a
Python dictionary to the driver, so use them only when the number of distinct keys
is small.

```python
>>> sorted(cat_amounts.countByKey().items())        # orders per category
[('books', 4), ('games', 2), ('music', 1)]
>>> sorted(visits.countByValue().items())           # visits per visitor
[('ann', 3), ('bob', 2), ('cid', 1)]

>>> spent.collectAsMap()["c1"]                       # pair RDD -> dict
60
>>> amounts.collectAsMap()["c1"]                     # duplicate keys: only ONE survives
25
```

**Notes:**

* `countByKey()` counts **elements per key**; it does not add the values.
* `countByValue()` counts identical **elements** (for a pair RDD, identical pairs).
* `collectAsMap()` on a key with several values keeps only one of them (here the
  last one, `25`). Reduce first, then collect.

### 3.9 `sortByKey()` and `sortBy()`

**Scenario:** rank the customers from the **biggest** spender down.

```python
>>> spent.sortBy(lambda kv: kv[1], ascending=False).collect()
[('c2', 65), ('c1', 60), ('c3', 40), ('c5', 10)]

>>> spent.map(lambda kv: (kv[1], kv[0])).sortByKey(ascending=False).collect()
[(65, 'c2'), (60, 'c1'), (40, 'c3'), (10, 'c5')]

>>> spent.sortByKey().collect()                     # by customer id
[('c1', 60), ('c2', 65), ('c3', 40), ('c5', 10)]
```

Ties: sort by a **tuple** so that the second field breaks the tie:

```python
>>> scores = sc.parallelize([("bob", 90), ("amy", 95), ("cal", 90), ("dee", 80)], 2)
>>> scores.sortBy(lambda kv: (-kv[1], kv[0])).collect()   # score desc, then name asc
[('amy', 95), ('bob', 90), ('cal', 90), ('dee', 80)]
```

**Notes:**

* `sortByKey()` sorts a pair RDD by its **key**. To sort by value, either swap the pair
  or use `sortBy(f)`.
* Both are **wide** (a range-partitioned shuffle) and run an extra job to sample
  the keys.
* For only the top few, `top(n, key=...)` / `takeOrdered(n, key=...)` is cheaper than a
  full sort.

### 3.10 Which reducer should I use?

| You need... | Use | Why |
|---|---|---|
| sum / count / min / max per key | `reduceByKey` | combines before the shuffle |
| the same, with an explicit zero | `foldByKey` | zero must be neutral |
| a result of a **different type** (avg via `(sum, count)`, a set, min+max) | `aggregateByKey` | zero + two functions |
| the same, but the first value starts the accumulator | `combineByKey` | the most general |
| **all** the values (median, sorted list, top-N within a key) | `groupByKey` | no combine; watch memory |
| one value for the **whole** RDD | `reduce` / `fold` / `aggregate` | actions |
| a small dict of counts on the driver | `countByKey` / `countByValue` | actions |

---

## Part 4: Joins

Joins combine **two pair RDDs** on equal **keys**. Before joining, use `map()` so that
the field you join on is element `[0]`.

The two inputs for this part:

```python
>>> names = customers.map(lambda c: (c[0], c[1]))      # (customer_id, name)
>>> names.collect()
[('c1', 'Alice'), ('c2', 'Bob'), ('c3', 'Chen'), ('c4', 'Dana')]
>>> amounts.collect()                                   # (customer_id, amount)
[('c1', 20), ('c2', 35), ('c1', 15), ('c3', 40), ('c1', 25), ('c5', 10), ('c2', 30)]
```

| Join | Keeps keys from | Missing side becomes |
|---|---|---|
| `a.join(b)` | **both** (inner) | (row dropped) |
| `a.leftOuterJoin(b)` | all of `a` | `None` for `b` |
| `a.rightOuterJoin(b)` | all of `b` | `None` for `a` |
| `a.fullOuterJoin(b)` | `a` or `b` | `None` on either side |
| `a.subtractByKey(b)` | `a`, but **not** in `b` | (anti-join) |
| `a.cogroup(b)` | `a` or `b` | an empty iterable |

### 4.1 `join()`

**Scenario:** the order list should show the **customer's name** next to each
amount.

**How it works:** `a.join(b)`: for each key in **both** RDDs, one output element
per **pair** of values: `(k, (v_a, v_b))`.

```python
>>> joined = names.join(amounts)
>>> sorted(joined.collect())
[('c1', ('Alice', 15)), ('c1', ('Alice', 20)), ('c1', ('Alice', 25)), ('c2', ('Bob', 30)), ('c2', ('Bob', 35)), ('c3', ('Chen', 40))]
>>> joined.count()
6

>>> sorted(joined.map(lambda kv: (kv[1][0], kv[1][1])).reduceByKey(lambda a, b: a + b).collect())
[('Alice', 60), ('Bob', 65), ('Chen', 40)]
```

**Notes:**

* Dana (`c4`, no orders) and order `o6` (`c5`, unknown customer) are **silently
  dropped**: an inner join keeps only matching keys.
* **Size rule:** a key with `m` values on the left and `n` on the right gives
  `m × n` rows. `c1`: 1 name × 3 orders = 3 rows. If both sides have duplicate keys
  the output can explode.
* The **left** RDD's value comes first: `names.join(amounts)` gives
  `(name, amount)`; `amounts.join(names)` gives `(amount, name)`.
* Access the parts as `kv[0]` (key), `kv[1][0]` (left value), `kv[1][1]` (right value).

### 4.2 `leftOuterJoin()`

**Scenario:** customer service wants **every** customer, including those who never
ordered, to send them a coupon.

```python
>>> left = names.leftOuterJoin(amounts)
>>> sorted(left.collect())
[('c1', ('Alice', 15)), ('c1', ('Alice', 20)), ('c1', ('Alice', 25)), ('c2', ('Bob', 30)), ('c2', ('Bob', 35)), ('c3', ('Chen', 40)), ('c4', ('Dana', None))]

>>> left.filter(lambda kv: kv[1][1] is None).map(lambda kv: kv[1][0]).collect()
['Dana']
```

**Notes:**

* Every **left** key survives. A key with no match on the right gets `None`.
* "Left outer join, then keep the rows with `None`" finds the **missing matches**
  (customers without orders).
* `None` breaks arithmetic: `None + 5` is an error. Replace it first, e.g.
  `mapValues(lambda p: (p[0], p[1] or 0))`.

### 4.3 `rightOuterJoin()` and `fullOuterJoin()`

**Scenario:** the accounting team asks: are there orders whose customer does
**not exist**? And they want one report with **everything** from both sides.

```python
>>> sorted(names.rightOuterJoin(amounts).filter(lambda kv: kv[1][0] is None).collect())
[('c5', (None, 10))]

>>> full = names.fullOuterJoin(amounts)
>>> full.count()
8
>>> sorted(full.filter(lambda kv: kv[1][0] is None or kv[1][1] is None).collect())
[('c4', ('Dana', None)), ('c5', (None, 10))]
```

**Notes:**

* `a.rightOuterJoin(b)` keeps every key of `b`. It is `b.leftOuterJoin(a)` with the
  two values swapped.
* `fullOuterJoin()`: the 6 matches + Dana + order `o6` = **8** rows. It is the
  standard **data-quality check** between two sources.

### 4.4 `subtractByKey()`

**Scenario:** the same two questions as above, answered directly: "customers with
**no** orders" and "orders with **no** customer".

**How it works:** `a.subtractByKey(b)` keeps the elements of `a` whose key does
**not** appear in `b` (an **anti-join**). The values of `b` are ignored.

```python
>>> names.subtractByKey(amounts).collect()
[('c4', 'Dana')]
>>> amounts.subtractByKey(names).collect()
[('c5', 10)]
```

### 4.5 `cogroup()`

**Scenario:** one summary line **per customer**: name, number of orders, total
spent, for **every** customer and every order, in one pass.

**How it works:** `a.cogroup(b)`: for each key found in **either** RDD, one element
`(k, (values_from_a, values_from_b))`, each side an iterable (possibly **empty**).
All the joins above are built on it.

```python
>>> cg = names.cogroup(amounts)
>>> sorted(cg.mapValues(lambda p: (list(p[0]), sorted(p[1]))).collect())
[('c1', (['Alice'], [15, 20, 25])), ('c2', (['Bob'], [30, 35])), ('c3', (['Chen'], [40])), ('c4', (['Dana'], [])), ('c5', ([], [10]))]

>>> def summary(pair):
...     ns, amts = pair
...     ns, amts = list(ns), list(amts)
...     name = ns[0] if ns else "UNKNOWN"
...     return (name, len(amts), sum(amts))
...
>>> sorted(cg.mapValues(summary).collect())
[('c1', ('Alice', 3, 60)), ('c2', ('Bob', 2, 65)), ('c3', ('Chen', 1, 40)), ('c4', ('Dana', 0, 0)), ('c5', ('UNKNOWN', 1, 10))]
```

**Notes:**

* One output element **per key** (5 keys here), not per matching pair. Unlike `join()`,
  `cogroup()` never multiplies rows.
* Compare: `join()` = the cartesian product of the two iterables, for keys where both
  are non-empty.
* `cogroup()` accepts more than one other RDD: `a.cogroup(b, c)`.

### 4.6 Re-keying: joining on a different field

**Scenario:** revenue **per city and category**. Orders know the customer, customers
know the city. Join on the customer id, then **re-key** on `(city, category)`.

```python
>>> cities = customers.map(lambda c: (c[0], c[2]))                 # (cid, city)
>>> order_info = orders.map(lambda o: (o[1], (o[2], o[3])))        # (cid, (category, amount))

>>> city_cat = (order_info.join(cities)                            # (cid, ((cat, amt), city))
...                       .map(lambda kv: ((kv[1][1], kv[1][0][0]), kv[1][0][1]))
...                       .reduceByKey(lambda a, b: a + b))
>>> sorted(city_cat.collect())
[(('Oakland', 'books'), 30), (('Oakland', 'games'), 35), (('San Jose', 'books'), 85), (('San Jose', 'games'), 15)]
```

**Notes:**

* A key can be a **tuple** (a composite key): `('San Jose', 'books')`.
* The nesting after a join gets deep: `kv[1][0][0]`. Write it out on paper
  (`(cid, ((cat, amt), city))`) before you index into it.
* Order `o6` (`c5`) disappears in the inner join: no city is known for it.

### 4.7 Self-join

**Scenario:** "customers in the same city" (for a local meetup invitation). Join the
customer list with **itself** on the city.

```python
>>> by_city = customers.map(lambda c: (c[2], c[1]))        # (city, name)
>>> pairs = by_city.join(by_city)
>>> sorted(pairs.collect())
[('Fresno', ('Dana', 'Dana')), ('Oakland', ('Bob', 'Bob')), ('San Jose', ('Alice', 'Alice')), ('San Jose', ('Alice', 'Chen')), ('San Jose', ('Chen', 'Alice')), ('San Jose', ('Chen', 'Chen'))]

>>> pairs.filter(lambda kv: kv[1][0] < kv[1][1]).collect()
[('San Jose', ('Alice', 'Chen'))]
```

**Notes:**

* A self-join pairs each row with itself, and gives every real pair **twice**
  (`(Alice, Chen)` and `(Chen, Alice)`). The filter `a < b` keeps each pair once.
* San Jose has 2 customers: `2 × 2 = 4` rows before the filter.

### 4.8 Broadcast (map-side) join

**Scenario:** the customer table is **small** (it fits in memory) and the orders
are **huge**. A normal `join()` shuffles both sides. Instead, send the small table to
every executor and look the names up inside a `map()`: **no shuffle**.

```python
>>> name_of = sc.broadcast(dict(names.collect()))      # {'c1': 'Alice', ...}
>>> def add_name(o):
...     oid, cid, category, amount = o
...     if cid in name_of.value:
...         return [(oid, name_of.value[cid], amount)]
...     return []                     # unknown customer: drop it (inner join)
...
>>> orders.flatMap(add_name).collect()
[('o1', 'Alice', 20), ('o2', 'Bob', 35), ('o3', 'Alice', 15), ('o4', 'Chen', 40), ('o5', 'Alice', 25), ('o7', 'Bob', 30)]
```

**Notes:**

* `flatMap()` with `[x]` / `[]` gives inner-join behavior. To keep unknown customers
  (left outer join), use `map()` with `name_of.value.get(cid)`, which gives `None`.
* Only for a **small** side: it is copied to every executor.
* The result keeps the order (and the partitioning) of the big RDD.

### 4.9 `cartesian()`

**Scenario:** compare **every product with every other** product (for a "similar
products" feature).

**How it works:** `a.cartesian(b)` pairs **every** element of `a` with **every**
element of `b`: `|a| × |b|` elements, no keys needed.

```python
>>> products = sc.parallelize(["p1", "p2", "p3"], 2)
>>> all_pairs = products.cartesian(products)
>>> all_pairs.count()
9
>>> sorted(all_pairs.filter(lambda p: p[0] < p[1]).collect())
[('p1', 'p2'), ('p1', 'p3'), ('p2', 'p3')]
```

**Notes:**

* 1,000,000 × 1,000,000 = 10¹² pairs. Use only on small inputs.
* Like the self-join, filter with `a < b` to keep each unordered pair once.

### 4.10 Set operations: `union()`, `intersection()`, `subtract()`

**Scenario:** two weekly lists of visitors. Who came in **either** week, in
**both** weeks, and **only** in week 1?

```python
>>> week1 = sc.parallelize(["ann", "bob", "cid", "ann"], 2)
>>> week2 = sc.parallelize(["bob", "dan"], 2)

>>> sorted(week1.union(week2).collect())                 # keeps duplicates!
['ann', 'ann', 'bob', 'bob', 'cid', 'dan']
>>> sorted(week1.union(week2).distinct().collect())
['ann', 'bob', 'cid', 'dan']

>>> week1.intersection(week2).collect()                  # removes duplicates
['bob']

>>> sorted(week1.subtract(week2).collect())              # keeps duplicates of week1
['ann', 'ann', 'cid']
```

**Notes:**

* `union()` is **narrow** and cheap: it just puts the partitions side by side
  (`2 + 2 = 4` partitions), with **no** de-duplication. It is not SQL's `UNION`;
  it is `UNION ALL`.
* `intersection()` **de-duplicates** its result. `subtract()` does **not**:
  `ann` appears twice.
* `intersection()` and `subtract()` are **wide** (they shuffle).

---

## Part 5: Putting it all together

**Scenario:** from the raw order file (with a header and a bad line), find the total
revenue **per city**, biggest first. Every part of this review shows up in one
pipeline:

```python
>>> raw_orders = sc.parallelize([
...     "order_id,customer_id,category,amount",
...     "o1,c1,books,20", "o2,c2,games,35", "o3,c1,games,15",
...     "o4,c3,books,40", "o5,c1,books,25", "o6,c5,music,10",
...     "o7,c2,books,30", "o8,c3,books,oops",
... ], 2)
>>> head = raw_orders.first()

>>> def parse_order(line):
...     try:
...         oid, cid, category, amount = line.split(",")
...         return [(cid, int(amount))]
...     except ValueError:
...         return []
...
>>> city_revenue = (raw_orders
...     .filter(lambda line: line != head)            # filter:  drop the header
...     .flatMap(parse_order)                         # mapper:  parse, drop bad lines
...     .reduceByKey(lambda a, b: a + b)              # reducer: total per customer
...     .join(cities)                                 # join:    (cid, (total, city))
...     .map(lambda kv: (kv[1][1], kv[1][0]))         # mapper:  re-key on city
...     .reduceByKey(lambda a, b: a + b)              # reducer: total per city
...     .sortBy(lambda kv: (-kv[1], kv[0])))          # biggest first
>>> city_revenue.collect()
[('San Jose', 100), ('Oakland', 65)]
```

**Notes:**

* Reducing per customer **before** the join is a deliberate choice: the join then
  sees one row per customer instead of one per order (less data shuffled).
* `o8` (`"oops"`) is dropped by `parse_order`; `o6` (`c5`) by the inner join.
* Nothing ran until `collect()`: everything before it only built the lineage.

---

## Part 6: Cheat sheet

`T` = transformation (returns an RDD, lazy). `A` = action (returns a value, runs a
job). Wide = causes a shuffle.

| Operation | T/A | Wide? | In → out | Example |
|---|---|---|---|---|
| `map(f)` | T | no | 1 → 1 | `rdd.map(lambda x: x * 2)` |
| `flatMap(f)` | T | no | 1 → 0..n | `lines.flatMap(lambda l: l.split())` |
| `mapValues(f)` | T | no | `(k, v)` → `(k, f(v))` | `pairs.mapValues(lambda v: v * 100)` |
| `flatMapValues(f)` | T | no | `(k, v)` → many `(k, w)` | `pairs.flatMapValues(lambda v: v.split("\|"))` |
| `keyBy(f)` | T | no | `x` → `(f(x), x)` | `orders.keyBy(lambda o: o[1])` |
| `keys()` / `values()` | T | no | `(k, v)` → `k` / `v` | `pairs.keys()` |
| `mapPartitions(f)` | T | no | partition → iterable | `rdd.mapPartitions(gen)` |
| `mapPartitionsWithIndex(f)` | T | no | `(i, partition)` → iterable | drop a header |
| `filter(f)` | T | no | keep if `f(x)` | `rdd.filter(lambda x: x > 0)` |
| `distinct()` | T | **yes** | remove duplicates | `rdd.distinct()` |
| `sample(r, f, s)` | T | no | random subset | `rdd.sample(False, 0.1, 42)` |
| `union(b)` | T | no | `a` + `b`, duplicates kept | `a.union(b)` |
| `intersection(b)` | T | **yes** | in both, de-duplicated | `a.intersection(b)` |
| `subtract(b)` | T | **yes** | in `a`, not in `b` | `a.subtract(b)` |
| `reduceByKey(f)` | T | **yes** | one value per key | `pairs.reduceByKey(add)` |
| `groupByKey()` | T | **yes** | `(k, iterable)` | `pairs.groupByKey().mapValues(list)` |
| `foldByKey(z, f)` | T | **yes** | like `reduceByKey` + zero | `pairs.foldByKey(0, add)` |
| `aggregateByKey(z, s, c)` | T | **yes** | any result type | `(sum, count)` per key |
| `combineByKey(c, m, mc)` | T | **yes** | the most general | `(sum, count)` per key |
| `sortByKey()` / `sortBy(f)` | T | **yes** | sorted | `rdd.sortBy(lambda x: -x[1])` |
| `join(b)` | T | **yes** | `(k, (v, w))`, inner | `a.join(b)` |
| `leftOuterJoin(b)` | T | **yes** | `(k, (v, w or None))` | `a.leftOuterJoin(b)` |
| `rightOuterJoin(b)` | T | **yes** | `(k, (v or None, w))` | `a.rightOuterJoin(b)` |
| `fullOuterJoin(b)` | T | **yes** | `None` on either side | `a.fullOuterJoin(b)` |
| `subtractByKey(b)` | T | **yes** | keys of `a` not in `b` | `a.subtractByKey(b)` |
| `cogroup(b)` | T | **yes** | `(k, (iter_a, iter_b))` | `a.cogroup(b)` |
| `cartesian(b)` | T | **yes** | every pair | `a.cartesian(b)` |
| `collect()` | A | | list of all | `rdd.collect()` |
| `count()` | A | | number | `rdd.count()` |
| `first()` / `take(n)` | A | | first element(s) | `rdd.take(3)` |
| `top(n)` / `takeOrdered(n)` | A | | largest / smallest n | `rdd.top(3)` |
| `reduce(f)` | A | | one value | `rdd.reduce(add)` |
| `fold(z, f)` / `aggregate(z, s, c)` | A | | one value | `rdd.aggregate((0, 0), s, c)` |
| `sum()` / `max()` / `min()` / `mean()` | A | | number | `rdd.sum()` |
| `countByKey()` / `countByValue()` | A | | dict | `pairs.countByKey()` |
| `collectAsMap()` | A | | dict (one value per key) | `pairs.collectAsMap()` |

---

## Part 7: Exam traps

1. **Transformations are lazy.** Writing `rdd.map(...)` does nothing until an
   action. An exception in your `map()` function shows up only at the action.
2. **`map()` vs `flatMap()`.** `map(split)` gives a list per line;
   `flatMap(split)` gives the words. `map` never changes the count.
3. **`flatMap()` over a string** gives its characters.
4. **Strings are not numbers.** After `split(",")`, convert with `int()` / `float()`.
5. **`mapValues(f)` receives only the value**, and keeps the partitioner; `map()`
   loses it.
6. **`mapPartitions(f)` must return an iterable**: `return (s, n)` gives two
   elements, not one tuple.
7. **`reduce()` needs an associative and commutative function.** Subtraction gives
   partition-dependent answers. `reduce()` on an empty RDD is an error.
8. **The zero value of `fold()` / `foldByKey()` / `aggregateByKey()` is used once
   per partition** (per key). A non-neutral zero gives wrong results.
9. **An average cannot be done with a plain `reduceByKey(f)`** on the raw values:
   carry `(sum, count)` and divide at the end.
10. **`groupByKey()` shuffles every value.** Use `reduceByKey()` / `aggregateByKey()`
    when the reduction can be done in pairs.
11. **`groupByKey()` gives a `ResultIterable`**, not a list: use `mapValues(list)`.
12. **`join()` is inner.** Unmatched keys disappear silently. A key with `m` and `n`
    values gives `m × n` rows.
13. **Joins need `(key, value)` pairs.** A 3-field tuple is not a pair RDD: re-shape it
    with `map()` first.
14. **Outer joins produce `None`.** Handle it before doing arithmetic.
15. **`union()` keeps duplicates**; `intersection()` removes them; `subtract()` keeps
    them.
16. **`take(n)` is not `top(n)`.** `take` = first in partition order;
    `top` = largest.
17. **`countByKey()`, `collectAsMap()`, `collect()` are actions** that bring data to
    the driver. `collectAsMap()` keeps only one value per key.
18. **Output order is not guaranteed** after a shuffle: sort it if order matters, and
    break ties explicitly.

---

## Part 8: Test yourself

Predict the output, then open the answer. All of them use the data from this page.

**Q1.** What does this return?

```text
sc.parallelize(["a b", "", "c"], 2).flatMap(lambda s: s.split()).count()
```

<details><summary>Answer</summary>

```python
>>> sc.parallelize(["a b", "", "c"], 2).flatMap(lambda s: s.split()).count()
3
```

`"a b"` gives 2 words, `""` gives none, `"c"` gives 1. With `map()` instead, the count
would be 3 as well, but for a different reason: 3 lines → 3 lists.

</details>

**Q2.** What is the number of orders per category, as a sorted list, using
`orders`, `map()` and `reduceByKey()`?

<details><summary>Answer</summary>

```python
>>> sorted(orders.map(lambda o: (o[2], 1)).reduceByKey(lambda a, b: a + b).collect())
[('books', 4), ('games', 2), ('music', 1)]
```

</details>

**Q3.** What does this return, and why?

```text
sc.parallelize([("a", 1), ("b", 2), ("a", 3)], 2).foldByKey(10, lambda x, y: x + y).collect()
```

<details><summary>Answer</summary>

```python
>>> sorted(sc.parallelize([("a", 1), ("b", 2), ("a", 3)], 2)
...          .foldByKey(10, lambda x, y: x + y).collect())
[('a', 24), ('b', 12)]
```

The two partitions are `[("a", 1)]` and `[("b", 2), ("a", 3)]`. Key `a` is in both
partitions, so it gets the zero `10` **twice**: `(10 + 1) + (10 + 3) = 24`. Key `b`
gets it once: `10 + 2 = 12`.

</details>

**Q4.** How many rows does `x.join(y)` return?

```text
x = sc.parallelize([("k", 1), ("k", 2), ("j", 3)])
y = sc.parallelize([("k", "a"), ("k", "b"), ("k", "c"), ("m", "d")])
```

<details><summary>Answer</summary>

```python
>>> x = sc.parallelize([("k", 1), ("k", 2), ("j", 3)], 2)
>>> y = sc.parallelize([("k", "a"), ("k", "b"), ("k", "c"), ("m", "d")], 2)
>>> x.join(y).count()
6
>>> x.fullOuterJoin(y).count()
8
```

Only `k` is on both sides: `2 × 3 = 6`. A full outer join adds `j` and `m`
(one row each): 8.

</details>

**Q5.** Using `amounts` (`(customer_id, amount)`), compute each customer's average
order amount with `aggregateByKey()`.

<details><summary>Answer</summary>

```python
>>> sorted(amounts.aggregateByKey((0, 0),
...                               lambda acc, v: (acc[0] + v, acc[1] + 1),
...                               lambda a, b: (a[0] + b[0], a[1] + b[1]))
...               .mapValues(lambda p: p[0] / p[1])
...               .collect())
[('c1', 20.0), ('c2', 32.5), ('c3', 40.0), ('c5', 10.0)]
```

</details>

**Q6.** Which customers (names) have **no** orders? Give two different solutions.

<details><summary>Answer</summary>

```python
>>> names.subtractByKey(amounts).values().collect()
['Dana']
>>> (names.leftOuterJoin(amounts)
...       .filter(lambda kv: kv[1][1] is None)
...       .map(lambda kv: kv[1][0])
...       .collect())
['Dana']
```

</details>

**Q7.** What does this return?

```text
a = sc.parallelize([1, 1, 2, 3])
b = sc.parallelize([1, 4])
a.union(b).count(), a.intersection(b).count(), a.subtract(b).count()
```

<details><summary>Answer</summary>

```python
>>> a = sc.parallelize([1, 1, 2, 3], 2)
>>> b = sc.parallelize([1, 4], 2)
>>> a.union(b).count(), a.intersection(b).count(), a.subtract(b).count()
(6, 1, 2)
```

`union` keeps all 6 elements. `intersection` = `{1}`. `subtract` removes **every**
`1` from `a` (both of them), leaving `[2, 3]`.

</details>

**Q8.** What is wrong with this "average per key"?

```text
pairs.reduceByKey(lambda a, b: (a + b) / 2)
```

<details><summary>Answer</summary>

Averaging two values at a time is not associative: the result depends on the order
and the grouping. For `[10, 20, 60]` the true average is `30`, but:

```python
>>> sc.parallelize([("k", 10), ("k", 20), ("k", 60)], 1).reduceByKey(lambda a, b: (a + b) / 2).collect()
[('k', 37.5)]
```

`((10 + 20) / 2 + 60) / 2 = 37.5`. Carry `(sum, count)` instead (Q5).

</details>

**Q9.** Find the **top 2 customers by total spending**, with their names, biggest
first.

<details><summary>Answer</summary>

```python
>>> (amounts.reduceByKey(lambda a, b: a + b)
...         .join(names)
...         .map(lambda kv: (kv[1][1], kv[1][0]))
...         .top(2, key=lambda kv: kv[1]))
[('Bob', 65), ('Alice', 60)]
```

`c5` (spent 10) has no name and is dropped by the join, which does not matter for
the top 2.

</details>

**Q10.** Which of these cause a shuffle?
`map`, `filter`, `reduceByKey`, `mapValues`, `join`, `union`, `distinct`,
`flatMap`, `groupByKey`, `sortBy`

<details><summary>Answer</summary>

`reduceByKey`, `join`, `distinct`, `groupByKey`, `sortBy`. The others (`map`,
`filter`, `mapValues`, `union`, `flatMap`) are narrow.

(One exception worth knowing: a `join()` or `reduceByKey()` on RDDs that are
**already** partitioned the same way by key can skip the shuffle; see
[1.3](#13-mapvalues).)

</details>

---

## Want more practice?

* [`../rdd_practice_questions/`](../rdd_practice_questions): 10 sets of practice
  questions with full answers (Set 10 is an exam-style mixed review).
* [`../rdd_reducers_by_key/real_life_examples/`](../rdd_reducers_by_key/real_life_examples):
  one real-life case per by-key reducer, each solved the "right" way and the
  awkward ways.
* [`../rdd_join_operation/`](../rdd_join_operation): join examples in RDD and
  MapReduce.
