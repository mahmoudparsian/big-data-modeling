# PySpark RDD Practice Questions — Set 09: Answers

Solutions to the nine [Set 09 practice questions](QUESTIONS_09.md). Every
session below was run in the `bin/pyspark` shell (Spark 4.2.0), started from
this folder. The output is copied from those real runs.

Try the questions yourself first. There are often several correct
solutions, and yours doesn't need to match these exactly.

* [Q1 (Basic): A broadcast lookup table](#q1-basic--a-broadcast-lookup-table)
* [Q2 (Basic): Counting bad records with an accumulator](#q2-basic--counting-bad-records-with-an-accumulator)
* [Q3 (Basic): Several accumulators in one pass](#q3-basic--several-accumulators-in-one-pass)
* [Q4 (Intermediate): A broadcast join](#q4-intermediate--a-broadcast-join)
* [Q5 (Intermediate): Broadcasting a set of stop words](#q5-intermediate--broadcasting-a-set-of-stop-words)
* [Q6 (Intermediate): When accumulators count twice](#q6-intermediate--when-accumulators-count-twice)
* [Q7 (Intermediate+): Custom accumulators](#q7-intermediate--custom-accumulators)
* [Q8 (Intermediate+): An iterative algorithm: k-means with a broadcast](#q8-intermediate--an-iterative-algorithm-k-means-with-a-broadcast)
* [Q9 (Intermediate+): A clean-and-enrich pipeline](#q9-intermediate--a-clean-and-enrich-pipeline)

The `stages_run()` helper from [Set 08](QUESTIONS_08.md#getting-started) is used
in Q4 and Q9:

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

## Q1 (Basic) — A broadcast lookup table

```python
>>> names = sc.broadcast({"US": "United States", "FR": "France", "JP": "Japan",
...                       "DE": "Germany", "IN": "India"})
>>> type(names).__name__
'Broadcast'
>>> len(names.value), names.value["JP"]
(5, 'Japan')

>>> codes = sc.parallelize(["US", "FR", "JP", "US", "DE", "XX", "IN"], 3)
>>> codes.map(lambda c: names.value.get(c, "unknown")).collect()
['United States', 'France', 'Japan', 'United States', 'Germany', 'unknown', 'India']
>>> codes.filter(lambda c: c in names.value).count()
6

>>> names.unpersist()
>>> codes.map(lambda c: names.value.get(c, "unknown")).collect()[:2]
['United States', 'France']
>>> names.destroy()
```

**Notes:**

* `sc.broadcast(v)` sends a **read-only** copy of `v` to every executor **once**, and
  keeps it there. Tasks read it with `.value`. Here it's a dictionary, but it can be any
  Python object that can be pickled: a set, a list, a trained model...
* **Why not just use the dictionary in the lambda?** That works too: Spark packs
  whatever a function uses into **every task** it sends. For a small dict, that's
  fine. For a 100 MB lookup table and 1,000 tasks, it means shipping 100 GB.
  A broadcast is sent **once per executor**, and shared by all the tasks that run there.
* Always look up with `.get(key, default)`. A code that's missing from the table
  (`"XX"`) shouldn't crash the whole job.
* **`unpersist()`** removes the copies from the executors, to free memory. If it's
  used again, Spark simply sends it again, so the job above still works.
* **`destroy()`** removes it **for good**, on the driver too. Using `names` in a job
  after that fails with an error. Call it when you're completely done.

---

## Q2 (Basic) — Counting bad records with an accumulator

```python
>>> bad = sc.accumulator(0)
>>> def parse(line):
...     f = line.split(",")
...     if len(f) == 3 and f[2].lstrip("-").isdigit():
...         return [(f[0], int(f[2]))]
...     bad.add(1)
...     return []
...
>>> raw = sc.textFile("data/temperatures.txt", 2)
>>> good = raw.flatMap(parse)
>>> bad.value
0
>>> good.count()
10
>>> bad.value
4
```

**Notes:**

* An **accumulator** is a shared counter that tasks can only **add to**
  (`bad.add(1)`), and only the **driver** can **read** (`bad.value`). Reading `.value`
  inside a task raises an error. Tasks run on many machines at once, so the total
  only exists after Spark adds up every task's part.
* This is the classic use: count something **on the side** (here, the 4 bad lines)
  while the RDD computes the main result (the 10 good records). You get both in **one
  pass** over the data.
* **Accumulators follow laziness.** Before the action, `parse` hasn't run, so
  `bad.value` is still 0. Read an accumulator **after** the action that updates it.
* `flatMap()` returning `[]` or `[record]` is a neat way to parse and filter in one step,
  and a good place to put the counter.
* Compare with [Set 01, Q8](ANSWERS_01.md#q8-intermediate--temperature-statistics-with-dirty-data),
  which needed a separate `filter()` and `count()` to find the bad lines.

---

## Q3 (Basic) — Several accumulators in one pass

```python
>>> header = sc.accumulator(0)
>>> blank = sc.accumulator(0)
>>> short = sc.accumulator(0)
>>> missing = sc.accumulator(0)
>>> total_temp = sc.accumulator(0.0)
>>> def classify(line):
...     f = line.split(",")
...     if line.strip() == "":
...         blank.add(1)
...         return []
...     if f[0] == "city":
...         header.add(1)
...         return []
...     if len(f) != 3:
...         short.add(1)
...         return []
...     if not f[2].lstrip("-").isdigit():
...         missing.add(1)
...         return []
...     total_temp.add(float(f[2]))
...     return [(f[0], int(f[2]))]
...
>>> readings = raw.flatMap(classify)
>>> n = readings.count()
>>> n
10
>>> (header.value, blank.value, short.value, missing.value)
(1, 1, 1, 1)
>>> total_temp.value, round(total_temp.value / n, 2)
(201.0, 20.1)
```

**Notes:**

* Each kind of problem gets its **own** counter, so the report says **why**
  records were dropped, not just how many. That's what you'd log or show on a
  data-quality dashboard.
* `sc.accumulator(0.0)` makes a **float** accumulator. The starting value decides the
  type: `0` for whole numbers, `0.0` for decimals.
* `total_temp` is a running sum of the valid temperatures, so the average comes
  "for free" from the same pass. (`reduce()` or a reducer from Set 04 would work
  too. An accumulator is handy when the main RDD is busy computing something else.)
* Five counters, **one** job: the data was read only once.

---

## Q4 (Intermediate) — A broadcast join

```python
>>> orders = sc.textFile("data/shop_orders.txt", 2).map(lambda line: line.split(","))
>>> product_table = dict(sc.textFile("data/shop_products.txt", 2)
...                        .map(lambda line: line.split(","))
...                        .map(lambda f: (f[0], (f[1], int(f[3]))))
...                        .collect())
>>> sorted(product_table.items())
[('p1', ('laptop', 1000)), ('p2', ('mouse', 20)), ('p3', ('desk', 300)), ('p4', ('chair', 150)), ('p5', ('lamp', 40))]

>>> products = sc.broadcast(product_table)
>>> def enrich(f):
...     p = products.value.get(f[2])
...     if p is None:
...         return []
...     return [(f[0], p[0], int(f[3]) * p[1])]
...
>>> order_lines = orders.flatMap(enrich)
>>> order_lines.count()
9
>>> sorted(order_lines.collect(), key=lambda r: int(r[0][1:]))[:4]
[('o1', 'laptop', 1000), ('o2', 'mouse', 60), ('o3', 'mouse', 40), ('o4', 'desk', 300)]

>>> def shuffle_join():
...     product_rdd = (sc.textFile("data/shop_products.txt", 2)
...                      .map(lambda line: line.split(","))
...                      .map(lambda f: (f[0], (f[1], int(f[3])))))
...     return (orders.map(lambda f: (f[2], (f[0], int(f[3]))))
...                   .join(product_rdd)
...                   .map(lambda kv: (kv[1][0][0], kv[1][1][0], kv[1][0][1] * kv[1][1][1])))
...
>>> sorted(shuffle_join().collect()) == sorted(order_lines.collect())
True
>>> stages_run(lambda: orders.flatMap(enrich).count())
1
>>> stages_run(lambda: shuffle_join().count())
2

>>> orders.filter(lambda f: f[2] not in products.value).map(lambda f: f[0]).collect()
['o10']
```

**Notes:**

* **A broadcast join** (also called a *map-side join*): collect the **small** table to
  the driver as a dict, broadcast it, and look each order up inside a `flatMap()`.
  Same result as `join()`, but **no shuffle**: 1 stage instead of 2. The big RDD
  (`orders`) never moves.
* The `flatMap()` with `[]` for a missing product gives **inner join** behavior: order
  `o10` (product `p9`) is dropped. For **left outer join** behavior, return the order
  with `None` instead of `[]`.
* **When to use it:** one side is **small** enough to fit in memory on the driver
  **and** on every executor: lookup tables, product catalogs, country codes, typically
  up to some tens or hundreds of MB. When **both** sides are big, use a normal
  `join()` (Set 05), which shuffles both sides.
* Spark SQL makes this choice automatically: DataFrame joins with a small table
  become broadcast joins.
* `int(r[0][1:])` sorts `o1`, `o2`, ..., `o10` by number, not as text (where `o10`
  would come before `o2`).

---

## Q5 (Intermediate) — Broadcasting a set of stop words

```python
>>> stop_words = sc.broadcast({"is", "the", "and", "a", "to"})
>>> words = (sc.textFile("data/quotes.txt", 2)
...            .flatMap(lambda line: line.split())
...            .map(lambda w: w.lower().strip(".,!?;:")))
>>> kept = words.filter(lambda w: w not in stop_words.value)
>>> words.count(), kept.count()
(40, 27)

>>> top = (kept.map(lambda w: (w, 1))
...            .reduceByKey(lambda a, b: a + b)
...            .map(lambda wc: ((-wc[1], wc[0]), wc))
...            .sortByKey()
...            .map(lambda kv: kv[1]))
>>> top.take(5)
[('data', 5), ('big', 4), ('spark', 4), ('learn', 3), ('map', 2)]
```

**Notes:**

* Compare with [Set 02, Q4](ANSWERS_02.md#q4-intermediate--word-count-with-the-top-words),
  where the top words were `the` (6) and `is` (5). Those **stop words** say nothing
  about the text, so removing them makes the top words useful: `data`, `big`,
  `spark`.
* Broadcast a **`set`**, not a list: `w in some_set` takes the same time however big the
  set is, while `w in some_list` checks every item. With a real stop-word list (hundreds
  of words) and billions of words, that difference matters.
* The same pattern works for any **allow-list** or **block-list**: valid product ids,
  banned users, known bots...

---

## Q6 (Intermediate) — When accumulators count twice

```python
>>> seen = sc.accumulator(0)
>>> def tick(x):
...     seen.add(1)
...     return x
...
>>> plain = sc.parallelize(range(10), 2).map(tick)
>>> plain.count()
10
>>> seen.value
10
>>> plain.count()
10
>>> seen.value
20

>>> seen = sc.accumulator(0)
>>> cached = sc.parallelize(range(10), 2).map(tick).cache()
>>> cached.count()
10
>>> cached.count()
10
>>> seen.value
10

>>> evens = sc.accumulator(0)
>>> sc.parallelize(range(10), 2).foreach(lambda x: evens.add(1) if x % 2 == 0 else None)
>>> evens.value
5
```

**Notes:**

* **Accumulators in transformations** (`map`, `filter`, `flatMap`...) are updated
  **every time the transformation runs**. Without a cache, every action runs it
  again, so 2 actions count 20 for 10 elements.
* `cache()` fixes it here, because the second action reads the cached partitions and
  `tick` doesn't run again. (We saw the same effect in
  [Set 03, Q3](ANSWERS_03.md#q3-basic--expensive-setup-once-per-partition) and
  [Set 08, Q3](ANSWERS_08.md#q3-basic--cache-compute-once-use-many-times).)
  But a cached partition can still be **recomputed** if it's lost, and a failed or
  slow task can be **re-run**, and each run adds again.
* **Accumulators in actions** (`foreach()`) have a stronger guarantee: Spark applies
  each successful task's updates **exactly once**, even if the task was re-run.
* **The rule:**
  * for **exact counts** that the program depends on, update the accumulator in an
    **action** like `foreach()`, or compute the count as a real result (`count()`,
    `reduceByKey()`);
  * accumulators in transformations are fine for **monitoring and debugging**
    ("about 4 bad lines"), as in Q2 and Q3, but don't build business logic on them.

---

## Q7 (Intermediate+) — Custom accumulators

```python
>>> from collections import Counter
>>> from pyspark.accumulators import AccumulatorParam
>>> class CounterParam(AccumulatorParam):
...     def zero(self, initial):
...         return Counter()
...     def addInPlace(self, c1, c2):
...         c1.update(c2)
...         return c1
...
>>> problems = sc.accumulator(Counter(), CounterParam())
>>> def parse_and_report(line):
...     f = line.split(",")
...     if line.strip() == "":
...         problems.add(Counter({"blank line": 1}))
...         return []
...     if len(f) != 3:
...         problems.add(Counter({"wrong field count": 1}))
...         return []
...     if not f[2].lstrip("-").isdigit():
...         problems.add(Counter({"bad temperature: " + f[2]: 1}))
...         return []
...     return [(f[0], int(f[2]))]
...
>>> raw.flatMap(parse_and_report).count()
10
>>> sorted(problems.value.items())
[('bad temperature: NA', 1), ('bad temperature: temp_c', 1), ('blank line', 1), ('wrong field count', 1)]

>>> class MaxParam(AccumulatorParam):
...     def zero(self, initial):
...         return float("-inf")
...     def addInPlace(self, a, b):
...         return max(a, b)
...
>>> hottest = sc.accumulator(float("-inf"), MaxParam())
>>> raw.flatMap(parse).foreach(lambda kv: hottest.add(kv[1]))
>>> hottest.value
38
```

**Notes:**

* Built-in accumulators only **add numbers**. For anything else, write an
  **`AccumulatorParam`** with two methods:
  * `zero(initial)`: the "empty" value each task starts from;
  * `addInPlace(a, b)`: combine two values into one. It's used both inside a task,
    for each `add()`, and on the driver, to merge the tasks' results.
* These are **the same rules as for a reducer** (Set 04, Set 06): `addInPlace` must be
  associative and commutative, because Spark merges the tasks' parts in any order, and
  `zero` must be **neutral** (an empty `Counter`, `-inf` for max), because every task
  starts from it. The zero-value trap from
  [Set 06, Q7](ANSWERS_06.md#q7-intermediate--the-zero-value-trap) applies here too.
* A `Counter` accumulator replaces Q3's five separate counters with one, and it
  discovers the problem **types** by itself. It even reported the header line
  as "bad temperature: temp_c", which Q3 needed a special rule for.
* `hottest` uses `foreach()`, an **action**, so it gets the exactly-once guarantee
  from Q6.
* Keep accumulator values **small** (counts, a max, a few dozen categories). Every
  task's part is sent back to the driver. Don't use an accumulator to collect the
  data itself: that's what `collect()`, or better a reducer, is for.

---

## Q8 (Intermediate+) — An iterative algorithm: k-means with a broadcast

```python
>>> points = sc.parallelize([1.0, 2.0, 3.0, 4.0, 10.0, 11.0, 12.0, 13.0], 2).cache()
>>> centroids = [1.0, 2.0]
>>> for iteration in range(1, 11):
...     current = sc.broadcast(centroids)
...     def closest(p):
...         cs = current.value
...         return min(range(len(cs)), key=lambda i: abs(p - cs[i]))
...     new = (points.map(lambda p: (closest(p), (p, 1)))
...                  .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
...                  .mapValues(lambda t: t[0] / t[1])
...                  .sortByKey()
...                  .collect())
...     new_centroids = [c for i, c in new]
...     shift = max(abs(a - b) for a, b in zip(centroids, new_centroids))
...     print(iteration, [round(c, 3) for c in new_centroids], round(shift, 3))
...     current.unpersist()
...     centroids = new_centroids
...     if shift < 0.001:
...         break
...
1 [1.0, 7.857] 5.857
2 [2.5, 11.5] 3.643
3 [2.5, 11.5] 0.0

>>> final = sc.broadcast(centroids)
>>> sorted(points.map(lambda p: (min(range(2), key=lambda i: abs(p - final.value[i])), p))
...              .groupByKey()
...              .mapValues(sorted)
...              .collect())
[(0, [1.0, 2.0, 3.0, 4.0]), (1, [10.0, 11.0, 12.0, 13.0])]
```

**Notes:**

* **k-means** groups points into *k* clusters. Each iteration does two steps:
  1. **assign** each point to its closest centroid (a `map()`, which needs the current
     centroids, so they are **broadcast**);
  2. **move** each centroid to the average of its points (the `(sum, count)` average
     from Set 04 and Set 06, with `reduceByKey()`).
  It stops when the centroids stop moving.
* Iteration 1: only point `1` is closer to `1.0` than to `2.0`, so the centroids become
  `1.0` and the average of the other 7 points, `55 / 7 = 7.857`. Iteration 2: points
  1–4 are now closer to `1.0`, so the centroids become `2.5` and `11.5`. Iteration 3:
  nothing changes, so it has converged.
* **The centroids change every iteration**, so the loop makes a **new** broadcast each
  time and `unpersist()`s the old one. A broadcast is **read-only**. You never change
  it; you replace it.
* **`points` is cached**, because every iteration reads it again. Without `cache()`,
  every iteration would re-create it from the source: the classic reason why
  iterative algorithms need caching (Set 08).
* Only *k* numbers go out (the broadcast) and *k* numbers come back (`collect()`) per
  iteration. The points themselves never leave their partitions. This is how MLlib's
  k-means works too, at much larger scale.

---

## Q9 (Intermediate+) — A clean-and-enrich pipeline

```python
>>> city_of = dict(sc.textFile("data/shop_customers.txt", 2)
...                  .map(lambda line: line.split(","))
...                  .map(lambda f: (f[0], f[2]))
...                  .collect())
>>> category_price = dict(sc.textFile("data/shop_products.txt", 2)
...                         .map(lambda line: line.split(","))
...                         .map(lambda f: (f[0], (f[2], int(f[3]))))
...                         .collect())
>>> customers_b = sc.broadcast(city_of)
>>> products_b = sc.broadcast(category_price)
>>> issues = sc.accumulator(Counter(), CounterParam())

>>> def to_sale(f):
...     order_id, customer_id, product_id, qty = f
...     city = customers_b.value.get(customer_id)
...     product = products_b.value.get(product_id)
...     if city is None:
...         issues.add(Counter({"unknown customer": 1}))
...         return []
...     if product is None:
...         issues.add(Counter({"unknown product": 1}))
...         return []
...     category, price = product
...     return [((city, category), int(qty) * price)]
...
>>> sales = orders.flatMap(to_sale).cache()
>>> sales.count()
8
>>> sorted(issues.value.items())
[('unknown customer', 1), ('unknown product', 1)]

>>> sales.reduceByKey(lambda a, b: a + b).sortByKey().collect()
[(('Austin', 'electronics'), 60), (('Austin', 'furniture'), 300), (('San Jose', 'electronics'), 1140), (('San Jose', 'furniture'), 450), (('San Jose', 'home'), 160)]
>>> sorted(issues.value.items())
[('unknown customer', 1), ('unknown product', 1)]

>>> stages_run(lambda: orders.flatMap(to_sale).reduceByKey(lambda a, b: a + b).count())
2
>>> def three_shuffles():
...     products = (sc.textFile("data/shop_products.txt", 2)
...                   .map(lambda line: line.split(","))
...                   .map(lambda f: (f[0], (f[2], int(f[3])))))
...     customers = (sc.textFile("data/shop_customers.txt", 2)
...                    .map(lambda line: line.split(","))
...                    .map(lambda f: (f[0], f[2])))
...     return (orders.map(lambda f: (f[2], (f[1], int(f[3]))))
...                   .join(products)
...                   .map(lambda kv: (kv[1][0][0], (kv[1][1][0], kv[1][0][1] * kv[1][1][1])))
...                   .join(customers)
...                   .map(lambda kv: ((kv[1][1], kv[1][0][0]), kv[1][0][1]))
...                   .reduceByKey(lambda a, b: a + b))
...
>>> sorted(three_shuffles().collect()) == sales.reduceByKey(lambda a, b: a + b).sortByKey().collect()
True
>>> stages_run(lambda: three_shuffles().count())
4
```

**Notes:**

* This is the **same report** as [Set 05, Q8](ANSWERS_05.md#q8-intermediate--revenue-per-city-and-category-a-3-way-join),
  revenue per `(city, category)`, built a different way: both small tables are
  **broadcast**, and each order is cleaned and enriched in **one** `flatMap()`.
* **Stages:** the broadcast version needs only the shuffle of the final
  `reduceByKey()`, so **2 stages**. The two-join version shuffles for each join and
  for the reduce, so **4 stages**, and both joins move the **whole** orders RDD across
  the network.
* **The accumulator** reports what was dropped, and why: one order with an unknown
  customer (`o8`) and one with an unknown product (`o10`). In Set 05, the inner joins
  dropped these two orders **silently**, and we had to find them with extra outer joins.
* **`sales` is cached, and the accumulator didn't double.** After the
  `reduceByKey()` job, `issues` still shows 1 + 1, because the second job read the cached
  `sales` instead of running `to_sale` again. Without `cache()` it would show 2 + 2 (Q6).
* **Broadcast and accumulators together:** broadcast variables carry **read-only data
  out** to the tasks, and accumulators carry **small summaries back** to the driver.
  The RDD itself carries the main result.
