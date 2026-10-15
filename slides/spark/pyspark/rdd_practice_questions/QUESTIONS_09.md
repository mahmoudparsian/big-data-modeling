# PySpark RDD Practice Questions — Set 09: Broadcast Variables and Accumulators

Nine practice questions, three at each level, meant to be solved
**interactively** in the `bin/pyspark` shell. This set is about Spark's two
**shared variables**, which move small amounts of data between the driver and the tasks:

| | Broadcast variable | Accumulator |
|---|---|---|
| Create | `b = sc.broadcast(value)` | `acc = sc.accumulator(0)` |
| Direction | driver → tasks | tasks → driver |
| Tasks can | **read** it: `b.value` | only **add** to it: `acc.add(n)` |
| Driver can | `b.value`, `b.unpersist()`, `b.destroy()` | **read** it: `acc.value` |
| Typical use | a lookup table, a set, model parameters | counting bad records, side statistics |

| # | Level | Topic | Input |
|---|---|---|---|
| [Q1](#q1-basic--a-broadcast-lookup-table) | Basic | A broadcast lookup table | Python dict and list |
| [Q2](#q2-basic--counting-bad-records-with-an-accumulator) | Basic | Counting bad records with an accumulator | [`data/temperatures.txt`](data/temperatures.txt) |
| [Q3](#q3-basic--several-accumulators-in-one-pass) | Basic | Several accumulators in one pass | [`data/temperatures.txt`](data/temperatures.txt) |
| [Q4](#q4-intermediate--a-broadcast-join) | Intermediate | A broadcast join | [`data/shop_orders.txt`](data/shop_orders.txt), [`data/shop_products.txt`](data/shop_products.txt) |
| [Q5](#q5-intermediate--broadcasting-a-set-of-stop-words) | Intermediate | Broadcasting a set of stop words | [`data/quotes.txt`](data/quotes.txt) |
| [Q6](#q6-intermediate--when-accumulators-count-twice) | Intermediate | When accumulators count twice | Python range |
| [Q7](#q7-intermediate--custom-accumulators) | Intermediate+ | Custom accumulators | [`data/temperatures.txt`](data/temperatures.txt) |
| [Q8](#q8-intermediate--an-iterative-algorithm-k-means-with-a-broadcast) | Intermediate+ | An iterative algorithm: k-means with a broadcast | Python list |
| [Q9](#q9-intermediate--a-clean-and-enrich-pipeline) | Intermediate+ | A clean-and-enrich pipeline | all three `data/shop_*.txt` files |

You may also use everything from the earlier sets.

Try each question on your own before you open the
[answers](ANSWERS_09.md).

## Files

| File | Purpose |
|---|---|
| [QUESTIONS_09.md](QUESTIONS_09.md) | The nine questions (this page) |
| [ANSWERS_09.md](ANSWERS_09.md) | Step-by-step solutions, with real output from Spark 4.2.0 |
| [data/temperatures.txt](data/temperatures.txt) | Input for Q2, Q3, Q7 (also used in Set 01): `city,date,temp_c`, with a header and bad records |
| [data/shop_orders.txt](data/shop_orders.txt) | Input for Q4, Q9 (also used in Set 05): `order_id,customer_id,product_id,quantity` |
| [data/shop_products.txt](data/shop_products.txt) | Input for Q4, Q9 (also used in Set 05): `product_id,name,category,price` |
| [data/shop_customers.txt](data/shop_customers.txt) | Input for Q9 (also used in Sets 05 and 08): `customer_id,name,city` |
| [data/quotes.txt](data/quotes.txt) | Input for Q5 (also used in Sets 02 and 03): plain text sentences |

## Getting started

Start `$SPARK_HOME/bin/pyspark` **from this folder**, as in
[Set 01](QUESTIONS_01.md#getting-started), and do the questions **in order**, in one
shell session. Later questions reuse RDDs and functions from earlier ones.

Q4 and Q9 count **stages** (shuffles) with the `stages_run()` helper from
[Set 08](QUESTIONS_08.md#getting-started). Paste it into the shell first.

---

## Q1 (Basic) — A broadcast lookup table

**Input:**

```python
names = sc.broadcast({"US": "United States", "FR": "France", "JP": "Japan",
                      "DE": "Germany", "IN": "India"})
codes = sc.parallelize(["US", "FR", "JP", "US", "DE", "XX", "IN"], 3)
```

**Tasks:**

1. Print the type of `names`, the number of entries in its value, and the name for `"JP"`.
2. Use the broadcast inside a `map()` to turn each code into a country name. Use
   `"unknown"` for a code that isn't in the table.
3. Count the codes that **are** in the table.
4. Call `names.unpersist()`, and then run task 2 again (show the first 2 names). Does it
   still work? Finally, call `names.destroy()`.

**Expected output:**

```
'Broadcast'
(5, 'Japan')
['United States', 'France', 'Japan', 'United States', 'Germany', 'unknown', 'India']
6
['United States', 'France']
```

*Think about it:* task 2 would also work with a plain dict inside the lambda, without
`sc.broadcast()`. So why broadcast?

---

## Q2 (Basic) — Counting bad records with an accumulator

**Input:** [`data/temperatures.txt`](data/temperatures.txt), read with **2** partitions.
It's the dirty file from [Set 01, Q8](QUESTIONS_01.md#q8-intermediate--temperature-statistics-with-dirty-data):
a header, an empty line, an `NA`, and a line with a missing field.

**Tasks:**

1. Create an accumulator `bad = sc.accumulator(0)`.
2. Write a function `parse(line)` for `flatMap()`. For a valid line
   (3 fields, a whole-number temperature), it returns `[(city, temp)]`. Otherwise it
   adds 1 to `bad` and returns `[]`.
3. Make `good = raw.flatMap(parse)`. Print `bad.value` **before** any action.
4. Count `good`, then print `bad.value` again.

**Expected output:**

```
0
10
4
```

---

## Q3 (Basic) — Several accumulators in one pass

**Input:** the same `raw` RDD as in Q2.

**Tasks:**

1. Create **4** integer accumulators, `header`, `blank`, `short`, and `missing`, and
   **1** float accumulator, `total_temp`, starting at `0.0`.
2. Write a `classify(line)` function for `flatMap()` that adds 1 to the right counter
   for each kind of bad line (the header line, an empty line, a line without 3 fields,
   a temperature that isn't a number). For a valid line, it adds the temperature to
   `total_temp` and returns `[(city, temp)]`.
3. Count the valid readings. Print the 4 counters as a tuple.
4. Print `total_temp.value` and the average temperature, rounded to 2 decimals.

**Expected output:**

```
10
(1, 1, 1, 1)
(201.0, 20.1)
```

---

## Q4 (Intermediate) — A broadcast join

**Input:** [`data/shop_orders.txt`](data/shop_orders.txt) (read with **2**
partitions, as lists of fields) and the **small** table
[`data/shop_products.txt`](data/shop_products.txt).

**Tasks:**

1. Read the products into a **Python dict** on the driver,
   `{product_id: (name, price)}`, with `collect()`. Show it sorted.
2. Broadcast the dict. With **one** `flatMap()` over the orders, make
   `(order_id, product_name, amount)` rows, where `amount = quantity × price`.
   Skip orders whose product isn't in the table. Count the rows, and show the first 4,
   sorted by order **number**.
3. Build the same rows with a normal `join()` (as in Set 05), and check that both give
   the same rows.
4. With `stages_run()`, compare the stages of the broadcast version and of the `join()`
   version. Build the `join()` version **fresh** inside the lambda.
5. Which orders were skipped?

**Expected output:**

```
[('p1', ('laptop', 1000)), ('p2', ('mouse', 20)), ('p3', ('desk', 300)), ('p4', ('chair', 150)), ('p5', ('lamp', 40))]
9
[('o1', 'laptop', 1000), ('o2', 'mouse', 60), ('o3', 'mouse', 40), ('o4', 'desk', 300)]
True
1
2
['o10']
```

---

## Q5 (Intermediate) — Broadcasting a set of stop words

**Input:** [`data/quotes.txt`](data/quotes.txt), read with **2** partitions, split into
lowercase words with the punctuation `. , ! ? ; :` removed (as in Set 02, Q4).

**Tasks:**

1. Broadcast the **stop words** `{"is", "the", "and", "a", "to"}` as a Python `set`.
2. Remove the stop words from the words. Count the words before and after.
3. Show the top 5 remaining words by count (ties in alphabetical order).

**Expected output:**

```
(40, 27)
[('data', 5), ('big', 4), ('spark', 4), ('learn', 3), ('map', 2)]
```

*Think about it:* why a `set`, and not a `list`?

---

## Q6 (Intermediate) — When accumulators count twice

**Input:**

```python
seen = sc.accumulator(0)

def tick(x):
    seen.add(1)
    return x
```

**Tasks:**

1. Make `plain = sc.parallelize(range(10), 2).map(tick)`. Call `plain.count()`, print
   `seen.value`, call `plain.count()` again, and print `seen.value` again.
2. Reset `seen`. Make the same RDD with `.cache()` at the end, call `count()` twice,
   and print `seen.value`.
3. Use an accumulator `evens` and **`foreach()`** (an action) to count the even numbers
   in `range(10)`.
4. When can you trust an accumulator's value exactly?

**Expected output:**

```
10
10
10
20
10
10
10
5
```

---

## Q7 (Intermediate+) — Custom accumulators

Built-in accumulators only add numbers. With an `AccumulatorParam`, you can define
your own:

```python
from pyspark.accumulators import AccumulatorParam

class MyParam(AccumulatorParam):
    def zero(self, initial):      # the "empty" value each task starts from
        ...
    def addInPlace(self, a, b):   # combine two values into one
        ...

acc = sc.accumulator(initial_value, MyParam())
```

**Tasks:**

1. Write `CounterParam`, an accumulator of Python `collections.Counter`s. Use it to
   count the problems in `data/temperatures.txt` **by type**: `"blank line"`,
   `"wrong field count"`, or `"bad temperature: <the bad value>"`. Count the valid
   readings, and show the problem counts, sorted.
2. Write `MaxParam`, an accumulator that keeps the **largest** value it was given. Use
   it with `foreach()` on the valid readings from Q2 (`raw.flatMap(parse)`) to find the
   highest temperature.

**Expected output:**

```
10
[('bad temperature: NA', 1), ('bad temperature: temp_c', 1), ('blank line', 1), ('wrong field count', 1)]
38
```

*Think about it:* what rules must `zero()` and `addInPlace()` follow? Where have you
seen these rules before?

---

## Q8 (Intermediate+) — An iterative algorithm: k-means with a broadcast

**k-means** groups points into *k* clusters. It starts with *k* guesses for the cluster
centers (the **centroids**), then repeats two steps until the centroids stop moving:

1. **assign** each point to its closest centroid;
2. **move** each centroid to the average of the points assigned to it.

**Input:** 8 numbers on a line, and *k* = 2 starting centroids:

```python
points = sc.parallelize([1.0, 2.0, 3.0, 4.0, 10.0, 11.0, 12.0, 13.0], 2).cache()
centroids = [1.0, 2.0]
```

**Tasks:**

1. Write a loop (at most 10 iterations). In each iteration: **broadcast** the current
   centroids, assign the points with a `map()`, compute the new centroids with
   `reduceByKey()` (sorted by cluster number), and compute the **shift**: the largest
   change of any centroid. Print the iteration number, the new centroids (rounded to 3
   decimals), and the shift. `unpersist()` the broadcast, and stop when the shift is
   below `0.001`.
2. Show the final clusters: each cluster number with its sorted points.

**Expected output:**

```
1 [1.0, 7.857] 5.857
2 [2.5, 11.5] 3.643
3 [2.5, 11.5] 0.0
[(0, [1.0, 2.0, 3.0, 4.0]), (1, [10.0, 11.0, 12.0, 13.0])]
```

*Think about it:* why must the loop create a **new** broadcast in every iteration? And
why is `points` cached?

---

## Q9 (Intermediate+) — A clean-and-enrich pipeline

**Goal:** the revenue per `(city, category)`, the same report as
[Set 05, Q8](QUESTIONS_05.md#q8-intermediate--revenue-per-city-and-category-a-3-way-join),
but **without any join**.

**Input:** the `orders` RDD from Q4, and the two **small** tables
[`data/shop_customers.txt`](data/shop_customers.txt) and
[`data/shop_products.txt`](data/shop_products.txt).

**Tasks:**

1. Collect and broadcast two dicts: `{customer_id: city}` and
   `{product_id: (category, price)}`.
2. Create a `CounterParam` accumulator (from Q7) called `issues`.
3. Write `to_sale(fields)` for `flatMap()`. It returns `[((city, category), amount)]`
   for a good order. For an order with an unknown customer or an unknown product, it adds
   `"unknown customer"` or `"unknown product"` to `issues` and returns `[]`.
4. Make `sales = orders.flatMap(to_sale).cache()`. Count it, and show `issues`.
5. Compute the revenue per `(city, category)`, sorted, and show `issues` again. Did it change?
6. With `stages_run()`, compare the stages of this pipeline with those of the
   **two-join** version from Set 05 (build that one fresh inside a function, and check that it
   gives the same report).

**Expected output:**

```
8
[('unknown customer', 1), ('unknown product', 1)]
[(('Austin', 'electronics'), 60), (('Austin', 'furniture'), 300), (('San Jose', 'electronics'), 1140), (('San Jose', 'furniture'), 450), (('San Jose', 'home'), 160)]
[('unknown customer', 1), ('unknown product', 1)]
2
True
4
```
