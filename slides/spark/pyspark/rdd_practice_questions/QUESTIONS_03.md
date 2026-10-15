# PySpark RDD Practice Questions — Set 03: The Power of mapPartitions()

Nine practice questions, three at each level, meant to be solved
**interactively** in the `bin/pyspark` shell. This set is all about
`mapPartitions()` and why it matters.

`map(f)` calls `f` **once per element**. `mapPartitions(f)` calls `f`
**once per partition**, and passes it an **iterator** over all the elements of
that partition. That one difference lets you:

* do expensive setup (open a connection, load a model or a lookup table)
  **once per partition** instead of once per element (Q3, Q7);
* **combine data locally** before a shuffle, so far less data crosses the network (Q4, Q8);
* compute several statistics in **one pass** (Q5);
* hand the whole partition to a library that wants an iterator, like Python's
  `csv.reader` (Q6);
* keep **state** across the elements of a partition, like a running total (Q9).

| # | Level | Topic | Input |
|---|---|---|---|
| [Q1](#q1-basic--seeing-the-partitions) | Basic | Seeing the partitions | Python range |
| [Q2](#q2-basic--mappartitions-can-do-what-map-filter-and-flatmap-do) | Basic | `mapPartitions()` can do what `map`, `filter`, and `flatMap` do | Python range |
| [Q3](#q3-basic--expensive-setup-once-per-partition) | Basic | Expensive setup, once per partition | Python list |
| [Q4](#q4-intermediate--word-count-with-local-combining) | Intermediate | Word count with local combining | [`data/quotes.txt`](data/quotes.txt) |
| [Q5](#q5-intermediate--one-pass-statistics-and-empty-partitions) | Intermediate | One-pass statistics, and empty partitions | [`data/transactions.txt`](data/transactions.txt) |
| [Q6](#q6-intermediate--parsing-a-real-csv-file) | Intermediate | Parsing a real CSV file | [`data/customers.csv`](data/customers.csv) |
| [Q7](#q7-intermediate--calling-a-batch-api) | Intermediate+ | Calling a batch API | Python list |
| [Q8](#q8-intermediate--global-top-n) | Intermediate+ | Global top-N | Python list |
| [Q9](#q9-intermediate--running-total-across-partitions) | Intermediate+ | Running total across partitions | Python range |

Use the operations from [Set 01](QUESTIONS_01.md) and [Set 02](QUESTIONS_02.md),
plus these:

* **`mapPartitions(f)`**: `f` gets an iterator over one partition and must
  **return an iterable** (a list, or a generator using `yield`).
* **`mapPartitionsWithIndex(f)`**: like `mapPartitions()`, but `f(index, iterator)`
  also gets the partition number (0, 1, 2, ...).
* **Helpers for looking inside:** `rdd.getNumPartitions()`, and `rdd.glom()`,
  which turns each partition into one list, so `rdd.glom().collect()` shows
  you the partitions.
* **`sc.accumulator(0)`**: a counter that tasks can add to (`acc.add(1)`) and
  the driver can read (`acc.value`). We use it to **count how many times** a
  function really runs.

Try each question on your own before you open the
[answers](ANSWERS_03.md).

## Files

| File | Purpose |
|---|---|
| [QUESTIONS_03.md](QUESTIONS_03.md) | The nine questions (this page) |
| [ANSWERS_03.md](ANSWERS_03.md) | Step-by-step solutions, with real output from Spark 4.2.0 |
| [data/quotes.txt](data/quotes.txt) | Input for Q4 (also used in Set 02): plain text sentences |
| [data/transactions.txt](data/transactions.txt) | Input for Q5 (also used in Set 02): `customer,amount` |
| [data/customers.csv](data/customers.csv) | Input for Q6: a CSV file with a header and quoted fields |

## Getting started

Start `$SPARK_HOME/bin/pyspark` **from this folder**, as in
[Set 01](QUESTIONS_01.md#getting-started).

**Always give the number of partitions explicitly in this set**, e.g.
`sc.parallelize(data, 3)` or `sc.textFile(path, 2)`. The default depends on
how many CPU cores your machine has, so without it your partitions (and your
output) won't match the expected answers.

---

## Q1 (Basic) — Seeing the partitions

**Input:**

```python
rdd = sc.parallelize(range(1, 13), 3)
```

**Tasks:**

1. How many partitions does `rdd` have? Use `glom()` to show what's in each one.
2. Use `mapPartitions()` to compute the **sum of each partition**.
3. Use `mapPartitions()` to compute the **number of elements** in each partition.
4. Add up the partition sums on the driver to get the total of all elements.

**Expected output:**

```
3
[[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]
[10, 26, 42]
[4, 4, 4]
78
```

*Think about it:* what happens if your function returns `sum(it)` instead of
`[sum(it)]`? Try it.

---

## Q2 (Basic) — mapPartitions() can do what map, filter, and flatMap do

**Input:**

```python
nums = sc.parallelize(range(1, 11), 3)
```

**Tasks:** using **only `mapPartitions()`** (no `map`, `filter`, or `flatMap`), and
writing each function as a generator with `yield`:

1. Show the partitions with `glom()`.
2. Square every number (like `map`). Check that you get the same result as
   `nums.map(lambda x: x * x)`.
3. Keep only the even numbers (like `filter`).
4. Return `(min, max)` of **each partition**, so one element per partition.
5. Return each number **and** its negative, so 2 elements per input (like
   `flatMap`). Count the result.

**Expected output:**

```
[[1, 2, 3], [4, 5, 6], [7, 8, 9, 10]]
[1, 4, 9, 16, 25, 36, 49, 64, 81, 100]
True
[2, 4, 6, 8, 10]
[(1, 3), (4, 6), (7, 10)]
20
```

---

## Q3 (Basic) — Expensive setup, once per partition

Imagine that turning a country code into a country name needs a **lookup table
that is expensive to load**, from a database or a large file. We simulate that
with a function that builds a small dictionary and counts how often it's called:

```python
codes = sc.parallelize(["US", "FR", "JP", "US", "DE", "FR",
                        "JP", "US", "IN", "DE", "FR", "US"], 3)

setups = sc.accumulator(0)

def load_country_names():
    setups.add(1)        # count each (expensive) load
    return {"US": "United States", "FR": "France", "JP": "Japan",
            "DE": "Germany", "IN": "India"}
```

**Tasks:**

1. Use `map()` to turn each code into its name, calling
   `load_country_names()` inside the lambda. Collect it, then print
   `setups.value`. How many times was the table loaded?
2. Reset the counter (`setups = sc.accumulator(0)`). Now use `mapPartitions()`
   so the table is loaded **once per partition** and reused for every element.
   Collect it and print `setups.value`.
3. Check that both results are equal.

**Expected output:**

```
['United States', 'France', 'Japan', 'United States', 'Germany', 'France', 'Japan', 'United States', 'India', 'Germany', 'France', 'United States']
12
['United States', 'France', 'Japan', 'United States', 'Germany', 'France', 'Japan', 'United States', 'India', 'Germany', 'France', 'United States']
3
True
```

*Think about it:* with 1 billion elements in 1,000 partitions, how many loads
does each version do?

---

## Q4 (Intermediate) — Word count with local combining

**Input:** [`data/quotes.txt`](data/quotes.txt), read with **2** partitions:

```python
lines = sc.textFile("data/quotes.txt", 2)
words = (lines.flatMap(lambda line: line.split())
              .map(lambda w: w.lower().strip(".,!?;:")))
```

**Tasks:**

1. Make the usual `(word, 1)` pairs with `map()`. How many pairs are there?
   That's how many records `reduceByKey()` would have to shuffle.
2. Instead, use `mapPartitions()` to count the words **inside each partition**
   with a Python `dict`, and return `(word, local_count)` pairs. How many
   pairs now? How many come from each partition?
3. Finish both versions with `reduceByKey()` and check that they give the same
   word counts. Show the first 5 in alphabetical order.

**Expected output:**

```
40
23
[14, 9]
True
[('and', 2), ('big', 4), ('data', 5), ('fast', 1), ('fun', 1)]
```

---

## Q5 (Intermediate) — One-pass statistics, and empty partitions

**Input:** the amounts from [`data/transactions.txt`](data/transactions.txt),
read with **2** partitions:

```python
amounts = sc.textFile("data/transactions.txt", 2).map(lambda line: int(line.split(",")[1]))
```

**Tasks:**

1. Show the partitions with `glom()`.
2. Write a function `stats(it)` for `mapPartitions()` that goes over the
   partition **once** and returns one tuple `(min, max, sum, count)`.
3. Combine the per-partition tuples on the driver into the overall
   `(min, max, sum, count, average)`, with the average rounded to 2 decimals.
4. Now try your function on `sc.parallelize([5, 8, 1], 4)`. Look at the
   partitions first. Does your function still work? If not, fix it so that an
   **empty partition** returns nothing at all.

**Expected output:**

```
[[20, 15, 35, 50, 5, 10, 40], [25, 45, 60, 30, 100]]
[(5, 50, 175, 7), (25, 100, 260, 5)]
(5, 100, 435, 12, 36.25)
[[], [5], [8], [1]]
[(5, 5, 5, 1), (8, 8, 8, 1), (1, 1, 1, 1)]
```

**Hint:** don't call `list(it)`. Keep a running min, max, sum, and count in
plain variables. That's what "one pass" means, and it works for a partition of
any size.

---

## Q6 (Intermediate) — Parsing a real CSV file

**Input:** [`data/customers.csv`](data/customers.csv), read with **2** partitions.
It has a header line, and some fields contain **commas inside quotes**:

```
id,name,city,balance
101,"Smith, John",San Jose,250.50
102,Maria Garcia,"Austin, TX",100.00
...
```

**Tasks:**

1. Count the lines and the lines per partition. Then show the number of fields
   you get for each line with a plain `line.split(",")`. What goes wrong?
2. Python's `csv.reader` handles quotes correctly, and it reads from an
   **iterator of lines**, which is exactly what `mapPartitions()` gives you. Use
   **`mapPartitionsWithIndex()`** to skip the header (it is the first line of
   partition 0) and parse every other line into a tuple
   `(id: int, name, city, balance: float)`. Count the customers and show the first 2.
3. Which customers have a balance **over 100**? Show their names.
4. Show all the cities.

**Expected output:**

```
7
[4, 3]
[4, 5, 5, 5, 4, 6, 4]
6
[(101, 'Smith, John', 'San Jose', 250.5), (102, 'Maria Garcia', 'Austin, TX', 100.0)]
['Smith, John', 'Chen, Wei']
['San Jose', 'Austin, TX', 'Boston', 'Seattle', 'Portland, OR', 'Denver']
```

---

## Q7 (Intermediate+) — Calling a batch API

Many web services and databases let you look up **many keys in one call**, and
each call is slow (a network round trip). We simulate such a service, and count
the calls:

```python
items = sc.parallelize(["a", "b", "c", "d", "e", "f",
                        "g", "h", "i", "j", "k", "l"], 2)

calls = sc.accumulator(0)

def api_lookup_batch(keys):
    calls.add(1)         # count each (slow) call
    return [k.upper() for k in keys]
```

**Tasks:**

1. Use `map()` to look up each item **on its own** (a batch of one key).
   Collect the result and print the number of calls.
2. Reset the counter. Use `mapPartitions()` to send the items in **batches of
   up to 4 keys**. Collect the result and print the number of calls.
3. Show how many items are in each partition, and explain the number of calls in task 2.

**Expected output:**

```
['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L']
12
['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L']
4
[6, 6]
```

**Hint:** `itertools.islice(it, 4)` takes the next (up to) 4 elements from an
iterator. Keep taking batches until you get an empty one.

---

## Q8 (Intermediate+) — Global top-N

**Input** (20 exam scores in 4 partitions):

```python
scores = sc.parallelize([72, 95, 61, 88, 79, 99, 54, 83, 90, 67,
                         76, 92, 58, 85, 70, 97, 63, 81, 74, 89], 4)
```

**Tasks:**

1. Show the partitions.
2. Use `mapPartitions()` to find the **top 3 of each partition**. Show the result
   with `glom()`.
3. Collect those local winners. How many are there? Find the overall top 3 from them.
4. Check your answer against simply sorting all the scores.

**Expected output:**

```
[[72, 95, 61, 88, 79], [99, 54, 83, 90, 67], [76, 92, 58, 85, 70], [97, 63, 81, 74, 89]]
[[95, 88, 79], [99, 90, 83], [92, 85, 76], [97, 89, 81]]
12
[99, 97, 95]
[99, 97, 95]
```

**Hint:** `heapq.nlargest(3, iterable)` returns the 3 largest items of any
iterable without sorting all of it.

*Think about it:* why is the overall top 3 **always** among the local top 3s?
With 1,000 partitions of 1 million scores each, how many numbers reach the driver?

---

## Q9 (Intermediate+) — Running total across partitions

A **running total** (or prefix sum) replaces each number with the sum of
itself and **all the numbers before it**: `[1, 2, 3, 4]` → `[1, 3, 6, 10]`.
Inside one partition that's easy, but each partition also needs the totals of
all the partitions **before** it.

**Input:**

```python
nums = sc.parallelize(range(1, 11), 3)
```

**Tasks:**

1. **Pass 1:** use `mapPartitionsWithIndex()` to return one `(partition_index, partition_sum)`
   pair per partition. Collect them.
2. On the driver, compute each partition's **offset**: the sum of all the
   partitions before it, as a dict `{index: offset}`.
3. **Pass 2:** use `mapPartitionsWithIndex()` again. Start each partition's
   running total at its offset, and output the running total for every element.
4. Show the result of pass 2 with `glom()` too.

**Expected output:**

```
[(0, 6), (1, 15), (2, 34)]
{0: 0, 1: 6, 2: 21}
[1, 3, 6, 10, 15, 21, 28, 36, 45, 55]
[[1, 3, 6], [10, 15, 21], [28, 36, 45, 55]]
```

*Think about it:* why can't this be done with `map()` alone?
