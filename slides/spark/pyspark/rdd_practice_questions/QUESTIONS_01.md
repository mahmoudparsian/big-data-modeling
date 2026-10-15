# PySpark RDD Practice Questions — Set 01: Basic Transformations

Nine practice questions on PySpark RDDs, three at each level, meant to be solved
**interactively** in the `bin/pyspark` shell. They start easy and
get harder:

| # | Level | Topic | Input |
|---|---|---|---|
| [Q1](#q1-basic--squares-of-even-numbers) | Basic | Squares of even numbers | Python list |
| [Q2](#q2-basic--unique-words) | Basic | Unique words | Python list |
| [Q3](#q3-basic--employee-raises) | Basic | Employee raises | Python list |
| [Q4](#q4-intermediate--total-sales-per-store) | Intermediate | Total sales per store | [`data/sales.txt`](data/sales.txt) |
| [Q5](#q5-intermediate--average-score-per-student) | Intermediate | Average score per student | Python list |
| [Q6](#q6-intermediate--movie-ratings) | Intermediate | Movie ratings | [`data/movie_ratings.txt`](data/movie_ratings.txt) |
| [Q7](#q7-intermediate--distinct-visitors-per-day) | Intermediate+ | Distinct visitors per day | [`data/web_logs.txt`](data/web_logs.txt) |
| [Q8](#q8-intermediate--temperature-statistics-with-dirty-data) | Intermediate+ | Temperature statistics with dirty data | [`data/temperatures.txt`](data/temperatures.txt) |
| [Q9](#q9-intermediate--best-selling-product-per-category) | Intermediate+ | Best-selling product per category | [`data/purchases.txt`](data/purchases.txt) |

Use **only** these RDD operations:

* **Creating RDDs:** `sc.parallelize(...)` and `sc.textFile(...)`
* **Transformations:** `map()`, `mapValues()`, `filter()`, `distinct()`, `reduceByKey()`
* **Actions:** `collect()`, `count()`

(`take(n)` is fine for peeking at an RDD while you work.)

Try each question on your own before you open the
[answers](ANSWERS_01.md).

## Files

| File | Purpose |
|---|---|
| [QUESTIONS_01.md](QUESTIONS_01.md) | The nine questions (this page) |
| [ANSWERS_01.md](ANSWERS_01.md) | Step-by-step solutions, with real output from Spark 4.2.0 |
| [data/sales.txt](data/sales.txt) | Input for Q4: `store,product,price` |
| [data/movie_ratings.txt](data/movie_ratings.txt) | Input for Q6: `user,movie,rating` |
| [data/web_logs.txt](data/web_logs.txt) | Input for Q7: `date,user,url,http_status` |
| [data/temperatures.txt](data/temperatures.txt) | Input for Q8: `city,date,temp_c` (has a header and some bad records on purpose) |
| [data/purchases.txt](data/purchases.txt) | Input for Q9: `customer,category,product` |

## Getting started

Open a terminal **in this folder** (so the relative paths `data/...`
work), then start the shell:

```bash
$SPARK_HOME/bin/pyspark
```

The shell already gives you `sc` (the `SparkContext`) and `spark`
(the `SparkSession`), so you don't need to create them:

```python
>>> sc.version
'4.2.0'
>>> rdd = sc.parallelize([1, 2, 3])
>>> rdd.collect()
[1, 2, 3]
```

Tips for the interactive shell:

* After an operation like `reduceByKey()` or `distinct()`, the order of
  `collect()` results can change from run to run. Wrap it in Python's
  `sorted(...)` so your output is easy to compare with the expected answer.
* Transformations are **lazy**: nothing runs until you call an action
  (`collect()`, `count()`). Calling an action after each step is a good way
  to check your work.
* If you type a multi-line `def` in the shell, **press Enter on an empty
  line** to end it before you type the next statement.
* Type `exit()` (or press Ctrl-D) to leave the shell.

---

## Q1 (Basic) — Squares of even numbers

**Input** (a Python list):

```python
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
```

**Tasks:**

1. Create an RDD from `numbers` and count its elements.
2. Use `map()` to make a new RDD with the square of each number. Collect it.
3. Use `filter()` to keep only the **even** squares. Collect them and count them.

**Expected output:**

```
10
[1, 4, 9, 16, 25, 36, 49, 64, 81, 100]
[4, 16, 36, 64, 100]
5
```

---

## Q2 (Basic) — Unique words

**Input** (a Python list; note the mixed upper/lower case):

```python
words = ["spark", "rdd", "Python", "spark", "map", "filter",
         "RDD", "python", "lambda", "map"]
```

**Tasks:**

1. Create an RDD from `words` and count it.
2. Change every word to lowercase, then find the **unique** words. Print them
   sorted, and count them. (`"RDD"` and `"rdd"` should count as the same word.)
3. From the unique words, keep only the ones **longer than 4 letters** and
   change them to UPPERCASE. Print them sorted.

**Expected output:**

```
10
['filter', 'lambda', 'map', 'python', 'rdd', 'spark']
6
['FILTER', 'LAMBDA', 'PYTHON', 'SPARK']
```

*Think about it:* what would `distinct().count()` give if you skipped the
lowercase step?

---

## Q3 (Basic) — Employee raises

**Input** (a Python list of `(name, department, salary)` tuples):

```python
employees = [("ann", "eng", 95000), ("raj", "sales", 60000),
             ("li", "eng", 120000), ("sam", "hr", 55000),
             ("eva", "sales", 72000), ("tom", "eng", 88000),
             ("mia", "hr", 61000)]
```

**Tasks:**

1. Create an RDD from `employees` and count it.
2. Find the **names** of everyone in the `"eng"` department. Print them sorted.
3. Everyone gets a **10% raise**. Make an RDD of `(name, new_salary)` pairs,
   with the new salary rounded to a whole number. Collect it.
4. Who earns **more than 80000** after the raise? Print them sorted, and count them.
5. Find the **unique departments**. Print them sorted, and count them.

**Expected output:**

```
7
['ann', 'li', 'tom']
[('ann', 104500), ('raj', 66000), ('li', 132000), ('sam', 60500), ('eva', 79200), ('tom', 96800), ('mia', 67100)]
[('ann', 104500), ('li', 132000), ('tom', 96800)]
3
['eng', 'hr', 'sales']
3
```

---

## Q4 (Intermediate) — Total sales per store

**Input:** [`data/sales.txt`](data/sales.txt). Each line is one item sold,
as `store,product,price`:

```
S1,apple,3.50
S2,banana,1.25
S1,milk,4.00
...
```

**Tasks:**

1. Read the file with `sc.textFile()` and count the lines.
2. Make a `(store, price)` pair RDD. The price must be a `float`.
3. Use `reduceByKey()` to find the **total sales per store**. Then use
   `mapValues()` to round each total to 2 decimals.
4. Find **how many times each product was sold**, as `(product, count)` pairs.

**Expected output:**

```
10
[('S1', 13.75), ('S2', 9.85), ('S3', 8.0)]
[('apple', 3), ('banana', 2), ('bread', 2), ('eggs', 1), ('milk', 2)]
```

---

## Q5 (Intermediate) — Average score per student

**Input** (a Python list of `(student, score)` pairs):

```python
scores = [("alice", 90), ("bob", 72), ("alice", 85), ("carol", 60),
          ("bob", 88), ("carol", 75), ("alice", 95), ("bob", 80)]
```

**Tasks:**

1. Find the **average score of each student**, rounded to 2 decimals.
2. Find the "honor roll": the students whose average is **80 or more**.

**Hint:** you can't average with a single `reduceByKey()`, because an
average of averages is not the right answer. Use `mapValues()` to turn
each score into a `(sum, count)` pair first, add the pairs together with
`reduceByKey()`, and only then divide.

**Expected output:**

```
[('alice', 90.0), ('bob', 80.0), ('carol', 67.5)]
[('alice', 90.0), ('bob', 80.0)]
```

---

## Q6 (Intermediate) — Movie ratings

**Input:** [`data/movie_ratings.txt`](data/movie_ratings.txt). Each line is
one rating from 1 to 5, as `user,movie,rating`:

```
u1,Inception,5
u2,Inception,4
u3,Inception,3
u1,Titanic,2
...
```

**Tasks:**

1. Count the ratings. Then find the **unique movies** (print them sorted and
   count them) and count the **unique users**.
2. Find the **number of ratings for each movie**.
3. Find the **highest** and the **lowest** rating of each movie.
4. Find the "popular" movies: those with **3 or more** ratings. Print only
   the movie names, sorted.

**Expected output:**

```
12
['Coco', 'Inception', 'Titanic', 'Up']
4
4
[('Coco', 2), ('Inception', 4), ('Titanic', 3), ('Up', 3)]
[('Coco', 5), ('Inception', 5), ('Titanic', 4), ('Up', 5)]
[('Coco', 3), ('Inception', 3), ('Titanic', 1), ('Up', 4)]
['Inception', 'Titanic', 'Up']
```

*Think about it:* `reduceByKey()` isn't only for adding. Which
built-in Python functions take two numbers and return one?

---

## Q7 (Intermediate+) — Distinct visitors per day

**Input:** [`data/web_logs.txt`](data/web_logs.txt). Each line is one page
request, as `date,user,url,http_status`:

```
2026-10-01,alex,/home,200
2026-10-01,bob,/home,200
2026-10-01,alex,/cart,200
2026-10-01,carol,/login,404
...
```

**Tasks:**

1. Count all requests, and count the **successful** requests (status `200`).
2. Using only successful requests, find the number of **distinct users per
   day**. A user who views 3 pages on the same day counts **once** for that day.
3. Find the "busy" days: days with **3 or more** distinct visitors.

**Expected output:**

```
14
11
[('2026-10-01', 2), ('2026-10-02', 4), ('2026-10-03', 1)]
[('2026-10-02', 4)]
```

*Think about it:* if you count `(date, 1)` pairs **without** removing the
duplicates first, what wrong answer do you get, and why?

---

## Q8 (Intermediate+) — Temperature statistics with dirty data

**Input:** [`data/temperatures.txt`](data/temperatures.txt), with lines
of the form `city,date,temp_c`. Like real data, this file is **dirty**:
it has a header line, an empty line, a missing value (`NA`), and a record
with a field missing.

```
city,date,temp_c
San Jose,2026-10-01,24
San Jose,2026-10-02,27
San Jose,2026-10-03,NA
...
```

**Tasks:**

1. Count all lines. Then use `filter()` to keep only **valid** records:
   exactly 3 fields, and a temperature that is a whole number. Count the
   valid records.
2. For each city, find the **minimum**, **maximum**, and **average**
   temperature (rounded to 2 decimals), as `(city, (min, max, avg))`.
   Try to do it with **one** `reduceByKey()`.
3. Find the cities where the temperature **varied by 5 degrees or more**
   (max − min ≥ 5).

**Hint:** use `mapValues()` to turn each temperature `t` into a tuple
`(t, t, t, 1)`, meaning *(min so far, max so far, sum, count)*, and then
combine two such tuples in `reduceByKey()`.

**Expected output:**

```
14
10
[('Boston', (9, 15, 12.0)), ('Phoenix', (35, 38, 36.5)), ('San Jose', (24, 27, 25.5)), ('Seattle', (11, 16, 13.67))]
[('Boston', (9, 15, 12.0)), ('Seattle', (11, 16, 13.67))]
```

*Bonus:* collect the **invalid** lines to see which 4 lines were dropped.


---

## Q9 (Intermediate+) — Best-selling product per category

**Input:** [`data/purchases.txt`](data/purchases.txt). Each line is one
item bought, as `customer,category,product`:

```
c1,fruit,apple
c2,fruit,apple
c3,fruit,banana
...
```

**Tasks:**

1. Count the lines. Then count how many times each **(category, product)**
   pair was bought, as `((category, product), count)`.
2. Find the **best-selling product in each category**, as
   `(category, (product, count))`. If two products tie, pick the one that
   comes **first alphabetically**.
3. Find the number of **distinct customers per category**.

**Hint for task 2:** this takes two `reduceByKey()` steps. The first one counts
by `(category, product)`. Then use `map()` to move the product out of
the key and into the value, `(category, (product, count))`, and use a second
`reduceByKey()` that keeps the "better" of two `(product, count)` values.
A short `def` is easier to read than a long `lambda` here.

**Expected output:**

```
15
[(('bakery', 'bagel'), 2), (('bakery', 'bread'), 2), (('dairy', 'cheese'), 1), (('dairy', 'milk'), 2), (('dairy', 'yogurt'), 1), (('fruit', 'apple'), 4), (('fruit', 'banana'), 2), (('fruit', 'cherry'), 1)]
[('bakery', ('bagel', 2)), ('dairy', ('milk', 2)), ('fruit', ('apple', 4))]
[('bakery', 4), ('dairy', 4), ('fruit', 5)]
```

*Think about it:* the `fruit` category has 7 purchases but only 5 distinct
customers. Why?
