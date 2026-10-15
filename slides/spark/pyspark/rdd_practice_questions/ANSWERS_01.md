# PySpark RDD Practice Questions — Set 01: Answers

Solutions to the nine [Set 01 practice questions](QUESTIONS_01.md). Every session below
was run in the `bin/pyspark` shell (Spark 4.2.0), started from this
folder. The output is copied from those real runs.

Try the questions yourself first. There are often several correct
solutions, and yours doesn't need to match these exactly.

* [Q1 (Basic): Squares of even numbers](#q1-basic--squares-of-even-numbers)
* [Q2 (Basic): Unique words](#q2-basic--unique-words)
* [Q3 (Basic): Employee raises](#q3-basic--employee-raises)
* [Q4 (Intermediate): Total sales per store](#q4-intermediate--total-sales-per-store)
* [Q5 (Intermediate): Average score per student](#q5-intermediate--average-score-per-student)
* [Q6 (Intermediate): Movie ratings](#q6-intermediate--movie-ratings)
* [Q7 (Intermediate+): Distinct visitors per day](#q7-intermediate--distinct-visitors-per-day)
* [Q8 (Intermediate+): Temperature statistics with dirty data](#q8-intermediate--temperature-statistics-with-dirty-data)
* [Q9 (Intermediate+): Best-selling product per category](#q9-intermediate--best-selling-product-per-category)

---

## Q1 (Basic) — Squares of even numbers

```python
>>> numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
>>> rdd = sc.parallelize(numbers)
>>> rdd.count()
10

>>> squares = rdd.map(lambda x: x * x)
>>> squares.collect()
[1, 4, 9, 16, 25, 36, 49, 64, 81, 100]

>>> even_squares = squares.filter(lambda x: x % 2 == 0)
>>> even_squares.collect()
[4, 16, 36, 64, 100]
>>> even_squares.count()
5
```

**Notes:**

* `map()` produces exactly **one** output element for each input element (10 in, 10 out).
* `filter()` keeps an element only when the function returns `True`, so it
  can make an RDD smaller but never larger.
* `squares` and `even_squares` are new RDDs. RDDs are **immutable**, so
  `rdd` still holds the original 10 numbers.

---

## Q2 (Basic) — Unique words

```python
>>> words = ["spark", "rdd", "Python", "spark", "map", "filter",
...          "RDD", "python", "lambda", "map"]
>>> words_rdd = sc.parallelize(words)
>>> words_rdd.count()
10

>>> lower_rdd = words_rdd.map(lambda w: w.lower())
>>> unique_rdd = lower_rdd.distinct()
>>> sorted(unique_rdd.collect())
['filter', 'lambda', 'map', 'python', 'rdd', 'spark']
>>> unique_rdd.count()
6

>>> long_words = unique_rdd.filter(lambda w: len(w) > 4).map(lambda w: w.upper())
>>> sorted(long_words.collect())
['FILTER', 'LAMBDA', 'PYTHON', 'SPARK']
```

**Notes:**

* `distinct()` compares elements **exactly**, so `"RDD"` and `"rdd"` are
  different. That's why we lowercase first. Skipping the `map()` would
  give 8 unique words instead of 6.
* `distinct()` has to bring equal elements together across partitions
  (a **shuffle**), so its output order isn't fixed. That's why we use `sorted()`.
* Transformations can be chained: `.filter(...).map(...)`.

---

## Q3 (Basic) — Employee raises

```python
>>> employees = [("ann", "eng", 95000), ("raj", "sales", 60000),
...              ("li", "eng", 120000), ("sam", "hr", 55000),
...              ("eva", "sales", 72000), ("tom", "eng", 88000),
...              ("mia", "hr", 61000)]
>>> emp_rdd = sc.parallelize(employees)
>>> emp_rdd.count()
7

>>> eng_names = emp_rdd.filter(lambda e: e[1] == "eng").map(lambda e: e[0])
>>> sorted(eng_names.collect())
['ann', 'li', 'tom']

>>> raised = emp_rdd.map(lambda e: (e[0], round(e[2] * 1.10)))
>>> raised.collect()
[('ann', 104500), ('raj', 66000), ('li', 132000), ('sam', 60500), ('eva', 79200), ('tom', 96800), ('mia', 67100)]

>>> high_earners = raised.filter(lambda ns: ns[1] > 80000)
>>> sorted(high_earners.collect())
[('ann', 104500), ('li', 132000), ('tom', 96800)]
>>> high_earners.count()
3

>>> depts = emp_rdd.map(lambda e: e[1]).distinct()
>>> sorted(depts.collect())
['eng', 'hr', 'sales']
>>> depts.count()
3
```

**Notes:**

* Each element is a tuple, so you pick fields by position: `e[0]` is the
  name, `e[1]` the department, `e[2]` the salary.
* **Filter first, then map.** After `map(lambda e: e[0])` only the names are
  left, so you can no longer filter on the department.
* `round()` with no second argument returns an `int`. Without it,
  `95000 * 1.10` gives `104500.00000000001`, because floats aren't exact.
* `map(...).distinct()` is a common way to ask "what values appear in this
  field?". Here eva did **not** make the list: her new salary, 79200, is
  just under 80000.

---

## Q4 (Intermediate) — Total sales per store

```python
>>> lines = sc.textFile("data/sales.txt")
>>> lines.count()
10

>>> records = lines.map(lambda line: line.split(","))
>>> pairs = records.map(lambda r: (r[0], float(r[2])))
>>> pairs.take(3)
[('S1', 3.5), ('S2', 1.25), ('S1', 4.0)]

>>> totals = pairs.reduceByKey(lambda a, b: a + b)
>>> totals_rounded = totals.mapValues(lambda t: round(t, 2))
>>> sorted(totals_rounded.collect())
[('S1', 13.75), ('S2', 9.85), ('S3', 8.0)]

>>> product_counts = records.map(lambda r: (r[1], 1)).reduceByKey(lambda a, b: a + b)
>>> sorted(product_counts.collect())
[('apple', 3), ('banana', 2), ('bread', 2), ('eggs', 1), ('milk', 2)]
```

**Notes:**

* `sc.textFile()` gives an RDD of **strings**, one per line. You must
  `split()` each line yourself and convert numbers with `float()`/`int()`.
  Forgetting the `float()` means `reduceByKey(lambda a, b: a + b)`
  **joins strings** (`'3.504.00...'`) instead of adding numbers.
* `reduceByKey()` works on `(key, value)` pairs. The function gets two
  **values** with the same key (never the key itself) and must return one
  value of the same type.
* `mapValues(f)` is the same as `map(lambda kv: (kv[0], f(kv[1])))`, but
  shorter, and it tells Spark that the keys didn't change.
* `(product, 1)` + `reduceByKey(add)` is the standard **counting** pattern,
  the same idea as word count.

---

## Q5 (Intermediate) — Average score per student

```python
>>> scores = [("alice", 90), ("bob", 72), ("alice", 85), ("carol", 60),
...           ("bob", 88), ("carol", 75), ("alice", 95), ("bob", 80)]
>>> scores_rdd = sc.parallelize(scores)

>>> sum_count = scores_rdd.mapValues(lambda s: (s, 1))
>>> sum_count.collect()
[('alice', (90, 1)), ('bob', (72, 1)), ('alice', (85, 1)), ('carol', (60, 1)), ('bob', (88, 1)), ('carol', (75, 1)), ('alice', (95, 1)), ('bob', (80, 1))]

>>> totals = sum_count.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
>>> sorted(totals.collect())
[('alice', (270, 3)), ('bob', (240, 3)), ('carol', (135, 2))]

>>> averages = totals.mapValues(lambda t: round(t[0] / t[1], 2))
>>> sorted(averages.collect())
[('alice', 90.0), ('bob', 80.0), ('carol', 67.5)]

>>> honor_roll = averages.filter(lambda kv: kv[1] >= 80)
>>> sorted(honor_roll.collect())
[('alice', 90.0), ('bob', 80.0)]
```

**Notes:**

* Why not `reduceByKey(lambda a, b: (a + b) / 2)`? Because averaging is
  **not associative**. For alice, Spark could compute `((90+85)/2 + 95)/2 = 91.25`,
  which is wrong (the correct value is 90.0), and the result could change
  depending on how the data is split into partitions. `reduceByKey()` needs
  a function where grouping and order don't matter.
* The fix is to carry `(sum, count)` pairs. Adding pairs **is** associative,
  so it's safe in `reduceByKey()`. Divide only once, at the end, with `mapValues()`.
* In `filter()` on a pair RDD, the function gets the whole `(key, value)`
  tuple, so the average is `kv[1]`.

---

## Q6 (Intermediate) — Movie ratings

```python
>>> ratings = sc.textFile("data/movie_ratings.txt").map(lambda line: line.split(","))
>>> ratings.count()
12

>>> movies = ratings.map(lambda r: r[1]).distinct()
>>> sorted(movies.collect())
['Coco', 'Inception', 'Titanic', 'Up']
>>> movies.count()
4
>>> ratings.map(lambda r: r[0]).distinct().count()
4

>>> movie_rating = ratings.map(lambda r: (r[1], int(r[2])))
>>> num_ratings = movie_rating.mapValues(lambda x: 1).reduceByKey(lambda a, b: a + b)
>>> sorted(num_ratings.collect())
[('Coco', 2), ('Inception', 4), ('Titanic', 3), ('Up', 3)]

>>> best = movie_rating.reduceByKey(lambda a, b: max(a, b))
>>> sorted(best.collect())
[('Coco', 5), ('Inception', 5), ('Titanic', 4), ('Up', 5)]
>>> worst = movie_rating.reduceByKey(min)
>>> sorted(worst.collect())
[('Coco', 3), ('Inception', 3), ('Titanic', 1), ('Up', 4)]

>>> popular = num_ratings.filter(lambda kv: kv[1] >= 3).map(lambda kv: kv[0])
>>> sorted(popular.collect())
['Inception', 'Titanic', 'Up']
```

**Notes:**

* `sc.textFile(...).map(...)` reads and splits in one line. `ratings` is
  then reused for several different questions.
* `mapValues(lambda x: 1)` swaps every rating for a `1` but keeps the movie
  as the key. Then `reduceByKey()` adds the 1s up, which is the counting pattern again.
* `max` and `min` are both **associative and commutative**, just like `+`,
  so they work in `reduceByKey()`. You can pass the built-in function itself
  (`reduceByKey(min)`) instead of writing a `lambda`.
* `int(r[2])` matters for `max`/`min` too. On strings, `max` compares
  **alphabetically**: `max("5", "10")` is `"5"`.

---

## Q7 (Intermediate+) — Distinct visitors per day

```python
>>> logs = sc.textFile("data/web_logs.txt")
>>> logs.count()
14

>>> fields = logs.map(lambda line: line.split(","))
>>> ok = fields.filter(lambda f: f[3] == "200")
>>> ok.count()
11

>>> date_user = ok.map(lambda f: (f[0], f[1]))
>>> unique_date_user = date_user.distinct()
>>> sorted(unique_date_user.collect())
[('2026-10-01', 'alex'), ('2026-10-01', 'bob'), ('2026-10-02', 'alex'), ('2026-10-02', 'bob'), ('2026-10-02', 'dana'), ('2026-10-02', 'erin'), ('2026-10-03', 'carol')]

>>> visitors_per_day = unique_date_user.map(lambda du: (du[0], 1)).reduceByKey(lambda a, b: a + b)
>>> sorted(visitors_per_day.collect())
[('2026-10-01', 2), ('2026-10-02', 4), ('2026-10-03', 1)]

>>> busy_days = visitors_per_day.filter(lambda kv: kv[1] >= 3)
>>> sorted(busy_days.collect())
[('2026-10-02', 4)]
```

**Notes:**

* The status is the **string** `"200"`, not the number `200`, because
  `split()` returns strings. `f[3] == 200` would be `False` for every line.
* `distinct()` on `(date, user)` pairs leaves each user **once per day**.
  This is the key step. Without it you count **page views** instead of visitors:

  ```python
  >>> wrong = date_user.map(lambda du: (du[0], 1)).reduceByKey(lambda a, b: a + b)
  >>> sorted(wrong.collect())
  [('2026-10-01', 4), ('2026-10-02', 5), ('2026-10-03', 2)]
  ```

  alex and bob each visited twice on 10-01, erin twice on 10-02, and carol
  twice on 10-03.
* Filter **before** `distinct()` and `reduceByKey()`: less data goes into
  the shuffles. carol's `404` on 10-01 is dropped, so she doesn't count
  as a visitor that day.
* For a fuller version of this problem (with a DataFrame solution next to
  it), see [`rdd_examples/distinct_visitor_per_day/`](../rdd_examples/distinct_visitor_per_day/).

---

## Q8 (Intermediate+) — Temperature statistics with dirty data

```python
>>> raw = sc.textFile("data/temperatures.txt")
>>> raw.count()
14

>>> fields = raw.map(lambda line: line.split(","))
>>> def is_valid(f):
...     return len(f) == 3 and f[2].lstrip("-").isdigit()
...
>>> valid = fields.filter(is_valid)
>>> valid.count()
10

>>> city_temp = valid.map(lambda f: (f[0], int(f[2])))
>>> stats0 = city_temp.mapValues(lambda t: (t, t, t, 1))
>>> stats = stats0.reduceByKey(lambda a, b: (min(a[0], b[0]), max(a[1], b[1]), a[2] + b[2], a[3] + b[3]))
>>> sorted(stats.collect())
[('Boston', (9, 15, 36, 3)), ('Phoenix', (35, 38, 73, 2)), ('San Jose', (24, 27, 51, 2)), ('Seattle', (11, 16, 41, 3))]

>>> summary = stats.mapValues(lambda s: (s[0], s[1], round(s[2] / s[3], 2)))
>>> sorted(summary.collect())
[('Boston', (9, 15, 12.0)), ('Phoenix', (35, 38, 36.5)), ('San Jose', (24, 27, 25.5)), ('Seattle', (11, 16, 13.67))]

>>> big_swing = summary.filter(lambda kv: kv[1][1] - kv[1][0] >= 5)
>>> sorted(big_swing.collect())
[('Boston', (9, 15, 12.0)), ('Seattle', (11, 16, 13.67))]
```

**Bonus — the 4 lines that were dropped:**

```python
>>> fields.filter(lambda f: not is_valid(f)).collect()
[['city', 'date', 'temp_c'], ['San Jose', '2026-10-03', 'NA'], [''], ['Boston', '2026-10-02']]
```

**Notes:**

* **Clean first, compute second.** If you call `int(f[2])` without the
  filter, the job fails on the first bad line (an `IndexError` for
  `['']`, a `ValueError` for `'NA'` or `'temp_c'`). The error only shows up
  when you call an **action**, because transformations are lazy.
* A `filter()` function can be a named `def` instead of a `lambda`. In the
  interactive shell, **press Enter on the empty `...` line** to end the
  `def` before you type the next line, or you get a `SyntaxError`.
* `f[2].lstrip("-").isdigit()` accepts whole numbers such as `24` or `-3`.
  It rejects `NA`, `temp_c`, and the empty string.
* One `reduceByKey()` can compute several statistics at once by carrying a
  tuple. Each part, `min`, `max`, and `+`, is associative, so the whole
  tuple combine is too. This is the same idea as the `(sum, count)` pair in Q5.
* In `big_swing`, `kv[1]` is the `(min, max, avg)` tuple, so `kv[1][1] - kv[1][0]`
  is max − min.


---

## Q9 (Intermediate+) — Best-selling product per category

```python
>>> purchases = sc.textFile("data/purchases.txt").map(lambda line: line.split(","))
>>> purchases.count()
15

>>> pair_counts = purchases.map(lambda p: ((p[1], p[2]), 1)).reduceByKey(lambda a, b: a + b)
>>> sorted(pair_counts.collect())
[(('bakery', 'bagel'), 2), (('bakery', 'bread'), 2), (('dairy', 'cheese'), 1), (('dairy', 'milk'), 2), (('dairy', 'yogurt'), 1), (('fruit', 'apple'), 4), (('fruit', 'banana'), 2), (('fruit', 'cherry'), 1)]

>>> by_category = pair_counts.map(lambda kv: (kv[0][0], (kv[0][1], kv[1])))
>>> sorted(by_category.collect())
[('bakery', ('bagel', 2)), ('bakery', ('bread', 2)), ('dairy', ('cheese', 1)), ('dairy', ('milk', 2)), ('dairy', ('yogurt', 1)), ('fruit', ('apple', 4)), ('fruit', ('banana', 2)), ('fruit', ('cherry', 1))]

>>> def better(a, b):
...     if a[1] != b[1]:
...         return a if a[1] > b[1] else b
...     return a if a[0] < b[0] else b
...
>>> best_seller = by_category.reduceByKey(better)
>>> sorted(best_seller.collect())
[('bakery', ('bagel', 2)), ('dairy', ('milk', 2)), ('fruit', ('apple', 4))]

>>> customers = (purchases.map(lambda p: (p[1], p[0]))
...                       .distinct()
...                       .mapValues(lambda c: 1)
...                       .reduceByKey(lambda a, b: a + b))
>>> sorted(customers.collect())
[('bakery', 4), ('dairy', 4), ('fruit', 5)]
```

**Notes:**

* A key can be a **tuple**. `((category, product), 1)` counts each
  combination separately, just like a `GROUP BY category, product` in SQL.
* The `map()` between the two `reduceByKey()` calls is the important step.
  It **changes the key** from `(category, product)` to just `category`, so
  the second `reduceByKey()` compares the products *within* each category.
  (This is a case where `mapValues()` can't help, because the key changes.)
* `better(a, b)` must give the same winner whatever order Spark passes the
  two values in. If it simply kept the larger count, the bakery tie
  (`bagel` 2, `bread` 2) could come out either way from run to run. Breaking
  ties alphabetically makes the answer **deterministic**.
* Task 3 is the same "distinct, then count" pattern as Q7. Without
  `distinct()` you'd count purchases (`fruit` = 7) instead of customers
  (`fruit` = 5): c1 bought apple twice and also bought banana.
