# PySpark RDD Practice Questions — Set 05: Answers

Solutions to the nine [Set 05 practice questions](QUESTIONS_05.md). Every
session below was run in the `bin/pyspark` shell (Spark 4.2.0), started from
this folder. The output is copied from those real runs.

Try the questions yourself first. There are often several correct
solutions, and yours doesn't need to match these exactly.

* [Q1 (Basic): Employees and departments: join()](#q1-basic--employees-and-departments-join)
* [Q2 (Basic): Who's missing? leftOuterJoin()](#q2-basic--whos-missing-leftouterjoin)
* [Q3 (Basic): Everything side by side: cogroup()](#q3-basic--everything-side-by-side-cogroup)
* [Q4 (Intermediate): Orders per customer and per city](#q4-intermediate--orders-per-customer-and-per-city)
* [Q5 (Intermediate): Data quality checks with outer joins](#q5-intermediate--data-quality-checks-with-outer-joins)
* [Q6 (Intermediate): Predict the size of a join](#q6-intermediate--predict-the-size-of-a-join)
* [Q7 (Intermediate+): Implement the joins yourself with cogroup()](#q7-intermediate--implement-the-joins-yourself-with-cogroup)
* [Q8 (Intermediate+): Revenue per city and category: a 3-way join](#q8-intermediate--revenue-per-city-and-category-a-3-way-join)
* [Q9 (Intermediate+): Customers who buy the same things: a self-join](#q9-intermediate--customers-who-buy-the-same-things-a-self-join)

---

## Q1 (Basic) — Employees and departments: join()

```python
>>> emp = sc.parallelize([("d1", "ann"), ("d2", "raj"), ("d1", "li"),
...                       ("d3", "sam"), ("d4", "eva")], 2)
>>> depts = sc.parallelize([("d1", "Engineering"), ("d2", "Sales"),
...                         ("d3", "HR"), ("d5", "Legal")], 2)

>>> joined = emp.join(depts)
>>> sorted(joined.collect())
[('d1', ('ann', 'Engineering')), ('d1', ('li', 'Engineering')), ('d2', ('raj', 'Sales')), ('d3', ('sam', 'HR'))]
>>> joined.count()
4

>>> sorted(joined.map(lambda kv: (kv[1][0], kv[1][1])).collect())
[('ann', 'Engineering'), ('li', 'Engineering'), ('raj', 'Sales'), ('sam', 'HR')]

>>> sorted(depts.join(emp).collect())
[('d1', ('Engineering', 'ann')), ('d1', ('Engineering', 'li')), ('d2', ('Sales', 'raj')), ('d3', ('HR', 'sam'))]
```

**Notes:**

* `join()` is an **inner join**: it keeps only keys found on **both** sides. `eva`
  (`d4`, no such department) and `Legal` (`d5`, no employees) are silently dropped.
* `d1` appears twice in `emp` and once in `depts`, so it gives `2 × 1 = 2` rows.
  The department name is **repeated** for each employee.
* Swapping the two sides gives the same rows, with the two values swapped inside the
  tuple: `(k, (v, w))` always has the **left** RDD's value first.
* `kv[1]` is the `(employee, dept_name)` pair, so `kv[1][0]` is the employee and
  `kv[1][1]` the department name.

---

## Q2 (Basic) — Who's missing? leftOuterJoin()

```python
>>> left = emp.leftOuterJoin(depts)
>>> sorted(left.collect())
[('d1', ('ann', 'Engineering')), ('d1', ('li', 'Engineering')), ('d2', ('raj', 'Sales')), ('d3', ('sam', 'HR')), ('d4', ('eva', None))]

>>> left.filter(lambda kv: kv[1][1] is None).map(lambda kv: kv[1][0]).collect()
['eva']

>>> (depts.leftOuterJoin(emp)
...       .filter(lambda kv: kv[1][1] is None)
...       .map(lambda kv: kv[1][0])
...       .collect())
['Legal']
```

**Notes:**

* `leftOuterJoin()` keeps **every row of the left RDD**. When the right side has
  no match, you get `None` in its place, as for `eva`.
* "Left outer join, then keep the `None` rows" is the standard way to find
  **missing matches**. In SQL it's called an **anti-join**.
* `a.rightOuterJoin(b)` is the same as `b.leftOuterJoin(a)`, with the two values
  swapped inside each tuple. So `leftOuterJoin()` is all you need.
* Use `is None`, not `== None`, to test for `None`.

---

## Q3 (Basic) — Everything side by side: cogroup()

```python
>>> grouped = emp.cogroup(depts)
>>> grouped.count()
5

>>> grouped.mapValues(lambda v: (sorted(v[0]), sorted(v[1]))).sortByKey().collect()
[('d1', (['ann', 'li'], ['Engineering'])), ('d2', (['raj'], ['Sales'])), ('d3', (['sam'], ['HR'])), ('d4', (['eva'], [])), ('d5', ([], ['Legal']))]

>>> grouped.mapValues(lambda v: (len(v[0]), len(v[1]))).sortByKey().collect()
[('d1', (2, 1)), ('d2', (1, 1)), ('d3', (1, 1)), ('d4', (1, 0)), ('d5', (0, 1))]
```

**Notes:**

* `cogroup()` gives **one element per key**, for every key that appears on
  **either** side. The value is a pair of iterables: all the left values and all
  the right values. Like `groupByKey()`, they are `ResultIterable`s, so use
  `sorted()` or `list()` to see them.
* `join()` gave 4 rows: `d1` produced 2 rows (one per employee), and `d4` and `d5`
  produced none. `cogroup()` gives 5 elements: one for each of `d1`…`d5`, with
  `d1`'s two employees in one list, and an **empty list** where a side is missing.
* `cogroup()` is the most general of the three. Q7 shows that `join()` and
  `leftOuterJoin()` can both be built from it.

---

## Q4 (Intermediate) — Orders per customer and per city

```python
>>> customers = (sc.textFile("data/shop_customers.txt", 2)
...                .map(lambda line: line.split(","))
...                .map(lambda f: (f[0], (f[1], f[2]))))
>>> orders = sc.textFile("data/shop_orders.txt", 2).map(lambda line: line.split(","))
>>> customers.count()
5
>>> orders.count()
10

>>> by_customer = orders.map(lambda f: (f[1], f[0]))
>>> order_rows = by_customer.join(customers)
>>> order_rows.count()
9
>>> sorted(order_rows.collect())[:3]
[('c1', ('o1', ('Ann', 'San Jose'))), ('c1', ('o3', ('Ann', 'San Jose'))), ('c1', ('o7', ('Ann', 'San Jose')))]

>>> (order_rows.map(lambda kv: (kv[1][1][1], 1))
...            .reduceByKey(lambda a, b: a + b)
...            .sortByKey()
...            .collect())
[('Austin', 3), ('San Jose', 6)]

>>> order_counts = by_customer.mapValues(lambda o: 1).reduceByKey(lambda a, b: a + b)
>>> order_counts.sortByKey().collect()
[('c1', 3), ('c2', 3), ('c3', 3), ('c6', 1)]
>>> (order_counts.join(customers)
...              .map(lambda kv: (kv[1][1][0], kv[1][0]))
...              .sortByKey()
...              .collect())
[('Ann', 3), ('Bob', 3), ('Cara', 3)]
>>> (order_counts.join(customers)
...              .map(lambda kv: (kv[1][1][1], kv[1][0]))
...              .reduceByKey(lambda a, b: a + b)
...              .sortByKey()
...              .collect())
[('Austin', 3), ('San Jose', 6)]
```

**Notes:**

* A joined row is `(customer_id, (order_id, (name, city)))`, so the city is
  `kv[1][1][1]`: the right value (`kv[1][1]`), and in it the second field.
* Only **9** of the 10 orders joined: order `o8` belongs to customer `c6`, who is
  **not** in the customer file, so the inner join dropped it without any warning.
  Q5 finds it.
* **Reduce before you join.** In task 2, every order is shuffled for the join. In
  task 4, the orders are first reduced to **one count per customer**, and only
  those counts are joined. With millions of orders but thousands of customers,
  that's a much smaller join, for the same answer.
* The customer's name, a **right-side** value, appears once per joined row. With
  task 4's plan, each customer joins just once.

---

## Q5 (Intermediate) — Data quality checks with outer joins

```python
>>> with_counts = customers.leftOuterJoin(order_counts)
>>> with_counts.sortByKey().collect()
[('c1', (('Ann', 'San Jose'), 3)), ('c2', (('Bob', 'Austin'), 3)), ('c3', (('Cara', 'San Jose'), 3)), ('c4', (('Dev', 'Boston'), None)), ('c5', (('Eli', 'Austin'), None))]

>>> sorted(with_counts.filter(lambda kv: kv[1][1] is None).map(lambda kv: kv[1][0][0]).collect())
['Dev', 'Eli']

>>> with_counts.mapValues(lambda v: (v[0][0], v[1] or 0)).sortByKey().collect()
[('c1', ('Ann', 3)), ('c2', ('Bob', 3)), ('c3', ('Cara', 3)), ('c4', ('Dev', 0)), ('c5', ('Eli', 0))]

>>> by_customer.leftOuterJoin(customers).filter(lambda kv: kv[1][1] is None).collect()
[('c6', ('o8', None))]
```

**Notes:**

* `customers.leftOuterJoin(order_counts)` keeps **all 5 customers**. Dev and Eli
  get `None` because they have no orders, and `c6` doesn't appear because it isn't
  a customer.
* `v[1] or 0` turns `None` into `0`. Careful: `x or default` replaces **any**
  "false-looking" value (`None`, `0`, `""`, `[]`) with the default. That's fine
  here, but when a real value could be `0` or `""`, write
  `default if x is None else x` instead.
* Task 4 is the **opposite** check: start from the orders, and look for orders
  whose customer is missing. Order `o8` points to a customer that doesn't exist,
  a classic "orphan record". In a real system, this is a bug to report, not just
  something to drop silently.
* Running these checks **before** your inner joins tells you how much data the joins
  will throw away.

---

## Q6 (Intermediate) — Predict the size of a join

```python
>>> left = sc.parallelize([("k1", "a1"), ("k1", "a2"), ("k2", "b1"),
...                        ("k3", "c1"), ("k3", "c2"), ("k3", "c3")], 2)
>>> right = sc.parallelize([("k1", "x1"), ("k1", "x2"), ("k1", "x3"), ("k2", "y1"),
...                         ("k3", "z1"), ("k3", "z2"), ("k4", "w1")], 2)

>>> left_counts = left.mapValues(lambda v: 1).reduceByKey(lambda a, b: a + b)
>>> right_counts = right.mapValues(lambda v: 1).reduceByKey(lambda a, b: a + b)
>>> sizes = left_counts.join(right_counts).mapValues(lambda mn: mn[0] * mn[1]).sortByKey()
>>> sizes.collect()
[('k1', 6), ('k2', 1), ('k3', 6)]
>>> sum(n for key, n in sizes.collect())
13

>>> joined = left.join(right)
>>> joined.count()
13
>>> sorted(joined.filter(lambda kv: kv[0] == "k3").collect())
[('k3', ('c1', 'z1')), ('k3', ('c1', 'z2')), ('k3', ('c2', 'z1')), ('k3', ('c2', 'z2')), ('k3', ('c3', 'z1')), ('k3', ('c3', 'z2'))]

>>> left.count() * right.count()
42
```

**Notes:**

* **The size of a join is the sum over keys of `m × n`**, where `m` and `n` are
  the number of values for that key on each side. `k1`: `2 × 3 = 6`, `k2`: `1 × 1 = 1`,
  `k3`: `3 × 2 = 6`, and `k4` is only on the right, so 0. Total **13**.
* Counting per key first is cheap: the counts are small. Then joining the **counts**
  tells you the join size **without** creating the rows. This is a good check
  before a big join.
* A join is **not** "everything with everything" (that would be 42 rows here), but
  a key that is very common on both sides behaves like it. A key with 10,000 values
  on each side gives **100 million** rows for that key alone, and all of them are
  produced by **one** task. This is called **data skew**, and it's a common reason
  why a join suddenly runs out of memory or takes hours.
* When both sides have unique keys (like a customer table), each key gives at most
  `1 × 1 = 1` row, and the join can't grow.

---

## Q7 (Intermediate+) — Implement the joins yourself with cogroup()

```python
>>> def my_join(a, b):
...     return a.cogroup(b).flatMap(
...         lambda kv: [(kv[0], (v, w)) for v in kv[1][0] for w in kv[1][1]])
...
>>> sorted(my_join(emp, depts).collect()) == sorted(emp.join(depts).collect())
True

>>> def my_left_outer_join(a, b):
...     return a.cogroup(b).flatMap(
...         lambda kv: [(kv[0], (v, w))
...                     for v in kv[1][0]
...                     for w in (list(kv[1][1]) or [None])])
...
>>> sorted(my_left_outer_join(emp, depts).collect()) == sorted(emp.leftOuterJoin(depts).collect())
True

>>> def my_full_outer_join(a, b):
...     return a.cogroup(b).flatMap(
...         lambda kv: [(kv[0], (v, w))
...                     for v in (list(kv[1][0]) or [None])
...                     for w in (list(kv[1][1]) or [None])])
...
>>> sorted(my_full_outer_join(emp, depts).collect())
[('d1', ('ann', 'Engineering')), ('d1', ('li', 'Engineering')), ('d2', ('raj', 'Sales')), ('d3', ('sam', 'HR')), ('d4', ('eva', None)), ('d5', (None, 'Legal'))]
>>> sorted(my_full_outer_join(emp, depts).collect()) == sorted(emp.fullOuterJoin(depts).collect())
True

>>> my_join(left, right).count()
13
```

**Notes:**

* **A join is a `cogroup()` plus a cross product per key.** The double loop
  `for v in lefts for w in rights` makes every `(v, w)` combination.
* **The empty side decides the join type**:
  * **inner**: if either list is empty, the double loop makes **no** rows, so the
    key disappears automatically;
  * **left outer**: if the right list is empty, use `[None]` instead, so each
    left value still gives one row;
  * **full outer**: do that on **both** sides.
* `list(...) or [None]` works because an empty list counts as `False` in Python.
  (A `ResultIterable` has a length, so an empty one is "false" too. The `list()` just
  makes the code work for **any** iterable, including a plain iterator, which is never "false".)
* PySpark's real `join()` (in `pyspark/join.py`) works almost the same way. It
  tags each value with which side it came from, puts both RDDs together, and groups by key:

  ```python
  vs = rdd.mapValues(lambda v: (1, v))
  ws = other.mapValues(lambda v: (2, v))
  return vs.union(ws).groupByKey(numPartitions).flatMapValues(lambda x: dispatch(x.__iter__()))
  ```

  For each key, `dispatch()` splits the values back into a left list and a right
  list, and returns every `(v, w)` combination, just like `my_join`.
* So **every join is a shuffle**: both RDDs are sent across the network, grouped by key.

---

## Q8 (Intermediate+) — Revenue per city and category: a 3-way join

```python
>>> products = (sc.textFile("data/shop_products.txt", 2)
...               .map(lambda line: line.split(","))
...               .map(lambda f: (f[0], (f[2], int(f[3])))))
>>> by_product = orders.map(lambda f: (f[2], (f[1], int(f[3]))))

>>> step1 = by_product.join(products)
>>> step1.count()
9
>>> sorted(step1.collect())[:2]
[('p1', (('c1', 1), ('electronics', 1000))), ('p2', (('c1', 2), ('electronics', 20)))]

>>> step2 = step1.map(lambda kv: (kv[1][0][0], (kv[1][1][0], kv[1][0][1] * kv[1][1][1])))
>>> sorted(step2.collect())[:3]
[('c1', ('electronics', 40)), ('c1', ('electronics', 1000)), ('c1', ('furniture', 150))]

>>> step3 = step2.join(customers)
>>> step3.count()
8

>>> revenue = (step3.map(lambda kv: ((kv[1][1][1], kv[1][0][0]), kv[1][0][1]))
...                 .reduceByKey(lambda a, b: a + b)
...                 .sortByKey())
>>> revenue.collect()
[(('Austin', 'electronics'), 60), (('Austin', 'furniture'), 300), (('San Jose', 'electronics'), 1140), (('San Jose', 'furniture'), 450), (('San Jose', 'home'), 160)]

>>> orders.count() - step3.count()
2
```

**Notes:**

* **A join can only use one key.** Orders connect to products by `product_id`,
  and to customers by `customer_id`, so this takes **two** joins, with a `map()`
  in between that **re-keys** the data.
* Reading `step1`'s rows: `(product_id, ((customer_id, quantity), (category, price)))`.
  So `kv[1][0][0]` is the customer, `kv[1][0][1]` the quantity, `kv[1][1][0]` the
  category, and `kv[1][1][1]` the price. Re-keying to `(customer_id, (category, amount))`
  also **drops everything we no longer need**, so less data goes into the next join.
* **2 orders were lost**: `o10` in the first join (product `p9` doesn't exist) and
  `o8` in the second (customer `c6` doesn't exist). Each inner join can silently
  drop rows. Count before and after, or use the `leftOuterJoin()` checks from Q5.
* Check one number by hand: San Jose electronics = Ann's laptop (1 × 1000) + Ann's
  mice (2 × 20) + Cara's mice (5 × 20) = **1140**.
* The final key is a **tuple** `(city, category)`, so `reduceByKey()` adds up
  per combination, and `sortByKey()` sorts by city first, then by category.

---

## Q9 (Intermediate+) — Customers who buy the same things: a self-join

```python
>>> bought = (sc.textFile("data/purchases.txt", 2)
...             .map(lambda line: line.split(","))
...             .map(lambda f: (f[2], f[0]))
...             .distinct())
>>> bought.count()
14

>>> pairs = bought.join(bought)
>>> pairs.count()
28

>>> ordered_pairs = pairs.filter(lambda kv: kv[1][0] < kv[1][1])
>>> ordered_pairs.count()
7
>>> sorted(ordered_pairs.collect())[:4]
[('apple', ('c1', 'c2')), ('apple', ('c1', 'c4')), ('apple', ('c2', 'c4')), ('bagel', ('c2', 'c4'))]

>>> shared = ordered_pairs.map(lambda kv: (kv[1], 1)).reduceByKey(lambda a, b: a + b).sortByKey()
>>> shared.collect()
[(('c1', 'c2'), 1), (('c1', 'c3'), 1), (('c1', 'c4'), 1), (('c2', 'c4'), 3), (('c3', 'c5'), 1)]

>>> shared.filter(lambda kv: kv[1] >= 2).collect()
[(('c2', 'c4'), 3)]
```

**Notes:**

* **`distinct()` first.** c1 bought apple twice. Without `distinct()`, apple would
  count twice for every pair that includes c1. We want *different products* in common.
* **A self-join** pairs every customer of a product with every customer of the
  same product, **including themselves** and **both orders**: for apple
  (c1, c2, c4) it makes 3 × 3 = 9 rows, such as `(c1, c1)`, `(c1, c2)`, and `(c2, c1)`.
* `c1 < c2` keeps each pair **once** and drops the "pair with yourself" rows: 9
  rows for apple become 3. With `!=` you'd keep both `(c1, c2)` and `(c2, c1)`, so
  every count would be doubled.
* `c2` and `c4` share 3 products (apple, milk, bagel), so they are the most
  similar customers. A recommender would suggest to `c2` the things `c4` bought
  that `c2` didn't buy, and the other way around.
* **Beware of skew**: a product bought by 1,000 customers makes 1,000 × 1,000 = **1
  million** rows in the self-join (about 500,000 after the filter), all in one
  task. Real systems often skip very popular products, or sample them. This is the
  same `m × n` rule as in Q6.
