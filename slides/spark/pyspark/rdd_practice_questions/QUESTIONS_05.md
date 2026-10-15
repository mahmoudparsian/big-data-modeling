# PySpark RDD Practice Questions — Set 05: Joins

Nine practice questions, three at each level, meant to be solved
**interactively** in the `bin/pyspark` shell. This set is about combining two
`(key, value)` RDDs by key:

| Operation | Result for a key `k` with values `v` (left) and `w` (right) | Keys in the result |
|---|---|---|
| `a.join(b)` | `(k, (v, w))` for **every pair** of `v` and `w` | only keys in **both** RDDs |
| `a.leftOuterJoin(b)` | like `join()`, but if `b` has no `w`, you get `(k, (v, None))` | every key of `a` |
| `a.cogroup(b)` | **one** element `(k, (all v's, all w's))`, two iterables | every key of `a` **or** `b` |

| # | Level | Topic | Input |
|---|---|---|---|
| [Q1](#q1-basic--employees-and-departments-join) | Basic | Employees and departments: `join()` | Python lists |
| [Q2](#q2-basic--whos-missing-leftouterjoin) | Basic | Who's missing? `leftOuterJoin()` | Python lists |
| [Q3](#q3-basic--everything-side-by-side-cogroup) | Basic | Everything side by side: `cogroup()` | Python lists |
| [Q4](#q4-intermediate--orders-per-customer-and-per-city) | Intermediate | Orders per customer and per city | [`data/shop_orders.txt`](data/shop_orders.txt), [`data/shop_customers.txt`](data/shop_customers.txt) |
| [Q5](#q5-intermediate--data-quality-checks-with-outer-joins) | Intermediate | Data quality checks with outer joins | [`data/shop_orders.txt`](data/shop_orders.txt), [`data/shop_customers.txt`](data/shop_customers.txt) |
| [Q6](#q6-intermediate--predict-the-size-of-a-join) | Intermediate | Predict the size of a join | Python lists |
| [Q7](#q7-intermediate--implement-the-joins-yourself-with-cogroup) | Intermediate+ | Implement the joins yourself with `cogroup()` | Python lists |
| [Q8](#q8-intermediate--revenue-per-city-and-category-a-3-way-join) | Intermediate+ | Revenue per city and category: a 3-way join | all three `data/shop_*.txt` files |
| [Q9](#q9-intermediate--customers-who-buy-the-same-things-a-self-join) | Intermediate+ | Customers who buy the same things: a self-join | [`data/purchases.txt`](data/purchases.txt) |

Use **`join()`, `leftOuterJoin()`, and `cogroup()`**, plus everything from
the earlier sets: `map()`, `flatMap()`, `filter()`, `mapValues()`, `distinct()`,
`reduceByKey()`, `sortByKey()`, `collect()`, `count()`, `take()`.

Try each question on your own before you open the
[answers](ANSWERS_05.md).

## Files

| File | Purpose |
|---|---|
| [QUESTIONS_05.md](QUESTIONS_05.md) | The nine questions (this page) |
| [ANSWERS_05.md](ANSWERS_05.md) | Step-by-step solutions, with real output from Spark 4.2.0 |
| [data/shop_customers.txt](data/shop_customers.txt) | Input for Q4, Q5, Q8: `customer_id,name,city` |
| [data/shop_orders.txt](data/shop_orders.txt) | Input for Q4, Q5, Q8: `order_id,customer_id,product_id,quantity` |
| [data/shop_products.txt](data/shop_products.txt) | Input for Q8: `product_id,name,category,price` |
| [data/purchases.txt](data/purchases.txt) | Input for Q9 (also used in Set 01): `customer,category,product` |

## Getting started

Start `$SPARK_HOME/bin/pyspark` **from this folder**, as in
[Set 01](QUESTIONS_01.md#getting-started).

Tips for this set:

* A join result looks like `(key, (left_value, right_value))`, a tuple **inside**
  a tuple. Draw it on paper before you write `kv[1][0][1]`.
* The order of join results isn't fixed, and `sortByKey()` only sorts by the key.
  Use Python's `sorted(rdd.collect())` to get a fully sorted result.

The three shop files are small but realistic. Like real data, they contain a
few **problems**: some customers have no orders, one order has an unknown
customer, and one order has an unknown product.

---

## Q1 (Basic) — Employees and departments: join()

**Input:** `(dept_id, employee)` and `(dept_id, dept_name)` pairs:

```python
emp = sc.parallelize([("d1", "ann"), ("d2", "raj"), ("d1", "li"),
                      ("d3", "sam"), ("d4", "eva")], 2)
depts = sc.parallelize([("d1", "Engineering"), ("d2", "Sales"),
                        ("d3", "HR"), ("d5", "Legal")], 2)
```

**Tasks:**

1. Join `emp` with `depts`. Show the sorted result, and count it.
2. Turn the result into `(employee, dept_name)` pairs, sorted.
3. Now join the other way, `depts.join(emp)`. What changes?

**Expected output:**

```
[('d1', ('ann', 'Engineering')), ('d1', ('li', 'Engineering')), ('d2', ('raj', 'Sales')), ('d3', ('sam', 'HR'))]
4
[('ann', 'Engineering'), ('li', 'Engineering'), ('raj', 'Sales'), ('sam', 'HR')]
[('d1', ('Engineering', 'ann')), ('d1', ('Engineering', 'li')), ('d2', ('Sales', 'raj')), ('d3', ('HR', 'sam'))]
```

*Think about it:* what happened to `eva` (`d4`) and to `Legal` (`d5`)?

---

## Q2 (Basic) — Who's missing? leftOuterJoin()

**Input:** the same `emp` and `depts` RDDs as in Q1.

**Tasks:**

1. Compute `emp.leftOuterJoin(depts)`, sorted.
2. Which employees have **no department**? Show just their names.
3. Which departments have **no employees**? (There is no `rightOuterJoin()` in
   this set. How can you get the answer with `leftOuterJoin()`?)

**Expected output:**

```
[('d1', ('ann', 'Engineering')), ('d1', ('li', 'Engineering')), ('d2', ('raj', 'Sales')), ('d3', ('sam', 'HR')), ('d4', ('eva', None))]
['eva']
['Legal']
```

---

## Q3 (Basic) — Everything side by side: cogroup()

**Input:** the same `emp` and `depts` RDDs as in Q1.

**Tasks:**

1. Compute `emp.cogroup(depts)`. How many elements does it have?
2. Show each key with **two sorted lists**: its employees and its department names,
   sorted by key.
3. Show each key with the **number** of employees and the number of department names.

**Expected output:**

```
5
[('d1', (['ann', 'li'], ['Engineering'])), ('d2', (['raj'], ['Sales'])), ('d3', (['sam'], ['HR'])), ('d4', (['eva'], [])), ('d5', ([], ['Legal']))]
[('d1', (2, 1)), ('d2', (1, 1)), ('d3', (1, 1)), ('d4', (1, 0)), ('d5', (0, 1))]
```

*Think about it:* `join()` gave 4 elements and `cogroup()` gives 5. Where does
the difference come from?

---

## Q4 (Intermediate) — Orders per customer and per city

**Input:** [`data/shop_customers.txt`](data/shop_customers.txt) and
[`data/shop_orders.txt`](data/shop_orders.txt), each read with **2** partitions:

```
c1,Ann,San Jose                 o1,c1,p1,1
c2,Bob,Austin                   o2,c2,p2,3
...                             ...
```

**Tasks:**

1. Read the customers as `(customer_id, (name, city))` and the orders as lists of
   fields. Count both.
2. Key the orders by customer, as `(customer_id, order_id)`, and join them with
   the customers. How many joined rows are there? Show the first 3 of the sorted result.
3. From the joined rows, find the **number of orders per city**.
4. A better plan: first count the **orders per customer** (before any join), then
   join the counts with the customers. Show the orders per customer **name**, and
   the orders per city again.

**Expected output:**

```
5
10
9
[('c1', ('o1', ('Ann', 'San Jose'))), ('c1', ('o3', ('Ann', 'San Jose'))), ('c1', ('o7', ('Ann', 'San Jose')))]
[('Austin', 3), ('San Jose', 6)]
[('c1', 3), ('c2', 3), ('c3', 3), ('c6', 1)]
[('Ann', 3), ('Bob', 3), ('Cara', 3)]
[('Austin', 3), ('San Jose', 6)]
```

(The line with `c6` is the order count per customer, **before** the join.)

*Think about it:* there are 10 orders, but only 9 joined rows. Why? And why is
task 4 a better plan when there are millions of orders?

---

## Q5 (Intermediate) — Data quality checks with outer joins

**Input:** the customers and the order counts per customer from Q4.

**Tasks:**

1. Use `leftOuterJoin()` to put **every customer** next to their order count,
   sorted by key.
2. Which customers have **never ordered**? Show their names, sorted.
3. Show `(customer_id, (name, number_of_orders))` for all customers, using `0`
   instead of `None`.
4. Which orders have a customer id that is **not** in the customer file? Show them
   as `(customer_id, (order_id, None))`.

**Expected output:**

```
[('c1', (('Ann', 'San Jose'), 3)), ('c2', (('Bob', 'Austin'), 3)), ('c3', (('Cara', 'San Jose'), 3)), ('c4', (('Dev', 'Boston'), None)), ('c5', (('Eli', 'Austin'), None))]
['Dev', 'Eli']
[('c1', ('Ann', 3)), ('c2', ('Bob', 3)), ('c3', ('Cara', 3)), ('c4', ('Dev', 0)), ('c5', ('Eli', 0))]
[('c6', ('o8', None))]
```

---

## Q6 (Intermediate) — Predict the size of a join

**Input:** keys that appear **several times on both sides**:

```python
left = sc.parallelize([("k1", "a1"), ("k1", "a2"), ("k2", "b1"),
                       ("k3", "c1"), ("k3", "c2"), ("k3", "c3")], 2)
right = sc.parallelize([("k1", "x1"), ("k1", "x2"), ("k1", "x3"), ("k2", "y1"),
                        ("k3", "z1"), ("k3", "z2"), ("k4", "w1")], 2)
```

**Tasks:**

1. **Without running the join**, predict how many rows `left.join(right)` will have.
   Then compute that number with Spark: count the values per key on each side,
   join the **counts**, and multiply them. Show the size per key and the total.
2. Now run the join and count it.
3. Show all the joined rows for key `k3`, sorted.
4. Compare with the number of rows of a "join everything with everything"
   (`left.count() * right.count()`).

**Expected output:**

```
[('k1', 6), ('k2', 1), ('k3', 6)]
13
13
[('k3', ('c1', 'z1')), ('k3', ('c1', 'z2')), ('k3', ('c2', 'z1')), ('k3', ('c2', 'z2')), ('k3', ('c3', 'z1')), ('k3', ('c3', 'z2'))]
42
```

---

## Q7 (Intermediate+) — Implement the joins yourself with cogroup()

Suppose `join()` and `leftOuterJoin()` were not available, but `cogroup()` was.

**Tasks:**

1. Write `my_join(a, b)` using `cogroup()` and `flatMap()`. Check that it gives the
   same rows as `a.join(b)`, on `emp` and `depts` from Q1.
2. Write `my_left_outer_join(a, b)`, and check it against `a.leftOuterJoin(b)`.
3. Write `my_full_outer_join(a, b)`: every key from **either** side, with `None`
   on the side that's missing. Show it for `emp` and `depts`, sorted, and check it
   against the built-in `a.fullOuterJoin(b)`.
4. Check that `my_join(left, right)` gives 13 rows, using `left` and `right` from Q6.

**Expected output:**

```
True
True
[('d1', ('ann', 'Engineering')), ('d1', ('li', 'Engineering')), ('d2', ('raj', 'Sales')), ('d3', ('sam', 'HR')), ('d4', ('eva', None)), ('d5', (None, 'Legal'))]
True
13
```

**Hint:** for each key, `cogroup()` gives you all the left values and all the right
values. A join is just **every combination** of one left value and one right value.
What should happen when one of the two lists is empty?

---

## Q8 (Intermediate+) — Revenue per city and category: a 3-way join

**Input:** all three shop files, each read with **2** partitions:
[`data/shop_orders.txt`](data/shop_orders.txt) (`order_id,customer_id,product_id,quantity`),
[`data/shop_products.txt`](data/shop_products.txt) (`product_id,name,category,price`), and
[`data/shop_customers.txt`](data/shop_customers.txt) (`customer_id,name,city`).

**Goal:** the total revenue (`quantity × price`) for each `(city, category)`.

**Tasks:**

1. Key the orders by **product**, as `(product_id, (customer_id, quantity))`, and
   the products as `(product_id, (category, price))`. Join them. Count the result
   and show the first 2 sorted rows.
2. **Re-key** the result by **customer**, as `(customer_id, (category, amount))`.
   Show the first 3 sorted rows.
3. Join with the customers. How many rows are left?
4. Compute the revenue per `(city, category)`, sorted.
5. How many of the 10 orders were lost in the joins, and why?

**Expected output:**

```
9
[('p1', (('c1', 1), ('electronics', 1000))), ('p2', (('c1', 2), ('electronics', 20)))]
[('c1', ('electronics', 40)), ('c1', ('electronics', 1000)), ('c1', ('furniture', 150))]
8
[(('Austin', 'electronics'), 60), (('Austin', 'furniture'), 300), (('San Jose', 'electronics'), 1140), (('San Jose', 'furniture'), 450), (('San Jose', 'home'), 160)]
2
```

---

## Q9 (Intermediate+) — Customers who buy the same things: a self-join

**Input:** [`data/purchases.txt`](data/purchases.txt), read with **2** partitions,
as `customer,category,product` (the same file as in Set 01, Q9).

**Goal:** for each **pair of customers**, find how many **different products** they
both bought. This is the first step of a "customers like you also bought..."
recommender.

**Tasks:**

1. Make unique `(product, customer)` pairs. Count them.
2. **Join this RDD with itself** on the product. How many rows?
3. Keep only the rows where the first customer comes **before** the second one
   alphabetically. How many are left? Show the first 4, sorted.
4. Count the shared products for each customer pair, as `((c1, c2), count)`, sorted.
5. Which pairs share **2 or more** products?

**Expected output:**

```
14
28
7
[('apple', ('c1', 'c2')), ('apple', ('c1', 'c4')), ('apple', ('c2', 'c4')), ('bagel', ('c2', 'c4'))]
[(('c1', 'c2'), 1), (('c1', 'c3'), 1), (('c1', 'c4'), 1), (('c2', 'c4'), 3), (('c3', 'c5'), 1)]
[(('c2', 'c4'), 3)]
```

*Think about it:* in task 3, why `<` and not `!=`? What would the self-join give
for a product bought by 1,000 customers?
