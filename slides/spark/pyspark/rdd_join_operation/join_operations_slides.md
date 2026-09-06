---
marp: true
theme: default
paginate: true
header: "Big Data Modeling — Join Operations"
footer: "RDDs · PySpark · MapReduce"
style: |
  section { font-size: 26px; }
  h1 { color: #1f4e79; }
  h2 { color: #1f4e79; }
  code { font-size: 0.85em; }
  pre { font-size: 0.72em; line-height: 1.2; }
  .small { font-size: 0.85em; }
  .muted { color: #6b7280; }
---

# Join Operations in RDDs, PySpark, and MapReduce

### SQL joins → RDD joins → the same join, done by hand in MapReduce

<!-- source: join_operations_examples.md -->

---

## Agenda

1. Spark supports join between **RDDs** and between **DataFrames**.
2. Classic **MapReduce** has no built-in join — but one can be implemented
   on top of `map` / `shuffle` / `reduce`.

We'll cover:

- SQL joins (reference diagram)
- RDD joins in PySpark — inner, left outer, right outer, full outer
- The same inner join, implemented step-by-step in classic MapReduce
- Homework: left / right / full outer join in MapReduce

---

## SQL Joins

SQL joins: [Medium article](https://medium.com/@iammanolov98/mastering-sql-joins-coding-interview-preparation-innerjoin-e96bef58afc2) · [W3Schools](https://www.w3schools.com/sql/sql_join.asp)

![w:780](sql_joins.png)

---

## RDD Joins in PySpark

Spark RDDs support the following join operations on `(key, value)` pairs:

- `RDD.join()` — inner join (most common)
- `RDD.leftOuterJoin()`
- `RDD.rightOuterJoin()`
- `RDD.fullOuterJoin()`
- `RDD.cogroup()`
- `RDD.groupWith()`
- `pyspark.sql.DataFrame.join()`

---

## `RDD.join()` signature

```text
RDD.join(other: pyspark.rdd.RDD[Tuple[K, U]],
    numPartitions: Optional[int] = None)
    → pyspark.rdd.RDD[Tuple[K, Tuple[V, U]]]

Return an RDD containing all pairs of elements
with matching keys in self and other. Each pair
of elements will be returned as a (k, (v1, v2))
tuple, where (k, v1) is in self and (k, v2) is
in other. Performs a hash join across the cluster.
```

---

## Example 1 — setup

```python
>>> A = [('k1', 2), ('k1', 3), ('k2', 4), ('k2', 5), ('k3', 20), ('k4', 200)]
>>> B = [('k1', 20), ('k1', 30), ('k2', 40), ('k2', 50), ('k7', 20), ('k8', 2)]

>>> rdd1 = sc.parallelize(A)
>>> rdd2 = sc.parallelize(B)

>>> rdd1.collect()
[('k1', 2), ('k1', 3), ('k2', 4), ('k2', 5), ('k3', 20), ('k4', 200)]
>>> rdd2.collect()
[('k1', 20), ('k1', 30), ('k2', 40), ('k2', 50), ('k7', 20), ('k8', 2)]
```

---

## Example 1 — inner join

```python
>>> joined = rdd1.join(rdd2)
>>> joined.collect()
[
 ('k1', (2, 20)), ('k1', (2, 30)),
 ('k1', (3, 20)), ('k1', (3, 30)),
 ('k2', (4, 40)), ('k2', (4, 50)),
 ('k2', (5, 40)), ('k2', (5, 50))
]
```

`k3` and `k4` (only in `rdd1`) and `k7`/`k8` (only in `rdd2`) are dropped —
inner join keeps only matching keys.

---

## Example 1 — left / right outer join

```python
>>> left_outer_join = rdd1.leftOuterJoin(rdd2)
>>> left_outer_join.collect()
[
 ('k1', (2, 20)), ('k1', (2, 30)), ('k1', (3, 20)), ('k1', (3, 30)),
 ('k2', (4, 40)), ('k2', (4, 50)), ('k2', (5, 40)), ('k2', (5, 50)),
 ('k3', (20, None)), ('k4', (200, None))
]

>>> right_outer_join = rdd1.rightOuterJoin(rdd2)
>>> right_outer_join.collect()
[
 ('k1', (2, 20)), ('k1', (2, 30)), ('k1', (3, 20)), ('k1', (3, 30)),
 ('k2', (4, 40)), ('k2', (4, 50)), ('k2', (5, 40)), ('k2', (5, 50)),
 ('k8', (None, 2)), ('k7', (None, 20))
]
```

---

## Example 2 — setup

Same idea, different data — this time we'll add `fullOuterJoin`.

```python
>>> data1 = [('A', 2), ('A', 3), ('B', 4), ('B', 5), ('C', 5), ('D', 6)]
>>> data2 = [('A', 7), ('A', 8), ('B', 20), ('B', 30), ('E', 8), ('F', 9)]

>>> rdd1 = sc.parallelize(data1)
>>> rdd2 = sc.parallelize(data2)

>>> rdd1.collect()
[('A', 2), ('A', 3), ('B', 4), ('B', 5), ('C', 5), ('D', 6)]
>>> rdd2.collect()
[('A', 7), ('A', 8), ('B', 20), ('B', 30), ('E', 8), ('F', 9)]
```

---

## Example 2 — inner join

```python
>>> joined = rdd1.join(rdd2)
>>> joined.collect()
[
 ('A', (2, 7)), ('A', (2, 8)), ('A', (3, 7)), ('A', (3, 8)),
 ('B', (4, 20)), ('B', (4, 30)), ('B', (5, 20)), ('B', (5, 30))
]
```

Keys `C`, `D` (only in `rdd1`) and `E`, `F` (only in `rdd2`) are dropped.

---

## Example 2 — left outer join

```python
>>> left_join = rdd1.leftOuterJoin(rdd2)
>>> left_join.collect()
[
 ('A', (2, 7)), ('A', (2, 8)), ('A', (3, 7)), ('A', (3, 8)),
 ('B', (4, 20)), ('B', (4, 30)), ('B', (5, 20)), ('B', (5, 30)),
 ('D', (6, None)), ('C', (5, None))
]
```

For each `(k, v)` in `rdd1` (left), keep `(k, (v, w))` for matches in `rdd2`,
or `(k, (v, None))` if `k` has no match on the right.

---

## Example 2 — right outer join

```python
>>> right_join = rdd1.rightOuterJoin(rdd2)
>>> right_join.collect()
[
 ('A', (2, 7)), ('A', (2, 8)), ('A', (3, 7)), ('A', (3, 8)),
 ('B', (4, 20)), ('B', (4, 30)), ('B', (5, 20)), ('B', (5, 30)),
 ('E', (None, 8)), ('F', (None, 9))
]
```

Mirror image of `leftOuterJoin` — now `E`, `F` (right-only) are kept with
`None` on the left.

---

## Example 2 — full outer join

```python
>>> full_join = rdd1.fullOuterJoin(rdd2)
>>> full_join.collect()
[
 ('A', (2, 7)), ('A', (2, 8)), ('A', (3, 7)), ('A', (3, 8)),
 ('B', (4, 20)), ('B', (4, 30)), ('B', (5, 20)), ('B', (5, 30)),
 ('C', (5, None)), ('D', (6, None)),
 ('E', (None, 8)), ('F', (None, 9))
]
```

Everything from both sides — unmatched rows get `None` on the missing side.

---

## Join in the MapReduce Paradigm

MapReduce has no native join — we build one from `map` / `shuffle` / `reduce`.

We'll re-derive `rdd1.join(rdd2)` from **Example 2** (`data1`, `data2`) by hand.

**Outline:**
1. Inner join by example *(this walkthrough)*
2. Homework: left join
3. Homework: right join
4. Homework: full outer join

---

## Step 1–2: `map()` tags each row with its source

```text
D1: data1                      D2: data2
('A', 2)  -> ('A', ('D1', 2))  ('A', 7)  -> ('A', ('D2', 7))
('A', 3)  -> ('A', ('D1', 3))  ('A', 8)  -> ('A', ('D2', 8))
('B', 4)  -> ('B', ('D1', 4))  ('B', 20) -> ('B', ('D2', 20))
('B', 5)  -> ('B', ('D1', 5))  ('B', 30) -> ('B', ('D2', 30))
('C', 5)  -> ('C', ('D1', 5))  ('E', 8)  -> ('E', ('D2', 8))
('D', 6)  -> ('D', ('D1', 6))  ('F', 9)  -> ('F', ('D2', 9))
```

Every value is wrapped with a label (`D1` or `D2`) so the reducer can later
tell which data set each value came from.

---

## Step 3–4: combine outputs, run the identity mapper

All mapper output goes to one place, then an **identity mapper** just
re-emits each `(k, v)` unchanged — this is the input to shuffle:

```text
('A', ('D1', 2))  ('B', ('D1', 4))  ('C', ('D1', 5))  ('D', ('D1', 6))
('A', ('D1', 3))  ('B', ('D1', 5))
('A', ('D2', 7))  ('B', ('D2', 20)) ('E', ('D2', 8))   ('F', ('D2', 9))
('A', ('D2', 8))  ('B', ('D2', 30))
```

---

## Step 5: sort & shuffle

Framework groups all values by key:

```text
('A', [('D1', 2), ('D1', 3), ('D2', 7), ('D2', 8)])
('B', [('D1', 4), ('D1', 5), ('D2', 20), ('D2', 30)])
('C', [('D1', 5)])
('D', [('D1', 6)])
('E', [('D2', 8)])
('F', [('D2', 9)])
```

Only keys `A` and `B` have values from **both** `D1` and `D2` —
that's the inner-join condition.

---

## Step 6: the reducer (inner join)

```text
reduce(key, values) {
    if (len(values) < 2) return          # need both sides present

    D1_list, D2_list = [], []
    for (label, data) in values {
        if (label == 'D1') D1_list.append(data)
        else                D2_list.append(data)
    }
    if (len(D1_list) == 0 or len(D2_list) == 0) return

    for x in D1_list {
        for y in D2_list {
            emit(key, (x, y))
        }
    }
}
```

---

## Step 6 — worked out for keys `A` and `B`

```text
key = 'A':  D1_list = [2, 3]     D2_list = [7, 8]
  emit -> ('A', (2, 7)), ('A', (2, 8)), ('A', (3, 7)), ('A', (3, 8))

key = 'B':  D1_list = [4, 5]     D2_list = [20, 30]
  emit -> ('B', (4, 20)), ('B', (4, 30)), ('B', (5, 20)), ('B', (5, 30))

key = 'C', 'D', 'E', 'F':  only one side has values -> no emit
```

**Output matches `rdd1.join(rdd2)` from Example 2, exactly.** ✅

---

## Homework

Using the same `map` → `shuffle` → `reduce` pattern, implement:

1. `leftOuterJoin` in the MapReduce paradigm
2. `rightOuterJoin` in the MapReduce paradigm
3. `fullOuterJoin` in the MapReduce paradigm

<br>

**Hint:** the reducer's early `return` on a missing side is exactly where
each variant diverges from inner join — decide what to `emit` instead of
returning when only `D1_list` or only `D2_list` is non-empty.

---

## Further Reading

1. SQL Join — [w3schools.com](https://www.w3schools.com/sql/sql_join.asp)
2. PySpark Join — [`pyspark.sql.DataFrame.join`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html)
3. PySpark Joins by Example — [learnbymarketing.com](http://www.learnbymarketing.com/1100/pyspark-joins-by-example/)
4. PySpark Join Explained — [dzone.com](https://dzone.com/articles/pyspark-join-explained-with-examples)
5. Cartesian Product example — [Wikipedia](https://en.wikipedia.org/wiki/Cartesian_product)

---

# Questions?

Full write-up with both worked examples and complete pseudocode:
`join_operations_examples.md`
