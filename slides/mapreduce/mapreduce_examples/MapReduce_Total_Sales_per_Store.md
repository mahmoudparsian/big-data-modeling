# MapReduce Example: Total Sales, Order Count, and Largest Order per Store

	Author: Mahmoud Parsian
	Last updated: 9/3/2026

## 1. Introduction

This article works through a **complete** `map()` /
`combine()` / `reduce()` job, by hand, on real sample data —
every mapper call, every combiner call, the shuffle output
before and after combining, and every reducer call. Unlike
Word Count, the reducer here combines **three** different
aggregates at once (a sum, a count, and a max) for the same
key, which is a more realistic shape for real analytics
jobs and a better test of whether a combiner is actually
correct.

## 2. Problem

Given a retail chain's order log, find, **per store**:

1. total revenue (sum of all order amounts),
2. number of orders (count), and
3. the single largest order placed at that store (max).

## 3. Input Data Format

```text
<order_id>,<date>,<store_id>,<store_name>,<amount>
```

## 4. Sample Dataset

12 orders across 3 stores, split across **two mapper
partitions** — this split matters later, because the
combiner trace in Section 14 runs separately on each one:

**Partition A** (Mapper 1's input):

```text
O1,1/2/2024,S1,Downtown Cafe,45.00
O2,1/2/2024,S2,Uptown Diner,30.00
O3,1/3/2024,S1,Downtown Cafe,60.00
O4,1/3/2024,S3,Riverside Grill,25.00
O5,1/4/2024,S1,Downtown Cafe,15.00
O6,1/4/2024,S2,Uptown Diner,50.00
```

**Partition B** (Mapper 2's input):

```text
O7,1/5/2024,S3,Riverside Grill,80.00
O8,1/5/2024,S1,Downtown Cafe,20.00
O9,1/6/2024,S2,Uptown Diner,70.00
O10,1/6/2024,S3,Riverside Grill,35.00
O11,1/7/2024,S1,Downtown Cafe,90.00
O12,1/7/2024,S2,Uptown Diner,10.00
```

`Downtown Cafe` (`S1`) has 5 orders, `Uptown Diner` (`S2`)
has 4, and `Riverside Grill` (`S3`) has 3 — different
counts on purpose, so the aggregates below aren't all
trivially identical.

## 5. Is This a Big Data Problem?

Yes, at real chain-store scale. A national chain with 5,000
stores, each logging 500 orders/day, produces
`5,000 * 500 * 365 = 912,500,000` order records a year —
close to a billion rows a year, growing every year. No
single machine holds years of that in memory, which is
exactly the case for splitting the input across many
mappers, as Section 4's two-partition split does in
miniature.

## 6. Output Data Format

```text
(store_id-store_name, (total_revenue, order_count, largest_order))
```

## 7. Input to Mappers

As in the companion examples in this folder, each input
line arrives at a mapper as a `(key, value)` pair where
`key` is a record number (ignored) and `value` is the raw
CSV line:

```text
(1, "O1,1/2/2024,S1,Downtown Cafe,45.00")
(2, "O2,1/2/2024,S2,Uptown Diner,30.00")
...
```

## 8. Mapper

```text
# key: a record number, ignored
# value: "<order_id>,<date>,<store_id>,<store_name>,<amount>"
map(key, value) {
   tokens = value.split(",")
   # order_id = tokens[0]
   # date     = tokens[1]
   store_id   = tokens[2]
   store_name = tokens[3]
   amount     = float(tokens[4])

   output_key = store_id + "-" + store_name
   emit(output_key, amount)
}
```

## 9. Output of Mappers (all 12 calls)

**Mapper 1** (Partition A):

```text
map(1, "O1,...,S1,Downtown Cafe,45.00")  -> ("S1-Downtown Cafe", 45.00)
map(2, "O2,...,S2,Uptown Diner,30.00")   -> ("S2-Uptown Diner", 30.00)
map(3, "O3,...,S1,Downtown Cafe,60.00")  -> ("S1-Downtown Cafe", 60.00)
map(4, "O4,...,S3,Riverside Grill,25.00")-> ("S3-Riverside Grill", 25.00)
map(5, "O5,...,S1,Downtown Cafe,15.00")  -> ("S1-Downtown Cafe", 15.00)
map(6, "O6,...,S2,Uptown Diner,50.00")   -> ("S2-Uptown Diner", 50.00)
```

**Mapper 2** (Partition B):

```text
map(7,  "O7,...,S3,Riverside Grill,80.00") -> ("S3-Riverside Grill", 80.00)
map(8,  "O8,...,S1,Downtown Cafe,20.00")   -> ("S1-Downtown Cafe", 20.00)
map(9,  "O9,...,S2,Uptown Diner,70.00")    -> ("S2-Uptown Diner", 70.00)
map(10, "O10,...,S3,Riverside Grill,35.00")-> ("S3-Riverside Grill", 35.00)
map(11, "O11,...,S1,Downtown Cafe,90.00")  -> ("S1-Downtown Cafe", 90.00)
map(12, "O12,...,S2,Uptown Diner,10.00")   -> ("S2-Uptown Diner", 10.00)
```

## 10. Sort & Shuffle Phase (no combiner)

Without a combiner, every one of the 12 mapped values
crosses the network individually, grouped only by key:

```text
("S1-Downtown Cafe",   [45.00, 60.00, 15.00, 20.00, 90.00])
("S2-Uptown Diner",    [30.00, 50.00, 70.00, 10.00])
("S3-Riverside Grill", [25.00, 80.00, 35.00])
```

## 11. Reducer

```text
# key: "<store_id>-<store_name>"
# values: Iterable<Float>
reduce(key, values) {
   total = 0.0
   count = 0
   largest = None
   for (v in values) {
      total += v
      count += 1
      if (largest is None OR v > largest) {
         largest = v
      }
   }
   emit(key, (total, count, largest))
}
```

## 12. Output of Reducers

```text
("S1-Downtown Cafe",   (230.00, 5, 90.00))
("S2-Uptown Diner",    (160.00, 4, 70.00))
("S3-Riverside Grill", (140.00, 3, 80.00))
```

These three numbers per store are the ground truth the rest
of this article checks the combiner against.

## 13. Combiner — Why (sum, count, max) Is Safe to Combine

A combiner runs **locally, on one mapper's output only**,
before the shuffle — see
[`MapReduce_with_Combiners.md`](../combiners/MapReduce_with_Combiners.md)
for the general theory. It is only correct to add one if
merging its partial results later is guaranteed to produce
the same answer as never combining at all. All three
aggregates here qualify, for the same underlying reason:
addition and `max` are both **associative** and
**commutative** — `(a + b) + c == a + (b + c)` and
`a + b == b + a`, and likewise for `max` — so it doesn't
matter which values a combiner happens to see together, or
in what order.

Compare this with the **wrong** move: emitting a
partial *average* (`total / count`) from the combiner
instead of a `(sum, count)` pair — averaging is not
associative, and combining partial averages gives the wrong
final answer. See "Combiner — Why Averaging Needs Care" in
[`MapReduce_Find_Average_Temperature.md`](MapReduce_Find_Average_Temperature.md)
for a worked example of exactly that failure. The fix used
here is the same one used there: never let the combiner
compute the final derived value (an average, in that case; a
ratio or rate in general) — only combine the raw ingredients
(`sum`, `count`, `max`) that are safe to combine, and compute
anything derived from them exactly once, in the reducer
(Section 18 does that here).

```text
# key: "<store_id>-<store_name>"
# values: Iterable<Float> (local to ONE mapper only)
combine(key, values) {
   total = 0.0
   count = 0
   largest = None
   for (v in values) {
      total += v
      count += 1
      if (largest is None OR v > largest) {
         largest = v
      }
   }
   # emit a (sum, count, max) triple -- never a derived average
   emit(key, (total, count, largest))
}
```

## 14. Combiner Trace: Two Mapper Partitions

**Mapper 1's combiner** runs only over Partition A's 6
mapped values (Section 9):

```text
combine("S1-Downtown Cafe", [45.00, 60.00, 15.00])
   -> ("S1-Downtown Cafe", (120.00, 3, 60.00))

combine("S2-Uptown Diner", [30.00, 50.00])
   -> ("S2-Uptown Diner", (80.00, 2, 50.00))

combine("S3-Riverside Grill", [25.00])
   -> ("S3-Riverside Grill", (25.00, 1, 25.00))
```

**Mapper 2's combiner** runs only over Partition B's 6
mapped values, completely independently of Mapper 1's:

```text
combine("S3-Riverside Grill", [80.00, 35.00])
   -> ("S3-Riverside Grill", (115.00, 2, 80.00))

combine("S1-Downtown Cafe", [20.00, 90.00])
   -> ("S1-Downtown Cafe", (110.00, 2, 90.00))

combine("S2-Uptown Diner", [70.00, 10.00])
   -> ("S2-Uptown Diner", (80.00, 2, 70.00))
```

12 raw values went in; 6 combined triples come out — the
combiner has already cut what crosses the network in half,
*before* the shuffle even runs.

## 15. Sort & Shuffle Phase (With Combiner)

The shuffle now groups the 6 combined triples from Section
14 by key, instead of the 12 raw values from Section 10:

```text
("S1-Downtown Cafe",   [(120.00, 3, 60.00), (110.00, 2, 90.00)])
("S2-Uptown Diner",    [(80.00, 2, 50.00), (80.00, 2, 70.00)])
("S3-Riverside Grill", [(25.00, 1, 25.00), (115.00, 2, 80.00)])
```

## 16. Reducer (With Combiner)

The reducer's shape changes slightly: it now receives
`(sum, count, max)` triples instead of raw floats, and
merges them the same way the combiner merged raw values —
sum the sums, sum the counts, and take the max of the maxes:

```text
# key: "<store_id>-<store_name>"
# values: Iterable<(sum, count, max)>
reduce(key, values) {
   total = 0.0
   count = 0
   largest = None
   for ((s, c, m) in values) {
      total += s
      count += c
      if (largest is None OR m > largest) {
         largest = m
      }
   }
   emit(key, (total, count, largest))
}
```

## 17. Output of Reducers (With Combiner)

```text
("S1-Downtown Cafe",   (230.00, 5, 90.00))
("S2-Uptown Diner",    (160.00, 4, 70.00))
("S3-Riverside Grill", (140.00, 3, 80.00))
```

**Identical** to Section 12's no-combiner output, for every
store — proof that adding the combiner changed *when* and
*how much* data crossed the network, not the answer.

## 18. Bonus: Deriving the Average Order Value

The reducer already has `(total, count)` for each store, so
computing the average order value is one more division —
done exactly once, after every value has been folded in,
never inside the combiner (Section 13):

```text
(S1-Downtown Cafe,   revenue=230.00, orders=5, largest=90.00, avg=46.00)
(S2-Uptown Diner,    revenue=160.00, orders=4, largest=70.00, avg=40.00)
(S3-Riverside Grill, revenue=140.00, orders=3, largest=80.00, avg=46.67)
```

## 19. Spark Equivalent (Sketch)

The same `(sum, count, max)` combiner shape maps directly
onto PySpark's `combineByKey` — this is exactly
[`intro_to_mapreduce_with_pyspark.md`'s Section 18](../intro_to_mapreduce_with_pyspark/intro_to_mapreduce_with_pyspark.md#18-worked-example--average-per-key-and-why-naive-averaging-breaks)
average-per-key pattern, extended with one more tracked
value:

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "sales")

orders = sc.textFile("orders.csv") \
    .map(lambda line: line.split(",")) \
    .map(lambda f: (f[2] + "-" + f[3], float(f[4])))   # (store_key, amount)

agg = orders.combineByKey(
    lambda v: (v, 1, v),                                        # createCombiner
    lambda acc, v: (acc[0] + v, acc[1] + 1, max(acc[2], v)),     # mergeValue
    lambda a, b: (a[0] + b[0], a[1] + b[1], max(a[2], b[2])),    # mergeCombiners
)
result = agg.mapValues(lambda t: (t[0], t[1], t[2], round(t[0] / t[1], 2)))
for store, (revenue, count, largest, avg) in sorted(result.collect()):
    print(store, revenue, count, largest, avg)
```

```text
S1-Downtown Cafe 230.0 5 90.0 46.0
S2-Uptown Diner 160.0 4 70.0 40.0
S3-Riverside Grill 140.0 3 80.0 46.67
```

`combineByKey`'s three functions are exactly Section 13's
`combine()` and Section 16's `reduce()` written as one
call: the first two functions *are* the mapper-local
combiner (build a `(sum, count, max)` triple, then fold
more values into it), and the third is the reducer merging
triples that arrived from different partitions — matching
this article's numbers exactly, verified against a real
PySpark run.

## 20. Food for Thought

1. Add a fourth tracked value, the **smallest** order per
   store, alongside `(sum, count, max)`. Is `min` safe to
   combine for the same reason `max` is? Why?

2. Suppose, instead of the largest *order amount*, you
   wanted the **`order_id` of the largest order** (e.g.
   `"O11"` for `S1-Downtown Cafe`, not `90.00`). Rewrite
   `map()`, `combine()`, and `reduce()` to carry the
   `order_id` through alongside the amount, so the max
   comparison still works but the identifier survives to the
   final output too.

3. `max` is **idempotent** (`max(x, x) == x`) but `sum` is
   not (`x + x != x`, unless `x == 0`). Construct a scenario
   where a combiner accidentally runs *twice* on the same
   partial data (e.g., due to task speculation/retry) and
   explain what breaks for the `sum`/`count` fields but not
   for the `max` field.

4. Rewrite Section 8's `map()` to drop any record whose
   `amount` is negative or zero (a data-quality filter) —
   does this filter belong in the mapper or the reducer, and
   why? (See "Mapper filter vs. reducer filter — why it
   matters" in
   [`MapReduce_Word_Count.md`](MapReduce_Word_Count.md) for
   the general rule.)

5. Extend the job to also compute **total revenue across all
   stores** (a single grand-total number, not per-store).
   What would the output key be, and does this change need a
   second MapReduce job, or can it be folded into this one?

## 21. Comments

Comments and suggestions are welcome!

## 22. References

1. [`MapReduce_Find_Average_Temperature.md`](MapReduce_Find_Average_Temperature.md) — companion example; the averaging pitfall this article's combiner is designed to avoid
2. [`MapReduce_Word_Count.md`](MapReduce_Word_Count.md) — companion example; mapper-filter vs. reducer-filter discussion referenced in Section 20
3. [`combiners/MapReduce_with_Combiners.md`](../combiners/MapReduce_with_Combiners.md) — the general theory of when a combiner is correct
4. [`associativity_and_commutativity/Associativity_Commutativity_and_Reducers.md`](../associativity_and_commutativity/Associativity_Commutativity_and_Reducers.md) — the formal version of Section 13's associativity/commutativity argument
5. [`intro_to_mapreduce_with_pyspark/intro_to_mapreduce_with_pyspark.md`, Section 18](../intro_to_mapreduce_with_pyspark/intro_to_mapreduce_with_pyspark.md#18-worked-example--average-per-key-and-why-naive-averaging-breaks) — the `combineByKey` pattern this article's Spark sketch extends
6. [Data-Intensive Text Processing with MapReduce by Jimmy Lin and Chris Dyer](https://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)
