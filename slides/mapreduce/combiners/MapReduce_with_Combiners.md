# MapReduce with Combiners: Examples

This document walks through two worked problems, each solved two
ways — once with plain `map()` + `reduce()`, and once with
`map()` + `combine()` + `reduce()` added on top. Comparing the two
versions of each problem shows exactly what a combiner buys you: the
reducer's logic barely changes, but far less data crosses the network
between the map and reduce phases.

* **Part 1 — Average per gene**
  * Example 1: `map()` + `reduce()`
  * Example 2: `map()` + `combine()` + `reduce()`
* **Part 2 — `(avg, min, max)` per gene**
  * Example 3: `map()` + `reduce()`
  * Example 4: `map()` + `combine()` + `reduce()`

## Input

Both parts use the same input format and sample data. Each record has
the form:

	<gene_id><,><gene_value>

Sample Input:

~~~text
g1,2.1
g1,1.1
g1,1.3
g2,1.1
g2,3.1
g2,4.3
g1,1.0
g1,2.0
g2,1.6
g2,4.4
...
~~~

## Why Add a Combiner?

Without a combiner, every `(gene_id, gene_value)` pair the mapper
emits travels across the network to the reducer. With millions of
records and a small number of distinct `gene_id`s, that's a lot of
shuffle traffic for very little useful information.

A combiner runs **locally, on the map side**, and pre-aggregates the
values for a key before they ever leave the node. This works whenever
the mapper's output value is designed as a **monoid under some merge
function** — partial results can be combined in any order, and
combining partial results gives the same answer as combining the raw
values directly. Part 1 uses `(sum, count)` as that partial result;
Part 2 extends it to `(sum, count, min, max)`.

Two caveats apply to every combiner:

* It must be **associative and commutative** — the framework may
  call it zero, one, or many times per key, in any order, so the
  result must not depend on how inputs are grouped.
* It is an **optimization, not a guarantee** — Hadoop/Spark may skip
  running it entirely. The reducer must therefore be able to produce
  a correct final result on its own, whether or not the combiner ran.
  This is why, in both examples below, the reducer repeats the same
  merge logic as the combiner.

---

# Part 1: Average per Gene

## Expected Output

Find the average `gene_value` per `gene_id`:

~~~text
(gene_id, avg)
~~~

## Example 1: `map()` and `reduce()`

### Mapper

~~~text
# key is a record number and ignored
# value is the entire record of input
map(key, value){
  # tokenize the given input record
  tokens = value.split(",")
  gene_id = tokens[0]
  gene_value = float(tokens[1])

  # generate output from mapper
  emit(gene_id, gene_value)
}
~~~

### Sort & Shuffle Output

~~~text
(gene_id_1, [v1, v2, v3, ...])
(gene_id_2, [a1, a2, a3, ...])
...
~~~

### Reducer

~~~text
# key is a gene_id
# values : Iterable<Double>
reduce(key, values){
  total = sum(values)
  count = len(values)
  avg = total / count
  emit(key, avg)
}
~~~

## Example 2: `map()`, `combine()`, and `reduce()`

### Mapper (revised to feed the combiner)

Instead of emitting the raw value, the mapper emits a `(sum, count)`
pair — the smallest unit that a combiner can meaningfully aggregate.

~~~text
# key is a record number and ignored
# value is the entire record of input
map(key, value){
  # tokenize the given input record
  tokens = value.split(",")
  gene_id = tokens[0]
  gene_value = float(tokens[1])

  # generate output from mapper
  # output of mapper is (sum, count),
  # which is a monoid under addition
  emit(gene_id, (gene_value, 1))
}
~~~

### Combiner

~~~text
# key is a gene_id
# values : Iterable<(Double, Integer)>
# example: values = [(1.1, 1), (1.7, 1), (1.2, 1)]
combine(key, values){
  sum = 0
  count = 0
  for p in values {
     # p = (sum, count)
     sum += p[0]
     count += p[1]
  }
  emit(key, (sum, count))
}
~~~

### Sort & Shuffle Output

Because the combiner already merged same-key pairs on each mapper
node, the reducer sees far fewer, pre-aggregated `(sum, count)` pairs
per key instead of every raw value:

~~~text
(gene_id_1, [(v1, c1), (v2, c2), (v3, c3), ...])
(gene_id_2, [(a1, d1), (a2, d2), (a3, d3), ...])
...
~~~

### Reducer

The reducer's logic is *identical in shape* to the combiner's — it
simply finishes the job by computing the final average. This is what
makes `(sum, count)` a good combiner design: the combine step and the
reduce step are the same associative merge function.

~~~text
# key is a gene_id
# values : Iterable<(Double, Integer)>
# example: values = [(4.0, 3), (7.2, 4), (9.3, 6)]
reduce(key, values){
  sum = 0
  count = 0
  for p in values {
     # p = (sum, count)
     sum += p[0]
     count += p[1]
  }
  avg = sum / count
  emit(key, avg)
}
~~~

## Part 1: Key Takeaway

Design the mapper's output around a value that is cheap to merge
pairwise and sufficient to compute the final answer — here,
`(sum, count)` instead of the raw value. That's what lets the same
merge function serve as both the combiner and the core of the
reducer.

---

# Part 2: `(avg, min, max)` per Gene

## Expected Output

Find `(avg, min, max)` per gene:

~~~text
(gene_id, (avg, min, max))
~~~

## Example 3: `map()` and `reduce()`

### Mapper

~~~text
# key is a record number and ignored
# value is the entire record of input
map(key, value){
  # tokenize the given input record
  tokens = value.split(",")
  gene_id = tokens[0]
  gene_value = float(tokens[1])

  # generate output from mapper
  emit(gene_id, gene_value)
}
~~~

### Sort & Shuffle Output

~~~text
(gene_id_1, [a1, a2, a3, ...])
(gene_id_2, [b1, b2, b3, ...])
...
(gene_id_100000, [v1, v2, ...])
~~~

### Reducer

~~~text
# key is a gene_id
# values : Iterable<Double>
reduce(key, values){
  total = sum(values)
  count = len(values)
  avg = total / count
  minimum = min(values)
  maximum = max(values)
  emit(key, (avg, minimum, maximum))
}
~~~

## Example 4: `map()`, `combine()`, and `reduce()`

### Mapper (revised to feed the combiner)

Instead of emitting the raw value, the mapper emits a
`(sum, count, min, max)` tuple seeded from that single value — the
smallest unit the combiner can meaningfully merge.

~~~text
# key is a record number and ignored
# value is the entire record of input
map(key, value){
  # tokenize the given input record
  tokens = value.split(",")
  gene_id = tokens[0]
  gene_value = float(tokens[1])

  # generate output from mapper
  #                  sum         count  min          max
  emit(gene_id, (gene_value,      1,   gene_value, gene_value))
}
~~~

### Sort & Shuffle Output (before combining)

~~~text
(gene_id_1, [
             (sum1, count1, min1, max1),
             (sum2, count2, min2, max2),
             ...
            ]
)
...
~~~

### Combiner

~~~text
# key is a gene_id
# values : Iterable<(Double, Integer, Double, Double)>
# values : Iterable<(sum, count, min, max)>
# example: values = [(1.1, 1, 1.1, 1.1),
#                    (1.7, 1, 1.7, 1.7),
#                    (2.1, 1, 2.1, 2.1), ...]
combine(key, values){
  sum = 0
  count = 0
  minimum = NULL
  maximum = NULL
  FIRST_TIME = True
  for p in values {
     # p = (sum, count, min, max)
     sum += p[0]
     count += p[1]
     if (FIRST_TIME) {
       minimum = p[2]
       maximum = p[3]
       FIRST_TIME = False
     }
     else {
        minimum = min(minimum, p[2])
        maximum = max(maximum, p[3])
     }
  }
  emit(key, (sum, count, minimum, maximum))
}
~~~

### Sort & Shuffle Output (after combining)

Because the combiner already merged same-key tuples on each mapper
node, the reducer sees a handful of pre-aggregated
`(sum, count, min, max)` tuples per key instead of every raw value:

~~~text
(gene_id_1, [
             (sum1, count1, min1, max1),
             (sum2, count2, min2, max2),
             ...
            ]
)
...
~~~

### Reducer

The reducer applies the *same merge logic* as the combiner, then
finishes the job by computing the average. This must hold whether or
not the combiner actually ran, since the framework doesn't guarantee it.

~~~text
# key is a gene_id
# values : Iterable<(Double, Integer, Double, Double)>
# values : Iterable<(sum, count, min, max)>
# example: values = [(4.1, 2, 2.0, 2.1),
#                    (1.7, 4, 0.5, 1.0),
#                    (9.1, 7, 1.1, 4.1), ...]
reduce(key, values){
  sum = 0
  count = 0
  minimum = NULL
  maximum = NULL
  FIRST_TIME = True
  for p in values {
     # p = (sum, count, min, max)
     sum += p[0]
     count += p[1]
     if (FIRST_TIME) {
       minimum = p[2]
       maximum = p[3]
       FIRST_TIME = False
     }
     else {
        minimum = min(minimum, p[2])
        maximum = max(maximum, p[3])
     }
  }
  # find average
  avg = sum / count
  emit(key, (avg, minimum, maximum))
}
~~~

## Part 2: Key Takeaway

`(sum, count, min, max)` is a monoid under pairwise merge: sum and
count combine by addition, min and max combine by taking the
min/max. Because combine and reduce use the exact same merge
function, the combiner is a pure optimization — correctness never
depends on whether it ran.

---

# Overall Key Takeaway

Both parts follow the same recipe for making a MapReduce job
combiner-friendly:

1. Choose a partial-result type for the mapper's output value that is
   a **monoid** — combinable pairwise, in any order, associatively.
2. Write **one merge function** for that type.
3. Use that merge function as **both** the combiner and the core of
   the reducer, so the job is correct with or without the combiner,
   and faster when it runs.
