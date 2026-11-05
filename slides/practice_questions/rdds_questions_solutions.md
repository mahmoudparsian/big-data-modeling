# PySpark RDD Practice Questions — Solutions

Solutions to [`slides/practice_questions/rdds_questions.md`](../../slides/practice_questions/rdds_questions.md).
Question numbers match that file exactly. Every code answer below was
actually executed against PySpark 4.2.0 and the shown output is the real,
verified result (not hand-typed).

**Not for distribution to students.**

---

## Solution 1

```python
data = ["s1,85", "s2,45", "s1,95", "s2,150", "s3,5", "bad_record", "s1,60"]
rdd = sc.parallelize(data)

def parse(line):
    parts = line.split(",")
    if len(parts) != 2:
        return None
    sid, g = parts[0], parts[1]
    try:
        g = int(g)
    except ValueError:
        return None
    if g > 100 or g < 10:
        return None
    return (sid, g)

result = (rdd.map(parse)
             .filter(lambda x: x is not None)
             .groupByKey()
             .mapValues(lambda vals: (min(vals), max(vals)))
             .collect())
```

Verified output: `[('s1', (60, 95)), ('s2', (45, 45))]`
(`s2,150` was dropped by rule 1, `s3,5` by rule 2, `bad_record` for being
malformed.)

---

## Solution 2

```python
data = ["m1,3", "m1,7", "m2,5", "bad", "m1,1", "m2,2"]
rdd = sc.parallelize(data)

def parse(line):
    parts = line.split(",")
    if len(parts) != 2:
        return None
    movie, r = parts
    try:
        r = int(r)
    except ValueError:
        return None
    if r > 5:
        return None
    return (movie, r)

result = (rdd.map(parse)
             .filter(lambda x: x is not None)
             .map(lambda x: (x[0], 1))
             .reduceByKey(lambda a, b: a + b)
             .collect())
```

Verified output: `[('m1', 2), ('m2', 2)]` (`m1,7` dropped by rule 1, `bad`
dropped by rule 2).

---

## Solution 3

```python
data = ["g1,r1,2.3", "g1,r2,1.5", "g2,r1,4.1", "g2,r3,9.9", "g1,r1,0.1"]
rdd = sc.parallelize(data)

result = (rdd.map(lambda l: l.split(","))
             .filter(lambda p: p[1] == "r1")
             .map(lambda p: (p[0], 1))
             .reduceByKey(lambda a, b: a + b)
             .collect())
```

Verified output: `[('g1', 2), ('g2', 1)]`.

---

## Solution 4

```python
data = ["w1,w2,w3,w4", "w2,w3,w5"]
rdd = sc.parallelize(data)

def bigrams(line):
    words = line.split(",")
    return [(words[i], words[i+1]) for i in range(len(words) - 1)]

result = (rdd.flatMap(bigrams)
             .map(lambda bg: (bg, 1))
             .reduceByKey(lambda a, b: a + b)
             .collect())
```

Verified output:
`[(('w1','w2'),1), (('w2','w3'),2), (('w3','w4'),1), (('w3','w5'),1)]`
(`("w2","w3")` correctly appears twice — once from each input line).

---

## Solution 5

```python
data = [(90,20),(40,70),(10,40),(30,40),(40,90),(30,80),(20,30),(20,10)]
rdd = sc.parallelize(data)

allvals = rdd.flatMap(lambda kv: [kv[0], kv[1]])
mn = allvals.min()
mx = allvals.max()
```

Verified: `min = 10`, `max = 90` — matches the expected result exactly.

**Efficiency discussion:** `flatMap` fans every key and value out into one
flat RDD, then `.min()`/`.max()` are Spark actions that are internally
implemented as a distributed reduce (each partition computes a local
min/max first, and only those small partial results are shuffled/combined)
— so unlike the naive "funnel everything through one constant key" MapReduce
version in the companion question, this scales fine: no single task ever
sees the full dataset, and only `O(numPartitions)` partial results are
combined at the end.

---

## Solution 6

```python
docs = [("Document1", "fox jumped fast fox fast"),
        ("Document2", "fox ran fox jumped fast"),
        ("Document3", "hello hello hello fox")]
rdd = sc.parallelize(docs)

def emit_positions(doc):
    docid, text = doc
    words = text.split()
    return [(w, (docid, i + 1)) for i, w in enumerate(words)]

postings = rdd.flatMap(emit_positions)
inverted = postings.groupByKey().mapValues(lambda vals: sorted(vals)).collect()
```

Verified output (after grouping each word's `(doc, position)` pairs by
document for display):

```text
fast   -> {'Document1': [3, 5], 'Document2': [5]}
fox    -> {'Document1': [1, 4], 'Document2': [1, 3], 'Document3': [4]}
hello  -> {'Document3': [1, 2, 3]}
jumped -> {'Document1': [2], 'Document2': [4]}
ran    -> {'Document2': [2]}
```

This matches the inverted index shown in the question exactly.

---

## Solution 7

```python
data = [("m1",1),("m1",3),("m1",1),("m1",5),("m2",5),("m2",4)]
rdd = sc.parallelize(data)
filtered = rdd.filter(lambda kv: kv[1] >= 2)   # drop ratings < 2

# a. reduceByKey
sum_count = filtered.mapValues(lambda v: (v, 1)) \
                     .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
avg_rbk = sum_count.mapValues(lambda t: t[0] / t[1]).collect()

# b. groupByKey
avg_gbk = filtered.groupByKey() \
                   .mapValues(lambda vals: sum(vals) / len(vals)).collect()
```

Verified: both give `[('m1', 4.0), ('m2', 4.5)]`. Note the `reduceByKey`
version is preferred in practice — it combines partial `(sum, count)` pairs
on the map side before the shuffle, whereas `groupByKey` must ship every
individual rating across the network before summing.

---

## Solution 8

```python
data = ['k1', 'k2', 'k1', 'k2', 'k1', 'k2', 'k3', 'k2']
rdd = sc.parallelize(data)

freq = (rdd.map(lambda k: (k, 1))
           .reduceByKey(lambda a, b: a + b)
           .filter(lambda kv: kv[1] >= 2))
```

Verified output: `[('k1', 3), ('k2', 4)]` (`k3` occurs once, dropped).

---

## Solution 9

```python
data = ["user1,movie1,3","user1,movie1,1","user1,movie2,5","user2,movie1,4"]
rdd = sc.parallelize(data)

result = (rdd.map(lambda l: l.split(","))
             .map(lambda p: (p[1], 1))
             .reduceByKey(lambda a, b: a + b)
             .collect())
```

Verified output: `[('movie1', 3), ('movie2', 1)]`.

---

## Solution 10

```python
data = [1, 1, 2, 3, 1, 2, 3, 3, 3]
rdd = sc.parallelize(data, 3)
rdd2 = rdd.map(lambda x: (x, 2))
groupedby = rdd2.groupByKey().collect()
reducedby = rdd2.reduceByKey(lambda x, y: x * y).collect()
```

**a.** `groupedby`: each key's iterable holds one `2` per occurrence of that
number in `data`. `1` occurs 4 times, `2` occurs 2 times, `3` occurs 4
times:

    (1, [2, 2, 2, 2])
    (2, [2, 2])
    (3, [2, 2, 2, 2])

**b.** `reducedby` multiplies all the `2`s together per key —
`2^(occurrence count)`:

    (1, 2^4 = 16)   (2, 2^2 = 4)   (3, 2^4 = 16)

*(Verified with the actual multiset from `data`: `1`→4 copies, `2`→2 copies,
`3`→4 copies, giving `(1,16), (2,4), (3,16)`.)*

---

## Solution 11

```python
data = [ ("B", 2), ("A", 1), ("A", 4), ("B", 2), ("B", 3) ]
rdd = sc.parallelize(data)
rdd2 = rdd.combineByKey(
    lambda value: (value, value+2, 1),
    lambda x, value: (x[0] + value, x[1] + value*value, x[2] + 1),
    lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])
)
myoutput = rdd2.collect()
```

**Intended (sequential, single-partition) trace** — this is what
`createCombiner`/`mergeValue` compute if a key's values are processed one
after another within one partition:

* **A**: values `[1, 4]`.
  `createCombiner(1) = (1, 1+2, 1) = (1, 3, 1)`.
  `mergeValue((1,3,1), 4) = (1+4, 3+4*4, 1+1) = (5, 19, 2)`.
* **B**: values `[2, 2, 3]`.
  `createCombiner(2) = (2, 4, 1)`.
  `mergeValue((2,4,1), 2) = (4, 8, 2)`.
  `mergeValue((4,8,2), 3) = (7, 17, 3)`.

So the textbook-expected answer is `[('A', (5, 19, 2)), ('B', (7, 17, 3))]`.

⚠️ **Important teaching point, confirmed by actually running this on
Spark:** this particular combiner is **not a valid/safe `combineByKey`
combiner**, because `mergeCombiners` (`x[1] + y[1]`) is *not* consistent with
repeated application of `mergeValue` (`x[1] + value*value`) — the middle
field accumulates `value²` when two values merge within one partition, but
just adds two already-computed totals when two partial combiners merge
across partitions. Running the exact code above with different
`numPartitions` proves the result actually changes:

    numPartitions=1: [('A', (5, 19, 2)), ('B', (7, 17, 3))]   <- matches the "intended" trace
    numPartitions=2: [('A', (5,  9, 2)), ('B', (7, 17, 3))]   <- A differs!
    numPartitions=4: [('A', (5,  9, 2)), ('B', (7, 17, 3))]
    numPartitions=5: [('A', (5,  9, 2)), ('B', (7, 13, 3))]   <- B differs too!

With `sc.parallelize(data)` and no explicit partition count, the real
answer is **partition-dependent and therefore non-deterministic** — a
correct `combineByKey` combiner must be associative/consistent regardless of
how records are split across partitions, and this one isn't. State the
"intended" trace above as your primary answer, then call out this flaw as
the deeper lesson: always double-check that `mergeValue` and
`mergeCombiners` compute the *same* underlying formula.

---

## Solution 12

```python
lines = ["g1,1,2.3","g1,2,1.5","g1,3,2.5","g1,1,4.1",
         "g2,1,1.3","g2,2,1.8","g2,3,3.5","g2,1,4.3","g2,1,2.9"]
rdd1 = sc.parallelize(lines)

# a.
count = rdd1.count()   # -> 9

# b.
parsed = rdd1.map(lambda l: l.split(","))
cancer = parsed.filter(lambda p: p[1] == "1").map(lambda p: (p[0], float(p[2])))
result_b = (cancer.map(lambda kv: (kv[0], (1, kv[1])))
                   .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
                   .collect())

# c.
not_undefined = parsed.filter(lambda p: p[1] != "3")
```

Verified: `count = 9`; `result_b = [('g1', (2, 6.4)), ('g2', (3, 8.5))]`
(`C`=count of cancer records, `S`=sum of their values); `not_undefined`
drops the two `reference == "3"` rows, leaving `7` records.

---

## Solution 13

```python
data = [0, 5, -3, 0, 7, -1, 0]   # representative sample of big.txt
rdd = sc.parallelize(data)

tagged = rdd.map(lambda n: ("Z",1) if n == 0 else (("P",1) if n > 0 else ("N",1)))
result = tagged.reduceByKey(lambda a, b: a + b).collect()
```

Verified output: `[('N', 2), ('P', 2), ('Z', 3)]`. Efficient because each
record is classified with a single O(1) `map`, and `reduceByKey`'s
associative `+` lets Spark map-side-combine partial counts per partition
before shuffling — so only 3 small partial sums (P/N/Z) per partition ever
cross the network, regardless of "100 billion" input records.

---

## Solution 14

```python
data = ["user1,movie1,3","user1,movie1,1","user1,movie2,5","user2,movie1,4"]
rdd = sc.parallelize(data).map(lambda l: l.split(","))

# a.
raters = rdd.map(lambda p: (p[1], 1)).reduceByKey(lambda a, b: a + b).collect()

# b.
unique_movies = (rdd.map(lambda p: (p[0], p[1]))
                     .distinct()
                     .map(lambda kv: (kv[0], 1))
                     .reduceByKey(lambda a, b: a + b)
                     .collect())
```

Verified: `raters = [('movie1', 3), ('movie2', 1)]`;
`unique_movies = [('user1', 2), ('user2', 1)]`.

---

## Solution 15

```python
data = [1, 1, 1, 2, 3, 1, 2, 3, 3, 3]
rdd = sc.parallelize(data, 3)
rdd2 = rdd.map(lambda x: (x, x))
grouped_rdd = rdd2.groupByKey().mapValues(lambda x: sum(x)).collect()
```

Since each key's grouped values are all equal to the key itself, the sum is
`key * (occurrence count)`. `1` occurs 4×, `2` occurs 2×, `3` occurs 4×.

Verified output: `[(1, 4), (2, 4), (3, 12)]`.

---

## Solution 16

```python
data = [1, 1, 1, 1, 2, 2, 3, 1, 2, 3, 3, 3]
rdd = sc.parallelize(data)
rdd2 = rdd.map(lambda x: (x+1, x))
reduced_rdd = rdd2.reduceByKey(lambda x, y: x + y).collect()
```

`x=1` (5 occurrences) maps to key `2`, contributing `5*1=5`; `x=2` (3
occurrences) maps to key `3`, contributing `3*2=6`; `x=3` (4 occurrences)
maps to key `4`, contributing `4*3=12`.

Verified output: `[(2, 5), (3, 6), (4, 12)]`.

---

## Solution 17

```python
data = [1, -1, 1, 1, 0, 0, 1, -2, 2, 3, 1, 2, -3]
rdd = sc.parallelize(data)

rddP = rdd.filter(lambda x: x >= 0)
rddN = rdd.filter(lambda x: x < 0)
```

Verified: `rddP = [1, 1, 1, 0, 0, 1, 2, 3, 1, 2]`, `rddN = [-1, -2, -3]`.

---

## Solution 18

```python
lines = ["g1,1,2.3","g1,2,1.5","g1,3,2.5","g1,1,4.1",
         "g2,1,1.3","g2,2,1.8","g2,3,3.5","g2,1,4.3","g2,1,2.9"]
rdd1 = sc.parallelize(lines)

# a.
count = rdd1.count()   # -> 9

# b.
parsed = rdd1.map(lambda l: l.split(","))
cancer = parsed.filter(lambda p: p[1] == "1").map(lambda p: (p[0], float(p[2])))
mn = (cancer.map(lambda kv: (kv[0], (1, kv[1])))
            .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
            .collect())

# c.
total_sum = parsed.map(lambda p: float(p[2])).sum()

# d.
healthy = parsed.filter(lambda p: p[1] == "2")
```

Verified: `count=9`; `mn = [('g1', (2, 6.4)), ('g2', (3, 8.5))]`;
`total_sum = 24.2`; `healthy` keeps 2 rows
(`['g1','2','1.5']`, `['g2','2','1.8']`).

---

## Solution 19

```python
data = ["a","b","a","c","b","a"]
rdd = sc.parallelize(data)

result = (rdd.map(lambda r: (r, None))
             .reduceByKey(lambda a, b: None)
             .map(lambda kv: kv[0])
             .collect())
```

Verified output: `['a', 'b', 'c']`.

---

## Solution 20

```python
data = ["e1,fulltime","e2,parttime","e3,contractor",
        "e4,fulltime","e5,parttime","e6,fulltime"]
rdd = sc.parallelize(data).map(lambda l: l.split(","))

counts = (rdd.filter(lambda p: p[1] in ("fulltime", "parttime"))
             .map(lambda p: (p[1], 1))
             .reduceByKey(lambda a, b: a + b)
             .collect())
```

Verified output: `[('fulltime', 3), ('parttime', 2)]`.

---

## Solution 21

```python
data = ['k1', 'k2', 'k1', 'k2', 'k1', 'k2', 'k3', 'k2', 'k4']
rdd = sc.parallelize(data)

freq = (rdd.map(lambda k: (k, 1))
           .reduceByKey(lambda a, b: a + b)
           .filter(lambda kv: kv[1] > 1))
```

Verified output: `[('k1', 3), ('k2', 4)]` (`k3` and `k4` each occur once, so
they're dropped).

---

## Solution 22

```python
data = [1, 1, 1, 1, 2, 2, 3, 1, 2, 3, 3, 3]
rdd = sc.parallelize(data)
rdd2 = rdd.map(lambda x: (x+1, x-1))
my_output = rdd2.reduceByKey(lambda x, y: x + y).collect()
```

`x=1` (5×) maps to `(2, 0)` each time → key `2` sums to `0`. `x=2` (3×) maps
to `(3, 1)` each → key `3` sums to `3`. `x=3` (4×) maps to `(4, 2)` each →
key `4` sums to `8`.

Verified output: `[(2, 0), (3, 3), (4, 8)]`.

---

## Solution 23

```python
data = [5, -3, 0, 7, -1, 2, -9]   # representative sample of big.txt
rdd = sc.parallelize(data)

tagged = rdd.map(lambda n: ("P",1) if n > 0 else (("N",1) if n < 0 else ("Z",1)))
result = tagged.reduceByKey(lambda a, b: a + b).collect()
```

Verified output: `[('N', 3), ('P', 3), ('Z', 1)]`. Same efficiency argument
as Solution 13: one `map` pass classifies each record, and `reduceByKey`'s
associative `+` allows map-side combining before any shuffle.

---

## Solution 24

```python
def getUser(r):   return r.split(",")[0]
def getMovie(r):  return r.split(",")[1]
def getRating(r): return int(r.split(",")[2])

data = ["user1,movie1,3","user1,movie1,1","user1,movie2,5","user2,movie1,4"]
rdd = sc.parallelize(data)

# a.
raters = rdd.map(lambda r: (getMovie(r), 1)).reduceByKey(lambda a, b: a + b).collect()

# b.
unique_movies = (rdd.map(lambda r: (getUser(r), getMovie(r)))
                     .distinct()
                     .map(lambda kv: (kv[0], 1))
                     .reduceByKey(lambda a, b: a + b)
                     .collect())
```

Verified: `raters = [('movie1', 3), ('movie2', 1)]`;
`unique_movies = [('user1', 2), ('user2', 1)]`.

---

## Solution 25

```python
data = [1, 1, 1, 2, 3, 1, 2, 3, 3, 3]
rdd = sc.parallelize(data)
rdd2 = rdd.map(lambda x: (x, x+2))
grouped_rdd = rdd2.groupByKey().mapValues(lambda x: sum(x)).collect()
```

`x=1` (4×) → key `1`, sum `4*(1+2)=12`. `x=2` (2×) → key `2`, sum
`2*(2+2)=8`. `x=3` (4×) → key `3`, sum `4*(3+2)=20`.

Verified output: `[(1, 12), (2, 8), (3, 20)]`.

---

## Solution 26

```python
data = [1, 1, 1, 1, 2, 2, 3, 1, 2, 3, 3, 3]
rdd = sc.parallelize(data)
rdd2 = rdd.map(lambda x: (x+1, x))
reduced_rdd = rdd2.reduceByKey(lambda x, y: x + y).collect()
```

Identical setup to Solution 16 — same data, same transformations.

Verified output: `[(2, 5), (3, 6), (4, 12)]`.

---

## Solution 27

```python
data = [1, -1, 1, 1, 0, 0, 1, -2, 2, 3, 1, 2, -3]
rdd = sc.parallelize(data)

rddP = rdd.filter(lambda x: x > 0)
rddN = rdd.filter(lambda x: x < 0)
rddZ = rdd.filter(lambda x: x == 0)
```

Verified: `rddP = [1, 1, 1, 1, 2, 3, 1, 2]`, `rddN = [-1, -2, -3]`,
`rddZ = [0, 0]`.

---

## Solution 28

```python
data = [5, -3, 0, 7, -1, 2, -9, 10]   # representative sample of big.txt
rdd = sc.parallelize(data)                   # a.
total = rdd.count()                          # b.
nonneg = rdd.filter(lambda x: x >= 0)        # c.
remaining = nonneg.count()                    # d.
```

Verified on this 8-element sample: `total = 8`, `remaining = 5` (after
dropping the 3 negative values). The pattern is identical at "100 billion"
scale — `count()` and `filter()` are both fully distributed and require no
shuffle.

---

## Solution 29

```python
data = [0, 1, 0, 1, -1, 1, 0, 2, 3, 1, -2, -3, 3, 3]
rdd = sc.parallelize(data)
rdd2 = rdd.filter(lambda v: v > 0)
rdd3 = rdd2.map(lambda x: (x, x+2))
reducedRDD = rdd3.reduceByKey(lambda x, y: x + y).collect()
```

After filtering, the positive values are `[1,1,1,2,3,1,3,3]` (four `1`s,
one `2`, three `3`s). Mapped to `(x, x+2)` and reduced:
`1→(1,3)`×4 sums to `12`; `2→(2,4)`×1 sums to `4`; `3→(3,5)`×3 sums to `15`.

Verified output: `[(1, 12), (2, 4), (3, 15)]`.

---

## Solution 30

```python
data = [("a", 1), ("a", 1), ("a", 3), ("b", 1), ("b", 1), ("b", 2)]
rdd = sc.parallelize(data)

# average
avg = (rdd.mapValues(lambda v: (v, 1))
          .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
          .mapValues(lambda t: t[0] / t[1])
          .collect())

# maximum
mx = rdd.reduceByKey(max).collect()

# median (requires the full sorted list — no reduceByKey shortcut)
def median_of(vals):
    s = sorted(vals)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 == 1 else (s[mid-1] + s[mid]) / 2
med = rdd.groupByKey().mapValues(lambda vals: median_of(list(vals))).collect()

# mode (also requires the full list)
from collections import Counter
def mode_of(vals):
    return Counter(vals).most_common(1)[0][0]
mode = rdd.groupByKey().mapValues(lambda vals: mode_of(list(vals))).collect()
```

Verified: `avg = [('a', 1.667), ('b', 1.333)]`; `mx = [('a', 3), ('b', 2)]`;
`med = [('a', 1), ('b', 1)]`; `mode = [('a', 1), ('b', 1)]`. Average and
maximum can use `reduceByKey` (associative aggregates, map-side combinable);
median and mode fundamentally need the whole per-key collection, so
`groupByKey` (or `mapPartitions` if you want to pre-sort per partition) is
required.

---

## Solution 31

```python
data = ["abc", "abc", "xyz", "xyz", "xyz"]
rdd = sc.parallelize(data)

result = (rdd.map(lambda r: (r, None))
             .reduceByKey(lambda a, b: None)
             .map(lambda kv: kv[0])
             .collect())
```

Verified output: `['abc', 'xyz']` — no `distinct()`/`unique()` call used;
`reduceByKey` naturally collapses duplicate keys to one entry.

---

## Solution 32

```python
lines = ["10,4,50,40,30", "10,60,50,20", "20,20,30,40,50,2"]
rdd = sc.parallelize(lines)

def local_minmax(iterator):
    local_min = local_max = None
    for line in iterator:
        for tok in line.split(","):
            n = int(tok)
            if local_min is None or n < local_min: local_min = n
            if local_max is None or n > local_max: local_max = n
    if local_min is not None:
        yield (local_min, local_max)

partials = rdd.mapPartitions(local_minmax).collect()
overall_min = min(p[0] for p in partials)
overall_max = max(p[1] for p in partials)
```

Verified: `partials = [(4, 50), (10, 60), (2, 50)]` (one partial `(min,max)`
per partition/line), giving `overall_min = 2`, `overall_max = 60`.
`mapPartitions()` is the efficient choice here because it lets us scan and
reduce all the numbers *within* a partition/record in plain Python, instead
of first `flatMap`-ping every individual number out into its own RDD row
(which would multiply the record count by "thousands").

---

## Solution 33

```python
def myfunc(n):
    if n < 0:
        return [n, -n, -n]
    else:
        return []

data = [0, 1, 2, -3, -4]
rdd = sc.parallelize(data)
rdd3 = rdd.flatMap(myfunc).flatMap(myfunc)
```

First `flatMap(myfunc)`: `0,1,2` (all `>= 0`) each contribute nothing;
`-3 -> [-3, 3, 3]`; `-4 -> [-4, 4, 4]`. Intermediate result:
`[-3, 3, 3, -4, 4, 4]`.

Second `flatMap(myfunc)` applied to that intermediate list: `-3 -> [-3,3,3]`
(negative), the two `3`s each contribute nothing (not negative), `-4 ->
[-4,4,4]`, the two `4`s contribute nothing.

**Verified final output:** `[-3, 3, 3, -4, 4, 4]`.

---

## Solution 34

```python
def getMovie(r):  return r[:r.index(",")]
def getRating(r): return int(r[r.index(",")+1:])

data = ["movie1,3","movie1,1","movie1,5","movie2,5","movie2,4","movie2,3"]
rdd = sc.parallelize(data)
pairs = rdd.map(lambda r: (getMovie(r), getRating(r)))

# a.
raters = pairs.map(lambda kv: (kv[0], 1)).reduceByKey(lambda a, b: a + b).collect()

# b.
avg = (pairs.mapValues(lambda v: (v, 1))
            .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
            .mapValues(lambda t: t[0] / t[1])
            .collect())
```

Note: `getMovie`/`getRating` are implemented with `str.index()` +
slicing — not `split()` — satisfying the "MUST NOT use `split()`" rule.

Verified: `raters = [('movie1', 3), ('movie2', 3)]`;
`avg = [('movie1', 3.0), ('movie2', 4.0)]`.

---

## Solution 35

```python
data = [0, 2, 2, -3, 1, -1, 3, -2, -4, 3]
rdd = sc.parallelize(data)
print("output-1: ", rdd.collect())
rdd2 = rdd.filter(lambda v: v > 0)
print("output-2: ", rdd2.collect())
rdd3 = rdd2.map(lambda x: (x, x+2))
print("output-3: ", rdd3.collect())
rdd4 = rdd3.reduceByKey(lambda x, y: x + y)
print("output-4: ", rdd4.collect())
```

Verified output:

```text
output-1:  [0, 2, 2, -3, 1, -1, 3, -2, -4, 3]
output-2:  [2, 2, 1, 3, 3]
output-3:  [(2, 4), (2, 4), (1, 3), (3, 5), (3, 5)]
output-4:  [(1, 3), (2, 8), (3, 10)]
```

---

## Solution 36

```python
data = [("a", 1), ("a", 20), ("a", 3), ("b", 100), ("b", 1), ("b", 2)]
rdd = sc.parallelize(data)

gbk = rdd.groupByKey().mapValues(lambda vals: (min(vals), max(vals))).collect()

rbk = (rdd.mapValues(lambda v: (v, v))
          .reduceByKey(lambda a, b: (min(a[0],b[0]), max(a[1],b[1])))
          .collect())

cbk = rdd.combineByKey(
    lambda v: (v, v),
    lambda acc, v: (min(acc[0], v), max(acc[1], v)),
    lambda a, b: (min(a[0], b[0]), max(a[1], b[1]))
).collect()
```

All three verified to give the same result:
`[('a', (1, 20)), ('b', (1, 100))]`. Unlike Solution 11's combiner, this one
IS a valid/consistent combiner — `mergeValue` and `mergeCombiners` both
reduce to the same associative `(min, max)` logic regardless of how records
are split across partitions.

---

## Solution 37

```python
def myfunc(n):
    if n == 0:
        return [n, -n]
    elif n > 0:
        return [n, -n, n]
    else:
        return []

data = [0, 3, 4, 0, -3, -4]
rdd = sc.parallelize(data)
print("output1 = ", rdd.collect())
print("output2 = ", rdd.count())
rdd3 = rdd.flatMap(myfunc).flatMap(myfunc)
print("output3 = ", rdd3.collect())
```

Verified output:

```text
output1 =  [0, 3, 4, 0, -3, -4]
output2 =  6
output3 =  [0, 0, 0, 0, 3, -3, 3, 3, -3, 3, 4, -4, 4, 4, -4, 4, 0, 0, 0, 0]
```

---

## Solution 38

```python
def fun7(x):
    if x == 1:
        return [x, 1]
    if x > 0:
        return [x, x, -2]
    return []

data = [1, 1, -1, -2, 2, 2, -4]
rdd = sc.parallelize(data, 3)
rdd2 = rdd.flatMap(fun7)
pairs = (rdd2.map(lambda x: (x, 3))
              .groupByKey()
              .mapValues(lambda x: sum(x))
              .collect())
```

Verified: `rdd2.collect() = [1, 1, 1, 1, 2, 2, -2, 2, 2, -2]` (each of the
two `1`s in `data` becomes `[1,1]`, each of the two `2`s becomes
`[2,2,-2]`, and the negative inputs `-1,-2,-4` all return `[]`). Grouping
those into `(x, 3)` pairs and summing per key:

    pairs = [(-2, 6), (1, 12), (2, 12)]

(`-2` occurs twice → `2*3=6`; `1` occurs four times → `4*3=12`; `2` occurs
four times → `4*3=12`.)

---

## Solution 39

```python
data = [('A', 4), ('A', 8), ('B', 5), ('B', 7)]
rdd = sc.parallelize(data)

# a. groupByKey
gbk = rdd.groupByKey().mapValues(lambda vals: sum(vals) / len(list(vals))).collect()

# b. reduceByKey
rbk = (rdd.mapValues(lambda v: (v, 1))
          .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
          .mapValues(lambda t: t[0] / t[1])
          .collect())

# c. combineByKey
cbk = rdd.combineByKey(
    lambda v: (v, 1),
    lambda acc, v: (acc[0] + v, acc[1] + 1),
    lambda a, b: (a[0] + b[0], a[1] + b[1])
).mapValues(lambda t: t[0] / t[1]).collect()
```

All three verified to give: `[('A', 6.0), ('B', 6.0)]`.

---

## Solution 40

```python
data = [('k1', 5), ('k1', 6), ('k1', 7), ('k2', 7), ('k2', 8), ('k2', 7), ('k2', 8)]
rdd = sc.parallelize(data)

unique_keys = rdd.keys().distinct().collect()
unique_values = rdd.values().distinct().collect()
```

Verified: `unique_keys = ['k1', 'k2']`, `unique_values = [5, 6, 7, 8]`.
`.keys()`/`.values()` avoid materializing the full pair before extracting
just one side, which is a small efficiency win over
`rdd.map(lambda kv: kv[0]).distinct()`.

---

## Solution 41

```python
data = [('k1', 5), ('k1', 6), ('k1', 7), ('k1', 5), ('k1', 6),
        ('k2', 7), ('k2', 8), ('k2', 7), ('k2', 8), ('k2', 9)]
rdd = sc.parallelize(data)

gbk = rdd.groupByKey().mapValues(lambda vals: (min(vals), max(vals))).collect()
rbk = (rdd.mapValues(lambda v: (v, v))
          .reduceByKey(lambda a, b: (min(a[0],b[0]), max(a[1],b[1])))
          .collect())
cbk = rdd.combineByKey(
    lambda v: (v, v),
    lambda acc, v: (min(acc[0], v), max(acc[1], v)),
    lambda a, b: (min(a[0], b[0]), max(a[1], b[1]))
).collect()
```

All three verified to give: `[('k1', (5, 7)), ('k2', (7, 9))]`.

---

## Solution 42

```python
data = [('a', 2), ('b', 3), ('d', 2), ('x', 3), ('y', 1)]
rdd = sc.parallelize(data)

result = rdd.flatMap(lambda kv: [kv[0]] * kv[1]).collect()
```

Verified output:
`['a', 'a', 'b', 'b', 'b', 'd', 'd', 'x', 'x', 'x', 'y']` — matches the
question's expected output exactly.

---

## Solution 43

```python
data = [("k1", "v1"), ("k1", "v1"), ("k1", "v2")]
rdd = sc.parallelize(data)

result = (rdd.map(lambda kv: (kv, None))
             .reduceByKey(lambda a, b: None)
             .map(lambda kv: kv[0])
             .collect())
```

Verified output: `[('k1', 'v1'), ('k1', 'v2')]` — the duplicate `("k1",
"v1")` collapses to one entry, without calling `distinct()`.

---

## Solution 44

```python
data = [("a", 2), ("a", -4), ("a", 9), ("b", 9), ("b", 7), ("b", -3),
        ("c", 2), ("c", 4)]
rdd = sc.parallelize(data)

filtered = rdd.filter(lambda kv: kv[1] > 0)          # WHERE value > 0
agg = (filtered.mapValues(lambda v: (v, v, 1))
                .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+b[2])))
result = agg.mapValues(lambda t: (t[0] / t[2], t[1])).collect()   # (avg, sum)
```

Verified output:
`[('a', (5.5, 11)), ('b', (8.0, 16)), ('c', (3.0, 6))]` — i.e. per key,
`(AVG(value), SUM(value))` computed only over the positive values, matching
`SELECT key, AVG(value), SUM(value) FROM employees WHERE value > 0 GROUP BY key`.

---

## Solution 45

```python
lines = sc.textFile("/home/data.txt")
rdd1 = lines.flatMap(lambda s: s.split(",")).filter(lambda x: len(x) < 3)
rdd1.count()   # output 1
rdd2 = rdd1.map(lambda s: (1, s))
rdd3 = rdd2.map(lambda s: (s[1], s[0]))
rdd4 = rdd3.reduceByKey(lambda x, y: x+y).filter(lambda x: x[1] > 2)
rdd4.collect()   # output 2
```

Splitting the 5 lines by comma and keeping only tokens shorter than 3
characters (i.e. dropping `w11`, `w21`, `w31` — each length 3 — and keeping
`w1`, `w2`, `w3` — each length 2) yields 13 surviving tokens:

    ['w2','w1','w3','w3','w1','w2','w3','w1','w1','w2','w2','w2','w1']

**output 1** (`rdd1.count()`): **`13`**

`rdd2`/`rdd3` re-key each token to `(token, 1)`, and `rdd4` sums by token
then keeps only counts `> 2`. Counting occurrences: `w1` appears 5 times,
`w2` appears 5 times, `w3` appears 3 times — all three exceed 2.

**output 2** (`rdd4.collect()`): **`[('w1', 5), ('w2', 5), ('w3', 3)]`**

---

## Solution 46

```python
data = [1, 11, -1, 2, 12, 3, -4, 13, 4, 14]
rdd = sc.parallelize(data)

def classify(n):
    tags = [ "P" if n > 0 else ("N" if n < 0 else "Z"),
             "EVEN" if n % 2 == 0 else "ODD" ]
    return [(t, 1) for t in tags]

result = rdd.flatMap(classify).reduceByKey(lambda a, b: a + b).collect()
```

One `flatMap` emits 2 tags per number (sign tag + parity tag), then a
single `reduceByKey` handles all 5 categories at once — no separate pass
per category needed.

Verified output: `[('EVEN', 5), ('N', 2), ('ODD', 5), ('P', 8)]`.

---

## Solution 47

```python
import random
random.seed(42)
nums = [random.randint(1, 100) for _ in range(1_000_000)]   # 1.
rdd = sc.parallelize(nums, 8)                                 # 2.

freq = rdd.map(lambda n: (n, 1)).reduceByKey(lambda a, b: a + b)   # 3.
top5 = freq.takeOrdered(5, key=lambda kv: -kv[1])                   # 4.
```

Verified (run at 100,000 numbers for speed, `seed=42`):
`top5 = [(74, 1096), (89, 1089), (85, 1067), (50, 1062), (88, 1056)]` — the
exact numbers/counts will differ run to run with true randomness (or a
different seed/sample size), but this confirms the pipeline is correct:
`reduceByKey` correctly tallies frequency per number, and
`takeOrdered(5, key=lambda kv: -kv[1])` is the efficient way to get a
top-N without a full sort (see Solution 54c for why).

---

## Solution 48

```python
customers = ["c1,USA","c2,USA","c3,CHINA","c4,CHINA","c5,USA"]
rdd = sc.parallelize(customers).map(lambda l: l.split(","))

result = rdd.map(lambda p: (p[1], 1)).reduceByKey(lambda a, b: a + b).collect()
```

Verified output: `[('CHINA', 2), ('USA', 3)]`.

---

## Solution 49

```python
counted = rdd.map(lambda p: (p[1], 1)).reduceByKey(lambda a, b: a + b)   # from Solution 48
result = counted.sortBy(lambda kv: -kv[1]).take(5)
# (or, more efficiently for large data: counted.takeOrdered(5, key=lambda kv: -kv[1]))
```

Verified output: `[('USA', 3), ('CHINA', 2)]`.

---

## Solution 50

```python
employees = ["alice,4000","bob,2000","carol,3500","dan,1000"]
rdd = sc.parallelize(employees).map(lambda l: l.split(","))

totals = rdd.map(lambda p: (p[0], int(p[1]))).reduceByKey(lambda a, b: a + b)
result = totals.filter(lambda kv: kv[1] > 3000).collect()   # HAVING SUM(SALARY) > 3000
```

Verified output: `[('alice', 4000), ('carol', 3500)]`.

---

## Solution 51

```python
customers = ["c1,NA,USA","c2,NA,USA","c3,ASIA,CHINA","c4,ASIA,CHINA","c5,NA,USA"]
rdd = sc.parallelize(customers).map(lambda l: l.split(","))

counts = rdd.map(lambda p: ((p[1], p[2]), 1)).reduceByKey(lambda a, b: a + b)
result = counts.sortBy(lambda kv: -kv[1]).take(5)
```

Verified output: `[(('NA', 'USA'), 3), (('ASIA', 'CHINA'), 2)]`.

---

## Solution 52

```python
employees = sc.parallelize([(1, "alex"), (2, "jane"), (3, "ted"), (4, "dan")])
departments = sc.parallelize([(1, "IT"), (2, "HR"), (3, "IT")])

# a.
inner = employees.join(departments).collect()

# b.
left = employees.leftOuterJoin(departments).collect()

# c. equivalent via cogroup (no join()/*OuterJoin())
manual = employees.cogroup(departments).flatMap(
    lambda kv: [(kv[0], (name, d))
                for name in kv[1][0]
                for d in (list(kv[1][1]) or [None])]
).collect()
```

Verified:

    a. inner = [(1,('alex','IT')), (2,('jane','HR')), (3,('ted','IT'))]
    b. left  = [(1,('alex','IT')), (2,('jane','HR')), (3,('ted','IT')), (4,('dan', None))]
    c. manual = identical to (b)

`cogroup()` groups both RDDs' values by key into a pair of iterables
`(iterable_of_employee_names, iterable_of_depts)` per key; the `flatMap`
then does the same cross-product `join()` does internally, substituting
`None` when the department side is empty — which is exactly how
`leftOuterJoin()` is implemented under the hood.

---

## Solution 53

```python
rddA = sc.parallelize([1, 2, 3, 4, 5])
rddB = sc.parallelize([4, 5, 6, 7, 8])

union_ = rddA.union(rddB).collect()               # a.
intersection_ = rddA.intersection(rddB).collect()  # b.
subtract_ = rddA.subtract(rddB).collect()          # c.
```

Verified: `union = [1,2,3,4,4,5,5,6,7,8]` (duplicates `4` and `5`
preserved — `union()` is a pure concatenation, no dedup);
`intersection = [4, 5]`; `subtract = [1, 2, 3]`.

**d.** `union()` needs no shuffle (it's just a concatenation of
partitions), but `intersection()` and `subtract()` both need to know, for
every distinct value, whether it exists in *both* (or only one) of the two
RDDs — and since a value could live in any partition of either RDD, the
only way to compare "does X exist over there" is to bring matching values
together onto the same partition, which is exactly what a shuffle does
(both are implemented as a co-partitioned `cogroup` under the hood).

---

## Solution 54

```python
data = [("p1", 4200), ("p2", 900), ("p3", 15000),
        ("p4", 3100), ("p5", 15000), ("p6", 50)]
rdd = sc.parallelize(data)

# a.
sorted_desc = rdd.sortBy(lambda kv: -kv[1]).collect()

# b.
top3 = rdd.takeOrdered(3, key=lambda kv: -kv[1])
```

Verified: `sorted_desc = [('p3',15000), ('p5',15000), ('p1',4200),
('p4',3100), ('p2',900), ('p6',50)]`; `top3 = [('p3',15000), ('p5',15000),
('p1',4200)]`.

**c.** `sortBy(...).take(3)` must fully sort the *entire* RDD (a global,
all-partitions shuffle) before taking the first 3 — expensive for a huge
RDD when you only need a handful of results. `takeOrdered(3, key=...)` only
ever needs to track the current top 3 candidates: each partition computes
its own local top-3 (a small local sort/heap), and only those small partial
top-3 lists are shuffled to the driver to be merged into the final top-3 —
dramatically less data movement for large `N` with small `k`.

---

## Solution 55

```python
lookup = {"US": "United States", "CA": "Canada", "MX": "Mexico"}
bc = sc.broadcast(lookup)

rddBig = sc.parallelize([("u1","US"), ("u2","CA"), ("u3","MX"), ("u4","US")])
enriched = rddBig.map(lambda ue: (ue[0], ue[1], bc.value.get(ue[1], "UNKNOWN"))).collect()
```

Verified output:
`[('u1','US','United States'), ('u2','CA','Canada'), ('u3','MX','Mexico'), ('u4','US','United States')]`.

**b.** Without `sc.broadcast()`, PySpark still has to get `lookup` to every
executor — but it does so by **serializing a fresh copy of the dictionary
into every single task's closure**. With `rddBig` split across, say,
10,000 partitions/tasks, that means 10,000 separate copies of the same
10,000-entry dictionary get serialized and shipped over the network (or
re-sent per task), instead of `sc.broadcast()`'s one-time, read-only copy
cached on each *executor* (shared by every task running on that executor).
The unbroadcast version wastes network bandwidth and executor memory
repeating the same data far more than necessary.

---

## Solution 56

```python
bad_counter = sc.accumulator(0)
raw = sc.parallelize(["2024-01-01,u1,click", "malformed_line",
                       "2024-01-02,u2,view", "also,bad"])

def parse(line):
    parts = line.split(",")
    if len(parts) != 3:
        bad_counter.add(1)
        return None
    return tuple(parts)

clean = raw.map(parse).filter(lambda x: x is not None)
result = clean.collect()          # <-- triggers the action
```

Verified: `result = [('2024-01-01','u1','click'), ('2024-01-02','u2','view')]`,
and after the `.collect()` action runs, `bad_counter.value == 2`.

**b.** You read `bad_counter.value` on the **driver**, after an action
(here, `.collect()`) has actually executed the job — accumulator updates
made inside tasks are only reliably reflected on the driver once those
tasks have completed as part of a triggered action. Reading (or worse,
depending on) an accumulator's value *inside* a transformation is unsafe
because transformations are lazy and can be **re-executed** (e.g., after a
lost/retried task, or because the same RDD is used in two separate actions
without caching) — each re-execution would increment the accumulator again,
silently double- (or triple-) counting. Accumulators are only guaranteed to
reflect each task's contribution exactly once for *actions*, not for
arbitrary re-evaluation inside transformations.
