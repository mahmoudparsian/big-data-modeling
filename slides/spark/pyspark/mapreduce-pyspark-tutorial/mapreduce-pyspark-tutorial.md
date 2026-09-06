---
marp: true
theme: default
paginate: true
header: "Big Data Modeling — MapReduce with PySpark"
footer: "MSIS · Tutorial 1"
style: |
  section { font-size: 26px; }
  h1 { color: #1f4e79; }
  h2 { color: #1f4e79; }
  code { font-size: 0.85em; }
  pre { font-size: 0.78em; line-height: 1.25; }
  .small { font-size: 0.85em; }
  .muted { color: #6b7280; }
  .key   { color: #b8860b; font-weight: bold; }
  .val   { color: #166534; font-weight: bold; }
---

# MapReduce with PySpark
### A hands-on tutorial 
### From "word count" to retail analytics


---

## Where this tutorial fits

This is **Tutorial 1**.

Later in the course we'll build a full **medallion data architecture** (Bronze / Silver / Gold) on the same retail dataset you'll see today. Today is the foundation:

- The **mental model** of MapReduce: thinking in (key, value) pairs.
- The **PySpark RDD API** — where MapReduce lives most naturally.
- Concrete patterns you'll reuse for the rest of the course.

We'll close with how the same ideas appear in modern DataFrames.

> A companion Jupyter notebook (`mapreduce-pyspark-notebook.ipynb`) mirrors every example on these slides — self-contained, no external files needed. New to PySpark itself? Start with [Checking Your PySpark Installation](../../installation/README.md).

---

## What you'll be able to do by the end

1. Explain the three phases of MapReduce — and why the middle one matters most.
2. Read an RDD pipeline and predict its output without running it.
3. Translate a business question into a chain of `map`, `filter`, `reduceByKey`, `join`.
4. Spot the difference between `groupByKey` and `reduceByKey` — and pick the right one.
5. Describe how Spark DataFrames are MapReduce in disguise.

---

## The scaling problem

> Your laptop processes 1 GB of CSV in 8 seconds. Easy.
>
> Your company has 100 TB of order data and the CFO wants a report by Monday.

Three options:

- **Buy a bigger laptop** — works until it doesn't (and gets expensive fast).
- **Rewrite everything in C++** — buys you ~10×, then you hit the same wall.
- **Use many ordinary computers in parallel** — the only path that scales linearly.

MapReduce is the *programming model* that makes the third option practical.

---

## What goes wrong when you split work across machines

Imagine 100 laptops doing one job together:

| Problem | Why it's hard |
|---|---|
| **Failures** | At any moment, ~one machine is broken or restarting. |
| **Coordination** | Who works on what? Who has the answer? |
| **Data movement** | Sending a TB across the network is slow. |
| **Stragglers** | One slow worker can hold up the whole job. |
| **Determinism** | Re-running shouldn't give a different number. |

A good distributed framework solves these so you don't have to.

---

## Enter MapReduce

MapReduce (Google, 2004 / Hadoop, 2006) gave us:

- A **simple model**: every job is map → shuffle → reduce.
- An **execution engine** that hides failures and coordination.
- A **storage assumption**: the data is already split into chunks across many machines.

You write *what* the computation is. The framework figures out *where* and *how*.

> Spark (2010) kept the model, made it ~100× faster by using memory and adding a richer set of operators on top.

---

## The MapReduce model in one picture

```
   input chunks         map outputs         shuffled by key       reduced
  ┌──────────┐        ┌──────────────┐     ┌──────────────┐     ┌────────┐
  │ chunk 1  │──map──▶│ (k1, v), ... │──┐  │ k1: [v,v,v]  │──┐  │ k1: r1 │
  │ chunk 2  │──map──▶│ (k2, v), ... │──┼─▶│ k2: [v,v]    │──┼─▶│ k2: r2 │
  │ chunk 3  │──map──▶│ (k3, v), ... │──┘  │ k3: [v]      │──┘  │ k3: r3 │
  └──────────┘        └──────────────┘     └──────────────┘     └────────┘
       parallel             parallel        network shuffle      parallel
```

**Three phases.** Two are embarrassingly parallel. The middle one moves data across the network — and that's where most performance issues live.

---

## Phase 1 — Map

> Take one record. Emit zero or more (key, value) pairs.

Properties:

- **Stateless** — same input always produces the same output.
- **Independent** — every record is processed on its own.
- **Parallel** — N machines can map N chunks simultaneously, no coordination needed.

```python
def map_word(line):
    for word in line.split():
        yield (word, 1)
```

Map decides **what to emit and how to key it**. Almost every business question is a "what do I emit" question in disguise.

---

## Phase 2 — Shuffle

The shuffle is the framework's job, not yours. It takes all the `(key, value)` pairs from every mapper and **groups them by key across the network**.

```
machine A emits: (cat, 1), (dog, 1), (cat, 1)
machine B emits: (dog, 1), (cat, 1)
machine C emits: (bird, 1), (cat, 1)

         ↓  shuffle by key  ↓

machine X gets: ("cat",  [1,1,1,1])
machine Y gets: ("dog",  [1,1])
machine Z gets: ("bird", [1])
```

This phase is **expensive** — every (key, value) potentially crosses the network. Most Spark optimization is "shuffle less."

---

## Phase 3 — Reduce

> For each key, take its list of values and produce one combined result.

```python
def reduce_count(values):
    return sum(values)
```

Properties of a good reducer:

- **Associative**: `(a + b) + c == a + (b + c)`
- **Commutative**: `a + b == b + a`

When both hold, the framework can **partial-reduce** in parallel and combine at the end. (Sum, count, min, max, distinct-count via HLL — all good. Median — bad.)

---

## The contract: pure functions

MapReduce assumes your map and reduce functions are **pure**:

- Same input → same output, every time.
- No side effects (no writing files, no incrementing globals, no calling APIs).
- No reliance on what *other* records did.

Why? The framework reserves the right to:

- Run your code on any machine.
- Re-run it after a failure.
- Run multiple copies in parallel ("speculative execution").

Side effects break all of these guarantees silently.

---

## Mental model: think in (key, value) pairs

Almost every analytical question can be phrased as:

1. **What's the key?** (the thing I'm grouping by)
2. **What's the value?** (the thing I'm summarizing)
3. **What's the reduce?** (the summary operation)

| Question | Key | Value | Reduce |
|---|---|---|---|
| Word frequency | the word | 1 | sum |
| Revenue by category | category | line_total | sum |
| Top customer | customer_id | order_total | sum, then top-N |
| Active users per day | day | user_id | distinct count |

Once you frame the problem this way, the code writes itself.

---

## From Hadoop MapReduce to Spark

Original Hadoop MR:

- Map output → write to disk → shuffle → reducer reads from disk.
- One map + one reduce per job. Multi-step pipelines = many jobs.
- Slow for iterative algorithms.

Apache Spark:

- Keeps intermediate data **in memory** when it fits.
- Lets you chain dozens of operations in **one job**.
- Adds richer operators: `filter`, `join`, `groupByKey`, `reduceByKey`, `flatMap`, ...
- Same MapReduce shape underneath.

For this tutorial: we use Spark via **PySpark**, the Python API.

---

## The RDD — Resilient Distributed Dataset

An **RDD** is Spark's classical data abstraction:

- A **collection** of records …
- … **distributed** across many machines (partitions) …
- … **immutable** (transformations make new RDDs) …
- … **resilient** (Spark can recompute lost partitions from lineage).

```python
rdd = sc.textFile("orders.csv")     # an RDD of strings
rdd2 = rdd.map(lambda s: s.upper()) # a NEW RDD, not yet computed
```

RDDs are where MapReduce thinking is most direct. (DataFrames at the end of the tutorial.)

---

## Transformations vs Actions (laziness)

Two kinds of operations:

| **Transformations** | **Actions** |
|---|---|
| `map`, `filter`, `flatMap`     | `collect`, `count`, `take` |
| `reduceByKey`, `groupByKey`    | `saveAsTextFile`           |
| `join`, `union`, `distinct`    | `first`, `top`, `reduce`   |

- Transformations are **lazy**: they only build a recipe (the *DAG*).
- Actions **run** the recipe and return a result to the driver.

This is how Spark sees the *whole* plan and optimizes it before executing.

---

## SparkSession — the entry point

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
            .appName("mapreduce-tutorial")
            .master("local[*]")          # use all local cores
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate())
sc = spark.sparkContext                  # for RDD work
sc.setLogLevel("WARN")
```

For RDD work we use `sc` (SparkContext). For DataFrame work we use `spark`. They're two faces of the same session.

---

## Your first RDD

Two ways to make one:

```python
# 1. From a local Python collection (testing, demos)
nums = sc.parallelize([1, 2, 3, 4, 5], numSlices=2)
print(nums.collect())                     # [1, 2, 3, 4, 5]
print(nums.map(lambda x: x*x).collect())  # [1, 4, 9, 16, 25]

# 2. From files (the real use case)
lines = sc.textFile("data/poem.txt")      # one record per line
print(lines.count())
```

`collect()` is your debugging friend on small data. **Never** call it on a TB-sized RDD — it pulls everything to the driver.

---

## Word count — the canonical example

> Given a text file, output the count of each distinct word.

This is the "Hello, World" of MapReduce. Every distributed framework can do this in 5 lines, and it shows the whole machine working.

We'll build it in three steps:

1. Split lines → words *(map / flatMap)*.
2. Tag each word with a 1 *(map)*.
3. Sum the 1s by key *(reduceByKey)*.

---

## Word count — data + pipeline

```python
lines = sc.parallelize([
    "the quick brown fox",
    "the lazy dog",
    "the quick fox jumps over the lazy dog",
])

counts = (lines
    .flatMap(lambda line: line.split())     # one record -> many words
    .map(lambda w: (w, 1))                  # (word, 1)
    .reduceByKey(lambda a, b: a + b)        # sum 1s by word
)

for word, n in sorted(counts.collect(), key=lambda kv: -kv[1]):
    print(f"{word:10s} {n}")
# the 4 | quick 2 | fox 2 | dog 2 | lazy 2 | brown 1 | jumps 1 | over 1
```

`flatMap` is `map` + flatten — perfect when each input emits many outputs. `reduceByKey` is the MapReduce reduce, applied per key, with **partial aggregation locally before shuffling**.

---

## Inspect what each step produces

Stage the pipeline and use `take(n)` — much safer than `collect()`:

```python
step1 = lines.flatMap(lambda l: l.split())
step2 = step1.map(lambda w: (w, 1))
step3 = step2.reduceByKey(lambda a, b: a + b)

step1.take(4)   # ['the', 'quick', 'brown', 'fox']
step2.take(4)   # [('the', 1), ('quick', 1), ('brown', 1), ('fox', 1)]
step3.collect() # [('the', 4), ('quick', 2), ('brown', 1), ...]
```

This is the basic debugging loop. Build → `take` → adjust → repeat.

---

## Variant 1 — top-N words

```python
top5 = counts.takeOrdered(5, key=lambda kv: -kv[1])
for word, n in top5:
    print(f"{word:10s} {n}")
```

`takeOrdered` is a *bounded* operation — it returns only N items to the driver, regardless of RDD size. The negative key makes it sort **descending**.

> ❗ Don't use `sortBy` + `take` for top-N on large RDDs — that sorts the whole dataset. `takeOrdered` does a bounded heap on each partition and merges.

---

## Variant 2 — case folding + stop words

```python
STOP = {"the", "a", "an", "of", "and", "to", "in", "on", "for"}

counts = (lines
    .flatMap(lambda l: l.lower().split())                  # case fold
    .filter(lambda w: w not in STOP)                       # drop stop words
    .map(lambda w: (w, 1))
    .reduceByKey(lambda a, b: a + b)
)
```

Notice how each phase composes cleanly. **Filtering close to the source** is a universal performance tip — you shrink the data before the expensive shuffle.

---

## Variant 3 — bigrams (n-grams)

> Count adjacent **pairs** of words instead of single words.

```python
def to_bigrams(line):
    tokens = line.lower().split()
    return zip(tokens, tokens[1:])           # adjacent pairs

bigrams = (lines
    .flatMap(to_bigrams)                     # ('the','quick'), ('quick','brown'), ...
    .map(lambda pair: (pair, 1))
    .reduceByKey(lambda a, b: a + b)
)

print(sorted(bigrams.collect())[:5])
```

Same shape as word count — only the *map* changed. That's the power of the model.

---

## Setting up the retail dataset

For the rest of the tutorial we'll work on **retail orders**. The block below creates a tiny inline dataset (no external dependencies — pure stdlib). Run it once.

```python
import csv, os, random
from datetime import date, timedelta

random.seed(7)
ROOT = "/tmp/mr_tutorial"
os.makedirs(ROOT, exist_ok=True)

CATEGORIES = ["Electronics", "Apparel", "Books", "Home", "Beauty"]
COUNTRIES  = ["US", "CA", "MX", "GB", "DE"]
PRODUCTS   = [(i, f"PROD-{i:03d}", random.choice(CATEGORIES),
               round(random.uniform(5, 400), 2)) for i in range(1, 51)]
```

(continued on next slide)

---

## Setting up the retail dataset (cont.)

```python
# customers.csv: customer_id,country,segment
with open(f"{ROOT}/customers.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["customer_id","country","segment"])
    for cid in range(1, 201):
        w.writerow([cid, random.choice(COUNTRIES),
                    random.choice(["consumer","smb","enterprise"])])

# products.csv: product_id,sku,category,price
with open(f"{ROOT}/products.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["product_id","sku","category","price"])
    for pid, sku, cat, price in PRODUCTS:
        w.writerow([pid, sku, cat, price])
```

(orders + items on next slide)

---

## Setting up the retail dataset (cont.)

```python
# orders.csv: order_id,customer_id,order_date,channel
# items.csv:  order_id,product_id,quantity,unit_price
start = date(2026, 1, 1)
with open(f"{ROOT}/orders.csv","w",newline="") as fo, \
     open(f"{ROOT}/items.csv","w",newline="") as fi:
    wo = csv.writer(fo); wo.writerow(["order_id","customer_id","order_date","channel"])
    wi = csv.writer(fi); wi.writerow(["order_id","product_id","quantity","unit_price"])
    for oid in range(1, 1001):
        cid     = random.randint(1, 200)
        d       = start + timedelta(days=random.randint(0, 89))
        channel = random.choice(["web","mobile","in_store"])
        wo.writerow([oid, cid, d.isoformat(), channel])
        for _ in range(random.randint(1, 4)):
            pid, _, _, price = random.choice(PRODUCTS)
            wi.writerow([oid, pid, random.randint(1,3), price])
print("data ready in", ROOT)
```

---

## Loading the retail data into RDDs

```python
def load_csv(path):
    rdd = sc.textFile(path)
    header = rdd.first()
    return rdd.filter(lambda line: line != header) \
              .map(lambda line: line.split(","))

customers = load_csv(f"{ROOT}/customers.csv")  # [cid, country, segment]
products  = load_csv(f"{ROOT}/products.csv")   # [pid, sku, category, price]
orders    = load_csv(f"{ROOT}/orders.csv")     # [oid, cid, date, channel]
items     = load_csv(f"{ROOT}/items.csv")      # [oid, pid, qty, price]

print("orders:", orders.count(), "items:", items.count())
```

`first()` + `filter` is the classic header-stripping idiom on RDDs. (DataFrames will do this for us later.)

---

## Q1 — Total revenue by category

> Each item line is `[oid, pid, qty, price]`. We need to join with `products` to get the category, then sum `qty × price` by category.

```python
# (pid, category)  --  the lookup we need
pid_to_cat = products.map(lambda r: (int(r[0]), r[2]))

# (pid, line_total)
item_kv = items.map(lambda r: (int(r[1]), int(r[2]) * float(r[3])))

# join is on key (pid)  ->  (pid, (line_total, category))
joined = item_kv.join(pid_to_cat)

# (category, line_total)  ->  sum
by_cat = (joined
    .map(lambda kv: (kv[1][1], kv[1][0]))
    .reduceByKey(lambda a, b: a + b))

for cat, rev in sorted(by_cat.collect(), key=lambda kv: -kv[1]):
    print(f"  {cat:12s} ${rev:>10,.2f}")
```

> Same shape as word count, with two extras: we **re-keyed twice** (first on `pid` for the join, then on `category` for the aggregation), and we did a **join** that shuffles both inputs by the join key. *Every `reduceByKey` and every `join` is a shuffle. Re-keying is how you steer it.*

---

## Q2 — Top 5 customers by total spend

```python
# (oid, line_total)
oid_total = items.map(lambda r: (r[0], int(r[2]) * float(r[3])))

# order -> customer mapping: (oid, cid)
oid_cid = orders.map(lambda r: (r[0], int(r[1])))

# join on oid -> (oid, (line_total, cid))
# re-key by cid -> (cid, line_total)
spend_per_customer = (oid_total
    .join(oid_cid)
    .map(lambda kv: (kv[1][1], kv[1][0]))
    .reduceByKey(lambda a, b: a + b))

top5 = spend_per_customer.takeOrdered(5, key=lambda kv: -kv[1])
for cid, total in top5:
    print(f"  customer {cid:>3}  ${total:>10,.2f}")
```

`takeOrdered(5, key=-spend)` keeps only 5 results in the driver — safe at any scale.

---

## Q3 — Orders per channel per month

> Two grouping keys at once: `(month, channel)`.

```python
def parse_order(row):
    oid, cid, d, ch = row
    month = d[:7]                 # 'YYYY-MM-DD' -> 'YYYY-MM'
    return ((month, ch), 1)       # composite key

monthly = (orders
    .map(parse_order)
    .reduceByKey(lambda a, b: a + b))

for (month, ch), n in sorted(monthly.collect()):
    print(f"  {month}  {ch:<10s} {n}")
```

**Composite keys** are tuples. Spark doesn't care what the key is, as long as it hashes consistently.

---

## Two-input join — the cost of shuffles

```python
# Naive: shuffle BOTH sides of the join by key
joined = item_kv.join(pid_to_cat)   # both shuffled by pid
```

For a small `products` (50 rows) and a big `items` (millions), shuffling `items` is wasteful — `products` would fit in every executor's memory.

Solution: **broadcast** the small side.

```python
prod_map = dict(pid_to_cat.collect())     # tiny — fits in driver
prod_bc  = sc.broadcast(prod_map)         # ship to every executor

by_cat = (items
    .map(lambda r: (prod_bc.value[int(r[1])],
                    int(r[2]) * float(r[3])))
    .reduceByKey(lambda a, b: a + b))
```

No shuffle of `items`. **This is the single biggest performance win for star-schema-style joins.**

---

## groupByKey vs reduceByKey — the classic gotcha

Both produce one record per key. **Performance is wildly different.**

```python
# BAD: ships every value across the network, groups in driver/executor memory
counts = pairs.groupByKey().mapValues(sum)

# GOOD: combines locally on each partition before shuffling, then sums the partials
counts = pairs.reduceByKey(lambda a, b: a + b)
```

Imagine 10 mappers each emit 1M `(country, 1)` pairs:

- `groupByKey` shuffles 10M values across the network.
- `reduceByKey` shuffles ~200 values (one per country per partition).

> **Rule of thumb:** if you can express it as `reduceByKey`, never use `groupByKey`.

---

## When `groupByKey` is the right tool

It's the right call when you genuinely need **all the values per key**, not just an aggregate:

```python
# all order_ids per customer (small lists per key)
oids_by_cust = (orders
    .map(lambda r: (int(r[1]), r[0]))
    .groupByKey()
    .mapValues(list))
```

But if the per-key list is huge, you're a memory-explosion away from a job failure. Prefer `aggregateByKey` or `combineByKey` for streaming-style accumulation.

---

## Combiners — what `reduceByKey` does for you

Conceptually, on each partition Spark runs a **mini-reduce** *before* the shuffle:

```
mapper outputs: (cat, 5), (cat, 3), (dog, 2), (cat, 1), (dog, 4)
                          │
                          ▼  partial reduce on this partition
shuffled:       (cat, 9), (dog, 6)              ← only 2 records leave this machine
```

`reduceByKey` enables the combiner automatically because your function is associative + commutative. `groupByKey` does not — it can't, because there's no aggregation to do.

---

## Broadcast variables — read-only lookups

We just used one for the products map. The pattern in general:

```python
small_lookup = ...                   # built on the driver, must fit in memory
bc = sc.broadcast(small_lookup)      # ship once per executor

big_rdd.map(lambda r: enrich(r, bc.value))
```

Use cases:

- Dimension tables (categories, country codes).
- ML model parameters.
- Reference data that doesn't change during the job.

Don't broadcast something the size of the data — at that point use a regular join.

---

## Accumulators — counters across the cluster

Need to count "skipped" or "bad" rows during processing? Use an **accumulator**:

```python
bad = sc.accumulator(0)

def parse_safe(row):
    try:
        return float(row[3])
    except (ValueError, IndexError):
        bad.add(1)
        return 0.0

total = items.map(parse_safe).sum()
print(f"total: {total:.2f}  (bad rows skipped: {bad.value})")
```

Accumulators are **write-only on workers, readable on the driver**. They're safe under failures (Spark dedups retries).

---

## Caching — when to keep an RDD in memory

If you use the same RDD twice, you compute it twice — unless you cache.

```python
cleaned = (items
    .map(parse_item)
    .filter(is_valid))

cleaned.cache()                     # mark for in-memory storage

print(cleaned.count())              # first action: computes + caches
print(cleaned.map(...).reduce(...)) # second action: reads from memory
```

Cache when:

- The RDD is **expensive to compute** (joins, parses).
- It's **used 2+ times** in actions.
- It **fits in memory** (or use `persist(StorageLevel.MEMORY_AND_DISK)`).

---

## DataFrames — MapReduce, modernized

Everything we've done can be expressed with **DataFrames**, Spark's higher-level API:

- A DataFrame is a distributed table with named, typed columns.
- Operations look like SQL or pandas: `select`, `groupBy`, `join`.
- Spark's **Catalyst optimizer** rewrites your query into an efficient plan.
- Often runs **2–10× faster** than equivalent RDD code.

The MapReduce model is still under the hood — you just stop writing the boilerplate.

---

## Word count, DataFrame style

```python
from pyspark.sql import functions as F

df = spark.createDataFrame(
    [(l,) for l in [
        "the quick brown fox",
        "the lazy dog",
        "the quick fox jumps over the lazy dog",
    ]], ["line"]
)

(df.select(F.explode(F.split("line", " ")).alias("word"))
   .groupBy("word")
   .count()
   .orderBy(F.desc("count"))
   .show())
```

`explode` is `flatMap`. `groupBy().count()` is `map(_,1).reduceByKey(_+_)`. Same MapReduce shape.

---

## Revenue by category, DataFrame style

```python
items_df    = spark.read.csv(f"{ROOT}/items.csv",    header=True, inferSchema=True)
products_df = spark.read.csv(f"{ROOT}/products.csv", header=True, inferSchema=True)

(items_df
    .join(products_df.select("product_id","category"), on="product_id")
    .withColumn("line_total", F.col("quantity") * F.col("unit_price"))
    .groupBy("category")
    .agg(F.round(F.sum("line_total"), 2).alias("revenue"))
    .orderBy(F.desc("revenue"))
    .show())
```

Compare to the RDD version (3 re-keys + 1 join + 1 reduce). Catalyst chooses partitioning, join strategy, and broadcast for you.

---

## Where MapReduce thinking still helps

Even after you graduate to DataFrames:

1. **Partitioning intuition.** Knowing what triggers a shuffle is still essential (`groupBy`, `join`, `distinct`, `window`).
2. **Skew detection.** When one key has 90% of the values, both APIs slow down. The fix is the same.
3. **Reasoning about cost.** "How many rows does this map / reduce process?" works at every level of abstraction.
4. **UDF safety.** A Python UDF in DataFrames *is* a `map` — same purity rules.

You're not "moving past" MapReduce. You're moving past *writing it explicitly*.

---

## Recap

We learned to:

- Phrase analytical questions as **(key, value, reduce)** triples.
- Use **PySpark RDDs** for direct MapReduce code.
- Recognize the **shuffle** as the dominant cost.
- Choose **`reduceByKey` over `groupByKey`**, **broadcast** small lookups, **cache** reused RDDs.
- See the **DataFrame API** as a Catalyst-optimized layer over the same model.

Next up in the course: putting these tools to work in a real **Bronze / Silver / Gold** pipeline (the medallion unit).

---

## Practice problems

Pick three. Solve with RDDs first; then redo with DataFrames.

1. **Top product** in each category by units sold.
2. **Repeat customers**: customers who placed orders on more than one date.
3. **Average order value** per channel.
4. **Best day of the week** for revenue (date → weekday → group).
5. **Co-purchase pairs**: pairs of products that appear in the same order, top 10 most common pairs.
6. **Customer cohort retention**: per cohort (first-order month), how many bought again the following month?

---

## Further reading

- Dean & Ghemawat, *MapReduce: Simplified Data Processing on Large Clusters* (Google, 2004) — the original 8-page paper. Still a beautiful read.
- Zaharia et al., *Resilient Distributed Datasets…* (NSDI 2012) — Spark's core idea.
- *Spark: The Definitive Guide* (Chambers & Zaharia) — pragmatic deep dive.
- PySpark API docs: https://spark.apache.org/docs/latest/api/python/

---

# Thank you

Slides: this file (`mapreduce-pyspark-tutorial.md`)
Render to PDF: `marp mapreduce-pyspark-tutorial.md --pdf`
Hands-on: run `mapreduce-pyspark-notebook.ipynb` alongside these slides
Questions: office hours / email
