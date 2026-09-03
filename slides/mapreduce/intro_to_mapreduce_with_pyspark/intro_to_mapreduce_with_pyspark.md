# Introduction to Big Data, MapReduce, and PySpark

	Author: Mahmoud Parsian
	Last updated: 9/3/2026

This write-up covers, in one continuous narrative, the
material spread across NYU Center for Data Science's
three-part "BigData with PySpark" lesson (Section 26,
references 1-3) — a Big Data / Spark motivation, a
MapReduce primer built entirely in pure Python, and an
introduction to PySpark's RDD API — and then goes beyond it:
modernized, working Python 3 code (the lesson's code samples
are Python 2 and don't run as printed), explicit
explanations of things it asserts but doesn't explain (lazy
evaluation, narrow vs. wide transformations, lineage-based
fault tolerance), and new material it doesn't cover at all
(DataFrames/Spark SQL, caching, a worked example computing a
correct per-key average with `combineByKey` — averaging is
not associative, and getting it wrong is a common bug — a
Top-N worked example, a pitfalls list, and a glossary).

## Table of Contents

1. Introduction: The Big Data Problem
2. Two Problems Every Distributed System Must Solve
3. Hadoop: HDFS + MapReduce, in Outline
4. Why a Single-Machine Dictionary Doesn't Scale
5. The Three Steps of MapReduce: Map, Shuffle, Reduce
6. Worked Example — Word Count in Pure Python
7. What MapReduce Is (and Isn't) Good For
8. From Google's Paper to Hadoop to the Cloud
9. What Apache Spark Adds on Top of MapReduce
10. The Spark Ecosystem, Then and Now
11. RDDs, the Driver/Executor Model, and the Cluster Manager
12. Transformations vs. Actions: Why Spark Is Lazy
13. Narrow vs. Wide Transformations — Where the Shuffle Really Happens
14. Worked Example — Word Count in PySpark (RDD API)
15. Broadcast Variables, Accumulators, and Why Closures Need Them
16. Fault Tolerance via Lineage, and Caching/Persistence
17. Worked Example — Counting Primes with `parallelize()`
18. Worked Example — Average per Key, and Why Naive Averaging Breaks
19. Beyond RDDs: DataFrames and Spark SQL
20. Worked Example — Top-N Words (RDD and DataFrame)
21. Running It: Shell, `spark-submit`, and Notebooks
22. Common Pitfalls
23. Key Terms (Glossary)
24. Food for Thought
25. Comments
26. References

## 1. Introduction: The Big Data Problem

A modern text corpus, clickstream log, or transaction
history can easily run to hundreds of gigabytes or
terabytes. That's larger than the RAM of any single
machine, and often larger than what one machine can even
read from disk in a reasonable amount of time. "Big Data"
processing isn't a different kind of computation from what
you already know — it's the same `map()`/`filter()`/
`reduce()`-shaped logic you'd write for a small file — but
run across many machines at once, because no single machine
is big enough or fast enough.

**Apache Hadoop** was the first widely-adopted open-source
system built around this idea, pairing a distributed file
system (HDFS) with a distributed computing model
(MapReduce). **Apache Spark** came a few years later as a
general-purpose successor: it keeps MapReduce's mental model
of mapping data into key/value pairs, but adds a much richer
set of operators, an in-memory execution engine, and
(via PySpark) a Python API — which is why this course
teaches MapReduce concepts first, then Spark, then PySpark:
each layer is the previous one made faster and easier to
program, not a different idea.

## 2. Two Problems Every Distributed System Must Solve

Any distributed computing framework has to answer two
separate questions:

1. **How do you distribute the *data*?** — even when a single
   disk is physically large enough to hold a multi-terabyte
   file, reading it end to end on one machine is too slow,
   and that one disk is a single point of failure. The file
   has to be split into chunks, spread across many machines
   for parallel read throughput and durability, and the
   system needs to track which chunk lives where.
2. **How do you distribute the *computation*?** — once the
   data is spread out, the code that processes it has to run
   *next to* each chunk (moving code to data is far cheaper
   than moving data to code at this scale), and the partial
   results from every machine have to be combined into one
   answer.

Hadoop answers question 1 with the **Hadoop Distributed
File System (HDFS)** — files are split into fixed-size
blocks, each block replicated across multiple machines for
durability — and question 2 with **MapReduce**, the
programming model this article builds up from first
principles in Sections 4-8. Spark (Sections 9 onward) can
run directly on top of HDFS for question 1, or read from
plain local disk, S3, Cassandra, HBase, and others; its own
contribution is a faster, richer answer to question 2.

## 3. Hadoop: HDFS + MapReduce, in Outline

You don't need to run Hadoop to learn MapReduce — Sections
4-8 build the whole idea in plain Python, no cluster
required — but it helps to know the two pieces by name
before diving in, since PySpark's vocabulary (`textFile`,
partitions, executors) borrows directly from them:

| Piece | Answers | Analogy |
|---|---|---|
| HDFS | Where does the data live? | A filesystem, but each "file" is really a set of 128 MB (or 64 MB, configurable) blocks scattered across a cluster, each block kept in 3 copies by default for durability |
| MapReduce | How does code run against that data? | A two-phase program: a `map()` function runs once per input record near where that record's block lives, and a `reduce()` function runs once per output key after all the mapped records for that key have been gathered together |

Section 5 unpacks the map/shuffle/reduce phases in detail;
Section 8 covers how this history led to Spark and to
managed cloud offerings like Amazon EMR.

## 4. Why a Single-Machine Dictionary Doesn't Scale

Before introducing MapReduce, it's worth seeing exactly why
the "obvious" single-machine approach breaks down — that's
the motivation for everything that follows.

The canonical MapReduce exercise — sometimes called the
"Hello, World!" of distributed computing — is counting word
frequencies in a large text. Here it is solved the obvious
way in Python 3, using [Moby Dick](https://www.gutenberg.org/ebooks/2701)
(`pg2701.txt`) as the sample input — a real, full-length book
rather than a toy snippet, the same choice the source lesson
makes and this article keeps throughout:

```python
import re
from collections import defaultdict

def splitter(line):
    """Strip leading/trailing non-word chars, split on
    runs of non-word chars, lowercase every token."""
    line = re.sub(r'^\W+|\W+$', '', line)
    return (w.lower() for w in re.split(r'\W+', line) if w)

sums = defaultdict(int)
with open('pg2701.txt', 'r') as in_file:
    for line in in_file:
        for word in splitter(line):
            sums[word] += 1

best_word = max(sums, key=sums.get)
print(f"max: {best_word} = {sums[best_word]}")
```

```text
max: the = 14620
```

(A caveat worth stating plainly, since it's a real gotcha: the
exact count depends on *which* snapshot of `pg2701.txt` you
download — Project Gutenberg periodically updates its
boilerplate header/footer text, and has done so more than
once since this figure was first reported. `14620` is what the
source lesson gets and what this article's outputs consistently
show; a fresh download today lands close to it, but not
necessarily exact. The word-counting *logic* is unaffected
either way — this article's code was verified end to end
against a real PySpark install, just not necessarily against
the identical file bytes the `14620` figure came from.
[`word_count_in_python/`](../word_count_in_python/) sidesteps
this entirely with small, fixed, checked-in toy text files
instead of a live download.)

This works fine on one 1.2 MB text file. The problem is the
`sums` dictionary: it's a **central, mutable data structure**
that every line of input has to pass through, and it has to
be held **entirely in memory** for the program to run at
all. As `sums` grows:

* while it fits in the CPU's data cache, throughput (words
  processed per second) is roughly constant;
* once it outgrows the cache — and it will, long before it
  outgrows RAM, because caches are small — every dictionary
  access becomes a slower main-memory access, and throughput
  drops;
* once it outgrows RAM entirely, the OS starts paging the
  dictionary to swap space, and throughput drops again, by
  orders of magnitude;
* once it outgrows swap space too, the program crashes with
  an out-of-memory error — there's no more room to grow into.

None of this is specific to word counting. **Any** program
built around "read everything, build one big in-memory
structure, then answer a question from it" hits the same
wall, just at a different data size depending on the
structure. That wall is exactly what MapReduce is designed
to route around.

## 5. The Three Steps of MapReduce: Map, Shuffle, Reduce

MapReduce's answer is to never require the whole dataset —
or even the whole *intermediate* result — to be in memory at
once. It restructures any "compute something per key" job
into three steps:

```text
input records  --map-->  (key, value) pairs
                            |
                          shuffle   (group by key)
                            |
                            v
              (key, [values...])  --reduce-->  (key, result)
```

1. **Map** — apply a function to every input record
   independently, producing zero or more `(key, value)`
   pairs. This step needs no memory of any other record: it
   can run one line at a time, streaming from disk, and it
   can run on many machines in parallel because no mapper
   needs to talk to any other mapper.
2. **Shuffle** — group all the emitted pairs by key. This is
   the one step that needs *some* form of global
   coordination (matching values for the same key that were
   produced by different mappers), but it's provided by the
   framework — a MapReduce/Spark programmer never writes
   shuffle code themselves, only `map()` and `reduce()`.
3. **Reduce** — for each key, process the full list of values
   that arrived for it (count them, sum them, find a max,
   join them against another dataset — see
   [`joins_in_mapreduce/`](../joins_in_mapreduce/join_operation_in_action_using_MapReduce.md)
   for a worked example of that last one) and emit a final
   result for that key.

Two properties fall out of this split, and they're the whole
reason it scales where the Section 4 approach didn't:

* **No step requires the whole dataset in memory.** Mapping
  is one record at a time; shuffling groups data using
  external sort/merge (disk-backed, not RAM-backed) once
  data outgrows memory; reducing processes one key's values
  at a time.
* **Map and reduce are embarrassingly parallel** — every
  mapper is independent of every other mapper, and every
  reducer (once its input key's values have all arrived) is
  independent of every other reducer. Only the shuffle
  couples them together, and it's a fixed, well-understood
  cost the framework handles once, not something every job
  reimplements.

## 6. Worked Example — Word Count in Pure Python

### In-memory, for comparison

Using Python's own `map()`, the mapping step for a small
in-memory list looks exactly like the framework operation
it's named after:

```python
words = ['deer', 'bear', 'river', 'car', 'car', 'river', 'deer', 'car', 'bear']
mapped = list(map(lambda w: (w, 1), words))
print(mapped)
```

```text
[('deer', 1), ('bear', 1), ('river', 1), ('car', 1), ('car', 1),
 ('river', 1), ('deer', 1), ('car', 1), ('bear', 1)]
```

Shuffle (group by key, via sort):

```python
shuffled = sorted(mapped)
print(shuffled)
```

```text
[('bear', 1), ('bear', 1), ('car', 1), ('car', 1), ('car', 1),
 ('deer', 1), ('deer', 1), ('river', 1), ('river', 1)]
```

Reduce (sum values sharing a key), using `itertools.groupby`
plus `functools.reduce` — Python 3 moved `reduce()` out of
the builtins and into `functools`, and this is a good moment
to notice that Python's own `functools.reduce(f, values)`
*is* a one-key MapReduce reducer: it repeatedly combines two
values with a binary function until one remains, exactly
what a MapReduce `reduce(key, values)` call does for a
single key:

```python
from itertools import groupby
from functools import reduce
from operator import itemgetter, add

grouped = groupby(shuffled, key=itemgetter(0))
counts = [(k, reduce(add, (v for _, v in g))) for k, g in grouped]
print(counts)
```

```text
[('bear', 2), ('car', 3), ('deer', 2), ('river', 2)]
```

### File-based, so nothing is ever fully in memory

The in-memory version above still builds a full Python list
at every step — fine for 9 words, not fine for a real
corpus. The version below processes `pg2701.txt` a line (map
phase) or a line-offset (shuffle/reduce phase) at a time, so
memory use stays flat regardless of file size — this is the
actual shape of a MapReduce job, just running on one machine
instead of many.

**Map** — read the input once, write one `word<TAB>1` line
per word to an intermediate file. At no point is the whole
file in memory:

```python
import re

def splitter(line):
    line = re.sub(r'^\W+|\W+$', '', line)
    return (w.lower() for w in re.split(r'\W+', line) if w)

with open('pg2701.txt', 'r') as in_file, \
     open('pg2701.txt.map', 'w') as out_file:
    for line in in_file:
        for word in splitter(line):
            out_file.write(f"{word}\t1\n")
```

**Shuffle** — group same-key lines together. A true
distributed shuffle uses external merge sort so it never
needs the whole intermediate file in RAM either; here's the
same idea on one machine, indexing line offsets instead of
loading line contents, then rewriting the file in sorted
order:

```python
def build_index(filename):
    index = []
    with open(filename) as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            key = line.split('\t')[0]
            index.append((key, offset, len(line)))
    index.sort()
    return index

index = build_index('pg2701.txt.map')
with open('pg2701.txt.map', 'r') as in_file, \
     open('pg2701.txt.map.sorted', 'w') as out_file:
    for _key, offset, length in index:
        in_file.seek(offset)
        out_file.write(in_file.read(length))
```

**Reduce** — walk the sorted file once; since equal keys are
now adjacent, sum runs of matching keys and reset on each new
key, tracking the running maximum as we go so the whole
result file never needs to be re-read afterward:

```python
best_word, best_count = None, 0
current_key, current_sum = None, 0

def flush(key, total):
    global best_word, best_count
    if key is not None and total > best_count:
        best_word, best_count = key, total

with open('pg2701.txt.map.sorted', 'r') as in_file:
    for line in in_file:
        key, value = line.rstrip('\n').split('\t')
        if key != current_key:
            flush(current_key, current_sum)
            current_key, current_sum = key, 0
        current_sum += int(value)
    flush(current_key, current_sum)   # last group

print(f"max: {best_word} = {best_count}")
```

```text
max: the = 14620
```

Same answer as Section 4's dictionary-based version — but
this one never holds more than one line, or one intermediate
file, in memory at a time. That property — not the specific
word-count problem — is what generalizes to distributed
MapReduce: split the map step across machines (each machine
maps the lines it can already see), let the framework's
shuffle merge everyone's intermediate files by key across the
network, and split the reduce step across machines too (each
reducer only needs the values for the keys assigned to it).
[`word_count_in_mapreduce/word_count_in_mapreduce.md`](../word_count_in_mapreduce/word_count_in_mapreduce.md)
traces that fully-distributed version call by call, including
combiners; [`word_count_in_python/`](../word_count_in_python/)
has similarly-shaped single-file and multi-file-directory
word-count scripts, over small checked-in toy text instead of
a downloaded book, for tinkering without the Section 4
caveat above.

## 7. What MapReduce Is (and Isn't) Good For

Word count is the simplest possible instance, but the same
map/shuffle/reduce shape covers a surprising range of
real problems:

* **Distributed sort** — map each record to `(sort_key,
  record)`; the shuffle's group-by-key step does the actual
  sorting for free; the reducer just re-emits its group.
* **Distributed search / filtering** — map each record
  through a predicate, emitting it only if it matches;
  reduce is often the identity function, or an aggregation
  over the matches.
* **Web-link graph traversal** (e.g. PageRank) — map each
  page to `(target_url, rank_contribution)` for every link on
  it; reduce sums the contributions landing on each URL; run
  the whole job again on the output for the next iteration.
* **Machine learning** — many classic algorithms (k-means'
  per-iteration reassignment step, naive Bayes' per-class
  statistics, gradient descent's per-batch gradient sum) are
  "compute a per-group statistic across a huge dataset,"
  which is a reduce; MLlib (Section 10) is built on exactly
  this pattern.

What it's **not** good for: anything that needs low-latency,
point-lookup access to a single record (that's what
databases and key-value stores are for), and anything
inherently sequential where step *N* depends on the *exact*
result of step *N-1* for the *same* key in a way that can't
be expressed as "reduce over the values that share a key."
MapReduce is a batch-processing model, not a general-purpose
parallel programming model — it's powerful because so many
data analysis tasks turn out to fit it, not because every
problem does.

## 8. From Google's Paper to Hadoop to the Cloud

MapReduce began as an internal Google system, described
publicly in Dean & Ghemawat's 2004 paper "MapReduce:
Simplified Data Processing on Large Clusters" (kept in this
repo at
[`google_mapreduce_paper/`](../google_mapreduce_paper/MapReduce_Simplified_Data_Processing_on_Large_Clusters_by_Jeff_Dean.pdf))
— Google's implementation stayed proprietary. **Apache
Hadoop** is the open-source clone, built to the same
programming model, and it's what carried MapReduce to Yahoo,
Facebook, Amazon, and beyond. **Amazon EMR (Elastic
MapReduce)** runs Hadoop (and, today, Spark) on
on-demand EC2 clusters — you don't buy or manage the
machines, you rent a cluster for the duration of the job.
That "rent a cluster, run the job, tear it down" pattern is
also exactly how Spark clusters are typically used today,
which is where the rest of this article picks up.

## 9. What Apache Spark Adds on Top of MapReduce

Classic Hadoop MapReduce writes its intermediate shuffle
output to disk between the map and reduce phases (and again
between chained jobs), because early Hadoop targeted clusters
where RAM was the scarce resource and disk was assumed
cheap and plentiful. That's safe but slow, especially for
**iterative** algorithms (most machine learning is a loop of
"map, reduce, repeat with the updated model") — each
iteration re-reads and re-writes everything to disk.

Apache Spark keeps MapReduce's key idea — express a
computation as data transformations keyed for grouping — but:

* keeps intermediate results **in memory** across steps and
  across iterations whenever possible, only spilling to disk
  under memory pressure;
* provides far more than two operators — over 80 high-level
  transformations and actions beyond `map`/`reduce` (`filter`,
  `join`, `groupByKey`, `distinct`, `sortByKey`, ...);
* runs standalone, or on top of Hadoop's YARN, Kubernetes, or
  Mesos as the cluster manager, and can read from HDFS,
  Cassandra, HBase, S3, or local disk;
* is fast enough to be used **interactively** — a
  `pyspark` shell behaves like Python's own REPL, running
  distributed queries as you type them, not just as
  submitted batch jobs;
* exposes its API in Scala (Spark's native implementation
  language), Java, Python (**PySpark**, this article's
  focus), R, and SQL.

Because Spark meets both the Big-Data-scale requirement and
the iterative, interactive workflow that data science needs,
it displaced plain Hadoop MapReduce as the default choice for
new cluster-computing work through the 2010s and remains the
dominant engine today.

## 10. The Spark Ecosystem, Then and Now

Spark ships as a stack of libraries built on one shared
execution engine. The source lesson (Section 26, references
1-3) describes the 2017-era lineup; a couple of pieces have
since been superseded by DataFrame-native replacements,
noted below:

| Component | What it does | Status today |
|---|---|---|
| **Spark Core** | Defines RDDs and the transformations/actions in Sections 12-13; everything else is built on it | Still the foundation |
| **Spark SQL** | SQL and DataFrame access to structured data, backed by the Catalyst query optimizer | Now the primary, recommended API for most jobs (Section 19) |
| **Spark Streaming** | Micro-batch processing of live data streams as a sequence of small RDDs | Superseded for new work by **Structured Streaming**, which reuses the DataFrame API and Catalyst optimizer instead of raw RDDs |
| **MLlib** | Machine learning algorithms implemented as RDD operations | The RDD-based API is in maintenance mode; new code uses **`spark.ml`**, the DataFrame-based ML library (same underlying algorithms, Pipeline API, works with Spark SQL) |
| **GraphX** | Graph algorithms and operations extending the RDD API | Still RDD-based and still maintained, but for DataFrame-native graph work most teams now reach for the community **GraphFrames** package instead |

The throughline: Spark's own internal direction since roughly
2015-2016 has been "move workloads off raw RDDs and onto
DataFrames," because the DataFrame API gets automatic query
optimization that hand-written RDD code doesn't (Section 19
covers why). RDDs remain the right tool when your data
doesn't fit a table shape or you need fine-grained control
that DataFrames don't expose — and they're the clearest way
to *learn* the underlying model, which is why Sections 11-18
teach them first.

## 11. RDDs, the Driver/Executor Model, and the Cluster Manager

A **Resilient Distributed Dataset (RDD)** is Spark's core
abstraction: a read-only, **partitioned** collection of
objects, spread across the machines in a cluster, that
supports parallel operations. "Resilient" refers to how it
survives machine failure — covered in Section 16.

A Spark program follows one consistent shape:

1. A **driver** — the process running your `main()` — creates
   one or more RDDs (from a file via `sc.textFile()`, from an
   in-memory Python collection via `sc.parallelize()`, or by
   transforming an existing RDD).
2. The driver describes a chain of **transformations** on
   those RDDs (Section 12).
3. The driver triggers computation with an **action**, which
   sends the actual work out to **executors** — one or more
   worker processes, each handling the partitions of the RDD
   that live on its machine — which run it and send results
   back.

```text
                 +-----------+
                 |  driver   |   <- your program's main()
                 +-----------+
                   |   |   |
        code + tasks  results
                   |   |   |
        +--------+ +--------+ +--------+
        |executor| |executor| |executor|   <- one per worker node
        +--------+ +--------+ +--------+
        [partition] [partition] [partition]  <- each holds a slice of the RDD
```

The driver talks to a **cluster manager** (Spark's own
standalone manager, YARN, Kubernetes, or Mesos) via a
`SparkContext`, and the cluster manager allocates executors
across the available machines. Each executor both runs
computation *and* manages storage/caching (Section 16) for
the partitions assigned to it. This is a meaningful
difference from classic Hadoop, where a job could be
submitted from anywhere to a central JobTracker that then
scheduled execution — in Spark, the driver is an active
participant for the whole job's lifetime, so it needs to stay
network-reachable from the cluster, not just able to submit
and walk away.
[`partitions_in_mapreduce/partitions_and_executors.md`](../partitions_in_mapreduce/partitions_and_executors.md)
goes deeper into how partitions map onto executors and tasks.

## 12. Transformations vs. Actions: Why Spark Is Lazy

Every RDD operation is one of two kinds, and the distinction
is the single most important thing to internalize before
writing PySpark code:

* **Transformations** (`map`, `flatMap`, `filter`,
  `reduceByKey`, `sortByKey`, `join`, `distinct`, `union`, …)
  build a **new RDD from an existing one**, and are **lazy**
  — calling `words.map(f)` does *not* run `f` over anything;
  it just records "this new RDD, when computed, is `words`
  with `f` applied."
* **Actions** (`collect`, `count`, `take`, `reduce`, `max`,
  `top`, `saveAsTextFile`, `foreach`, …) **trigger actual
  computation** and either return a value to the driver or
  write output — this is the only point at which anything
  actually runs on the cluster.

Chaining transformations therefore builds up a **DAG
(directed acyclic graph)** of *how* to compute an RDD, not
the RDD's contents. Nothing runs until an action forces
evaluation of that DAG, working backward from the action to
the original data source. This has real consequences:

* **`print(some_rdd)`** does not print your data — it prints
  Python's repr of an `RDD` object, because no action has run.
  You need `some_rdd.take(10)` or `.collect()` to actually
  pull values back to the driver.
* Spark can **optimize the whole chain before running any of
  it** — e.g. pipelining a `filter()` immediately followed by
  a `map()` into one pass over the data with no intermediate
  materialization, since it can see both steps before
  executing either.
* Every action **re-runs the full DAG from source** unless
  something was cached along the way (Section 16) — laziness
  means "not yet computed," not "computed once and remembered."

## 13. Narrow vs. Wide Transformations — Where the Shuffle Really Happens

Not all transformations cost the same, and the difference
maps directly onto whether they need a shuffle:

* **Narrow transformations** — `map`, `flatMap`, `filter`,
  `union` — each output partition depends on a fixed, small
  (often exactly one) number of input partitions. Spark can
  compute these **within** each executor, pipelined, with no
  data movement across the network.
* **Wide transformations** — `reduceByKey`, `groupByKey`,
  `sortByKey`, `join`, `distinct`, `repartition` — an output
  partition can depend on data spread across *every* input
  partition, because rows with the same key might currently
  live on different machines. Spark has to **shuffle**: write
  each partition's data out keyed by its destination, and
  have every downstream partition pull the pieces meant for
  it across the network.

```text
narrow:  [p0] [p1] [p2]        wide:    [p0] [p1] [p2]
           |    |    |                   \    |    /
         [p0'][p1'][p2']                  (shuffle: all-to-all)
                                          /    |    \
                                       [p0'] [p1'] [p2']
```

A shuffle is a **stage boundary** — Spark breaks a job's DAG
into stages at every wide transformation, and stages run in
sequence (a later stage can't start until the shuffle
feeding it is done), while everything narrow inside one
stage is pipelined together. This is the same shuffle cost
discussed from the MapReduce side in
[`joins_in_mapreduce/join_operation_in_action_using_MapReduce.md`, Section 9](../joins_in_mapreduce/join_operation_in_action_using_MapReduce.md#9-generalized-join-algorithms-inner-left-outer-right-outer)
— `reduceByKey`/`join` in Spark *are* the tag-group-match
reduce-side join algorithm described there, just with the
tagging, shuffling, and grouping done for you.

## 14. Worked Example — Word Count in PySpark (RDD API)

Same problem as Section 6, same `splitter()` function, now
distributed via PySpark instead of hand-rolled:

```python
from pyspark import SparkContext
from operator import add
import re

def splitter(line):
    line = re.sub(r'^\W+|\W+$', '', line)
    return (w.lower() for w in re.split(r'\W+', line) if w)

if __name__ == '__main__':
    sc = SparkContext("local[*]", "wordcount")

    text = sc.textFile('pg2701.txt')        # 1 RDD element per line
    words = text.flatMap(splitter)          # flatten: many words out per line in
    pairs = words.map(lambda w: (w, 1))     # map step: (word, 1)
    counts = pairs.reduceByKey(add)         # shuffle + reduce step, combined
    best = counts.max(key=lambda kv: kv[1])
    print(f"max: {best[0]} = {best[1]}")
```

```text
max: the = 14620
```

A few differences from the source lesson's version, worth
calling out explicitly:

* **`reduceByKey(add)` instead of `sortByKey()` then
  `reduceByKey(add)`.** The original walks through
  `sortByKey()` as its "shuffle" step, to mirror Section 6's
  explicit sort. But `reduceByKey` already performs its own
  shuffle (grouping by key) as part of combining values — an
  extra `sortByKey()` beforehand shuffles the data a *second*
  time for no benefit, since `reduceByKey`'s own shuffle
  doesn't preserve or need that order. Only add a sort if you
  actually want the *final* output ordered (e.g. before
  `.saveAsTextFile()`), and do it after the reduce, on the
  much smaller reduced result — which is exactly what the
  Top-N example in Section 20 does.
* **`reduceByKey`, not `groupByKey`, for the aggregation.**
  `reduceByKey` combines values for the same key locally on
  each executor **before** shuffling (a map-side combine, the
  same optimization covered for classic MapReduce in
  [`combiners/MapReduce_with_Combiners.md`](../combiners/MapReduce_with_Combiners.md)),
  shipping one partial sum per key per executor across the
  network instead of every individual `(word, 1)` pair.
  `groupByKey` ships every value across the network first and
  combines afterward — for word count on a large corpus, that
  is dramatically more shuffle traffic for the same answer.
* **`.max(key=...)`, not a hand-written running max.** RDDs
  already provide `max()`, `min()`, `top()`, and
  `takeOrdered()` as actions, each accepting a `key=` function
  exactly like Python's own `max()`/`sorted()` — no need to
  reimplement Section 6's `flush()`/running-max logic once
  you're on PySpark.
* **Python 3 syntax throughout** — `functools.reduce` (not a
  builtin), generator-based `splitter()`, f-strings — the
  source lesson uses Python 2 idioms (`sums.iterkeys()`, bare
  `print` statements, `reduce()` as a builtin) that raise
  `NameError`/`AttributeError` under Python 3.

## 15. Broadcast Variables, Accumulators, and Why Closures Need Them

When Spark runs a closure (a lambda or function you pass to
`map`, `filter`, etc.) on a worker, **any variable it
references is serialized and copied to that executor**, and
lives only in that copy's local scope. This has a surprising
consequence: mutating a variable inside a closure does
**not** update the driver's copy, and does **not** propagate
between executors either — each task gets its own independent
copy.

```python
# THIS DOES NOT WORK THE WAY IT LOOKS LIKE IT SHOULD:
bad_counter = 0
def count_it(x):
    global bad_counter
    bad_counter += 1     # increments a COPY on the executor
    return x
sc.parallelize(range(1000)).map(count_it).collect()
print(bad_counter)       # still 0 on the driver!
```

Spark provides two purpose-built mechanisms for the two
legitimate reasons you'd want to share state across the
driver/executor boundary:

* **Broadcast variables** — a **read-only** value pushed out
  once to every executor and cached there, instead of being
  re-serialized into every task's closure. Good for lookup
  tables, stop-word lists, or a small side dataset every task
  needs to read (a map-side "replicated join," per
  [`joins_in_mapreduce`, Section 13's exercise 5](../joins_in_mapreduce/join_operation_in_action_using_MapReduce.md#13-food-for-thought)):

  ```python
  stopwords = sc.broadcast({'the', 'a', 'an', 'of', 'and'})
  filtered = words.filter(lambda w: w not in stopwords.value)
  ```

* **Accumulators** — a **write-only-from-executors,
  read-from-driver** variable that workers can add to using
  an associative operation; Spark handles combining every
  executor's partial updates back at the driver. Typically
  used as distributed counters, e.g. counting malformed input
  lines while mapping the good ones through normally:

  ```python
  bad_lines = sc.accumulator(0)
  def safe_split(line):
      try:
          return splitter(line)
      except Exception:
          bad_lines.add(1)
          return []
  words = text.flatMap(safe_split)
  words.count()          # action, forces evaluation
  print(f"malformed lines: {bad_lines.value}")
  ```

## 16. Fault Tolerance via Lineage, and Caching/Persistence

**How RDDs recover from a lost executor.** HDFS gets
durability from *replication* — every block is stored on
multiple machines, so losing one doesn't lose the data.
RDDs take a different, cheaper approach: Spark records the
**lineage** — the DAG of transformations from Section 12,
i.e. exactly how each RDD was derived from its parents, all
the way back to the original data source. If a partition is
lost (an executor crashes), Spark doesn't need a replica of
that partition sitting somewhere — it just **recomputes**
that partition by re-running its recorded lineage. This is
why RDDs are read-only and transformations always build a new
RDD rather than mutating one in place: immutability is what
makes "just recompute it" a correct recovery strategy.

The tradeoff: a very long, deep lineage (many chained
iterations, e.g. inside an iterative ML loop) means a lost
partition triggers a lot of recomputation. `RDD.checkpoint()`
addresses this by writing an RDD's data to reliable storage
(e.g. HDFS) and truncating its lineage back to that point, at
the cost of the write itself.

**Caching.** Because every action re-runs an RDD's full
lineage (Section 12), an RDD that's used in **more than one**
action — or repeatedly inside a loop — gets recomputed from
scratch every single time, unless you tell Spark to keep it
around:

```python
counts = pairs.reduceByKey(add)
counts.cache()                 # or .persist(StorageLevel.MEMORY_ONLY)
print(counts.count())          # 1st action: computes counts, caches it
print(counts.max(key=lambda kv: kv[1]))   # 2nd action: reuses the cache
```

`.cache()` is shorthand for
`.persist(StorageLevel.MEMORY_ONLY)`. Other storage levels
trade memory for durability or space: `MEMORY_AND_DISK`
(spill to disk instead of recomputing if it doesn't fit in
RAM), `DISK_ONLY`, and `_SER` variants that store a
serialized (more compact, slower to access) representation.
Caching is exactly the optimization that makes Spark fast for
**iterative** workloads (Section 9) — an ML training loop
that reuses the same training-data RDD every iteration only
pays to build it once.

## 17. Worked Example — Counting Primes with `parallelize()`

Not every RDD starts from a file. `sc.parallelize()` turns an
existing in-driver Python collection into a distributed one —
useful for demos and for algorithmically-generated input like
a range of numbers:

```python
from pyspark import SparkContext

def isprime(n):
    """Check whether n is prime by trial division up to sqrt(n)."""
    n = abs(int(n))
    if n < 2:
        return False
    if n == 2:
        return True
    if not n & 1:            # even and > 2
        return False
    for x in range(3, int(n ** 0.5) + 1, 2):
        if n % x == 0:
            return False
    return True

if __name__ == '__main__':
    sc = SparkContext("local[*]", "primes")
    nums = sc.parallelize(range(1_000_000), numSlices=8)
    print(nums.filter(isprime).count())
```

```text
78498
```

`numSlices` (or `numPartitions` on other constructors)
controls how many partitions the collection is split into —
and therefore the maximum parallelism available for it. On a
`local[*]` master this just means concurrent threads on one
machine; true parallelism across multiple *machines* requires
an actual cluster (Section 21 covers the different master
strings). Note also that `filter()` here is narrow
(Section 13) — no shuffle at all, since each partition's
primality check is fully independent of every other partition
— so this job is a single stage.

## 18. Worked Example — Average per Key, and Why Naive Averaging Breaks

Word count and prime counting both reduce with a
function that's safe to apply in *any* order and *any*
grouping: `+` and logical-AND-style filtering are both
associative and commutative, so it doesn't matter which
values a mapper's local combine step sees first, or how the
shuffle happens to group them (Section 14's discussion of
combiners, and
[`combiners/MapReduce_with_Combiners.md`](../combiners/MapReduce_with_Combiners.md),
cover why that property matters). Averaging looks like it
should be just as safe — and it isn't, which makes it a good
example to work through explicitly, and a good reason to
finally show `combineByKey` in real code instead of only
asking for it in Food for Thought.

### The tempting, wrong way

Given per-station temperature readings, it's tempting to
`reduceByKey` with a function that looks like it computes a
running average:

```python
sc = SparkContext("local[*]", "averages")
readings = sc.parallelize([
    ("NYC", 62), ("NYC", 70), ("NYC", 78),
    ("SF", 58), ("SF", 60), ("SF", 61),
    ("LA", 75),
])

naive_avg = readings.reduceByKey(lambda a, b: (a + b) / 2.0)
```

This silently produces the **wrong** answer for any key with
more than two values, and — worse — a *different* wrong
answer depending on the order the shuffle happens to deliver
values in, which is not guaranteed or reproducible.
`functools.reduce` (Section 6) makes this easy to see without
Spark at all, just by feeding NYC's three readings — `62`,
`70`, `78`, true mean `70.0` — through the same combining
function in two different, equally valid orders:

```python
from functools import reduce

naive = lambda a, b: (a + b) / 2.0
print(reduce(naive, [62, 70, 78]))   # (62+70)/2=66, then (66+78)/2 = 72.0
print(reduce(naive, [70, 78, 62]))   # (70+78)/2=74, then (74+62)/2 = 68.0
```

```text
72.0
68.0
```

Two different orderings of the *same three numbers*, through
the *same* function, give two different answers — `72.0` and
`68.0` — and neither one is the true mean, `70.0`. This is
exactly the associativity failure
[`associativity_and_commutativity/Associativity_Commutativity_and_Reducers.md`](../associativity_and_commutativity/Associativity_Commutativity_and_Reducers.md)
covers in depth: `(a + b) / 2` is a perfectly good function
for averaging *two* numbers, but chaining it pairwise across
more than two is not the same operation as "the mean of all
of them," and MapReduce/Spark never promise an order.

### The correct way: defer the division

The fix is the same one Food for Thought exercise 1 asks you
to apply to word count: don't reduce straight to an average —
reduce to a `(sum, count)` pair, which *is* associative and
commutative (it's just two independent sums), and only divide
once, at the very end, on the final merged pair:

```python
sum_count = readings.combineByKey(
    lambda v: (v, 1),                          # createCombiner: first value seen for a key
    lambda acc, v: (acc[0] + v, acc[1] + 1),    # mergeValue: fold one more value in
    lambda a, b: (a[0] + b[0], a[1] + b[1]),    # mergeCombiners: merge two partial (sum, count) pairs
)
averages = sum_count.mapValues(lambda v: v[0] / v[1])
print(sorted(averages.collect()))
```

```text
[('LA', 75.0), ('NYC', 70.0), ('SF', 59.666666666666664)]
```

`NYC` now comes out to the true `70.0`, regardless of how the
shuffle grouped or ordered the three readings — because the
only thing being combined pairwise is `(sum, count)` addition,
which really is associative and commutative, and the
division happens exactly once, after every value has been
folded in.

### The DataFrame version doesn't even give you the footgun

A quick preview of Section 19's `SparkSession`/DataFrame API
— covered properly there, used here just to make the point:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("avg_demo").getOrCreate()
df = spark.createDataFrame(
    [("NYC", 62), ("NYC", 70), ("NYC", 78),
     ("SF", 58), ("SF", 60), ("SF", 61),
     ("LA", 75)],
    ["station", "temperature"])

df.groupBy("station").agg(F.avg("temperature").alias("avg_temp")).orderBy("station").show()
```

```text
+-------+------------------+
|station|          avg_temp|
+-------+------------------+
|     LA|              75.0|
|    NYC|              70.0|
|     SF|59.666666666666664|
+-------+------------------+
```

`F.avg()` is implemented internally with exactly the
sum/count-then-divide strategy above — but as a Spark SQL
built-in, there's no `reduceByKey(naive_avg)` version to
accidentally reach for in the first place. That's a second,
quieter reason (beyond Catalyst's query optimization, Section
19) DataFrames are the recommended default for aggregation
today: some correctness bugs are avoided just by not exposing
the raw reducer.

## 19. Beyond RDDs: DataFrames and Spark SQL

RDDs are untyped, unstructured Python objects as far as Spark
is concerned — it can't see inside a lambda to know that
`kv[1]` is always an `int`, so it can't optimize around that
fact. **DataFrames** add a schema (named, typed columns) on
top of a distributed dataset, which lets Spark's **Catalyst**
query optimizer reorder, combine, and prune operations *before*
running them, and its **Tungsten** engine generate more
efficient code and memory layouts than generic Python objects
allow. For structured or semi-structured data (which is most
real-world data), DataFrames are both less code and
meaningfully faster.

The modern entry point is `SparkSession` — introduced in
Spark 2.0 to unify what used to be three separate driver
objects (`SparkContext`, `SQLContext`, `HiveContext`). It
still exposes the underlying `SparkContext` (and therefore
the full RDD API) via `spark.sparkContext` when you need it:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("wordcount").getOrCreate()

lines = spark.read.text("pg2701.txt")            # 1 column: "value"
words = (lines
    .select(F.explode(F.split(F.lower(F.col("value")), r"\W+")).alias("word"))
    .filter(F.col("word") != ""))
counts = words.groupBy("word").count()

counts.orderBy(F.desc("count")).show(1)
```

```text
+----+-----+
|word|count|
+----+-----+
| the|14620|
+----+-----+
```

The same job as a SQL query, once `counts` (or the raw
`words`) is registered as a temp view — useful when a query
is more naturally expressed declaratively, or written by
someone who knows SQL but not the DataFrame API:

```python
words.createOrReplaceTempView("words")
spark.sql("""
    SELECT word, COUNT(*) AS count
    FROM words
    GROUP BY word
    ORDER BY count DESC
    LIMIT 1
""").show()
```

Both produce identical results and, in fact, an **identical
physical execution plan** — `spark.sql(...)` compiles down to
the same Catalyst plan the DataFrame calls build directly, so
picking one over the other is purely a matter of which reads
more naturally for a given job.

## 20. Worked Example — Top-N Words (RDD and DataFrame)

Section 14's word count already finds the single most
frequent word via `.max()`. Extending that to the top-N most
frequent words is the same "local top-N, then merge" pattern
used in
[`joins_in_mapreduce`, Section 8](../joins_in_mapreduce/join_operation_in_action_using_MapReduce.md#8-worked-example-goal-3--top-n-customers-by-spending)
— and on RDDs, it's built in, so there's no heap to write by
hand:

```python
N = 10
# counts here is the RDD from Section 14: reduceByKey(add) over (word, 1) pairs
top_n = counts.top(N, key=lambda kv: kv[1])   # RDD API
for word, count in top_n:
    print(f"{word}\t{count}")
```

`RDD.top(N, key=...)` computes a bounded top-N *per
partition* first (exactly the local min-heap step from the
joins article's Section 8, linked above), then merges those
partial results at the driver — it never
pulls the full, unbounded `counts` RDD back with `.collect()`
first, which is the mistake to avoid: `counts.collect()`
sorted and sliced in the driver would force every single
`(word, count)` pair across the network just to keep 10 of
them.

The DataFrame equivalent reads even more directly (here
`counts` is Section 19's DataFrame instead — same name,
different type, so don't mix the two up):

```python
counts.orderBy(F.desc("count")).limit(N).show()
```

Catalyst recognizes the `orderBy().limit(N)` pattern and
plans it as a **top-N**, not a full distributed sort followed
by a truncation — internally, the same
local-bounded-then-merge strategy `RDD.top()` uses explicitly.

## 21. Running It: Shell, `spark-submit`, and Notebooks

Three ways to actually run the code in this article:

* **`pyspark` shell** — an interactive REPL (Section 9)
  with a `SparkContext` already created for you as `sc`
  (and a `SparkSession` as `spark`); good for exploring an
  RDD/DataFrame interactively, one transformation at a time.
* **`spark-submit`** — the way to run a finished script as a
  batch job, same as the source lesson uses:

  ```text
  spark-submit wordcount_spark.py
  ```

  For a real cluster, `spark-submit` also takes `--master`,
  `--num-executors`, `--executor-memory`, and similar flags
  that override whatever the script passed to
  `SparkContext(...)`.
* **Jupyter notebooks** — via `findspark.init()` or a
  `PYSPARK_DRIVER_PYTHON=jupyter` environment setup, useful
  for the same exploratory workflow as the shell but with
  persistent cells and inline output.

The **master** string passed to `SparkContext`/
`SparkSession.builder` says where executors come from:

| Master string | Meaning |
|---|---|
| `local` | Single thread, one machine — no real parallelism, good for tiny tests |
| `local[*]` | One thread per available CPU core, one machine — real parallelism, still one machine |
| `yarn` | Executors allocated by a YARN cluster manager |
| `spark://host:port` | Executors allocated by Spark's own standalone cluster manager |
| `k8s://https://host:port` | Executors run as pods on a Kubernetes cluster |

Every code example in this article used `local[*]`
deliberately, so it's runnable on a laptop with just
`pip install pyspark` — swapping in a real master string is
the only change needed to run the same code on an actual
cluster.

## 22. Common Pitfalls

1. **Treating an RDD/DataFrame like its contents.**
   `print(rdd)` or `if rdd:` inspect the *object*, not the
   data — nothing has been computed yet (Section 12). Use an
   action (`.take()`, `.collect()`, `.count()`, `.show()`).
2. **Mutating driver-side state from inside a closure.**
   Covered in Section 15 — it silently does nothing useful;
   reach for a broadcast variable (read) or accumulator
   (write) instead.
3. **Calling `.collect()` on something huge.** It pulls every
   partition back to the driver's memory — fine for a
   reduced/aggregated result, likely to crash the driver on a
   raw, unreduced dataset. Prefer `.take(n)`, `.top(n)`, or
   writing straight to storage with `.saveAsTextFile()`
   /`.write`.
4. **Forgetting to cache a reused RDD/DataFrame.** Every
   action re-walks the full lineage (Section 12) unless
   something along the way was `.cache()`d (Section 16) —
   easy to accidentally recompute an expensive join or
   aggregation many times inside a loop.
5. **`groupByKey()` where `reduceByKey()`/`aggregateByKey()`
   would do.** `groupByKey` ships every raw value across the
   shuffle before combining anything; `reduceByKey` combines
   locally first (Section 14) — same result, far less network
   traffic for anything associative like sum/count/max.
6. **Ignoring partition count.** Too few partitions
   under-utilizes a cluster; too many creates scheduling
   overhead that swamps tiny tasks. `numSlices`/
   `numPartitions` at creation, or `repartition()`/
   `coalesce()` afterward, control this directly; for
   DataFrames, `spark.sql.shuffle.partitions` controls the
   post-shuffle partition count.

## 23. Key Terms (Glossary)

* **RDD** — Resilient Distributed Dataset; an immutable,
  partitioned collection, recomputable from its lineage.
* **DataFrame** — a schema'd, columnar distributed dataset;
  Spark SQL's primary data structure, optimized by Catalyst.
* **Driver** — the process running your `main()`; builds the
  DAG and coordinates executors.
* **Executor** — a worker process that runs tasks against the
  partitions assigned to it, and manages their caching.
* **Cluster manager** — allocates executors across machines
  (Spark standalone, YARN, Kubernetes, Mesos).
* **Partition** — one chunk of an RDD/DataFrame, the unit of
  parallelism and of task scheduling.
* **Transformation** — a lazy operation building a new
  RDD/DataFrame from an existing one (narrow or wide,
  Section 13).
* **Action** — an operation that triggers actual computation
  and returns/writes a result.
* **Shuffle** — redistributing data across the network so
  rows with the same key end up on the same partition; the
  cost driver behind wide transformations.
* **Stage** — a chunk of the DAG between shuffle boundaries,
  internally pipelined with no data movement.
* **Task** — one stage's work on one partition; the smallest
  unit Spark schedules onto an executor.
* **Lineage / DAG** — the recorded chain of transformations
  used to recompute a lost partition (Section 16).
* **Broadcast variable** — a read-only value cached once per
  executor instead of re-shipped per task (Section 15).
* **Accumulator** — a write-from-executors, read-from-driver
  aggregator, typically used as a distributed counter
  (Section 15).
* **Lazy evaluation** — transformations only describe work;
  nothing runs until an action (Section 12).
* **SparkSession** — the modern unified entry point
  (replaces `SparkContext`/`SQLContext`/`HiveContext`).

## 24. Food for Thought

1. Rewrite Section 14's `reduceByKey(add)` using
   `combineByKey()` instead, on the same three-function model
   Section 18's average example uses (create a combiner, merge
   a value into a combiner, merge two combiners). Confirm the
   output is identical, and explain why `reduceByKey` is
   really just `combineByKey` with the "create" and "merge"
   functions forced to be the same associative function.
2. Section 14 removed the source lesson's
   `sortByKey()` step before `reduceByKey()`. Trace through
   what `reduceByKey`'s own shuffle does to any ordering
   `sortByKey()` had produced — does the sort survive?
3. Using `RDD.top(N, key=...)` from Section 20, find the 10
   largest primes below 1,000,000 from Section 17's `nums`
   RDD, without ever calling `.collect()` on the full
   1,000,000-element RDD.
4. Section 16 says caching doesn't remove the need for
   lineage tracking. Why not — what still needs the lineage
   even for a cached RDD?
5. Rewrite Section 19's DataFrame word count so that words
   shorter than a broadcast-variable-configured minimum
   length are excluded, mirroring
   [`word_count_in_mapreduce/word_count_in_mapreduce.md`, Section 7](../word_count_in_mapreduce/word_count_in_mapreduce.md)'s
   mapper-side length filter but expressed as a DataFrame
   `.filter()`.
6. Extend Section 18's `combineByKey` example to also track
   each station's minimum and maximum reading in the same
   pass — i.e. combine to `(sum, count, min, max)` instead of
   `(sum, count)` — and confirm `min`/`max` didn't actually
   need this treatment: unlike averaging, why were they safe
   to compute with a plain `reduceByKey` all along?

## 25. Comments

Comments and suggestions are welcome!

## 26. References

1. NYU Center for Data Science, ["BigData with PySpark: Introduction"](https://nyu-cds.github.io/python-bigdata/01-introduction/) — source of Sections 1-2, 9-10
2. NYU Center for Data Science, ["BigData with PySpark: MapReduce Primer"](https://nyu-cds.github.io/python-bigdata/02-mapreduce/) — source of Sections 4-8
3. NYU Center for Data Science, ["BigData with PySpark: Introduction to Spark"](https://nyu-cds.github.io/python-bigdata/03-spark/) — source of Sections 11, 14-15, 17
4. Jeffrey Dean and Sanjay Ghemawat, "MapReduce: Simplified
   Data Processing on Large Clusters" (2004) — kept locally at
   [`google_mapreduce_paper/`](../google_mapreduce_paper/MapReduce_Simplified_Data_Processing_on_Large_Clusters_by_Jeff_Dean.pdf)
5. [Apache Spark RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
6. [Apache Spark SQL, DataFrames and Datasets Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
7. [`word_count_in_mapreduce/word_count_in_mapreduce.md`](../word_count_in_mapreduce/word_count_in_mapreduce.md) — full mapper/reducer trace of word count, plus combiners
8. [`combiners/MapReduce_with_Combiners.md`](../combiners/MapReduce_with_Combiners.md) — the map-side combine optimization referenced in Section 14
9. [`partitions_in_mapreduce/partitions_and_executors.md`](../partitions_in_mapreduce/partitions_and_executors.md) — a deeper look at partitions and executors, referenced in Section 11
10. [`joins_in_mapreduce/join_operation_in_action_using_MapReduce.md`](../joins_in_mapreduce/join_operation_in_action_using_MapReduce.md) — the reduce-side join and Top-N patterns referenced in Sections 13, 15, and 20
11. [`associativity_and_commutativity/Associativity_Commutativity_and_Reducers.md`](../associativity_and_commutativity/Associativity_Commutativity_and_Reducers.md) — the full theory behind why naive averaging breaks, referenced in Section 18
12. [`mapreduce_in_action_by_pyspark/mapreduce_in_action_with_pyspark_examples.md`](../mapreduce_in_action_by_pyspark/mapreduce_in_action_with_pyspark_examples.md) — a larger catalog of worked PySpark MapReduce examples
13. Benjamin Bengfort, "Getting Started with Spark (in Python)" — cited in reference 3 above as further reading
14. Lucas Allen, "Spark DataFrames and MLlib" — cited in reference 3 above as further reading
