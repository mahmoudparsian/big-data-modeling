# Word Count in MapReduce — with `map()` and `reduce()`

	Author: Mahmoud Parsian
	Last updated: 8/29/2026
	
**What is a  Word Count problem in MapReduce?**<br>
A word count problem in MapReduce is a 
classic **distributed computing task** 
that counts how many times each word 
appears in a large collection of text 
(such a text files/documents).

## Table of Contents

1. Goal
2. Why Python syntax, and why it matters for PySpark later
3. INPUT: multiple files in `data/`
4. Step 1 — input files are mapped to `(filename, record)` pairs
5. Step 2 — the `mapper()` function
6. Step 3 — output of every mapper call (all 12 of them)
7. Mappers with Filters: ignore words shorter than M
8. Step 4 — shuffle & sort: group by key
9. Step 5 — the `reducer()` function
10. Step 6 — output of every reducer call (all 8 of them)
11. Reducers with Filters: ignore words with frequency less than N
12. Final Output
13. Combiners in MapReduce
14. `combiner()` function
15. Output of final shuffle & sort (after combiners)
16. Reducer output (after all combiners are done)
17. Looking ahead: this becomes a PySpark job
18. PySpark with Filters: M and N
19. Homework

## 1. Goal

Walk through classic MapReduce word count step by step,
showing **every** mapper call and **every** reducer call
for a small multi-file input — nothing is abbreviated 
with "...". The `mapper()` / `reducer()` functions below 
are written in real Python syntax (not `{ }` pseudocode) 
on purpose: a later lecture ports this *exact* pair 
of functions to PySpark, so the shapes matter now.

## 2. Why Python syntax, and why it matters for PySpark later

* **`mapper(key, value)`** <br>
  returns a **list of `(word, 1)` pairs**. <br>
  That's precisely what you hand to PySpark's
  `rdd.flatMap(...)` — a function that turns 
  one input record into zero or more output pairs.
  
* **`reducer(word, counts)`** <br>
  below is written as a **binary** function 
  — `f(a, b) -> combined` — because that's what
  PySpark's `rdd.reduceByKey(f)` expects. It never 
  sees the full list of values for a key at once; 
  it repeatedly combines two values until one remains. 
  That's exactly what Python's own `functools.reduce()` 
  does, which is where this document's title comes from.

So: this write-up already uses the vocabulary 
and shapes that `rdd.flatMap(mapper)` and 
`rdd.reduceByKey(reducer)` will use, later, 
almost unchanged.

## 3. INPUT: multiple files in `data/`

Word count input is rarely one file — it's 
a **directory** of files (see [`data/`](data/)). 
Each file is shorter here than in 
[`word_count_in_python/data/`](../word_count_in_python/data/)
so that every mapper and reducer call below can be shown 
in full.

~~~text
$ cat data/file1.txt
fox jumped
red fox
fox jumped high

$ cat data/file2.txt
gray fox
fox jumped
red fox jumped
fox is quick

$ cat data/file3.txt
fox jumped
gray fox jumped
red fox
fox is red
fox ran
~~~

3 files, 12 records (records/lines) total: 3 + 4 + 5.

## 4. Step 1 — input files are mapped to `(filename, record)` pairs

Before any mapper runs, the MapReduce framework splits every
input file into records and pairs each record with the name
of the file it came from:

~~~text
key   = filename
value = one record (one line of text)
~~~

That `(filename, record)` pair is what gets passed 
to a mapper — not the raw file. (Word count itself 
never looks at the filename; it's included here to 
make explicit *where* each record came from, and 
because it's exactly the `(key, value)` shape 
`sc.wholeTextFiles()` produces in PySpark — see 
the preview at the end.)

Every one of the 12 records, and the pair it's 
passed to a mapper as:

| filename | record # | record | passed to a mapper as |
|---|---|---|---|
| `file1.txt` | 1 | `fox jumped` | `("file1.txt", "fox jumped")` |
| `file1.txt` | 2 | `red fox` | `("file1.txt", "red fox")` |
| `file1.txt` | 3 | `fox jumped high` | `("file1.txt", "fox jumped high")` |
| `file2.txt` | 1 | `gray fox` | `("file2.txt", "gray fox")` |
| `file2.txt` | 2 | `fox jumped` | `("file2.txt", "fox jumped")` |
| `file2.txt` | 3 | `red fox jumped` | `("file2.txt", "red fox jumped")` |
| `file2.txt` | 4 | `fox is quick` | `("file2.txt", "fox is quick")` |
| `file3.txt` | 1 | `fox jumped` | `("file3.txt", "fox jumped")` |
| `file3.txt` | 2 | `gray fox jumped` | `("file3.txt", "gray fox jumped")` |
| `file3.txt` | 3 | `red fox` | `("file3.txt", "red fox")` |
| `file3.txt` | 4 | `fox is red` | `("file3.txt", "fox is red")` |
| `file3.txt` | 5 | `fox ran` | `("file3.txt", "fox ran")` |


## 5. Step 2 — the `mapper()` function

~~~python
def mapper(filename, record):
    """
    map(key, value) -> list of (word, 1) pairs

    key   = filename            (ignored — word count doesn't
                                  care which file a word is in)
    value = record               (one line of text)
    """
    words = record.split()
    return [(word, 1) for word in words]
~~~

`mapper()` is called once per `(filename, record)` 
pair — it never sees any other record, and it never 
sees any other mapper's output. That's what makes 
it safe to run in parallel, one mapper per record 
(or per batch of records), across a cluster.

## 6. Step 3 — output of every mapper call (all 12 of them)

~~~text
mapper("file1.txt", "fox jumped")       -> [("fox", 1), ("jumped", 1)]
mapper("file1.txt", "red fox")          -> [("red", 1), ("fox", 1)]
mapper("file1.txt", "fox jumped high")  -> [("fox", 1), ("jumped", 1), ("high", 1)]

mapper("file2.txt", "gray fox")         -> [("gray", 1), ("fox", 1)]
mapper("file2.txt", "fox jumped")       -> [("fox", 1), ("jumped", 1)]
mapper("file2.txt", "red fox jumped")   -> [("red", 1), ("fox", 1), ("jumped", 1)]
mapper("file2.txt", "fox is quick")     -> [("fox", 1), ("is", 1), ("quick", 1)]

mapper("file3.txt", "fox jumped")       -> [("fox", 1), ("jumped", 1)]
mapper("file3.txt", "gray fox jumped")  -> [("gray", 1), ("fox", 1), ("jumped", 1)]
mapper("file3.txt", "red fox")          -> [("red", 1), ("fox", 1)]
mapper("file3.txt", "fox is red")       -> [("fox", 1), ("is", 1), ("red", 1)]
mapper("file3.txt", "fox ran")          -> [("fox", 1), ("ran", 1)]
~~~

Flattened, that's **29** `(word, 1)` pairs crossing 
from mappers into the shuffle — one per word token 
in the input (matches `wc -w data/*.txt`).

## 7. Mappers with Filters: ignore words shorter than M

A mapper filter is a **local** decision — it only 
needs the word itself, nothing about its overall 
frequency across the input — so it can run before 
the shuffle, cutting the amount of data that ever 
crosses the network:

~~~python
M = 3  # minimum word length to keep

def mapper_with_filter(filename, record):
    """
    map(key, value) -> list of (word, 1) pairs,
    dropping any word shorter than M characters
    """
    words = record.split()
    return [(word, 1) for word in words if len(word) >= M]
~~~

Only two of the 12 records contain a word 
shorter than `M = 3` — both contain `is` 
(2 characters) — so only those two calls change; 
the other 10 are identical to Step 3:

~~~text
mapper_with_filter("file1.txt", "fox jumped")       -> [("fox", 1), ("jumped", 1)]
mapper_with_filter("file1.txt", "red fox")          -> [("red", 1), ("fox", 1)]
mapper_with_filter("file1.txt", "fox jumped high")  -> [("fox", 1), ("jumped", 1), ("high", 1)]

mapper_with_filter("file2.txt", "gray fox")         -> [("gray", 1), ("fox", 1)]
mapper_with_filter("file2.txt", "fox jumped")       -> [("fox", 1), ("jumped", 1)]
mapper_with_filter("file2.txt", "red fox jumped")   -> [("red", 1), ("fox", 1), ("jumped", 1)]
mapper_with_filter("file2.txt", "fox is quick")     -> [("fox", 1), ("quick", 1)]              # "is" dropped (len 2 < M)

mapper_with_filter("file3.txt", "fox jumped")       -> [("fox", 1), ("jumped", 1)]
mapper_with_filter("file3.txt", "gray fox jumped")  -> [("gray", 1), ("fox", 1), ("jumped", 1)]
mapper_with_filter("file3.txt", "red fox")          -> [("red", 1), ("fox", 1)]
mapper_with_filter("file3.txt", "fox is red")       -> [("fox", 1), ("red", 1)]                # "is" dropped (len 2 < M)
mapper_with_filter("file3.txt", "fox ran")          -> [("fox", 1), ("ran", 1)]
~~~

`is` never reaches the shuffle at all — both of 
its occurrences (the only two in the whole input) 
are gone before anything is grouped, so the flattened 
pair count drops from 29 (Step 3) to **27**. If Step 
4 were re-run on this filtered output, `is` would simply 
be absent from the grouped keys — the reducer never even 
gets *called* for it. (Contrast this with the frequency 
filter below, which *does* still call the reducer for 
every key.)

## 8. Step 4 — shuffle & sort: group by key

The framework collects all 29 `(word, 1)` pairs 
from every mapper, groups them by `word`, and 
hands each reducer a `(word, list_of_1s)` pair:

~~~text
(fox,    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])   # 12 ones
(jumped, [1, 1, 1, 1, 1, 1])                     # 6 ones
(red,    [1, 1, 1, 1])                           # 4 ones
(gray,   [1, 1])                                 # 2 ones
(is,     [1, 1])                                 # 2 ones
(high,   [1])
(quick,  [1])
(ran,    [1])
~~~

8 unique keys (words) come out of shuffle & sort 
— that's also how many times `reducer()` will be 
invoked, once per key.

## 9. Step 5 — the `reducer()` function

Two equivalent ways to write it. Both matter: the first
reads naturally; the second is the one that ports directly
to PySpark's `reduceByKey()`.

**Aggregate form** — takes the whole list of values at once
(this is how Hadoop's `Reducer.reduce(key, Iterable<value>)`
is shaped):

~~~python
def reducer(word, counts):
    """
    reduce(key, values) -> (key, total)
    counts: list of 1's for this word, e.g. [1, 1, 1]
    """
    total = sum(counts)
    return (word, total)
~~~

**Binary / `functools.reduce()` form** — combines values two
at a time (this is how PySpark's `reduceByKey(f)` is shaped —
`f` is a binary function, applied pairwise until one value
remains per key):

~~~python
from functools import reduce

def combine(a, b):
    return a + b

def reducer(word, counts):
    total = reduce(combine, counts)   # same as functools.reduce(combine, counts)
    return (word, total)
~~~

`combine` is exactly the function you'll later 
pass straight to `rdd.reduceByKey(combine)` in 
PySpark —  no translation needed, because addition 
is **associative** and **commutative**: it doesn't 
matter what order the 1's are combined in, or how 
they're split across partitions, the total is always
the same.

## 10. Step 6 — output of every reducer call (all 8 of them)

~~~text
reducer("fox",    [1]*12) -> ("fox", 12)
reducer("jumped", [1]*6)  -> ("jumped", 6)
reducer("red",    [1]*4)  -> ("red", 4)
reducer("gray",   [1]*2)  -> ("gray", 2)
reducer("is",     [1]*2)  -> ("is", 2)
reducer("high",   [1])    -> ("high", 1)
reducer("quick",  [1])    -> ("quick", 1)
reducer("ran",    [1])    -> ("ran", 1)
~~~

## 11. Reducers with Filters: ignore words with frequency less than N

A reducer filter is a **global** decision — it 
depends on the *total* count for a word, and only 
the reducer ever sees that total (a single mapper 
only ever sees one record at a time), so this filter 
can only be applied here, after aggregation:

~~~python
N = 2  # minimum frequency to keep

def reducer_with_filter(word, counts):
    """
    reduce(key, values) -> (key, total), or None
    if the total is below N
    """
    total = sum(counts)
    if total < N:
        return None
    return (word, total)
~~~

Applied to all 8 reducer calls from Step 6:

~~~text
reducer_with_filter("fox",    [1]*12) -> ("fox", 12)
reducer_with_filter("jumped", [1]*6)  -> ("jumped", 6)
reducer_with_filter("red",    [1]*4)  -> ("red", 4)
reducer_with_filter("gray",   [1]*2)  -> ("gray", 2)
reducer_with_filter("is",     [1]*2)  -> ("is", 2)
reducer_with_filter("high",   [1])    -> None   # 1 < N, dropped
reducer_with_filter("quick",  [1])    -> None   # 1 < N, dropped
reducer_with_filter("ran",    [1])    -> None   # 1 < N, dropped
~~~

Output with the filter applied:

~~~text
(fox, 12)
(jumped, 6)
(red, 4)
(gray, 2)
(is, 2)
~~~

Notice this filter **keeps** `is` — its frequency 
(2) meets the `N = 2` threshold — while the *mapper* 
filter above dropped `is` unconditionally, on length 
alone. Same word, opposite outcome, because the two 
filters look at completely different information: one 
word at a time vs. the aggregated total. See the "Mapper 
filter vs. reducer filter" comparison table in
[`mapreduce_examples/MapReduce_Word_Count.md`](../mapreduce_examples/MapReduce_Word_Count.md)
for the general rule of thumb.

## 12. Final Output

~~~text
(fox, 12)
(jumped, 6)
(red, 4)
(gray, 2)
(is, 2)
(high, 1)
(quick, 1)
(ran, 1)
~~~

`12 + 6 + 4 + 2 + 2 + 1 + 1 + 1 = 29` — matches the 
29 tokens counted in Step 3, confirming no word was 
lost or double counted along the way. Sorted alphabetically 
(the convention used by 
[`word_count_dir_to_tsv.py`](../word_count_in_python/word_count_dir_to_tsv.py)'s
TSV output):

~~~text
fox     12
gray    2
high    1
is      2
jumped  6
quick   1
ran     1
red     4
~~~

## 13. Combiners in MapReduce

A combiner is an **optional** step that runs locally, 
on a single partition's worth of mapper output, *before* 
that output is shuffled across the network to a reducer. 
It's shaped exactly like a reducer, but it only ever sees 
the values produced within its own partition — never the 
whole dataset. The point is to cut how much data crosses 
the network, without changing the final answer.

To keep the partitioning simple here, route by source 
file: everything `mapper()` produced from `file1.txt` 
goes to partition **P1**, `file2.txt`'s output goes 
to **P2**, and `file3.txt`'s output goes to **P3**. 
(This mirrors the 3 files from Step 1 — a common real-world 
case, since one input split often *is* one file.) Flattening 
Step 3's output by file:

~~~text
P1 (from file1.txt, 7 pairs):
(fox, 1), (jumped, 1), (red, 1), (fox, 1), (fox, 1), (jumped, 1), (high, 1)

P2 (from file2.txt, 10 pairs):
(gray, 1), (fox, 1), (fox, 1), (jumped, 1), (red, 1), (fox, 1), (jumped, 1), (fox, 1), (is, 1), (quick, 1)

P3 (from file3.txt, 12 pairs):
(fox, 1), (jumped, 1), (gray, 1), (fox, 1), (jumped, 1), (red, 1), (fox, 1), (fox, 1), (is, 1), (red, 1), (fox, 1), (ran, 1)
~~~

## 14. `combiner()` function

Same shape as `reducer()`'s aggregate form — 
that's not a coincidence, it's *why* a combiner 
is safe to use for word count: addition is associative 
and commutative, so summing a subset of the 1's early 
(per partition) and re-summing those partial sums later 
(in the reducer) gives the exact same final total.

~~~python
def combiner(word, counts):
    """
    combine(key, values) -> (key, partial_total)
    counts: the 1's for this word within ONE partition only
    """
    partial_total = sum(counts)
    return (word, partial_total)
~~~

Each partition first groups its own pairs by word, then
calls `combiner()` once per word it contains:

~~~text
P1 grouped -> combiner() -> P1 output
(fox,    [1, 1, 1]) -> combiner("fox", [1,1,1])    -> (fox, 3)
(jumped, [1, 1])    -> combiner("jumped", [1,1])   -> (jumped, 2)
(red,    [1])       -> combiner("red", [1])        -> (red, 1)
(high,   [1])       -> combiner("high", [1])       -> (high, 1)

P2 grouped -> combiner() -> P2 output
(fox,    [1, 1, 1, 1]) -> combiner("fox", [1,1,1,1])  -> (fox, 4)
(jumped, [1, 1])       -> combiner("jumped", [1,1])   -> (jumped, 2)
(red,    [1])          -> combiner("red", [1])        -> (red, 1)
(gray,   [1])          -> combiner("gray", [1])       -> (gray, 1)
(is,     [1])          -> combiner("is", [1])         -> (is, 1)
(quick,  [1])          -> combiner("quick", [1])      -> (quick, 1)

P3 grouped -> combiner() -> P3 output
(fox,    [1, 1, 1, 1, 1]) -> combiner("fox", [1,1,1,1,1]) -> (fox, 5)
(jumped, [1, 1])          -> combiner("jumped", [1,1])    -> (jumped, 2)
(gray,   [1])             -> combiner("gray", [1])        -> (gray, 1)
(red,    [1, 1])          -> combiner("red", [1,1])       -> (red, 2)
(is,     [1])             -> combiner("is", [1])          -> (is, 1)
(ran,    [1])             -> combiner("ran", [1])         -> (ran, 1)
~~~

With a combiner, only **16** `(word, partial_total)` pairs
cross into the final shuffle — down from the 29 raw pairs in
Step 3, a 44.8% cut — and the savings would grow with a
bigger, more repetitive input.

## 15. Output of final shuffle & sort (after combiners)

The final shuffle groups the 3 partitions' combiner outputs
by word — note the values are now **partial sums**
(`3`, `4`, `5`, ...), not raw `1`'s:

~~~text
(fox,    [3, 4, 5])   # from P1, P2, P3
(jumped, [2, 2, 2])   # from P1, P2, P3
(red,    [1, 1, 2])   # from P1, P2, P3
(gray,   [1, 1])      # from P2, P3 -- P1 never saw "gray"
(is,     [1, 1])      # from P2, P3 -- P1 never saw "is"
(high,   [1])         # from P1 only
(quick,  [1])         # from P2 only
(ran,    [1])         # from P3 only
~~~

## 16. Reducer output (after all combiners are done)

The same `reducer()` from Step 5 runs unchanged — it doesn't
know or care whether its input values are raw 1's or
partial sums from a combiner:

~~~text
reducer("fox",    [3, 4, 5]) -> ("fox", 12)
reducer("jumped", [2, 2, 2]) -> ("jumped", 6)
reducer("red",    [1, 1, 2]) -> ("red", 4)
reducer("gray",   [1, 1])    -> ("gray", 2)
reducer("is",     [1, 1])    -> ("is", 2)
reducer("high",   [1])       -> ("high", 1)
reducer("quick",  [1])       -> ("quick", 1)
reducer("ran",    [1])       -> ("ran", 1)
~~~

Identical to Step 6's output — the combiner only changed
**how much data moved**, never **what the answer was**. See
[`combiners/Word_Count_in_MapReduce.md`](../combiners/Word_Count_in_MapReduce.md)
for a similar worked example — same combiner idea, applied
to a different sample input and a different partitioning
scheme — and
[`combiners/MapReduce_with_Combiners.md`](../combiners/MapReduce_with_Combiners.md)
for cases (like `average`) where a naive combiner *would*
silently break correctness.

## 17. Looking ahead: this becomes a PySpark job

A later lecture ports `mapper()` and `combine()` above,
unchanged, into a real PySpark job — `flatMap` replaces the
mapper loop, `reduceByKey` replaces shuffle/sort + reducer:

~~~python
# preview only — worked through fully in the PySpark lecture
files = sc.wholeTextFiles("data/")          # RDD of (filename, full_file_text)

records = files.flatMap(
    lambda kv: [(kv[0], line) for line in kv[1].splitlines() if line.strip()]
)                                            # RDD of (filename, record) -- Step 1

pairs = records.flatMap(
    lambda kv: mapper(kv[0], kv[1])
)                                            # RDD of (word, 1) -- Steps 2-3

counts = pairs.reduceByKey(combine)          # RDD of (word, total) -- Steps 4-6

for word, total in sorted(counts.collect()):
    print(word, total)
~~~

Notice `mapper()` and `combine()` are called **as-is** — no
rewrite required. That's the payoff of writing this
walkthrough in Python syntax with a binary reducer from the
start. See
[`mapreduce_in_action_by_pyspark/mapreduce_in_action_with_pyspark_examples.md`](../mapreduce_in_action_by_pyspark/mapreduce_in_action_with_pyspark_examples.md)'s
"Example 1: Word Count" for this same `flatMap`/`reduceByKey`
skeleton run as an actual PySpark session, with real
`.count()`/`.collect()` output at every step.

## 18. PySpark with Filters: M and N

Same `M = 3` and `N = 2` thresholds as the "Mappers with
Filters" and "Reducers with Filters" sections above, now
expressed as two extra transformations on the pipeline from
"Looking ahead":

~~~python
# preview only — worked through fully in the PySpark lecture
M = 3   # minimum word length to keep
N = 2   # minimum frequency to keep

files = sc.wholeTextFiles("data/")

records = files.flatMap(
    lambda kv: [(kv[0], line) for line in kv[1].splitlines() if line.strip()]
)                                               # (filename, record) -- Step 1

pairs = records.flatMap(
    lambda kv: mapper(kv[0], kv[1])
)                                               # (word, 1), unfiltered -- Steps 2-3

pairs_filtered = pairs.filter(
    lambda word_count: len(word_count[0]) >= M
)                                               # mapper-side filter -- M

counts = pairs_filtered.reduceByKey(combine)    # (word, total) -- Steps 4-6

counts_filtered = counts.filter(
    lambda word_total: word_total[1] >= N
)                                                # reducer-side filter -- N

for word, total in sorted(counts_filtered.collect()):
    print(word, total)
~~~

Output (both filters together, unlike the single-filter
sections above):

~~~text
fox     12
gray    2
jumped  6
red     4
~~~

Two things worth noticing:

* `pairs.filter(...)` runs **before** `reduceByKey` — on the
  raw `(word, 1)` pairs — which is exactly equivalent to
  embedding the same check inside `mapper_with_filter()` in
  "Mappers with Filters" above. Either placement works,
  because the decision only ever needs the word itself.
* `counts.filter(...)` can only run **after** `reduceByKey`,
  because `N` is a property of the *aggregated* total, and
  no earlier point in the pipeline has that total yet. This
  is the PySpark version of "Reducers with Filters" above —
  the same mapper-filter-vs-reducer-filter distinction from
  [`mapreduce_examples/MapReduce_Word_Count.md`](../mapreduce_examples/MapReduce_Word_Count.md),
  now expressed as `.filter()` placement instead of an `if`
  inside `map()`/`reduce()`.

## 19. Homework

1. Run the pseudocode above by hand (or in a Python REPL) on
   [`word_count_in_python/data/`](../word_count_in_python/data/)
   instead of this folder's shortened `data/` — you won't be
   able to list every mapper call anymore without it getting
   unwieldy; explain why (compare record and token counts).
2. Run the "PySpark with Filters" pipeline above (in a real
   Spark shell or a local PySpark install) against
   [`word_count_in_python/data/`](../word_count_in_python/data/)
   with `M = 3` and `N = 2`, and confirm it produces the same
   words/counts as running
   [`word_count_dir_to_tsv_with_filter.py`](../word_count_in_python/word_count_dir_to_tsv_with_filter.py)
   on that same folder with the same `M` and `N`.
