# People You May Know (PYMK)

Five runnable implementations of the classic "People You May Know"
mutual-friend recommendation algorithm:

* [`pymk_pure_python.py`](pymk_pure_python.py) — a dependency-free,
  standard-library-only Python script that *simulates* MapReduce's
  Map → Shuffle → Reduce phases with plain dicts and lists, so the
  mechanics are visible line by line.
* [`pymk_pyspark_using_groupbykey.py`](pymk_pyspark_using_groupbykey.py) — the same algorithm as a
  real, standalone PySpark RDD job (`flatMap` → `groupByKey` →
  `mapValues`), runnable locally or via `spark-submit` on a cluster.
* [`pymk_pyspark_using_reducebykey.py`](pymk_pyspark_using_reducebykey.py) —
  the same PySpark job again, but aggregating with `reduceByKey()`
  and a map-side combiner instead of `groupByKey()` — see
  [Section 7](#7-how-the-five-implementations-map-onto-each-other) for
  why that requires reshaping what the mapper emits, not just
  swapping one method call for another.
* [`pymk_pyspark_using_dataframes.py`](pymk_pyspark_using_dataframes.py) —
  the same algorithm again, this time on the Spark SQL DataFrame API
  instead of RDDs: `explode`/`least`/`greatest` for Rule 1, a
  self-join for Rule 2, `groupBy().agg()` for the shuffle+reduce, a
  `left_anti` join to drop already-friend pairs, and a ranking
  `Window` function in place of a second `groupByKey().mapValues(sort)`.
* [`pymk_pyspark_using_graphframes.py`](pymk_pyspark_using_graphframes.py) —
  the same algorithm expressed as a single declarative graph
  pattern-match query over a [GraphFrames](https://graphframes.io/)
  `GraphFrame` instead of joins: `g.find("(a)-[]->(c); (b)-[]->(c)")`
  finds every pair sharing a mutual friend `c` in one call, with no
  `combinations()`/self-join in sight — see
  [Section 7](#7-how-the-five-implementations-map-onto-each-other) for
  why that's a genuinely different (not just shorter) way to express
  Rule 2.

All five scripts implement **the same algorithm**, produce **the
same output**, and are cross-checked against each other and against
the worked-by-hand example below — they are different execution
strategies for one idea, not different ideas.

This folder is the Spark/Python companion to the original MapReduce
write-up:

> [`slides/mapreduce/mapreduce_examples/MapReduce_People_You_May_Know.md`](../../../mapreduce/mapreduce_examples/MapReduce_People_You_May_Know.md)

That document works the algorithm out by hand, key by key, on the
same 6-person graph used here as `data/friends.txt`, and includes a
much deeper discussion of scalability, hub-node skew, and mitigation
strategies (Section 14) than is repeated here. **Read it first** if
you want the full derivation; this README focuses on the three
runnable implementations.

## 1. The Problem

Given a social graph as an adjacency list —

```text
Person -> [List of Friends]
```

— for **every pair of people who are not already friends**, find:

* whether they have any friends in common, and
* if so, the count and names of those mutual friends.

Pairs who are *already* friends must **not** appear in the output —
recommending someone you're already friends with makes no sense.
(That narrower, related problem — mutual friends for pairs who
*are* already connected, the "you and Joe have 230 friends in
common" feature — is the separate
[`MapReduce_Finding_Friends.html`](../../../mapreduce/mapreduce_examples/MapReduce_Finding_Friends.html)
example. PYMK extends it to non-friends, which is structurally a
different computation — see Section 3 of the companion doc for why
the Finding-Friends mapper can never produce a non-friend pair as a
key at all.)

**"Friend" here means a mutual, bidirectional relationship —
Facebook-style, not Twitter/X-style.** If `A` lists `B` as a friend,
`B` is expected to list `A` right back, and `sort_pair(A, B)` treats
`(A, B)` and `(B, A)` as the same edge — which is exactly why every
recommendation this algorithm produces is symmetric (if `A` is
recommended `E`, `E` is recommended `A` back, with the same
mutual-friend count and names). A one-way "follows" graph, where `A`
following `B` doesn't imply `B` follows `A`, doesn't fit this
algorithm as-is: Rule 1 and Rule 2 would both need to distinguish
"follows" direction before "mutual friend" even has a well-defined
meaning. Adapting the two rules for a directed graph is listed as an
open extension in [Section 9](#9-extending-this), item 2.

## 2. The Algorithm

The key idea: if a person `X` appears in the friend lists of both
`A` and `E`, then `X` is a mutual friend of `A` and `E` —
regardless of whether `A` and `E` are friends with each other.
Turning this around: **for every person `P`, every pair of `P`'s own
friends shares `P` as a mutual friend.**

That gives a mapper with two rules, applied to every
`(person, friends)` record:

* **Rule 1 — tag existing edges.** For each friend `F` of `P`, emit
  `(sort_pair(P, F), "FRIEND")`. This records which pairs are
  already directly connected, so the reducer can throw them out.
* **Rule 2 — vouch for every pair of `P`'s friends.** For every
  2-combination `(X, Y)` drawn from `P`'s own friend list, emit
  `(sort_pair(X, Y), P)` — *"`P` is a mutual friend of `X` and
  `Y`."*

`sort_pair(a, b)` returns the two ids in a fixed order (e.g.
lexicographic) so that `(X, Y)` and `(Y, X)` always land on the same
key — the equivalent of a MapReduce framework routing both to the
same reducer because they hash identically.

After grouping by key (the shuffle), the reducer drops any pair
whose group contains a `"FRIEND"` value — already connected — and
for every other pair, reports how many names are left (the mutual
friend count) and what they are.

```text
map(P, friends) {
   for each F in friends:
      emit(sort_pair(P, F), "FRIEND")              # Rule 1

   for each unordered pair (X, Y) in friends:
      emit(sort_pair(X, Y), P)                      # Rule 2
}

reduce(pair, values) {
   if "FRIEND" in values:
      return                                        # already friends, suppress
   emit(pair, len(values), sorted(values))           # count + mutual-friend names
}
```

This is exactly [Section 4 and Section 10](../../../mapreduce/mapreduce_examples/MapReduce_People_You_May_Know.md)
of the companion MapReduce document, and the three RDD-based scripts
in this folder name their functions after it (`sort_pair`,
`map_person`/`rule1_friend_tags`, `reduce_pair`/`reduce_group`/
`combine_pair_states`) so you can read the code and the algorithm doc
side by side. `pymk_pyspark_using_dataframes.py` implements the same
two rules under DataFrame-flavored names instead
(`build_edges_df`/`build_vouches_df`) — see Section 7 for why its
vocabulary (joins, `groupBy().agg()`, a `Window`) departs further from
the algorithm doc's pseudocode than the other two Spark scripts do.

### Why not just intersect friend sets directly?

For a *single* pair `(A, E)`, `friends(A) ∩ friends(E)` is a
perfectly good — and simpler — way to compute their mutual friends.
The reason PYMK is framed as a MapReduce/Spark pairs algorithm
instead is that a real system needs the answer for **every**
non-friend pair in the whole graph at once, and doing that as
`O(N²)` pairwise set intersections over `N` people doesn't scale.
The map-and-shuffle formulation turns the problem into a single pass
over each person's own (typically short) friend list, letting the
shuffle do the "which people share this mutual friend" grouping in
one distributed step. See Section 7 and Section 14 of the companion
document for the numbers.

## 3. Input / Output Data Format

**Input**, one line per person (`#`-prefixed and blank lines are
comments, ignored by all five scripts):

```text
<person>,<friend_1>,<friend_2>,...,<friend_n>
```

Friendship is symmetric (bidirectional, Facebook-style — not a
one-way Twitter/X "follows"), so every edge appears on both
endpoints' lines (see [`data/friends.txt`](data/friends.txt)).

**Output**, per person, ranked by descending mutual-friend count
(ties broken alphabetically by candidate name):

```text
<person>: <candidate> (<count> mutual: <mutual_friend_1>/<mutual_friend_2>/...), ...
```

or, with `--output`, one TSV row per `(person, candidate)`
recommendation:

```text
<person><TAB><candidate><TAB><mutual_count><TAB><mutual_friend_1>,<mutual_friend_2>,...
```

## 4. Folder Contents

| Name | Type | Description |
|---|---|---|
| [`data/friends.txt`](data/friends.txt) | txt | The 6-person graph (`A`..`F`) worked by hand in the companion MapReduce doc — one non-edge for each of `AE`, `AF`, `DF`, `EF` |
| [`data/friends_larger.txt`](data/friends_larger.txt) | txt | A 12-person graph (`A`..`L`, two loosely bridged clusters) with enough non-friend pairs to make `--top-k` ranking meaningful |
| [`pymk_pure_python.py`](pymk_pure_python.py) | py | Dependency-free Python implementation (Map/Shuffle/Reduce simulated with dicts) |
| [`pymk_pyspark_using_groupbykey.py`](pymk_pyspark_using_groupbykey.py) | py | Standalone PySpark RDD implementation (`flatMap`/`groupByKey`/`mapValues`) |
| [`pymk_pyspark_using_reducebykey.py`](pymk_pyspark_using_reducebykey.py) | py | Same PySpark job, aggregating with `reduceByKey`/a map-side combiner instead of `groupByKey` — see Section 7 |
| [`pymk_pyspark_using_dataframes.py`](pymk_pyspark_using_dataframes.py) | py | Same algorithm on the Spark SQL DataFrame API — `explode`/joins/`groupBy().agg()`/a ranking `Window` instead of RDD operators — see Section 7 |
| [`pymk_pyspark_using_graphframes.py`](pymk_pyspark_using_graphframes.py) | py | Same algorithm as one [GraphFrames](https://graphframes.io/) motif query (`g.find(...)`) over a `GraphFrame` instead of joins — needs the extra `graphframes` dependency, see Section 6 |
| [`run_all_pyspark.sh`](run_all_pyspark.sh) | sh | Convenience script — runs all four PySpark scripts above against both sample graphs in one go, see Section 6 |

## 5. Running the Pure-Python Version

No dependencies beyond the standard library.

```text
python3 pymk_pure_python.py <input_file> [--top-k K] [--output OUT.tsv]
```

### Sample run

```text
% python3 pymk_pure_python.py data/friends.txt
input_file= data/friends.txt
people_in_graph= 6
non_friend_pairs_with_mutual_friends= 4

A: E (3 mutual: B/C/D), F (2 mutual: B/C)
B: (no recommendations)
C: (no recommendations)
D: F (2 mutual: B/C)
E: A (3 mutual: B/C/D), F (2 mutual: B/C)
F: A (2 mutual: B/C), D (2 mutual: B/C), E (2 mutual: B/C)
```

This matches the companion document's Section 12 output exactly:
`((A,E), 3, [B,C,D])`, `((A,F), 2, [B,C])`, `((D,F), 2, [B,C])`,
`((E,F), 2, [B,C])` — each pair shown here from both endpoints'
point of view. `B` and `C` have no recommendations because they're
already friends with everyone else in this tiny graph.

### Sample run — larger graph, top-2 recommendations per person

```text
% python3 pymk_pure_python.py data/friends_larger.txt --top-k 2
input_file= data/friends_larger.txt
people_in_graph= 12
non_friend_pairs_with_mutual_friends= 19
top_k= 2

A: F (2 mutual: B/E), G (2 mutual: D/E)
B: G (2 mutual: D/E), H (1 mutual: F)
C: F (2 mutual: B/E), G (2 mutual: D/E)
D: F (2 mutual: B/E), H (1 mutual: G)
E: H (2 mutual: F/G), I (1 mutual: G)
F: A (2 mutual: B/E), C (2 mutual: B/E)
G: A (2 mutual: D/E), B (2 mutual: D/E)
H: E (2 mutual: F/G), K (2 mutual: I/J)
I: L (2 mutual: J/K), D (1 mutual: G)
J: G (2 mutual: H/I), F (1 mutual: H)
K: H (2 mutual: I/J), G (1 mutual: I)
L: I (2 mutual: J/K), H (1 mutual: J)
```

### Step-by-Step Data Structure Trace (`data/friends.txt`)

`pymk_pure_python.py` has no cluster, no shuffle, no serialization —
just five plain Python data structures built one after another. This
section prints each one in full for the same 6-person graph, so you
can see exactly what `graph`, `grouped`, and `recommendations` (the
three structures that matter) actually hold at every stage, not just
the final answer. Every result below is real output from calling the
script's own functions directly; nothing here is hand-derived.

Recall the graph:

```text
A -> B C D
B -> A C D E F
C -> A B D E F
D -> A B C E
E -> B C D
F -> B C
```

**Step 1 — `graph = read_graph('data/friends.txt')`**

A plain `dict[str, list[str]]`, one entry per person, built by a
single pass over the file (comments and blank lines skipped):

```python
{
    'A': ['B', 'C', 'D'],
    'B': ['A', 'C', 'D', 'E', 'F'],
    'C': ['A', 'B', 'D', 'E', 'F'],
    'D': ['A', 'B', 'C', 'E'],
    'E': ['B', 'C', 'D'],
    'F': ['B', 'C'],
}
```

Every later structure is derived from this one dict — nothing else
touches the input file.

**Step 2 — `map_person(person, friends)` called once per person**

This is the in-memory stand-in for the Map phase: a plain Python
generator, called once per `graph` entry, yielding `(pair, value)`
tuples — never written to a shared structure itself. `shuffle()`
(Step 3) is what collects these:

```text
map_person('A', ['B', 'C', 'D']) yields:
    ('A', 'B') -> 'FRIEND'
    ('A', 'C') -> 'FRIEND'
    ('A', 'D') -> 'FRIEND'
    ('B', 'C') -> 'A'
    ('B', 'D') -> 'A'
    ('C', 'D') -> 'A'
map_person('B', ['A', 'C', 'D', 'E', 'F']) yields:
    ('A', 'B') -> 'FRIEND'
    ('B', 'C') -> 'FRIEND'
    ('B', 'D') -> 'FRIEND'
    ('B', 'E') -> 'FRIEND'
    ('B', 'F') -> 'FRIEND'
    ('A', 'C') -> 'B'
    ('A', 'D') -> 'B'
    ('A', 'E') -> 'B'
    ('A', 'F') -> 'B'
    ('C', 'D') -> 'B'
    ('C', 'E') -> 'B'
    ('C', 'F') -> 'B'
    ('D', 'E') -> 'B'
    ('D', 'F') -> 'B'
    ('E', 'F') -> 'B'
map_person('C', ['A', 'B', 'D', 'E', 'F']) yields:
    ('A', 'C') -> 'FRIEND'
    ('B', 'C') -> 'FRIEND'
    ('C', 'D') -> 'FRIEND'
    ('C', 'E') -> 'FRIEND'
    ('C', 'F') -> 'FRIEND'
    ('A', 'B') -> 'C'
    ('A', 'D') -> 'C'
    ('A', 'E') -> 'C'
    ('A', 'F') -> 'C'
    ('B', 'D') -> 'C'
    ('B', 'E') -> 'C'
    ('B', 'F') -> 'C'
    ('D', 'E') -> 'C'
    ('D', 'F') -> 'C'
    ('E', 'F') -> 'C'
map_person('D', ['A', 'B', 'C', 'E']) yields:
    ('A', 'D') -> 'FRIEND'
    ('B', 'D') -> 'FRIEND'
    ('C', 'D') -> 'FRIEND'
    ('D', 'E') -> 'FRIEND'
    ('A', 'B') -> 'D'
    ('A', 'C') -> 'D'
    ('A', 'E') -> 'D'
    ('B', 'C') -> 'D'
    ('B', 'E') -> 'D'
    ('C', 'E') -> 'D'
map_person('E', ['B', 'C', 'D']) yields:
    ('B', 'E') -> 'FRIEND'
    ('C', 'E') -> 'FRIEND'
    ('D', 'E') -> 'FRIEND'
    ('B', 'C') -> 'E'
    ('B', 'D') -> 'E'
    ('C', 'D') -> 'E'
map_person('F', ['B', 'C']) yields:
    ('B', 'F') -> 'FRIEND'
    ('C', 'F') -> 'FRIEND'
    ('B', 'C') -> 'F'
```

Notice `('A', 'B')` is yielded by **both** `map_person('A', ...)`
(Rule 1, from `A`'s own edge to `B`) and `map_person('C', ...)`
(Rule 2, `C` vouching that `A` and `B` share `C` as a mutual friend)
— this is exactly why `shuffle()` needs to collect by key across
*all six* calls before anything can be decided; no single call ever
sees the whole picture for a pair.

**Step 3 — `grouped = shuffle(graph)`**

A `dict[tuple[str, str], list[str]]`, built by running every person
through `map_person()` and appending each yielded value onto a list
keyed by pair — the in-memory equivalent of a cluster's shuffle.
15 keys total (`C(6,2) = 15`, one for every possible pair among 6
people). The value lists interleave `"FRIEND"` tags and vouching
names in whatever order the six `map_person()` calls happened to run
(here, alphabetical by person, since that's dict iteration order):

```python
{
    ('A', 'B'): ['FRIEND', 'FRIEND', 'C', 'D'],
    ('A', 'C'): ['FRIEND', 'B', 'FRIEND', 'D'],
    ('A', 'D'): ['FRIEND', 'B', 'C', 'FRIEND'],
    ('A', 'E'): ['B', 'C', 'D'],
    ('A', 'F'): ['B', 'C'],
    ('B', 'C'): ['A', 'FRIEND', 'FRIEND', 'D', 'E', 'F'],
    ('B', 'D'): ['A', 'FRIEND', 'C', 'FRIEND', 'E'],
    ('B', 'E'): ['FRIEND', 'C', 'D', 'FRIEND'],
    ('B', 'F'): ['FRIEND', 'C', 'FRIEND'],
    ('C', 'D'): ['A', 'B', 'FRIEND', 'FRIEND', 'E'],
    ('C', 'E'): ['B', 'FRIEND', 'D', 'FRIEND'],
    ('C', 'F'): ['B', 'FRIEND', 'FRIEND'],
    ('D', 'E'): ['B', 'C', 'FRIEND', 'FRIEND'],
    ('D', 'F'): ['B', 'C'],
    ('E', 'F'): ['B', 'C'],
}
```

Compare each entry against the graph: any key whose list contains
`'FRIEND'` is a pair that's already connected; the four with no
`'FRIEND'` — `('A','E')`, `('A','F')`, `('D','F')`, `('E','F')` —
are exactly the non-edges predicted in the graph comment.

**Step 4 — `reduce_pair(pair, values)` called once per `grouped` key**

The in-memory Reduce phase: one call per `grouped` entry, each
returning either `None` (drop) or `(pair, count, mutual_friends)`:

```text
reduce_pair(('A', 'B'), ['FRIEND', 'FRIEND', 'C', 'D'])      -> None
reduce_pair(('A', 'C'), ['FRIEND', 'B', 'FRIEND', 'D'])      -> None
reduce_pair(('A', 'D'), ['FRIEND', 'B', 'C', 'FRIEND'])      -> None
reduce_pair(('A', 'E'), ['B', 'C', 'D'])                     -> (('A', 'E'), 3, ['B', 'C', 'D'])
reduce_pair(('A', 'F'), ['B', 'C'])                          -> (('A', 'F'), 2, ['B', 'C'])
reduce_pair(('B', 'C'), ['A', 'FRIEND', 'FRIEND', 'D', 'E', 'F']) -> None
reduce_pair(('B', 'D'), ['A', 'FRIEND', 'C', 'FRIEND', 'E']) -> None
reduce_pair(('B', 'E'), ['FRIEND', 'C', 'D', 'FRIEND'])      -> None
reduce_pair(('B', 'F'), ['FRIEND', 'C', 'FRIEND'])           -> None
reduce_pair(('C', 'D'), ['A', 'B', 'FRIEND', 'FRIEND', 'E']) -> None
reduce_pair(('C', 'E'), ['B', 'FRIEND', 'D', 'FRIEND'])      -> None
reduce_pair(('C', 'F'), ['B', 'FRIEND', 'FRIEND'])           -> None
reduce_pair(('D', 'E'), ['B', 'C', 'FRIEND', 'FRIEND'])      -> None
reduce_pair(('D', 'F'), ['B', 'C'])                          -> (('D', 'F'), 2, ['B', 'C'])
reduce_pair(('E', 'F'), ['B', 'C'])                          -> (('E', 'F'), 2, ['B', 'C'])
```

**Step 5 — `results = compute_pymk(graph)`**

`compute_pymk()` runs Steps 1–4 end to end and keeps only the
non-`None` results — a plain `list[PairResult]`, one entry per
non-friend pair, unordered (each pair once, not once per person):

```python
[
    (('A', 'E'), 3, ['B', 'C', 'D']),
    (('A', 'F'), 2, ['B', 'C']),
    (('D', 'F'), 2, ['B', 'C']),
    (('E', 'F'), 2, ['B', 'C']),
]
```

**Step 6 (final) — `recommendations = recommendations_by_person(graph, results)`**

The "downstream sort" pass: duplicates each pair for both endpoints,
ranks each person's candidates by descending mutual count, and — via
the `graph` argument — restores `B` and `C` with an empty list
instead of omitting them. A `dict[str, list[tuple[str, int,
list[str]]]]`, one entry per person, which is exactly what
`format_recommendations()` prints:

```python
{
    'A': [('E', 3, ['B', 'C', 'D']), ('F', 2, ['B', 'C'])],
    'B': [],
    'C': [],
    'D': [('F', 2, ['B', 'C'])],
    'E': [('A', 3, ['B', 'C', 'D']), ('F', 2, ['B', 'C'])],
    'F': [('A', 2, ['B', 'C']), ('D', 2, ['B', 'C']), ('E', 2, ['B', 'C'])],
}
```

Structure-count summary, to see the shape of the pipeline at a
glance (unlike the PySpark traces below, nothing here is
partitioned or shuffled across a network — every structure lives in
one process's memory, one dict or list at a time):

| Step | Data structure | Type | Size |
|---|---|---|---|
| 1 | `graph` | `dict[str, list[str]]` | 6 people |
| 2 | `map_person()` output (not stored — consumed immediately by Step 3) | generator of `(Pair, str)` | 22 Rule-1 + 33 Rule-2 = 55 yielded pairs |
| 3 | `grouped` | `dict[Pair, list[str]]` | 15 keys (`C(6,2)`) |
| 4 | `reduce_pair()` output (not stored — consumed immediately by Step 5) | `PairResult \| None` per key | 15 calls, 4 non-`None` |
| 5 | `results` | `list[PairResult]` | 4 non-friend pairs |
| 6 | `recommendations` | `dict[str, list[Candidate]]` | 6 people (4 with candidates, 2 empty) |

## 6. Running the PySpark Versions

### Quick start: run all four at once

[`run_all_pyspark.sh`](run_all_pyspark.sh) runs all four PySpark
scripts below against both sample graphs, one after another, so you
can see all four agree without typing eight commands by hand:

```text
./run_all_pyspark.sh
```

It's a thin loop around the same `python3 <script> <input_file>
[--top-k K]` commands documented for each script below — read on for
what each one does differently, `spark-submit` equivalents for a real
cluster, and (for `pymk_pyspark_using_graphframes.py`) the one extra
dependency it needs. (`pymk_pure_python.py` isn't included in the
script since it's a plain Python script, not a Spark job — see
Section 5 above.)

### `pymk_pyspark_using_groupbykey.py`

Locally (starts an in-process local Spark context via
`SparkSession.builder...getOrCreate()`):

```text
python3 pymk_pyspark_using_groupbykey.py <input_file> [--top-k K] [--output OUT_DIR]
```

Or on a real Spark install / cluster:

```text
$SPARK_HOME/bin/spark-submit pymk_pyspark_using_groupbykey.py <input_file> [--top-k K] [--output OUT_DIR]
```

#### Sample run

```text
% python3 pymk_pyspark_using_groupbykey.py data/friends.txt
input_file= data/friends.txt
people_in_graph= 6

A: E (3 mutual: B/C/D), F (2 mutual: B/C)
B: (no recommendations)
C: (no recommendations)
D: F (2 mutual: B/C)
E: A (3 mutual: B/C/D), F (2 mutual: B/C)
F: A (2 mutual: B/C), D (2 mutual: B/C), E (2 mutual: B/C)
```

Byte-for-byte the same recommendations as the pure-Python run above
(Spark's log noise on stderr trimmed for readability).

#### Step-by-Step Transformation Trace (`data/friends.txt`)

The final printout above is the *end* of the pipeline. This section
`.collect()`s the RDD after **every single transformation** in
`main()` / `build_recommendations()`, in order, on the same 6-person
graph, so you can see exactly what each line of `pymk_pyspark_using_groupbykey.py`
does to the data — not just the final answer. Every result below was
produced by actually running the pipeline step by step; nothing here
is hand-derived.

Recall the graph:

```text
A -> B C D
B -> A C D E F
C -> A B D E F
D -> A B C E
E -> B C D
F -> B C
```

**Step 1 — `sc.textFile(input_file)` → `lines`**

Reads the file as an RDD of raw text lines, comments and all — 27
lines in `data/friends.txt`, of which 21 are `#`-comments/blank and
6 are data:

```text
# People You May Know -- sample social graph
#
# Same 6-person graph used in the worked MapReduce example:
...                                            (21 comment/blank lines total)
A,B,C,D
B,A,C,D,E,F
C,A,B,D,E,F
D,A,B,C,E
E,B,C,D
F,B,C
(count = 27)
```

**Step 2 — `lines.map(parse_line)` → `parsed`**

`parse_line()` turns a data line into `(person, [friends])` and a
comment/blank line into `None` — still 27 rows, same count as Step 1,
one output per input row (that's what `map` guarantees, unlike
`flatMap`):

```text
None
None
...                                            (21 Nones total)
('A', ['B', 'C', 'D'])
('B', ['A', 'C', 'D', 'E', 'F'])
('C', ['A', 'B', 'D', 'E', 'F'])
('D', ['A', 'B', 'C', 'E'])
('E', ['B', 'C', 'D'])
('F', ['B', 'C'])
(count = 27)
```

**Step 3 — `parsed.filter(lambda r: r is not None)` → `friends_rdd`**

Drops the 21 `None`s from comment/blank lines, leaving one row per
person — this is the RDD every later step builds from:

```text
('A', ['B', 'C', 'D'])
('B', ['A', 'C', 'D', 'E', 'F'])
('C', ['A', 'B', 'D', 'E', 'F'])
('D', ['A', 'B', 'C', 'E'])
('E', ['B', 'C', 'D'])
('F', ['B', 'C'])
(count = 6)
```

**Step 4 — `friends_rdd.flatMap(rule1_friend_tags)` → `edges`** (Rule 1)

One `(sort_pair(P, F), "FRIEND")` per existing edge. Note each
friendship is emitted **twice** — once from each endpoint's own
record (`A,B,C,D` emits `(A,B)`, and `B,A,C,D,E,F` also emits
`(A,B)`) — 6 people with degrees `3,5,5,4,3,2` sum to 22 Rule-1
records:

```text
(('A', 'B'), 'FRIEND')
(('A', 'B'), 'FRIEND')
(('A', 'C'), 'FRIEND')
(('A', 'C'), 'FRIEND')
(('A', 'D'), 'FRIEND')
(('A', 'D'), 'FRIEND')
(('B', 'C'), 'FRIEND')
(('B', 'C'), 'FRIEND')
(('B', 'D'), 'FRIEND')
(('B', 'D'), 'FRIEND')
(('B', 'E'), 'FRIEND')
(('B', 'E'), 'FRIEND')
(('B', 'F'), 'FRIEND')
(('B', 'F'), 'FRIEND')
(('C', 'D'), 'FRIEND')
(('C', 'D'), 'FRIEND')
(('C', 'E'), 'FRIEND')
(('C', 'E'), 'FRIEND')
(('C', 'F'), 'FRIEND')
(('C', 'F'), 'FRIEND')
(('D', 'E'), 'FRIEND')
(('D', 'E'), 'FRIEND')
(count = 22)
```

**Step 5 — `friends_rdd.flatMap(rule2_mutual_vouches)` → `vouches`** (Rule 2)

One `(sort_pair(X, Y), P)` per 2-combination of each person `P`'s
friends. `B` and `C` each have 5 friends, so each contributes
`C(5,2) = 10` records — the largest contributors of the 33 total:

```text
(('A', 'B'), 'C')
(('A', 'B'), 'D')
(('A', 'C'), 'B')
(('A', 'C'), 'D')
(('A', 'D'), 'B')
(('A', 'D'), 'C')
(('A', 'E'), 'B')
(('A', 'E'), 'C')
(('A', 'E'), 'D')
(('A', 'F'), 'B')
(('A', 'F'), 'C')
(('B', 'C'), 'A')
(('B', 'C'), 'D')
(('B', 'C'), 'E')
(('B', 'C'), 'F')
(('B', 'D'), 'A')
(('B', 'D'), 'C')
(('B', 'D'), 'E')
(('B', 'E'), 'C')
(('B', 'E'), 'D')
(('B', 'F'), 'C')
(('C', 'D'), 'A')
(('C', 'D'), 'B')
(('C', 'D'), 'E')
(('C', 'E'), 'B')
(('C', 'E'), 'D')
(('C', 'F'), 'B')
(('D', 'E'), 'B')
(('D', 'E'), 'C')
(('D', 'F'), 'B')
(('D', 'F'), 'C')
(('E', 'F'), 'B')
(('E', 'F'), 'C')
(count = 33)
```

**Step 6 — `edges.union(vouches)` → `unioned`**

A plain concatenation, no shuffle yet — `22 + 33 = 55` rows, "FRIEND"
tags and vouches interleaved by partition, not yet grouped by key:

```text
(('A', 'B'), 'C')
(('A', 'B'), 'D')
(('A', 'B'), 'FRIEND')
(('A', 'B'), 'FRIEND')
(('A', 'C'), 'B')
...                                            (55 rows total)
(('E', 'F'), 'B')
(('E', 'F'), 'C')
(count = 55)
```

**Step 7 — `unioned.groupByKey()` → `grouped`** (the shuffle)

This is the shuffle: all 55 rows are redistributed across the
cluster by key and grouped, collapsing to one entry per unique pair —
15 pairs total (`C(6,2) = 15`, every possible pair among 6 people).
Compare each group against the graph above: a group containing
`"FRIEND"` is a pair that's already connected; a group with **no**
`"FRIEND"` is a PYMK candidate:

```text
('A', 'B') -> ['C', 'D', 'FRIEND', 'FRIEND']          <- has FRIEND
('A', 'C') -> ['B', 'D', 'FRIEND', 'FRIEND']          <- has FRIEND
('A', 'D') -> ['B', 'C', 'FRIEND', 'FRIEND']          <- has FRIEND
('A', 'E') -> ['B', 'C', 'D']                         <- no FRIEND!
('A', 'F') -> ['B', 'C']                              <- no FRIEND!
('B', 'C') -> ['A', 'D', 'E', 'F', 'FRIEND', 'FRIEND'] <- has FRIEND
('B', 'D') -> ['A', 'C', 'E', 'FRIEND', 'FRIEND']     <- has FRIEND
('B', 'E') -> ['C', 'D', 'FRIEND', 'FRIEND']          <- has FRIEND
('B', 'F') -> ['C', 'FRIEND', 'FRIEND']               <- has FRIEND
('C', 'D') -> ['A', 'B', 'E', 'FRIEND', 'FRIEND']     <- has FRIEND
('C', 'E') -> ['B', 'D', 'FRIEND', 'FRIEND']          <- has FRIEND
('C', 'F') -> ['B', 'FRIEND', 'FRIEND']               <- has FRIEND
('D', 'E') -> ['B', 'C', 'FRIEND', 'FRIEND']          <- has FRIEND
('D', 'F') -> ['B', 'C']                              <- no FRIEND!
('E', 'F') -> ['B', 'C']                              <- no FRIEND!
(count = 15)
```

**Step 8 — `grouped.mapValues(reduce_group)` → `reduced0`**

The reduce step itself, applied to each group: `None` if `"FRIEND"`
was present, else `(mutual_count, sorted_mutual_friends)`. Still 15
rows — `mapValues` never changes row count, only values:

```text
(('A', 'B'), None)
(('A', 'C'), None)
(('A', 'D'), None)
(('A', 'E'), (3, ['B', 'C', 'D']))
(('A', 'F'), (2, ['B', 'C']))
(('B', 'C'), None)
(('B', 'D'), None)
(('B', 'E'), None)
(('B', 'F'), None)
(('C', 'D'), None)
(('C', 'E'), None)
(('C', 'F'), None)
(('D', 'E'), None)
(('D', 'F'), (2, ['B', 'C']))
(('E', 'F'), (2, ['B', 'C']))
(count = 15)
```

**Step 9 — `reduced0.filter(lambda kv: kv[1] is not None)` → `filtered`**

Drops the 11 `None`s (already-friend pairs) — the 4 remaining rows
are exactly the 4 non-edges the graph comment predicted (`AE`, `AF`,
`DF`, `EF`):

```text
(('A', 'E'), (3, ['B', 'C', 'D']))
(('A', 'F'), (2, ['B', 'C']))
(('D', 'F'), (2, ['B', 'C']))
(('E', 'F'), (2, ['B', 'C']))
(count = 4)
```

**Step 10 — `filtered.map(...)` → `reduced`**

Flattens `((a, b), (count, mutual))` into a plain 4-tuple
`(a, b, count, mutual)` — still 4 rows, one per non-friend *pair*
(not yet duplicated per person):

```text
('A', 'E', 3, ['B', 'C', 'D'])
('A', 'F', 2, ['B', 'C'])
('D', 'F', 2, ['B', 'C'])
('E', 'F', 2, ['B', 'C'])
(count = 4)
```

**Step 11 — `reduced.flatMap(...)` → `symmetric`**

Each pair recommends each endpoint to the other, so every row is
duplicated with `a`/`b` swapped — 4 pairs become 8 `(person,
candidate_info)` rows, keyed by the *recommended-to* person:

```text
('A', ('E', 3, ['B', 'C', 'D']))
('A', ('F', 2, ['B', 'C']))
('D', ('F', 2, ['B', 'C']))
('E', ('A', 3, ['B', 'C', 'D']))
('E', ('F', 2, ['B', 'C']))
('F', ('A', 2, ['B', 'C']))
('F', ('D', 2, ['B', 'C']))
('F', ('E', 2, ['B', 'C']))
(count = 8)
```

**Step 12 — `symmetric.groupByKey().mapValues(rank)` → `ranked_candidates`**

A second shuffle, this time keyed by person: groups the 8 rows down
to one entry per person who has at least one candidate (4 of the 6
people — `B` and `C` are missing, see Step 13), each candidate list
sorted by descending mutual count:

```text
A -> [('E', 3, ['B', 'C', 'D']), ('F', 2, ['B', 'C'])]
D -> [('F', 2, ['B', 'C'])]
E -> [('A', 3, ['B', 'C', 'D']), ('F', 2, ['B', 'C'])]
F -> [('A', 2, ['B', 'C']), ('D', 2, ['B', 'C']), ('E', 2, ['B', 'C'])]
(count = 4)
```

**Step 13 (final) — `all_people.leftOuterJoin(ranked_candidates).mapValues(...)` → `final`**

`all_people` is every one of the 6 original people, each paired with
`None`. The `leftOuterJoin` restores `B` and `C` with an empty list
instead of dropping them — this is the fix discussed in Section 7
("A correctness detail: people with zero candidates"). Now all 6
people are present, which is what the script actually prints:

```text
A -> [('E', 3, ['B', 'C', 'D']), ('F', 2, ['B', 'C'])]
B -> []
C -> []
D -> [('F', 2, ['B', 'C'])]
E -> [('A', 3, ['B', 'C', 'D']), ('F', 2, ['B', 'C'])]
F -> [('A', 2, ['B', 'C']), ('D', 2, ['B', 'C']), ('E', 2, ['B', 'C'])]
(count = 6)
```

Row-count summary, to see the shape of the pipeline at a glance:

| Step | Transformation | Row count |
|---|---|---|
| 1 | `sc.textFile` | 27 |
| 2 | `.map(parse_line)` | 27 |
| 3 | `.filter(not None)` | 6 |
| 4 | `.flatMap(rule1_friend_tags)` | 22 |
| 5 | `.flatMap(rule2_mutual_vouches)` | 33 |
| 6 | `.union()` | 55 |
| 7 | `.groupByKey()` | 15 (unique pairs) |
| 8 | `.mapValues(reduce_group)` | 15 |
| 9 | `.filter(kv[1] is not None)` | 4 (non-friend pairs) |
| 10 | `.map(flatten)` | 4 |
| 11 | `.flatMap(symmetrize)` | 8 |
| 12 | `.groupByKey().mapValues(rank)` | 4 (people with ≥1 candidate) |
| 13 | `.leftOuterJoin(all_people)` | 6 (every person) |

#### Sample run — larger graph, top-2 recommendations per person

```text
% python3 pymk_pyspark_using_groupbykey.py data/friends_larger.txt --top-k 2
input_file= data/friends_larger.txt
people_in_graph= 12
top_k= 2

A: F (2 mutual: B/E), G (2 mutual: D/E)
B: G (2 mutual: D/E), H (1 mutual: F)
C: F (2 mutual: B/E), G (2 mutual: D/E)
D: F (2 mutual: B/E), H (1 mutual: G)
E: H (2 mutual: F/G), I (1 mutual: G)
F: A (2 mutual: B/E), C (2 mutual: B/E)
G: A (2 mutual: D/E), B (2 mutual: D/E)
H: E (2 mutual: F/G), K (2 mutual: I/J)
I: L (2 mutual: J/K), D (1 mutual: G)
J: G (2 mutual: H/I), F (1 mutual: H)
K: H (2 mutual: I/J), G (1 mutual: I)
L: I (2 mutual: J/K), H (1 mutual: J)
```

Again identical to the pure-Python output — a useful sanity check
that the Spark version is a faithful re-implementation, not a
different algorithm that happens to agree on small inputs.

### `pymk_pyspark_using_reducebykey.py`

Same CLI, same usage pattern:

```text
python3 pymk_pyspark_using_reducebykey.py <input_file> [--top-k K] [--output OUT_DIR]
```

```text
$SPARK_HOME/bin/spark-submit pymk_pyspark_using_reducebykey.py <input_file> [--top-k K] [--output OUT_DIR]
```

#### Sample run

```text
% python3 pymk_pyspark_using_reducebykey.py data/friends.txt
input_file= data/friends.txt
people_in_graph= 6

A: E (3 mutual: B/C/D), F (2 mutual: B/C)
B: (no recommendations)
C: (no recommendations)
D: F (2 mutual: B/C)
E: A (3 mutual: B/C/D), F (2 mutual: B/C)
F: A (2 mutual: B/C), D (2 mutual: B/C), E (2 mutual: B/C)
```

Byte-for-byte identical to `pymk_pyspark_using_groupbykey.py`'s
output above — expected, since this script computes the same thing,
just with a different aggregation strategy (see the design-choice
discussion in Section 7).

#### Step-by-Step Transformation Trace (`data/friends.txt`)

Steps 1–3 (`sc.textFile` → `.map(parse_line)` → `.filter(not None)`
→ `friends_rdd`) are identical to the `groupByKey()` trace above —
same input, same parsing. Everything from Rule 1 onward changes
shape, because this script's mapper emits a **state tuple**
`(is_friend, mutual_friend_names)` instead of a raw `"FRIEND"`
string or a bare name, so that `reduceByKey()` has something
associative to combine.

```text
friends_rdd (unchanged from the groupByKey() trace):
('A', ['B', 'C', 'D'])
('B', ['A', 'C', 'D', 'E', 'F'])
('C', ['A', 'B', 'D', 'E', 'F'])
('D', ['A', 'B', 'C', 'E'])
('E', ['B', 'C', 'D'])
('F', ['B', 'C'])
(count = 6)
```

**Step 4 — `friends_rdd.flatMap(rule1_friend_tags)` → `edges`** (Rule 1, state form)

Same 22 records as before, but each value is now
`(True, frozenset())` — "already friends, no names to report" —
instead of the string `"FRIEND"`:

```text
(('A', 'B'), (True, frozenset()))
(('A', 'B'), (True, frozenset()))
(('A', 'C'), (True, frozenset()))
(('A', 'C'), (True, frozenset()))
(('A', 'D'), (True, frozenset()))
(('A', 'D'), (True, frozenset()))
(('B', 'C'), (True, frozenset()))
(('B', 'C'), (True, frozenset()))
(('B', 'D'), (True, frozenset()))
(('B', 'D'), (True, frozenset()))
(('B', 'E'), (True, frozenset()))
(('B', 'E'), (True, frozenset()))
(('B', 'F'), (True, frozenset()))
(('B', 'F'), (True, frozenset()))
(('C', 'D'), (True, frozenset()))
(('C', 'D'), (True, frozenset()))
(('C', 'E'), (True, frozenset()))
(('C', 'E'), (True, frozenset()))
(('C', 'F'), (True, frozenset()))
(('C', 'F'), (True, frozenset()))
(('D', 'E'), (True, frozenset()))
(('D', 'E'), (True, frozenset()))
(count = 22)
```

**Step 5 — `friends_rdd.flatMap(rule2_mutual_vouches)` → `vouches`** (Rule 2, state form)

Same 33 records as before, each value now
`(False, frozenset({vouching_person}))`:

```text
(('A', 'B'), (False, frozenset({'C'})))
(('A', 'B'), (False, frozenset({'D'})))
(('A', 'C'), (False, frozenset({'B'})))
(('A', 'C'), (False, frozenset({'D'})))
(('A', 'D'), (False, frozenset({'B'})))
(('A', 'D'), (False, frozenset({'C'})))
(('A', 'E'), (False, frozenset({'B'})))
(('A', 'E'), (False, frozenset({'C'})))
(('A', 'E'), (False, frozenset({'D'})))
(('A', 'F'), (False, frozenset({'B'})))
(('A', 'F'), (False, frozenset({'C'})))
(('B', 'C'), (False, frozenset({'A'})))
(('B', 'C'), (False, frozenset({'D'})))
(('B', 'C'), (False, frozenset({'E'})))
(('B', 'C'), (False, frozenset({'F'})))
(('B', 'D'), (False, frozenset({'A'})))
(('B', 'D'), (False, frozenset({'C'})))
(('B', 'D'), (False, frozenset({'E'})))
(('B', 'E'), (False, frozenset({'C'})))
(('B', 'E'), (False, frozenset({'D'})))
(('B', 'F'), (False, frozenset({'C'})))
(('C', 'D'), (False, frozenset({'A'})))
(('C', 'D'), (False, frozenset({'B'})))
(('C', 'D'), (False, frozenset({'E'})))
(('C', 'E'), (False, frozenset({'B'})))
(('C', 'E'), (False, frozenset({'D'})))
(('C', 'F'), (False, frozenset({'B'})))
(('D', 'E'), (False, frozenset({'B'})))
(('D', 'E'), (False, frozenset({'C'})))
(('D', 'F'), (False, frozenset({'B'})))
(('D', 'F'), (False, frozenset({'C'})))
(('E', 'F'), (False, frozenset({'B'})))
(('E', 'F'), (False, frozenset({'C'})))
(count = 33)
```

**Step 6 — `edges.union(vouches)` → `unioned`**

Still a plain concatenation, no shuffle yet — `22 + 33 = 55` rows,
same as the `groupByKey()` trace, just carrying state tuples instead
of strings:

```text
(('A', 'B'), (False, frozenset({'C'})))
(('A', 'B'), (False, frozenset({'D'})))
(('A', 'B'), (True, frozenset()))
(('A', 'B'), (True, frozenset()))
(('A', 'C'), (False, frozenset({'B'})))
...                                            (55 rows total)
(('E', 'F'), (False, frozenset({'C'})))
(count = 55)
```

**Step 7 — `unioned.reduceByKey(combine_pair_states)` → `reduced_states`** (shuffle + reduce, fused)

This is the step where `reduceByKey()` diverges from `groupByKey()`
in a way worth stopping on. Compare this directly against Step 7 of
the `groupByKey()` trace above: there, `('B', 'C')` grouped to
`['A', 'D', 'E', 'F', 'FRIEND', 'FRIEND']` — all 6 raw values shipped
across the shuffle before anything was discarded. Here, the same
pair reduces straight to `(True, frozenset())` — the moment
`combine_pair_states()` sees an `is_friend=True` on either side, it
throws away every accumulated name, because `A`, `D`, `E`, and `F`
were never going to be reported anyway. Spark can apply this combiner
on the map side, per partition, *before* the network shuffle even
happens — so for a pair like `('B', 'C')`, less data crosses the
wire in the first place, not just less data survives filtering
afterward. 55 input rows still collapse to 15 unique pairs, but the
already-friend pairs carry only a two-value constant instead of a
growing list of names:

```text
(('A', 'B'), (True, frozenset()))
(('A', 'C'), (True, frozenset()))
(('A', 'D'), (True, frozenset()))
(('A', 'E'), (False, frozenset({'B', 'C', 'D'})))
(('A', 'F'), (False, frozenset({'B', 'C'})))
(('B', 'C'), (True, frozenset()))
(('B', 'D'), (True, frozenset()))
(('B', 'E'), (True, frozenset()))
(('B', 'F'), (True, frozenset()))
(('C', 'D'), (True, frozenset()))
(('C', 'E'), (True, frozenset()))
(('C', 'F'), (True, frozenset()))
(('D', 'E'), (True, frozenset()))
(('D', 'F'), (False, frozenset({'B', 'C'})))
(('E', 'F'), (False, frozenset({'B', 'C'})))
(count = 15)
```

**Step 8 — `reduced_states.filter(lambda kv: not kv[1][0])` → `filtered`**

Drops every pair whose merged state is `is_friend=True` — the same
11 pairs the `groupByKey()` trace dropped in its Step 9, just reached
one step earlier since reduce already happened in Step 7:

```text
(('A', 'E'), (False, frozenset({'B', 'C', 'D'})))
(('A', 'F'), (False, frozenset({'B', 'C'})))
(('D', 'F'), (False, frozenset({'B', 'C'})))
(('E', 'F'), (False, frozenset({'B', 'C'})))
(count = 4)
```

**Step 9 — `filtered.map(...)` → `reduced`**

Flattens `((a, b), (is_friend, mutual_names))` into a plain 4-tuple
`(a, b, count, mutual)`, same shape as the `groupByKey()` trace's
Step 10 output from here on — the two pipelines have converged:

```text
('A', 'E', 3, ['B', 'C', 'D'])
('A', 'F', 2, ['B', 'C'])
('D', 'F', 2, ['B', 'C'])
('E', 'F', 2, ['B', 'C'])
(count = 4)
```

**Step 10 — `reduced.flatMap(...)` → `symmetric`**

Each pair recommends each endpoint to the other, same as before —
but each row is now a **single-element list** `[(candidate, count,
mutual)]` rather than a bare tuple, so `reduceByKey()` has a list to
merge in the next step:

```text
('A', [('E', 3, ['B', 'C', 'D'])])
('A', [('F', 2, ['B', 'C'])])
('D', [('F', 2, ['B', 'C'])])
('E', [('A', 3, ['B', 'C', 'D'])])
('E', [('F', 2, ['B', 'C'])])
('F', [('A', 2, ['B', 'C'])])
('F', [('D', 2, ['B', 'C'])])
('F', [('E', 2, ['B', 'C'])])
(count = 8)
```

**Step 11 — `symmetric.reduceByKey(merge_ranked_candidates)` → `ranked_candidates`** (shuffle + rank, fused)

`merge_ranked_candidates()` concatenates two candidate lists,
re-sorts by descending mutual count, and (if `--top-k` was given)
truncates — an associative, commutative merge, so this too runs as a
map-side combiner. 8 single-element lists collapse to 4 fully-ranked
per-person lists — same final values as the `groupByKey()` trace's
Step 12, reached via merging instead of collect-then-sort:

```text
A -> [('E', 3, ['B', 'C', 'D']), ('F', 2, ['B', 'C'])]
D -> [('F', 2, ['B', 'C'])]
E -> [('A', 3, ['B', 'C', 'D']), ('F', 2, ['B', 'C'])]
F -> [('A', 2, ['B', 'C']), ('D', 2, ['B', 'C']), ('E', 2, ['B', 'C'])]
(count = 4)
```

**Step 12 (final) — `all_people.leftOuterJoin(ranked_candidates).mapValues(...)` → `final`**

Identical purpose and identical result to the `groupByKey()` trace's
Step 13: restores `B` and `C` with an empty list so all 6 people
appear in the output:

```text
A -> [('E', 3, ['B', 'C', 'D']), ('F', 2, ['B', 'C'])]
B -> []
C -> []
D -> [('F', 2, ['B', 'C'])]
E -> [('A', 3, ['B', 'C', 'D']), ('F', 2, ['B', 'C'])]
F -> [('A', 2, ['B', 'C']), ('D', 2, ['B', 'C']), ('E', 2, ['B', 'C'])]
(count = 6)
```

Row-count summary — notice Step 7 already does the work Steps 7+8
took two rows to do in the `groupByKey()` trace, and the already-friend
pairs never carry more than a 2-element constant through the shuffle:

| Step | Transformation | Row count |
|---|---|---|
| 1–3 | `sc.textFile` → `.map` → `.filter` | 6 (same as groupByKey trace) |
| 4 | `.flatMap(rule1_friend_tags)` | 22 |
| 5 | `.flatMap(rule2_mutual_vouches)` | 33 |
| 6 | `.union()` | 55 |
| 7 | `.reduceByKey(combine_pair_states)` | 15 (unique pairs, **already reduced**) |
| 8 | `.filter(not is_friend)` | 4 (non-friend pairs) |
| 9 | `.map(flatten)` | 4 |
| 10 | `.flatMap(symmetrize)` | 8 |
| 11 | `.reduceByKey(merge_ranked_candidates)` | 4 (people with ≥1 candidate, **already ranked**) |
| 12 | `.leftOuterJoin(all_people)` | 6 (every person) |

#### Sample run — larger graph, top-2 recommendations per person

```text
% python3 pymk_pyspark_using_reducebykey.py data/friends_larger.txt --top-k 2
input_file= data/friends_larger.txt
people_in_graph= 12
top_k= 2

A: F (2 mutual: B/E), G (2 mutual: D/E)
B: G (2 mutual: D/E), H (1 mutual: F)
C: F (2 mutual: B/E), G (2 mutual: D/E)
D: F (2 mutual: B/E), H (1 mutual: G)
E: H (2 mutual: F/G), I (1 mutual: G)
F: A (2 mutual: B/E), C (2 mutual: B/E)
G: A (2 mutual: D/E), B (2 mutual: D/E)
H: E (2 mutual: F/G), K (2 mutual: I/J)
I: L (2 mutual: J/K), D (1 mutual: G)
J: G (2 mutual: H/I), F (1 mutual: H)
K: H (2 mutual: I/J), G (1 mutual: I)
L: I (2 mutual: J/K), H (1 mutual: J)
```

Identical to `pymk_pyspark_using_groupbykey.py`'s output on the same
input, and identical to the pure-Python output — the third
confirmation that all three scripts compute the same thing.

### `pymk_pyspark_using_dataframes.py`

Same CLI, same usage pattern:

```text
python3 pymk_pyspark_using_dataframes.py <input_file> [--top-k K] [--output OUT_DIR]
```

```text
$SPARK_HOME/bin/spark-submit pymk_pyspark_using_dataframes.py <input_file> [--top-k K] [--output OUT_DIR]
```

#### Sample run

```text
% python3 pymk_pyspark_using_dataframes.py data/friends.txt
input_file= data/friends.txt
people_in_graph= 6

A: E (3 mutual: B/C/D), F (2 mutual: B/C)
B: (no recommendations)
C: (no recommendations)
D: F (2 mutual: B/C)
E: A (3 mutual: B/C/D), F (2 mutual: B/C)
F: A (2 mutual: B/C), D (2 mutual: B/C), E (2 mutual: B/C)
```

Byte-for-byte identical to the other two PySpark scripts' output
above — the fourth confirmation that all four implementations compute
the same thing.

#### Step-by-Step Transformation Trace (`data/friends.txt`)

Unlike the two RDD scripts, there's no `flatMap`-per-record trace to
walk here — each "step" below is a DataFrame with a fixed schema,
built and `.show()`n by actually running `pymk_pyspark_using_dataframes.py`'s
pipeline stage by stage on the same 6-person graph. Every table below
is real Spark output, not hand-derived.

Recall the graph:

```text
A -> B C D
B -> A C D E F
C -> A B D E F
D -> A B C E
E -> B C D
F -> B C
```

**Step 1 — `spark.read.text(input_file)` → raw lines, then trim +
filter comments/blanks → `data_lines`**

Same 27 raw lines as the RDD scripts' `sc.textFile`, filtered down to
the 6 data lines:

```text
+-----------+
|line       |
+-----------+
|A,B,C,D    |
|B,A,C,D,E,F|
|C,A,B,D,E,F|
|D,A,B,C,E  |
|E,B,C,D    |
|F,B,C      |
+-----------+
```

**Step 2 — `split` + `slice` → `friends_df`**

One row per person, `friends` as an `array<string>` column instead of
a Python list — the DataFrame counterpart of `friends_rdd`:

```text
+------+---------------+
|person|friends        |
+------+---------------+
|A     |[B, C, D]      |
|B     |[A, C, D, E, F]|
|C     |[A, B, D, E, F]|
|D     |[A, B, C, E]   |
|E     |[B, C, D]      |
|F     |[B, C]         |
+------+---------------+
```

**Step 3 — `build_edges_df(friends_df)` → `edges_df`** (Rule 1)

`explode()` each person's friends into (person, friend) rows, then
`least()`/`greatest()` fixes the order and `distinct()` drops the
duplicate that each symmetric edge produces (22 exploded rows → 11
distinct edges — half of Step 4's RDD-trace count, since `distinct()`
collapses what the RDD traces leave as two `"FRIEND"` records per
edge):

```text
+---+---+
|a  |b  |
+---+---+
|A  |B  |
|A  |C  |
|A  |D  |
|B  |C  |
|B  |D  |
|B  |E  |
|B  |F  |
|C  |D  |
|C  |E  |
|C  |F  |
|D  |E  |
+---+---+
(count = 11)
```

**Step 4 — `build_vouches_df(friends_df)` → `vouches_df`** (Rule 2)

The self-join: explode `friends` twice (`friend1`, `friend2`), join
on `person`, keep `friend1 < friend2`. Same 33 rows as the RDD
scripts' Rule-2 output — the self-join produces exactly the
2-combinations `itertools.combinations()` produces, just via a join
condition instead of a loop:

```text
+---+---+-------+
|a  |b  |voucher|
+---+---+-------+
|A  |B  |C      |
|A  |B  |D      |
|A  |C  |B      |
|A  |C  |D      |
|A  |D  |B      |
|A  |D  |C      |
|A  |E  |B      |
|A  |E  |C      |
|A  |E  |D      |
|A  |F  |B      |
|A  |F  |C      |
|B  |C  |A      |
|B  |C  |D      |
|B  |C  |E      |
|B  |C  |F      |
|B  |D  |A      |
|B  |D  |C      |
|B  |D  |E      |
|B  |E  |C      |
|B  |E  |D      |
|B  |F  |C      |
|C  |D  |A      |
|C  |D  |B      |
|C  |D  |E      |
|C  |E  |B      |
|C  |E  |D      |
|C  |F  |B      |
|D  |E  |B      |
|D  |E  |C      |
|D  |F  |B      |
|D  |F  |C      |
|E  |F  |B      |
|E  |F  |C      |
+---+---+-------+
(count = 33)
```

**Step 5 — `vouches_df.groupBy("a", "b").agg(collect_set, count)` →
`grouped`** (shuffle + reduce, before suppressing friend pairs)

All 15 unique pairs (`C(6,2) = 15`, same as the RDD scripts'
`groupByKey()` step), each with its full voucher set and count — the
DataFrame counterpart of `grouped.mapValues(reduce_group)`, except
the "is this pair already friends?" question hasn't been asked yet:

```text
+---+---+--------------+------------+
|a  |b  |mutual_friends|mutual_count|
+---+---+--------------+------------+
|A  |B  |[C, D]        |2           |
|A  |C  |[B, D]        |2           |
|A  |D  |[B, C]        |2           |
|A  |E  |[B, C, D]     |3           |
|A  |F  |[B, C]        |2           |
|B  |C  |[A, D, E, F]  |4           |
|B  |D  |[A, C, E]     |3           |
|B  |E  |[C, D]        |2           |
|B  |F  |[C]           |1           |
|C  |D  |[A, B, E]     |3           |
|C  |E  |[B, D]        |2           |
|C  |F  |[B]           |1           |
|D  |E  |[B, C]        |2           |
|D  |F  |[B, C]        |2           |
|E  |F  |[B, C]        |2           |
+---+---+--------------+------------+
(count = 15)
```

**Step 6 — `grouped.join(edges_df, on=["a","b"], how="left_anti")` →
`reduced_df`**

The `left_anti` join is this script's version of "if `FRIEND` in
values: drop" — instead of checking membership inside a collected
list, it checks whether `(a, b)` has a matching row in `edges_df` at
all. 11 of the 15 grouped pairs match an edge and are dropped, the
same 4 survive as the RDD traces' `filtered` step:

```text
+---+---+--------------+------------+
|a  |b  |mutual_friends|mutual_count|
+---+---+--------------+------------+
|A  |E  |[B, C, D]     |3           |
|A  |F  |[B, C]        |2           |
|D  |F  |[B, C]        |2           |
|E  |F  |[B, C]        |2           |
+---+---+--------------+------------+
(count = 4)
```

**Step 7 — `unionByName` with `(a,b)` swapped → `symmetric_df`**

Each pair recommends each endpoint to the other — 4 pairs become 8
`(person, candidate, ...)` rows, the same shape and count as the RDD
traces' `symmetric`:

```text
+------+---------+------------+--------------+
|person|candidate|mutual_count|mutual_friends|
+------+---------+------------+--------------+
|A     |E        |3           |[B, C, D]     |
|A     |F        |2           |[B, C]        |
|D     |F        |2           |[B, C]        |
|E     |A        |3           |[B, C, D]     |
|E     |F        |2           |[B, C]        |
|F     |A        |2           |[B, C]        |
|F     |D        |2           |[B, C]        |
|F     |E        |2           |[B, C]        |
+------+---------+------------+--------------+
(count = 8)
```

**Step 8 — `Window.partitionBy("person").orderBy(...)` +
`row_number()` → `ranked_df`**

This is the step with no RDD-script equivalent shown as a single
operation — `groupByKey().mapValues(sort)` collapses 8 rows down to 4
grouped rows in one step, while the window function keeps all 8 rows
and adds a per-person `rank` column alongside them instead:

```text
+------+---------+------------+--------------+----+
|person|candidate|mutual_count|mutual_friends|rank|
+------+---------+------------+--------------+----+
|A     |E        |3           |[B, C, D]     |1   |
|A     |F        |2           |[B, C]        |2   |
|D     |F        |2           |[B, C]        |1   |
|E     |A        |3           |[B, C, D]     |1   |
|E     |F        |2           |[B, C]        |2   |
|F     |A        |2           |[B, C]        |1   |
|F     |D        |2           |[B, C]        |2   |
|F     |E        |2           |[B, C]        |3   |
+------+---------+------------+--------------+----+
(count = 8)
```

With `--top-k K` given, this step gains one more line —
`.filter(F.col("rank") <= K)` — right here, before the final join.

**Step 9 (final) — `all_people_df.join(ranked_df, on="person",
how="left")` → `final_df`**

The DataFrame counterpart of `leftOuterJoin`: restores `B` and `C`
with null candidate columns instead of dropping them (same fix as
Section 7's "people with zero candidates" correctness detail below,
applied at the DataFrame level):

```text
+------+---------+------------+--------------+----+
|person|candidate|mutual_count|mutual_friends|rank|
+------+---------+------------+--------------+----+
|A     |E        |3           |[B, C, D]     |1   |
|A     |F        |2           |[B, C]        |2   |
|B     |NULL     |NULL        |NULL          |NULL|
|C     |NULL     |NULL        |NULL          |NULL|
|D     |F        |2           |[B, C]        |1   |
|E     |A        |3           |[B, C, D]     |1   |
|E     |F        |2           |[B, C]        |2   |
|F     |A        |2           |[B, C]        |1   |
|F     |D        |2           |[B, C]        |2   |
|F     |E        |2           |[B, C]        |3   |
+------+---------+------------+--------------+----+
(count = 10)
```

`rows_to_recommendations()` then does what the RDD scripts do inside
Spark — group by `person`, sort by `(-mutual_count, candidate)`,
build the `{person: [...]}` dict `format_recommendations()` prints —
in plain Python after `.collect()`, since a DataFrame `collect()`
gives no ordering guarantee across a join to rely on directly (see
that function's docstring).

Row-count summary, to compare against both RDD traces at a glance:

| Step | DataFrame operation | Row count |
|---|---|---|
| 1 | `spark.read.text` + filter comments/blanks | 6 |
| 2 | `split`/`slice` → `friends_df` | 6 |
| 3 | `build_edges_df` (`explode` + `least`/`greatest` + `distinct`) | 11 (unique edges) |
| 4 | `build_vouches_df` (self-join) | 33 |
| 5 | `groupBy("a","b").agg(...)` | 15 (unique pairs) |
| 6 | `.join(edges_df, how="left_anti")` | 4 (non-friend pairs) |
| 7 | `.unionByName(swapped)` | 8 |
| 8 | `Window` + `row_number()` | 8 |
| 9 | `.join(all_people_df, how="left")` | 10 (8 candidate rows + 2 null rows for `B`/`C`) |

Notice Step 9's row count (10) is *not* "6, one per person" the way
the RDD traces' final step is — a DataFrame keeps one row per
`(person, candidate)` pair throughout (relational, not nested), so a
person with multiple candidates (like `F`, with 3) contributes
multiple rows, and only a person with *zero* candidates (`B`, `C`)
contributes exactly one (null) row. `rows_to_recommendations()`
performs the "one row per candidate" → "one entry per person, with a
list of candidates" reshaping that the RDD scripts get for free from
`groupByKey()`/`reduceByKey()`.

#### Sample run — larger graph, top-2 recommendations per person

```text
% python3 pymk_pyspark_using_dataframes.py data/friends_larger.txt --top-k 2
input_file= data/friends_larger.txt
people_in_graph= 12
top_k= 2

A: F (2 mutual: B/E), G (2 mutual: D/E)
B: G (2 mutual: D/E), H (1 mutual: F)
C: F (2 mutual: B/E), G (2 mutual: D/E)
D: F (2 mutual: B/E), H (1 mutual: G)
E: H (2 mutual: F/G), I (1 mutual: G)
F: A (2 mutual: B/E), C (2 mutual: B/E)
G: A (2 mutual: D/E), B (2 mutual: D/E)
H: E (2 mutual: F/G), K (2 mutual: I/J)
I: L (2 mutual: J/K), D (1 mutual: G)
J: G (2 mutual: H/I), F (1 mutual: H)
K: H (2 mutual: I/J), G (1 mutual: I)
L: I (2 mutual: J/K), H (1 mutual: J)
```

Identical to both RDD scripts' output on the same input — the fourth
confirmation that all five implementations compute the same thing.

### `pymk_pyspark_using_graphframes.py`

This script needs one more thing than the other four: the
[GraphFrames](https://graphframes.io/) package, on both sides of the
PySpark/JVM boundary.

```text
pip install graphframes-py
```

installs the Python wrapper the script imports; the actual
pattern-matching engine is a JVM library that PySpark loads
separately. The script's `main()` adds it via `spark.jars.packages`
on the `SparkSession` builder (pinned to
`io.graphframes:graphframes-spark4_2.13:0.10.0` — a Spark 4.x /
Scala 2.13 build, matching this folder's development environment), so
plain `python3` resolves and downloads it automatically through
Ivy/Maven the first time it runs (needs network access once; cached
locally after that):

```text
python3 pymk_pyspark_using_graphframes.py <input_file> [--top-k K] [--output OUT_DIR]
```

On a real cluster, pass the coordinate explicitly instead of relying
on the script's baked-in config, so the driver doesn't need internet
access at submit time — and update the version suffix
(`graphframes-spark4_2.13`) to match that cluster's Spark major
version and Scala version if it differs from this one (run
`python3 pymk_pyspark_using_graphframes.py --version-info` to print
this install's):

```text
$SPARK_HOME/bin/spark-submit --packages io.graphframes:graphframes-spark4_2.13:0.10.0 \
    pymk_pyspark_using_graphframes.py <input_file> [--top-k K] [--output OUT_DIR]
```

#### Sample run

```text
% python3 pymk_pyspark_using_graphframes.py data/friends.txt
input_file= data/friends.txt
people_in_graph= 6

A: E (3 mutual: B/C/D), F (2 mutual: B/C)
B: (no recommendations)
C: (no recommendations)
D: F (2 mutual: B/C)
E: A (3 mutual: B/C/D), F (2 mutual: B/C)
F: A (2 mutual: B/C), D (2 mutual: B/C), E (2 mutual: B/C)
```

Byte-for-byte identical to the other four scripts' output above — the
fifth and final confirmation that all five implementations compute
the same thing.

#### Step-by-Step Transformation Trace (`data/friends.txt`)

Every table below is real output from actually running
`pymk_pyspark_using_graphframes.py`'s pipeline stage by stage on the
same 6-person graph — nothing here is hand-derived.

Recall the graph:

```text
A -> B C D
B -> A C D E F
C -> A B D E F
D -> A B C E
E -> B C D
F -> B C
```

**Step 1 — `build_graph(friends_df)` → `vertices`, `edges`, `g`**

One vertex per person; one *directed* edge per (person, friend) pair
— 22 edges, the same count as the RDD scripts' 22 Rule-1
`"FRIEND"`-tagged records (Step 4 of the `groupByKey()` trace above),
because both are "one record per existing (person, friend)
ordered-pair", just under different names:

```text
vertices (6 rows):        edges (22 rows):
+---+                     +---+---+
|id |                     |src|dst|
+---+                     +---+---+
|A  |                     |A  |B  |
|B  |                     |A  |C  |
|C  |                     |A  |D  |
|D  |                     |B  |A  |
|E  |                     |B  |C  |
|F  |                     |B  |D  |
+---+                     |B  |E  |
                           |B  |F  |
                           |C  |A  |
                           ...                  (22 rows total)
                           |F  |C  |
                           +---+---+
```

**Step 2 — `g.find("(a)-[]->(c); (b)-[]->(c)").filter("a.id != b.id")`
→ `motifs`** (Rule 2, in one declarative query)

Every `(a, c, b)` triple where `a` and `b` each have an edge to the
same vertex `c`, with `a != b` — 66 matches. This single query is
doing Rule 2's whole job: no `combinations()`, no self-join written
by hand, just "find two edges into the same vertex from two different
places":

```text
(A, B, C)   (A, B, D)   (A, B, E)   (A, B, F)   (A, C, B)
(A, C, D)   (A, C, E)   (A, C, F)   (A, D, B)   (A, D, C)
(A, D, E)   (B, A, C)   (B, A, D)   (B, C, A)   (B, C, D)
...                                            (66 rows total)
(F, B, D)   (F, B, E)   (F, C, A)   (F, C, B)   (F, C, D)
(F, C, E)
```

Compare against Step 5 of the `pymk_pyspark_using_groupbykey.py`
trace (`rule2_mutual_vouches`, 33 rows): that step emits one
`(sort_pair(x,y), vouching_person)` record per unordered pair;
`motifs` here has exactly *twice* as many rows (66 = 33 × 2), one for
`(x, y)` and one for `(y, x)`, because `a` and `b` range freely over
*all* vertices with a common neighbor rather than being pre-ordered
by `sort_pair()`. That turns out to be convenient, not wasteful — see
Step 4 below.

**Step 3 — `.select(a.id, b.id, c.id)` → `candidate_pairs`, then
`.join(edges, how="left_anti")` → `non_friend_pairs`** (Rule 1's job)

`candidate_pairs` just renames `motifs`' columns (still 66 rows); the
`left_anti` join against `edges` is this script's version of "if
FRIEND in values: drop" — instead of scanning a collected list for a
sentinel, it asks the same "does this key exist in that other table?"
question as a join. 48 of the 66 candidate rows have a matching edge
(already friends) and are dropped, leaving 18:

```text
+------+---------+-------------+
|person|candidate|mutual_friend|
+------+---------+-------------+
|A     |E        |B            |
|A     |E        |C            |
|A     |E        |D            |
|A     |F        |B            |
|A     |F        |C            |
|D     |F        |B            |
|D     |F        |C            |
|E     |A        |B            |
|E     |A        |C            |
|E     |A        |D            |
|E     |F        |B            |
|E     |F        |C            |
|F     |A        |B            |
|F     |A        |C            |
|F     |D        |B            |
|F     |D        |C            |
|F     |E        |B            |
|F     |E        |C            |
+------+---------+-------------+
(count = 18)
```

**Step 4 — `.groupBy("person", "candidate").agg(collect_set,
count)` → `reduced_df`**

Groups the 18 rows down to 8 `(person, candidate)` pairs — and
because Step 2's motif already produced both `(A, E, ...)` and `(E,
A, ...)` directions, this table is **already** in the final
per-person-recommendation shape. Unlike every other script in this
folder, there is no separate "symmetrize: union the rows with `a`/`b`
swapped" step here — the motif query made both directions in Step 2,
for free:

```text
+------+---------+--------------+------------+
|person|candidate|mutual_friends|mutual_count|
+------+---------+--------------+------------+
|A     |E        |[B, C, D]     |3           |
|A     |F        |[B, C]        |2           |
|D     |F        |[B, C]        |2           |
|E     |A        |[B, C, D]     |3           |
|E     |F        |[B, C]        |2           |
|F     |A        |[B, C]        |2           |
|F     |D        |[B, C]        |2           |
|F     |E        |[B, C]        |2           |
+------+---------+--------------+------------+
(count = 8)
```

Notice this is exactly Step 7 of the `pymk_pyspark_using_dataframes.py`
trace's `symmetric_df` above — same 8 rows, same values — reached
here in one fewer explicit step.

**Step 5 — `Window.partitionBy("person").orderBy(...)` +
`row_number()` → `ranked_df`**

Identical in shape and purpose to Step 8 of the
`pymk_pyspark_using_dataframes.py` trace — adds a per-person `rank`
column instead of collapsing rows, so `--top-k` becomes a
`.filter(rank <= K)`:

```text
+------+---------+--------------+------------+----+
|person|candidate|mutual_friends|mutual_count|rank|
+------+---------+--------------+------------+----+
|A     |E        |[B, C, D]     |3           |1   |
|A     |F        |[B, C]        |2           |2   |
|D     |F        |[B, C]        |2           |1   |
|E     |A        |[B, C, D]     |3           |1   |
|E     |F        |[B, C]        |2           |2   |
|F     |A        |[B, C]        |2           |1   |
|F     |D        |[B, C]        |2           |2   |
|F     |E        |[B, C]        |2           |3   |
+------+---------+--------------+------------+----+
(count = 8)
```

**Step 6 (final) — `all_people_df.join(ranked_df, on="person",
how="left")` → `final_df`**

Restores `B` and `C` with null candidate columns, same fix and same
result as Step 9 of the `pymk_pyspark_using_dataframes.py` trace:

```text
+------+---------+--------------+------------+----+
|person|candidate|mutual_friends|mutual_count|rank|
+------+---------+--------------+------------+----+
|A     |E        |[B, C, D]     |3           |1   |
|A     |F        |[B, C]        |2           |2   |
|B     |NULL     |NULL          |NULL        |NULL|
|C     |NULL     |NULL          |NULL        |NULL|
|D     |F        |[B, C]        |2           |1   |
|E     |A        |[B, C, D]     |3           |1   |
|E     |F        |[B, C]        |2           |2   |
|F     |A        |[B, C]        |2           |1   |
|F     |D        |[B, C]        |2           |2   |
|F     |E        |[B, C]        |2           |3   |
+------+---------+--------------+------------+----+
(count = 10)
```

Row-count summary, to compare against the other two Spark traces at a
glance:

| Step | GraphFrames operation | Row count |
|---|---|---|
| 1 | `build_graph` (vertices + directed edges) | 6 vertices, 22 edges |
| 2 | `g.find("(a)-[]->(c); (b)-[]->(c)")` + filter `a != b` | 66 |
| 3 | `.join(edges, how="left_anti")` | 18 (non-friend `(person,candidate,mutual_friend)` rows) |
| 4 | `.groupBy("person","candidate").agg(...)` | 8 (already symmetric — no union step needed) |
| 5 | `Window` + `row_number()` | 8 |
| 6 | `.join(all_people_df, how="left")` | 10 (8 candidate rows + 2 null rows for `B`/`C`) |

#### Sample run — larger graph, top-2 recommendations per person

```text
% python3 pymk_pyspark_using_graphframes.py data/friends_larger.txt --top-k 2
input_file= data/friends_larger.txt
people_in_graph= 12
top_k= 2

A: F (2 mutual: B/E), G (2 mutual: D/E)
B: G (2 mutual: D/E), H (1 mutual: F)
C: F (2 mutual: B/E), G (2 mutual: D/E)
D: F (2 mutual: B/E), H (1 mutual: G)
E: H (2 mutual: F/G), I (1 mutual: G)
F: A (2 mutual: B/E), C (2 mutual: B/E)
G: A (2 mutual: D/E), B (2 mutual: D/E)
H: E (2 mutual: F/G), K (2 mutual: I/J)
I: L (2 mutual: J/K), D (1 mutual: G)
J: G (2 mutual: H/I), F (1 mutual: H)
K: H (2 mutual: I/J), G (1 mutual: I)
L: I (2 mutual: J/K), H (1 mutual: J)
```

Identical to all four other scripts' output on the same input — the
fifth and final confirmation that all five implementations compute
the same thing.

## 7. How the Five Implementations Map Onto Each Other

| MapReduce concept (companion doc) | `pymk_pure_python.py` | `pymk_pyspark_using_groupbykey.py` | `pymk_pyspark_using_reducebykey.py` | `pymk_pyspark_using_dataframes.py` | `pymk_pyspark_using_graphframes.py` |
|---|---|---|---|---|---|
| Mapper Rule 1 (tag edges) | `map_person()`, first loop | `rule1_friend_tags()` via `flatMap` | `rule1_friend_tags()` via `flatMap` (emits a state tuple, not `"FRIEND"`) | `build_edges_df()` — `explode()` + `least()`/`greatest()` + `distinct()` | `build_graph()` — one directed edge per (person, friend); no separate tagging, since suppression happens later as a join |
| Mapper Rule 2 (vouch for pairs) | `map_person()`, second loop | `rule2_mutual_vouches()` via `flatMap` | `rule2_mutual_vouches()` via `flatMap` (emits a state tuple, not a raw name) | `build_vouches_df()` — a self-join of `friends` exploded twice, filtered on `friend1 < friend2` | `g.find("(a)-[]->(c); (b)-[]->(c)")` — one motif query; the pattern matcher enumerates shared-neighbor pairs directly |
| Shuffle & sort (group by key) | `shuffle()` — a `dict[pair, list]` built by hand | `.union().groupByKey()` | `.union().reduceByKey(combine_pair_states)` — merges with a map-side combiner instead of shipping every value | `.groupBy("a", "b").agg(collect_set, count)` | `.groupBy("person", "candidate").agg(collect_set, count)` |
| Reducer | `reduce_pair()` | `reduce_group()`, applied via `mapValues()` | folded into `combine_pair_states()` + a `.filter()`/`.map()` after `reduceByKey()` | a `.join(edges_df, how="left_anti")` — membership check as a join instead of a list scan | a `.join(edges, how="left_anti")` — same idea, against the graph's own edge table |
| Downstream "sort by count" pass | `recommendations_by_person()` | `.groupByKey().mapValues(rank)` on the symmetrized RDD | `.reduceByKey(merge_ranked_candidates)` — merges already-ranked lists instead of collecting then sorting | `Window.partitionBy("person").orderBy(...)` + `row_number()` — a declarative ranking window, no combiner to write | Same `Window` + `row_number()` — and no separate symmetrize step first, since the motif already produced both directions |
| Execution model | Single process, in-memory dicts, `O(1)` — fine for classroom-sized graphs | Distributed, partitioned, shuffles over the network — the point is *this* is what scales | Same, but with map-side pre-aggregation reducing shuffle volume — see the design-choice discussion below | Same distributed execution, but expressed declaratively so Spark's Catalyst optimizer chooses the join/aggregation strategy instead of the script hand-picking one | Same distributed execution again, on top of GraphFrames' graph abstraction (itself built from DataFrames + a pattern-matching layer) — needs the extra `graphframes` dependency |

The pure-Python version exists to make the algorithm's *logic*
inspectable without any framework machinery in the way. The two
RDD-based PySpark versions exist to show the same logic expressed in
the lower-level operators (`flatMap`, `groupByKey`/`reduceByKey`,
`mapValues`) a distributed engine provides, with each step an
explicit, hand-written transformation. The DataFrame version exists
to show the same logic again in Spark SQL's declarative, columnar
vocabulary — joins, `groupBy().agg()`, window functions — which reads
closer to "what to compute" than "how to compute it" and lets
Catalyst pick execution strategies (broadcast vs. shuffle join,
partial aggregation, etc.) the RDD scripts pick by hand. The
GraphFrames version goes one step further: it exists to show that
once the data is modeled as an actual *graph* rather than two flat
tables, "find pairs with a mutual friend" stops being something you
build out of joins and becomes something you ask for directly, in one
pattern-match query — see the GraphFrames-vs-DataFrame discussion
below. All four Spark scripts are legitimate starting points for
`spark-submit` against a bigger dataset or a cluster; which one to
reach for in practice is mostly a house-style (and, for GraphFrames, a
dependency-tolerance) question.

### A design choice worth calling out: `groupByKey`, not `reduceByKey`

`pymk_pyspark_using_groupbykey.py` uses `groupByKey()` rather than the usually-preferred
`reduceByKey()`. That's deliberate, not an oversight: with the values
`pymk_pyspark_using_groupbykey.py`'s mapper emits — the raw string `"FRIEND"`, or a
raw person id — the reduce step needs to see the **entire** list of
values for a key before it can decide whether to drop the pair
(`"FRIEND"` present) or report a count. There's no way to combine two
of *those* particular values into a third value of the same type that
preserves enough information, unlike Word Count's simple integer
`add`. See Section 13 of the companion document ("Combiner —
In-Mapper Combining, Not a Per-Record Combiner") for the same
conclusion reached independently on the MapReduce side.

That doesn't mean `reduceByKey()` is off the table for this
algorithm — only that it can't be a drop-in swap for `groupByKey()`
here. [`pymk_pyspark_using_reducebykey.py`](pymk_pyspark_using_reducebykey.py)
gets there by changing *what the mapper emits*: instead of a raw
string, each record carries a small state,
`(is_friend: bool, mutual_friend_names: frozenset[str])`, with an
associative, commutative merge function over that state
(`combine_pair_states()`) — two states combine to
`(True, frozenset())` the moment either side is known to be a friend
pair, discarding any accumulated names early since they'll never be
reported anyway. `reduceByKey()` runs that merge as a map-side
combiner before the shuffle, exactly like a MapReduce combiner —
something `groupByKey()` has no equivalent hook for. It applies the
same trick a second time for the final per-person ranking step
(`merge_ranked_candidates()`: merging two already-ranked, already
top-K-truncated lists into one). Read that file's module docstring
for the full reasoning; both scripts are verified to produce
byte-for-byte identical output on both sample graphs.

For very large, high-degree graphs, prefer `aggregateByKey()` with an
early "already saw FRIEND, stop looking" short-circuit instead of
materializing the full value list per key — see Section 8 below.

### DataFrame vs. RDD: why `pymk_pyspark_using_dataframes.py` looks so different

The two RDD scripts above differ in *how* they aggregate
(`groupByKey()` vs. `reduceByKey()`) but agree on vocabulary —
`flatMap`, tuples, Python functions passed as arguments.
[`pymk_pyspark_using_dataframes.py`](pymk_pyspark_using_dataframes.py)
looks unlike either, because the DataFrame API isn't "the RDD API
with different method names" — it's a different programming model:
columns and query-plan operators instead of arbitrary Python
callables over opaque records. Two consequences worth calling out:

* **No `combinations()`, so Rule 2 becomes a join.** A pure/RDD
  mapper can just write `for x, y in combinations(friends, 2)`
  inside a Python function. A DataFrame column is not something you
  iterate a nested loop over — the DataFrame way to pair up elements
  *within* the same row's array is to `explode()` it twice (under
  two aliases) and join those two exploded views back together on
  the original row's key, then filter out the diagonal and one
  ordering. It computes the identical set of pairs; it just has to
  be phrased as a join because there's no per-row Python loop to drop
  into.
* **No membership check in a Python list, so dropping friend pairs
  becomes a `left_anti` join.** `reduce_group()`/`combine_pair_states()`
  can just ask `"FRIEND" in values` because they run as ordinary
  Python code against a collected/merged list. A DataFrame reducer
  step is a relational operator, not a Python function with access to
  a materialized list — the relational way to ask "does this key
  exist in that other table?" is a `left_anti` join against
  `edges_df`, which is exactly what a query planner would choose for
  a set-membership filter anyway.

Every script here still implements `sort_pair()`'s job — collapsing
`(X, Y)` and `(Y, X)` to one key — but the three RDD-based scripts do
it once, up front, as a plain function call, while the DataFrame
script does it per join column via `least()`/`greatest()`. Neither
approach is "more correct" than the other; which one reads more
naturally often
comes down to whether your team's Spark code already leans RDD or
DataFrame — see [Section 9](#9-extending-this) for further directions
either style could be taken.

### GraphFrames vs. DataFrame: Rule 2 as a graph query instead of a join

[`pymk_pyspark_using_dataframes.py`](pymk_pyspark_using_dataframes.py)'s
Rule 2 (`build_vouches_df()`) is a *workaround*: a DataFrame has no
native notion of "two people connected through a third," so the
script fakes it with a self-join — explode the same array twice under
different aliases, join those two views back together, filter out the
diagonal. It computes the right answer, but nothing about the join
*says* "shared neighbor" — you have to already know the trick to
recognize what it's doing.

[`pymk_pyspark_using_graphframes.py`](pymk_pyspark_using_graphframes.py)
replaces that whole self-join with one line that says exactly what it
means:

```python
g.find("(a)-[]->(c); (b)-[]->(c)").filter("a.id != b.id")
```

"Two edges into the same vertex `c`, from two different vertices `a`
and `b`" *is* the definition of "`a` and `b` have a mutual friend
`c`." Loading the data into a `GraphFrame` first — a small amount of
extra setup (`build_graph()`) — buys a pattern-matching query language
where the question you actually want to ask ("who shares a neighbor
with whom?") is directly expressible, instead of needing to be
reconstructed from `explode`/join/filter primitives that don't know
what a graph is. That's the sense in which this version is "more
elegant" and not just shorter: the DataFrame script's join *implements*
graph adjacency by hand; the GraphFrames script's motif query *is*
graph adjacency, used directly.

The trade-off is the dependency itself (see Section 6 above) and a
second one worth flagging: `g.find()` enumerates a full path for every
matching combination before any filtering happens (66 rows for this
6-person graph before the `left_anti` join trims it to 18 — see that
script's trace), the same `C(d,2)`-per-hub-node blowup Rule 2 always
has, just produced by a general-purpose pattern matcher instead of a
purpose-built self-join. For very large, high-degree graphs, a
hand-tuned DataFrame or RDD join over a pre-filtered edge set can still
out-scale a generic motif search — GraphFrames buys clarity, not a
free performance win. See [Section 8](#8-complexity--scalability)
below.

### A correctness detail: people with zero candidates

A person who is already friends with everyone else in the graph (`B`
and `C` in `data/friends.txt`) never vouches for a pair involving
themselves as a *candidate*, and so never appears as a key in the
grouped-candidates RDD (or DataFrame) on its own. The two RDD scripts
restore them with an empty list via a `leftOuterJoin()`; both
DataFrame-shaped scripts (`pymk_pyspark_using_dataframes.py` and
`pymk_pyspark_using_graphframes.py`) restore them the relational way,
via a `how="left"` join against `all_people_df` that leaves
`candidate`/`mutual_count`/`mutual_friends` as `NULL` for that row
(see the last step of each script's trace above). All four PySpark
scripts' output — deliberately — accounts for every person in the
input graph, not just the ones with at least one recommendation.

## 8. Complexity & Scalability

Rule 2 emits `C(d, 2)` records for a person with `d` friends —
quadratic in that person's degree, not linear. Real social graphs
are power-law: a small number of hub nodes (public figures, brand
pages) have enormous friend/follower counts, and that tail dominates
cost:

```text
d =     200 friends   ->  C(200, 2)     =      19,900 emitted records
d =   10,000 friends  ->  C(10,000, 2)  =  49,995,000 emitted records
d = 1,000,000 friends ->  C(1e6, 2)     ≈ 5 * 10^11 emitted records
```

None of the five scripts here does anything about that — they're
teaching implementations over classroom-sized graphs (6 and 12
people). A production system would add:

1. **Degree capping / sampling** — cap how many of a hub node's
   friends participate in Rule 2, trading recall for tractability.
2. **Incremental computation** — recompute only the neighborhoods
   that changed since the last run, not a full batch recompute.
3. **Graph-native engines** — Pregel/GraphX-style frameworks
   partition and process neighborhoods more efficiently than a flat
   pairs-emission when a few very-high-degree vertices dominate.
4. **Approximate set overlap** — MinHash/LSH sketches turn "exact
   intersection of two friend lists" into "estimate overlap from
   small fixed-size sketches," avoiding materializing `C(d,2)` pairs
   for a hub node.

Full derivation, worked numbers, and discussion:
[Section 7](../../../mapreduce/mapreduce_examples/MapReduce_People_You_May_Know.md#7-is-this-a-big-data-problem)
and
[Section 14](../../../mapreduce/mapreduce_examples/MapReduce_People_You_May_Know.md#14-complexity--scalability-notes)
of the companion document.

## 9. Extending This

A few directions to take this code, in roughly increasing order of
effort (also posed as open questions in
[Section 16](../../../mapreduce/mapreduce_examples/MapReduce_People_You_May_Know.md#16-food-for-thought)
of the companion document):

1. **Top-K per person** — already implemented (`--top-k`) two ways:
   a Python-side `sorted()[:k]` inside `mapValues`/`reduceByKey` in
   the two RDD scripts, and a Spark DataFrame `Window` function
   (`row_number()` + `filter(rank <= K)`) in both
   [`pymk_pyspark_using_dataframes.py`](pymk_pyspark_using_dataframes.py)
   and [`pymk_pyspark_using_graphframes.py`](pymk_pyspark_using_graphframes.py) —
   which avoids collecting full candidate lists into a single
   partition's memory the way `mapValues(sorted)` does.
2. **Directed graphs** — adapt Rule 1 and Rule 2 for one-way
   "follows" relationships (Twitter/X-style) instead of symmetric,
   Facebook-style friendships (see the callout in
   [Section 1](#1-the-problem)) — mutual friends and one-way follows
   are different graphs, so this changes what "mutual" even means,
   not just how the pipeline is executed.
3. **Exclusion lists** — suppress candidates a user has already
   dismissed, by anti-joining against a "dismissed" RDD/DataFrame
   before ranking.
4. **Weighted scoring** — instead of raw mutual-friend count, weight
   by interaction recency/strength, and merge with other signals
   (shared groups, shared workplace, etc.) — real PYMK systems use
   dozens of signals, of which mutual-friend count is just one.
5. **Robustness** — a friend list with duplicate entries or a
   person's own id (a data bug) will currently corrupt counts; add
   validation to `parse_line()`/`read_graph()`.
6. **Other graph algorithms, now that the data is a `GraphFrame`** —
   `pymk_pyspark_using_graphframes.py` already pays the cost of
   building a graph; once it exists, GraphFrames' other built-ins
   become one-line experiments on the same data: `triangleCount()` to
   flag tightly-knit friend groups, `connectedComponents()` to check
   whether the whole graph is really one community or several
   isolated ones, or `pageRank()`/`stronglyConnectedComponents()` as
   an alternative ranking signal to blend with raw mutual-friend
   count (see item 4 above).

## 10. Comparison with Another Implementation

[Andres Romero's "People You May Know"](https://andresromero.github.io/People-you-may-know/)
is another write-up of this same MapReduce exercise, and lands on the
same core algorithm — same Rule 1 / Rule 2 mapper split, same
"group by pair, filter, rank by count" reducer shape. The two
differ mainly in *how* the reducer suppresses an already-friend
pair:

| | Romero's version | This folder |
|---|---|---|
| Mapper: existing edges | Emit `(sorted(user, friend), 1)` | Emit `(sort_pair(P, F), "FRIEND")` |
| Mapper: candidate pairs | Emit `(sorted(friend_i, friend_j), 0)` | Emit `(sort_pair(X, Y), P)` |
| Suppressing existing friends | Sum all values into a counter, **decrement it by 1** whenever a `flag=1` record is seen, and set a separate boolean flag | Check **membership**: if `"FRIEND"` appears anywhere in the group, drop the pair — no arithmetic |
| What the tag value means | An arithmetic quantity being summed (a counter contribution) | A sentinel used only for a presence check |
| Output per person | Top 10 by count, ties broken by ascending user ID | All (or `--top-k`) by count, ties broken alphabetically by candidate name |
| Mutual-friend identities | Not kept — only the count and recommended IDs | Kept — output includes *which* friends are mutual |

The one design difference worth flagging: Romero's `counter -= 1`
trick assumes exactly one `flag=1` record shows up per already-friend
pair. That's true for clean input, but it's a more fragile invariant
to depend on than a presence check — a duplicate edge in messy input
would silently produce a wrong (off-by-one) mutual-friend count
instead of failing safe the way a membership check does. Otherwise
the two implementations are the same algorithm, independently
arrived at — which is expected, since this is the standard
Hadoop-course PYMK exercise (see References below).

## 11. References

1. [`MapReduce_People_You_May_Know.md`](../../../mapreduce/mapreduce_examples/MapReduce_People_You_May_Know.md) —
   the full MapReduce derivation this folder implements, worked by
   hand on the same 6-person graph used here as `data/friends.txt`.
2. [`MapReduce_Finding_Friends.html`](../../../mapreduce/mapreduce_examples/MapReduce_Finding_Friends.html) —
   the companion "mutual friends for pairs who are already friends"
   example that motivates this one.
3. [Data-Intensive Text Processing with MapReduce by Jimmy Lin and Chris Dyer](https://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf) —
   source of the "pairs vs. stripes" and in-mapper combining patterns
   referenced in Section 7 above.
4. [Introduction to MapReduce by Mahmoud Parsian](http://mapreduce4hackers.com/docs/Introduction-to-MapReduce.pdf)
5. [People You May Know by Andres Romero](https://andresromero.github.io/People-you-may-know/) —
   an independent write-up of the same algorithm, compared with this
   folder's implementation in Section 10 above.
6. [GraphFrames documentation](https://graphframes.io/) — the package
   [`pymk_pyspark_using_graphframes.py`](pymk_pyspark_using_graphframes.py)
   is built on, including the motif-finding (`find()`) syntax used
   for Rule 2 and the other graph algorithms mentioned in Section 9,
   item 6.

## 12. Comments

Comments and suggestions are welcome!
