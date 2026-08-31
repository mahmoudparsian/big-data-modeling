# People You May Know (PYMK)

Three runnable implementations of the classic "People You May Know"
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
  [Section 7](#7-how-the-two-implementations-map-onto-each-other) for
  why that requires reshaping what the mapper emits, not just
  swapping one method call for another.

All three scripts implement **the same algorithm**, produce **the
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
of the companion MapReduce document, and all three scripts in this
folder name their functions after it (`sort_pair`, `map_person`/
`rule1_friend_tags`, `reduce_pair`/`reduce_group`/`combine_pair_states`)
so you can read the code and the algorithm doc side by side.

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
comments, ignored by all three scripts):

```text
<person>,<friend_1>,<friend_2>,...,<friend_n>
```

Friendship is symmetric, so every edge appears on both endpoints'
lines (see [`data/friends.txt`](data/friends.txt)).

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

## 7. How the Three Implementations Map Onto Each Other

| MapReduce concept (companion doc) | `pymk_pure_python.py` | `pymk_pyspark_using_groupbykey.py` | `pymk_pyspark_using_reducebykey.py` |
|---|---|---|---|
| Mapper Rule 1 (tag edges) | `map_person()`, first loop | `rule1_friend_tags()` via `flatMap` | `rule1_friend_tags()` via `flatMap` (emits a state tuple, not `"FRIEND"`) |
| Mapper Rule 2 (vouch for pairs) | `map_person()`, second loop | `rule2_mutual_vouches()` via `flatMap` | `rule2_mutual_vouches()` via `flatMap` (emits a state tuple, not a raw name) |
| Shuffle & sort (group by key) | `shuffle()` — a `dict[pair, list]` built by hand | `.union().groupByKey()` | `.union().reduceByKey(combine_pair_states)` — merges with a map-side combiner instead of shipping every value |
| Reducer | `reduce_pair()` | `reduce_group()`, applied via `mapValues()` | folded into `combine_pair_states()` + a `.filter()`/`.map()` after `reduceByKey()` |
| Downstream "sort by count" pass | `recommendations_by_person()` | `.groupByKey().mapValues(rank)` on the symmetrized RDD | `.reduceByKey(merge_ranked_candidates)` — merges already-ranked lists instead of collecting then sorting |
| Execution model | Single process, in-memory dicts, `O(1)` — fine for classroom-sized graphs | Distributed, partitioned, shuffles over the network — the point is *this* is what scales | Same, but with map-side pre-aggregation reducing shuffle volume — see the design-choice discussion below |

The pure-Python version exists to make the algorithm's *logic*
inspectable without any framework machinery in the way. The PySpark
version exists to show the same logic expressed in the operators
(`flatMap`, `groupByKey`, `mapValues`) a real distributed engine
provides, and to be a starting point you can actually `spark-submit`
against a bigger dataset or a cluster.

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

### A correctness detail: people with zero candidates

A person who is already friends with everyone else in the graph (`B`
and `C` in `data/friends.txt`) never vouches for a pair involving
themselves as a *candidate*, and so never appears as a key in the
Spark job's grouped-candidates RDD on its own. `build_recommendations()`
restores them with an empty list via a `leftOuterJoin()` against the
full set of people, so both PySpark scripts' output — deliberately —
accounts for every person in the input graph, not just the ones with
at least one recommendation.

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

Neither script here does anything about that — they're teaching
implementations over classroom-sized graphs (6 and 12 people). A
production system would add:

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

1. **Top-K per person** — already implemented (`--top-k`); try
   wiring it into a Spark DataFrame `Window` function instead of a
   Python-side `sorted()[:k]` inside `mapValues`, which avoids
   collecting full candidate lists into a single partition's memory.
2. **Directed graphs** — adapt Rule 1 and Rule 2 for one-way
   "follows" relationships (Twitter-style) instead of symmetric
   friendships.
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

## 12. Comments

Comments and suggestions are welcome!
