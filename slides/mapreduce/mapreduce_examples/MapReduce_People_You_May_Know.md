# MapReduce Example: <br> People You May Know (PYMK)<br> (Mutual Friends for Non-Friends)

	Author: Mahmoud Parsian
	Last updated: 8/30/2026

## 1. Introduction

This example is a companion to
[`MapReduce_Finding_Friends.html`](MapReduce_Finding_Friends.html).
That example solves a real but narrower problem: given two people
who are **already friends**, find the friends they have in common
(the classic "You and Joe have 230 friends in common" feature shown
on a friend's profile).

It cannot answer a more valuable question: given two people who are
**not** friends, do they have friends in common, and if so, how
many? That second question is exactly the seed algorithm behind
"People You May Know" (PYMK)-style recommendations on Facebook,
LinkedIn, and similar social platforms. This document works through
that algorithm in MapReduce, step by step, on a small sample
dataset.

## 2. Problem

Given a social graph represented as 

`Person -> [List of Friends]`

for **every pair of people who are not already friends**, find:

- whether they have any friends in common, and
- if so, the list (and count) of those mutual friends.

Pairs who are already friends should **not** appear in the output —
they are the case the companion "Finding Friends" example already
covers, and recommending someone as a "person you may know" when
you're already friends with them makes no sense.

## 3. Why the "Finding Friends" Algorithm Can't Answer This

In `MapReduce_Finding_Friends.html`, the mapper only pairs up a
person `A` with the people already *in* `A`'s own friend list:

```text
map(A -> [B, C, D]):
    emit (A, B) -> [B, C, D]
    emit (A, C) -> [B, C, D]
    emit (A, D) -> [B, C, D]
```

Every key that algorithm can ever produce is, by construction, a
pair of people who are directly connected — `A` and `B` are
friends because `B` came from `A`'s own friend list. Two people who
are *not* friends never appear in each other's friend lists, so no
key is ever generated for them, and the reducer never gets a chance
to compute anything for that pair. The gap is structural, not a
bug — it's simply a different question than the one that algorithm
was built to answer.

## 4. The Key Idea

If a person `X` appears in the friend lists of both `A` and `E`,
then `X` is a mutual friend of `A` and `E` — regardless of whether
`A` and `E` are friends with each other. Turning this around: for
**every** person `P`, every *pair* of `P`'s own friends shares `P`
as a mutual friend.

So the mapper does two things instead of one:

1. **Tag existing edges.** For each friend `F` of `P`, emit the pair
   `(P, F)` with a special marker `FRIEND`. This records which pairs
   are already directly connected, so the reducer can throw them out.
2. **Vouch for every pair of P's friends.** For every two friends
   `(X, Y)` in `P`'s own friend list, emit the pair `(X, Y)` with
   value `P` — "`P` is a mutual friend of `X` and `Y`."

After grouping by key, any pair `(X, Y)` whose group contains a
`FRIEND` marker is already connected and gets dropped. Any pair
whose group contains only vouching names is a non-friend pair with
one or more friends in common — a PYMK candidate, ranked by how
many names showed up.

## 5. Input Data Format

Same adjacency-list shape as the Finding Friends example, one line
per person:

```text
<person>,<friend_1>,<friend_2>,...,<friend_n>
```

## 6. Sample Dataset

We extend the 5-person graph from the Finding Friends example with
one more person, `F`, who is only friends with `B` and `C`. This
gives us more than one non-friend pair to rank, which the original
5-person graph (only one non-edge) doesn't show.

```text
A,B,C,D
B,A,C,D,E,F
C,A,B,D,E,F
D,A,B,C,E
E,B,C,D
F,B,C
```

As a friendship graph:

```text
A -> B C D
B -> A C D E F
C -> A B D E F
D -> A B C E
E -> B C D
F -> B C
```

Of the `C(6,2) = 15` possible pairs among `{A,B,C,D,E,F}`, 11 are
already direct friendships:

```text
AB  AC  AD  BC  BD  BE  BF  CD  CE  CF  DE
```

and 4 are **not**:

```text
AE  AF  DF  EF
```

Those 4 pairs are exactly what this algorithm should surface —
each with its list of mutual friends.

## 7. Is This a Big Data Problem?

Yes, and more aggressively than most MapReduce examples. A
real social network isn't 6 people, it's on the order of billions,
with average friend counts in the hundreds. Rule 2 below emits
`C(d, 2)` records for a person with `d` friends — quadratic in
degree, not linear. For a graph with `N` people and average degree
`d`, total mapper output is on the order of `N * d^2 / 2`. Even a
modest average degree makes this enormous:

```text
N = 1,000,000,000 people
d = 200 average friends
N * C(d, 2) = 1,000,000,000 * 19,900 ≈ 2 * 10^13 emitted records
```

That's 20 trillion intermediate records from the vouching step
alone, before accounting for people whose friend count is far
above average (see Section 12). This is precisely the kind of
shuffle-heavy, embarrassingly-parallel-but-enormous workload
MapReduce (or Spark) is for.

## 8. Output Data Format

One record per non-friend pair with at least one mutual friend,
key is the pair, value is the count and the list of mutual friends:

```text
((A, E), 3, [B, C, D])
((A, F), 2, [B, C])
((D, F), 2, [B, C])
((E, F), 2, [B, C])
```

## 9. Input to Mappers

As in the Finding Friends example, each mapper call receives one
person and their friend list:

```text
(A, [B, C, D])
(B, [A, C, D, E, F])
(C, [A, B, D, E, F])
(D, [A, B, C, E])
(E, [B, C, D])
(F, [B, C])
```

## 10. Mapper

### 10.1 `sort_pair()` function

```python
def sort_pair(a, b):
    """Return two comparable values in ascending order."""
    return (a, b) if a < b else (b, a)
```

### 10.2 Mapper function

```text
# key: person P
# value: P's friend list, an array of friend IDs
#
map(P, friends) {

   # --- Rule 1: tag existing edges ---
   # so the reducer can suppress pairs that are already friends
   for each F in friends:
      pair = sort_pair(P, F)
      emit(pair, "FRIEND")

   # --- Rule 2: vouch for every pair of P's own friends ---
   # if P is friends with both X and Y, P is a mutual
   # friend of X and Y, whether or not X and Y are friends
   for i in 0 .. len(friends) - 1:
      for j in i + 1 .. len(friends) - 1:
         pair = sort_pair(friends[i], friends[j])
         emit(pair, P)
}
```

`sort_pair(a, b)` returns `(a, b)` with the two IDs in a fixed
order (e.g., lexicographic), so that `(A, E)` and `(E, A)` always
collapse to the same key.

### 10.1 Rule 1 output (FRIEND tags), all 6 people

```text
map(A, [B,C,D]):          (A,B)->FRIEND  (A,C)->FRIEND  (A,D)->FRIEND
map(B, [A,C,D,E,F]):      (A,B)->FRIEND  (B,C)->FRIEND  (B,D)->FRIEND  (B,E)->FRIEND  (B,F)->FRIEND
map(C, [A,B,D,E,F]):      (A,C)->FRIEND  (B,C)->FRIEND  (C,D)->FRIEND  (C,E)->FRIEND  (C,F)->FRIEND
map(D, [A,B,C,E]):        (A,D)->FRIEND  (B,D)->FRIEND  (C,D)->FRIEND  (D,E)->FRIEND
map(E, [B,C,D]):          (B,E)->FRIEND  (C,E)->FRIEND  (D,E)->FRIEND
map(F, [B,C]):            (B,F)->FRIEND  (C,F)->FRIEND
```

### 10.2 Rule 2 output (mutual-friend vouches), all 6 people

```text
map(A, [B,C,D]):
   pairs of {B,C,D}:             (B,C)->A  (B,D)->A  (C,D)->A

map(B, [A,C,D,E,F]):
   pairs of {A,C,D,E,F}:         (A,C)->B  (A,D)->B  (A,E)->B  (A,F)->B
                                  (C,D)->B  (C,E)->B  (C,F)->B
                                  (D,E)->B  (D,F)->B  (E,F)->B

map(C, [A,B,D,E,F]):
   pairs of {A,B,D,E,F}:         (A,B)->C  (A,D)->C  (A,E)->C  (A,F)->C
                                  (B,D)->C  (B,E)->C  (B,F)->C
                                  (D,E)->C  (D,F)->C  (E,F)->C

map(D, [A,B,C,E]):
   pairs of {A,B,C,E}:           (A,B)->D  (A,C)->D  (A,E)->D
                                  (B,C)->D  (B,E)->D  (C,E)->D

map(E, [B,C,D]):
   pairs of {B,C,D}:             (B,C)->E  (B,D)->E  (C,D)->E

map(F, [B,C]):
   pairs of {B,C}:                (B,C)->F
```

A person with `d` friends contributes `d` Rule-1 records and
`C(d, 2)` Rule-2 records — `B` and `C`, each with 5 friends here,
contribute 5 + 10 = 15 records apiece, the largest of the six.

## 11. Sort & Shuffle Phase

All emitted `(pair, value)` records — from both rules, from all six
mappers — are grouped by key. Combining Sections 10.1 and 10.2:

```text
(A,B) -> [FRIEND, FRIEND, C, D]              <- has FRIEND
(A,C) -> [FRIEND, FRIEND, B, D]              <- has FRIEND
(A,D) -> [FRIEND, FRIEND, B, C]              <- has FRIEND
(A,E) -> [B, C, D]                           <- no FRIEND!
(A,F) -> [B, C]                              <- no FRIEND!
(B,C) -> [FRIEND, FRIEND, A, D, E, F]        <- has FRIEND
(B,D) -> [FRIEND, FRIEND, A, C, E]           <- has FRIEND
(B,E) -> [FRIEND, FRIEND, C, D]              <- has FRIEND
(B,F) -> [FRIEND, FRIEND, C]                 <- has FRIEND
(C,D) -> [FRIEND, FRIEND, A, B, E]           <- has FRIEND
(C,E) -> [FRIEND, FRIEND, B, D]              <- has FRIEND
(C,F) -> [FRIEND, FRIEND, B]                 <- has FRIEND
(D,E) -> [FRIEND, FRIEND, B, C]              <- has FRIEND
(D,F) -> [B, C]                              <- no FRIEND!
(E,F) -> [B, C]                              <- no FRIEND!
```

Exactly the four non-edges from Section 6 — `(A,E)`, `(A,F)`,
`(D,F)`, `(E,F)` — come through with no `FRIEND` marker. Every
other key has at least one, because the two endpoints' own `map()`
calls always tag their real edges.

## 12. Reducer

```text
# key: a pair (X, Y)
# values: Iterable<String> -- either "FRIEND" or a vouching person ID
#
reduce(pair, values) {
   mutual_friends = []
   for v in values:
      if v == "FRIEND":
         # already friends -- suppress, emit nothing
         return                       
      mutual_friends.append(v)

   emit(pair, (len(mutual_friends), sorted(mutual_friends)))
}
```

Applied to Section 11's groups, the 11 `FRIEND`-tagged pairs emit
nothing, and the 4 clean pairs produce:

```text
((A, E), 3, [B, C, D])
((A, F), 2, [B, C])
((D, F), 2, [B, C])
((E, F), 2, [B, C])
```

which matches the Section 8 output exactly, and matches a direct
set-intersection sanity check (e.g., `friends(A) ∩ friends(E) =
{B,C,D} ∩ {B,C,D} = {B,C,D}`, count 3).

A downstream sort (a second, trivial MapReduce/Spark pass, or just
an in-memory `sortBy` if the result set is small enough) ranks these
by count to decide what to actually show a user: `A` would see `E`
recommended above `F`, since `A` and `E` have 3 friends in common
versus 2.

## 13. Combiner — In-Mapper Combining, Not a Per-Record Combiner

A classic per-record combiner (like Word Count's) doesn't help much
here: within a *single* `map()` call, each key from Rule 2 is
produced exactly once (each unordered pair of friends appears once
in the double loop), so there's nothing local to pre-sum.

What does help at scale is the **in-mapper combining** pattern (Lin
& Dyer, *Data-Intensive Text Processing with MapReduce*): instead of
emitting immediately, a mapper task accumulates vouch-lists in a
local hash map across *all* the person-records it processes in that
task (`setup()` → many `map()` calls → `cleanup()`), and only emits
once per key at `cleanup()` time. If two people processed by the
same mapper task happen to vouch for the same pair, their vouches
are merged locally before ever hitting the shuffle, cutting network
I/O. It doesn't change the worst case (Section 14), but it reduces
shuffle volume in practice.

## 14. Complexity & Scalability Notes

Rule 2's cost is `C(d, 2)` per person, quadratic in that person's
degree `d`. Real social graphs are **power-law**: most people have a
modest number of friends, but a small number of hub nodes (public
figures, brand pages) have enormous ones. That tail dominates cost:

```text
d =     200 friends   ->  C(200, 2)     =      19,900 emitted records
d =   10,000 friends  ->  C(10,000, 2)  =  49,995,000 emitted records
d = 1,000,000 friends ->  C(1e6, 2)     ≈ 5 * 10^11 emitted records
```

A single celebrity account with a million friends/followers would,
on its own, emit half a trillion Rule-2 records from one `map()`
call — infeasible in a naive implementation. This is the same
"hub node" scalability problem noted for the original Finding
Friends example, and it's inherent to *exactly* computing common-
neighbor counts for every non-adjacent pair — you cannot know two
people share a friend without, in some form, looking at pairs of
that friend's connections.

Mitigations used in practice (LinkedIn's and Facebook's PYMK-style
systems, in spirit if not exact implementation):

1. **Degree capping / sampling** — cap how many of a hub node's
   friends participate in Rule 2 (e.g., sample or take the most
   "recently active" `K` friends), trading recall for tractability.
2. **Two-hop / incremental computation** — recompute only the
   neighborhoods that changed since the last run (new friendships)
   instead of a full daily batch recompute over the whole graph.
3. **Graph-native engines** — frameworks built for iterative graph
   computation (Pregel, Spark GraphX) partition and process
   neighborhoods more efficiently than a flat pairs-emission when
   the workload is dominated by a few very-high-degree vertices.
4. **Approximate set overlap** — MinHash/LSH-style sketches of each
   person's friend set turn "exact intersection of two friend
   lists" into "estimate overlap from small fixed-size sketches,"
   avoiding ever materializing `C(d,2)` pairs for a hub node.

## 15. Spark Equivalent (Sketch)

```python
from itertools import combinations

def rule1_edges(person_friends):
    person, friends = person_friends
    for f in friends:
        yield (tuple(sorted((person, f))), "FRIEND")

def rule2_vouches(person_friends):
    person, friends = person_friends
    for x, y in combinations(sorted(friends), 2):
        yield ((x, y), person)

edges = friends_rdd.flatMap(rule1_edges)
vouches = friends_rdd.flatMap(rule2_vouches)

recommendations = (
    edges.union(vouches)
         .groupByKey()
         .mapValues(list)
         .filter(lambda kv: "FRIEND" not in kv[1])
         .map(lambda kv: (kv[0], len(kv[1]), sorted(kv[1])))
         .sortBy(lambda triple: -triple[1])
)
```

`groupByKey` is the operation to watch here: it's exactly as
skew-sensitive as the shuffle in Section 14 — a hub node's pairs
land in whichever partitions their key pairs hash to, and if those
pairs cluster, a few executors get overloaded while others idle. For
production-scale graphs, prefer `aggregateByKey`/`reduceByKey` with
an early "already saw FRIEND, drop the rest" short-circuit, and
consider salting or pre-bucketing very high-degree vertices
separately, as in Section 14.

## 16. Food for Thought

1. `sort_pair` assumes stable, comparable person IDs. What breaks
   if IDs aren't globally comparable (e.g., UUIDs mixed with
   integers), and how would you fix it?

2. The reducer in Section 12 discards a pair the instant it sees a
   single `"FRIEND"` value, without looking at the rest of the
   list. In a real MapReduce/Spark job, does the iteration order of
   `values` matter for this early-exit to be correct? Why or why not?

3. How would you extend the output to produce, for each person, only
   their **top-K** recommendations by mutual-friend count, instead
   of every qualifying pair in the whole graph?

4. Suppose the platform models relationships as directed "follows"
   (like Twitter) rather than symmetric "friends." How would Rule 1
   and Rule 2 need to change?

5. A person's friend list might (due to a data bug) contain
   duplicate entries or the person's own ID. How would you make the
   mapper robust to that, and what would happen to the output if you
   didn't?

6. Section 14 lists degree-capping as a mitigation. If you cap a
   hub node's friend list to a random sample of `K` friends before
   running Rule 2, what does that do to recall (pairs you should
   have found but didn't) versus precision (pairs you did find)?

7. How would you incorporate a "already recommended, dismissed by
   user" exclusion list so the same non-friend pair isn't
   re-suggested every run?

8. Combine this algorithm with the original Finding Friends example
   to produce a **single** output that, for every pair of people,
   states either "already friends, N mutual friends" or "not
   friends, N mutual friends, recommended." What would the merged
   mapper and reducer look like?

## 17. Comments

Comments and suggestions are welcome!

## 18. References

1. [`MapReduce_Finding_Friends.html`](MapReduce_Finding_Friends.html) —
   the companion example this document extends (mutual friends for
   pairs who are already connected).

2. [Data-Intensive Text Processing with MapReduce by Jimmy Lin and Chris Dyer](https://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf) —
   source of the "pairs vs. stripes" and in-mapper combining patterns
   referenced in Sections 13–14.

3. [A Very Brief Introduction to MapReduce by Diana MacLean](https://hci.stanford.edu/courses/cs448g/a2/files/map_reduce_tutorial.pdf)

4. [Introduction to MapReduce by Mahmoud Parsian](http://mapreduce4hackers.com/docs/Introduction-to-MapReduce.pdf)
