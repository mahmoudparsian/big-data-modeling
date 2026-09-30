# MapReduce Example: Top 10 Disaster-Relief Hubs by Meals Delivered

	Author: Mahmoud Parsian
	Last updated: 9/30/2026

Which ten relief hubs delivered the most meals during an emergency-response
week? Delivery batches arrive in separate files; a hub can appear in several
files. We must **sum every hub's contributions before ranking hubs**.

This example uses a deliberately constructed, **synthetic teaching dataset**,
not records from a real disaster. Counts represent meals actually delivered,
not donations or requests. Ranking by volume answers a logistics question;
it does not measure need, efficiency, or quality of service.

This walkthrough assumes you already know the basic Map → Combine →
Shuffle & Sort → Reduce mechanics — see
[`MapReduce_Word_Count.md`](../MapReduce_Word_Count.md) first if you need
that groundwork. What's new here is a **second** MapReduce job layered on
top of the first, needed to safely pick a global top 10 out of everything
the first job computed.

## 1. Input and Ranking Contract

The companion [relief_meals.csv](relief_meals.csv) contains 32 batches for
21 hubs across two illustrative mapper partitions. `mapper_partition` makes
the trace reproducible; in production, input splits determine mapper tasks.
`batch_id` uniquely identifies a batch, and each batch occurs once. Replayed
source batches must be deduplicated upstream; summation does not deduplicate.

Ten hubs receive a **second batch** later in the week (a realistic pattern —
relief trucks restock a hub more than once), so their per-partition total
below is a sum of two batches, not one. The rest, plus Central, receive a
single batch:

| Mapper partition | Hub | Batches in this partition | Meals delivered (this partition) |
|---|---|---:|---:|
| 1 | H01 Bayview | 2 | 114,000 |
| 1 | H02 Cedar | 1 | 109,000 |
| 1 | H03 Dunes | 2 | 112,000 |
| 1 | H04 Elm | 1 | 107,000 |
| 1 | H05 Forest | 2 | 110,000 |
| 1 | H06 Grove | 1 | 105,000 |
| 1 | H07 Harbor | 2 | 108,000 |
| 1 | H08 Island | 1 | 103,000 |
| 1 | H09 Juniper | 2 | 106,000 |
| 1 | H10 Kings | 1 | 101,000 |
| 1 | H21 Central | 1 | 60,000 |
| 2 | H11 Lake | 2 | 114,000 |
| 2 | H12 Mesa | 1 | 109,000 |
| 2 | H13 North | 2 | 112,000 |
| 2 | H14 Oak | 1 | 107,000 |
| 2 | H15 Pine | 2 | 110,000 |
| 2 | H16 Quarry | 1 | 105,000 |
| 2 | H17 River | 2 | 108,000 |
| 2 | H18 Summit | 1 | 103,000 |
| 2 | H19 Timber | 2 | 106,000 |
| 2 | H20 Valley | 1 | 101,000 |
| 2 | H21 Central | 1 | 60,000 |

Notice Central's row: 60,000 meals in *each* partition, clearly lower than
its neighbors. That looks unremarkable — until Section 2 shows why it's
actually about to become the global #1 hub.

Return exactly `min(10, number_of_hubs)` rows. Rank by:

1. Total meals descending.
2. Hub ID ascending to break ties deterministically.

IDs have the fixed-width form `H01` through `H21`; numeric and lexical
ID ordering agree. The 10th and 11th hubs tie on meals, so the tie rule
matters. Returning *all* hubs tied at 10th place is a different contract.
Reject malformed IDs and negative or noninteger counts before processing.
Use a sufficiently wide integer type for sums. Python integers do not overflow.

## 2. The Trap: Mapper Top 10 Before Summation

Central ranks 11th in **both** input partitions. An algorithm that keeps only
the ten largest locally completed hub totals drops both Central records. Yet
its total is `60,000 + 60,000 = 120,000`, which makes it the global winner!

Local top-k pruning is safe only when candidates already carry their **final
scores**. It is unsafe for partial sums, including mapper-local sums for hubs
that may occur elsewhere.

## 3. Two-Job Algorithm

| Stage | Input → output | Purpose |
|---|---|---|
| Job 1 map | batch → `(hub_id, meals_delivered)` | Extract contributions |
| Job 1 optional combine | `(hub_id, counts)` → `(hub_id, sum(counts))` | Reduce local traffic without dropping hubs |
| Job 1 shuffle | Group all contributions by hub ID | Route each hub to exactly one reducer |
| Job 1 reduce | `(hub_id, counts)` → completed total, retained in a reducer-wide heap | Aggregate first, then retain local top 10 |
| Job 1 reducer cleanup | heap → up to 10 `(hub_id, total)` records | Emit candidates after all keys have been processed |
| Job 2 map | candidate → `("TOP10", (hub_id, total))` | Send candidates to one final group |
| Job 2 reduce | `("TOP10", candidates)` → ranked top 10 | Merge using the same ranking rule |

The sum combiner is optional: correctness holds if it never runs, runs on
partial groups, or runs repeatedly. Its input/output types match, and integer
addition is associative and commutative. A **top-10 combiner on raw counts is
not safe**.

Reducer lifecycle matters here: the heap must persist across *every* hub key
that reducer handles, not reset for each one — Section 6.2 explains the
Hadoop API hooks (`setup()` / `reduce()` / `cleanup()`) that make this
possible. Concretely: create the heap once when the reducer starts, update
it after each key's sum completes, and emit its contents only once, after
the last key. Creating a *new* heap per key would only ever compare a hub
against itself, never against the other hubs on that reducer — so it could
never select a top 10. A framework without this kind of reducer-wide state
can instead write out all completed totals and add a separate
local-selection pass afterward.

## 4. Job 1: Map, Combine, Shuffle, and Complete Totals

### 4.1 Full Mapper Output

The CSV header is metadata, not an input batch. Each mapper emits
`(hub_id, meals_delivered)` for every batch in its partition, in the order
batches are read. Batch IDs and hub names are not included in these
intermediate records.

**Mapper 1 — 16 emitted records:**

```text
(H01, 110000)
(H02, 109000)
(H03, 108000)
(H04, 107000)
(H05, 106000)
(H06, 105000)
(H07, 104000)
(H08, 103000)
(H09, 102000)
(H10, 101000)
(H21, 60000)
(H01, 4000)
(H03, 4000)
(H05, 4000)
(H07, 4000)
(H09, 4000)
```

**Mapper 2 — 16 emitted records:**

```text
(H11, 110000)
(H12, 109000)
(H13, 108000)
(H14, 107000)
(H15, 106000)
(H16, 105000)
(H17, 104000)
(H18, 103000)
(H19, 102000)
(H20, 101000)
(H21, 60000)
(H11, 4000)
(H13, 4000)
(H15, 4000)
(H17, 4000)
(H19, 4000)
```

There are 32 mapper output records in total. Five hubs repeat **within**
Mapper 1's own output (H01, H03, H05, H07, H09), and five repeat within
Mapper 2's own output (H11, H13, H15, H17, H19) — each from that hub's second
batch. Unlike the smaller, one-batch-per-hub-per-mapper version of this
dataset, the combiner now has real duplicate keys to collapse before the
shuffle.

### 4.2 Combine Phase

The Combiner runs per mapper, on that mapper's own output only — it cannot
see or merge records emitted by the other mapper. It sums same-key values
locally, using the same associative, commutative sum as the Reducer:

```text
function COMBINE(hub_id, list_of_meals):
    emit(hub_id, sum(list_of_meals))
```

**Mapper 1 combined output — 16 records collapse to 11 keys:**

```text
(H01, 114000)
(H02, 109000)
(H03, 112000)
(H04, 107000)
(H05, 110000)
(H06, 105000)
(H07, 108000)
(H08, 103000)
(H09, 106000)
(H10, 101000)
(H21, 60000)
```

**Mapper 2 combined output — 16 records collapse to 11 keys:**

```text
(H11, 114000)
(H12, 109000)
(H13, 112000)
(H14, 107000)
(H15, 110000)
(H16, 105000)
(H17, 108000)
(H18, 103000)
(H19, 106000)
(H20, 101000)
(H21, 60000)
```

H21 still shows up as a single, un-collapsed entry in each mapper's combined
output, because its two batches were read by two *different* mappers — a
combiner only ever sees its own mapper's records, never another mapper's.
Only the Reduce step, after shuffle has grouped both `60000` values under
the same key, can add them together into `120000`.

This is exactly why the trap in Section 2 is dangerous: if you ranked and
pruned each mapper's output down to a "top 10" *before* that reduce-time
merge happens, you would be ranking Central's two halves — 60,000 and
60,000 — separately, and each half alone looks weak enough to discard.

### 4.3 Full Sort and Shuffle Output

For this trace, the partitioner sends a hub to reducer
`numeric_hub_id % 3`. (This is an unrelated use of the word "partition" from
the CSV's `mapper_partition` column: that column says which *mapper* read a
batch; this partitioner decides which *reducer* a key is routed to.) The
shuffle brings together the (already-combined) contributions from all
mappers, and sort orders the hub keys within each reducer. Each entry below
is one complete `(hub_id, list_of_counts)` group passed to a reduce call.
Values are shown as lists for readability; reducers can stream them. Value
order is not guaranteed, and these three reducer inputs do not form one
globally sorted stream.

**Reducer R0 — 7 grouped keys, sorted by hub ID:**

```text
(H03, [112000])
(H06, [105000])
(H09, [106000])
(H12, [109000])
(H15, [110000])
(H18, [103000])
(H21, [60000, 60000])
```

**Reducer R1 — 7 grouped keys, sorted by hub ID:**

```text
(H01, [114000])
(H04, [107000])
(H07, [108000])
(H10, [101000])
(H13, [112000])
(H16, [105000])
(H19, [106000])
```

**Reducer R2 — 7 grouped keys, sorted by hub ID:**

```text
(H02, [109000])
(H05, [110000])
(H08, [103000])
(H11, [114000])
(H14, [107000])
(H17, [108000])
(H20, [101000])
```

Each mapper's 16 raw records combined down to 11 keys (Section 4.2), so
11 + 11 = 22 values move into the shuffle. Those 22 values land in 21
groups: 20 hubs supply exactly one value each, and H21 supplies two — the
same two values Section 4.2 could not combine, since they came from
different mappers. Sort and shuffle only *group* those values; they do not
sum them, which is why R0 above still shows `[60000, 60000]` instead of
`120000`.

### 4.4 Reducer Totals and Local Selection

Each reduce call sums its complete value group. For example:

```text
reduce(H01, [114000])       → completed total (H01, 114000)
reduce(H11, [114000])       → completed total (H11, 114000)
reduce(H21, [60000, 60000]) → completed total (H21, 120000)
```

The following table shows all completed totals in ranking order within each
reducer, after aggregation. This score order differs from the hub-key order
of the sort and shuffle input above.

| Reducer | Completed totals in ranking order |
|---|---|
| R0 | H21:120000, H03:112000, H15:110000, H12:109000, H09:106000, H06:105000, H18:103000 |
| R1 | H01:114000, H13:112000, H07:108000, H04:107000, H19:106000, H16:105000, H10:101000 |
| R2 | H11:114000, H05:110000, H02:109000, H17:108000, H14:107000, H08:103000, H20:101000 |

Each reducer has only seven hubs in this small trace, so all 21 totals survive
local selection. That is expected: top-k pruning reduces traffic when a
reducer has more than k completed totals. With one reducer, the same dataset
already prunes 21 totals to 10. The algorithm works for any positive reducer
count; the simulation checks both cases.

## 5. Bounded Heap Selection

Maintain a min-heap of at most `k=10` candidates, with the **worst retained
candidate at its root** — that's what a min-heap naturally keeps at the top,
and it's exactly the entry we want to be able to evict cheaply when a better
candidate arrives. A higher score is better; at equal scores a smaller ID is
better.

Python's `heapq` (and most min-heaps) can only compare plain tuples, and
always treats the *smallest* tuple as the root. A larger `total` should be
"better" — that already sorts the right way (bigger total → bigger tuple →
not the root). But a *smaller* ID should be "better," which sorts
*backwards*, so we negate it: `(total, -numeric_id)`. For example, H07 and
H17 both have 108,000 meals, and H07 should win the tie: sure enough,
`(108000, -7)` is a larger tuple than `(108000, -17)`, so H17 — not H07 —
ends up as the smaller, evictable tuple at the root.

```text
offer(hub, total):
    if heap has fewer than k entries:
        insert candidate
    else if candidate is better than heap root:
        replace root with candidate
    else:
        discard candidate
```

The final reducer uses this same operation on all incoming candidates. Sort
its retained heap by the ranking contract before emitting rows; heap array
order itself is not ranked order. MapReduce's shuffle sorts **keys**, not
values by score, and separate reducer files are not globally ranked.

## 6. Job 2: Mapper Lifecycle, TreeMap, and Final Output

A **TreeMap** is Java's sorted dictionary: it stores key–value pairs and
keeps the keys in order as entries are added or removed. Think of a list of
labeled cards that automatically stays sorted. You can quickly find, inspect,
or remove the first or last entry, and walk through the entries in order.
A comparison rule determines that order. Each key has one value, so inserting
an existing key replaces its value.

For this top-10 algorithm, we order candidates from worst to best —
deliberately worst-first, so that the entry we want to evict is always the
one `pollFirstEntry()` removes. TreeMap makes removing the *first* entry
cheap, so we simply define "first" to mean "worst." After adding a
candidate, if there are eleven entries, we remove the first (worst) one.
That leaves the ten best candidates seen so far. TreeMap does not impose the
ten-entry limit itself; our code does. Including both the meal total and hub
ID in the key lets hubs with equal totals remain separate entries.

### 6.1 Why the Current Mapper Is Simple

In the current design, Job 1 already retains each reducer's top 10 completed
hub totals. Job 2's mapper simply emits each candidate under the constant key
`TOP10`. The final reducer maintains a bounded heap and emits the ranked
winners. This is sufficient for correctness.

A Job 2 mapper can also select its own top 10 before emitting. This additional
pruning is safe because every input candidate already has a **completed total**.
It may reduce the shuffle further when an input split contains more than ten
candidates. Job 2 input splits need not correspond to Job 1 reducer files.

### 6.2 Stateful Mapper: setup(), map(), cleanup()

In Hadoop's Java Mapper API, the initialization hook is named `setup()`,
rather than `initialize()`. The framework calls `setup()` once at the start
of each mapper task, `map()` for each input record, and `cleanup()` once at
the end. This state belongs to one mapper task, not to the whole job.
See the [official Hadoop Mapper lifecycle documentation](https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/Mapper.html).

For mapper-side selection, keep one bounded TreeMap across all calls:

```text
Job2Mapper.setup():
    best = empty TreeMap ordered worst-to-best

Job2Mapper.map(hub_id, completed_total):
    best.put((completed_total, hub_id), candidate)
    if best.size() > 10:
        best.pollFirstEntry()  # remove the worst retained candidate

Job2Mapper.cleanup():
    for candidate in best.values():
        emit("TOP10", candidate)
```

Do not reinitialize the TreeMap inside `map()`: it must remember candidates
from earlier rows. Emit only the retained candidates in `cleanup()`, at most
ten per mapper. Copy parsed values into immutable candidate records when
retaining them; Hadoop input objects may be reused between calls.

All mapper outputs share `TOP10`, so one final reducer receives them:

```text
Job2Reducer.setup():
    best = empty TreeMap ordered worst-to-best

Job2Reducer.reduce("TOP10", candidates):
    for candidate in candidates:
        best.put((candidate.total, candidate.hub_id), candidate)
        if best.size() > 10:
            best.pollFirstEntry()

Job2Reducer.cleanup():
    for candidate in best.descendingMap().values():
        emit(candidate.hub_id, candidate.total)
```

Configure Job 2 with one reducer for a single ranked output file. The constant
key makes all candidates one reduce group; the TreeMap determines their score
order, independently of shuffle value order. With empty input, the result is
empty; with fewer than ten hubs, emit every available hub.

### 6.3 TreeMap Keys and Tie Handling

A bounded TreeMap and a bounded min-heap both implement exact top-k selection
with `O(k)` retained memory and `O(log k)` updates. A TreeMap additionally
supports ordered traversal; a heap requires sorting its retained winners.

Use a composite key `(total, hub_id)` with this **worst-first** comparator:

1. Compare totals ascending: lower total comes first.
2. At equal totals, compare hub IDs descending: larger ID comes first.

For fixed-width IDs such as H01 and H21, a Java comparator can be written as:

```java
// Candidate exposes long total() and String hubId().
Comparator<Candidate> worstFirst =
    Comparator.comparingLong(Candidate::total)
              .thenComparing(Candidate::hubId, Comparator.reverseOrder());
TreeMap<Candidate, Candidate> best = new TreeMap<>(worstFirst);
```

This implements the composite key without requiring a separate key class.
Use immutable Candidate objects. Comparator methods avoid subtraction-based
comparisons that could overflow. `pollFirstEntry()` removes the worst;
`descendingMap()` traverses best-to-worst, matching our ranking contract.

**Do not use `TreeMap<Long, Hub>` keyed only by total.** Equal totals would
share a key and overwrite one another. For example, H01 and H11 both have
114,000 meals and must both survive. At the boundary, H07 and H17 both have
108,000 meals: H07 wins because its ID is smaller. Both fields must participate
in the comparator, so distinct hubs with equal totals remain distinct entries.

### 6.4 Two Valid Places for Local Selection

| Design | Job 1 output | Job 2 mapper | Final reducer |
|---|---|---|---|
| Current walkthrough and Python simulation | Each reducer's top 10 completed totals | Emit every candidate under TOP10 | Select global top 10 |
| Mapper-selection alternative | All completed hub totals | Keep top 10 across the split; emit in cleanup | Select global top 10 |
| Both optimizations | Each reducer's top 10 completed totals | Keep top 10 across the split; emit in cleanup | Select global top 10 |

The alternative lets Job 1 focus exclusively on aggregation and puts local
ranking in Job 2's mapper. Both optimizations can also be combined. Neither
requires top-k selection on partial sums or raw batches.

For the alternative, Job 1 writes U completed totals, and M Job 2 mappers
emit at most `min(U, M*k)` candidates in total. For the current design,
Job 1 already emits at most `min(U, R*k)` candidates. Additional Job 2 mapper
pruning cannot increase that count. Each stateful mapper uses `O(k)` retained
memory. The companion Python simulation implements the current heap-based
design; the TreeMap lifecycle above describes a Java implementation option.

### 6.5 Final Output

All 21 candidates in this trace share `TOP10`, so one reducer sees them all.
It selects and sorts these ten:

| Rank | Hub ID | Hub | Total meals |
|---|---|---|---:|
| 1 | H21 | Central | 120,000 |
| 2 | H01 | Bayview | 114,000 |
| 3 | H11 | Lake | 114,000 |
| 4 | H03 | Dunes | 112,000 |
| 5 | H13 | North | 112,000 |
| 6 | H05 | Forest | 110,000 |
| 7 | H15 | Pine | 110,000 |
| 8 | H02 | Cedar | 109,000 |
| 9 | H12 | Mesa | 109,000 |
| 10 | H07 | Harbor | 108,000 |

H17 River also has 108,000 meals but loses the boundary tie to H07.
Central must never be discarded before its two contributions are summed.

## 7. Why Local Selection Is Exact

In plain terms: if a hub didn't make its reducer's local top 10, that's only
because ten *other* hubs on that same reducer already scored higher (or tied
with a smaller ID). Nothing happening on any other reducer can ever push
those ten back down — so the excluded hub could never have broken into the
global top 10, no matter how every other reducer's hubs scored.

More formally: after summation, each hub belongs to exactly one reducer and
has one final score. If a hub is absent from that reducer's local top k, at
least k hubs in that same reducer rank ahead of it under the complete
tie-breaking order. Those k hubs also rank ahead of it globally. Therefore
the omitted hub cannot be in the global top k.

Consequently:

```text
TopK(all completed totals)
    = TopK(union of each reducer's TopK(completed totals))
```

The same argument applies to Job 2 mapper splits: a completed candidate
excluded from a mapper's top k has k candidates ahead of it in that split,
so cannot belong to the global top k. Applying local selection again to an
already-pruned candidate set is therefore safe.

This proof also allows a hierarchy of candidate-merging jobs when the final
candidate set is very large. Every merge must preserve the same ranking rule.
It assumes unique completed records per hub and correctly committed task
outputs; accidental duplicate candidates must not consume multiple slots.

## 8. Cost and Scaling

Let N be the batch count, U the distinct hub count, and R the number of Job 1
reducers. Job 1 still processes all N contributions; candidate pruning does
not remove the initial aggregation shuffle. A sum combiner can reduce it.

Selection across U completed totals costs `O(U log k)` time and `O(k)` heap
memory **per reducer**, beyond framework buffers and aggregation state. Sum
can stream each key's values. Job 1 emits at most `min(U, R*k)` candidates;
Job 2 processes at most R*k records in `O(R*k log k)` time with `O(k)` heap
memory, then sorts at most k winners in `O(k log k)` time.

The final single reducer is practical for small k and moderate R; use a
merge tree if R*k becomes large. A hub with many batches can make Job 1 skewed.
Local sum combining or an additional salted partial-sum stage can help, but
all partial totals must be reunited before ranking.

## 9. Run and Check

The companion [top10_relief_meals.py](top10_relief_meals.py) is a pure-Python
in-memory simulation of the two jobs, not a distributed runtime. Its grouping
lists model shuffle data; production frameworks spill and stream that data.
The selection helper uses a bounded heap in both stages.

From this directory, run:

```sh
python3 top10_relief_meals.py
```

It prints the ranked table above and checks results against an independent
full-sort baseline, with and without combining, reversed input order, five
reducer counts, empty input, fewer than k hubs, and deterministic ties. It
also demonstrates that premature mapper pruning drops the winner.

## 10. Practice Questions

1. Change k to 3. Which hubs survive globally?
2. Return all hubs tied on the 10th score. Why can a fixed-size heap miss some?
3. Rank by average meals per batch. Why must sum and count be combined before
   computing and ranking averages?
4. Find ten hubs per region. What grouping key should Job 2 use, and when must
   each hub's regional total be complete?

**Answer checks:** (1) H21, H01, H11. (2) There are 11 winners here; a fixed-size
local heap may discard boundary ties. Determine the global 10th score, then
filter all completed totals at or above that threshold in another pass.
(3) Partial averages cannot be summed or ranked safely; merge `(sum, count)`
first. (4) Aggregate by `(region, hub_id)`, select local top k per region only
after those totals are complete, then merge candidates under each region key.

Related: [Total Sales per Store](../MapReduce_Total_Sales_per_Store.md) explains
sum aggregation; [Distinct Visitors per Day](../MapReduce_Distinct_Visitors_per_Day.md)
explains why changing grouping keys requires separate jobs.

## 11. Small Working Example: Top 3 from 10 Input Records

This separate synthetic example uses **exactly ten delivery batches**, six
hubs, and `k=3`. It uses smaller meal counts so every step can be checked by
hand. It does not change `relief_meals.csv`. Rank completed totals descending,
then hub IDs ascending, just as in the main example.

### 11.1 Input

| Batch | Mapper partition | Hub ID | Hub | Meals delivered |
|---|---:|---|---|---:|
| S01 | 1 | H21 | Central | 40 |
| S02 | 1 | H01 | Bayview | 70 |
| S03 | 1 | H02 | Cedar | 65 |
| S04 | 1 | H03 | Dunes | 60 |
| S05 | 1 | H04 | Elm | 10 |
| S06 | 2 | H21 | Central | 40 |
| S07 | 2 | H01 | Bayview | 5 |
| S08 | 2 | H02 | Cedar | 5 |
| S09 | 2 | H03 | Dunes | 5 |
| S10 | 2 | H05 | Forest | 55 |

### 11.2 Job 1: Full Mapper Output

```text
Mapper 1:
(H21, 40)
(H01, 70)
(H02, 65)
(H03, 60)
(H04, 10)

Mapper 2:
(H21, 40)
(H01, 5)
(H02, 5)
(H03, 5)
(H05, 55)
```

These are all ten emitted records. A sum combiner leaves them unchanged,
since each hub appears at most once in each partition.

### 11.3 Job 1: Full Sort and Shuffle Output

Use **two reducers** for this example, with partition rule
`numeric_hub_id % 2`. Keys are sorted within each reducer:

```text
Reducer R0:
(H02, [65, 5])
(H04, [10])

Reducer R1:
(H01, [70, 5])
(H03, [60, 5])
(H05, [55])
(H21, [40, 40])
```

The shuffle has grouped ten values under six hub keys. Values within a group
may arrive in another order; the sum remains the same.

### 11.4 Job 1: Completed Totals and Local Top 3

Each reducer sums its groups and keeps at most three completed candidates.
The retained set below is displayed best-first, regardless of whether the
implementation uses a heap or a TreeMap.

| Reducer | Key processed | Completed total | Retained candidates after this key |
|---|---|---:|---|
| R0 | H02 | 70 | H02:70 |
| R0 | H04 | 10 | H02:70, H04:10 |
| R1 | H01 | 75 | H01:75 |
| R1 | H03 | 65 | H01:75, H03:65 |
| R1 | H05 | 55 | H01:75, H03:65, H05:55 |
| R1 | H21 | 80 | H21:80, H01:75, H03:65; discard H05:55 |

In cleanup, the reducers emit five candidates:

```text
R0 cleanup:
(H02, 70)
(H04, 10)

R1 cleanup:
(H21, 80)
(H01, 75)
(H03, 65)
```

R0 has fewer than three hubs, so it emits both. R1 discards Forest only after
all of Forest's meals have been counted. Central's two batches produce the
highest total: `40 + 40 = 80`.

### 11.5 Job 2: Map and Shuffle

Following the current implementation, Job 2 maps all five candidates to one
constant key:

```text
("TOP3", (H02, 70))
("TOP3", (H04, 10))
("TOP3", (H21, 80))
("TOP3", (H01, 75))
("TOP3", (H03, 65))
```

The full grouped input to the final reducer is:

```text
("TOP3", [(H02, 70), (H04, 10), (H21, 80), (H01, 75), (H03, 65)])
```

This is one possible value order, not a shuffle ordering guarantee. Using
`TOP3` instead of `TOP10` simply makes the label match this example; either
constant works. A mapper-side TreeMap as described in Section 6 is also safe
here, but the simple mapper already gives the correct result.

### 11.6 Job 2: Retain Three Winners

For the value order above, selection proceeds as follows. Again, retained
candidates are displayed best-first, not in heap array order.

| Incoming candidate | Retained candidates | Action |
|---|---|---|
| H02:70 | H02:70 | Insert |
| H04:10 | H02:70, H04:10 | Insert |
| H21:80 | H21:80, H02:70, H04:10 | Insert |
| H01:75 | H21:80, H01:75, H02:70 | Remove H04:10 |
| H03:65 | H21:80, H01:75, H02:70 | Discard H03:65 |

The final ranked output is:

| Rank | Hub ID | Hub | Total meals |
|---|---|---|---:|
| 1 | H21 | Central | 80 |
| 2 | H01 | Bayview | 75 |
| 3 | H02 | Cedar | 70 |

### 11.7 Run This Example

Save the following as `top3_demo.py` next to `top10_relief_meals.py`, then run
`python3 top3_demo.py`. It imports the existing two-job simulation and supplies
these ten rows directly, so no extra CSV file or packages are needed. The
simulation's constant grouping key is implicit; changing k determines the
number of retained winners.

```python
import csv
from io import StringIO
from top10_relief_meals import run

DATA = """batch_id,mapper_partition,hub_id,hub_name,meals_delivered
S01,1,H21,Central,40
S02,1,H01,Bayview,70
S03,1,H02,Cedar,65
S04,1,H03,Dunes,60
S05,1,H04,Elm,10
S06,2,H21,Central,40
S07,2,H01,Bayview,5
S08,2,H02,Cedar,5
S09,2,H03,Dunes,5
S10,2,H05,Forest,55
"""
rows = list(csv.DictReader(StringIO(DATA)))
assert len(rows) == 10
expected = [('H21', 80), ('H01', 75), ('H02', 70)]
for combine in (False, True):
    assert run(rows, k=3, reducers=2, combine=combine) == expected
    assert run(list(reversed(rows)), k=3, reducers=2,
               combine=combine) == expected

names = {row['hub_id']: row['hub_name'] for row in rows}
for rank, (hub, total) in enumerate(run(rows, k=3, reducers=2), 1):
    print(f'{rank}. {hub} {names[hub]} {total}')
print('All top-3 checks passed.')
```

Expected output:

```text
1. H21 Central 80
2. H01 Bayview 75
3. H02 Cedar 70
All top-3 checks passed.
```
