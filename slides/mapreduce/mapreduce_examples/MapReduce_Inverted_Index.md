# MapReduce Example: Inverted Index Construction

	Author: Mahmoud Parsian
	Last updated: 9/20/2026

## 1. Introduction

This article works through a **complete** `map()` / `combine()` /
`reduce()` job, by hand, on real sample data — every mapper call,
every combiner call, the shuffle output before and after combining,
and every reducer call. Unlike Word Count or the store-sales example
in this folder, the reducer here doesn't collapse values down to a
single number; it **groups** them into a structure (a dictionary of
lists) — the shape behind every search engine's inverted index, and
one of the original motivating use cases for MapReduce itself.

This is the fully worked solution to [Question 19 in
`practice_questions/mapreduce_questions.md`](../../practice_questions/mapreduce_questions.md#question-19)
— traced mapper-by-mapper and reducer-by-reducer, with a combiner
added on top.

## 2. Problem

Given a collection of text documents, build an **inverted index**: for
every word that appears anywhere in the collection, list every
document it appears in, together with the word's position(s) within
that document.

```text
word  →  { doc_id: [positions in that doc], ... }
```

This is the data structure a search engine uses to answer "which
documents contain this word, and where" without scanning every
document at query time.

## 3. Input Data Format

```text
<document_id>, <full text of the document>
```

## 4. Sample Dataset

The first three documents below are exactly [Question 19's sample
data](../../practice_questions/mapreduce_questions.md#question-19).
A fourth document is added here, split into its own mapper partition,
so the combiner trace in Section 14 has two genuinely independent
partial indexes to merge:

**Partition A** (Mapper 1's input):

```text
Document1: "fox jumped fast fox fast"
Document2: "fox ran fox jumped fast"
```

**Partition B** (Mapper 2's input):

```text
Document3: "hello hello hello fox"
Document4: "fast fox ran home"
```

Word positions are 1-indexed and reset at the start of each document.

## 5. Is This a Big Data Problem?

Yes — this is one of the original MapReduce use cases. A web-scale
search engine indexes billions of pages; no single machine can hold
the full text of the web in memory at once. But each document can be
indexed **independently** of every other document (indexing
`Document1` never needs to look at `Document2`), which is exactly
what makes this problem split cleanly across many mappers, as the two
partitions above do in miniature.

## 6. Output Data Format

```text
(word, {doc_id: [positions], doc_id: [positions], ...})
```

## 7. Input to Mappers

Each document arrives at a mapper as a `(key, value)` pair where `key`
is the document ID and `value` is the document's full text:

```text
("Document1", "fox jumped fast fox fast")
("Document2", "fox ran fox jumped fast")
...
```

## 8. Mapper

Unlike Word Count's or the store-sales example's mapper — one `emit()`
per call — this mapper is a natural `flatMap`: a single call emits one
`(word, (doc_id, position))` pair for **every** word in the document.

```text
# key: document ID
# value: full text of the document
map(doc_id, text) {
   position = 0
   for (word in text.split(" ")) {
      position += 1
      emit(word, (doc_id, position))
   }
}
```

## 9. Output of Mappers (all 4 calls, 18 pairs)

**Mapper 1** (Partition A):

```text
map("Document1", "fox jumped fast fox fast")
   -> (fox, (Document1,1)), (jumped, (Document1,2)), (fast, (Document1,3)),
      (fox, (Document1,4)), (fast, (Document1,5))

map("Document2", "fox ran fox jumped fast")
   -> (fox, (Document2,1)), (ran, (Document2,2)), (fox, (Document2,3)),
      (jumped, (Document2,4)), (fast, (Document2,5))
```

**Mapper 2** (Partition B):

```text
map("Document3", "hello hello hello fox")
   -> (hello, (Document3,1)), (hello, (Document3,2)), (hello, (Document3,3)),
      (fox, (Document3,4))

map("Document4", "fast fox ran home")
   -> (fast, (Document4,1)), (fox, (Document4,2)), (ran, (Document4,3)),
      (home, (Document4,4))
```

10 pairs from Mapper 1, 8 from Mapper 2 — 18 in total.

## 10. Sort & Shuffle Phase (No Combiner)

Without a combiner, all 18 pairs cross the network individually,
grouped only by word:

```text
("fast",   [(Document1,3), (Document1,5), (Document2,5), (Document4,1)])
("fox",    [(Document1,1), (Document1,4), (Document2,1), (Document2,3), (Document3,4), (Document4,2)])
("hello",  [(Document3,1), (Document3,2), (Document3,3)])
("home",   [(Document4,4)])
("jumped", [(Document1,2), (Document2,4)])
("ran",    [(Document2,2), (Document4,3)])
```

## 11. Reducer

```text
# key: word
# values: Iterable<(doc_id, position)>
reduce(word, values) {
   index = {}
   for ((doc_id, position) in values) {
      if (doc_id not in index) {
         index[doc_id] = []
      }
      index[doc_id].append(position)
   }
   for (doc_id in index) {
      index[doc_id] = sort(index[doc_id])
   }
   emit(word, index)
}
```

## 12. Output of Reducers

```text
("fast",   {Document1: [3, 5], Document2: [5], Document4: [1]})
("fox",    {Document1: [1, 4], Document2: [1, 3], Document3: [4], Document4: [2]})
("hello",  {Document3: [1, 2, 3]})
("home",   {Document4: [4]})
("jumped", {Document1: [2], Document2: [4]})
("ran",    {Document2: [2], Document4: [3]})
```

Compare `fox`, `jumped`, `fast`, `ran`, and `hello` restricted to
`Document1`–`Document3` only against
[Question 19's expected output](../../practice_questions/mapreduce_questions.md#question-19)
— they match exactly.

## 13. Combiner — What's Safe to Combine Here, and What Isn't Automatic

A combiner runs **locally, on one mapper's output only**, before the
shuffle — see [`MapReduce_with_Combiners.md`](../combiners/MapReduce_with_Combiners.md)
for the general theory. Here, a combiner can build a **partial index**
— the same `{doc_id: [positions]}` grouping the reducer builds, just
scoped to one mapper's documents — and later combiner/reducer calls
merge partial indexes together by unioning their keys and
concatenating their position lists.

This is safe for the same underlying reason `sum` and `max` were safe
in [`MapReduce_Total_Sales_per_Store.md`](MapReduce_Total_Sales_per_Store.md):
list concatenation is **associative** — `(a + b) + c == a + (b + c)` —
so it doesn't matter how the partial indexes get grouped back together.

There is one difference worth noticing, though. `sum` and `max` are
commutative on the *values themselves* — `3 + 5 == 5 + 3` — so merge
order never matters, full stop. List concatenation is only
associative, not commutative: `[3, 5] + [1] != [1] + [3, 5]` as
*lists* (same elements, different order). If two combiners' outputs
for `fox` reach the reducer in a different order than the trace below
shows, the intermediate concatenation could come out `[1, 4, 4]` or
`[4, 1, 4]` before sorting — order-dependent. The reducer's explicit
`sort()` call (Section 11) is what makes the *final* answer
order-independent again, regardless of what order the merges happened
in. Contrast this with a true monoid like `sum` under `+`, which never
needs a cleanup step — see [`monoids/`](../monoids/) for the general
distinction.

```text
# key: word
# values: Iterable<(doc_id, position)> (local to ONE mapper only)
combine(word, values) {
   partial_index = {}
   for ((doc_id, position) in values) {
      if (doc_id not in partial_index) {
         partial_index[doc_id] = []
      }
      partial_index[doc_id].append(position)
   }
   emit(word, partial_index)
}
```

## 14. Combiner Trace: Two Mapper Partitions

**Mapper 1's combiner** runs only over Partition A's 10 mapped pairs
(Section 9):

```text
combine("fox",    [(Document1,1), (Document1,4), (Document2,1), (Document2,3)])
   -> ("fox", {Document1: [1, 4], Document2: [1, 3]})

combine("jumped", [(Document1,2), (Document2,4)])
   -> ("jumped", {Document1: [2], Document2: [4]})

combine("fast",   [(Document1,3), (Document1,5), (Document2,5)])
   -> ("fast", {Document1: [3, 5], Document2: [5]})

combine("ran",    [(Document2,2)])
   -> ("ran", {Document2: [2]})
```

**Mapper 2's combiner** runs only over Partition B's 8 mapped pairs,
completely independently of Mapper 1's:

```text
combine("hello", [(Document3,1), (Document3,2), (Document3,3)])
   -> ("hello", {Document3: [1, 2, 3]})

combine("fox",   [(Document3,4), (Document4,2)])
   -> ("fox", {Document3: [4], Document4: [2]})

combine("fast",  [(Document4,1)])
   -> ("fast", {Document4: [1]})

combine("ran",   [(Document4,3)])
   -> ("ran", {Document4: [3]})

combine("home",  [(Document4,4)])
   -> ("home", {Document4: [4]})
```

18 raw pairs went in; 9 combined records come out — the combiner has
already cut what crosses the network in half, *before* the shuffle
even runs.

## 15. Sort & Shuffle Phase (With Combiner)

The shuffle now groups the 9 combined records from Section 14 by
word, instead of the 18 raw pairs from Section 10:

```text
("fast",   [{Document1: [3, 5], Document2: [5]}, {Document4: [1]}])
("fox",    [{Document1: [1, 4], Document2: [1, 3]}, {Document3: [4], Document4: [2]}])
("hello",  [{Document3: [1, 2, 3]}])
("home",   [{Document4: [4]}])
("jumped", [{Document1: [2], Document2: [4]}])
("ran",    [{Document2: [2]}, {Document4: [3]}])
```

## 16. Reducer (With Combiner)

The reducer's shape changes slightly: it now receives partial-index
dictionaries instead of raw `(doc_id, position)` pairs, and merges
them by union-ing keys and concatenating position lists — the same
merge logic the combiner used, applied one level up:

```text
# key: word
# values: Iterable<dict{doc_id -> [positions]}>  (one partial index per mapper)
reduce(word, values) {
   merged = {}
   for (partial in values) {
      for (doc_id in partial) {
         if (doc_id not in merged) {
            merged[doc_id] = []
         }
         merged[doc_id].extend(partial[doc_id])
      }
   }
   for (doc_id in merged) {
      merged[doc_id] = sort(merged[doc_id])
   }
   emit(word, merged)
}
```

## 17. Output of Reducers (With Combiner)

```text
("fast",   {Document1: [3, 5], Document2: [5], Document4: [1]})
("fox",    {Document1: [1, 4], Document2: [1, 3], Document3: [4], Document4: [2]})
("hello",  {Document3: [1, 2, 3]})
("home",   {Document4: [4]})
("jumped", {Document1: [2], Document2: [4]})
("ran",    {Document2: [2], Document4: [3]})
```

**Identical** to Section 12's no-combiner output, for every word —
proof that adding the combiner changed *when* and *how much* data
crossed the network, not the answer.

## 18. Bonus: Document Frequency (df) for TF-IDF

The finished index already has everything needed to compute each
word's **document frequency** — `df(word)`, the number of *distinct*
documents it appears in — for free: it's just the number of keys in
that word's dictionary. `df` is the standard denominator in TF-IDF
(`idf = log(N / df)`, where `N` is the total document count):

```text
word     df   (out of N=4 documents)
fast     3    (Document1, Document2, Document4)
fox      4    (Document1, Document2, Document3, Document4)
hello    1    (Document3)
home     1    (Document4)
jumped   2    (Document1, Document2)
ran      2    (Document2, Document4)
```

`fox` appearing in every document (`df=4`) makes it a poor
search-ranking signal despite its high raw count — exactly the
insight TF-IDF is built to capture, and exactly why real search
engines compute `df` as a direct byproduct of building the index.

## 19. Spark Equivalent (Sketch, Verified)

`combineByKey` again plays the combiner's role directly, this time
merging partial-index dictionaries instead of numeric triples:

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "inverted-index")

docs = sc.parallelize([
    ("Document1", "fox jumped fast fox fast"),
    ("Document2", "fox ran fox jumped fast"),
    ("Document3", "hello hello hello fox"),
    ("Document4", "fast fox ran home"),
])

def emit_word_positions(doc_id, text):
    for pos, word in enumerate(text.split(), start=1):
        yield (word, (doc_id, pos))

word_doc_pos = docs.flatMap(lambda kv: emit_word_positions(kv[0], kv[1]))

def to_partial_index(pairs):
    idx = {}
    for doc_id, pos in pairs:
        idx.setdefault(doc_id, []).append(pos)
    return idx

def merge_indexes(a, b):
    merged = dict(a)
    for doc_id, positions in b.items():
        merged[doc_id] = sorted(merged.get(doc_id, []) + positions)
    return merged

inverted_index = word_doc_pos.combineByKey(
    lambda v: to_partial_index([v]),                       # createCombiner
    lambda acc, v: merge_indexes(acc, to_partial_index([v])),  # mergeValue
    merge_indexes,                                          # mergeCombiners
)

for word, index in sorted(inverted_index.collect()):
    print(word, index)
```

```text
fast {'Document1': [3, 5], 'Document2': [5], 'Document4': [1]}
fox {'Document1': [1, 4], 'Document2': [1, 3], 'Document3': [4], 'Document4': [2]}
hello {'Document3': [1, 2, 3]}
home {'Document4': [4]}
jumped {'Document1': [2], 'Document2': [4]}
ran {'Document2': [2], 'Document4': [3]}
```

Matches this article's numbers exactly, verified against a real
PySpark run.

## 20. Food for Thought

1. Extend the mapper/reducer to also track **term frequency** —
   how many times each word appears in each document — alongside its
   position list. Can this be read off the existing output without
   re-running anything, or does it need a new field carried all the
   way through?

2. Positions here reset to `1` at the start of every document. Rewrite
   the mapper to use a single position counter across the **entire
   corpus** instead. What real feature would want that instead of
   per-document positions, and what does the per-document version make
   easier?

3. Compare this article's combiner (merging dictionaries of lists) to
   `combine()` in [`MapReduce_Total_Sales_per_Store.md`](MapReduce_Total_Sales_per_Store.md)
   (merging numeric triples). Which of associativity, commutativity,
   and idempotency hold for list concatenation, and which one needed
   the explicit `sort()` "fix-up" in Section 13 that the numeric
   version never needed?

4. Real text has punctuation and mixed case ("fox." vs. "Fox" vs.
   "fox"). Where should case-folding and punctuation-stripping happen
   — mapper or reducer — and why? (See "Mapper filter vs. reducer
   filter — why it matters" in [`MapReduce_Word_Count.md`](MapReduce_Word_Count.md)
   for the general rule.)

5. Section 18 computes `df(word)` as a bonus derived from the final
   index. Could `df` instead be computed as a *second*, independent
   MapReduce job that reads the Section 17 output as its input? Sketch
   that job's `map()` and `reduce()`.

## 21. Comments

Comments and suggestions are welcome!

## 22. References

1. [Question 19, `practice_questions/mapreduce_questions.md`](../../practice_questions/mapreduce_questions.md#question-19) — the original problem statement this article is the fully worked solution to
2. [`MapReduce_Word_Count.md`](MapReduce_Word_Count.md) — companion example; mapper-filter vs. reducer-filter discussion referenced in Section 20
3. [`MapReduce_Total_Sales_per_Store.md`](MapReduce_Total_Sales_per_Store.md) — companion example; a scalar `(sum, count, max)` combiner, contrasted with this article's dict-of-lists combiner in Section 13
4. [`combiners/MapReduce_with_Combiners.md`](../combiners/MapReduce_with_Combiners.md) — the general theory of when a combiner is correct
5. [`associativity_and_commutativity/Associativity_Commutativity_and_Reducers.md`](../associativity_and_commutativity/Associativity_Commutativity_and_Reducers.md) — the formal version of Section 13's associativity/commutativity argument
6. [`monoids/`](../monoids/) — why some combinable structures (like `sum`) need no cleanup step, and others (like this article's position lists) do
7. [Data-Intensive Text Processing with MapReduce by Jimmy Lin and Chris Dyer](https://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)
