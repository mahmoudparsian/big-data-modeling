# MapReduce

	Author: Mahmoud Parsian
	Last updated: 8/19/2026

Materials for teaching the MapReduce paradigm: the original papers,
the theory behind correct/parallelizable reducers, worked examples,
third-party course materials, and — for contrast only — a handful of
classic Hadoop Java programs.

## Folder Structure

```text
mapreduce/
├── README.md                          # this file
│
├── google_mapreduce_paper/             # the original Google MapReduce papers
├── pros_and_cons_of_mapreduce/         # formal map()/reduce() definition, benefits & drawbacks
├── associativity_and_commutativity/    # the algebra behind correct, parallelizable reducers
├── monoids/                            # monoids as a design principle for reducers/combiners
├── partitions_in_mapreduce/            # partitioning, data locality, executors & cores
├── distributed_file_system/            # HDFS/Bigtable background reading
│
├── mapreduce_by_Mahmoud_Parsian/       # Mahmoud's own 00-11 slide-deck series
├── mapreduce_documents/                # curated reading list (6 papers/decks)
├── mapreduce_by_Jimmy_Lin/             # UMD/Waterloo course materials by Jimmy Lin
├── mapreduce_with_pyspark/             # comprehensive intro/tutorial + worked-examples catalog, both in PySpark
│
├── word_count_in_python/               # plain-Python baseline (no MapReduce, no Spark)
├── word_count_in_mapreduce/            # same problem, solved as mapper()/reducer() -- bridges to PySpark
├── mapreduce_examples/                 # Word Count, Palindromes, Average Temperature, ...
├── combiners/                          # combiner-focused worked examples
├── joins_in_mapreduce/                 # reduce-side join, worked end to end, generalized to INNER/LEFT/RIGHT OUTER
│
└── classic_mapreduce_progs/            # real Hadoop Java programs — kept only for contrast with Spark
```

## Folders

| Folder | Description |
|---|---|
| [`google_mapreduce_paper/`](./google_mapreduce_paper/) | The primary sources: Dean & Ghemawat's *"MapReduce: Simplified Data Processing on Large Clusters"* (Google's original paper), plus Ralf Lämmel's *"Google's MapReduce Programming Model — Revisited."* |
| [`pros_and_cons_of_mapreduce/`](./pros_and_cons_of_mapreduce/pros_and_cons_of_mapreduce.md) | A formal `map()`/`reduce()` definition, followed by MapReduce's benefits (scalability, fault tolerance, ...) and drawbacks. |
| [`associativity_and_commutativity/`](./associativity_and_commutativity/) | *"Associativity, Commutativity, and Reducers"* — a practical algebra of MapReduce reducers: why some reduce functions parallelize for free, why others quietly return wrong answers, and a "lifting recipe" for repairing the ones that don't. |
| [`monoids/`](./monoids/) | *"Monoid: A Design Principle for Correct and Efficient Reducers"* — the algebraic hierarchy (magma → semigroup → monoid), why "average of averages" isn't the average, and how this connects to Spark's `reduceByKey()`. |
| [`partitions_in_mapreduce/`](./partitions_in_mapreduce/) | *"Partitions and Executors in MapReduce"* — data parallelism, two kinds of partitioning, data locality, and how partitions map onto Spark executors/cores in practice. |
| [`distributed_file_system/`](./distributed_file_system/) | Background reading on the storage layer beneath MapReduce/Hadoop: Google's Bigtable paper, and a distributed file systems seminar deck. |
| [`mapreduce_documents/`](./mapreduce_documents/) | A curated reading list of 6 third-party papers/slide decks on MapReduce fundamentals (Ullman, MacLean, Zaharia, Freire, Anjum, Jermaine), pared down from 11 — see its own README for the full breakdown  |
| [`mapreduce_by_Mahmoud_Parsian/`](./mapreduce_by_Mahmoud_Parsian/) | Mahmoud's own numbered slide-deck series (00–11): parallelism fundamentals → the MapReduce model → Word Count → filters → HDFS → combiners, with a matched without/with-combiner example pair. See its own README for the full sequence. |
| [`mapreduce_by_Jimmy_Lin/`](./mapreduce_by_Jimmy_Lin/) | Course materials from Jimmy Lin (University of Maryland / Waterloo): intro session slides, the *"Data-Intensive Text Processing with MapReduce"* book manuscript (two draft years), the WWW 2013 *"MapReduce Algorithm Design"* tutorial, and a Big Data Infrastructure course deck. |
| [`mapreduce_with_pyspark/`](./mapreduce_with_pyspark/) | Two write-ups: `mapreduce_with_pyspark_intro.md`, a comprehensive tutorial covering NYU Center for Data Science's three-part "BigData with PySpark" lesson (Big Data intro, MapReduce primer, intro to Spark) and beyond — modernized Python 3 code, lazy evaluation, narrow/wide transformations, DataFrames/Spark SQL, caching, a Top-N worked example, pitfalls, glossary; and `mapreduce_with_pyspark_examples.md`, a larger catalog of simple → intermediate → complex MapReduce-style problems solved directly in PySpark. |
| [`word_count_in_python/`](./word_count_in_python/) | Word count in **plain Python** — no Hadoop, no Spark — the "before MapReduce" baseline students see first. |
| [`word_count_in_mapreduce/`](./word_count_in_mapreduce/) | The same word-count problem solved as a real MapReduce job: `(filename, record)` pairs → a Python `mapper()` → shuffle/sort → a Python `reducer()`, with every mapper/reducer call shown in full and a preview of the PySpark port (`flatMap`/`reduceByKey`). |
| [`mapreduce_examples/`](./mapreduce_examples/) | Fully worked MapReduce examples: Word Count, Palindromes, and Average Temperature per City, each with mapper/reducer pseudocode, sample data, and homework questions; also includes a third-party "Finding Friends" article. |
| [`combiners/`](./combiners/) | Combiner-focused worked examples: Word Count with/without a combiner (partition-by-partition trace), plus average and `(avg, min, max)` per gene, showing why a naive combiner breaks and how to fix it with `(sum, count)`-style partial results. See its own README. |
| [`joins_in_mapreduce/`](./joins_in_mapreduce/) | *"Join Operation in Action using MapReduce"* — a bank customers/transactions dataset joined end to end via the reduce-side tag/group/split/match recipe (every mapper and reducer call traced by hand), then generalized into INNER/LEFT OUTER/RIGHT OUTER join algorithms and bridged to PySpark's `join()` on RDDs and DataFrames. |
| [`classic_mapreduce_progs/`](./classic_mapreduce_progs/) | Real, compilable Hadoop MapReduce programs in Java (Word Count, Top Movie, Log Handler, Telecom CDR analytics) plus a Hadoop install guide. Kept **only** so students can appreciate the elegance of Spark/PySpark by contrast — students will not write any Hadoop programs. See its own README. |

## Suggested path through this material

1. **Read the source**: `google_mapreduce_paper/` — what Google actually built and why.
2. **Learn the model**: `introduction_to_mapreduce/`, `mapreduce_by_Mahmoud_Parsian/`, or `mapreduce_by_Jimmy_Lin/` — pick one full treatment.
3. **Learn the theory that keeps it correct**: `associativity_and_commutativity/` and `monoids/` — why some reducers/combiners are safe and others silently lie; `partitions_in_mapreduce/` for how the work is actually distributed.
4. **See it worked out**: `word_count_in_python/` → `word_count_in_mapreduce/` → `mapreduce_examples/` → `combiners/` → `joins_in_mapreduce/` → `mapreduce_with_pyspark/`.
5. **Appreciate what Spark replaced**: `classic_mapreduce_progs/` — read-only, for contrast.
