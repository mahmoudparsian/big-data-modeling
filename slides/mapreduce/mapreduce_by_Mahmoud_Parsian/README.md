# MapReduce Slide Deck Series — by Mahmoud Parsian

	Author: Mahmoud Parsian
	Last updated: 8/19/2026

A numbered sequence of slide decks teaching the MapReduce paradigm
from first principles (parallelism) through a complete, comparable
worked example (with and without a combiner). Files are numbered so
they can be read/presented in order.

## Files

| # | File | Format | Slides | Description |
|---|---|---|---|---|
| 00 | [`00_Understanding_MapReduce_by_Databricks.pdf`](./00_Understanding_MapReduce_by_Databricks.pdf) | PDF | 1 pg | *External reference, not authored by Mahmoud Parsian.* A short Databricks glossary page: what MapReduce is and a brief history (Dean & Ghemawat, Google, 2004). Useful as a one-page warm-up before the deck series below. |
| 01 | [`01_understanding_parallelism.pptx`](./01_understanding_parallelism.pptx) | PPTX | 28 | Informal introduction to parallelism & concurrency — the foundation MapReduce, Hadoop, Spark, and Snowflake are all built on (partitioning data + executing in parallel). |
| 02 | [`02_understanding_parallelism_birthday_party.pptx`](./02_understanding_parallelism_birthday_party.pptx) | PPTX | 22 | A second, more intuitive pass at the same parallelism & concurrency concepts, built around a birthday-party analogy — good for building intuition before the formal treatment. |
| 03 | [`03_introduction_to_mapreduce.pptx`](./03_introduction_to_mapreduce.pptx) | PPTX | 66 | The core, in-depth introduction to the MapReduce programming model: partitioning data into chunks, and executing map/reduce tasks in parallel. The main deck in the series. |
| 04 | [`04_Introduction_to_MapReduce_highlevel.pptx`](./04_Introduction_to_MapReduce_highlevel.pptx) | PPTX | 32 | A shorter, higher-level version of the MapReduce introduction (originally taught for CS512, Spring 2014) — a faster alternative to deck 03 when time is limited. |
| 05 | [`05_word_count_in_python.pptx`](./05_word_count_in_python.pptx) | PPTX | 13 | Warm-up: solves the classic Word Count problem in plain Python first, so the MapReduce version (deck 06) has a familiar baseline to compare against. |
| 06 | [`06_word_count_in_mapreduce.pptx`](./06_word_count_in_mapreduce.pptx) | PPTX | 68 | The full Word Count walkthrough in MapReduce: problem statement, MapReduce vs. Hadoop/GFS/HDFS, fundamentals, example code, and job workflows. |
| 07 | [`07_filters_in_mapreduce.pptx`](./07_filters_in_mapreduce.pptx) | PPTX | 37 | How to apply filters inside a MapReduce job — at the mapper, at the reducer, and the tradeoffs between the two. |
| 08 | [`08_Introduction_to_HDFS.pptx`](./08_Introduction_to_HDFS.pptx) | PPTX | 44 | Introduction to the Hadoop Distributed File System (HDFS) — the storage layer MapReduce jobs typically read from and write to. |
| 09 | [`09_combiners_in_mapreduce.pptx`](./09_combiners_in_mapreduce.pptx) | PPTX | 34 | Introduces the optional `combine()` function: the components of a MapReduce job, and why/when a combiner helps. |
| 10 | [`10_mapreduce_without_combiners.pptx`](./10_mapreduce_without_combiners.pptx) | PPTX | 20 | Worked example — average temperature per city — solved **without** a combiner. Pairs with deck 11 for a direct before/after comparison. |
| 11 | [`11_mapreduce_with_combiners.pptx`](./11_mapreduce_with_combiners.pptx) | PPTX | 42 | The same average-temperature-per-city example, redone **with** a combiner, so decks 10 and 11 can be compared side by side to see exactly what the combiner changes. |

## Suggested order

The numeric prefixes already give the intended order:

1. **00–02** — motivation and foundations (what MapReduce is, why parallelism matters)
2. **03–04** — the MapReduce model itself (long form, then a condensed version)
3. **05–06** — Word Count, first in plain Python, then in MapReduce
4. **07–08** — filters, and the storage layer (HDFS) underneath MapReduce
5. **09–11** — combiners, illustrated with a matched without/with example pair

## Note on provenance

Every deck prefixed `01`–`11` is Mahmoud Parsian's own material. The
`00_Understanding_MapReduce_by_Databricks.pdf` file is a third-party
reference (Databricks) kept here only as an introductory pointer —
see the PDF for its original source and copyright.
