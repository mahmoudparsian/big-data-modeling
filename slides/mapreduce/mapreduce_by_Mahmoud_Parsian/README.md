# MapReduce Slide Deck Series — Markdown/Marp Edition

	Author: Mahmoud Parsian
	Last updated: 8/31/2026

A numbered sequence of [Marp](https://marp.app/)-format Markdown
slide decks teaching the MapReduce paradigm, from first principles
(parallelism) through a complete, comparable worked example (with
and without a combiner). This folder is a from-scratch rewrite,
replacing the 11 original PowerPoint decks (and the third-party
Databricks reference PDF) that used to live here — **11 original
PPTX decks became the 9 Markdown files below**, merging decks that
turned out to be near-duplicates of each other (`01`+`02`, `03`+`04`)
and, where this repo already had a more thorough writeup of a deck's
material elsewhere, replacing that deck with a short **bridge** file
that links to it instead of repeating it. See [Design
Notes](#design-notes) below for the reasoning file by file.

## Running These Decks

Each `.md` file is plain Marp Markdown — read it directly on GitHub,
or render it:

```text
npm install -g @marp-team/marp-cli    # once
marp --allow-local-files 01_understanding_parallelism_and_concurrency.md -o out.pdf
```

`--allow-local-files` is required whenever a deck embeds an image
from `images/` (Marp blocks local file access by default). A
rendered `.pdf` is committed alongside every `.md` source in this
folder already, so you don't have to render anything just to read
them — regenerate only if you edit a `.md` file.

## Files

| # | File | From (PPTX) | Slides | Type | Description |
|---|---|---|---|---|---|
| 00 | [`01_MapReduce_Introduction_24_slides.pptx`](01_MapReduce_Introduction_24_slides.pptx) | 00 | 24 | Full rewrite | Introduction to MapReduce paradigm |
| 01 | [`01_understanding_parallelism_and_concurrency.md`](01_understanding_parallelism_and_concurrency.md) | `01`+`02` | 24 | Full rewrite | Parallelism & concurrency from first principles, through the birthday-party analogy — the two source decks turned out ~90% identical, so this merges them into one deduplicated pass. |
| 02 | [`02_introduction_to_mapreduce.md`](02_introduction_to_mapreduce.md) | `03`+`04` | 31 | Full rewrite | The core MapReduce model: motivation, LISP/functional-programming origins, the `(key,value)` interface, the pipeline, fault tolerance, job components. Merges the long-form deck with the condensed CS512 version, keeping what each contributed uniquely. |
| 03 | [`03_word_count_in_python.md`](03_word_count_in_python.md) | `05` | 10 | Mostly rewrite | Word Count solved in plain Python first. Bridges to [`../word_count_in_python/`](../word_count_in_python/) for the runnable, file-based scripts rather than re-embedding older code. |
| 04 | [`04_word_count_in_mapreduce.md`](04_word_count_in_mapreduce.md) | `06` | 15 | Bridge | Word Count as a complete MapReduce job. Three existing docs already cover the mapper/reducer/combiner/filter mechanics in depth (linked from this file); this one covers what they don't — sizing, the partitioner, cluster architecture, output committer, Writables, and chaining multiple jobs (YARN/Oozie). |
| 05 | [`05_filters_in_mapreduce.md`](05_filters_in_mapreduce.md) | `07` | 13 | Full rewrite | The mapper-vs-reducer filter placement rule, with worked examples, a common mistake, a filter that's *only* possible in the mapper, and a self-check quiz. |
| 06 | [`06_introduction_to_hdfs.md`](06_introduction_to_hdfs.md) | `08` | 27 | Full rewrite | HDFS architecture (NameNode/DataNode), replication, the read and write paths, security, configuration, and a shell command reference. |
| 07 | [`07_combiners_in_mapreduce.md`](07_combiners_in_mapreduce.md) | `09` | 9 | Bridge | What a combiner is and where it sits in the pipeline. The theory (associativity/commutativity, the "average of an average" trap) is already covered far more rigorously elsewhere in this repo — linked, not repeated. |
| 08 | [`08_mapreduce_example_without_combiners.md`](08_mapreduce_example_without_combiners.md) | `10` | 10 | Bridge + example | Average-temperature-per-city, worked without a combiner. The full derivation of this exact problem already exists elsewhere; this file gives a second, standalone worked example with its own numbers plus a "Try It Yourself" exercise. |
| 09 | [`09_mapreduce_example_with_combiners.md`](09_mapreduce_example_with_combiners.md) | `11` | 13 | Bridge + example | The same example, redone with a combiner — a complete multi-partition mapper→combiner→shuffle→reducer numeric trace showing the average-of-an-average fix in action, plus a matching "Try It Yourself" exercise cross-checked against deck 08's answer. |

The 11 original `.pptx` decks and the third-party
`00_Understanding_MapReduce_by_Databricks.pdf` reference have been
removed from the repo — this folder now holds only the finished
Markdown/PDF series.

## Suggested Order

0. **00**    - Introduction to MapReduce
1. **01–02** — parallelism, then the MapReduce model itself
2. **03–04** — Word Count, first in plain Python, then as a full MapReduce job
3. **05–06** — filters, and the storage layer (HDFS) underneath MapReduce
4. **07–09** — combiners, illustrated with a matched without/with example pair

## Design Notes

**Merging vs. bridging.** Two different problems came up while
converting this series, and got two different fixes:

- **decks `01`+`02` and `03`+`04` duplicated each other** almost
  entirely — those got merged into one deduplicated file each
  (`01`, `02` above).
- **Several decks duplicated material that already existed elsewhere
  in this repo**, sometimes written more thoroughly than the deck
  itself (the Word Count mapper/reducer mechanics have three separate
  existing writeups; the associativity/commutativity theory behind
  combiners has a 1400+ line dedicated document; the temperature
  example has its own complete derivation). Rewriting those a 4th or
  5th time would only add to the repetition this project exists to
  cut — so files `04`, `07`, `08`, and `09` are short **bridge**
  files: enough to orient you, then a link to the fuller existing
  treatment, plus (where the source deck had one) a genuinely
  distinct worked example not found elsewhere.

**Images.** A handful of diagrams were extracted directly from the
source `.pptx` files into [`images/`](images/) (NameNode/DataNode
architecture, replication, HDFS read/write sequence diagrams, the
classic Deer-Bear-River Word Count diagram, and others). Two images
found in the `09_combiners_in_mapreduce.pptx` source carried a
visible third-party watermark and were deliberately **not** reused
here.

**Companion documents referenced throughout this series:**

- [`../word_count_in_python/`](../word_count_in_python/) — runnable Word Count scripts
- [`../mapreduce_examples/MapReduce_Word_Count.md`](../mapreduce_examples/MapReduce_Word_Count.md), [`../word_count_in_mapreduce/word_count_in_mapreduce.md`](../word_count_in_mapreduce/word_count_in_mapreduce.md), [`../combiners/Word_Count_in_MapReduce.md`](../combiners/Word_Count_in_MapReduce.md) — Word Count mechanics, three independent treatments
- [`../mapreduce_examples/MapReduce_Find_Average_Temperature.md`](../mapreduce_examples/MapReduce_Find_Average_Temperature.md) — the temperature-per-city derivation, without and with a combiner
- [`../associativity_and_commutativity/Associativity_Commutativity_and_Reducers.md`](../associativity_and_commutativity/Associativity_Commutativity_and_Reducers.md) — the formal theory behind combiner correctness
- [`../combiners/MapReduce_with_Combiners.md`](../combiners/MapReduce_with_Combiners.md) — combiners worked through a different example (average/min/max per gene)
- [`../mapreduce_examples/MapReduce_2_Examples.md`](../mapreduce_examples/MapReduce_2_Examples.md) — full worked scenarios (Word Count, Sales Revenue by Region)

## Note on Provenance

Every deck this series is derived from (`01`–`11`) is Mahmoud
Parsian's own material; this folder is a reformatting and light edit
of that material, not new authorship. Two embedded diagrams (in
`02_introduction_to_mapreduce.md`) reuse a public-domain-style
piano-keyboard illustration and the classic Deer-Bear-River Word
Count diagram common across MapReduce tutorials — both unwatermarked
and uncredited in the source deck.
