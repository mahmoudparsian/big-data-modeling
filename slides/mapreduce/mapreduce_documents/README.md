# Introduction to MapReduce — Curated Reading List

	Curated by: Mahmoud Parsian
	Last updated: 9/5/2026

This folder collects external articles, papers, and slide decks
that introduce the MapReduce paradigm. None of these were authored
by Mahmoud Parsian — they are third-party reference material kept
here for teaching. The list below was pared down from an original
11 papers to the 6 that offer the most distinct value (shortest
path to understanding, deepest algorithm-design theory, or the
most hands-on depth), with minimal overlap between entries, plus
one short critical-perspective piece (#7) added afterward as an
optional epilogue for once Spark has also been covered.

## Papers

| # | Title | Author(s) | Format | Length | Best for | File |
|---|---|---|---|---|---|---|
| 1 | A Very Brief Introduction to MapReduce | Diana MacLean (Stanford, CS448G) | PDF notes | 3 pp. | **Start here.** The fastest, clearest on-ramp to `map`/`reduce`/`<key,value>` thinking — read this first if you only read one thing. | [`Introduction_to_MapReduce_by_Diana_MacLean.pdf`](./Introduction_to_MapReduce_by_Diana_MacLean.pdf) |
| 2 | Designing Good MapReduce Algorithms | Jeffrey D. Ullman (Stanford), *Communications of the ACM*, 2012 | Magazine article | 5 pp. | The **theory** of MapReduce algorithm design — replication rate and the communication-cost tradeoff that separates a good MapReduce algorithm from a naive one. | [`Designing-Good-MapReduce-Algorithms-Ullman-2012.pdf`](./Designing-Good-MapReduce-Algorithms-Ullman-2012.pdf) |
| 3 | MapReduce for Beginners | Bushra Anjum (Amazon; PDC curriculum chapter) | Textbook chapter | 19 pp. | A **classroom-ready** treatment with explicit learning outcomes — good source for lecture structure and student-facing framing. | [`MapReduce_for_Beginners_by_Anjum_Bushra.pdf`](./MapReduce_for_Beginners_by_Anjum_Bushra.pdf) |
| 4 | Introduction to MapReduce and Hadoop | Matei Zaharia (UC Berkeley RAD Lab; later creator of Apache Spark) | Slide deck | 61 slides | The most **comprehensive general overview** here — model, examples, and the Hadoop ecosystem, from a top-tier source. | [`MapReduce_by_Matei_Zaharia.pdf`](./MapReduce_by_Matei_Zaharia.pdf) |
| 5 | MapReduce: Algorithm Design | Juliana Freire (NYU); slides draw on Jimmy Lin, Jeffrey Ullman, and Jure Leskovec | Slide deck | 76 slides | Deep dive on **algorithm-design patterns** (pairs vs. stripes, secondary sort, etc.) — the natural next step after Ullman's short article (#2). | [`mapreduce-algorithm-design-compiled-by-Juliana-Freire.pdf`](./mapreduce-algorithm-design-compiled-by-Juliana-Freire.pdf) |
| 6 | MapReduce, Distributed File Systems, Hadoop, and Data Mining | Chris Jermaine (Rice University) | Workshop slide deck | 147 slides | **Advanced / optional.** A full two-day workshop: standing up a real Hadoop cluster on EC2, then implementing data-mining algorithms (K-Means, KNN) on top of MapReduce. The only entry here with hands-on cluster + ML content. | [`MapReduce_DISTRIBUTED_FILE_SYSTEMS_HADOOP_and_DATA_MINING.pdf`](./MapReduce_DISTRIBUTED_FILE_SYSTEMS_HADOOP_and_DATA_MINING.pdf) |
| 7 | In Defense of MapReduce | Jimmy Lin (University of Waterloo), *IEEE Internet Computing* "Big Data Bites" column, 2017 | Magazine column | 5 pp. | **Optional epilogue, read after Spark.** A critical, comparative essay: MAP/REDUCE are *physical* operators while Spark's transformations are *logical* ones, so "MapReduce vs. Spark" is an apples-to-oranges comparison — argues MapReduce's constrained API still has real conceptual merit even though Spark is, overall, a superior implementation. | [`In_Defense_of_MapReduce_Jimmy_Lin_IEEE2017.pdf`](./In_Defense_of_MapReduce_Jimmy_Lin_IEEE2017.pdf) |

## Suggested reading order

1. **MacLean** — get the core `map`/`reduce` mental model in 10 minutes.
2. **Ullman** — understand *why* a MapReduce algorithm is good or bad (cost theory).
3. **Anjum** — see it framed as a full lesson, with learning outcomes.
4. **Zaharia** — go broad: the full model plus the Hadoop ecosystem around it.
5. **Freire** — go deep on algorithm-design patterns.
6. **Jermaine** — optional: hands-on cluster setup and MapReduce-based data mining.
7. **Lin** — optional epilogue, once Spark has also been covered: revisit MapReduce with a critical eye on how it really compares to Spark.

## Other files in this folder

- [`MapReduce_with_map_and_reduce.jpg`](./MapReduce_with_map_and_reduce.jpg) — diagram illustrating the map/reduce data flow.

## Note on provenance

Every paper/deck in the table above was written by its listed
author(s), not by Mahmoud Parsian. They are kept here strictly as
reference/teaching material; see each PDF for its original
copyright and source.
