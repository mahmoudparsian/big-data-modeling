#!/usr/bin/env python3
"""
pymk_pyspark_using_graphframes.py

People You May Know (PYMK) -- the same algorithm and the same output
as the other three scripts in this folder, this time expressed as a
single graph pattern-match query over a GraphFrames `GraphFrame`
instead of as pair-emission (`flatMap`/`groupByKey`/`reduceByKey`) or
DataFrame joins (`explode`/self-join/`groupBy`). Read
pymk_pyspark_using_dataframes.py first; this file exists to show what
happens to the DataFrame version's Rule 1 + Rule 2 + shuffle&reduce
once the data is loaded into an actual graph abstraction with a
declarative pattern-matching query language, instead of staying a
pair of flat tables joined by hand.

Friendship here is symmetric ("mutual", Facebook-style, not a
one-way Twitter/X "follows") -- same as every other script in this
folder; see the README (Section 1) for why that assumption is load-
bearing for this whole algorithm.

------------------------------------------------------------------
THE SAME ALGORITHM, AS A GRAPH MOTIF QUERY
------------------------------------------------------------------
Build a GraphFrame directly from the input: one vertex per person,
one directed edge per (person, friend) pair. Because friendship is
symmetric, every edge already exists in both directions -- A's line
listing B, and B's line listing A -- so this graph *is* the mutual-
friend graph, with no extra symmetrization step needed to build it.

The entire "Rule 1 tags edges, Rule 2 vouches for pairs, shuffle
groups them by pair, reduce drops the FRIEND-tagged ones" pipeline
that the other three scripts build up over several stages collapses
to one motif query plus one join:

    "a and b are two DIFFERENT people who both have an edge to some
     common vertex c"

        motifs = g.find("(a)-[]->(c); (b)-[]->(c)").filter("a.id != b.id")

That single line *is* Rule 2 -- every match is exactly one (a, b, c)
triple where c is a mutual friend of a and b -- and there is no
`combinations()` call, no self-join written by hand, no explicit
pair-key at all: GraphFrames' pattern matcher enumerates every pair
of length-1 paths that share an endpoint, which is precisely what
"two people have a mutual friend" means on a graph. (It also means
Rule 1's "FRIEND" tag and Rule 2's "vouch" are no longer two
differently-shaped things emitted by one mapper, the way they are in
every other script here -- there's only ever one kind of edge.)

Rule 1 -- suppressing pairs that are already friends -- becomes a
single `left_anti` join of the motif results against the same edges
DataFrame the graph itself was built from: drop any (a, b) match that
already has a direct edge. groupBy(a, b) on what's left plays the
same role as the other DataFrame script's `groupBy("a",
"b").agg(collect_set, count)`, and a ranking `Window` finishes the
job exactly as in pymk_pyspark_using_dataframes.py.

The "elegant" part is qualitative, not just fewer lines: Rule 2 in
every other script in this folder is fundamentally a workaround --
combinations()/a self-join is how you ask "which pairs share a
neighbor" using data structures that don't know what a graph is.
Phrased as a GraphFrame, "which pairs share a neighbor" is a native
question the pattern-matching API answers directly.

------------------------------------------------------------------
DEPENDENCY NOTE
------------------------------------------------------------------
Unlike the other three scripts, this one needs the `graphframes`
package on BOTH sides of the PySpark/JVM boundary:

    pip install graphframes-py

installs the *Python* wrapper (imported below), but GraphFrame's
actual pattern-matching engine is a JVM library that PySpark has to
load separately. main() below adds it via
`spark.jars.packages` on the SparkSession builder, so
`python3 pymk_pyspark_using_graphframes.py ...` resolves and
downloads it automatically through Ivy/Maven on first run (needs
network access once; cached under `~/.ivy2*/cache` after that) --
no separate `--packages` flag required for the same "just run it
with python3" experience the other three scripts give you.

`GRAPHFRAMES_MAVEN_COORDINATE` below is pinned to match a Spark 4.x /
Scala 2.13 build (`graphframes-spark4_2.13:0.10.0`), which is what
this folder was developed and tested against. GraphFrames publishes
one build per (GraphFrames version, Spark major version, Scala
version) combination -- e.g. `graphframes-spark3_2.12` for a Spark
3.x / Scala 2.12 cluster -- so on a different Spark install, update
the coordinate to match `spark.version` and the cluster's Scala
version (both printed by this script's `--version-info` flag) or the
job will fail to load the JVM class at `GraphFrame(...)` construction
time, not at import time.

On a real cluster, prefer passing the coordinate explicitly instead
of relying on this script's baked-in config, so the driver doesn't
need internet access at submit time:

    spark-submit --packages io.graphframes:graphframes-spark4_2.13:0.10.0 \\
        pymk_pyspark_using_graphframes.py <input_file> [--top-k K] [--output OUT_DIR]

See the companion documents for the underlying algorithm and the
three implementations this file is a fourth alternative to:

    slides/mapreduce/mapreduce_examples/MapReduce_People_You_May_Know.md
    pymk_pyspark_using_groupbykey.py                      (this folder)
    pymk_pyspark_using_reducebykey.py                     (this folder)
    pymk_pyspark_using_dataframes.py                      (this folder)

Usage:
    spark-submit --packages io.graphframes:graphframes-spark4_2.13:0.10.0 \\
        pymk_pyspark_using_graphframes.py <input_file> [--top-k K] [--output OUT_DIR]

Run it locally with:
    python3 pymk_pyspark_using_graphframes.py data/friends.txt

Or, on the larger 12-person sample graph, keeping only each person's
top-2 recommendations:
    python3 pymk_pyspark_using_graphframes.py data/friends_larger.txt --top-k 2
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Pin to match this folder's development environment: Spark 4.x,
# Scala 2.13. See the "DEPENDENCY NOTE" section above for how to
# update this for a different Spark/Scala combination.
GRAPHFRAMES_MAVEN_COORDINATE = "io.graphframes:graphframes-spark4_2.13:0.10.0"

Candidate = tuple[str, int, list[str]]


def read_friends_df(spark: SparkSession, input_file: str) -> DataFrame:
    """Read an adjacency-list file into a `(person: str, friends:
    array<string>)` DataFrame -- identical to
    pymk_pyspark_using_dataframes.py's function of the same name.
    Duplicated here (rather than imported) so this file stays a
    single, self-contained `spark-submit`-able script, matching the
    other three scripts' style.

    Expected format, one person per line:
        <person>,<friend_1>,<friend_2>,...,<friend_n>
    Blank lines and lines starting with '#' are ignored, so the
    sample data files can carry documentation as comments.
    """
    raw = spark.read.text(input_file).withColumnRenamed("value", "line")
    trimmed = raw.select(F.trim(F.col("line")).alias("line"))
    data_lines = trimmed.filter(
        (F.col("line") != "") & (~F.col("line").startswith("#"))
    )
    fields = data_lines.select(
        F.split(F.col("line"), r"\s*,\s*").alias("fields")
    )
    return fields.select(
        F.col("fields")[0].alias("person"),
        F.slice(F.col("fields"), 2, F.size(F.col("fields"))).alias("friends"),
    )


def build_graph(friends_df: DataFrame):
    """Turn a `(person, friends)` DataFrame into vertices and edges
    DataFrames in the shape GraphFrame expects (`id`, and `src`/`dst`
    respectively), and return `(vertices, edges, GraphFrame)`.

    One directed edge per (person, friend) pair. Because friendship
    is symmetric (every edge appears on both endpoints' input lines),
    this graph already has an edge in both directions for every
    friendship -- no separate symmetrization step is needed the way
    the RDD scripts need sort_pair() or the DataFrame script needs
    least()/greatest().
    """
    from graphframes import GraphFrame

    vertices = friends_df.select(F.col("person").alias("id")).distinct()
    edges = friends_df.select(
        F.col("person").alias("src"), F.explode("friends").alias("dst")
    )
    return vertices, edges, GraphFrame(vertices, edges)


def build_recommendations_df(
    friends_df: DataFrame, top_k: int | None = None
) -> DataFrame:
    """Run the full PYMK pipeline via a GraphFrames motif query.

    Returns a `(person, candidate, mutual_count, mutual_friends,
    rank)` DataFrame -- the same shape as
    pymk_pyspark_using_dataframes.py's build_recommendations_df().

    Every step mirrors a phase of the algorithm, expressed on a
    GraphFrame instead of flat DataFrames:
        build_graph()                              -> the graph itself
        g.find("(a)-[]->(c); (b)-[]->(c)")          -> Rule 2, in one
                                                        declarative query
        .filter("a.id != b.id")                     -> drop the
                                                        self-pair (P,P)
        left_anti join against `edges`               -> Rule 1's job:
                                                        drop already-
                                                        friend pairs
        groupBy("person","candidate").agg(...)       -> Shuffle + Reduce
        Window.partitionBy("person").orderBy(...)    -> downstream ranking
    """
    _vertices, edges, g = build_graph(friends_df)

    # The motif itself: every pair of DIFFERENT people (a, b) who
    # each have an outgoing edge to the same vertex c -- i.e. c is a
    # mutual friend of a and b. This one query does the job of Rule 2
    # (rule2_mutual_vouches / build_vouches_df) in every other script.
    motifs = g.find("(a)-[]->(c); (b)-[]->(c)").filter("a.id != b.id")

    candidate_pairs = motifs.select(
        F.col("a.id").alias("person"),
        F.col("b.id").alias("candidate"),
        F.col("c.id").alias("mutual_friend"),
    )

    # Rule 1's job: a pair that already has a direct edge is already
    # friends and must be suppressed. A left_anti join against the
    # same `edges` DataFrame the graph was built from does this in
    # one step -- no "FRIEND" sentinel to check for, because there's
    # only one kind of edge in a GraphFrame to begin with.
    non_friend_pairs = candidate_pairs.join(
        edges.withColumnRenamed("src", "person").withColumnRenamed(
            "dst", "candidate"
        ),
        on=["person", "candidate"],
        how="left_anti",
    )

    reduced_df = non_friend_pairs.groupBy("person", "candidate").agg(
        F.array_sort(F.collect_set("mutual_friend")).alias("mutual_friends"),
        F.count("mutual_friend").alias("mutual_count"),
    )
    # reduced_df is already in the final, per-person-recommendation
    # shape -- unlike the other DataFrame script, there's no separate
    # "symmetrize" union step here: the motif query already produced
    # BOTH (A, E, ...) and (E, A, ...) as independent matches, because
    # a and b range freely over all vertices with a common neighbor.

    ranking = Window.partitionBy("person").orderBy(
        F.col("mutual_count").desc(), F.col("candidate").asc()
    )
    ranked_df = reduced_df.withColumn("rank", F.row_number().over(ranking))
    if top_k is not None:
        ranked_df = ranked_df.filter(F.col("rank") <= top_k)

    # Same fix as the other two DataFrame-shaped joins in this folder:
    # a person embedded in a friend clique with no outside mutual-
    # friend connections (B and C in data/friends.txt) never appears
    # in reduced_df on its own. Restore them with null candidate
    # columns via a left join against every known person.
    all_people_df = friends_df.select("person").distinct()
    return all_people_df.join(ranked_df, on="person", how="left")


def rows_to_recommendations(
    rows: Iterable, top_k: int | None = None
) -> dict[str, list[Candidate]]:
    """Turn build_recommendations_df()'s collected Rows into the same
    `{person: [(candidate, mutual_count, mutual_friends), ...]}` shape
    used by every other script's format_recommendations() -- identical
    to pymk_pyspark_using_dataframes.py's function of the same name,
    including the same "don't trust collect()'s row order" reasoning.
    """
    by_person: dict[str, list[Candidate]] = defaultdict(list)
    for row in rows:
        by_person[row.person]  # ensure every person has an entry
        if row.candidate is not None:
            by_person[row.person].append(
                (row.candidate, row.mutual_count, list(row.mutual_friends))
            )
    for person, candidates in by_person.items():
        candidates.sort(key=lambda c: (-c[1], c[0]))
        if top_k is not None:
            by_person[person] = candidates[:top_k]
    return dict(sorted(by_person.items()))


def format_recommendations(
    recommendations: dict[str, list[Candidate]],
) -> str:
    """Render (person, [(candidate, count, mutual), ...]) rows as text,
    sorted by person for deterministic, readable output."""
    lines = []
    for person, candidates in recommendations.items():
        if not candidates:
            lines.append(f"{person}: (no recommendations)")
            continue
        rendered = ", ".join(
            f"{candidate} ({count} mutual: {'/'.join(mutual)})"
            for candidate, count, mutual in candidates
        )
        lines.append(f"{person}: {rendered}")
    return "\n".join(lines)


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    """Parse command-line arguments (argv excludes the script name,
    matching how spark-submit forwards user args)."""
    parser = argparse.ArgumentParser(
        description=(
            "People You May Know: recommend non-friends ranked by "
            "mutual-friend count, as a standalone PySpark job "
            "expressed as a GraphFrames motif query instead of "
            "RDD/DataFrame joins."
        )
    )
    parser.add_argument(
        "input_file",
        nargs="?",
        help="path to an adjacency-list file (person,friend1,friend2,...)",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=None,
        help="keep only each person's top K recommendations (default: all)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="optional directory to also write the results as a TSV "
        "text file (person<TAB>candidate<TAB>count<TAB>mutual_friends)",
    )
    parser.add_argument(
        "--version-info",
        action="store_true",
        help="print this Spark install's version and Scala version "
        "(useful for picking the right --packages coordinate) and exit",
    )
    args = parser.parse_args(argv)
    if not args.version_info and args.input_file is None:
        parser.error("input_file is required unless --version-info is given")
    return args


def main() -> int:
    """Run the Spark job."""
    args = parse_arguments(sys.argv[1:])

    spark = (
        SparkSession.builder.appName("PeopleYouMayKnowGraphFrames")
        .config("spark.jars.packages", GRAPHFRAMES_MAVEN_COORDINATE)
        .getOrCreate()
    )

    if args.version_info:
        scala_version = (
            spark.sparkContext._jvm.scala.util.Properties.versionNumberString()
        )
        print(f"spark.version= {spark.version}")
        print(f"scala.version= {scala_version}")
        spark.stop()
        return 0

    print("input_file=", args.input_file)

    friends_df = read_friends_df(spark, args.input_file)

    people_count = friends_df.count()
    print(f"people_in_graph= {people_count}")
    if args.top_k is not None:
        print(f"top_k= {args.top_k}")

    recommendations_df = build_recommendations_df(friends_df, top_k=args.top_k)
    rows = recommendations_df.collect()
    recommendations = rows_to_recommendations(rows, top_k=args.top_k)

    print()
    print(format_recommendations(recommendations))

    if args.output is not None:
        tsv_rows = [
            f"{person}\t{candidate}\t{count}\t{','.join(mutual)}"
            for person, candidates in recommendations.items()
            for candidate, count, mutual in candidates
        ]
        spark.sparkContext.parallelize(tsv_rows).saveAsTextFile(args.output)
        print()
        print(f"Wrote recommendations to {args.output}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
SAMPLE RUN:

% python3 pymk_pyspark_using_graphframes.py data/friends.txt
input_file= data/friends.txt
people_in_graph= 6

A: E (3 mutual: B/C/D), F (2 mutual: B/C)
B: (no recommendations)
C: (no recommendations)
D: F (2 mutual: B/C)
E: A (3 mutual: B/C/D), F (2 mutual: B/C)
F: A (2 mutual: B/C), D (2 mutual: B/C), E (2 mutual: B/C)

SAMPLE RUN (larger graph, top-2 recommendations per person):

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

Byte-for-byte identical to the other three scripts' output on the
same inputs -- this file changes HOW the computation is expressed (a
declarative graph motif query instead of RDD operators or DataFrame
joins), not WHAT is computed.
"""
