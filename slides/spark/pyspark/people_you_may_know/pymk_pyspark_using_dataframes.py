#!/usr/bin/env python3
"""
pymk_pyspark_using_dataframes.py

People You May Know (PYMK) -- the same algorithm and the same output
as pymk_pyspark_using_groupbykey.py and pymk_pyspark_using_reducebykey.py in this
folder, rewritten a third time using the Spark SQL DataFrame API
instead of the RDD API. Read pymk_pyspark_using_groupbykey.py first for the
algorithm in RDD/MapReduce terms; this file exists to show what the
*same* two mapper rules and reduce step look like as DataFrame
operations -- columns, joins, and a window function -- instead of
`flatMap`/`groupByKey`/`mapValues`.

Friendship here is symmetric ("mutual", Facebook-style) rather than a
one-way "follows" relationship (Twitter/X-style): if A lists B as a
friend, B is expected to list A too (see data/friends.txt), and every
recommendation this script makes is symmetric as a result -- if A
gets recommended E, E gets recommended A with the same mutual-friend
count and names. Adapting Rule 1/Rule 2 for a directed "follows" graph
is listed as an open extension in the companion README (Section 9,
item 2).

------------------------------------------------------------------
THE SAME ALGORITHM, IN DATAFRAME TERMS
------------------------------------------------------------------
There is no per-record Python `yield` in a DataFrame job -- every
"mapper rule" below is a column expression or a join applied to every
row at once, and the "shuffle & reduce" is a `groupBy().agg()`. Same
two rules, same idea, different vocabulary:

  Rule 1 -- tag existing edges (build_edges_df).
      explode() each person's friends array into one (person, friend)
      row per edge, then reorder each row with least()/greatest()
      into (a, b) so (X, Y) and (Y, X) collapse to the same row --
      the DataFrame equivalent of sort_pair() in the other two
      scripts. distinct() dedupes, since both endpoints of a
      friendship list each other, so every edge appears twice before
      this step.

  Rule 2 -- vouch for every pair of a person's own friends
      (build_vouches_df).
      A mapper can loop `for x, y in combinations(friends, 2)`; a
      DataFrame has no combinations() operator, so the same pairing
      is expressed as a SELF-JOIN: explode() a person's friends twice
      under different aliases (friend1, friend2), join those two
      exploded views back together on `person`, and keep only rows
      where friend1 < friend2 -- an ordinary string-inequality filter
      that both discards self-pairs (friend1 == friend2 can't satisfy
      `<`) and picks a single, fixed order per pair, exactly like
      sort_pair() elsewhere. What is a nested Python loop over one
      person's friend list in the other scripts becomes a Cartesian
      join *within* each person's own friend list here.

  Shuffle + Reduce (groupBy("a", "b").agg(...)).
      Group the Rule-2 output by pair and collect every voucher's
      name (collect_set) and how many there are (count) -- Spark's
      DataFrame-level equivalent of groupByKey().mapValues(reduce_group).
      Suppressing already-friend pairs -- the "if FRIEND in values:
      drop" check in the other two scripts -- becomes a LEFT ANTI
      JOIN against build_edges_df's (a, b) pairs: keep only grouped
      rows that have no matching edge.

  Downstream "rank by count" pass (a Window function).
      The other two scripts finish with a groupByKey().mapValues(sort)
      (or its reduceByKey() equivalent). Here that becomes a SQL
      window: partitionBy("person").orderBy(mutual_count desc,
      candidate asc), with row_number() giving each person's
      candidates a rank so `--top-k` is a plain `rank <= K` filter --
      this is the "wire top-K into a DataFrame Window function"
      extension the companion README suggests in Section 9, item 1.

See the companion documents for the underlying algorithm and the two
RDD-based implementations this file is a third alternative to:

    slides/mapreduce/mapreduce_examples/MapReduce_People_You_May_Know.md
    pymk_pyspark_using_groupbykey.py                      (this folder)
    pymk_pyspark_using_reducebykey.py                     (this folder)

Usage:
    spark-submit pymk_pyspark_using_dataframes.py <input_file> [--top-k K] [--output OUT_DIR]

Run it locally with:
    python3 pymk_pyspark_using_dataframes.py data/friends.txt

Or, on the larger 12-person sample graph, keeping only each person's
top-2 recommendations:
    python3 pymk_pyspark_using_dataframes.py data/friends_larger.txt --top-k 2
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

Candidate = tuple[str, int, list[str]]


def read_friends_df(spark: SparkSession, input_file: str) -> DataFrame:
    """Read an adjacency-list file into a `(person: str, friends:
    array<string>)` DataFrame.

    Expected format, one person per line:
        <person>,<friend_1>,<friend_2>,...,<friend_n>
    Blank lines and lines starting with '#' are ignored (via a
    `.filter()` on the raw text), so the sample data files can carry
    documentation as comments -- the DataFrame counterpart of
    parse_line()'s `if not stripped or stripped.startswith("#")` in
    the other two scripts.
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


def build_edges_df(friends_df: DataFrame) -> DataFrame:
    """Rule 1: one (a, b) row per existing friendship edge, a < b.

    explode()s each person's friends array into individual
    (person, friend) rows, then least()/greatest() reorders each row
    into a fixed (a, b) order -- the DataFrame equivalent of
    sort_pair(). Since friendship is symmetric (both endpoints list
    each other, per data/friends.txt), every edge shows up twice
    before explode(); distinct() collapses each back to one row.
    """
    edges = friends_df.select(
        F.col("person"), F.explode("friends").alias("friend")
    )
    return edges.select(
        F.least("person", "friend").alias("a"),
        F.greatest("person", "friend").alias("b"),
    ).distinct()


def build_vouches_df(friends_df: DataFrame) -> DataFrame:
    """Rule 2: one (a, b, voucher) row per unordered pair of a
    person's own friends, a < b -- 'voucher is a mutual friend of a
    and b'.

    There's no combinations() operator over a DataFrame column, so
    the same pairing is done as a self-join: explode() the same
    friends array twice, under two aliases (friend1, friend2), join
    those two views back together on `person` (a Cartesian product
    *within* each person's own friend list), and keep only rows where
    friend1 < friend2. That inequality does two jobs at once: it
    drops the friend1 == friend2 diagonal (a person can't vouch for a
    pair containing themselves twice) and it fixes a single,
    consistent pair order, exactly like sort_pair() in the other
    scripts.
    """
    friend1 = friends_df.select(
        "person", F.explode("friends").alias("friend1")
    )
    friend2 = friends_df.select(
        "person", F.explode("friends").alias("friend2")
    )
    joined = friend1.join(friend2, on="person").filter(
        F.col("friend1") < F.col("friend2")
    )
    return joined.select(
        F.col("friend1").alias("a"),
        F.col("friend2").alias("b"),
        F.col("person").alias("voucher"),
    )


def build_recommendations_df(
    friends_df: DataFrame, top_k: int | None = None
) -> DataFrame:
    """Run the full PYMK pipeline on a `(person, friends)` DataFrame.

    Returns a `(person, candidate, mutual_count, mutual_friends,
    rank)` DataFrame -- one row per person per recommended candidate,
    `candidate`/`mutual_count`/`mutual_friends`/`rank` all null for a
    person with zero candidates -- with the same information as the
    other two scripts' `RDD[(person, [(candidate, count, mutual), ...])]`,
    just column-shaped instead of nested.

    Every step below mirrors a phase of the MapReduce algorithm in
    the companion document, expressed as DataFrame operations:
        build_edges_df / build_vouches_df        -> the Map phase
        groupBy("a", "b").agg(...)                -> Shuffle + Reduce
        left_anti join against build_edges_df     -> drop friend pairs
        Window.partitionBy("person").orderBy(...) -> downstream ranking
    """
    edges_df = build_edges_df(friends_df)
    vouches_df = build_vouches_df(friends_df)

    reduced_df = (
        vouches_df.groupBy("a", "b")
        .agg(
            F.array_sort(F.collect_set("voucher")).alias("mutual_friends"),
            F.count("voucher").alias("mutual_count"),
        )
        .join(edges_df, on=["a", "b"], how="left_anti")
    )
    # reduced_df: one row per non-friend PAIR, not yet duplicated per
    # person -- the DataFrame counterpart of the other scripts'
    # `RDD[(person_a, person_b, mutual_count, mutual_friends)]`.

    # Every pair recommends each endpoint to the other, so union the
    # (a, b) rows with themselves swapped to (b, a) before ranking --
    # the DataFrame counterpart of the other scripts' symmetrizing
    # flatMap().
    symmetric_df = reduced_df.select(
        F.col("a").alias("person"),
        F.col("b").alias("candidate"),
        "mutual_count",
        "mutual_friends",
    ).unionByName(
        reduced_df.select(
            F.col("b").alias("person"),
            F.col("a").alias("candidate"),
            "mutual_count",
            "mutual_friends",
        )
    )

    ranking = Window.partitionBy("person").orderBy(
        F.col("mutual_count").desc(), F.col("candidate").asc()
    )
    ranked_df = symmetric_df.withColumn("rank", F.row_number().over(ranking))
    if top_k is not None:
        ranked_df = ranked_df.filter(F.col("rank") <= top_k)

    # A person who is already friends with everyone else in the graph
    # (e.g. B and C in data/friends.txt) never emits a Rule-2 vouch
    # for themselves and so never appears in symmetric_df. A left
    # join against every known person restores them with null
    # candidate columns instead of silently dropping them from the
    # output.
    all_people_df = friends_df.select("person").distinct()
    return all_people_df.join(ranked_df, on="person", how="left")


def rows_to_recommendations(
    rows: Iterable, top_k: int | None = None
) -> dict[str, list[Candidate]]:
    """Turn build_recommendations_df()'s collected Rows into the same
    `{person: [(candidate, mutual_count, mutual_friends), ...]}` shape
    the other two scripts' format_recommendations() consumes.

    Re-sorts each person's candidates by (-mutual_count, candidate) in
    plain Python rather than trusting collect()'s row order to match
    the DataFrame's window-ranked order -- Spark makes no row-order
    guarantee across a collect() once rows have been shuffled through
    a join, so relying on it here would be the same class of bug as
    assuming groupByKey() returns its values pre-sorted.
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
            "mutual-friend count, as a standalone PySpark DataFrame "
            "job using explode/join/groupBy and a ranking Window "
            "function instead of the RDD API."
        )
    )
    parser.add_argument(
        "input_file",
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
    return parser.parse_args(argv)


def main() -> int:
    """Run the Spark job."""
    args = parse_arguments(sys.argv[1:])

    print("input_file=", args.input_file)

    spark = SparkSession.builder.appName("PeopleYouMayKnowDataFrames").getOrCreate()

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

% python3 pymk_pyspark_using_dataframes.py data/friends.txt
input_file= data/friends.txt
people_in_graph= 6

A: E (3 mutual: B/C/D), F (2 mutual: B/C)
B: (no recommendations)
C: (no recommendations)
D: F (2 mutual: B/C)
E: A (3 mutual: B/C/D), F (2 mutual: B/C)
F: A (2 mutual: B/C), D (2 mutual: B/C), E (2 mutual: B/C)

SAMPLE RUN (larger graph, top-2 recommendations per person):

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

Byte-for-byte identical to pymk_pyspark_using_groupbykey.py's and
pymk_pyspark_using_reducebykey.py's output on the same inputs -- this
file changes HOW the computation is expressed (DataFrame columns,
joins, and a window function instead of RDD flatMap/groupByKey/
reduceByKey), not WHAT is computed.
"""
