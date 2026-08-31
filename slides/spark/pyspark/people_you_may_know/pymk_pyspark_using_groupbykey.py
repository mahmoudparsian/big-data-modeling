#!/usr/bin/env python3
"""
pymk_pyspark_using_groupbykey.py

People You May Know (PYMK), the distributed version: for every pair
of people who are NOT already friends, find how many friends they
have in common and who those mutual friends are -- implemented as a
standalone PySpark RDD job.

This is the Spark counterpart of pymk_pure_python.py in this same
folder, and both follow the same MapReduce "pairs" algorithm worked
out step by step (with a hand-traceable 6-person example) in the
companion write-up:

    slides/mapreduce/mapreduce_examples/MapReduce_People_You_May_Know.md

Reading that document first is recommended -- this script is the
"Section 15: Spark Equivalent" sketch from that document, expanded
into a complete, runnable, documented job with a CLI, ranking, and a
--top-k option, in the same spirit as this folder's
pymk_pure_python.py.

------------------------------------------------------------------
THE ALGORITHM, IN SPARK TERMS
------------------------------------------------------------------
Input:  an RDD of (person, [friend, ...]) records -- an adjacency list.
Output: for every pair of people who are NOT already friends, the
        count and names of the friends they have in common.

Two "mapper rules" are applied to every (person, friends) record via
flatMap():

  Rule 1 -- tag existing edges (rule1_friend_tags).
      For each friend F of person P, emit (sort_pair(P, F), "FRIEND").
      This records which pairs are already directly connected.

  Rule 2 -- vouch for every pair of P's own friends (rule2_mutual_vouches).
      For every 2-combination (X, Y) of P's friends, emit
      (sort_pair(X, Y), P) -- "P is a mutual friend of X and Y",
      regardless of whether X and Y are friends with each other.

The two flatMap outputs are unioned and grouped by key with
groupByKey() -- Spark's equivalent of MapReduce's shuffle & sort. The
reducing step (done here as a mapValues() over the grouped RDD) drops
any pair whose group contains "FRIEND", and for the rest reports how
many mutual friends there are and who they are.

groupByKey() is used here (rather than reduceByKey()) because the
reducing step needs the *whole* list of vouchers per pair before it
can decide count and membership -- there's no partial, associative
combine that would let reduceByKey() do less shuffle work here (see
the "Combiner" discussion in the companion MapReduce doc, Section
13). For very large, high-degree ("hub node") graphs, prefer
aggregateByKey() with an early "already saw FRIEND, drop the rest"
short-circuit -- see the Scalability Notes section of the companion
document and the README in this folder.

Usage:
    spark-submit pymk_pyspark_using_groupbykey.py <input_file> [--top-k K] [--output OUT_DIR]

Run it locally with:
    python3 pymk_pyspark_using_groupbykey.py data/friends.txt

Or, on the larger 12-person sample graph, keeping only each person's
top-2 recommendations:
    python3 pymk_pyspark_using_groupbykey.py data/friends_larger.txt --top-k 2
"""

from __future__ import annotations

import argparse
import sys
from itertools import combinations
from typing import Iterable, Iterator

from pyspark.sql import SparkSession

Pair = tuple[str, str]


def sort_pair(a: str, b: str) -> Pair:
    """Return (a, b) in a fixed, comparable order.

    This is what makes (X, Y) and (Y, X) collapse to the same key
    during groupByKey() -- the Spark equivalent of a MapReduce
    framework routing both to the same reducer because they hash to
    the same partition.
    """
    return (a, b) if a < b else (b, a)


def parse_line(line: str) -> tuple[str, list[str]] | None:
    """Parse one 'person,friend1,friend2,...' line into (person, friends).

    Returns None for blank lines or comment lines (starting with '#')
    so the sample data files can carry documentation.
    """
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    fields = [field.strip() for field in stripped.split(",")]
    person, friends = fields[0], fields[1:]
    return person, friends


def rule1_friend_tags(record: tuple[str, list[str]]) -> Iterator[tuple[Pair, str]]:
    """Mapper Rule 1: tag every existing (person, friend) edge as
    'FRIEND', so the reduce step can suppress pairs that are already
    directly connected.
    """
    person, friends = record
    for friend in friends:
        yield sort_pair(person, friend), "FRIEND"


def rule2_mutual_vouches(record: tuple[str, list[str]]) -> Iterator[tuple[Pair, str]]:
    """Mapper Rule 2: every unordered pair drawn from person P's own
    friend list shares P as a mutual friend, whether or not that pair
    is friends with each other -- so P "vouches" for every such pair.
    """
    person, friends = record
    for x, y in combinations(sorted(friends), 2):
        yield sort_pair(x, y), person


def reduce_group(values: Iterable[str]) -> tuple[int, list[str]] | None:
    """The reduce step for one grouped pair: drop it if it was ever
    tagged 'FRIEND' (already friends); otherwise return the mutual
    friend count and sorted names.
    """
    values = list(values)
    if "FRIEND" in values:
        return None
    mutual_friends = sorted(values)
    return len(mutual_friends), mutual_friends


def build_recommendations(friends_rdd, top_k: int | None = None):
    """Run the full PYMK pipeline on an RDD of (person, [friends]).

    Returns an RDD of (person, candidate, mutual_count, mutual_friends)
    rows -- one row per person per recommended candidate, each
    person's rows sorted by descending mutual_count (ties broken by
    candidate name), optionally truncated to their top K.

    Every step below mirrors a phase of the MapReduce algorithm in
    the companion document:
        flatMap (Rule 1) + flatMap (Rule 2)  -> the Map phase
        union().groupByKey()                 -> Shuffle & Sort
        mapValues(reduce_group)              -> the Reduce phase
    """
    edges = friends_rdd.flatMap(rule1_friend_tags)
    vouches = friends_rdd.flatMap(rule2_mutual_vouches)

    reduced = (
        edges.union(vouches)
        .groupByKey()
        .mapValues(reduce_group)
        .filter(lambda kv: kv[1] is not None)
        .map(lambda kv: (kv[0][0], kv[0][1], kv[1][0], kv[1][1]))
    )
    # reduced: RDD[(person_a, person_b, mutual_count, mutual_friends)]
    # -- one row per non-friend PAIR, not yet duplicated per person.

    # Every pair recommends each endpoint to the other, so duplicate
    # each row with (a, b) swapped before ranking per person. This is
    # the "downstream sort" step mentioned in the companion document
    # (Section 12) -- here folded into the same job as a final
    # groupByKey + sort instead of a second MapReduce/Spark pass.
    symmetric = reduced.flatMap(
        lambda row: [
            (row[0], (row[1], row[2], row[3])),
            (row[1], (row[0], row[2], row[3])),
        ]
    )

    def rank(candidates: Iterable[tuple[str, int, list[str]]]):
        ranked = sorted(candidates, key=lambda c: (-c[1], c[0]))
        return ranked[:top_k] if top_k is not None else ranked

    ranked_candidates = symmetric.groupByKey().mapValues(rank)

    # A person who is already friends with everyone else in the graph
    # (e.g. B and C in data/friends.txt) never emits a Rule-2 vouch
    # for themselves and so never appears as a key above. leftOuterJoin
    # against every known person restores them with an empty
    # candidate list instead of silently dropping them from the output.
    all_people = friends_rdd.map(lambda record: (record[0], None))
    return all_people.leftOuterJoin(ranked_candidates).mapValues(
        lambda joined: joined[1] if joined[1] is not None else []
    )


def format_recommendations(rows: list[tuple[str, list[tuple[str, int, list[str]]]]]) -> str:
    """Render (person, [(candidate, count, mutual), ...]) rows as text,
    sorted by person for deterministic, readable output."""
    lines = []
    for person, candidates in sorted(rows, key=lambda item: item[0]):
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
            "mutual-friend count, as a standalone PySpark RDD job."
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
        help="optional directory to also write the results as a Spark "
        "text file (person<TAB>candidate<TAB>count<TAB>mutual_friends)",
    )
    return parser.parse_args(argv)


def main() -> int:
    """Run the Spark job."""
    args = parse_arguments(sys.argv[1:])

    print("input_file=", args.input_file)

    spark = SparkSession.builder.appName("PeopleYouMayKnow").getOrCreate()
    sc = spark.sparkContext

    lines = sc.textFile(args.input_file)
    friends_rdd = lines.map(parse_line).filter(lambda r: r is not None)

    people_count = friends_rdd.count()
    print(f"people_in_graph= {people_count}")
    if args.top_k is not None:
        print(f"top_k= {args.top_k}")

    recommendations = build_recommendations(friends_rdd, top_k=args.top_k)
    rows = recommendations.collect()

    print()
    print(format_recommendations(rows))

    if args.output is not None:
        tsv_rows = recommendations.flatMap(
            lambda pc: [
                f"{pc[0]}\t{candidate}\t{count}\t{','.join(mutual)}"
                for candidate, count, mutual in pc[1]
            ]
        )
        tsv_rows.saveAsTextFile(args.output)
        print()
        print(f"Wrote recommendations to {args.output}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
SAMPLE RUN:

% python3 pymk_pyspark_using_groupbykey.py data/friends.txt
input_file= data/friends.txt
people_in_graph= 6

A: E (3 mutual: B/C/D), F (2 mutual: B/C)
B: (no recommendations)
C: (no recommendations)
D: F (2 mutual: B/C)
E: A (3 mutual: B/C/D), F (2 mutual: B/C)
F: A (2 mutual: B/C), D (2 mutual: B/C), E (2 mutual: B/C)

SAMPLE RUN (larger graph, top-2 recommendations per person):

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

(Run via `spark-submit pymk_pyspark_using_groupbykey.py data/friends.txt` on a real
cluster/local Spark install; the output above was produced by running
the script directly with `python3`, which works too since SparkSession
.getOrCreate() starts a local in-process Spark context when one isn't
already running.)
"""
