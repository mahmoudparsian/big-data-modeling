#!/usr/bin/env python3
"""
pymk_pyspark_using_reducebykey.py

People You May Know (PYMK) -- the same algorithm and the same output
as pymk_pyspark_using_groupbykey.py in this folder, rewritten to use reduceByKey()
instead of groupByKey() for both aggregation steps. Read
pymk_pyspark_using_groupbykey.py first; this file exists to show *why* that script
chose groupByKey() (explained there in Section "reducing step needs
the whole list...") and what changes if you want reduceByKey()
instead.

------------------------------------------------------------------
WHY groupByKey() ISN'T A DROP-IN reduceByKey() HERE
------------------------------------------------------------------
reduceByKey(f) requires f to be an *associative, commutative* binary
function that combines two values of the SAME type into one value of
that type -- e.g. Word Count's `add` on two integers. Spark exploits
this by running f as a partial "combiner" on each partition BEFORE
the shuffle (mirroring a MapReduce combiner), so less data crosses
the network.

pymk_pyspark_using_groupbykey.py's mapper emits raw, heterogeneous-looking values --
the string "FRIEND" (Rule 1) or a person id (Rule 2) -- and its
reduce step (reduce_group) needs the *entire* list of them before it
can decide anything. There's no way to combine two individual string
values into a third string that preserves enough information, so
that script uses groupByKey() and reduces after the shuffle.

To use reduceByKey() instead, the mapper here emits a small STATE
object per record instead of a raw string -- a
(is_friend: bool, mutual_friend_names: frozenset[str]) pair -- and
defines an associative, commutative merge over that state:

    combine_pair_states((is_friend_a, names_a), (is_friend_b, names_b)):
        is_friend = is_friend_a or is_friend_b
        if is_friend:
            return (True, frozenset())   # <-- see below
        return (False, names_a | names_b)

This is associative (order of combining doesn't matter) and
commutative (which operand is "a" vs "b" doesn't matter), so
reduceByKey() can apply it as a map-side combiner. Two states with
is_friend=True or a mix always reduce to (True, frozenset()) --
notice the accumulated names are DISCARDED the moment a FRIEND tag is
seen, before ever crossing the network, because a pair known to
already be friends is going to be dropped from the output anyway and
its mutual-friend names are useless. That is a genuine efficiency
win reduceByKey() gives you here that groupByKey() cannot: for
already-friend pairs (typically the majority of pairs in a real
graph), the shuffle carries a constant-size (True, frozenset())
instead of the full, potentially long, list of vouching names.

The same idea is applied a second time for the final "rank each
person's candidates" step: instead of groupByKey().mapValues(sort),
each candidate starts life as a one-element ranked list and
reduceByKey() merges two (already truncated, already sorted) lists
with a "merge two top-K lists into one top-K list" combiner -- an
associative, commutative operation that, when --top-k is set, keeps
the per-key state bounded to K elements throughout the shuffle
instead of collecting every candidate before truncating.

See the companion documents for the underlying algorithm and the
groupByKey()-based implementation this file is an alternative to:

    slides/mapreduce/mapreduce_examples/MapReduce_People_You_May_Know.md
    pymk_pyspark_using_groupbykey.py                      (this folder)

Usage:
    spark-submit pymk_pyspark_using_reducebykey.py <input_file> [--top-k K] [--output OUT_DIR]

Run it locally with:
    python3 pymk_pyspark_using_reducebykey.py data/friends.txt

Or, on the larger 12-person sample graph, keeping only each person's
top-2 recommendations:
    python3 pymk_pyspark_using_reducebykey.py data/friends_larger.txt --top-k 2
"""

from __future__ import annotations

import argparse
import sys
from itertools import combinations
from typing import Iterable, Iterator

from pyspark.sql import SparkSession

Pair = tuple[str, str]
# (is_friend, mutual_friend_names) -- the associative, combinable state
# emitted per (pair, ...) record and merged by combine_pair_states().
PairState = tuple[bool, frozenset[str]]
# (candidate, mutual_count, mutual_friends) -- one ranked recommendation.
Candidate = tuple[str, int, list[str]]


def sort_pair(a: str, b: str) -> Pair:
    """Return (a, b) in a fixed, comparable order.

    This is what makes (X, Y) and (Y, X) collapse to the same key
    during reduceByKey() -- the Spark equivalent of a MapReduce
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


def rule1_friend_tags(record: tuple[str, list[str]]) -> Iterator[tuple[Pair, PairState]]:
    """Mapper Rule 1: tag every existing (person, friend) edge.

    Emits state (True, frozenset()) -- 'this pair is already
    friends' -- with no names attached, since a friend pair's mutual
    friends are never reported.
    """
    person, friends = record
    for friend in friends:
        yield sort_pair(person, friend), (True, frozenset())


def rule2_mutual_vouches(record: tuple[str, list[str]]) -> Iterator[tuple[Pair, PairState]]:
    """Mapper Rule 2: every unordered pair drawn from person P's own
    friend list shares P as a mutual friend, whether or not that pair
    is friends with each other -- so P "vouches" for every such pair.

    Emits state (False, frozenset({P})) -- 'not (yet) known to be a
    friend pair, and P is one mutual friend'.
    """
    person, friends = record
    for x, y in combinations(sorted(friends), 2):
        yield sort_pair(x, y), (False, frozenset({person}))


def combine_pair_states(a: PairState, b: PairState) -> PairState:
    """The reduceByKey() combiner for one pair's states.

    Associative and commutative, so Spark can run it as a map-side
    (per-partition) combiner before the shuffle, exactly like a
    MapReduce combiner -- and unlike groupByKey(), which must ship
    every raw value across the network first.

    The moment either side is known to be a friend pair, the merged
    state drops any accumulated names: they will never be reported
    (the pair gets filtered out downstream), so there is nothing to
    gain by carrying them further through the shuffle.
    """
    a_is_friend, a_names = a
    b_is_friend, b_names = b
    if a_is_friend or b_is_friend:
        return True, frozenset()
    return False, a_names | b_names


def rank_candidates(
    candidates: Iterable[Candidate], top_k: int | None
) -> list[Candidate]:
    """Sort candidates by descending mutual_count (ties by name), and
    truncate to the top K if requested."""
    ranked = sorted(candidates, key=lambda c: (-c[1], c[0]))
    return ranked[:top_k] if top_k is not None else ranked


def merge_ranked_candidates(
    a: list[Candidate], b: list[Candidate], top_k: int | None
) -> list[Candidate]:
    """The reduceByKey() combiner for one person's ranked-candidate
    lists: merge two already-ranked lists into one ranked list.

    Associative and commutative (merging preserves the final sorted
    order regardless of how the inputs were grouped or ordered), so
    it too can run as a map-side combiner. When top_k is set, both
    inputs are already bounded to K elements and the merged result is
    re-truncated to K -- the per-key state never grows past K
    elements at any point in the shuffle, unlike
    groupByKey().mapValues(rank), which must collect every candidate
    for a person before truncating.
    """
    return rank_candidates(list(a) + list(b), top_k)


def build_recommendations(friends_rdd, top_k: int | None = None):
    """Run the full PYMK pipeline on an RDD of (person, [friends]),
    using reduceByKey() for both aggregation steps.

    Returns an RDD of (person, [(candidate, mutual_count,
    mutual_friends), ...]) -- the same shape as pymk_pyspark_using_groupbykey.py's
    build_recommendations(), one entry per person in the input graph.

    Every step below mirrors a phase of the MapReduce algorithm in
    the companion document, with reduceByKey() taking the place of
    groupByKey() + a post-shuffle reduce:
        flatMap (Rule 1) + flatMap (Rule 2)      -> the Map phase
        union().reduceByKey(combine_pair_states)  -> Shuffle + Reduce,
                                                      combined, with a
                                                      map-side combiner
    """
    edges = friends_rdd.flatMap(rule1_friend_tags)
    vouches = friends_rdd.flatMap(rule2_mutual_vouches)

    reduced = (
        edges.union(vouches)
        .reduceByKey(combine_pair_states)
        .filter(lambda kv: not kv[1][0])  # drop pairs known to be friends
        .map(
            lambda kv: (
                kv[0][0],
                kv[0][1],
                len(kv[1][1]),
                sorted(kv[1][1]),
            )
        )
    )
    # reduced: RDD[(person_a, person_b, mutual_count, mutual_friends)]
    # -- one row per non-friend PAIR, not yet duplicated per person.

    # Every pair recommends each endpoint to the other, so duplicate
    # each row with (a, b) swapped, each starting life as a
    # single-element ranked list, before reduceByKey() merges each
    # person's candidate lists together.
    symmetric = reduced.flatMap(
        lambda row: [
            (row[0], [(row[1], row[2], row[3])]),
            (row[1], [(row[0], row[2], row[3])]),
        ]
    )

    ranked_candidates = symmetric.reduceByKey(
        lambda a, b: merge_ranked_candidates(a, b, top_k)
    )

    # A person who is already friends with everyone else in the graph
    # (e.g. B and C in data/friends.txt) never emits a Rule-2 vouch
    # for themselves and so never appears as a key above. leftOuterJoin
    # against every known person restores them with an empty
    # candidate list instead of silently dropping them from the output.
    all_people = friends_rdd.map(lambda record: (record[0], None))
    return all_people.leftOuterJoin(ranked_candidates).mapValues(
        lambda joined: joined[1] if joined[1] is not None else []
    )


def format_recommendations(rows: list[tuple[str, list[Candidate]]]) -> str:
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
            "mutual-friend count, as a standalone PySpark RDD job "
            "using reduceByKey() instead of groupByKey()."
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

    spark = SparkSession.builder.appName("PeopleYouMayKnowReduceByKey").getOrCreate()
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

% python3 pymk_pyspark_using_reducebykey.py data/friends.txt
input_file= data/friends.txt
people_in_graph= 6

A: E (3 mutual: B/C/D), F (2 mutual: B/C)
B: (no recommendations)
C: (no recommendations)
D: F (2 mutual: B/C)
E: A (3 mutual: B/C/D), F (2 mutual: B/C)
F: A (2 mutual: B/C), D (2 mutual: B/C), E (2 mutual: B/C)

SAMPLE RUN (larger graph, top-2 recommendations per person):

% python3 pymk_pyspark_using_reducebykey.py data/friends_larger.txt --top-k 2
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

Both sample runs are byte-for-byte identical to pymk_pyspark_using_groupbykey.py's
output on the same inputs -- this file changes HOW the aggregation
is executed (reduceByKey with a map-side combiner instead of
groupByKey), not WHAT is computed.
"""
