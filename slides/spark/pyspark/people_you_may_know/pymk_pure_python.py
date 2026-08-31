#!/usr/bin/env python3
"""
pymk_pure_python.py

People You May Know (PYMK): for every pair of people who are NOT
already friends, find how many friends they have in common and who
those mutual friends are.

This is a dependency-free (no Spark, no NumPy -- standard library
only) simulation of the MapReduce "pairs" algorithm worked out in
detail in the companion write-up:

    slides/mapreduce/mapreduce_examples/MapReduce_People_You_May_Know.md

The point of this script is pedagogical: map_person(), shuffle(), and
reduce_pair() below are named and shaped after the three MapReduce
phases (Map, Shuffle & Sort, Reduce) so you can see, in plain Python
dict/list operations, exactly what a cluster would be doing with
intermediate files and network shuffles. See pymk_pyspark_using_groupbykey.py in this
same folder for the distributed (PySpark RDD) version of the same
algorithm.

------------------------------------------------------------------
THE ALGORITHM
------------------------------------------------------------------
Input:  Person -> [list of friends]              (an adjacency list)
Output: For every pair of people who are NOT already friends,
        the count and names of the friends they have in common.

Two "mapper rules" are applied to every (person, friends) record:

  Rule 1 -- tag existing edges.
      For each friend F of P, emit (sort_pair(P, F), "FRIEND").
      This records which pairs are already directly connected, so
      the reducer can throw them out -- recommending someone you are
      already friends with makes no sense.

  Rule 2 -- vouch for every pair of P's own friends.
      For every 2-combination (X, Y) drawn from P's own friend list,
      emit (sort_pair(X, Y), P).
      If P is friends with both X and Y, P is a mutual friend of X
      and Y -- whether or not X and Y are friends with each other.

The "shuffle" phase groups every emitted (pair, value) record by its
pair key -- exactly what happens between Map and Reduce on a real
cluster. The "reducer" then looks at each group: if it contains the
marker "FRIEND", the pair is already connected and is dropped;
otherwise, every value left is the name of a person who vouched for
that pair, i.e. a mutual friend, and the reducer reports how many
there are and who they are.

Usage:
    python3 pymk_pure_python.py <input_file> [--top-k K] [--output OUT.tsv]

Run it with:
    python3 pymk_pure_python.py data/friends.txt

Or, on the larger 12-person sample graph, keeping only each person's
top-2 recommendations:
    python3 pymk_pure_python.py data/friends_larger.txt --top-k 2
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from itertools import combinations
from pathlib import Path
from typing import Iterator

Pair = tuple[str, str]
PairResult = tuple[Pair, int, list[str]]


def sort_pair(a: str, b: str) -> Pair:
    """Return (a, b) in a fixed, comparable order.

    This is what makes (X, Y) and (Y, X) collapse to the same
    dictionary key during "shuffle" -- the equivalent of a MapReduce
    framework routing both to the same reducer because they hash to
    the same key.
    """
    return (a, b) if a < b else (b, a)


def read_graph(input_file: str | Path) -> dict[str, list[str]]:
    """Read an adjacency-list file into {person: [friend, ...]}.

    Expected format, one person per line:
        <person>,<friend_1>,<friend_2>,...,<friend_n>
    Blank lines and lines starting with '#' are ignored, so the
    sample data files can carry documentation as comments.
    """
    graph: dict[str, list[str]] = {}
    with Path(input_file).open(mode="r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            fields = [field.strip() for field in line.split(",")]
            person, friends = fields[0], fields[1:]
            graph[person] = friends
    return graph


def map_person(person: str, friends: list[str]) -> Iterator[tuple[Pair, str]]:
    """The 'mapper': apply Rule 1 and Rule 2 to one (person, friends) record.

    Yields (pair, value) tuples exactly like a MapReduce mapper's
    emit() calls. value is either the literal string "FRIEND"
    (Rule 1) or a person id who vouches for the pair (Rule 2).
    """
    # Rule 1: tag existing edges, so the reducer can suppress them.
    for friend in friends:
        yield sort_pair(person, friend), "FRIEND"

    # Rule 2: every unordered pair of P's own friends shares P as a
    # mutual friend, regardless of whether they are friends with
    # each other.
    for x, y in combinations(sorted(friends), 2):
        yield sort_pair(x, y), person


def shuffle(graph: dict[str, list[str]]) -> dict[Pair, list[str]]:
    """The 'shuffle & sort' phase: run every record through
    map_person() and group the emitted values by key (pair) -- the
    in-memory stand-in for a cluster's shuffle.
    """
    grouped: dict[Pair, list[str]] = defaultdict(list)
    for person, friends in graph.items():
        for pair, value in map_person(person, friends):
            grouped[pair].append(value)
    return grouped


def reduce_pair(pair: Pair, values: list[str]) -> PairResult | None:
    """The 'reducer': one call per grouped key.

    Drops the pair (returns None) if it was ever seen as an existing
    edge (a "FRIEND" value present in this group). Otherwise, every
    remaining value is a mutual friend's name -- return the pair, how
    many mutual friends it has, and their names, sorted.
    """
    if "FRIEND" in values:
        return None
    mutual_friends = sorted(values)
    return pair, len(mutual_friends), mutual_friends


def compute_pymk(graph: dict[str, list[str]]) -> list[PairResult]:
    """Run the full map -> shuffle -> reduce pipeline.

    Returns one (pair, mutual_count, mutual_friends) record per
    qualifying non-friend pair -- unordered, i.e. each pair appears
    once, not once per person.
    """
    grouped = shuffle(graph)
    results = []
    for pair, values in grouped.items():
        reduced = reduce_pair(pair, values)
        if reduced is not None:
            results.append(reduced)
    return results


def recommendations_by_person(
    graph: dict[str, list[str]],
    results: list[PairResult],
    top_k: int | None = None,
) -> dict[str, list[tuple[str, int, list[str]]]]:
    """Turn the symmetric pair results into a per-person view.

    A downstream "sort" pass -- in a real system, a second trivial
    MapReduce/Spark job, or (as here, since the result set is small)
    an in-memory sort -- ranks each person's candidates by descending
    mutual-friend count so the most relevant recommendation comes
    first. Ties break alphabetically by candidate name for
    deterministic output.

    Every person in the input graph appears in the returned dict,
    even if their candidate list ends up empty (fully embedded in a
    friend clique with no outside mutual-friend connections).

    Args:
        graph: the original adjacency list (used only to make sure
            people with zero candidates still show up in the output).
        results: the reducer's output from compute_pymk().
        top_k: if given, keep only each person's top K candidates.

    Returns:
        {person: [(candidate, mutual_count, mutual_friends), ...]},
        sorted by descending mutual_count then candidate name, keyed
        in alphabetical order of person.
    """
    by_person: dict[str, list[tuple[str, int, list[str]]]] = defaultdict(list)
    for person in graph:
        by_person[person] = []

    for (a, b), count, mutual in results:
        by_person[a].append((b, count, mutual))
        by_person[b].append((a, count, mutual))

    for person, candidates in by_person.items():
        candidates.sort(key=lambda item: (-item[1], item[0]))
        if top_k is not None:
            by_person[person] = candidates[:top_k]

    return dict(sorted(by_person.items()))


def format_recommendations(
    recommendations: dict[str, list[tuple[str, int, list[str]]]],
) -> str:
    """Render the per-person recommendation dict as readable text."""
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


def write_tsv(
    output_path: str | Path,
    recommendations: dict[str, list[tuple[str, int, list[str]]]],
) -> int:
    """Write one row per (person, candidate) recommendation to a TSV
    file: person, candidate, mutual_count, mutual_friends (comma
    joined). Returns the number of rows written.
    """
    rows_written = 0
    with Path(output_path).open(mode="w", encoding="utf-8") as handle:
        for person, candidates in recommendations.items():
            for candidate, count, mutual in candidates:
                handle.write(f"{person}\t{candidate}\t{count}\t{','.join(mutual)}\n")
                rows_written += 1
    return rows_written


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "People You May Know: recommend non-friends ranked by "
            "mutual-friend count, using a pure-Python simulation of "
            "the MapReduce pairs algorithm."
        )
    )
    parser.add_argument(
        "input_file",
        type=Path,
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
        type=Path,
        default=None,
        help="optional path to also write a <person><TAB><candidate>"
        "<TAB><count><TAB><mutual_friends> TSV file",
    )
    return parser.parse_args()


def main() -> int:
    """Run the command-line program."""
    args = parse_arguments()

    try:
        print("input_file=", args.input_file)
        graph = read_graph(args.input_file)
    except OSError as error:
        print(f"Error: unable to read {args.input_file}: {error}")
        return 1

    print(f"people_in_graph= {len(graph)}")

    results = compute_pymk(graph)
    print(f"non_friend_pairs_with_mutual_friends= {len(results)}")

    recommendations = recommendations_by_person(graph, results, top_k=args.top_k)
    if args.top_k is not None:
        print(f"top_k= {args.top_k}")

    print()
    print(format_recommendations(recommendations))

    if args.output is not None:
        rows_written = write_tsv(args.output, recommendations)
        print()
        print(f"Wrote {rows_written} recommendation rows to {args.output}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
SAMPLE RUN:

% python3 pymk_pure_python.py data/friends.txt
input_file= data/friends.txt
people_in_graph= 6
non_friend_pairs_with_mutual_friends= 4

A: E (3 mutual: B/C/D), F (2 mutual: B/C)
B: (no recommendations)
C: (no recommendations)
D: F (2 mutual: B/C)
E: A (3 mutual: B/C/D), F (2 mutual: B/C)
F: A (2 mutual: B/C), D (2 mutual: B/C), E (2 mutual: B/C)

SAMPLE RUN (larger graph, top-2 recommendations per person):

% python3 pymk_pure_python.py data/friends_larger.txt --top-k 2
input_file= data/friends_larger.txt
people_in_graph= 12
non_friend_pairs_with_mutual_friends= 19
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
"""
