#!/usr/bin/env python3
"""
A Real-Life Case for foldByKey(): Election-Night Vote Totals
=============================================================

The scenario
------------
On election night, every precinct (a neighborhood polling place) sends its
TALLY to the state election office: how many votes each candidate got
there. A tally is a dictionary {candidate: votes}. The office publishes a
running total for every county:

    Santa Clara   Patel 1,385   Lee 1,235   Rivera 348   write-in 4

Each report is a (county, tally) pair. A precinct only lists the candidates
who got votes there, so some tallies have fewer names (or an extra
'write-in'). (All vote counts in this example are made up.)

    Input:  one record per precinct  (county, {candidate: votes})
    Output: one record per county    (county, {candidate: votes})
            = the precinct tallies added up, candidate by candidate

Why this is a job for foldByKey()
---------------------------------
  1. The value and the result have the SAME TYPE (a tally), and one rule
     combines any two tallies: add the votes candidate by candidate.
  2. There is a natural EMPTY tally: {} -- "no votes counted yet", exactly
     what every county shows on TV before the first precinct reports.

foldByKey(zeroValue, func) is reduceByKey() PLUS a starting value:

    Business rule                               foldByKey() argument  Type
    ------------------------------------------  --------------------  ----------
    The empty tally, before any votes counted   zeroValue             V
    Add two tallies, candidate by candidate     func                  (V, V) -> V

What this program shows
-----------------------
  1. the sample data and its 2 partitions
  2. the foldByKey() solution, and the statewide total with RDD.fold()
  3. why add_tallies may safely change its first argument (with foldByKey)
  4. the alternatives (reduceByKey, aggregateByKey, groupByKey)
  5. a common mistake: a zeroValue that is not empty (a "rigged" election)

How to run
----------
    $SPARK_HOME/bin/spark-submit foldbykey_election_results.py

or, if PySpark is installed in your Python (pip install pyspark):

    python3 foldbykey_election_results.py

The output of a real run is in foldbykey_election_results.log.
The full write-up is in foldbykey_election_results.md.
"""
import copy
from functools import reduce

from pyspark.sql import SparkSession


def section(title):
    """Print a section header, to make the output easy to follow."""
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


# -------------------------------------------------------------------------
# The one business rule: add two tallies, candidate by candidate. (V, V) -> V
# -------------------------------------------------------------------------
def add_tallies(tally1, tally2):
    """Add tally2 into tally1, candidate by candidate, and return tally1.

    With foldByKey(), changing tally1 IN PLACE is safe: tally1 is always a
    fresh copy of the zeroValue (or a running total built from it), never a
    precinct's own report. tally2 must never be changed.
    """
    for candidate, votes in tally2.items():
        tally1[candidate] = tally1.get(candidate, 0) + votes
    return tally1


def new_total(tally1, tally2):
    """Add two tallies WITHOUT changing either one (safe for reduceByKey())."""
    return {c: tally1.get(c, 0) + tally2.get(c, 0)
            for c in tally1.keys() | tally2.keys()}


def show(name, tally):
    """Print a tally as on TV: candidates from most to fewest votes."""
    ranked = sorted(tally.items(), key=lambda cv: (-cv[1], cv[0]))
    print(f"{name:12}", "   ".join(f"{c} {v:,}" for c, v in ranked))


def main():
    spark = SparkSession.builder.appName("foldByKey-election-results").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    print("Spark version:", sc.version)

    # ---------------------------------------------------------------------
    section("1. Sample data: (county, {candidate: votes})")
    # ---------------------------------------------------------------------
    reports = [
        ('Santa Clara', {'Lee': 410, 'Patel': 385, 'Rivera': 120}),
        ('Alameda',     {'Lee': 700, 'Patel': 640, 'Rivera': 300}),
        ('Santa Clara', {'Lee': 530, 'Patel': 760, 'Rivera': 140}),
        ('Alameda',     {'Lee': 510, 'Patel': 560, 'Rivera': 210, 'write-in': 7}),
        ('Santa Clara', {'Lee': 295, 'Patel': 240, 'Rivera': 88, 'write-in': 4}),
        ('San Mateo',   {'Lee': 220, 'Patel': 260, 'Rivera': 95}),
        ('San Mateo',   {'Lee': 180, 'Patel': 150, 'Rivera': 60}),
        ('San Mateo',   {'Lee': 140, 'Patel': 300}),
    ]

    # 2 partitions, so the output is the same on every machine.
    # reports_rdd : RDD[(county, tally)]
    reports_rdd = sc.parallelize(reports, 2)
    for i, part in enumerate(reports_rdd.glom().collect()):
        print("partition", i, ":", [county for county, tally in part])
    print("Santa Clara has reports in BOTH partitions; Alameda only in")
    print("partition 0, and San Mateo only in partition 1.")

    # ---------------------------------------------------------------------
    section("2. The foldByKey() solution: one empty tally, one function")
    # ---------------------------------------------------------------------
    # county_totals : RDD[(county, {candidate: votes})]
    county_totals = reports_rdd.foldByKey({}, add_tallies)
    for county, tally in sorted(county_totals.collect()):
        show(county, tally)

    # How Santa Clara is folded. EACH PARTITION STARTS FROM ITS OWN COPY of
    # the empty tally:
    #   partition 0: {} + {Lee: 410, ...} + {Lee: 530, ...}
    #                -> {Lee: 940, Patel: 1145, Rivera: 260}
    #   partition 1: {} + {Lee: 295, Patel: 240, Rivera: 88, write-in: 4}
    #   after the shuffle: add the two partial tallies
    #                -> {Lee: 1235, Patel: 1385, Rivera: 348, write-in: 4}

    # Bonus: the statewide total, with the SAME zero and the SAME function.
    # RDD.fold() is the version without keys: it folds all values into one.
    print()
    statewide = county_totals.values().fold({}, add_tallies)
    show("Statewide", statewide)

    # ---------------------------------------------------------------------
    section("3. Why add_tallies may safely change its first argument")
    # ---------------------------------------------------------------------
    # Inside RDD.foldByKey() ($SPARK_HOME/python/pyspark/core/rdd.py),
    # simplified:
    #     def createZero():
    #         return copy.deepcopy(zeroValue)
    #     return self.combineByKey(lambda v: func(createZero(), v), func, func)
    # The FIRST value of a key in a partition is added to a FRESH COPY of the
    # empty tally, so tally1 is never a precinct's own report.
    #
    # With reduceByKey(), the first report of a county in a partition
    # BECOMES the running total (createCombiner is lambda x: x), so
    # reduceByKey(add_tallies) adds the other precincts' votes INTO that
    # precinct's own report object.
    #
    # Python's functools.reduce() shows the difference: without an initial
    # value it works like reduceByKey(), with one like foldByKey().
    # (We use a deep copy of Santa Clara's reports, so that the original
    # data stays intact for the rest of the program.)
    santa_clara = copy.deepcopy([tally for county, tally in reports
                                 if county == 'Santa Clara'])
    print("the first precinct's report:", santa_clara[0])

    print("like foldByKey()   :", reduce(add_tallies, santa_clara, {}))
    print("   first report now:", santa_clara[0], " <- unchanged")

    print("like reduceByKey() :", reduce(add_tallies, santa_clara))
    print("   first report now:", santa_clara[0], " <- overwritten!")
    print("Same answer both times, but the second call quietly destroyed the")
    print("first precinct's report. With reduceByKey(), build a NEW tally.")

    # ---------------------------------------------------------------------
    section("4. Why not reduceByKey(), aggregateByKey(), or groupByKey()?")
    # ---------------------------------------------------------------------
    # reduceByKey(): correct, but must build a brand-new dictionary in every
    # call, and there is no "no votes yet" starting point in the code.
    totals2 = reports_rdd.reduceByKey(new_total)
    print("reduceByKey(new_total)                       same answer?",
          sorted(totals2.collect()) == sorted(county_totals.collect()))

    # aggregateByKey(): the same function written twice (seqFunc and
    # combFunc), because the value and the result have the same type.
    totals3 = reports_rdd.aggregateByKey({}, add_tallies, add_tallies)
    print("aggregateByKey({}, add_tallies, add_tallies) same answer?",
          sorted(totals3.collect()) == sorted(county_totals.collect()))

    # groupByKey(): ships every precinct report over the network and holds
    # all reports of a county in memory, only to add them up anyway.
    totals4 = reports_rdd.groupByKey().mapValues(
        lambda tallies: reduce(add_tallies, tallies, {}))
    print("groupByKey().mapValues(...)                  same answer?",
          sorted(totals4.collect()) == sorted(county_totals.collect()))

    # ---------------------------------------------------------------------
    section("5. A common mistake: a zeroValue that is not empty")
    # ---------------------------------------------------------------------
    # The zeroValue is used ONCE PER KEY PER PARTITION. Suppose a careless
    # (or dishonest!) programmer gives Lee a "head start" of 100 votes:
    rigged = reports_rdd.foldByKey({'Lee': 100}, add_tallies)
    for county, tally in sorted(rigged.collect()):
        show(county, tally)
    print()
    print("Alameda and San Mateo (reports in one partition each) gave Lee 100")
    print("extra votes. Santa Clara (reports in two partitions) gave 200, and")
    print("that flipped the result: Lee now 'wins' Santa Clara, which Patel")
    print("really won by 150 votes. The zeroValue must be truly empty: {}.")

    spark.stop()


if __name__ == '__main__':
    main()
