#!/usr/bin/env python3
"""
A Real-Life Case for reduceByKey(): Package Tracking
=====================================================

The scenario
------------
A delivery company scans every package many times on its way: when it is
picked up, at every warehouse, when it goes out for delivery, and when it is
delivered. The "Track your package" web page shows the CURRENT status:

    PKG-1001   delivered   Santa Clara, CA   (2026-10-21 14:32)

Every scan is a (package_id, (timestamp, location, status)) pair. Scanners
upload their scans in batches, so the log is NOT in time order, and one
package's scans are spread over many partitions. We must compare timestamps,
not trust the arrival order.

    Input:  one record per scan     (package_id, (timestamp, location, status))
    Output: one record per package  (package_id, (timestamp, location, status))
            = the scan with the LATEST timestamp

Why this is a job for reduceByKey()
-----------------------------------
The result for a package is ONE SCAN: exactly the same type as each input
value. And one business rule combines any two scans, in any order:

    Business rule                        reduceByKey() argument   Type
    -----------------------------------  -----------------------  ----------
    Of two scans, keep the later one     func                     (V, V) -> V

The rule also works no matter how the scans are split up: the later of two
partial "latest scans" is the true latest scan. Spark relies on exactly this
to reduce each partition first and then merge the partial results.

What this program shows
-----------------------
  1. the sample data and its 2 partitions
  2. the reduceByKey() solution (and the even shorter reduceByKey(max))
  3. a bonus status dashboard, with reduceByKey() again (word-count pattern)
  4. the alternatives (groupByKey, aggregateByKey, combineByKey): same
     answer, more awkward
  5. a common mistake: "the last one wins"

How to run
----------
    $SPARK_HOME/bin/spark-submit reducebykey_package_tracking.py

or, if PySpark is installed in your Python (pip install pyspark):

    python3 reducebykey_package_tracking.py

The output of a real run is in reducebykey_package_tracking.log.
The full write-up is in reducebykey_package_tracking.md.
"""
from functools import reduce

from pyspark.sql import SparkSession


def section(title):
    """Print a section header, to make the output easy to follow."""
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


# -------------------------------------------------------------------------
# The one business rule: of two scans, keep the later one. (V, V) -> V
# -------------------------------------------------------------------------
def later_scan(scan1, scan2):
    """Return the scan with the later timestamp.

    A scan is (timestamp, location, status). Timestamps are written as
    'YYYY-MM-DD HH:MM' strings, so comparing two of them as strings gives
    the correct time order.
    """
    return scan1 if scan1[0] >= scan2[0] else scan2    # scan[0] is the timestamp


def last_wins(scan1, scan2):
    """WRONG: assumes the scans arrive in time order. They don't."""
    return scan2


def main():
    spark = SparkSession.builder.appName("reduceByKey-package-tracking").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    print("Spark version:", sc.version)

    # ---------------------------------------------------------------------
    section("1. Sample data: (package_id, (timestamp, location, status))")
    # ---------------------------------------------------------------------
    scans = [
        ('PKG-1001', ('2026-10-21 06:05', 'Santa Clara, CA', 'out for delivery')),
        ('PKG-1002', ('2026-10-22 03:10', 'Portland, OR',    'in transit')),
        ('PKG-1001', ('2026-10-20 08:15', 'San Jose, CA',    'picked up')),
        ('PKG-1003', ('2026-10-22 11:45', 'Austin, TX',      'picked up')),
        ('PKG-1001', ('2026-10-21 14:32', 'Santa Clara, CA', 'delivered')),
        ('PKG-1002', ('2026-10-21 09:00', 'Seattle, WA',     'picked up')),
        ('PKG-1003', ('2026-10-22 18:20', 'Dallas, TX',      'in transit')),
        ('PKG-1001', ('2026-10-20 19:40', 'Oakland, CA',     'in transit')),
    ]

    # 2 partitions, so the output is the same on every machine.
    # scans_rdd : RDD[(package_id, (timestamp, location, status))]
    scans_rdd = sc.parallelize(scans, 2)
    for i, part in enumerate(scans_rdd.glom().collect()):
        print("partition", i)
        for scan in part:
            print("   ", scan)
    print("Every package has scans in BOTH partitions.")

    # ---------------------------------------------------------------------
    section("2. The reduceByKey() solution: the current scan per package")
    # ---------------------------------------------------------------------
    # current : RDD[(package_id, (timestamp, location, status))]
    current = scans_rdd.reduceByKey(later_scan)
    for package, (time, location, status) in sorted(current.collect()):
        print(f"{package}   {status:<16} {location:<16} ({time})")

    # An even shorter version: Python compares tuples element by element,
    # starting with the first one. Each scan STARTS with the timestamp, so
    # the built-in max() already picks the later scan (and breaks ties by
    # location, then status, so the answer never depends on the order).
    print()
    print("Same answer with reduceByKey(max)?",
          sorted(scans_rdd.reduceByKey(max).collect()) == sorted(current.collect()))

    # How PKG-1001 is reduced (2 scans in each partition):
    #   partition 0: 10-21 06:05 out for delivery, 10-20 08:15 picked up
    #                -> keep 10-21 06:05 out for delivery
    #   partition 1: 10-21 14:32 delivered, 10-20 19:40 in transit
    #                -> keep 10-21 14:32 delivered
    #   after the shuffle: 06:05 vs 14:32 -> 10-21 14:32 delivered
    # Only ONE scan per package per partition crosses the network.

    # ---------------------------------------------------------------------
    section("3. Bonus: a status dashboard, with reduceByKey() again")
    # ---------------------------------------------------------------------
    # How many packages are in each status right now? The word-count
    # pattern: (status, 1) for every package, then add the 1s per status.
    dashboard = (current
                 .map(lambda kv: (kv[1][2], 1))     # kv[1][2] is the status
                 .reduceByKey(lambda a, b: a + b))
    print(sorted(dashboard.collect()))

    # ---------------------------------------------------------------------
    section("4. Why not groupByKey(), aggregateByKey(), or combineByKey()?")
    # ---------------------------------------------------------------------
    # groupByKey(): ships EVERY scan over the network and keeps them all in
    # memory, just to throw away all of them except one.
    current2 = scans_rdd.groupByKey().mapValues(max)
    print("groupByKey().mapValues(max)    same answer?",
          sorted(current2.collect()) == sorted(current.collect()))

    # aggregateByKey(): needs a fake "empty" scan from the year 0000 that
    # does not exist in the business, and seqFunc == combFunc (a sign that
    # the value and the result are the same type).
    no_scan = ('0000-00-00 00:00', '', '')     # "earlier than every real scan"
    current3 = scans_rdd.aggregateByKey(no_scan, later_scan, later_scan)
    print("aggregateByKey(no_scan, ...)   same answer?",
          sorted(current3.collect()) == sorted(current.collect()))

    # combineByKey(): three functions, two of them identical, one doing
    # nothing. This is EXACTLY how PySpark defines reduceByKey()
    # ($SPARK_HOME/python/pyspark/core/rdd.py):
    #     return self.combineByKey(lambda x: x, func, func, ...)
    current4 = scans_rdd.combineByKey(lambda scan: scan, later_scan, later_scan)
    print("combineByKey(identity, f, f)   same answer?",
          sorted(current4.collect()) == sorted(current.collect()))

    # ---------------------------------------------------------------------
    section("5. A common mistake: \"the last one wins\"")
    # ---------------------------------------------------------------------
    # Spark promises no order: inside a partition, values are combined in
    # the order they happen to be stored, and after the shuffle, partial
    # results are merged in whatever order they arrive. A function given to
    # reduceByKey() must give the same answer for EVERY order (it must be
    # associative and commutative). Python's functools.reduce() shows the
    # problem on the 4 scans of PKG-1001, in two different orders:
    pkg_1001 = [scan for package, scan in scans if package == 'PKG-1001']
    print("last_wins,  log order:     ", reduce(last_wins, pkg_1001))
    print("last_wins,  another order: ", reduce(last_wins, reversed(pkg_1001)))
    print("later_scan, log order:     ", reduce(later_scan, pkg_1001))
    print("later_scan, another order: ", reduce(later_scan, reversed(pkg_1001)))
    print()
    print("last_wins says 'in transit' (or 'out for delivery'), when the package")
    print("was delivered a day ago. later_scan compares the timestamps, so the")
    print("order doesn't matter.")

    spark.stop()


if __name__ == '__main__':
    main()
