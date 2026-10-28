#!/usr/bin/env python3
"""
Package Tracking: DataFrame Version
====================================

The DataFrame companion to reducebykey_package_tracking.py (same scans).

The scenario
------------
A delivery company scans every package many times on its way. The "Track
your package" page shows the CURRENT status: the scan with the LATEST
timestamp. Scanners upload in batches, so the scans arrive out of order.

    Input:  one row per scan     (package_id, scan_time, location, status)
    Output: one row per package  (package_id, scan_time, location, status)
            = the row with the latest scan_time

From reduceByKey() to DataFrames
--------------------------------
With RDDs we wrote the business rule as a function for reduceByKey(). With
DataFrames we pick a built-in aggregate that already does what the rule says:

    Business rule                     RDD                         DataFrame
    --------------------------------  --------------------------  ------------------------------------
    Of two scans, keep the later one  reduceByKey(later_scan)     max(struct(scan_time, location, status))
                                      reduceByKey(max)            or max_by(column, scan_time)

The trick is the same in both APIs: put the timestamp FIRST in a tuple (RDD)
or a struct (DataFrame), and the largest one is the latest scan. And Spark
plans the built-in max() like reduceByKey(): a partial max in each partition
(partial_max), a shuffle, then the final max.

What this program shows
-----------------------
  1. the sample data
  2. the solution with max() of a struct, and with max_by()
  3. the plan: partial_max before the shuffle, like reduceByKey()
  4. the status dashboard
  5. other ways: a row_number() window, and Spark SQL
  6. a common mistake: max() of every column (a scan that never happened)

How to run
----------
    $SPARK_HOME/bin/spark-submit reducebykey_package_tracking_dataframe.py

or, if PySpark is installed in your Python (pip install pyspark):

    python3 reducebykey_package_tracking_dataframe.py

The output of a real run is in reducebykey_package_tracking_dataframe.log.
The full write-up is in reducebykey_package_tracking_dataframe.md.
"""
import contextlib
import io

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def section(title):
    """Print a section header, to make the output easy to follow."""
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


def plan_of(df):
    """Return the physical plan of a DataFrame as text (instead of printing it)."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain()
    return buf.getvalue()


def main():
    spark = SparkSession.builder.appName("package-tracking-dataframe").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print("Spark version:", spark.version)

    # ---------------------------------------------------------------------
    section("1. Sample data: one row per scan")
    # ---------------------------------------------------------------------
    scans = [
        ('PKG-1001', '2026-10-21 06:05', 'Santa Clara, CA', 'out for delivery'),
        ('PKG-1002', '2026-10-22 03:10', 'Portland, OR',    'in transit'),
        ('PKG-1001', '2026-10-20 08:15', 'San Jose, CA',    'picked up'),
        ('PKG-1003', '2026-10-22 11:45', 'Austin, TX',      'picked up'),
        ('PKG-1001', '2026-10-21 14:32', 'Santa Clara, CA', 'delivered'),
        ('PKG-1002', '2026-10-21 09:00', 'Seattle, WA',     'picked up'),
        ('PKG-1003', '2026-10-22 18:20', 'Dallas, TX',      'in transit'),
        ('PKG-1001', '2026-10-20 19:40', 'Oakland, CA',     'in transit'),
    ]

    # The timestamps arrive as text: convert them to a real timestamp column.
    scans_df = (spark.createDataFrame(
                    scans, "package_id STRING, scan_time STRING, location STRING, status STRING")
                .withColumn("scan_time", F.to_timestamp("scan_time")))
    scans_df.printSchema()
    scans_df.show(truncate=False)

    # ---------------------------------------------------------------------
    section("2. The solution: max() of a struct, and max_by()")
    # ---------------------------------------------------------------------
    # F.struct() packs several columns into one value, like a Python tuple.
    # Structs are compared field by field, starting with the first one, so
    # the LARGEST (scan_time, location, status) struct is the LATEST scan.
    # "latest.*" unpacks the struct back into columns.
    # This is the DataFrame twin of scans_rdd.reduceByKey(max).
    current = (scans_df
               .groupBy("package_id")
               .agg(F.max(F.struct("scan_time", "location", "status")).alias("latest"))
               .select("package_id", "latest.*"))
    print("max(struct(scan_time, location, status)):")
    # after a groupBy() the row order is not guaranteed: orderBy() before show()
    current.orderBy("package_id").show(truncate=False)

    # F.max_by(col, ord): the value of col from the row where ord is largest.
    # It reads like the business rule: "the status AT the latest scan time".
    # (With a tie on scan_time, max_by() may return either row; max() of a
    # struct then compares the next field, so it never depends on row order.)
    current2 = scans_df.groupBy("package_id").agg(
        F.max("scan_time").alias("scan_time"),
        F.max_by("location", "scan_time").alias("location"),
        F.max_by("status", "scan_time").alias("status"))
    print("max_by(column, scan_time):")
    current2.orderBy("package_id").show(truncate=False)

    # ---------------------------------------------------------------------
    section("3. What Spark does with it: the reduceByKey() plan")
    # ---------------------------------------------------------------------
    # Read from the bottom up, the plan has:
    #   partial_max                -> the latest scan per package, inside
    #                                 each partition (like later_scan)
    #   Exchange hashpartitioning  -> the shuffle, by package_id
    #   max                        -> merge the partial results
    # Only ONE scan per package per partition crosses the network.
    plan = plan_of(current)
    print("'partial_max' in plan:              ", "partial_max" in plan)
    print("'Exchange hashpartitioning' in plan:", "Exchange hashpartitioning" in plan)

    # ---------------------------------------------------------------------
    section("4. Bonus: the status dashboard")
    # ---------------------------------------------------------------------
    # With RDDs: a second reduceByKey() (word count). Here: a second groupBy().
    current.groupBy("status").count().orderBy("status").show()

    # ---------------------------------------------------------------------
    section("5. Other ways: a row_number() window, and Spark SQL")
    # ---------------------------------------------------------------------
    # Rank each package's scans from newest to oldest, keep rank 1. It keeps
    # every column without listing them, but it shuffles and sorts EVERY
    # scan first (like groupByKey().mapValues(max)). Use it when you need
    # the latest N rows (rank <= 3); for the latest row, max()/max_by().
    newest_first = Window.partitionBy("package_id").orderBy(F.desc("scan_time"))
    current3 = (scans_df
                .withColumn("rank", F.row_number().over(newest_first))
                .filter("rank = 1")
                .drop("rank"))
    print("row_number() over a window:")
    current3.orderBy("package_id").show(truncate=False)

    scans_df.createOrReplaceTempView("scans")
    print("Spark SQL:")
    spark.sql("""
        SELECT package_id,
               max(scan_time)                AS scan_time,
               max_by(location, scan_time)   AS location,
               max_by(status, scan_time)     AS status
        FROM scans
        GROUP BY package_id
        ORDER BY package_id
    """).show(truncate=False)

    # ---------------------------------------------------------------------
    section("6. A common mistake: the max() of every column")
    # ---------------------------------------------------------------------
    # Each max() runs ON ITS OWN COLUMN: max("status") is the alphabetically
    # largest status ('picked up'), not the status of the latest scan. The
    # three answers come from three DIFFERENT rows, glued together.
    wrong = scans_df.groupBy("package_id").agg(
        F.max("scan_time").alias("scan_time"),
        F.max("location").alias("location"),      # WRONG
        F.max("status").alias("status"))          # WRONG
    wrong.orderBy("package_id").show(truncate=False)
    print("Every package is now 'picked up', and PKG-1002 is in Seattle at")
    print("03:10 on 10-22: a scan that never happened (it was in Portland).")
    print("Columns that must come from the SAME row must travel together: in")
    print("one struct, with max_by(), or with a window. (Similarly, F.last()")
    print("is 'the last one wins': non-deterministic after a shuffle.)")

    spark.stop()


if __name__ == '__main__':
    main()
