#!/usr/bin/env python3
"""
Election-Night Vote Totals: DataFrame Version
==============================================

The DataFrame companion to foldbykey_election_results.py (same reports).

The scenario
------------
Every precinct sends its TALLY, {candidate: votes}, to the state election
office, which publishes a running total for every county:

    Santa Clara   Patel 1,385   Lee 1,235   Rivera 348   write-in 4

A precinct only lists the candidates who got votes there. (All vote counts
in this example are made up.)

    Input:  one row per precinct  (county, tally MAP<STRING, INT>)
    Output: the county's tally    (county, candidate, votes)

From foldByKey() to DataFrames
------------------------------
With RDDs, a tally was a dictionary, and add_tallies added two dictionaries
candidate by candidate, starting from the empty tally {}. DataFrames have no
built-in aggregate that "adds two dictionaries". The DataFrame way is to
CHANGE THE SHAPE of the data: one row per (county, candidate, votes). Then
"add the tallies candidate by candidate" is just a sum():

    Business rule                        RDD               DataFrame
    -----------------------------------  ----------------  ------------------------------------
    The empty tally (no votes yet)       zeroValue = {}    nothing to write: no rows = no votes
    Add two tallies, candidate by        add_tallies       explode() the tally into rows, then
      candidate                                            groupBy("county", "candidate")
                                                             .agg(sum("votes"))

A long table (one row per fact) is easier to aggregate than a column of
dictionaries.

What this program shows
-----------------------
  1. the sample data (a MAP column)
  2. step 1: explode() each tally into one row per candidate
  3. step 2: groupBy().agg(sum()), and the TV view with pivot()
  4. the statewide total, and every level at once with rollup()
  5. keeping the dictionaries: aggregate() over collect_list() (a literal
     foldByKey(), but it collects every tally like groupByKey())
  6. a common mistake: NULL is not zero

How to run
----------
    $SPARK_HOME/bin/spark-submit foldbykey_election_results_dataframe.py

or, if PySpark is installed in your Python (pip install pyspark):

    python3 foldbykey_election_results_dataframe.py

The output of a real run is in foldbykey_election_results_dataframe.log.
The full write-up is in foldbykey_election_results_dataframe.md.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def section(title):
    """Print a section header, to make the output easy to follow."""
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


def add_tallies(t1, t2):
    """Column version of add_tallies: add two map columns key by key.

    map_zip_with() visits every key of both maps; a key missing from one
    map gives NULL there, hence the coalesce(..., 0).
    """
    return F.map_zip_with(t1, t2,
                          lambda k, v1, v2: F.coalesce(v1, F.lit(0)) + F.coalesce(v2, F.lit(0)))


def main():
    spark = SparkSession.builder.appName("election-results-dataframe").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print("Spark version:", spark.version)

    # ---------------------------------------------------------------------
    section("1. Sample data: (county, tally MAP<STRING, INT>)")
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

    # MAP<STRING, INT> is Spark's type for a dictionary. (We don't show() the
    # raw map column: Spark doesn't promise an order for a map's entries.)
    reports_df = spark.createDataFrame(reports, "county STRING, tally MAP<STRING, INT>")
    reports_df.printSchema()
    print("precinct reports:", reports_df.count())

    # ---------------------------------------------------------------------
    section("2. Step 1: one row per candidate per precinct, with explode()")
    # ---------------------------------------------------------------------
    # explode() on a map column turns EACH ENTRY into its own row, with two
    # new columns: the key (candidate) and the value (votes).
    votes = reports_df.select("county", F.explode("tally").alias("candidate", "votes"))
    votes.printSchema()
    print("rows after explode():", votes.count())
    print("Santa Clara's rows:")
    votes.filter("county = 'Santa Clara'").orderBy("candidate", "votes").show()
    print("A candidate with no votes in a precinct simply has NO ROW: that is")
    print("the DataFrame's 'empty tally'.")

    # ---------------------------------------------------------------------
    section("3. Step 2: add up the votes, with groupBy().agg(sum())")
    # ---------------------------------------------------------------------
    # sum() is combined per partition before the shuffle (partial_sum), just
    # like foldByKey(), and its "zero" is built in: nobody can pass it a
    # wrong starting value (no "rigged" {'Lee': 100} zero is possible).
    county_totals = (votes.groupBy("county", "candidate")
                          .agg(F.sum("votes").alias("votes")))
    # after a groupBy() the row order is not guaranteed: orderBy() before show()
    county_totals.orderBy("county", F.desc("votes")).show()

    # The TV view: one row per county, one column per candidate. Listing the
    # values fixes the column order and saves Spark a job to find them.
    candidates = ['Lee', 'Patel', 'Rivera', 'write-in']
    tv = votes.groupBy("county").pivot("candidate", candidates).agg(F.sum("votes"))
    print("The TV view, with pivot():")
    tv.orderBy("county").show()
    print("San Mateo had no write-in rows, so its sum is NULL, not 0.")

    # ---------------------------------------------------------------------
    section("4. Bonus: the statewide total, and rollup()")
    # ---------------------------------------------------------------------
    # With RDDs: RDD.fold() with the same zero and function. Here: a
    # groupBy() on fewer columns.
    print("statewide, per candidate:")
    votes.groupBy("candidate").agg(F.sum("votes").alias("votes")).orderBy(F.desc("votes")).show()

    # rollup() computes every level at once: per (county, candidate), per
    # county, and the grand total. NULL marks the "all" rows.
    levels = votes.rollup("county", "candidate").agg(F.sum("votes").alias("votes"))
    print("votes counted per county, and in the whole state (rollup()):")
    levels.filter(F.col("candidate").isNull()).orderBy(F.col("county").asc_nulls_last()).show()

    # ---------------------------------------------------------------------
    section("5. Keeping the dictionaries: aggregate() over collect_list()")
    # ---------------------------------------------------------------------
    # The most literal translation of foldByKey(): collect each county's
    # tallies into a list, then FOLD the list, starting from an empty map.
    # F.aggregate(array, zero, merge) is functools.reduce(merge, array, zero).
    empty_tally = F.create_map().cast("map<string,int>")      # {}
    folded = reports_df.groupBy("county").agg(
        F.aggregate(F.collect_list("tally"), empty_tally, add_tallies).alias("tally"))
    same = (folded.select("county", F.explode("tally").alias("candidate", "votes"))
                  .orderBy("county", "candidate").collect()
            == county_totals.orderBy("county", "candidate").collect())
    print("aggregate(collect_list(tally), {}, add_tallies) same answer?", same)
    print("It works, but collect_list() gathers EVERY tally of a county in one")
    print("place (that's groupByKey(), not foldByKey()): every tally crosses the")
    print("network and no votes are added before the shuffle. To aggregate")
    print("dictionaries, explode() them into rows first.")

    # ---------------------------------------------------------------------
    section("6. A common mistake: NULL is not zero")
    # ---------------------------------------------------------------------
    # The total number of votes counted per county, from the TV view:
    total = F.col("Lee") + F.col("Patel") + F.col("Rivera") + F.col("write-in")
    tv.withColumn("total", total).orderBy("county").show()
    print("San Mateo's total is NULL: in SQL, anything + NULL is NULL. One NULL")
    print("(no write-in votes) erased 1,405 real votes.")
    print()
    # In foldByKey() we had to SAY what "empty" means ({}). In a DataFrame an
    # empty group is NULL; when the business means "0 votes", say so with
    # fillna(0) (or F.coalesce(col, F.lit(0))).
    print("With fillna(0) first:")
    tv.fillna(0).withColumn("total", total).orderBy("county").show()
    print("The totals now match the rollup() in section 4.")

    spark.stop()


if __name__ == '__main__':
    main()
