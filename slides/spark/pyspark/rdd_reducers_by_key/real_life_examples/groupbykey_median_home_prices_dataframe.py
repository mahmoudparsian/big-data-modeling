#!/usr/bin/env python3
"""
Median Home Price per Zip Code: DataFrame Version
==================================================

The DataFrame companion to groupbykey_median_home_prices.py (same sales).

The scenario
------------
A real-estate website shows, for every zip code, the number of homes sold
and the MEDIAN price (the middle price, which one mansion can't distort the
way it distorts the average):

    95050 (Santa Clara)   5 homes sold   median price: $1,310K

    Input:  one row per sale      (zip_code, price in $1000s)
    Output: one row per zip code  (zip_code, homes_sold, median_price)

From groupByKey() to DataFrames
-------------------------------
The RDD version made one point: a median needs ALL the values of a key
(partial medians cannot be merged), so we collected the prices with
groupByKey() and computed the median ourselves. DataFrames have a built-in
median(), but it can't escape the math: inside, it too keeps the prices of
each group.

    Step                                     RDD                DataFrame
    ---------------------------------------  -----------------  ------------------------------
    1. Collect all sales of each zip code    groupByKey()       inside median()
                                                                (by hand: collect_list())
    2. Sort each pile and take the middle    mapValues(median)  median()

What this program shows
-----------------------
  1. the sample data
  2. the solution with groupBy().agg(median()), and average vs median
  3. what median() does inside (partial_percentile: every distinct value
     with its count), and groupByKey() by hand with collect_list()
  4. Spark SQL: median() and percentile_cont()
  5. a common mistake: percentile_approx() is not the median

How to run
----------
    $SPARK_HOME/bin/spark-submit groupbykey_median_home_prices_dataframe.py

or, if PySpark is installed in your Python (pip install pyspark):

    python3 groupbykey_median_home_prices_dataframe.py

The output of a real run is in groupbykey_median_home_prices_dataframe.log.
The full write-up is in groupbykey_median_home_prices_dataframe.md.
"""
import contextlib
import io

from pyspark.sql import SparkSession
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
    spark = SparkSession.builder.appName("median-home-prices-dataframe").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print("Spark version:", spark.version)

    # ---------------------------------------------------------------------
    section("1. Sample data: (zip_code, price in $1000s)")
    # ---------------------------------------------------------------------
    sales = [
        ('95050', 1250), ('95014', 2900), ('95050', 9800),
        ('95112',  980), ('95050', 1400), ('95014', 3100),
        ('95050', 1310), ('95112', 1050), ('95014', 2750),
        ('95050', 1180), ('95112', 1120), ('95014', 3300),
    ]
    names = {'95014': 'Cupertino', '95050': 'Santa Clara', '95112': 'San Jose'}

    # Zip codes are strings: a zip code is a label ('02138' keeps its zero).
    sales_df = spark.createDataFrame(sales, "zip_code STRING, price INT")
    sales_df.printSchema()
    print("sales:", sales_df.count())

    # ---------------------------------------------------------------------
    section("2. The solution: groupBy().agg(median())")
    # ---------------------------------------------------------------------
    summary = sales_df.groupBy("zip_code").agg(
        F.count("*").alias("homes_sold"),
        F.median("price").alias("median_price"))
    # after a groupBy() the row order is not guaranteed: orderBy() before show()
    summary.orderBy("zip_code").show()
    print("95014 has 4 sales: its median is (2900 + 3100) / 2 = 3000.0.")
    print("(median() always returns a double.)")

    print()
    print("The report the website shows:")
    for row in summary.orderBy("zip_code").collect():
        print(f"  {row.zip_code} ({names[row.zip_code]})   {row.homes_sold} homes sold   "
              f"median price: ${row.median_price:,.0f}K")

    print()
    print("Average vs. median:")
    sales_df.groupBy("zip_code").agg(
        F.avg("price").alias("average"),
        F.median("price").alias("median")).orderBy("zip_code").show()
    print("The one $9,800K mansion in 95050 more than doubles the average.")

    # ---------------------------------------------------------------------
    section("3. What median() does inside: still 'all the values'")
    # ---------------------------------------------------------------------
    # median() is a short name for percentile(price, 0.5). There IS a partial
    # step before the shuffle, but its partial result for each zip code is a
    # table of EVERY DISTINCT PRICE WITH ITS COUNT (an OpenHashMap[value,
    # count] in Spark's PercentileBase class). Merging two tables is easy
    # (add the counts); the median is computed only at the end. So median()
    # is groupByKey() with one saving (repeated values are stored once), and
    # each group must still fit in memory.
    plan = plan_of(summary)
    print("'partial_percentile' in plan:       ", "partial_percentile" in plan)
    print("'Exchange hashpartitioning' in plan:", "Exchange hashpartitioning" in plan)

    # collect_list() is the DataFrame groupByKey(): use it when a job needs
    # every value of a group. It gives no guaranteed order: array_sort().
    print()
    print("groupByKey() by hand, with collect_list():")
    grouped = sales_df.groupBy("zip_code").agg(
        F.array_sort(F.collect_list("price")).alias("prices"))
    grouped.orderBy("zip_code").show(truncate=False)

    # ---------------------------------------------------------------------
    section("4. Spark SQL: median() and percentile_cont()")
    # ---------------------------------------------------------------------
    sales_df.createOrReplaceTempView("sales")
    spark.sql("""
        SELECT zip_code,
               count(*)                                           AS homes_sold,
               median(price)                                      AS median_price,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS percentile_cont
        FROM sales
        GROUP BY zip_code
        ORDER BY zip_code
    """).show()

    # ---------------------------------------------------------------------
    section("5. A common mistake: percentile_approx() is not the median")
    # ---------------------------------------------------------------------
    sales_df.groupBy("zip_code").agg(
        F.median("price").alias("median"),
        F.percentile_approx("price", 0.5).alias("percentile_approx")).orderBy("zip_code").show()
    print("For 95014 (4 sales), percentile_approx() says 2900, not 3000.0:")
    print("  1. it never averages: it returns an ACTUAL value from the data (the")
    print("     smallest price with at least half of the prices <= it);")
    print("  2. it is approximate: it keeps a small fixed-size sketch per group,")
    print("     not every value (error controlled by its 'accuracy' argument).")
    print("Point 2 is also its strength: sketches merge like partial sums, so it")
    print("works on groups of ANY size. Use median() for an exact median when each")
    print("group fits in memory; percentile_approx() for huge groups.")

    spark.stop()


if __name__ == '__main__':
    main()
