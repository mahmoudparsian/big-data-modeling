#!/usr/bin/env python3
"""
A Real-Life Case for groupByKey(): Median Home Price per Zip Code
==================================================================

The scenario
------------
A real-estate website shows a summary for every zip code:

    95050 (Santa Clara)   5 homes sold   median price: $1,310K

Every night it recomputes this summary from the log of all home sales. Each
sale is a (zip_code, price) pair, with the price in thousands of dollars.

    Input:  one record per sale      (zip_code, price)
    Output: one record per zip code  (zip_code, (number of sales, median price))

Why the median and not the average? One mansion that sold for $9,800K pulls
the average of 95050 up to $2,988K, although 4 of its 5 homes sold for less
than $1.5M. The median (the middle value of the sorted prices) ignores how
extreme the extreme values are.

Why this job needs ALL the values
---------------------------------
For a count, a sum, or a rating box, we keep a small summary per key that we
can ADD one value to and MERGE with another partial summary. A median has no
such summary: knowing that the partial median is 1400 tells you nothing
about the new median after one more sale. The only summary that is enough is
THE WHOLE LIST OF PRICES -- and collecting the whole list of values of each
key is exactly what groupByKey() does:

    Step                                              Spark
    ------------------------------------------------  -----------------
    1. Collect: put all sales of each zip in one pile  groupByKey()
    2. Compute: sort each pile and take the middle     mapValues(median)

What this program shows
-----------------------
  1. the sample data and its 2 partitions
  2. the groupByKey() solution, and the average vs the median
  3. why partial medians cannot be merged
  4. the alternative (combineByKey building lists = groupByKey by hand)

How to run
----------
    $SPARK_HOME/bin/spark-submit groupbykey_median_home_prices.py

or, if PySpark is installed in your Python (pip install pyspark):

    python3 groupbykey_median_home_prices.py

The output of a real run is in groupbykey_median_home_prices.log.
The full write-up is in groupbykey_median_home_prices.md.
"""
from pyspark.sql import SparkSession


def section(title):
    """Print a section header, to make the output easy to follow."""
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


def median(values):
    """Return the median of a collection of numbers.

    Odd number of values: the middle value of the sorted values.
    Even number of values: the average of the two middle values.
    """
    s = sorted(values)
    n = len(s)
    middle = n // 2
    if n % 2 == 1:                            # odd: the middle value
        return s[middle]
    return (s[middle - 1] + s[middle]) / 2    # even: average of the two middle values


def medians_in_partition(records):
    """For each zip code IN ONE PARTITION: (zip, (its prices, their median)).

    Used only to show that partial medians cannot be merged.
    """
    prices = {}
    for zip_code, price in records:
        prices.setdefault(zip_code, []).append(price)
    return [(z, (sorted(p), median(p))) for z, p in prices.items()]


# The three combineByKey() functions that build a list of prices per key:
# exactly what groupByKey() does internally.
def start_list(price):
    return [price]


def add_price(prices, price):
    prices.append(price)
    return prices


def merge_lists(prices1, prices2):
    prices1.extend(prices2)
    return prices1


def main():
    spark = SparkSession.builder.appName("groupByKey-median-home-prices").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    print("Spark version:", sc.version)

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

    # 2 partitions, so the output is the same on every machine.
    # sales_rdd : RDD[(zip_code, price)]
    sales_rdd = sc.parallelize(sales, 2)
    for i, part in enumerate(sales_rdd.glom().collect()):
        print("partition", i, ":", part)
    print("Every zip code has sales in BOTH partitions.")

    # ---------------------------------------------------------------------
    section("2. The groupByKey() solution")
    # ---------------------------------------------------------------------
    # Step 1: collect all prices of each zip code.
    # grouped : RDD[(zip_code, Iterable[price])]
    grouped = sales_rdd.groupByKey()
    print("Step 1: all prices of each zip code")
    # sorted() is only for display: the order of values in a group is not
    # guaranteed.
    for zip_code, prices in sorted(grouped.collect()):
        print("  ", zip_code, sorted(prices))

    # Step 2: compute the summary from each complete list.
    # summary : RDD[(zip_code, (number of sales, median price))]
    summary = grouped.mapValues(lambda prices: (len(prices), median(prices)))
    print()
    print("Step 2: (number of sales, median price)")
    print("  ", sorted(summary.collect()))

    print()
    print("The report the website shows:")
    for zip_code, (n, med) in sorted(summary.collect()):
        print(f"  {zip_code} ({names[zip_code]})   {n} homes sold   "
              f"median price: ${med:,.0f}K")

    print()
    print("The (misleading) average, side by side with the median:")
    for zip_code, prices in sorted(grouped.collect()):
        prices = list(prices)
        print("  ", zip_code, "average:", sum(prices) / len(prices),
              "  median:", median(prices))
    print("The one mansion in 95050 more than doubles the average.")

    # ---------------------------------------------------------------------
    section("3. Why partial medians cannot be merged")
    # ---------------------------------------------------------------------
    # Could we save the shuffle by computing a median INSIDE EACH PARTITION
    # first, and then combining the partial medians (the way reduceByKey()
    # combines partial sums)? Let's try it for 95050:
    partial = sales_rdd.mapPartitions(medians_in_partition)
    partial_95050 = partial.filter(lambda kv: kv[0] == '95050').collect()
    print("partial medians of 95050:", partial_95050)

    medians = [med for zip_code, (prices, med) in partial_95050]
    print("median of the partial medians:", median(medians), " <- WRONG")
    print("the true median:              ",
          median([price for zip_code, price in sales if zip_code == '95050']))
    print("The partial medians do not have enough information to recover the")
    print("true median, and the wrong answer depends on how Spark happened to")
    print("split the data. (The sum of partial sums, by contrast, is ALWAYS")
    print("the true sum: that's why reduceByKey() works for sums, not medians.)")

    # ---------------------------------------------------------------------
    section("4. Why not reduceByKey(), aggregateByKey(), or combineByKey()?")
    # ---------------------------------------------------------------------
    # Since the only summary that works is the whole list of prices, any
    # other reducer would have to build that list. With combineByKey():
    lists = sales_rdd.combineByKey(start_list, add_price, merge_lists)
    summary2 = lists.mapValues(lambda prices: (len(prices), median(prices)))
    print("combineByKey(start_list, add_price, merge_lists):")
    print("  ", sorted(summary2.collect()))
    print("same answer?", sorted(summary2.collect()) == sorted(summary.collect()))
    print()
    print("Same answer, but we just rewrote groupByKey() by hand: PySpark's own")
    print("groupByKey() is built from these very same three functions. When the")
    print("job really needs every value, say so: use groupByKey().")
    print()
    print("The price: EVERY value crosses the network, and all values of one key")
    print("must fit in one executor's memory. Fine here (a zip code has only a")
    print("few thousand sales a year); NOT fine for keys with billions of values.")

    spark.stop()


if __name__ == '__main__':
    main()
