#!/usr/bin/env python3
"""
Building Customer Profiles: DataFrame Version
==============================================

The DataFrame companion to combinebykey_customer_profile.py (same orders).

The scenario
------------
The marketing team of an online coffee shop wants a PROFILE for every
customer: how many orders they placed, how much they spent, the dates of
their first and last order, and their FAVORITE PRODUCT (the one they bought
most often). They will send "We miss you!" emails and coupons. The order log
is not sorted by date.

    Input:  one row per order     (customer, order_date, product, amount)
    Output: one row per customer  (customer, orders, total, first_order,
                                   last_order, favorite)

From combineByKey() to DataFrames
---------------------------------
With RDDs, a profile was one dictionary, built by the three combineByKey()
functions. With DataFrames, each field of the profile is its OWN COLUMN,
computed by its own built-in aggregate. Each aggregate already knows how to
start, add one value, and merge two partial results:

    Profile field      RDD (start / add / merge)           DataFrame
    -----------------  ----------------------------------  ---------------------------------
    number of orders   1, += 1, +=                         count("*")
    total spent        amount, += amount, +=               sum("amount")
    first order        date, min(...), min(...)            min("order_date")
    last order         date, max(...), max(...)            max("order_date")
    favorite product   {product: count}, then top count    mode("product", deterministic=True)

The RDD version avoided aggregateByKey() because an empty profile needs fake
dates (a first order in the year 9999). DataFrame aggregates don't have that
problem: min() and max() start from NULL ("no value yet"), and the first
real date replaces it -- createCombiner's idea, built into every aggregate.

What this program shows
-----------------------
  1. the sample data
  2. the solution: one agg(), one column per field; the "We miss you!" list
  3. the favorite with any tie-breaking rule: count per product + a window
  4. Spark SQL
  5. a common mistake: max() (or first()) as the "favorite"

How to run
----------
    $SPARK_HOME/bin/spark-submit combinebykey_customer_profile_dataframe.py

or, if PySpark is installed in your Python (pip install pyspark):

    python3 combinebykey_customer_profile_dataframe.py

The output of a real run is in combinebykey_customer_profile_dataframe.log.
The full write-up is in combinebykey_customer_profile_dataframe.md.
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def section(title):
    """Print a section header, to make the output easy to follow."""
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


def main():
    spark = SparkSession.builder.appName("customer-profile-dataframe").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print("Spark version:", spark.version)

    # ---------------------------------------------------------------------
    section("1. Sample data: one row per order")
    # ---------------------------------------------------------------------
    orders = [
        ('alice', '2026-01-05', 'coffee',  12.0),
        ('bob',   '2026-01-07', 'tea',      8.0),
        ('alice', '2026-02-11', 'mug',     15.0),
        ('alice', '2026-01-20', 'coffee',  12.0),
        ('bob',   '2026-03-02', 'tea',      8.0),
        ('alice', '2026-03-15', 'coffee',  24.0),
        ('carol', '2026-02-28', 'teapot',  30.0),
        ('bob',   '2026-02-14', 'cookies',  5.0),
    ]

    # The dates arrive as text: convert them to a real date column.
    orders_df = (spark.createDataFrame(
                     orders, "customer STRING, order_date STRING, product STRING, amount DOUBLE")
                 .withColumn("order_date", F.to_date("order_date")))
    orders_df.printSchema()
    orders_df.show()

    # ---------------------------------------------------------------------
    section("2. The solution: one agg(), one column per profile field")
    # ---------------------------------------------------------------------
    # F.mode() is the most frequent value. deterministic=True decides ties:
    # it returns the LOWEST value (the alphabetically first product), the
    # same tie rule as the RDD version's favorite(). Without it, mode() may
    # return any of the tied values.
    profiles = orders_df.groupBy("customer").agg(
        F.count("*").alias("orders"),
        F.sum("amount").alias("total"),
        F.min("order_date").alias("first_order"),
        F.max("order_date").alias("last_order"),
        F.mode("product", deterministic=True).alias("favorite"))
    # after a groupBy() the row order is not guaranteed: orderBy() before show()
    profiles.orderBy("customer").show()

    # The "We miss you!" list: no order since March 1st. last_order is a
    # real date, so we compare it with a date.
    print("We miss you! list (no order since 2026-03-01):")
    (profiles.filter(F.col("last_order") < F.lit("2026-03-01").cast("date"))
             .select("customer").orderBy("customer").show())

    # ---------------------------------------------------------------------
    section("3. The favorite product, with any tie-breaking rule")
    # ---------------------------------------------------------------------
    # mode() can only break ties by the lowest value. For another rule (the
    # product bought most recently, the one that brings the most money...),
    # compute the favorite in two steps: count per (customer, product), then
    # keep the top product per customer with a window. Here the tie rule is
    # the product name, so we can check the result against mode().
    per_product = orders_df.groupBy("customer", "product").agg(F.count("*").alias("times"))
    print("orders per (customer, product): the RDD profile's 'products' dict, as rows")
    per_product.orderBy("customer", "product").show()

    top_first = Window.partitionBy("customer").orderBy(F.desc("times"), "product")
    favorites = (per_product
                 .withColumn("rank", F.row_number().over(top_first))
                 .filter("rank = 1")
                 .select("customer", F.col("product").alias("favorite")))
    print("the top product per customer (window: times desc, then product):")
    favorites.orderBy("customer").show()

    profiles2 = (orders_df.groupBy("customer").agg(
                     F.count("*").alias("orders"),
                     F.sum("amount").alias("total"),
                     F.min("order_date").alias("first_order"),
                     F.max("order_date").alias("last_order"))
                 .join(favorites, "customer"))
    print("profile + window favorite, same as section 2?",
          profiles2.orderBy("customer").collect() == profiles.orderBy("customer").collect())

    # ---------------------------------------------------------------------
    section("4. Spark SQL")
    # ---------------------------------------------------------------------
    orders_df.createOrReplaceTempView("orders")
    spark.sql("""
        SELECT customer,
               count(*)            AS orders,
               sum(amount)         AS total,
               min(order_date)     AS first_order,
               max(order_date)     AS last_order,
               mode(product, true) AS favorite
        FROM orders
        GROUP BY customer
        ORDER BY customer
    """).show()

    # ---------------------------------------------------------------------
    section("5. A common mistake: max() (or first()) as the 'favorite'")
    # ---------------------------------------------------------------------
    orders_df.groupBy("customer").agg(
        F.max("product").alias("favorite")).orderBy("customer").show()
    print("alice bought coffee 3 times and a mug once, yet her coupon is for")
    print("MUGS: max() of a string is the alphabetically last value ('mug' >")
    print("'coffee'). bob's 'tea' is right only by luck. F.first() is no better:")
    print("whichever row comes first, non-deterministic after a shuffle.")
    print("'Most often' is a count: use mode(), or count per product (section 3).")

    spark.stop()


if __name__ == '__main__':
    main()
