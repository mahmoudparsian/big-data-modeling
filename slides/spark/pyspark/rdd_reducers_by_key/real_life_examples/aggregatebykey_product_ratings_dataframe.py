#!/usr/bin/env python3
"""
Star Ratings on Product Pages: DataFrame Version
=================================================

The DataFrame companion to aggregatebykey_product_ratings.py (same reviews).

The scenario
------------
An online store shows a RATING BOX on every product page: how many reviews
gave 5, 4, ..., 1 stars, plus the number of reviews and the average:

    grinder   4.25 out of 5   (4 reviews)
      5 stars | ##   | 2
      4 stars | #    | 1
      3 stars | #    | 1
      2 stars |      | 0
      1 stars |      | 0

    Input:  one row per review   (product, stars)
    Output: one row per product  (product, stars_1, ..., stars_5, reviews, average)

From aggregateByKey() to DataFrames
-----------------------------------
With RDDs, the box was a list of 5 counters, and aggregateByKey() got the
empty box, "add one review" and "merge two boxes". With DataFrames, each
counter is its OWN COLUMN, and each column is one built-in aggregate. Spark
already knows how to start, add to, and merge a count:

    Business rule               RDD                           DataFrame
    --------------------------  ----------------------------  ----------------------------------
    1. The empty rating box     zeroValue = [0, 0, 0, 0, 0]   built into count(): starts at 0
    2. Add one review           add_review                    count(when(stars == k, 1)),
                                                              one column per star level k
    3. Merge two boxes          merge_boxes                   built into count()

What this program shows
-----------------------
  1. the sample data
  2. the solution: one count(when(...)) per star level, and the rating boxes
  3. the empty box for a product with no reviews (left join + fillna(0) on
     the counters only: the average of no reviews stays NULL)
  4. other ways: pivot() with the list of values, and Spark SQL count_if()
  5. a common mistake: pivot() without the list of values

How to run
----------
    $SPARK_HOME/bin/spark-submit aggregatebykey_product_ratings_dataframe.py

or, if PySpark is installed in your Python (pip install pyspark):

    python3 aggregatebykey_product_ratings_dataframe.py

The output of a real run is in aggregatebykey_product_ratings_dataframe.log.
The full write-up is in aggregatebykey_product_ratings_dataframe.md.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def section(title):
    """Print a section header, to make the output easy to follow."""
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


def show_box(row):
    """Print a rating box exactly as the product page shows it."""
    print(f"{row.product}   {row.average} out of 5   ({row.reviews} reviews)")
    for k in range(5, 0, -1):
        n = row[f"stars_{k}"]
        print(f"  {k} stars | {'#' * n:<4} | {n}")


def main():
    spark = SparkSession.builder.appName("product-ratings-dataframe").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print("Spark version:", spark.version)

    # ---------------------------------------------------------------------
    section("1. Sample data: (product, stars)")
    # ---------------------------------------------------------------------
    reviews = [
        ('grinder', 5), ('kettle', 4), ('grinder', 4), ('mug', 5),
        ('grinder', 5), ('kettle', 2), ('grinder', 3), ('kettle', 4),
    ]
    reviews_df = spark.createDataFrame(reviews, "product STRING, stars INT")
    reviews_df.printSchema()
    print("reviews:", reviews_df.count())

    # ---------------------------------------------------------------------
    section("2. The solution: one count(when(...)) per star level")
    # ---------------------------------------------------------------------
    # F.when(cond, 1) is 1 when cond holds and NULL otherwise, and F.count()
    # counts only non-NULL values: count(when(stars == 3, 1)) is "the number
    # of 3-star reviews". All columns are computed in ONE pass, combined per
    # partition before the shuffle, like aggregateByKey().
    star_columns = [F.count(F.when(F.col("stars") == k, 1)).alias(f"stars_{k}")
                    for k in range(1, 6)]
    boxes = reviews_df.groupBy("product").agg(
        *star_columns,
        F.count("*").alias("reviews"),
        F.round(F.avg("stars"), 2).alias("average"))
    # after a groupBy() the row order is not guaranteed: orderBy() before show()
    boxes.orderBy("product").show()

    for row in boxes.orderBy("product").collect():
        show_box(row)

    # ---------------------------------------------------------------------
    section("3. The empty box, for a product with no reviews")
    # ---------------------------------------------------------------------
    catalog = spark.createDataFrame(
        [('grinder', 'Coffee grinder'), ('kettle', 'Electric kettle'),
         ('mug', 'Travel mug'), ('teapot', 'Glass teapot')],
        "product STRING, name STRING")

    # A left join keeps the teapot, but its box columns are all NULL.
    pages = catalog.join(boxes, "product", "left")
    print("After the left join:")
    pages.orderBy("product").show()

    # No reviews means 0 of each: fill the COUNTERS (only those) with 0, and
    # the empty box [0, 0, 0, 0, 0] reappears. NOT fillna(0) for all
    # columns: the teapot's average would become 0.0, "0 out of 5", the
    # worst rating, for a product nobody rated. An average of no reviews is
    # undefined: NULL says exactly that ("No reviews yet").
    counters = [f"stars_{k}" for k in range(1, 6)] + ["reviews"]
    pages = pages.fillna(0, subset=counters)
    print("After fillna(0, subset=counters):")
    pages.orderBy("product").show()

    # ---------------------------------------------------------------------
    section("4. Other ways: pivot(), and Spark SQL")
    # ---------------------------------------------------------------------
    # pivot("stars", [1, 2, 3, 4, 5]) turns each star level into a column.
    # An empty cell is NULL, not 0, hence the fillna(0).
    pivoted = reviews_df.groupBy("product").pivot("stars", [1, 2, 3, 4, 5]).count()
    print("pivot('stars', [1, 2, 3, 4, 5]).count(), then fillna(0):")
    pivoted.fillna(0).orderBy("product").show()

    reviews_df.createOrReplaceTempView("reviews")
    print("Spark SQL, with count_if():")
    spark.sql("""
        SELECT product,
               count_if(stars = 1)  AS stars_1,
               count_if(stars = 2)  AS stars_2,
               count_if(stars = 3)  AS stars_3,
               count_if(stars = 4)  AS stars_4,
               count_if(stars = 5)  AS stars_5,
               count(*)             AS reviews,
               round(avg(stars), 2) AS average
        FROM reviews
        GROUP BY product
        ORDER BY product
    """).show()

    # ---------------------------------------------------------------------
    section("5. A common mistake: pivot() without the list of values")
    # ---------------------------------------------------------------------
    # Without the list, pivot() runs an extra job to find the distinct values
    # of stars IN THE DATA, and makes one column for each.
    reviews_df.groupBy("product").pivot("stars").count().orderBy("product").show()
    print("1. The 1-star column is gone: no one gave 1 star, so Spark never saw")
    print("   the value 1. The shape of the report depends on the data.")
    print("2. Empty cells are NULL, not 0: the page would print '2 stars | NULL'.")
    print("A rating box ALWAYS has 5 bars (the RDD version's 5-counter zeroValue).")
    print("Say it in the code: list the values, or use count(when(...)) as above.")

    spark.stop()


if __name__ == '__main__':
    main()
