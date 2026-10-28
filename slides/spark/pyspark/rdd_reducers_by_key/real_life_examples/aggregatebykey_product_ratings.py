#!/usr/bin/env python3
"""
A Real-Life Case for aggregateByKey(): Star Ratings on Product Pages
=====================================================================

The scenario
------------
An online store shows a RATING BOX on every product page:

    grinder   4.25 out of 5   (4 reviews)
      5 stars | ##   | 2
      4 stars | #    | 1
      3 stars | #    | 1
      2 stars |      | 0
      1 stars |      | 0

Every night, the store recomputes the rating box of every product from the
log of all reviews. Each review is a (product, stars) pair, stars = 1..5.

    Input:  one record per review   (product, stars)
    Output: one record per product  (product, [n1, n2, n3, n4, n5])
            n1 = number of 1-star reviews, ..., n5 = number of 5-star reviews

Everything in the rating box comes from the 5 counters: the bars, the number
of reviews (their sum), and the average (sum of stars x count / reviews).

Why this is a job for aggregateByKey()
--------------------------------------
The value (one review: a number 1..5) and the result (a list of 5 counters)
are DIFFERENT types, and the result has a natural EMPTY value with a real
business meaning: a brand-new product shows 0 reviews and five empty bars,
[0, 0, 0, 0, 0]. The web team's three rules are exactly the three arguments:

    Business rule                    aggregateByKey() argument  Type
    -------------------------------  -------------------------  -----------
    1. The empty rating box          zeroValue                  U
    2. Add one review to a box       seqFunc                    (U, V) -> U
    3. Merge two boxes               combFunc                   (U, U) -> U

What this program shows
-----------------------
  1. the sample data and its 2 partitions
  2. the aggregateByKey() solution and the rating boxes
  3. the empty box shows up in the final report too (a product with no
     reviews, via leftOuterJoin)
  4. the alternatives (combineByKey, map + reduceByKey)
  5. a common mistake: a zeroValue that is not empty (a "free" review)

How to run
----------
    $SPARK_HOME/bin/spark-submit aggregatebykey_product_ratings.py

or, if PySpark is installed in your Python (pip install pyspark):

    python3 aggregatebykey_product_ratings.py

The output of a real run is in aggregatebykey_product_ratings.log.
The full write-up is in aggregatebykey_product_ratings.md.
"""
from pyspark.sql import SparkSession


def section(title):
    """Print a section header, to make the output easy to follow."""
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


# -------------------------------------------------------------------------
# Rule 1: the empty rating box (zeroValue)
# -------------------------------------------------------------------------
EMPTY_BOX = [0, 0, 0, 0, 0]


# -------------------------------------------------------------------------
# Rule 2: add one review to a box. (U, V) -> U
# -------------------------------------------------------------------------
def add_review(box, stars):
    """Add 1 to the counter of the review's star level, and return the box.

    Changing box IN PLACE is safe: PySpark gives every key in every
    partition its own copy of the zeroValue (copy.deepcopy(zeroValue)).
    """
    box[stars - 1] += 1           # 1 star -> position 0, ..., 5 stars -> position 4
    return box


# -------------------------------------------------------------------------
# Rule 3: merge two boxes of the same product. (U, U) -> U
# -------------------------------------------------------------------------
def merge_boxes(box1, box2):
    """Add the counters of box2 into box1, position by position."""
    for i in range(5):
        box1[i] += box2[i]
    return box1


def summary(box):
    """(number of reviews, average rating) of a rating box."""
    num_reviews = sum(box)
    total_stars = sum((i + 1) * n for i, n in enumerate(box))
    return (num_reviews, round(total_stars / num_reviews, 2))


def show(product, box):
    """Print a rating box exactly as the product page shows it."""
    num_reviews, average = summary(box)
    print(f"{product}   {average} out of 5   ({num_reviews} reviews)")
    for stars in range(5, 0, -1):
        n = box[stars - 1]
        print(f"  {stars} stars | {'#' * n:<4} | {n}")


# For the alternatives in section 4:
def first_review(stars):
    """createCombiner for combineByKey(): an empty box plus one review."""
    box = [0, 0, 0, 0, 0]    # an empty box ...
    box[stars - 1] += 1      # ... plus one review: the same code as add_review!
    return box


def one_review_box(stars):
    """For map() + reduceByKey(): dress up one review as a 5-counter box."""
    box = [0, 0, 0, 0, 0]
    box[stars - 1] = 1
    return box


def main():
    spark = SparkSession.builder.appName("aggregateByKey-product-ratings").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    print("Spark version:", sc.version)

    # ---------------------------------------------------------------------
    section("1. Sample data: (product, stars)")
    # ---------------------------------------------------------------------
    reviews = [
        ('grinder', 5), ('kettle', 4), ('grinder', 4), ('mug', 5),
        ('grinder', 5), ('kettle', 2), ('grinder', 3), ('kettle', 4),
    ]

    # 2 partitions, so the output is the same on every machine.
    # reviews_rdd : RDD[(product, stars)]
    reviews_rdd = sc.parallelize(reviews, 2)
    for i, part in enumerate(reviews_rdd.glom().collect()):
        print("partition", i, ":", part)
    print("grinder and kettle have reviews in BOTH partitions; mug only in one.")

    # ---------------------------------------------------------------------
    section("2. The aggregateByKey() solution")
    # ---------------------------------------------------------------------
    # boxes : RDD[(product, [count of 1 star, ..., count of 5 stars])]
    boxes = reviews_rdd.aggregateByKey(EMPTY_BOX, add_review, merge_boxes)
    print("rating boxes:", sorted(boxes.collect()))
    print("(number of reviews, average rating):",
          sorted(boxes.mapValues(summary).collect()))
    print()
    for product, box in sorted(boxes.collect()):
        show(product, box)

    # How grinder is aggregated. EACH PARTITION STARTS FROM ITS OWN EMPTY BOX:
    #   partition 0: [0,0,0,0,0] + 5 stars + 4 stars -> [0,0,0,1,1]
    #   partition 1: [0,0,0,0,0] + 5 stars + 3 stars -> [0,0,1,0,1]
    #   after the shuffle: merge_boxes            -> [0,0,1,1,2]
    print()
    print("EMPTY_BOX itself never changed:", EMPTY_BOX)

    # ---------------------------------------------------------------------
    section("3. The empty box shows up in the final report, too")
    # ---------------------------------------------------------------------
    # The review log only has products that HAVE reviews. But the catalog
    # also has a teapot that nobody has reviewed yet, and its page needs a
    # rating box too: the same empty box.
    catalog = sc.parallelize([('grinder', 'Coffee grinder'),
                              ('kettle', 'Electric kettle'),
                              ('mug', 'Travel mug'),
                              ('teapot', 'Glass teapot')], 2)

    # (product, (name, box or None))  ->  (product, (name, box))
    pages = (catalog.leftOuterJoin(boxes)
                    .mapValues(lambda v: (v[0], v[1] if v[1] is not None else EMPTY_BOX)))
    for product, (name, box) in sorted(pages.collect()):
        print(f"{name:16} {box}")
    print("The zeroValue is the SAME value the business uses for 'no data yet':")
    print("the sign that aggregateByKey() is the right reducer.")

    # ---------------------------------------------------------------------
    section("4. Why not combineByKey() or map() + reduceByKey()?")
    # ---------------------------------------------------------------------
    # combineByKey(): no zeroValue, so createCombiner must build a box from
    # the FIRST review -- "an empty box, then add_review": the same rule
    # written twice. (PySpark implements aggregateByKey() exactly this way:
    # combineByKey(lambda v: seqFunc(createZero(), v), seqFunc, combFunc).)
    boxes2 = reviews_rdd.combineByKey(first_review, add_review, merge_boxes)
    print("combineByKey(first_review, add_review, merge_boxes) same answer?",
          sorted(boxes2.collect()) == sorted(boxes.collect()))

    # map() + reduceByKey(): every single review must first become a box
    # holding just that review, then boxes are added to boxes. The natural
    # rule "+1 to one counter" is gone.
    one_review_boxes = reviews_rdd.mapValues(one_review_box)
    print("one-review boxes (first 2):", one_review_boxes.take(2))
    print("mapValues(one_review_box).reduceByKey(merge_boxes)  same answer?",
          sorted(one_review_boxes.reduceByKey(merge_boxes).collect())
          == sorted(boxes.collect()))

    # ---------------------------------------------------------------------
    section("5. A common mistake: a zeroValue that is not empty")
    # ---------------------------------------------------------------------
    # The zeroValue is used ONCE PER KEY PER PARTITION, not once per key.
    # Suppose someone gives every product a "free" 5-star review to start:
    generous_box = [0, 0, 0, 0, 1]
    generous = reviews_rdd.aggregateByKey(generous_box, add_review, merge_boxes)
    print(sorted(generous.collect()))
    print("mug got ONE free 5-star review (its reviews are in one partition),")
    print("but grinder and kettle got TWO (their reviews are in two partitions).")
    print("Business rules like 'add a bonus' belong AFTER the aggregation")
    print("(in mapValues()), never in the zeroValue.")

    spark.stop()


if __name__ == '__main__':
    main()
