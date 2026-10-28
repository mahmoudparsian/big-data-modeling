#!/usr/bin/env python3
"""
A Real-Life Case for sortByKey(): Printing Presorted Mail
==========================================================

The scenario
------------
Every month, a credit-card company prints a paper statement for each of its
customers and mails it. The post office gives a DISCOUNT FOR PRESORTED MAIL:
if the envelopes arrive already in ZIP code order, the post office doesn't
have to sort them itself.

The print shop has 3 PRINTERS. Each printer should print one CONTIGUOUS
RANGE of ZIP codes, so that its stack of envelopes can go straight into mail
trays, in order: printer 1 the lowest ZIP codes, printer 3 the highest.

    Input:  statements in billing order  (zip_code, (customer, amount_due))
    Output: the SAME statements, in ZIP code order, split into 3 ranges

Unlike the reducers, sortByKey() does not combine anything: the output has
EVERY input record ('10001' appears twice, because two customers live there).

Why this is a job for sortByKey()
---------------------------------
  * The sort key is already the key: each record is (zip_code, statement).
  * Spark sorts in parallel, on all the machines, so millions of statements
    never have to fit on one computer.
  * The output partitions are RANGES: sortByKey() puts the smallest keys in
    partition 0, the next ones in partition 1, ..., and each partition is
    sorted. With numPartitions=3, each partition is one printer's batch.

What this program shows
-----------------------
  1. the sample data
  2. the sortByKey() solution, and the 3 printers' batches (glom())
  3. how sortByKey() finds the range boundaries
  4. sorting by more than one field, and in descending order; sortBy()
  5. the alternatives (collect() + sorted(), takeOrdered(), reducers)
  6. a common mistake: sorting numbers that are stored as strings
     (and why ZIP codes must stay strings)

How to run
----------
    $SPARK_HOME/bin/spark-submit sortbykey_presorted_mail.py

or, if PySpark is installed in your Python (pip install pyspark):

    python3 sortbykey_presorted_mail.py

The output of a real run is in sortbykey_presorted_mail.log.
The full write-up is in sortbykey_presorted_mail.md.
"""
from pyspark.sql import SparkSession


def section(title):
    """Print a section header, to make the output easy to follow."""
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


def main():
    spark = SparkSession.builder.appName("sortByKey-presorted-mail").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    print("Spark version:", sc.version)

    # ---------------------------------------------------------------------
    section("1. Sample data: (zip_code, (customer, amount_due))")
    # ---------------------------------------------------------------------
    # The ZIP codes are real (Cambridge MA, New York, Atlanta, Chicago,
    # Austin, San Francisco, Santa Clara, Seattle); the customers and
    # amounts are made up. ZIP codes are 5-character STRINGS: since they
    # all have the same length, string order is the same as numeric order.
    statements = [
        ('95050', ('Chen',       120.00)),
        ('10001', ('Okafor',      45.10)),
        ('98101', ('Larsen',       9.50)),
        ('02138', ('Murphy',     310.25)),
        ('60601', ('Kowalski',    88.00)),
        ('95050', ('Alvarez',     64.75)),
        ('30301', ('Washington', 150.00)),
        ('94105', ('Nakamura',   210.40)),
        ('10001', ('Bernstein',   19.99)),
        ('73301', ('Hernandez',   75.00)),
        ('95050', ('Baker',       33.30)),
        ('98101', ('Andersen',    99.00)),
    ]

    # statements_rdd : RDD[(zip_code, (customer, amount_due))]
    statements_rdd = sc.parallelize(statements, 2)
    print("number of statements:", statements_rdd.count())
    print("number of partitions:", statements_rdd.getNumPartitions())

    # ---------------------------------------------------------------------
    section("2. The sortByKey() solution: one ZIP range per printer")
    # ---------------------------------------------------------------------
    # by_zip : RDD[(zip_code, (customer, amount_due))], in ZIP code order
    by_zip = statements_rdd.sortByKey(numPartitions=3)

    # No sorted() around collect(), unlike the reducer examples: the RDD
    # itself is in order, so collect() returns the records in order.
    for zip_code, (customer, amount) in by_zip.collect():
        print(zip_code, f"{customer:<11} {amount:>7.2f}")

    # The 3 printers' batches: glom() shows each partition as a list.
    # (With by_zip.saveAsTextFile(...), the files part-00000, part-00001,
    # part-00002 would be the three printers' batches, in order.)
    print()
    for printer, batch in enumerate(by_zip.glom().collect(), start=1):
        zips = [zip_code for zip_code, statement in batch]
        print(f"printer {printer}: {len(batch)} statements, ZIP {zips[0]} to {zips[-1]}")
    print("Only the KEY is sorted: customers in the same ZIP code (Okafor and")
    print("Bernstein, or the three in 95050) are in no particular order.")

    # ---------------------------------------------------------------------
    section("3. How sortByKey() works: find the boundaries, then sort")
    # ---------------------------------------------------------------------
    # Step 1: Spark samples the keys, sorts the sample, and picks
    #         numPartitions - 1 boundary keys; every record is sent to the
    #         partition of its key range (a shuffle with a RANGE partitioner,
    #         instead of the hash partitioner the reducers use).
    # Step 2: each partition is sorted on its own, in parallel.
    # Here is step 1 by hand (our RDD is small: the sample is every key):
    keys = sorted(zip_code for zip_code, statement in statements)
    print("sorted keys:", keys)
    bounds = [keys[int(len(keys) * 1 / 3)], keys[int(len(keys) * 2 / 3)]]
    print("boundaries (keys at 1/3 and 2/3):", bounds)
    print("Keys up to '60601' -> printer 1, up to '95050' -> printer 2, the")
    print("rest -> printer 3. All three 95050 statements must stay together,")
    print("so printer 3 got only 2: the batches are only ROUGHLY equal.")

    # ---------------------------------------------------------------------
    section("4. Sorting by more than one field, and in descending order")
    # ---------------------------------------------------------------------
    # Within a ZIP code, by customer name: make the key a TUPLE
    # (zip_code, customer). Python compares tuples field by field.
    # by_zip_name : RDD[((zip_code, customer), amount_due)]
    by_zip_name = (statements_rdd
                   .map(lambda kv: ((kv[0], kv[1][0]), kv[1][1]))
                   .sortByKey(numPartitions=3))
    print("by (zip_code, customer):")
    for (zip_code, customer), amount in by_zip_name.collect():
        print(" ", zip_code, f"{customer:<11} {amount:>7.2f}")

    # By amount due, largest first (for the collections department):
    # put the amount in the key, and pass ascending=False.
    by_amount = (statements_rdd
                 .map(lambda kv: (kv[1][1], (kv[1][0], kv[0])))   # (amount, (customer, zip))
                 .sortByKey(ascending=False, numPartitions=3))
    print()
    print("by amount due, largest first (top 3):", by_amount.take(3))

    # sortBy(keyfunc) sorts by a key YOU COMPUTE, without building
    # (key, value) pairs yourself. In PySpark's source it is
    #     self.keyBy(keyfunc).sortByKey(ascending, numPartitions).values()
    top3 = statements_rdd.sortBy(lambda kv: kv[1][1], ascending=False,
                                 numPartitions=3).take(3)
    print("same with sortBy(amount, ascending=False):", top3)

    # ---------------------------------------------------------------------
    section("5. Why not collect() + sorted(), or takeOrdered()?")
    # ---------------------------------------------------------------------
    # collect() + sorted(): brings EVERY record to the driver (one machine)
    # and sorts there, alone. Fine for 12 statements, not for 50 million.
    print("collect() + sorted() same records?",
          sorted(statements_rdd.collect()) == sorted(by_zip.collect()))

    # takeOrdered(n, key): the FIRST n records in order, without sorting
    # everything. Right for "the 3 largest amounts due", but the print shop
    # needs ALL statements in order.
    print("takeOrdered(3, largest amount first):",
          statements_rdd.takeOrdered(3, key=lambda kv: -kv[1][1]))

    # The reducers DO NOT SORT: their result comes out in hash-partition
    # order. If a reducer's result must be in key order, add sortByKey().
    counts = statements_rdd.mapValues(lambda s: 1).reduceByKey(lambda a, b: a + b)
    print("statements per ZIP, in ZIP order:", counts.sortByKey().collect())

    # ---------------------------------------------------------------------
    section("6. A common mistake: sorting numbers stored as strings")
    # ---------------------------------------------------------------------
    # Data from a CSV file arrives as text. If the amounts are still
    # STRINGS, sortByKey() compares them character by character:
    as_text = statements_rdd.map(lambda kv: (f"{kv[1][1]:.2f}", kv[1][0]))   # ('120.00', 'Chen'), ...
    print("amounts as strings, 'largest' first:",
          as_text.sortByKey(ascending=False, numPartitions=3).keys().take(5))
    print("  -> '99.00' first ('9' is the largest first character), and the")
    print("     real largest amount '310.25' much later.")

    # The fix: convert the key to a number before sorting.
    print("amounts as numbers, largest first:  ",
          as_text.map(lambda kv: (float(kv[0]), kv[1]))
                 .sortByKey(ascending=False, numPartitions=3).keys().take(5))

    # But the OPPOSITE is true for ZIP codes: keep them as strings. A ZIP
    # code is a label that happens to use digits; converting it to an int
    # throws away its leading zero.
    print()
    print("int('02138') =", int('02138'), " <- only 4 digits: not a valid ZIP code")
    print("Rule: sort by the type that has the order you want. Amounts are")
    print("numbers; ZIP codes, phone and account numbers are fixed-width strings.")

    spark.stop()


if __name__ == '__main__':
    main()
