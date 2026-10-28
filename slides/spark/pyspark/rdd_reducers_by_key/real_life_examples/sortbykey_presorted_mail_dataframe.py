#!/usr/bin/env python3
"""
Printing Presorted Mail: DataFrame Version
===========================================

The DataFrame companion to sortbykey_presorted_mail.py (same statements).

The scenario
------------
A credit-card company prints a paper statement for every customer. The post
office gives a discount for mail that arrives IN ZIP CODE ORDER, and the
print shop has 3 PRINTERS, each printing one CONTIGUOUS RANGE of ZIP codes
(printer 1 the lowest, printer 3 the highest).

    Input:  statements in billing order  (zip_code, customer, amount_due)
    Output: the SAME statements, in ZIP code order, in 3 ranges

From sortByKey() to DataFrames
------------------------------
sortByKey(numPartitions=3) did two things at once: it SORTED the records,
and it split them into 3 contiguous key ranges, one per partition. With
DataFrames these are two separate questions, each with its own method:

    What the print shop needs                RDD                          DataFrame
    ---------------------------------------  ---------------------------  -----------------------------------
    All statements in ZIP code order         sortByKey()                  orderBy("zip_code")
    ... in 3 ZIP ranges, one per printer     sortByKey(numPartitions=3)   repartitionByRange(3, "zip_code")
                                                                            .sortWithinPartitions("zip_code")

What this program shows
-----------------------
  1. the sample data
  2. orderBy(): the order, but not the partitions (AQE merged them into 1)
  3. repartitionByRange() + sortWithinPartitions(): one ZIP range per printer
  4. several sort columns, and the top 3 amounts with orderBy().limit()
     (the DataFrame takeOrdered()); Spark SQL
  5. a common mistake: letting Spark guess the CSV column types
     (ZIP codes lose their leading zero; amounts as text sort wrong)

How to run
----------
    $SPARK_HOME/bin/spark-submit sortbykey_presorted_mail_dataframe.py

or, if PySpark is installed in your Python (pip install pyspark):

    python3 sortbykey_presorted_mail_dataframe.py

The output of a real run is in sortbykey_presorted_mail_dataframe.log.
The full write-up is in sortbykey_presorted_mail_dataframe.md.
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
    spark = SparkSession.builder.appName("presorted-mail-dataframe").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    print("Spark version:", spark.version)

    # ---------------------------------------------------------------------
    section("1. Sample data: (zip_code, customer, amount_due)")
    # ---------------------------------------------------------------------
    # The ZIP codes are real (Cambridge MA, New York, Atlanta, Chicago,
    # Austin, San Francisco, Santa Clara, Seattle); the customers and amounts
    # are made up. ZIP codes are 5-character STRINGS: string order is ZIP
    # code order, and '02138' keeps its leading zero.
    statements = [
        ('95050', 'Chen',       120.00),
        ('10001', 'Okafor',      45.10),
        ('98101', 'Larsen',       9.50),
        ('02138', 'Murphy',     310.25),
        ('60601', 'Kowalski',    88.00),
        ('95050', 'Alvarez',     64.75),
        ('30301', 'Washington', 150.00),
        ('94105', 'Nakamura',   210.40),
        ('10001', 'Bernstein',   19.99),
        ('73301', 'Hernandez',   75.00),
        ('95050', 'Baker',       33.30),
        ('98101', 'Andersen',    99.00),
    ]
    statements_df = spark.createDataFrame(
        statements, "zip_code STRING, customer STRING, amount_due DOUBLE")
    statements_df.printSchema()
    print("statements:", statements_df.count())

    # ---------------------------------------------------------------------
    section("2. All statements in ZIP code order: orderBy()")
    # ---------------------------------------------------------------------
    # Like sortByKey(), orderBy() is a DISTRIBUTED sort: sample the keys,
    # shuffle each row to its key range (a range partitioner), and sort each
    # partition in parallel. Only the key is sorted: customers in the same
    # ZIP code are in no particular order, and that order can change from
    # run to run. So we print only the ZIP codes here.
    by_zip = statements_df.orderBy("zip_code")
    print("ZIP codes in order:", [row.zip_code for row in by_zip.collect()])

    # orderBy() takes no number of partitions: its shuffle uses
    # spark.sql.shuffle.partitions (200), and Adaptive Query Execution (AQE)
    # merges them for small data. Spark's choice, not the print shop's.
    print("number of partitions (printers) after orderBy():", by_zip.rdd.getNumPartitions())
    print("orderBy() promises the ORDER, not the PARTITIONS.")

    # ---------------------------------------------------------------------
    section("3. One ZIP range per printer: repartitionByRange()")
    # ---------------------------------------------------------------------
    # repartitionByRange(3, "zip_code"): exactly 3 contiguous ZIP ranges
    #     (the shuffle part of sortByKey()). It splits on zip_code only, so
    #     a ZIP code is never split between two printers.
    # sortWithinPartitions("zip_code", "customer"): sort each range on its
    #     own, with no further shuffle (the second part of sortByKey()).
    #     "customer" is a tie-breaker: each stack has a fixed order.
    batches = (statements_df
               .repartitionByRange(3, "zip_code")
               .sortWithinPartitions("zip_code", "customer"))
    print("number of partitions (printers):", batches.rdd.getNumPartitions())

    # spark_partition_id() is the partition of each row: the printer (+1).
    printed = batches.withColumn("printer", F.spark_partition_id() + 1)
    printed.show()
    (printed.groupBy("printer")
            .agg(F.count("*").alias("statements"),
                 F.min("zip_code").alias("from_zip"),
                 F.max("zip_code").alias("to_zip"))
            .orderBy("printer").show())

    # The RDD version got 5, 5, 2 statements; this one 4, 6, 2. Both are
    # correct ranges: PySpark's sortByKey() takes the keys at 1/3 and 2/3 of
    # the sorted sample; Spark SQL's range partitioner aims for ~4 rows per
    # range and keeps the three 95050 statements together.
    # batches.write.csv(...) would write 3 files, one per printer.
    print("Three contiguous ranges, in order, each one sorted.")

    # ---------------------------------------------------------------------
    section("4. Several sort columns, top N, and Spark SQL")
    # ---------------------------------------------------------------------
    # Sort by the first column, break ties with the next (the RDD version
    # needed a tuple key). This order IS the same on every run.
    print("orderBy('zip_code', 'customer'):")
    statements_df.orderBy("zip_code", "customer").show()

    # The 3 largest amounts due. With RDDs: takeOrdered(). Here: orderBy()
    # + limit(), which Spark plans as TakeOrderedAndProject: each partition
    # keeps its own top 3, and the whole table is never sorted.
    top3 = statements_df.orderBy(F.desc("amount_due")).limit(3)
    print("orderBy(desc('amount_due')).limit(3):")
    top3.show()
    print("'TakeOrderedAndProject' in plan:", "TakeOrderedAndProject" in plan_of(top3))

    statements_df.createOrReplaceTempView("statements")
    print()
    print("Spark SQL, ORDER BY ... LIMIT 3:")
    spark.sql("""
        SELECT zip_code, customer, amount_due
        FROM statements
        ORDER BY amount_due DESC
        LIMIT 3
    """).show()

    # ---------------------------------------------------------------------
    section("5. A common mistake: letting Spark guess the column types")
    # ---------------------------------------------------------------------
    # Statements usually arrive as a CSV file. spark.read.csv() also accepts
    # an RDD of CSV lines, so we don't need a file.
    lines = (["zip_code,customer,amount_due"]
             + [f"{z},{c},{a:.2f}" for z, c, a in statements])
    print("first CSV lines:", lines[:3])

    # inferSchema=True: Spark sees only digits in zip_code and guesses INT.
    guessed = spark.read.csv(sc.parallelize(lines), header=True, inferSchema=True)
    print()
    print("With inferSchema=True:")
    guessed.printSchema()
    guessed.orderBy("zip_code", "customer").show(3)
    print("'02138' became 2138: a 4-digit ZIP code the post office can't deliver.")

    # Without inferSchema, every column is a string, including the amounts:
    # compared character by character, '99.00' and '9.50' come first.
    as_text = spark.read.csv(sc.parallelize(lines), header=True)
    print()
    print("Without inferSchema (all strings), 'largest amount first':")
    as_text.orderBy(F.desc("amount_due")).show(5)
    print("The real largest amount, '310.25', is not even in the top 5.")

    # The fix for both: give the schema yourself, with the type that has the
    # order you want.
    typed = spark.read.csv(sc.parallelize(lines), header=True,
                           schema="zip_code STRING, customer STRING, amount_due DOUBLE")
    print()
    print("With an explicit schema:")
    typed.orderBy("zip_code", "customer").show(3)
    print("largest amounts correct?",
          typed.orderBy(F.desc("amount_due")).select("amount_due").limit(5).collect()
          == statements_df.orderBy(F.desc("amount_due")).select("amount_due").limit(5).collect())

    spark.stop()


if __name__ == '__main__':
    main()
