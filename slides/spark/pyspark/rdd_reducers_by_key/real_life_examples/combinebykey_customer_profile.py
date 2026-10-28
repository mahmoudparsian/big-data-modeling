#!/usr/bin/env python3
"""
A Real-Life Case for combineByKey(): Building Customer Profiles
================================================================

The scenario
------------
An online coffee shop keeps a log of every order. The marketing team asks:

    "For every customer, give us a PROFILE: how many orders they placed, how
     much they spent in total, the date of their first and last order, and
     their FAVORITE PRODUCT (the one they bought most often)."

They will use it to send "We miss you!" emails to customers whose last order
was long ago, and coupons for each customer's favorite product.

Each order is a (customer, (date, product, amount)) pair. The log is NOT
sorted by date (orders arrive from many web servers).

    Input:  one record per order     (customer, (date, product, amount))
    Output: one record per customer  (customer, (orders, total, first date,
                                                 last date, favorite product))

Why this is a job for combineByKey()
------------------------------------
The value V (ONE ORDER) and the result C (a CUSTOMER PROFILE) are completely
different types, and a profile has NO sensible empty value (what would the
first and last order dates of an empty profile be?). So the customer's first
real order starts the profile. Ask anyone how to build a profile from a pile
of order slips and they describe exactly the three combineByKey() functions:

    Business rule                       combineByKey() function  Type
    ----------------------------------  -----------------------  -----------
    1. Start a profile from one order   createCombiner           V -> C
    2. Add one order to a profile       mergeValue               (C, V) -> C
    3. Merge two partial profiles       mergeCombiners           (C, C) -> C

What this program shows
-----------------------
  1. the sample data and its 2 partitions
  2. the combineByKey() solution, the report, and the "We miss you!" list
  3. the alternative map() + reduceByKey(): same answer, but rule 2 vanishes
  4. why not aggregateByKey(): it would need fake dates

How to run
----------
    $SPARK_HOME/bin/spark-submit combinebykey_customer_profile.py

or, if PySpark is installed in your Python (pip install pyspark):

    python3 combinebykey_customer_profile.py

The output of a real run is in combinebykey_customer_profile.log.
The full write-up is in combinebykey_customer_profile.md.
"""
from pyspark.sql import SparkSession


def section(title):
    """Print a section header, to make the output easy to follow."""
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


# A profile C is a Python dictionary, for example:
#   {'orders': 4, 'total': 63.0, 'first': '2026-01-05', 'last': '2026-03-15',
#    'products': {'coffee': 3, 'mug': 1}}

# -------------------------------------------------------------------------
# Rule 1: start a profile from a customer's first order. V -> C
# -------------------------------------------------------------------------
def start_profile(order):
    """Build a NEW profile from one order (must never reuse an object)."""
    date, product, amount = order
    return {'orders': 1, 'total': amount,
            'first': date, 'last': date,
            'products': {product: 1}}


# -------------------------------------------------------------------------
# Rule 2: add one more order to a profile. (C, V) -> C
# -------------------------------------------------------------------------
def add_order(profile, order):
    """Add one order to a profile (changes and returns the profile).

    Orders arrive in any order, so use min()/max() for the dates instead of
    "replace the last date". Dates are ISO strings ('YYYY-MM-DD'), so min()
    and max() on the strings give the earliest and latest date.
    """
    date, product, amount = order
    profile['orders'] += 1
    profile['total'] += amount
    profile['first'] = min(profile['first'], date)
    profile['last'] = max(profile['last'], date)
    profile['products'][product] = profile['products'].get(product, 0) + 1
    return profile


# -------------------------------------------------------------------------
# Rule 3: merge two partial profiles of the same customer. (C, C) -> C
# -------------------------------------------------------------------------
def merge_profiles(p1, p2):
    """Merge profile p2 into profile p1 (changes and returns p1)."""
    p1['orders'] += p2['orders']
    p1['total'] += p2['total']
    p1['first'] = min(p1['first'], p2['first'])
    p1['last'] = max(p1['last'], p2['last'])
    for product, n in p2['products'].items():
        p1['products'][product] = p1['products'].get(product, 0) + n
    return p1


def favorite(products):
    """The product with the highest count (ties broken by product name)."""
    return min(products, key=lambda p: (-products[p], p))


def report(profile):
    """(orders, total, first order, last order, favorite product)"""
    return (profile['orders'], profile['total'],
            profile['first'], profile['last'],
            favorite(profile['products']))


def main():
    spark = SparkSession.builder.appName("combineByKey-customer-profile").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    print("Spark version:", sc.version)

    # ---------------------------------------------------------------------
    section("1. Sample data: (customer, (date, product, amount))")
    # ---------------------------------------------------------------------
    orders = [
        ('alice', ('2026-01-05', 'coffee',  12.0)),
        ('bob',   ('2026-01-07', 'tea',      8.0)),
        ('alice', ('2026-02-11', 'mug',     15.0)),
        ('alice', ('2026-01-20', 'coffee',  12.0)),
        ('bob',   ('2026-03-02', 'tea',      8.0)),
        ('alice', ('2026-03-15', 'coffee',  24.0)),
        ('carol', ('2026-02-28', 'teapot',  30.0)),
        ('bob',   ('2026-02-14', 'cookies',  5.0)),
    ]

    # 2 partitions, so the output is the same on every machine.
    # orders_rdd : RDD[(customer, (date, product, amount))]
    orders_rdd = sc.parallelize(orders, 2)
    for i, part in enumerate(orders_rdd.glom().collect()):
        print("partition", i)
        for order in part:
            print("   ", order)
    print("alice and bob have orders in BOTH partitions; carol only in one.")

    # ---------------------------------------------------------------------
    section("2. The combineByKey() solution")
    # ---------------------------------------------------------------------
    # profiles : RDD[(customer, profile)]
    profiles = orders_rdd.combineByKey(start_profile, add_order, merge_profiles)

    print("(orders, total, first order, last order, favorite product)")
    for customer, row in sorted(profiles.mapValues(report).collect()):
        print(customer, row)

    # How alice's profile is built (3 orders in partition 0, 1 in partition 1):
    #   partition 0: start_profile(coffee 01-05) -> 1 order, 12.0
    #                add_order(mug 02-11)        -> 2 orders, 27.0, last 02-11
    #                add_order(coffee 01-20)     -> 3 orders, 39.0 (dates stay)
    #   partition 1: start_profile(coffee 03-15) -> 1 order, 24.0
    #   after the shuffle: merge_profiles -> 4 orders, 63.0, 01-05 .. 03-15,
    #                                        {coffee: 3, mug: 1}

    # The "We miss you!" list: customers with no order since March 1st.
    print()
    print("We miss you! list (no order since 2026-03-01):",
          sorted(profiles.filter(lambda kv: kv[1]['last'] < '2026-03-01')
                         .keys().collect()))

    # ---------------------------------------------------------------------
    section("3. Can't we just use map() + reduceByKey()?")
    # ---------------------------------------------------------------------
    # reduceByKey(f) needs f: (C, C) -> C, so its input must ALREADY be
    # profiles. So first, EVERY order is dressed up as a fake "profile of a
    # customer who placed exactly one order":
    one_order_profiles = orders_rdd.mapValues(start_profile)
    print("first one-order profile:", one_order_profiles.first())

    profiles2 = one_order_profiles.reduceByKey(merge_profiles)
    print("mapValues(start_profile).reduceByKey(merge_profiles) same answer?",
          sorted(profiles2.mapValues(report).collect())
          == sorted(profiles.mapValues(report).collect()))
    print()
    print("Same answer, but rule 2 ('add an order') disappears: 8 fake")
    print("'profiles' for 3 customers, and one profile dictionary built for")
    print("EVERY order. combineByKey() builds one profile per customer per")
    print("partition, and the code reads like the business rules.")

    # ---------------------------------------------------------------------
    section("4. And aggregateByKey()?")
    # ---------------------------------------------------------------------
    # aggregateByKey() also accepts V != C, and its seqFunc would be
    # add_order. But it needs a zeroValue: an EMPTY profile. Its dates
    # would have to be fake: "later than everything" and "earlier than
    # everything", just so min() and max() still work.
    zero = {'orders': 0, 'total': 0.0, 'first': '9999-12-31', 'last': '0000-01-01',
            'products': {}}
    profiles3 = orders_rdd.aggregateByKey(zero, add_order, merge_profiles)
    print("fake empty profile:", zero)
    print("aggregateByKey(zero, add_order, merge_profiles)      same answer?",
          sorted(profiles3.mapValues(report).collect())
          == sorted(profiles.mapValues(report).collect()))
    print()
    print("It works, but a profile with 0 orders and a first order in the year")
    print("9999 makes no sense in the business. With combineByKey(), the")
    print("customer's first REAL order starts the profile.")

    spark.stop()


if __name__ == '__main__':
    main()
