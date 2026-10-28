# Real-Life Examples of the By-Key Operations

One real-life scenario per by-key operation, each showing **why** that operation is the
natural fit: the code reads like the business rule. Every example uses small
data you can check by hand, shows how each output field is computed, solves the
same job with the alternatives to show why they are more awkward, and ends
with a common mistake.

### The reducers

| Reducer | Example | Why this reducer fits |
|---|---|---|
| `reduceByKey()` | [Package tracking](reducebykey_package_tracking.md): the current status of each package from out-of-order scans | The value and the result are the same type (one scan); one rule, "keep the later scan", combines any two |
| `foldByKey()` | [Election-night vote totals](foldbykey_election_results.md) per county | Like `reduceByKey()` (a precinct tally and a county tally are the same type), plus a natural empty tally `{}` to start from |
| `groupByKey()` | [Median home price per zip code](groupbykey_median_home_prices.md) | A median needs **all** the values of a key; partial medians cannot be merged |
| `aggregateByKey()` | [Star ratings on product pages](aggregatebykey_product_ratings.md) | The result (a 5-counter rating box) has a natural empty value, `[0, 0, 0, 0, 0]`, with a real business meaning |
| `combineByKey()` | [Customer profiles](combinebykey_customer_profile.md) from an order log | The result (a profile) has no sensible empty value (first/last order dates), so the first real order starts it |

### Beyond the reducers: ordering by key

`sortByKey()` is not a reducer: it never combines records, it only puts them in order (the output has every input record).

| Operation | Example | Why this operation fits |
|---|---|---|
| `sortByKey()` | [Printing presorted mail](sortbykey_presorted_mail.md): statements in ZIP code order, one ZIP range per printer | The whole RDD must be in order by its key, and the output partitions are contiguous key ranges |

### Runnable programs

Each case also comes as a standalone PySpark program, with its sample data
built inside the program. Each program follows the sections of its write-up
(sample data, solution, alternatives, common mistake) and prints the same
results. Its docstring tells the business story and maps each business rule
to an argument of the operation. Each `.log` file is the output of a real
run.

| Program | Output of a real run | Write-up |
|---|---|---|
| [`reducebykey_package_tracking.py`](reducebykey_package_tracking.py) | [`.log`](reducebykey_package_tracking.log) | [Package tracking](reducebykey_package_tracking.md) |
| [`foldbykey_election_results.py`](foldbykey_election_results.py) | [`.log`](foldbykey_election_results.log) | [Election-night vote totals](foldbykey_election_results.md) |
| [`groupbykey_median_home_prices.py`](groupbykey_median_home_prices.py) | [`.log`](groupbykey_median_home_prices.log) | [Median home prices](groupbykey_median_home_prices.md) |
| [`aggregatebykey_product_ratings.py`](aggregatebykey_product_ratings.py) | [`.log`](aggregatebykey_product_ratings.log) | [Star ratings](aggregatebykey_product_ratings.md) |
| [`combinebykey_customer_profile.py`](combinebykey_customer_profile.py) | [`.log`](combinebykey_customer_profile.log) | [Customer profiles](combinebykey_customer_profile.md) |
| [`sortbykey_presorted_mail.py`](sortbykey_presorted_mail.py) | [`.log`](sortbykey_presorted_mail.log) | [Presorted mail](sortbykey_presorted_mail.md) |

Run any of them with `spark-submit`, or with plain `python3` if PySpark is
installed in your Python (`pip install pyspark`):

```
$SPARK_HOME/bin/spark-submit reducebykey_package_tracking.py
python3 reducebykey_package_tracking.py
```

### DataFrame versions

The same six cases, with the same data, solved with DataFrames and Spark SQL,
so you can compare the two APIs side by side. With DataFrames you don't write
the combining functions: you pick built-in aggregates, and Spark already knows
how to start, combine, and merge each one per partition. Each write-up maps
the RDD solution to the DataFrame one, checks the plan where it matters, and
ends with a common mistake that only happens with DataFrames.

| Case | RDD | DataFrame | Common mistake | Program | Output |
|---|---|---|---|---|---|
| [Package tracking](reducebykey_package_tracking_dataframe.md) | `reduceByKey(later_scan)` | `max(struct(time, ...))` or `max_by(col, time)` | `max()` of every column glues together a scan that never happened | [`.py`](reducebykey_package_tracking_dataframe.py) | [`.log`](reducebykey_package_tracking_dataframe.log) |
| [Election-night vote totals](foldbykey_election_results_dataframe.md) | `foldByKey({}, add_tallies)` | `explode()` each tally into rows, then `sum()`; `rollup()` for the statewide total | `NULL` is not zero: one missing candidate makes the county total `NULL` | [`.py`](foldbykey_election_results_dataframe.py) | [`.log`](foldbykey_election_results_dataframe.log) |
| [Median home prices](groupbykey_median_home_prices_dataframe.md) | `groupByKey()` + `mapValues(median)` | `median()`; `collect_list()` when you need every value | `percentile_approx()` is not the median (2900 instead of 3000) | [`.py`](groupbykey_median_home_prices_dataframe.py) | [`.log`](groupbykey_median_home_prices_dataframe.log) |
| [Star ratings](aggregatebykey_product_ratings_dataframe.md) | `aggregateByKey([0, 0, 0, 0, 0], ...)` | `count(when(stars == k, 1))` per star level | `pivot()` without the list of values drops the 1-star column | [`.py`](aggregatebykey_product_ratings_dataframe.py) | [`.log`](aggregatebykey_product_ratings_dataframe.log) |
| [Customer profiles](combinebykey_customer_profile_dataframe.md) | `combineByKey(start, add, merge)` | `count()`, `sum()`, `min()`, `max()`, `mode()` in one `agg()` | `max("product")` as the favorite gives alice a coupon for mugs | [`.py`](combinebykey_customer_profile_dataframe.py) | [`.log`](combinebykey_customer_profile_dataframe.log) |
| [Presorted mail](sortbykey_presorted_mail_dataframe.md) | `sortByKey(numPartitions=3)` | `orderBy()`; `repartitionByRange(3, key).sortWithinPartitions(key)` for one range per printer | `inferSchema` turns ZIP code `02138` into `2138` | [`.py`](sortbykey_presorted_mail_dataframe.py) | [`.log`](sortbykey_presorted_mail_dataframe.log) |

All examples were run in the PySpark shell on Spark 4.2.0. For the basics of
the four reducers on one simple problem, start with
[Find Sum, Count, and Average Per Key](../find_sum_count_avg_per_key.md).
