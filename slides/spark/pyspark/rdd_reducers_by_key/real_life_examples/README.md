# Real-Life Examples of the Reducers

One real-life scenario per reducer, each showing **why** that reducer is the
natural fit: the code reads like the business rule. Every example uses small
data you can check by hand, shows how each output field is computed, solves the
same job with the other reducers to show why they are more awkward, and ends
with a common mistake.

| Reducer | Example | Why this reducer fits |
|---|---|---|
| `reduceByKey()` | [Package tracking](reducebykey_package_tracking.md): the current status of each package from out-of-order scans | The value and the result are the same type (one scan); one rule, "keep the later scan", combines any two |
| `foldByKey()` | [Election-night vote totals](foldbykey_election_results.md) per county | Like `reduceByKey()` (a precinct tally and a county tally are the same type), plus a natural empty tally `{}` to start from |
| `groupByKey()` | [Median home price per zip code](groupbykey_median_home_prices.md) | A median needs **all** the values of a key; partial medians cannot be merged |
| `aggregateByKey()` | [Star ratings on product pages](aggregatebykey_product_ratings.md) | The result (a 5-counter rating box) has a natural empty value, `[0, 0, 0, 0, 0]`, with a real business meaning |
| `combineByKey()` | [Customer profiles](combinebykey_customer_profile.md) from an order log | The result (a profile) has no sensible empty value (first/last order dates), so the first real order starts it |

All examples were run in the PySpark shell on Spark 4.2.0. For the basics of
the four reducers on one simple problem, start with
[Find Sum, Count, and Average Per Key](../find_sum_count_avg_per_key.md).
