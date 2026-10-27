# Reducer Transformations

Lecture slides and examples on RDD reducers (`groupByKey`, `reduceByKey`, `combineByKey`).

## Contents

| Name | Description |
|---|---|
| [`6.1_reducers_in_pyspark_intro.pptx`](6.1_reducers_in_pyspark_intro.pptx) | Overview of reduction on `(key, value)` pairs: `groupByKey`, `reduceByKey`, `aggregateByKey`, `combineByKey` |
| [`6.2_reducers_in_pyspark_groupbykey.pptx`](6.2_reducers_in_pyspark_groupbykey.pptx) | `RDD.groupByKey()` reducer |
| [`6.3_reducers_in_pyspark_reducebykey.pptx`](6.3_reducers_in_pyspark_reducebykey.pptx) | `RDD.reduceByKey(f)` reducer |
| [`6.4_reducers_in_pyspark_combinebykey.pptx`](6.4_reducers_in_pyspark_combinebykey.pptx) | `RDD.combineByKey()` reducer |
| [`6.5_reducers_API_and_Tutorials_and_Videos.pptx`](6.5_reducers_API_and_Tutorials_and_Videos.pptx) | Pointers to the Spark Reducers API, docs, tutorials, and videos |
| [`key_value_reducer_transformations.md`](key_value_reducer_transformations.md) | PySpark (Key, Value) RDD Transformations: a semi-mathematical guide to the pair-RDD reduction and aggregation operations, with examples |
| [`groupbykey_vs_reducebykey_guide.md`](groupbykey_vs_reducebykey_guide.md) | When to use `groupByKey()` vs `reduceByKey()` in PySpark |
| [`find_sum_count_avg_per_key.md`](find_sum_count_avg_per_key.md) | Find sum, count, and average per key, solved four ways: `groupByKey`, `reduceByKey`, `aggregateByKey`, `combineByKey` |
| [`find_sum_count_avg_per_key_dataframe.md`](find_sum_count_avg_per_key_dataframe.md) | DataFrame version of the same problem: `groupBy().agg()`, Spark SQL, window functions, and how Spark plans the aggregation |
| [`reducebykey_max_avg_filter.md`](reducebykey_max_avg_filter.md) | PySpark Example: Using `reduceByKey()` to Find Maximum or Average Salary |
| [`reducebykey_max_avg_filter_dataframe.md`](reducebykey_max_avg_filter_dataframe.md) | DataFrame version of the same example: `groupBy().agg()` with `max`/`avg`, filtering groups (`HAVING`) vs. rows (`WHERE`) |
| [`combinebykey_customer_profile.md`](combinebykey_customer_profile.md) | A real-life case for `combineByKey()`: building customer profiles from an order log, and why it is a cleaner fit than `map()` + `reduceByKey()` or `aggregateByKey()` |
