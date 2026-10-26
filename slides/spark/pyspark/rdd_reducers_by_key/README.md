# Reducers Transformations

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
| [`reducebykey_max_avg_filter.md`](reducebykey_max_avg_filter.md) | PySpark Example: Using `reduceByKey()` to Find Maximum or Average Salary |
