# Distinct Visitors per Day Using PySpark RDDs

Target: **Apache Spark 4.2.0**, classic PySpark. Complete runnable solution:
[distinct_visitors_rdd.py](distinct_visitors_rdd.py). Installation, data,
and shared checks are in the [README](README.md).

## 1. Problem and Sample Inputs

Count distinct user IDs per date. A visit is not a unique visitor: one
user may visit several pages, and duplicates can cross input partitions.

[Partition 1](data/partition_1.csv) contains these six `(date, user)` pairs:

```text
(2026-09-01, U1)
(2026-09-01, U2)
(2026-09-01, U1)
(2026-09-02, U1)
(2026-09-02, U3)
(2026-09-02, U3)
```

[Partition 2](data/partition_2.csv) contains seven:

```text
(2026-09-01, U2)
(2026-09-01, U3)
(2026-09-01, U3)
(2026-09-02, U3)
(2026-09-02, U4)
(2026-09-02, U4)
(2026-09-02, U5)
```

Local distinct counts would be `(2, 2)` for the first date and `(2, 3)`
for the second. Adding them gives **4 and 5**, both too large. We must
preserve user identity until duplicates have been removed globally.

## 2. Read and Parse the CSV Files

The executable creates a `SparkSession`, then uses its `SparkContext`.
The computation itself uses RDDs only; creating a session does not make
it a DataFrame solution.

```python
lines = spark.sparkContext.textFile(input_path, minPartitions=2)
pairs = lines.mapPartitions(parse_partition)
```

`parse_partition` is defined in the script. It uses Python's CSV reader,
skips each file's header, checks the four-field shape, trims date/user
strings, and excludes empty keys. It yields `(date, user_id)` tuples.
It assumes one physical line per CSV record.

`minPartitions=2` is a requested minimum, not an exact mapper assignment.
The worked partition lists above describe the data files, not a promise
about Spark's file splitting.

## 3. First Aggregation: Deduplicate Composite Keys

```python
unique_pairs = pairs.map(lambda pair: (pair, 1)).reduceByKey(max)
```

The intermediate record has this shape:

```text
((date, user_id), presence_marker)
```

For example, all three visits from U3 on September 2 contribute:

```text
((2026-09-02, U3), 1)
((2026-09-02, U3), 1)
((2026-09-02, U3), 1)
```

`reduceByKey(max)` merges them into one record with marker `1`. Equal
full composite keys meet in the same reduce partition. The marker means
“present,” not a visit count.

For the illustrated source split, complete local combining could reduce
6 + 7 input records to 4 + 5 records before the global merge. The global
result has **seven** unique pairs:

```text
((2026-09-01, U1), 1)
((2026-09-01, U2), 1)
((2026-09-01, U3), 1)
((2026-09-02, U1), 1)
((2026-09-02, U3), 1)
((2026-09-02, U4), 1)
((2026-09-02, U5), 1)
```

`reduceByKey` performs local merging before shuffling. `max` is an
associative, commutative merge with the same input/output value type;
there is no custom `combine()` function to register.
See the [Spark 4.2.0 reduceByKey API](https://spark.apache.org/docs/4.2.0/api/python/reference/api/pyspark.RDD.reduceByKey.html).

## 4. Second Aggregation: Count by Date

Once each pair is unique, discard the user ID and emit a count of one:

```python
from operator import add

result = unique_pairs.map(lambda item: (item[0][0], 1)).reduceByKey(add)
```

Here `item[0]` is the `(date, user_id)` key, and `item[0][0]` is its date.
Conceptually, the grouped counts are:

```text
2026-09-01 → [1, 1, 1]    → 3
2026-09-02 → [1, 1, 1, 1] → 4
```

Local summation may replace some `1` values with partial sums before
shuffle. This is correct because distinctness was already established.

## 5. Complete Core Function

The runnable script uses this function:

```python
from operator import add


def distinct_visitors(pairs):
    unique_pairs = pairs.map(lambda pair: (pair, 1)).reduceByKey(max)
    return unique_pairs.map(lambda item: (item[0][0], 1)).reduceByKey(add)
```

An equivalent shorter design uses `pairs.distinct()` followed by mapping
to `(date, 1)` and summing. The explicit presence-marker design makes the
relationship to the MapReduce combiner easier to see.

## 6. Run and Expected Output

After completing the [setup](README.md#1-setup):

```text
"$SPARK_HOME/bin/spark-submit" --master "local[2]" distinct_visitors_rdd.py
```

Ignoring Spark startup logs:

```text
Spark version: 4.2.0
Spark master: local[2]
date,distinct_visitors
2026-09-01,3
2026-09-02,4
```

The program collects only the tiny daily summary and sorts it on the
driver for display. Do not collect a production-scale user list or large
summary; use distributed output such as `saveAsTextFile` instead.

## 7. How This Relates to Two MapReduce Jobs

The two logical aggregations are the same as the handwritten example:
first group by `(date, user_id)`, then group by `date`. Spark expresses
both within one application's lineage and need not save intermediate
results to external storage.

Transformations are lazy. The final `collect()` triggers execution.
These two key-changing aggregations involve shuffle boundaries in this
pipeline. Do not equate two `reduceByKey` calls with exactly two Spark
jobs: jobs are associated with actions, and stages with dependencies.
Use the Spark UI to inspect the actual execution.

## 8. Mistakes and Exercises

- Counting original events produces 6 and 7, not 3 and 4.
- Summing per-partition distinct counts produces 4 and 5 for the shown split.
- Deduplicating by user alone loses the date dimension.
- Grouping all users into a Python set per date can require large memory.
  The composite-key design distributes deduplication, although the final
  aggregation can still have hot dates.

Try these modifications:

1. Add U4 on September 1: the results become 4 and 4.
2. Count unique users for the entire period: deduplicate by user, then
   count; the answer is 5, not the daily sum of 7.
3. Count distinct users per page per day: preserve page while parsing,
   deduplicate `(date, page, user_id)`, then count by `(date, page)`.

Run `"$SPARK_HOME/bin/spark-submit" --master "local[2]" test_solutions.py` to check this
solution against the expected results. A companion
[DataFrame solution](../../dataframes_examples/distinct_visitor_per_day/Distinct_Visitors_DataFrames.md)
solves the same problem in its own sibling folder; its test checks that
solution independently, so the two are no longer cross-checked against
each other in one run.
