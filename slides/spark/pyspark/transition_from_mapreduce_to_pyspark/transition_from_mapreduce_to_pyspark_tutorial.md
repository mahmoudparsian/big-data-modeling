# Transitioning from MapReduce to PySpark: <br> A Practical Guide

This tutorial is designed for developers familiar with the classic Hadoop MapReduce paradigm (`map()`, `shuffle/sort`, `combine()`, `reduce()`) who want to transition to **PySpark**. 

Unlike traditional MapReduce, which writes intermediate states to disk after every job stage, Apache Spark holds data in memory across pipeline operations whenever possible, resulting in significantly faster performance and a much richer, expressive API.

---

## 1. Mental Model: MapReduce vs. Spark

To bridge the gap between traditional MapReduce and Spark, keep the following core conceptual mappings in mind:

| MapReduce Concept | Spark Equivalent |
| :--- | :--- |
| **InputSplit / RecordReader** | Data Partition / DataSource Reader |
| **Mapper (`map`)** | `map()`, `flatMap()`, `filter()` |
| **Combiner (`combine`)** | `reduceByKey()`, `aggregateByKey()`, or `treeAggregate()` |
| **Shuffle & Sort** | Wide Dependency / Shuffle (triggered by `groupByKey()`, `join()`, etc.) |
| **Reducer (`reduce`)** | `reduceByKey()`, `groupByKey()`, or DataFrame `groupBy().agg()` |
| **OutputFormat** | Actions like `saveAsTextFile()` or `write.format().save()` |
| **HDFS Disk I/O** | In-Memory Data Sharing via Lineage Graphs (Lazy Evaluation) |

---

## 2. Core Architectural Concepts

### Lazy Evaluation, Lineage, Transformations vs. Actions

* **Transformations** (e.g., `map`, `filter`, `select`): Define a new dataset derived from an existing one. They are **lazy**—Spark records the operation in a **Logical Plan / Lineage Graph** (Directed Acyclic Graph or DAG) rather than executing it immediately.
* **Actions** (e.g., `collect`, `count`, `take`, `saveAsTextFile`): Trigger the execution of the DAG. Spark evaluates the entire pipeline, optimizes it, splits it into stages, and submits tasks across the cluster.

---

## 3. Part I: Resilient Distributed Datasets (RDDs)

An **RDD** is Spark’s core low-level abstraction: an immutable, fault-tolerant collection of elements partitioned across cluster nodes. It is closest to raw MapReduce programming.

### 3.1 Initializing PySpark

```python
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MapReduce_To_PySpark_RDD") \
    .master("local[*]") \
    .getOrCreate()

# Access underlying SparkContext for RDD operations
sc = spark.sparkContext
```

---

### 3.2 Basic Level: Word Count (MapReduce Classic)

Let's start with the quintessential MapReduce example: Word Count.

#### MapReduce Logic Review
1. **Map**: Parse text into key-value pairs `(word, 1)`.
2. **Combine**: Local aggregation per mapper `(word, sum)`.
3. **Reduce**: Global aggregation per word key.

#### PySpark RDD Implementation

```python
# Sample dataset
raw_lines = [
    "spark mapreduce spark python",
    "hadoop mapreduce python",
    "spark dataframe rdd"
]

# Step 1: Distribute data across the cluster
rdd = sc.parallelize(raw_lines)

# Step 2: flatMap (1-to-many mapper) -> tokenize text
words = rdd.flatMap(lambda line: line.split(" "))

# Step 3: map (1-to-1 mapper) -> emit (word, 1) pairs
word_pairs = words.map(lambda word: (word, 1))

# Step 4: reduceByKey -> acts as COMBINER and REDUCER combined!
# Note: reduceByKey performs map-side aggregation automatically.
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

# Action: Collect results to driver
results = word_counts.collect()
print("Word Count Output:", results)
```

**Key Insight:** `reduceByKey` replaces both the Combiner and Reducer in MapReduce. It performs local map-side aggregation before sending data across the network (shuffle), avoiding the network bottlenecks common in naive MapReduce jobs.

---

### 3.3 Intermediate Level: Filtering, Custom Keys, and `aggregateByKey`

In Hadoop MapReduce, custom aggregations often require complex `Writable` types, custom partitioners, and custom combiners. PySpark provides built-in functional primitives to handle these seamlessly.

#### Scenario: Computing Average Score per Subject
Suppose we want to find the average score for each subject from a stream of `(subject, score)` tuples.

If we use `groupByKey()`, Spark collects all scores for a key across the network before aggregating, which can easily trigger Out-Of-Memory (OOM) errors on large datasets (similar to doing a raw shuffle without a combiner in MapReduce). Instead, we use **`aggregateByKey`**.

```python
scores = sc.parallelize([
    ("Math", 85), ("Math", 95), ("Physics", 80),
    ("Math", 90), ("Physics", 90), ("Chemistry", 70)
])

# Goal: Calculate Average = Sum / Count

# Zero Value: Initial accumulator state for a key: (total_score, count)
zero_value = (0, 0)

# SeqOp (Sequence Operator): How to merge a new value into the local accumulator (within a partition)
def seq_op(accumulator, element):
    return (accumulator[0] + element, accumulator[1] + 1)

# CombOp (Combine Operator): How to merge two accumulators from different partitions (across partitions)
def comb_op(acc1, acc2):
    return (acc1[0] + acc2[0], acc1[1] + acc2[1])

# Run aggregateByKey (Map-side combine + Reduce step)
aggregated = scores.aggregateByKey(zero_value, seq_op, comb_op)

# Final transformation: Map to calculate average
averages = aggregated.mapValues(lambda total_and_count: total_and_count[0] / total_and_count[1])

print("Average Scores:", averages.collect())
```

---

### 3.4 Intermediate+ Level: Map-Side Joins & Broadcast Variables

In MapReduce, performing a **Map-Side Join** (Replicated Join) requires manually passing smaller lookup datasets through Hadoop's `DistributedCache`.

In PySpark, this is accomplished using **Broadcast Variables**.

#### Scenario: Enriching Transaction Logs with User Profiles

```python
# Large dataset (distributed across cluster)
transactions = sc.parallelize([
    (101, "ProdA", 150.0),
    (102, "ProdB", 200.0),
    (101, "ProdC", 50.0)
])

# Small dataset (lookup table)
user_profiles = {
    101: {"name": "Alice", "tier": "Gold"},
    102: {"name": "Bob", "tier": "Silver"}
}

# Broadcast the small lookup dataset to all worker nodes
broadcast_users = sc.broadcast(user_profiles)

def enrich_transaction(record):
    user_id, prod, amount = record
    # Lookup directly from local memory without triggering a shuffle
    user_info = broadcast_users.value.get(user_id, {"name": "Unknown", "tier": "None"})
    return (user_id, user_info["name"], user_info["tier"], prod, amount)

enriched_txns = transactions.map(enrich_transaction)
print("Enriched Transactions:", enriched_txns.collect())
```

---

## 4. Part II: PySpark DataFrames & Catalyst Optimizer

While RDDs offer low-level control, **DataFrames** (built on Spark’s Structured APIs) provide optimized execution via the **Catalyst Optimizer** and **Tungsten Engine**.

DataFrames introduce named columns and strong typing concepts, similar to relational tables or pandas DataFrames, while remaining fully distributed.

---

### 4.1 Basic Level: Schema Definition & Basic Queries

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pyspark.sql.functions as F

# Define Schema (Best practice vs schema inference)
schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True)
])

data = [
    (1, "Alice", "Engineering", 95000.0),
    (2, "Bob", "Engineering", 80000.0),
    (3, "Charlie", "Sales", 60000.0),
    (4, "Diana", "Sales", 75000.0),
    (5, "Evan", "Marketing", 50000.0)
]

df = spark.createDataFrame(data, schema=schema)

# Show Schema & Top Rows
df.printSchema()
df.show()

# Basic Operations: Filter and Select
eng_df = df.filter(F.col("department") == "Engineering") \
           .select("employee_id", "name", "salary")

eng_df.show()
```

---

### 4.2 Intermediate Level: Aggregations & GroupBy

In DataFrames, you don't need manual map-side combiners or `aggregateByKey` functions; Catalyst automatically plans optimal shuffles and local aggregations.

```python
# Grouping and Aggregating
dept_summary = df.groupBy("department").agg(
    F.count("employee_id").alias("employee_count"),
    F.avg("salary").alias("avg_salary"),
    F.max("salary").alias("max_salary")
)

dept_summary.show()
```

#### DataFrame Join Operations

```python
dept_data = [
    ("Engineering", "Building A"),
    ("Sales", "Building B"),
    ("Marketing", "Building C")
]
dept_schema = ["department", "location"]

dept_df = spark.createDataFrame(dept_data, dept_schema)

# Join DataFrames
joined_df = df.join(dept_df, on="department", how="inner")
joined_df.show()
```

---

### 4.3 Intermediate+ Level: Window Functions & Spark SQL Engine

In traditional MapReduce, computing running totals, rankings, or moving averages required writing complex secondary sorts and custom partitioning algorithms. 

PySpark **Window Functions** allow you to perform these operations cleanly and efficiently.

#### Scenario: Rank Employees by Salary within Each Department

```python
from pyspark.sql.window import Window

# Define Window Specification (partitioned by department, ordered by salary desc)
window_spec = Window.partitionBy("department").orderBy(F.col("salary").desc())

# Apply Window Function
ranked_df = df.withColumn("rank", F.rank().over(window_spec)) \
              .withColumn("dense_rank", F.dense_rank().over(window_spec))

ranked_df.show()
```

#### Native Spark SQL Query Execution

You can also write standard SQL queries directly over your DataFrames by registering temporary views:

```python
# Register temporary view
df.createOrReplaceTempView("employees")

# Execute SQL Query
sql_results = spark.sql("""
    SELECT 
        department, 
        AVG(salary) as average_salary
    FROM employees
    GROUP BY department
    HAVING average_salary > 65000
""")

sql_results.show()
```

---

## 5. Summary Checklist: Transitioning Mindset

1. **Avoid `groupByKey` in RDDs**: Use `reduceByKey` or `aggregateByKey` to ensure map-side combining occurs before shuffles.
2. **Prefer DataFrames over RDDs**: Unless you are working with unformatted raw binary streams or custom legacy Java objects, DataFrames run significantly faster thanks to the Catalyst Optimizer.
3. **Minimize Shuffling**: Use broadcast joins (`F.broadcast(df_small)`) when joining a large dataset with a small reference dataset.
4. **Understand Actions vs. Transformations**: Remember that no work is executed until an action (`collect`, `count`, `save`, `show`) is called on the pipeline.