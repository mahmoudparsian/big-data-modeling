# PySpark (Key, Value) RDD Transformations: <br> A Semi-Mathematical Guide

In Apache Spark, operations on Resilient Distributed Datasets (RDDs) containing key-value pairs—often written as $\text{RDD}[(K, V)]$—are foundational to distributed data processing. 

This guide formulates the four main pair-RDD reduction and aggregation operations semi-mathematically, explains their execution mechanisms (including shuffle efficiency), and provides realistic PySpark examples.

---

## Math Notation & Terminology Quick Reference

* $\mathcal{K}, \mathcal{V}, \mathcal{C}, \mathcal{U}$: Sets representing Keys, Values, Combiner types, and Accumulator/Output types, respectively.
* $K_i \in \mathcal{K}, V_i \in \mathcal{V}$.
* $[(K, V)]$: A distributed collection (RDD) of tuples.
* $f: (A, B) \to C$: A function taking inputs of type $A$ and $B$ and returning type $C$.
* $\oplus$: An associative (and typically commutative) binary combination operator.
* $\text{Partition}_p$: A chunk of the RDD stored on a single worker node.

---


## 1. `groupByKey`

### Mathematical Formulation

$$\text{groupByKey} : \text{RDD}[(K, V)] \longrightarrow \text{RDD}[(K, \text{Iterable}[V])]$$

Given an input RDD dataset $D = \{(k_i, v_i)\}_{i=1}^N$:

$$\text{groupByKey}(D) = \left\{ \left( k, [v_i \mid (k_i, v_i) \in D \text{ and } k_i = k] \right) \;\middle|\; k \in \{ k_1, \dots, k_N \} \right\}$$


### Execution & Shuffle Mechanism

1. **Map Phase (Pre-shuffle):** No local aggregation occurs. All pairs $(k, v)$ are collected per partition.
2. **Shuffle Phase:** All values $v$ associated with key $k$ across all partitions are transferred over the network to the partition assigned to key $k$.
3. **Reduce Phase:** Values are gathered into a single memory buffer $\text{Iterable}[V]$.

> **Warning:** High risk of `OutOfMemoryError` (OOM) if a single key has millions of values, because the entire list of values for a key must fit into a single executor's memory.

### Realistic Example: Grouping User Logs by ID

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDD-GroupByKey").getOrCreate()
sc = spark.sparkContext

# Log data: (user_id, page_visited)
user_logs = sc.parallelize([
    ("user_101", "home"),
    ("user_102", "cart"),
    ("user_101", "product_a"),
    ("user_101", "checkout"),
    ("user_102", "home")
])

# Group all pages visited by user
grouped_logs = user_logs.groupByKey()

# Materialize result as list
result = grouped_logs.mapValues(list).collect()
print(result)
# Output: 
# [
#  ('user_101', ['home', 'product_a', 'checkout']),
#  ('user_102', ['cart', 'home'])
# ]
```

---

## 2. `reduceByKey`

### Mathematical Formulation

$$\text{reduceByKey}(f: (V, V) \to V) : \text{RDD}[(K, V)] \longrightarrow \text{RDD}[(K, V)]$$

Given an associative binary function $f: V \times V \to V$ (where $f(v_1, v_2) = v_1 \oplus v_2$):

$$\text{reduceByKey}(f, D) = \left\lbrace \left( k, \bigoplus_{(k_i, v_i) \in D, k_i = k} v_i \right) \middle\vert{} k \in \{k_i\}_{i=1}^N \right\rbrace$$


#### Mathematical Property Requirement
The reduction function $f$ **must** be associative:
$$(a \oplus b) \oplus c = a \oplus (b \oplus c)$$
Ideally, it should also be commutative ($a \oplus b = b \oplus a$) so partition processing order does not alter results.

### Execution & Shuffle Mechanism

1. **Map Phase (Map-side Combine):** Applies $f$ locally within each partition *before* shuffling:
   $$v_{\text{local}}(k) = \bigoplus_{v_i \in \text{Partition}_p, k_i=k} v_i$$
2. **Shuffle Phase:** Only the locally combined single tuple $(k, v_{\text{local}})$ per partition is sent over the network.
3. **Reduce Phase:** Applies $f$ across the intermediate values received from all partitions to compute final $v(k)$.

### Realistic Example: Word Count / Event Aggregation

```python
# Event data: (sensor_id, error_count)
sensor_errors = sc.parallelize([
    ("sensor_A", 1), ("sensor_B", 3), ("sensor_A", 2),
    ("sensor_A", 5), ("sensor_B", 1), ("sensor_C", 4)
])

# Sum total errors per sensor locally and globally
total_errors = sensor_errors.reduceByKey(lambda x, y: x + y)

print(total_errors.collect())
# Output: 
# [
#  ('sensor_A', 8), 
#  ('sensor_B', 4), 
#  ('sensor_C', 4)
# ]
```

---

## 3. `combineByKey`

`combineByKey` is the underlying general primitive upon which `reduceByKey` and `aggregateByKey` are built. It allows the output/accumulator type $C$ to differ from the input value type $V$.

### Mathematical Formulation

$$\text{combineByKey}(\text{createCombiner}, \text{mergeValue}, \text{mergeCombiners})$$

Where the input signatures are:

1. **`createCombiner`** $: V \longrightarrow C$
   Converts an initial single value $v \in V$ into a combiner structure $c \in C$.
   
2. **`mergeValue`** $: (C, V) \longrightarrow C$
   Merges an incoming value $v \in V$ into an existing local combiner $c \in C$.
   
3. **`mergeCombiners`** $: (C, C) \longrightarrow C$
   Merges two combiners $c_1, c_2 \in C$ coming from different partitions.

$$\text{combineByKey} : \text{RDD}[(K, V)] \longrightarrow \text{RDD}[(K, C)]$$

### Step-by-Step Mathematical Flow

For a partition with values $\{v_1, v_2, \dots, v_m\}$ for key $k$:

1. Initial value: $c^{(1)} = \text{createCombiner}(v_1)$
2. For $i = 2 \dots m$: $c^{(i)} = \text{mergeValue}(c^{(i-1)}, v_i)$
3. Across partitions $P_1, P_2$: $c_{\text{final}} = \text{mergeCombiners}(c_{P_1}, c_{P_2})$

### Realistic Example: Computing Average Score per Student Without Extra Passes

To calculate an average, we need both $\sum x$ (sum) and $N$ (count). The value is `score` ($\mathbb{R}$), but the combiner structure is a pair $(\text{sum}, \text{count}) \in \mathbb{R} \times \mathbb{Z}^+$.

```python
# Student Scores: (student_name, test_score)
scores_rdd = sc.parallelize([
    ("Alice", 85.0), ("Bob", 75.0), ("Alice", 95.0),
    ("Bob", 80.0), ("Alice", 90.0), ("Bob", 90.0)
])

# 1. createCombiner: v -> (v, 1)
create_combiner = lambda score: (score, 1)

# 2. mergeValue: (combiner, v) -> updated combiner
merge_value = lambda acc, score: (acc[0] + score, acc[1] + 1)

# 3. mergeCombiners: (combiner1, combiner2) -> unified combiner
merge_combiners = lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])

# Apply combineByKey
combined = scores_rdd.combineByKey(
    create_combiner,
    merge_value,
    merge_combiners
)

# Compute final average: (sum, count) -> sum / count
averages = combined.mapValues(lambda acc: acc[0] / acc[1])

print(averages.collect())
# Output: 
# [
#  ('Alice', 90.0), 
#  ('Bob', 81.66666666666667)
# ]
```

---

## 4. `aggregateByKey`

`aggregateByKey` provides a cleaner, explicit zero-value initialization for the output structure compared to `combineByKey`.

### Mathematical Formulation

$$\text{aggregateByKey}(\text{zeroValue})(seqOp, combOp)$$

Where:

* **$\text{zeroValue} \in U$**: Neutral element of type $U$ (e.g., $0$ for addition, $1$ for multiplication, $\emptyset$ for sets).
* **`seqOp`** $: (U, V) \longrightarrow U$: Accumulates value $V$ into intermediate accumulator $U$ within a partition.
* **`combOp`** $: (U, U) \longrightarrow U$: Combines two intermediate accumulators $U$ across partitions.

$$\text{aggregateByKey} : \text{RDD}[(K, V)] \longrightarrow \text{RDD}[(K, U)]$$

#### Mathematical Identity Requirement
For any $u \in U$:
$$\text{combOp}(\text{zeroValue}, u) = u$$

### Execution Mechanism

```
[Partition 1] ──> seqOp(zeroValue, v1) ──> seqOp(acc1, v2) ──┐ (accP1)
                                                              ├──> combOp(accP1, accP2) ──> Result
[Partition 2] ──> seqOp(zeroValue, v3) ──> seqOp(acc2, v4) ──┘ (accP2)
```

### Realistic Example: Multi-Metric Customer Analytics

Calculate both total purchase amount and unique transactions count per customer.

```python
# Customer transactions: (customer_id, purchase_amount)
transactions = sc.parallelize([
    ("cust_A", 120.50),
    ("cust_B", 45.00),
    ("cust_A", 80.00),
    ("cust_A", 15.00),
    ("cust_B", 100.00)
])

# Target accumulator type U: (total_amount: float, transaction_count: int)
zero_value = (0.0, 0)

# Sequence operation: Add value to partition accumulator
seq_op = lambda acc, amount: (acc[0] + amount, acc[1] + 1)

# Combine operation: Merge accumulators across partitions
comb_op = lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])

customer_stats = transactions.aggregateByKey(
    zero_value,
    seq_op,
    comb_op
)

print(customer_stats.collect())
# Output: 
# [
#  ('cust_A', (215.5, 3)), 
#  ('cust_B', (145.0, 2))
# ]
```

---

## Comparison Summary

| Operation | Input Tuple | Output Tuple | Map-side Combine? | Type Flexibility |
| :--- | :--- | :--- | :--- | :--- |
| **`groupByKey`** | $(K, V)$ | $(K, \text{Iterable}[V])$ | **No** (High Network I/O) | $V \to \text{Iterable}[V]$ |
| **`reduceByKey`** | $(K, V)$ | $(K, V)$ | **Yes** (Efficient) | Requires output type $= V$ |
| **`combineByKey`** | $(K, V)$ | $(K, C)$ | **Yes** (Efficient) | Output type $C \neq V$ via 3 explicit functions |
| **`aggregateByKey`** | $(K, V)$ | $(K, U)$ | **Yes** (Efficient) | Output type $U \neq V$ via `zeroValue` + 2 functions |

### Hierarchy of Abstraction
$$\text{reduceByKey} \subset \text{aggregateByKey} \subset \text{combineByKey}$$

* `reduceByKey` is `aggregateByKey` where $U = V$ and $\text{seqOp} = \text{combOp}$.
* `aggregateByKey` is `combineByKey` where `createCombiner(v)` $= \text{seqOp}(\text{zeroValue}, v)$.
