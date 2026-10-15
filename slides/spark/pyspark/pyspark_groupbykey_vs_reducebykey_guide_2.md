# When to Use `groupByKey()` vs `reduceByKey()` in PySpark

* In PySpark, `reduceByKey()` is almost always preferred over `groupByKey()` because `reduceByKey()` performs **map-side aggregation** (combining data locally before shuffling across the network). In contrast, `groupByKey()` shuffles all raw values across the network, which can lead to high network bandwidth usage and frequent `OutOfMemoryError` exceptions on large datasets.

* Here is the definition of `RDD.groupByKey()`
and `RDD.reduceByKey(func)`:

| Reduction Transformation | Description |
|--------------------------|-------------|
|`RDD.groupByKey()`| Group the values for each key in the RDD into a single sequence.|
|`RDD.reduceByKey(func)`| Merge the values for each key using an **associative** and **commutative** reduce function (`func`).|

* However, **there are specific scenarios where `groupByKey()` is necessary or appropriate**.


---

## 1. Deduplication with Global Context & Tied Rank Resolution (Top-N per Key)

Suppose you need to select the **Top-N items** per key, or deduplicate records based on a multi-attribute priority rank, where the selection criteria depend on **global relative ranking across the entire partition or group**.

While picking a single `max()` or `min()` value can easily be done with `reduceByKey()`, picking the *top 3* items or resolving tied ranks based on relative distribution requires collecting all records for that key to perform sorting or set comparisons.

### Example: Finding Top 2 Highest Scoring Attempts per Student

First understand Python's `sorted()` function:
Here is a detailed breakdown of how Python's built-in `sorted()` function works, specifically using the **`key` argument** used in the PySpark examples.

---

### How Python's `sorted()` Works with `key`

Python's `sorted()` takes an iterable (like a list 
or tuple sequence) and returns a new sorted list. 
When you provide a `key` function, Python runs that 
function on every element to extract a comparison key.

#### Basic Syntax

```python
sorted(iterable, key=key_function, reverse=False)
```

---

### Example 1: Sorting Tuples by a Specific Element (Used in PySpark Trajectory)

In the PySpark user trajectory example, each item 
in the iterator is a tuple: `(timestamp, action)`.

```python
events = [
    ("2026-03-01 12:00:00", "checkout"),
    ("2026-03-01 09:00:00", "login"),
    ("2026-03-01 10:30:00", "add_to_cart")
]

# Sort by timestamp (the 0th index of each tuple)
sorted_events = sorted(events, key=lambda event: event[0])
print(sorted_events)
```

**Output:**

```python
[
    ('2026-03-01 09:00:00', 'login'),
    ('2026-03-01 10:30:00', 'add_to_cart'),
    ('2026-03-01 12:00:00', 'checkout')
]
```

* **How `lambda event: event[0]` works:** For each tuple `("2026-03-01 12:00:00", "checkout")`, it extracts `"2026-03-01 12:00:00"`. Python then compares those string timestamps to determine the final order.

---

### Example 2: Multi-Key Sorting (Used in Deduplication / Top-N per Key)

In the Top-N / priority example, records needed to be sorted by score (descending) and then by timestamp (ascending) to resolve ties.

```python
records = [
    {"score": 95, "timestamp": "2026-03-02", "item_id": "A"},
    {"score": 100, "timestamp": "2026-03-05", "item_id": "B"},
    # Same score as A, but earlier date
    {"score": 95, "timestamp": "2026-03-01", "item_id": "C"}  
]

# Sort by (-score) for descending, then timestamp 
# for ascending tie-breaker
sorted_records = sorted(records, key=lambda x: (-x["score"], x["timestamp"]))

print(sorted_records)
```

**Output:**

```python
[
    {'score': 100, 'timestamp': '2026-03-05', 'item_id': 'B'},
    {'score': 95,  'timestamp': '2026-03-01', 'item_id': 'C'},  # Came first due to earlier timestamp
    {'score': 95,  'timestamp': '2026-03-02', 'item_id': 'A'}
]
```

---

### Alternative: Using `operator.itemgetter` (Faster in Python)

Instead of a `lambda` function, Python's built-in `operator` 
module provides `itemgetter`, which is implemented in C and 
runs faster inside PySpark transformations:

```python
from operator import itemgetter

events = [
    ("2026-03-01 12:00:00", "checkout"),
    ("2026-03-01 09:00:00", "login")
]

# Equivalent to lambda x: x[0]
sorted_events = sorted(events, key=itemgetter(0))

```

### PySpark Solution using `sorted()`
```python
from pyspark import SparkContext

sc = SparkContext.getOrCreate()

# Dataset: (student_id, (score, submission_timestamp))
records = sc.parallelize([
    ("student_1", (85, "2026-03-01")),
    ("student_1", (92, "2026-03-02")),
    ("student_1", (78, "2026-03-03")),
    ("student_1", (95, "2026-03-04")),
    ("student_2", (88, "2026-03-01")),
    ("student_2", (91, "2026-03-02")),
])

def get_top_n_scores(attempts, n=2):
    # Requires sorting the entire set of records per student
    sorted_attempts = sorted(attempts, key=lambda x: x[0], reverse=True)
    return sorted_attempts[:n]

# groupByKey allows access to all elements to sort and slice the Top N
top_scores_rdd = records.groupByKey().mapValues(get_top_n_scores)
print(top_scores_rdd.collect())
# Output: 
[
 ('student_1', [(95, '2026-03-04'), (92, '2026-03-02')]),
 ('student_2', [(91, '2026-03-02'), (88, '2026-03-01')])
]
```

---

## 2. Sessionization / Inactivity Gap Analysis

In clickstream analysis, user events need to be grouped into discrete **sessions**. A new session starts whenever a user has been inactive for longer than a specified threshold (e.g., 30 minutes).

Because determining session boundaries requires examining the timestamp difference between **consecutive, chronologically ordered events**, this stateful calculation cannot be reduced pairwise or aggregated locally on map sides without seeing the full, ordered event stream per user.

### Example: Grouping Clickstream Events into Sessions

```python
from datetime import datetime, timedelta
from pyspark import SparkContext

sc = SparkContext.getOrCreate()

# Format: (user_id, timestamp_str)
clicks = sc.parallelize([
    ("user_1", "2026-03-01 10:00:00"),
    ("user_1", "2026-03-01 10:15:00"), # +15m (Same session)
    ("user_1", "2026-03-01 11:00:00"), # +45m (>30m limit -> New session 2)
    ("user_1", "2026-03-01 11:10:00"), # +10m (Same session 2)
    ("user_2", "2026-03-01 09:00:00"),
])

def create_sessions(timestamps, max_gap_minutes=30):
    # Parse and sort timestamps chronologically
    fmt = "%Y-%m-%d %H:%M:%S"
    dt_list = sorted([datetime.strptime(ts, fmt) for ts in timestamps])
    
    sessions = []
    current_session = [dt_list[0]]
    
    for prev, curr in zip(dt_list[:-1], dt_list[1:]):
        if curr - prev > timedelta(minutes=max_gap_minutes):
            sessions.append(current_session)
            current_session = [curr]
        else:
            current_session.append(curr)
            
    sessions.append(current_session)
    return [[dt.strftime(fmt) for dt in sess] for sess in sessions]

# groupByKey allows session boundaries to be evaluated over the full sorted dataset per user
user_sessions = clicks.groupByKey().mapValues(create_sessions)
print(user_sessions.collect())
```

---

## 3. The Operation is Non-Associative or Non-Commutative

`reduceByKey()` requires a binary associative and commutative function $f(a, b)$ such that:

* **Associative:** $f(f(a, b), c) = f(a, f(b, c))$
* **Commutative:** $f(a, b) = f(b, a)$

If your operation depends on the **exact order** of elements, or cannot be broken down into a binary pairwise combination, `reduceByKey()` cannot be used.

### Example: Calculating a Time-Series Path / Trajectory

Suppose you have customer log data, and for each user, you need to sort their events chronologically to compute a custom time-series path or sequence-based feature.

```python
rdd = sc.parallelize([
    ("user_1", ("2026-03-01", "login")),
    ("user_1", ("2026-03-02", "checkout")),
    ("user_1", ("2026-03-01 10:00", "add_to_cart")),
    ("user_2", ("2026-03-01", "login"))
])

# Group all events for a user, then sort by timestamp to evaluate user path
def analyze_user_journey(events):
    # Sort events chronologically by timestamp
    sorted_events = sorted(events, key=lambda x: x[0])
    actions = [action for ts, action in sorted_events]
    # Return path signature (e.g., 'login -> add_to_cart -> checkout')
    return " -> ".join(actions)

# groupByKey allows you to inspect/sort the ENTIRE sequence per key
journey_rdd = rdd.groupByKey().mapValues(analyze_user_journey)
print(journey_rdd.collect())
```

---

## 4. Output Requires an Exact Non-Aggregated Collection or Complex Structure

If your target deliverable is literally a grouped list, 
set, or nested structure (e.g., preparing data for a 
downstream machine learning library, JSON export, or 
graph algorithm), `groupByKey()` provides direct access 
to the iterable.

### Example: Preparing an Adjacency List for Graph Algorithms

Building an adjacency list where each node points to a 
list of target neighbors:

```python
# Graph edges: (source_node, target_node)
edges = sc.parallelize([
    ("A", "B"), ("A", "C"), ("B", "D"), ("A", "E")
])

# Generate adjacency list: ("A", ["B", "C", "E"])
adj_list = edges.groupByKey().mapValues(list)
print(adj_list.collect())
```

---

## 5. Calculating Complex Multi-Value Statistics in a Single Pass

While simple metrics like sum, max, or min work 
seamlessly with `reduceByKey()`, computing multi-value 
structural metrics (like median, exact percentile, or 
mode) requires access to the full distribution per key.

### Example: Calculating Median or Mode per Key

Computing the exact median value per category requires 
sorting all elements per key, which cannot be expressed 
as a pairwise reduction.

```python
import statistics

scores = sc.parallelize([
    ("math", 80), 
    ("math", 95), 
    ("math", 60), 
    ("math", 100),
    ("physics", 70), 
    ("physics", 85), 
    ("physics", 90)
])

# Median requires full collection of elements per key
medians = scores.groupByKey().mapValues(lambda vals: statistics.median(vals))
print(medians.collect())
```

---

## Performance Summary & Alternatives

If you are considering `groupByKey()` purely to 
compute custom aggregates (such as standard deviation, 
average, or custom objects) where intermediate map-side 
aggregation *is* mathematically possible, use 
**`aggregateByKey()`**  or **`combineByKey()`** instead. 
They provide the map-side aggregation efficiency of 
`reduceByKey()` without restricting you to a single 
output type matching the input type.

| Use Case | Recommended Method | 
| ----- | ----- | 
| **Sum, Count, Min, Max** | `reduceByKey()` | 
| **1. Average, <br> 2. Standard Deviation** | `aggregateByKey()` / `combineByKey()` | 
| **1. Top-N per key <br> 2. Sessionization, <br> 3. Ordered sequences, <br> 4. Medians, <br> 5. Adjacency Lists** | `groupByKey()` | 

---

## Summary

In PySpark, you should almost always prefer reduceByKey() over groupByKey() because reduceByKey() performs local aggregation (combiner step) on each partition before shuffling data across the network.

However, there are a few specific, niche situations where groupByKey() is necessary or inherently more appropriate because you genuinely need to operate on the entire collection of values associated with a key, rather than reducing them down to a single value.