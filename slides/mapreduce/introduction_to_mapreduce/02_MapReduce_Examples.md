# MapReduce: Complete Scenarios and Step-by-Step Solutions

This document walks through two MapReduce examples in full detail:

1. **Basic example** — Word Count (the canonical example, used to introduce the mechanics)
2. **Intermediate example** — Sales Revenue by Region and Category (a composite-key aggregation with a combiner and a custom partitioner, closer to a real business use case)

Both examples follow the same five phases: 

```
Input Split 
→ Map 
→ Combine (optional) 
→ Shuffle & Sort 
→ Reduce 
→ Output
```

---

## Part 1: Basic Example — Word Count

### Scenario

You have three short text documents and want to count how many times each word appears across all of them.

**Input documents:**

```
doc1.txt: "the cat sat on the mat"
doc2.txt: "the dog sat on the log"
doc3.txt: "the cat and the dog played"
```

### Step 1 — Input Splitting

The MapReduce framework splits the input into chunks and assigns one chunk (an "input split") to each Mapper. Here, one document = one split = one Mapper.

| Split | Assigned to | Content |
|---|---|---|
| Split 1 | Mapper 1 | "the cat sat on the mat" |
| Split 2 | Mapper 2 | "the dog sat on the log" |
| Split 3 | Mapper 3 | "the cat and the dog played" |

### Step 2 — Map Phase

Each Mapper independently tokenizes its line and emits a key-value pair `(word, 1)` for every word occurrence.

**Map pseudocode:**

```
function MAP(document_id, document_text):
    for each word w in split(document_text):
        emit(w, 1)
```

**Mapper 1 output** (from "the cat sat on the mat"):

```
(the, 1) (cat, 1) (sat, 1) (on, 1) (the, 1) (mat, 1)
```

**Mapper 2 output** (from "the dog sat on the log"):

```
(the, 1) (dog, 1) (sat, 1) (on, 1) (the, 1) (log, 1)
```

**Mapper 3 output** (from "the cat and the dog played"):

```
(the, 1) (cat, 1) (and, 1) (the, 1) (dog, 1) (played, 1)
```

### Step 3 — Combine Phase (optional local reduce)

A Combiner runs on each Mapper's node before data is sent across the network. It pre-aggregates values sharing the same key, cutting network traffic.

**Combiner pseudocode** (identical logic to the Reducer, applied locally):

```
function COMBINE(word, list_of_counts):
    emit(word, sum(list_of_counts))
```

| Mapper | Combined output |
|---|---|
| Mapper 1 | (the, 2) (cat, 1) (sat, 1) (on, 1) (mat, 1) |
| Mapper 2 | (the, 2) (dog, 1) (sat, 1) (on, 1) (log, 1) |
| Mapper 3 | (the, 2) (cat, 1) (and, 1) (dog, 1) (played, 1) |

### Step 4 — Shuffle & Sort

The framework groups all values by key across every mapper, and sorts the keys before handing them to Reducers.

**Grouped intermediate data** (this is what each Reducer actually receives):

```
and     -> [1]
cat     -> [1, 1]
dog     -> [1, 1]
log     -> [1]
mat     -> [1]
on      -> [1, 1]
played  -> [1]
sat     -> [1, 1]
the     -> [2, 2, 2]
```

### Step 5 — Reduce Phase

Each Reducer sums the list of values for its assigned key(s).

**Reduce pseudocode:**

```
function REDUCE(word, list_of_counts):
    total = sum(list_of_counts)
    emit(word, total)
```

### Final Output

| Word | Count |
|---|---|
| and | 1 |
| cat | 2 |
| dog | 2 |
| log | 1 |
| mat | 1 |
| on | 2 |
| played | 1 |
| sat | 2 |
| the | 6 |

---

## Part 2: Intermediate Example — Sales Revenue by Region and Category

### Scenario

You run an e-commerce platform and have a raw transaction log. 

Each line is: 

`order_id, region, category, amount` 

You need the **total revenue per (region, category) pair**, computed at scale across a cluster.

This example is "intermediate" because it introduces:

- A **composite key** (`region|category`) instead of a single word
- A **Combiner** that does meaningful local aggregation (not just a formality)
- A **custom Partitioner** that routes keys to specific Reducers by region
- **Multiple Reducers** working in parallel, each producing part of the final result

**Input log (`transactions.csv`):**

```
1001,West,Electronics,250
1002,East,Clothing,80
1003,West,Clothing,120
1004,South,Electronics,300
1005,East,Electronics,150
1006,West,Electronics,90
1007,South,Clothing,60
1008,East,Clothing,200
```

### Step 1 — Input Splitting

The file is split across 2 Mappers (in a real cluster this would be block-based, e.g. 128 MB per split; here we split by line count for clarity).

| Split | Assigned to | Lines |
|---|---|---|
| Split 1 | Mapper 1 | orders 1001–1004 |
| Split 2 | Mapper 2 | orders 1005–1008 |

### Step 2 — Map Phase

Each Mapper parses each CSV line and emits `(region|category, amount)`.

**Map pseudocode:**

```
function MAP(line_number, line_text):
    order_id, region, category, amount = split(line_text, ",")
    key = region + "|" + category
    emit(key, parse_number(amount))
```

**Mapper 1 output:**

```
(West|Electronics, 250)
(East|Clothing, 80)
(West|Clothing, 120)
(South|Electronics, 300)
```

**Mapper 2 output:**

```
(East|Electronics, 150)
(West|Electronics, 90)
(South|Clothing, 60)
(East|Clothing, 200)
```

### Step 3 — Combine Phase

Each Mapper's Combiner sums amounts sharing the same key **within that Mapper's own output** before shuffling. In this small dataset no key repeats within a single mapper split, so the combined output equals the raw output — but at real scale (millions of rows per split) this step is what prevents huge volumes of duplicate keys from crossing the network.

**Combiner pseudocode:**

```
function COMBINE(key, list_of_amounts):
    emit(key, sum(list_of_amounts))
```

| Mapper | Combined output |
|---|---|
| Mapper 1 | (West\|Electronics, 250) (East\|Clothing, 80) (West\|Clothing, 120) (South\|Electronics, 300) |
| Mapper 2 | (East\|Electronics, 150) (West\|Electronics, 90) (South\|Clothing, 60) (East\|Clothing, 200) |

### Step 4 — Partitioning

Before shuffling, a **Partitioner** decides which Reducer each key goes to. Here we partition by region so all data for a region lands on one Reducer, giving 3 Reducers total.

**Partitioner pseudocode:**

```
function PARTITION(key, num_reducers):
    region = key.split("|")[0]
    if region == "West":  return Reducer_1
    if region == "East":  return Reducer_2
    if region == "South": return Reducer_3
```

### Step 5 — Shuffle & Sort

The framework groups and sorts all intermediate key-value pairs and routes them to the assigned Reducer.

**Reducer 1 receives (West):**

```
West|Clothing    -> [120]
West|Electronics -> [250, 90]
```

**Reducer 2 receives (East):**

```
East|Clothing    -> [80, 200]
East|Electronics -> [150]
```

**Reducer 3 receives (South):**

```
South|Clothing    -> [60]
South|Electronics -> [300]
```

### Step 6 — Reduce Phase

Each Reducer sums the values for each key it owns.

**Reduce pseudocode:**

```
function REDUCE(key, list_of_amounts):
    total = sum(list_of_amounts)
    emit(key, total)
```

| Reducer | Output |
|---|---|
| Reducer 1 | West\|Clothing = 120, West\|Electronics = 340 |
| Reducer 2 | East\|Clothing = 280, East\|Electronics = 150 |
| Reducer 3 | South\|Clothing = 60, South\|Electronics = 300 |

### Step 7 — Final Merged Output

Each Reducer writes its own output file (`part-r-00000`, `part-r-00001`, `part-r-00002`); merged and sorted for reporting:

| Region | Category | Total Revenue |
|---|---|---|
| East | Clothing | 280 |
| East | Electronics | 150 |
| South | Clothing | 60 |
| South | Electronics | 300 |
| West | Clothing | 120 |
| West | Electronics | 340 |

---

## Recap: MapReduce Phases

| Phase | Runs where | Purpose |
|---|---|---|
| Input Split | Framework (e.g. HDFS) | Divides input into chunks, one per Mapper |
| Map | Each Mapper node | Transforms raw records into key-value pairs |
| Combine (optional) | Each Mapper node | Local pre-aggregation to reduce network transfer |
| Partition | Framework | Decides which Reducer handles which key |
| Shuffle & Sort | Framework (across network) | Groups all values by key, sorts keys, delivers to the right Reducer |
| Reduce | Each Reducer node | Aggregates all values for a key into the final result |
| Output | Framework | Writes each Reducer's result to a separate output file |

### Why these two examples together

Word Count shows the mechanics with the simplest possible key (a single string) and no real need for a custom partitioner. 

The sales example builds on it directly: same five phases, but with a composite key, a Combiner that actually matters at scale, and a Partitioner that deliberately controls data placement across Reducers — which is closer to how MapReduce is used in production (log analysis, revenue rollups, click aggregation, etc.).
