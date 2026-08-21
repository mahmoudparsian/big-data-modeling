# Partitions and Executors in MapReduce

	Author: Mahmoud Parsian
	Last updated: 8/18/2026

## Table of Contents

1. [Introduction to MapReduce and Beyond](#introduction-to-mapreduce-and-beyond)
2. [Data Parallelism in MapReduce](#data-parallelism-in-mapreduce)
3. [Two Kinds of Partitioning](#two-kinds-of-partitioning)
4. [Data Locality](#data-locality)
5. [Example: Input Data](#example-input-data)
6. [Example: Cluster Configuration](#example-cluster-configuration)
7. [Executors, Cores, and Task Slots](#executors-cores-and-task-slots)
8. [Distributing Partitions to Worker Nodes](#distributing-partitions-to-worker-nodes)
9. [Scaling Out: Adding More Worker Nodes](#scaling-out-adding-more-worker-nodes)
10. [Configuring Partitions and Executors in Spark](#configuring-partitions-and-executors-in-spark)
11. [Key Takeaways](#key-takeaways)
12. [References](#references)

---

## Introduction to MapReduce and Beyond

MapReduce is a parallel programming model
and an associated implementation introduced
by Google (Dean and Ghemawat, 2004 — see
[References](#references)). In this programming
model, a user specifies the computation with two
functions, `map()` and `reduce()`. The MapReduce
paradigm, or systems inspired by it, are implemented
by many projects:

* Google MapReduce (proprietary, not open sourced)
* Apache Hadoop MapReduce (open source)
* Apache Tez (open source) — a more general
  DAG-based execution engine (used by Hive and Pig)
  that generalizes MapReduce's rigid map-then-reduce
  pipeline into an arbitrary graph of stages
* Apache Spark (open source)
  * Spark is a superset of MapReduce and, for
    iterative or in-memory-friendly workloads,
    performs significantly faster than Hadoop
    MapReduce. For simple, single-pass, I/O-bound
    jobs the gap is much smaller, since both systems
    are then bound by disk/network throughput.
  * Spark supports a much richer set of transformations
    than just `map()` and `reduce()` (`filter()`,
    `join()`, `groupByKey()`, `reduceByKey()`,
    windowed and SQL operations, and more).
  * Spark favors in-memory (RAM) computation and can
    cache intermediate results across stages, while
    classic Hadoop MapReduce persists intermediate
    output to disk (HDFS) between the map and reduce
    phases.
  * Spark is a multi-language engine (Scala, Java,
    Python, R, SQL) for data engineering, data
    science, and machine learning, and can run on a
    single machine or on a cluster (via its standalone
    scheduler, YARN, Kubernetes, or Mesos).
  * Spark is generally preferred over Hadoop MapReduce
    today; published benchmarks have shown speedups
    from roughly 10x to 100x on workloads that reuse
    data across multiple passes (e.g., iterative
    machine learning), though real-world speedup
    depends heavily on the workload and cluster
    configuration.

## Data Parallelism in MapReduce

The MapReduce programming model was created
to exploit **data parallelism**: the ability to
compute many independent operations, in any order,
at the same time. When the MapReduce system receives
a job, it first divides the job's input data into
many data blocks of roughly equal size, called
**partitions** (in Hadoop these are called *input
splits*). Each `map()` task (a *mapper*) is
responsible for processing exactly one partition.
All mappers that the cluster has capacity to run
execute at the same time, independently of one
another, which is what gives MapReduce its
parallelism. The same is true on the reduce side:
all `reduce()` tasks (*reducers*) that are running
execute at the same time and independently, forming
parallel processing of the reduce phase. Because a
job's input can be split into many independent
partitions, and each partition can be processed on
its own, MapReduce enables parallel and distributed
processing at scale.

Apache Spark is a superset of MapReduce and goes
well beyond it. When a Spark task is parallelized,
concurrent tasks run on worker (executor) processes;
the driver coordinates the job but does not itself
execute partition-level tasks. How a task is split
across the nodes of a cluster depends on the data
structures and APIs you use (RDD, DataFrame, Dataset)
and on the current number of partitions of that
dataset.

The concept of data partitioning applies equally
to MapReduce and to Spark, though — as the next
section explains — the word "partition" actually
covers two related but distinct ideas.

## Two Kinds of Partitioning

It's easy to conflate two different concepts that
both go by the name "partition":

1. **Input partitioning** — how the *input* data is
   split before the map phase. Each input partition
   becomes exactly one map task. In Hadoop this is
   governed by the `InputFormat` and is typically
   aligned to the HDFS block size (128 MB by default
   in modern Hadoop). In Spark, for file-based
   sources, it's governed by
   `spark.sql.files.maxPartitionBytes` (128 MB by
   default) and the file/block layout; for an RDD or
   DataFrame already in memory, it's simply however
   many partitions that dataset currently has.
2. **Shuffle (output) partitioning** — how
   intermediate `(key, value)` pairs are grouped and
   routed to reduce tasks *after* the map phase, via
   a `Partitioner` (by default, hash partitioning on
   the key). The number of reduce tasks is **not**
   automatically the same as the number of input/map
   partitions — it is a separate, explicitly
   configured number: `mapreduce.job.reduces` in
   Hadoop, or `spark.sql.shuffle.partitions`
   (default 200) / the `numPartitions` argument to
   `reduceByKey()`, `groupByKey()`, `join()`, or
   `repartition()` in Spark.

The worked examples later in this document (40,000
partitions, 12 executors) describe **input
partitioning**, i.e., how many map tasks exist and
how they get scheduled. Reduce-side parallelism
would be sized independently, typically much smaller
than the map-side partition count.

## Data Locality

Real cluster schedulers don't hand out partitions
purely first-come-first-served — they also try to
minimize network I/O by exploiting **data locality**.
When possible, a task is scheduled on (in order of
preference):

1. A node that already holds the data block on local
   disk (`node-local` — Spark calls this
   `PROCESS_LOCAL` or `NODE_LOCAL`).
2. A node in the same rack as the data
   (`rack-local`).
3. Any available node, at the cost of pulling the
   data over the network (`any`).

Spark's scheduler will wait a short, configurable
amount of time (`spark.locality.wait`, 3 seconds by
default) for a local slot to free up before falling
back to a less-local placement. This means the
simplified "first idle executor gets the next
partition in the queue" model used below is a useful
mental model, but a real scheduler also factors in
where the data physically lives.

## Example: Input Data

Your input is partitioned into chunks called partitions.
For example, if you have `80,000,000,000` records
(data points) and you split them into `40,000` chunks:

* Number of partitions: `40,000`
* Approximate number of records per partition: `2,000,000`
* `40,000 x 2,000,000 = 80,000,000,000`
* Label the partitions `P_1`, `P_2`, ..., `P_40000`

## Example: Cluster Configuration

Assume you have a cluster of 4 nodes: one master
(`M`) and 3 worker nodes (`W1`, `W2`, `W3`). We
denote this cluster as `C = {M, W1, W2, W3}`.

The master node acts only as the **cluster manager**
(in Spark terms, it runs the *driver* and/or the
cluster's resource manager, e.g., the YARN
ResourceManager or Spark's standalone Master): it
schedules and tracks work, but it does not itself
execute transformations (mappers, filters, reducers,
etc.).

Assume further that each worker node hosts 4
**executors**, so the cluster has `3 x 4 = 12`
executors in total. The number of executors a worker
node can host depends on that node's size and power,
and on how the cluster is configured: a worker node
with ample RAM and CPU might host 10-16 executors
instead of just 4, or (as is common in practice) run
just one large executor per node and rely on that
executor's core count for parallelism instead.

> **Note on terminology.** "Executor" is Spark's
> term for a worker process. Classic Hadoop MapReduce
> uses different terminology for the analogous unit
> of work: a task runs inside a YARN **container**,
> scheduled into a **map slot** or **reduce slot**.
> The concepts are similar, but the words are not
> interchangeable across systems.

We label the 12 executors as:

```
E = {E_1, E_2, E_3, ..., E_12}
```

```
            M  (cluster manager / driver, no map/reduce tasks)
            |
   +--------+--------+
   |        |         |
   W1       W2        W3
 E1-E4    E5-E8     E9-E12
```

For the walk-through in the next two sections, we
start with the simplest possible assumption: **each
executor runs one task at a time** (i.e., 1 core per
executor). The following section relaxes that
assumption.

## Executors, Cores, and Task Slots

An executor is not necessarily limited to one task
at a time. In Spark, each executor is a JVM process
that is allocated some number of CPU cores
(`spark.executor.cores`); each core generally hosts
one concurrent **task slot**. So the cluster's true
concurrent task capacity is:

```
total task slots = number of executors x cores per executor
```

The hierarchy, from cluster down to a single running
task, has four levels:

```
Cluster
 └─ Worker Node        (3 in our example: W1, W2, W3)
     └─ Executor        (4 per worker → 12 total; a JVM process)
         └─ Core         (= 1 task slot; N cores per executor)
             └─ Task      (processes exactly 1 partition at a time)
```

Zooming into a single worker node makes the
executor → core → task-slot → partition relationship
concrete. Here, `W1` runs 4 executors, each with 4
cores (`spark.executor.cores = 4`), for
`4 x 4 = 16` task slots on that one node alone:

```
                              Cluster Manager / Driver (M)
                                          │
                              assigns partitions to free slots
                                          ▼
┌───────────────────────────────── Worker Node W1 ─────────────────────────────────┐
│                                                                                    │
│  ┌──────────── Executor E1 ────────────┐  ┌──────────── Executor E2 ────────────┐ │
│  │  (JVM process, 4 cores)              │  │  (JVM process, 4 cores)             │ │
│  │  Core 1 [slot] → task: map(P_1)      │  │  Core 1 [slot] → task: map(P_5)     │ │
│  │  Core 2 [slot] → task: map(P_2)      │  │  Core 2 [slot] → task: map(P_6)     │ │
│  │  Core 3 [slot] → task: map(P_3)      │  │  Core 3 [slot] → task: map(P_7)     │ │
│  │  Core 4 [slot] → task: map(P_4)      │  │  Core 4 [slot] → idle, awaits next  │ │
│  └───────────────────────────────────────┘  └───────────────────────────────────┘ │
│                                                                                    │
│  Executor E3 (4 cores → 4 slots) ...        Executor E4 (4 cores → 4 slots) ...    │
│                                                                                    │
│  W1 totals: 4 executors x 4 cores = 16 concurrent task slots                      │
└────────────────────────────────────────────────────────────────────────────────────┘

  Worker Node W2: executors E5-E8   → 16 more task slots (same structure as W1)
  Worker Node W3: executors E9-E12  → 16 more task slots (same structure as W1)

  Cluster total: 3 nodes x 4 executors x 4 cores = 48 concurrent task slots
```

Each leaf task slot pulls one partition off the
queue, runs `map()` (or `reduce()`) on it, reports
back to the cluster manager, and is then handed the
next unprocessed partition. That queue-and-reassign
logic is exactly the round-based process described
next — it works the same way regardless of how many
slots `S` there are; only the round count changes.

The walkthrough in the next section uses the
simplest possible case, `S = 12` (12 executors, 1
core each), matching the earlier tree diagram of
`M → {W1, W2, W3} → E1-E12`. The 4-cores-per-executor
diagram above shows the more realistic alternative:
the same 12 executors, configured with 4 cores each,
yield `S = 12 x 4 = 48` task slots — roughly 4x the
concurrency, and (ignoring overhead) roughly a 4x
faster job, since 4x as many partitions can be
processed at once. This is why, when sizing a real
Spark cluster, you must reason about
*executors x cores*, not just the executor count.

## Distributing Partitions to Worker Nodes

The question is: how does the cluster manager
distribute and execute `40,000` partitions across
3 worker nodes, each with 4 executors (12 executors,
12 task slots total, under our 1-core-per-executor
assumption)?

Assume the `40,000` partitions sit in a queue waiting
to be processed, and the first transformation to run
is a `map()` function (a `map()` task receives a
single partition and emits a stream of `(key, value)`
pairs).

The assignment proceeds in rounds:

1. **Round 1:** the cluster manager assigns the first
   12 partitions to the 12 idle task slots, one
   partition per slot — subject to data-locality
   preferences (see [Data Locality](#data-locality)),
   which this simplified walk-through otherwise
   ignores.
2. As soon as a task slot finishes `map()` on its
   partition, it sends the result back to the cluster
   manager, which immediately assigns that slot the
   next unprocessed partition from the queue.
3. This repeats until the queue is exhausted.

At any given moment, up to 12 tasks run
in parallel and independently. With
`40,000` partitions and `12` task slots, this takes
`ceil(40,000 / 12) = 3,334` rounds: the first 3,333
rounds keep all 12 slots busy, and the final
round only needs 4 slots (`40,000 - 3,333 x 12 = 4`
partitions remain) while the other 8 sit idle.

In general, for `P` partitions and `S` available
task slots (where `S = executors x cores per
executor`), the number of rounds is
`ceil(P / S)`, and total elapsed time is
approximately `ceil(P / S) x (average task duration)`
— an approximation, because real task durations vary
(data skew) and the final, partially-filled round
under-utilizes the cluster.

The more worker nodes (and executors/cores) we have
available, the faster the whole job completes, up
to the point where partitions run out to assign or
some other resource (network, disk I/O, the driver
itself) becomes the bottleneck.

## Scaling Out: Adding More Worker Nodes

Suppose that, with our original cluster `C`
(3 worker nodes, 12 executors, 1 core each ⇒ 12 task
slots), executing all `40,000` partitions takes `T`
seconds. In an **idealized case** — partitions of
equal cost, no stragglers, and negligible
shuffle/network overhead — doubling the worker nodes
roughly halves the elapsed time, and tripling them
roughly divides it by three:

| Worker nodes | Executors (4/node) | Task slots (1 core/exec.) | Approx. elapsed time |
|---|---|---|---|
| 3 (`W1-W3`)  | 12 | 12 | `T`   |
| 6 (`W1-W6`)  | 24 | 24 | `T/2` |
| 9 (`W1-W9`)  | 36 | 36 | `T/3` |

More precisely, this idealized linear speedup is the
special case (`p = 1`, fully parallelizable) of
[Amdahl's Law](https://en.wikipedia.org/wiki/Amdahl%27s_law):

```
speedup(N) = 1 / ( (1 - p) + p / N )
```

where `p` is the fraction of the job's total work
that can be parallelized across `N` workers, and
`(1 - p)` is the fraction that is inherently
sequential (e.g., job setup/teardown, the final
reduce merge, driver-side aggregation). If `p = 1`,
`speedup(N) = N` — the idealized linear case shown in
the table above. If, say, 5% of the job is inherently
sequential (`p = 0.95`), then even with `N = 36`
workers the maximum possible speedup is only about
`17.6x`, not `36x` — because that sequential 5% is
paid in full no matter how many workers you add.
Coordination overhead, data skew across partitions,
network/shuffle costs, and straggler tasks further
reduce the actual speedup you observe in practice,
so real numbers typically fall short even of what
Amdahl's Law predicts.

## Configuring Partitions and Executors in Spark

The concepts above map directly onto Spark
configuration knobs you can set at submit time or in
code:

* **Number of executors**
  * Static: `--num-executors` (YARN) or
    `spark.executor.instances`.
  * Dynamic: `spark.dynamicAllocation.enabled=true`,
    which lets Spark add/remove executors based on
    workload.
* **Cores per executor:** `--executor-cores` /
  `spark.executor.cores` — directly sets the number
  of concurrent task slots per executor.
* **Memory per executor:** `--executor-memory` /
  `spark.executor.memory`.
* **Input-side (map) parallelism:**
  * `spark.sql.files.maxPartitionBytes` caps the
    size of each input partition read from files.
  * `df.repartition(n)` reshuffles data into exactly
    `n` partitions (full shuffle).
  * `df.coalesce(n)` reduces the partition count to
    `n` without a full shuffle (only merges, doesn't
    redistribute), useful for shrinking partition
    counts cheaply before writing output.
* **Shuffle-side (reduce) parallelism:**
  `spark.sql.shuffle.partitions` (DataFrame/SQL,
  default 200) or the `numPartitions` argument to
  RDD wide transformations such as `reduceByKey()`.

A common rule of thumb: size partitions to roughly
100-200 MB each (too few partitions under-utilizes
the cluster; too many adds scheduling overhead), and
aim for 2-4 tasks queued per core so that stragglers
and uneven task durations can be smoothed out rather
than leaving idle slots at the end of a round.

## Key Takeaways

* Input data is split into many equal-sized
  **partitions** (input splits); each partition is
  processed by exactly one `map()` task.
* "Partition" actually names two distinct things:
  the input split that determines map-task count, and
  the shuffle partitioning that determines reduce-task
  count — the two are configured independently.
* A cluster has one manager/driver node and several
  worker nodes; each worker node hosts one or more
  **executors** (Spark terminology; Hadoop's analog is
  a YARN container running a map/reduce slot), and
  each executor's core count determines how many
  tasks it can run concurrently.
* The cluster manager assigns partitions to idle task
  slots from a queue, round by round, favoring
  data-local placement when possible, until all
  partitions are processed.
* Adding more worker nodes/executors/cores increases
  parallelism and, up to a point, reduces total
  execution time — but real-world speedup is bounded
  by Amdahl's Law and further reduced by overhead,
  skew, and stragglers, so it falls short of ideal
  linear speedup.

## References

* Jeffrey Dean and Sanjay Ghemawat,
  ["MapReduce: Simplified Data Processing on Large Clusters"](../google_mapreduce_paper/MapReduce_Simplified_Data_Processing_on_Large_Clusters_by_Jeff_Dean.pdf)
  (Google, 2004) — the original MapReduce paper.
* Apache Spark,
  [Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html)
  — official definitions of driver, executor, and
  cluster manager.
* Apache Spark,
  [Tuning Guide — Level of Parallelism](https://spark.apache.org/docs/latest/tuning.html)
  — practical guidance on sizing partitions and
  parallelism.
* Apache Hadoop,
  [MapReduce Tutorial](https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
  — input splits, map/reduce slots, and job
  configuration.
* [Amdahl's Law](https://en.wikipedia.org/wiki/Amdahl%27s_law)
  — the theoretical limit of parallel speedup used in
  [Scaling Out](#scaling-out-adding-more-worker-nodes).
