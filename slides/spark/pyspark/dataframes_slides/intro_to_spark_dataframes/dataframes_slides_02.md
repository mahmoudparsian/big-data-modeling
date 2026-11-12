---

title: Introduction to PySpark DataFrames
subtitle: From MapReduce and RDDs to Structured Distributed Processing
course: Graduate MSIS — Distributed Data Engineering & Big Data Analytics

---

# Slide 1: Introduction to PySpark DataFrames
## Bridging Low-Level Distributed Computing and High-Level Analytics

* **Audience:** MSIS Graduate Students
* **Prerequisites:** MapReduce Paradigm, Apache Spark Architecture, and Resilient Distributed Datasets (RDDs)
* **Goal:** Understand *why* DataFrames replaced RDDs as the primary Spark API, *how* Spark optimizes them under the hood, and *how* to write expressive, scalable PySpark transformations.

> **Notes:**  
> 
> 1. Up to this point, you have worked with MapReduce and Spark RDDs, giving you a strong foundation in distributed partitioning, immutability, and lazy evaluation. 
>
> 2. Today, we elevate our abstraction level to PySpark DataFrames—the industry standard for production big data engineering and analytics.

---

# Slide 2: Where We Are Coming From (MapReduce & RDDs)
## Recapping Your Foundation

* **MapReduce:**
  * Disk-heavy execution between Map and Reduce phases.
  * Rigid two-stage programming model; verbose Java/Python boilerplate.
* **Spark RDDs (Resilient Distributed Datasets):**
  * **In-memory** distributed collections of JVM/Python objects.
  * **Fault-tolerant** via lineage graphs (DAGs) and **lazy evaluation**.
  * **Low-level control:** You specify *how* to compute (`map`, `flatMap`, `reduceByKey`).
* **The Catch with RDDs:**
  * Spark treats your data as opaque objects (`PythonPickledObject` or JVM bytecode).
  * Spark **cannot see inside** your functions or data structures to optimize execution.

> **Notes:** 
> 
> 1. Remind students of the classic `reduceByKey` vs. `groupByKey` lesson in RDDs. 
> 
> 2. With RDDs, the developer bears the burden of manual optimization because Spark has zero visibility into what a lambda function actually does.

---

# Slide 3: Why Move Beyond RDDs?
## The Motivation for Structure

Consider calculating the average department salary in an RDD vs. a DataFrame:

* **RDD Approach (Imperative — "How"):**
  ```python
  # Spark sees opaque Python tuples and arbitrary lambda functions
  rdd.map(lambda row: (row[1], (row[2], 1))) \
     .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
     .mapValues(lambda v: v[0] / v[1])
  ```
* **The Bottlenecks in PySpark RDDs:**
  1. **Serialization Overhead:** Data must be pickled/unpickled between the JVM and Python workers for every row.
  2. **No Schema Awareness:** Field names and data types are tracked manually by the developer.
  3. **Zero Query Optimization:** Spark executes your exact instructions, even if you filter *after* an expensive join.

> **Notes:** 
> 
> 1. Highlight the PySpark-specific penalty here. In Python RDDs, every transformation spins up a Python subprocess and ships serialized bytes back and forth across a socket.
> 
> 2. DataFrames eliminate almost all of this overhead for built-in operations.

---

# Slide 4: Enter the PySpark DataFrame
## What is a DataFrame?

* **Conceptual Definition:** A **distributed** collection of data organized into **named columns**, conceptually equivalent to a table in a relational database or a Pandas DataFrame—but partitioned across a cluster.
* **Key Characteristics:**
  * **Immutable & Lazy:** Inherits RDD fault tolerance and DAG execution.
  * **Strongly Typed Schema:** Every column has an explicit name and data type (`StructType` / `StructField`).
  * **Declarative API:** You tell Spark *what* result you want (`select`, `filter`, `groupBy`), and Spark decides *how* to execute it.
  * **Language Agnostic Performance:** PySpark, Scala, Java, and Spark SQL all compile down to the exact same execution plan.

> **Notes:** <br>
> Emphasize that a PySpark DataFrame is built *on top of* RDDs. At the end of the optimization pipeline, Spark still compiles a DataFrame query down into low-level RDD bytecode—it just writes much better RDD code than humans usually do!

---

# Slide 5: Architectural Comparison — RDD vs. DataFrame

| Dimension | Spark RDD | PySpark DataFrame |
| :--- | :--- | :--- |
| **Data Representation** | Opaque objects (Java/Python objects) | Structured rows with a known **Schema** |
| **Programming Style** | Imperative (*how* to compute) | Declarative (*what* to compute) |
| **Optimization** | Manual (developer-driven) | Automatic (**Catalyst Optimizer**) |
| **Memory Management** | Standard JVM Heap / Python memory | Off-heap binary format (**Project Tungsten**) |
| **PySpark Overhead** | High (constant JVM ↔ Python pickling) | Minimal (runs natively inside the JVM) |
| **Best Use Cases** | Unstructured data, custom low-level algorithms | Tabular data, ETL, SQL analytics, ML feature prep |

> **Notes:** Walk through this table row by row. For MSIS students, connecting the declarative nature of DataFrames to relational databases (SQL) makes the transition intuitive.

---

# Slide 6: Under the Hood (Part 1) — The Catalyst Optimizer
## How Spark Thinks Before It Acts

Because DataFrames have a schema and declarative expressions, Spark uses the **Catalyst Optimizer** to rewrite your query before running it:

```text
[PySpark Code / SQL] 
       │
       ▼
(1) Unresolved Logical Plan  ──► Validated against Catalog (Schema/Tables)
       │
       ▼
(2) Resolved Logical Plan    ──► Catalyst Rule-Based Optimization
       │                         (Predicate Pushdown, Column Pruning, Constant Folding)
       ▼
(3) Optimized Logical Plan   ──► Cost-Based Physical Planning
       │
       ▼
(4) Selected Physical Plan   ──► Whole-Stage Code Generation (Java Bytecode / RDDs)
```

* **Predicate Pushdown:** Moves `filter()` operations as close to the data source (e.g., Parquet) as possible—skipping irrelevant files/row groups.
* **Column Pruning:** Reads *only* the columns referenced in your `select()`, ignoring the rest on disk.

> **Notes:** Use `.explain(True)` in a live demo later to show students these exact four stages. Seeing Spark automatically push a `.filter()` below a `.join()` is usually an "aha!" moment for students used to MapReduce.

---

# Slide 7: Under the Hood (Part 2) — Project Tungsten
## Hardware-Aware Execution

* **Why JVM Objects Are Slow:** A standard Java `String` or `Integer` carries massive memory overhead (headers, pointers) and triggers expensive Garbage Collection (GC) pauses.
* **Tungsten's Solution:**
  1. **Off-Heap Binary Memory (`UnsafeRow`):** Stores rows in compact, raw byte arrays outside the standard JVM heap—eliminating GC bottlenecks.
  2. **Cache-Aware Computation:** Arranges data in memory to maximize CPU L1/L2/L3 cache hits.
  3. **Whole-Stage Code Generation:** Fuses multiple operators (e.g., `filter` + `project`) into a single optimized Java function at runtime, avoiding virtual function calls.

> **Notes:** The main takeaway is that DataFrames do not store Python dictionaries or Java objects in RAM; they store raw, compact binary rows tailored for modern CPUs.

---

# Slide 8: Getting Started — `SparkSession` & Creating DataFrames
## The Entry Point

In modern Spark, `SparkSession` unifies the old `SparkContext`, `SQLContext`, and `HiveContext`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# 1. Initialize SparkSession
spark = SparkSession.builder.getOrCreate()

# 2. Define an Explicit Schema (Best Practice for Production)
emp_schema = StructType([
    StructField("emp_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("department", StringType(), nullable=True),
    StructField("salary", DoubleType(), nullable=True)
])

# 3. Read Data (e.g., CSV or Parquet)
df = spark.read.csv("employees.csv", header=True, schema=emp_schema)
df.printSchema()
```

* **Pro-Tip:** Always prefer defining an explicit `StructType` 
* schema over `inferSchema=True` on large datasets to avoid 
* an extra full pass over the data.

> **Notes:** <br>
> 1. Contrast `SparkContext` (used for RDDs) with 
`SparkSession` (used for DataFrames). 
> 2. Note that `spark.sparkContext` is still accessible 
underneath if they ever need to drop down to the RDD layer.

---

# Slide 9: Core DataFrame Transformations (Lazy)
## Selecting, Filtering, and Mutating Columns

Just like RDD transformations, DataFrame transformations return a **new DataFrame** and do not execute until an action is called.

```python
from pyspark.sql import functions as F

# Select & rename columns
df_clean = df.select(
    F.col("emp_id"),
    F.col("name"),
    F.col("department").alias("dept"),
    F.col("salary")
)

# Filter rows (Predicate)
high_earners = df_clean.filter((F.col("dept") == "Engineering") & (F.col("salary") > 120000))

# Add or transform columns using withColumn
df_bonus = high_earners.withColumn("bonus", F.col("salary") * 0.15) \
                       .withColumn("total_comp", F.col("salary") + F.col("bonus"))
```

* **Key Rule:** Always use built-in functions from `pyspark.sql.functions` (`F.*`) so Catalyst can optimize them.

> **Speaker Notes:** Point out the column expression syntax `F.col("salary")`. Explain that `F.col("salary") * 0.15` is not doing Python math—it is building an Abstract Syntax Tree (AST) expression node for Catalyst.

---

# Slide 10: Aggregations & Joins (Wide Transformations)
## Shuffles in the DataFrame World

Just like `reduceByKey` and `join` in RDDs, `groupBy` and `join` trigger **shuffle boundaries** across the network:

```python
# 1. GroupBy & Aggregations
dept_summary = df.groupBy("department").agg(
    F.count("emp_id").alias("headcount"),
    F.round(F.avg("salary"), 2).alias("avg_salary"),
    F.max("salary").alias("max_salary")
)

# 2. Joining two DataFrames
# Assume dept_df has columns: ["department", "location"]
enriched_df = df.join(dept_df, on="department", how="inner")

# Performance Tip: Broadcast small lookup tables to avoid shuffling the large table!
enriched_fast = df.join(F.broadcast(dept_df), on="department", how="inner")
```

* **Broadcast Hash Join:** When one table is small enough to fit in executor memory (default `< 10MB`), Spark broadcasts it to all nodes, eliminating the shuffle on the large fact table.

> **Notes:** Connect this directly to MapReduce! In 
MapReduce, joining two large datasets required a Reduce-Side 
Join (expensive shuffle), whereas joining a large dataset 
with a small lookup table used a Map-Side / Distributed Cache 
Join. `F.broadcast()` is Spark's declarative equivalent of a 
Map-Side Join.

---

# Slide 11: DataFrames & Spark SQL — Two Sides of the Same Coin
## Seamless Interoperability

Every DataFrame can be queried using standard ANSI SQL, 
and SQL queries return DataFrames:

```python
# Register the DataFrame as a temporary SQL view
df.createOrReplaceTempView("employees")

# Run pure SQL
sql_result = spark.sql("""
    SELECT 
        department,
        COUNT(*) AS headcount,
        ROUND(AVG(salary), 2) AS avg_salary
    FROM employees
    WHERE salary > 80000
    GROUP BY department
    ORDER BY avg_salary DESC
""")

# The output is a standard DataFrame!
sql_result.show()
```

* **Identical Execution Plan:** `df.groupBy(...)` and 
`spark.sql("SELECT ... GROUP BY ...")` produce the 
**exact same** physical execution plan and identical 
runtime performance.

> **Notes:** Students usually have strong SQL backgrounds. Reassuring them that mixing DataFrame API calls and `spark.sql()` has zero performance penalty empowers them to use whichever syntax is cleanest for a given business logic step.

---

# Slide 12: Common Actions & Execution Inspection
## Triggering Computation and Debugging

**Common DataFrame Actions (Eager Execution):**

* `df.show(n=20, truncate=True)` — Prints top `n` rows to the driver console.
* `df.count()` — Returns the total number of rows across all partitions.
* `df.collect()` — Brings **all** rows back to driver memory *(Danger: causes `OutOfMemoryError` on big data!)*.
* `df.write.parquet("output_path/")` — Writes distributed partitions to storage.

**Inspecting the Execution Plan:**
```python
# View the Physical Plan (look for PushedFilters, BroadcastHashJoin, Exchange/Shuffle)
dept_summary.explain(mode="formatted")
```

> **Notes:** Strongly warn against `.collect()` on 
large DataFrames—just like in RDDs, bringing terabytes 
of distributed partitions back to a single driver node 
will crash the application. Introduce `.explain()` as 
their primary debugging tool.

---

# Slide 13: PySpark Pitfall — Built-in Functions vs. Python UDFs
## Protecting Your Performance

What happens when you write a custom Python User-Defined Function (`@udf`)?

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# ANTI-PATTERN: Custom Python UDF for simple math
@udf(returnType=DoubleType())
def calc_tax_udf(sal):
    return sal * 0.25

# BEST PRACTICE: Native Spark Column Expression
df.withColumn("tax", F.col("salary") * 0.25)
```

* **Why Standard Python UDFs Hurt:**
  1. Catalyst treats a Python UDF as a **black box** (breaks predicate pushdown and optimization).
  2. Forces data out of Tungsten binary memory, serializes it to Python workers, and deserializes it back—reintroducing the RDD bottleneck!
* **When You Need Custom Python Logic:** Use **Pandas UDFs (Vectorized UDFs)** powered by **Apache Arrow**, which process batches of rows as Pandas Series instead of one row at a time.

> **Speaker Notes:** This is the #1 real-world mistake junior PySpark developers make. Always check `pyspark.sql.functions` first before ever writing a custom Python UDF.

---

# Slide 14: Summary & Key Takeaways
## Mental Model Checklist for Students

1. **Think Declaratively:** Describe *what* you want using `select`, `filter`, `groupBy`, and `join`; let **Catalyst** figure out *how* to run it.
2. **Schema is King:** Schemas enable Catalyst query optimization and **Tungsten** off-heap binary memory management.
3. **Preserve Spark's Heritage:** DataFrames are still **immutable**, **partitioned**, and **lazily evaluated**—shuffles still happen on wide transformations (`groupBy`, `join`).
4. **Stay Native:** Stick to `pyspark.sql.functions` and Spark SQL; avoid row-at-a-time Python UDFs unless absolutely necessary.
5. **Use the Right Storage Format:** Pair DataFrames with columnar formats like **Apache Parquet** or **Delta Lake** to unlock column pruning and predicate pushdown on disk.

---

# Slide 15: Hands-On Lab Preview & Discussion
## Putting Theory into Practice

**In Today's Lab Exercise:**
1. Load a multi-million-row NYC Taxi dataset using an explicit `StructType` schema.
2. Compare execution plans using `.explain()` before and after adding filters and broadcast joins.
3. Benchmark a native `pyspark.sql.functions` transformation against a standard Python `@udf`.

**Discussion Questions:**

* *When would you still choose to use an RDD instead of a DataFrame in a modern Spark pipeline?*
* *Why does Parquet pair so much better with Catalyst than CSV or JSON?*


