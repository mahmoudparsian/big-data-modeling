# What is Big Data Modeling?

## Table of Contents

1. [Overview](#1--overview)
   - [1.1 Modern Data Architecture Styles](#11--modern-data-architecture-styles)
   - [1.2 Key Structural Patterns](#12--key-structural-patterns)
2. [Traditional vs. Big Data Modeling](#2--traditional-vs-big-data-modeling)
3. [The Three Core Perspectives](#3--the-three-core-perspectives)
4. [Query-Driven Schema Design](#4--query-driven-schema-design)
5. [Common Modeling Techniques for Big Data](#5--common-modeling-techniques-for-big-data)
6. [Four Worked Examples](#6--four-worked-examples)
   - [6.1 Example 1: Ride-Hailing Platform](#61--example-1-ride-hailing-platform-eg-uberlyft--wide-column--geo-partitioned-model)
   - [6.2 Example 2: E-Commerce Personalization](#62--example-2-e-commerce-personalization--star-schema-on-a-lakehouse-schema-on-read)
   - [6.3 Example 3: Healthcare IoT Telemetry](#63--example-3-healthcare-iot-telemetry--document-based-time-series-model)
   - [6.4 Example 4: LLM Retrieval-Augmented Generation](#64--example-4-llm-retrieval-augmented-generation-rag--vector-embedding-store-model)
7. [End-to-End PySpark Example](#7--end-to-end-pyspark-example-building-example-2s-star-schema)
8. [Tools for Big Data Modeling Analytics](#8--tools-for-big-data-modeling-analytics)
   - [8.1 Storage & Data Warehouses](#81--storage--data-warehouses)
   - [8.2 Processing & Analytics Engines](#82--processing--analytics-engines)
   - [8.3 Data Modeling & Transformation](#83--data-modeling--transformation)
   - [8.4 Visualization & Business Intelligence](#84--visualization--business-intelligence)

---

## 1. 🔎 Overview

**Big data modeling** is the process of creating a visual or
logical framework that defines how massive, complex datasets
are **structured, stored, and interrelated** within a modern
data architecture.

While traditional data modeling focuses on clean, structured
tables inside a centralized database, big data modeling must
account for the **5 Vs of big data**: volume, velocity, variety,
veracity, and value. It provides a blueprint that lets analytical
systems — like Apache Spark, Hive, or cloud-based data lakes —
query billions of rows of structured, semi-structured (JSON, XML),
and unstructured (logs, sensor feeds) data without grinding to a
halt.

### 1.1 🏗️ Modern Data Architecture Styles

Modern data architecture styles share a common goal: decoupling
storage from compute, enabling real-time processing, and removing
the operational bottlenecks of a single, centrally managed database.
The three most prominent styles are:

* **Data Lakehouse**
* **Data Mesh**
* **Data Fabric**

| Architecture Type | Core Structural Premise | Primary Technical Drivers | Best For |
| :--- | :--- | :--- | :--- |
| **Data Lakehouse** | Merges the low-cost storage of a data lake with the ACID transactions and governance of a warehouse. | Open table formats (Delta Lake, Apache Iceberg, Apache Hudi). | Unifying traditional SQL analytics with unstructured machine learning/AI workloads. |
| **Data Mesh** | Decentralized, domain-driven architecture where data is treated as an independent product. | Microservices principles, federated computational governance, self-service data infrastructure. | Large enterprises with many distributed product lines or business units. |
| **Data Fabric** | A centralized semantic layer, driven by active metadata and AI, that automates pipeline orchestration. | Knowledge graphs, automated schema mapping, unified metadata catalogs. | Multi-cloud and hybrid environments that need a single logical access plane. |

A term that's often mistaken for a fourth style is the **Medallion
Architecture**. It isn't an alternative to the three above — it's
the physical design pattern most commonly used to *implement* a
Data Lakehouse (see [Section 1.2](#12--key-structural-patterns) below).

---

### 1.2 🧩 Key Structural Patterns

* **Medallion Architecture (multi-hop lakes):**
  * **Bronze layer** — raw data landed with minimal schema enforcement; captures append-only change logs.
  * **Silver layer** — cleansed, enriched, validated data; fields are normalized and row-level duplicates are removed.
  * **Gold layer** — heavily aggregated, business-ready data, typically shaped into dimensional star schemas for reporting.

* **Kappa Architecture (stream-first):**
  * **Streaming backbone** — a single real-time log engine (e.g., Apache Kafka) replaces the separate batch and speed layers of a Lambda architecture.
  * **Reprocessing by replay** — there's no parallel batch layer to keep in sync. When processing logic changes, historical results are recomputed by replaying the immutable event log through that same stream-processing job.

* **Modern Data Stack (MDS):**
  * **ELT, not ETL** — managed connectors (e.g., Fivetran) extract and load raw data into the lakehouse/warehouse first; transformations run downstream, inside high-throughput MPP compute (e.g., Snowflake, BigQuery).
  * **Modular by design** — each phase is owned by a dedicated tool: ingestion (Fivetran), version-controlled SQL transformations (dbt), and BI/serving (see [Section 8.4](#84--visualization--business-intelligence)).

---

## 2. 💡 Traditional vs. Big Data Modeling

The shift from standard databases to big data systems
fundamentally changes how data is organized:

| Feature | Traditional Data Modeling | Big Data Modeling |
| :--- | :--- | :--- |
| **Data Types** | Strictly structured (rows & columns) | Structured, semi-structured, and unstructured |
| **Schema Design** | **Schema-on-Write:** the schema must be defined *before* loading data. | **Schema-on-Read:** raw data is loaded as-is; structure is applied when queried. |
| **Normalization** | Highly normalized (rules to reduce data duplication across multiple tables). | Highly denormalized (flattened tables to speed up distributed querying). |
| **Storage Paradigm** | Scale-up (single, powerful database server). | Scale-out (distributed across clusters of machines). |

---

## 3. ⚙️ The Three Core Perspectives

A comprehensive big data model is built in three distinct
phases, moving from business logic to technical execution:

* **Conceptual Modeling:** defines *what* business entities 
  exist (e.g., customers, transactions, IoT devices) and 
  how they relate at a high level, completely independent 
  of technology.

* **Logical Modeling:** details the specific attributes, 
  keys, and data structures needed. It maps out the analytics 
  needs without tying them to a specific database software.

* **Physical Modeling:** translates the design into the underlying 
  infrastructure. This phase optimizes for the target platform (e.g., 
  designing partitions in an Apache Spark cluster, or sizing data 
  blocks in a cloud lakehouse).

---

## 4. 🎯 Query-Driven Schema Design

Unlike traditional relational modeling — where the schema is
normalized first and queries are written against it afterward —
big data modeling typically works in the **opposite order**:

1. **Study the requirements.** Identify who needs the 
   data and why: which dashboards, reports, ML features, 
   or applications will consume it.

2. **Enumerate the access patterns.** Determine exactly 
   how the data will be queried — which fields are filtered 
   on, which are aggregated, how the data is expected to 
   grow, and how fresh it needs to be.

3. **Design the schema to fit those queries.** Choose partition 
   keys, denormalization strategy, and physical layout so the 
   *known* queries run fast — even if that means duplicating 
   data or shaping the same entity differently across multiple 
   tables.

This is sometimes called **query-first** (or **query-driven**)
design, and it's the reasoning behind every partition-key and
denormalization choice made in the four worked examples below:
the ride-hailing platform partitions by geospatial hex specifically
because "find nearby drivers" is the dominant query, the
e-commerce lakehouse's star schema is shaped around the BI
aggregations analysts actually run, the healthcare IoT model buckets
vitals by device and hour so a time-range query never has to scan
per-heartbeat rows, and the LLM/RAG example indexes its vector store
around nearest-neighbor similarity search instead of exact-match
lookups — none of it is an abstract, query-agnostic ideal of a
"correct" schema.

> **Exception — Data Vault:** Data Vault Modeling (below) deliberately
> inverts this order. It's *source-driven*, not query-driven: Hubs,
> Links, and Satellites are modeled to mirror how the business and its
> source systems are structured, precisely so the model doesn't need
> to be redesigned every time a new reporting requirement or query
> pattern shows up. Query-optimized structures (like a star schema)
> are then built as a downstream layer on top of the Data Vault, not
> baked into it directly.

---

## 5. 🛠️ Common Modeling Techniques for Big Data

Depending on the business case, engineers lean on specific
frameworks to organize the information:

* **Dimensional Modeling (Star / Snowflake Schema):** optimizes 
  business intelligence (BI) workloads by organizing data into 
  central **fact tables** (numeric metrics) surrounded by 
  **dimension tables** (descriptive context).

* **Data Vault Modeling:** a flexible, agile method designed 
  for enterprise data warehouses. It separates business keys 
  (Hubs), relationships (Links), and context (Satellites) to 
  easily absorb changing data sources.

* **NoSQL Data Models:** wide-column stores, document stores, 
  key-value stores, and graph models. These bypass relational 
  limits entirely to focus on high-velocity throughput.

---

## 6. 📚 Four Worked Examples

### 6.1 🚗 Example 1: Ride-Hailing Platform (e.g., Uber/Lyft) — Wide-Column & Geo-Partitioned Model

Ride-hailing applications handle massive **velocity** and
**volume**, with millions of concurrent GPS pings and trip
events. Relational databases choke on this scale, so engineers
use distributed wide-column stores (like Apache Cassandra or
ScyllaDB) optimized for fast writes and time-series lookups.

* **Core Architecture:** distributed NoSQL wide-column store.
* **Key Design Paradigm:** denormalization by query pattern, with a partition key built from a geospatial cluster (using H3 or S2 spatial grids) paired with a timestamp.

```
TABLE: driver_location_logs
+-----------------------+---------------------+-------------+-------------------+
| Partition Key (Row)   | Clustering Key      | Column      | Column            |
| geo_hex_resolution_8  | timestamp (DESC)    | driver_id   | lat_lng           |
+-----------------------+---------------------+-------------+-------------------+
| "8826856231ffffff"    | 2026-09-19 10:30:01 | "drv_98124" | "37.7749,-122.41" |
| "8826856231ffffff"    | 2026-09-19 10:30:00 | "drv_11204" | "37.7742,-122.41" |
+-----------------------+---------------------+-------------+-------------------+
```

**Why this works for big data:**

* **Schema-on-Read mechanics:** raw location payloads 
  are dropped into the cluster rapidly, with no upfront 
  schema negotiation.
  
* **Optimized distributed reads:** using the geospatial 
  index (`geo_hex`) as the partition key means all drivers 
  in the same neighborhood live on the same physical node. 
  The system finds available rides in milliseconds, without 
  a cluster-wide scan.

---

### 6.2 🛍️ Example 2: E-Commerce Personalization — Star Schema on a Lakehouse (Schema-on-Read)

Modern e-commerce lakehouses (built on Delta Lake, Apache
Iceberg, or AWS Redshift) ingest billions of web clicks, cart
additions, and purchases to run recommendation algorithms and
executive dashboards.

* **Core Architecture:** cloud data lakehouse (e.g., Databricks / Snowflake).

* **Key Design Paradigm:** dimensional modeling (star schema) 
  optimized for massively parallel processing (MPP) engines 
  using **columnar storage (Parquet)**.

```
       dim_users (Dim)               dim_products (Dim)
   +------------------------+     +-------------------------+
   | user_id (PK)           |     | product_id (PK)         |
   | signup_date, user_tier |     | product_name, category  |
   +------------------------+     +-------------------------+
              |  1                         1  |
              |                               |
              v  M                         M  v
      +--------------------------------------------------+
      |              fact_user_clicks (Fact)             |
      +--------------------------------------------------+
      | click_id (PK)                                    |
      | user_id (FK) | product_id (FK) | date_key (FK)   |
      | click_timestamp, session_id, device_type,        |
      | dwell_time_seconds                               |
      +--------------------------------------------------+
```

**Why this works for big data:**

* **High denormalization:** the central fact table stores 
  event-driven historical telemetry at full grain. Columnar 
  formats let queries read only the columns needed (e.g., 
  skipping `device_type` when computing average 
  `dwell_time_seconds`).

* **Time-based partitioning:** the fact table is physically 
  partitioned by `date_key` (e.g., `year=2026/month=09/day=19`). 
  Queries for today's metrics skip past petabytes of historical 
  data sitting in older folders.

---

### 6.3 🏥 Example 3: Healthcare IoT Telemetry — Document-Based Time-Series Model

Hospital tracking systems monitor patient vitals (heart rate,
SpO2, blood pressure) from thousands of medical IoT sensors.
The data arrives as continuous streams of semi-structured JSON
telemetry, varying slightly depending on the manufacturer's
firmware.

* **Core Architecture:** document store / time-series database 
  (e.g., MongoDB time-series collections or TimescaleDB).

* **Key Design Paradigm:** the **bucket pattern** — grouping 
  sequential streaming records into unified documents to reduce 
  index size.

```json
{
  "_id": "patient_88319_2026-09-19_10:00:00",
  "patient_id": "pat_88319",
  "device_id": "iot_heart_monitor_v4",
  "metric_type": "heart_rate",
  "base_time": "2026-09-19T10:00:00Z",
  "measurements": [
    {"offset_seconds": 0, "value": 72},
    {"offset_seconds": 1, "value": 73},
    {"offset_seconds": 2, "value": 75},
    {"offset_seconds": 3, "value": 72}
  ],
  "device_metadata": {
    "firmware_version": "2.4.1",
    "battery_level_pct": 94
  }
}
```

**Why this works for big data:**

* **Handles variety easily:** if a new model of heart 
  monitor starts capturing an extra metric (like `signal_quality`), 
  the document schema absorbs the new key seamlessly — no migration 
  or database downtime required.

* **Prevents index bloat:** instead of writing a new row for every 
  single heartbeat (which generates billions of distinct entries 
  and overwhelms standard relational memory structures), this model 
  "buckets" data by device and hour. That dramatically shrinks index 
  size and keeps real-time tracking fast.

---

### 6.4 🤖 Example 4: LLM Retrieval-Augmented Generation (RAG) — Vector Embedding Store Model

Large Language Models don't query raw text — they query *meaning*.
Grounding an LLM's answers in your own documents, instead of relying
solely on what it memorized during training, means modeling millions
of unstructured documents as high-dimensional vectors that support
fast **similarity search** at big-data scale. It's the same
query-driven discipline from [Section 4](#4--query-driven-schema-design),
applied to a genuinely new access pattern: "find the *K* most
semantically similar chunks," not "find the row where `id = X`."

* **Core Architecture:** a vector database (e.g., Pinecone, 
  Weaviate, Milvus, or Postgres with `pgvector`), populated 
  by a distributed PySpark ingestion pipeline 
  (see [`slides/AI-LLM-Claude/`](./slides/AI-LLM-Claude)).

* **Key Design Paradigm:** documents are chunked and embedded 
  at big-data scale, then indexed with an **Approximate Nearest Neighbor (ANN)** 
  structure (e.g., HNSW or IVF) instead of the B-tree/hash indexes used for 
  exact-match lookups.

```
TABLE: document_chunks_vector_store
+---------------+-----------------------+-----------------------------------------+
| Column        | Type                  | Purpose                                 |
+---------------+-----------------------+-----------------------------------------+
| chunk_id (PK) | string                | Unique ID for this chunk of text        |
| document_id   | string (FK)           | Source document this chunk belongs to   |
| embedding     | vector<float32, 1536> | Semantic representation (ANN-indexed)   |
| chunk_text    | text                  | Raw text returned to the LLM as context |
| metadata      | JSON                  | source_url, page_number, chunk_offset   |
| created_at    | timestamp             | Freshness filtering / cache TTL         |
+---------------+-----------------------+-----------------------------------------+
```

**Why this works for big data:**

* **Handles volume and variety at ingestion:** a distributed 
  PySpark job chunks and embeds millions of heterogeneous 
  documents in parallel (PDFs, HTML, transcripts, support tickets) 
  — the same "LLM as a distributed mapper" pattern used to enrich 
  any large DataFrame.

* **Query-driven index design, in practice:** because the 
  dominant query is nearest-neighbor similarity search over 
  the `embedding` column, the physical layout is built around 
  an ANN index rather than an exact-match index — a schema 
  decision made to fit that one access pattern, exactly like 
  the geo-partitioning chosen in Example 1.

* **Keeps the model current without retraining:** updating 
  what the LLM "knows" becomes a data-modeling problem (re-chunk, 
  re-embed, re-index new or changed documents) rather than a costly 
  machine-learning problem (fine-tuning or retraining).

---

## 7. 🐍 End-to-End PySpark Example (Building Example 2's Star Schema)

The pipeline below is a complete, runnable implementation 
of the **E-Commerce Lakehouse Star Schema** from Example 2. 
It demonstrates the full modeling lifecycle: schema-on-read
ingestion, data-type correction, veracity checks (repairing 
bad values), multi-table dimensional joins, aggregation, 
and physical partitioning — the same concepts covered in
[`slides/spark/pyspark/`](./slides/spark/pyspark).

```python
# -*- coding: utf-8 -*-
"""
E-Commerce Lakehouse Data Modeling Pipeline in PySpark
Implements an optimized, schema-on-read star schema data model.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import col, to_timestamp, when, avg, count


def run_pipeline():
    # 1. Initialize a local Spark session
    spark = SparkSession.builder \
        .appName("EcommerceLakehouseModeling") \
        .config("spark.sql.shuffle.partitions", "4") \
        .master("local[*]") \
        .getOrCreate()

    print("\n====== 1. Spark Session Initialized ======")

    # 2. Raw landed telemetry (fact data - ingested web click-stream)
    raw_clicks_data = [
        ("clk_001", "usr_101", "prod_501", "2026-09-19 10:00:00", "sess_99", "desktop", 45),
        ("clk_002", "usr_102", "prod_502", "2026-09-19 10:01:15", "sess_98", "mobile", 120),
        ("clk_003", "usr_101", "prod_503", "2026-09-19 10:02:30", "sess_99", "desktop", 15),
        ("clk_004", "usr_103", "prod_501", "2026-09-19 10:05:00", "sess_97", "tablet", 200),
        ("clk_005", "usr_102", "prod_501", "2026-09-19 11:15:00", "sess_98", "mobile", -5),  # veracity issue: corrupt value
        ("clk_006", "usr_104", "prod_502", "2026-09-19 11:20:00", "sess_96", "mobile", 85),
    ]

    clicks_schema = ["click_id", "user_id", "product_id", "click_timestamp",
                      "session_id", "device_type", "dwell_time_seconds"]
    df_raw_clicks = spark.createDataFrame(raw_clicks_data, schema=clicks_schema)

    # 3. Dimension tables (contextual business attributes)
    users_data = [
        ("usr_101", "2025-01-10", "Premium"),
        ("usr_102", "2025-06-15", "Free"),
        ("usr_103", "2026-02-20", "Premium"),
        ("usr_104", "2026-08-01", "Free"),
    ]
    df_dim_users = spark.createDataFrame(users_data, ["user_id", "signup_date", "user_tier"])

    products_data = [
        ("prod_501", "Wireless Headphones", "Electronics"),
        ("prod_502", "Running Shoes", "Apparel"),
        ("prod_503", "Mechanical Keyboard", "Electronics"),
    ]
    df_dim_products = spark.createDataFrame(products_data, ["product_id", "product_name", "category"])

    print("\n====== 2. Raw Ingested Data Schema ======")
    df_raw_clicks.printSchema()

    # 4. Clean and structure: cast types, repair bad values, derive partition columns
    print("\n====== 3. Cleaning & Structuring ======")
    df_fact_clicks = df_raw_clicks \
        .withColumn("click_timestamp", to_timestamp(col("click_timestamp"))) \
        .withColumn("dwell_time_seconds", when(col("dwell_time_seconds") < 0, 0).otherwise(col("dwell_time_seconds"))) \
        .withColumn("year", year(col("click_timestamp"))) \
        .withColumn("month", month(col("click_timestamp"))) \
        .withColumn("day", dayofmonth(col("click_timestamp")))

    # 5. Join dimensions onto the fact stream to realize the star schema
    print("\n====== 4. Modeling the Star Schema via Distributed Joins ======")
    df_star_model = df_fact_clicks \
        .join(df_dim_users, on="user_id", how="inner") \
        .join(df_dim_products, on="product_id", how="inner")

    df_star_model.show(truncate=False)

    # 6. Aggregate across business dimensions
    print("\n====== 5. Aggregating Across Business Dimensions ======")
    df_analytics = df_star_model \
        .groupBy("category", "user_tier") \
        .agg(
            count("click_id").alias("total_clicks"),
            avg("dwell_time_seconds").alias("avg_dwell_time"),
        ) \
        .orderBy(col("total_clicks").desc())

    df_analytics.show(truncate=False)

    # 7. Write the fact table as partitioned, columnar Parquet
    output_path = "generated/lakehouse_fact_clicks"
    print(f"\n====== 6. Saving Partitioned Parquet Model to {output_path} ======")

    df_fact_clicks.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(output_path)

    print("\nPipeline execution completed successfully.")
    spark.stop()


if __name__ == "__main__":
    run_pipeline()
```

### ⚙️ Why This Reflects Core Big Data Modeling Principles

* **Schema-on-Read handling:** the incoming web click 
  records (`raw_clicks_data`) are loaded into a DataFrame 
  as plain text fields first; explicit timestamp casting 
  and structure are applied only afterward, during 
  transformation.

* **Data veracity checks:** the transformation step checks 
  for negative streaming metrics (`dwell_time_seconds < 0`) 
  and uses a `when().otherwise()` conditional to repair 
  corrupt payloads in-flight, without interrupting the 
  pipeline.

* **Optimized multi-dimensional joins:** the two dimension 
  DataFrames (`df_dim_users`, `df_dim_products`) are joined 
  against the fact stream using standard star-schema keys 
  (`user_id`, `product_id`).

* **Physical write partitioning:** 
  `.write.partitionBy("year", "month", "day").parquet(...)` 
  makes Spark create a hierarchical folder layout on disk. 
  Downstream query engines can then skip entire directories 
  that fall outside a requested date range, instead of 
  scanning the whole dataset.

This example was run end to end with PySpark 4.2.0 
(`local[*]` master) to confirm it executes cleanly 
and produces the aggregation shown in the pipeline's 
own `.show()` output.

---

## 8. 🧰 Tools for Big Data Modeling Analytics

The worked examples in [Section 6](#6--four-worked-examples) 
each paired a modeling technique with a purpose-built engine 
— Cassandra for wide-column geo-partitioning, Delta Lake/Iceberg 
for the lakehouse star schema, TimescaleDB for the IoT bucket 
pattern, Pinecone/Milvus for vector search. The tools below are 
the more general-purpose, enterprise-grade platforms a big data 
modeler works with day to day: scalable storage systems, 
distributed processing engines, transformation frameworks, and 
the BI layer that sits on top of all of it.


### 8.1 🗄️ Storage & Data Warehouses

* **Snowflake:** a cloud data platform with separated compute
  and storage for automated scaling.

* **Google BigQuery:** a serverless, highly scalable enterprise
  data warehouse for rapid SQL queries.

* **MongoDB:** a NoSQL document database designed for handling
  flexible and unstructured data schemas — the same document
  model used for the bucket pattern in
  [Example 3](#63--example-3-healthcare-iot-telemetry--document-based-time-series-model).

* **Amazon Athena (with S3):** a serverless SQL query engine
  for data sitting directly in S3, with no cluster to manage
  (see [`slides/amazon_athena/`](./slides/amazon_athena)).


### 8.2 ⚡ Processing & Analytics Engines

* **Apache Spark:** an in-memory distributed processing engine
  used for large-scale data transformation and real-time analytics
  — the engine behind [Section 7](#7--end-to-end-pyspark-example-building-example-2s-star-schema)'s
  worked pipeline (see [`slides/spark/`](./slides/spark)).

* **Apache Hadoop & MapReduce Paradigm:** a legacy framework for 
  distributed storage (HDFS/S3) and MapReduce-based batch processing 
  across server clusters (see [`slides/mapreduce/`](./slides/mapreduce)).

* **Databricks:** a unified lakehouse platform built 
  on top of Spark for collaborative data engineering 
  and machine learning.


### 8.3 🔧 Data Modeling & Transformation

* **dbt (Data Build Tool):** a transformation workflow 
  tool that lets teams build and test data models inside 
  their data warehouse using SQL.

* **erwin Data Modeler:** enterprise modeling software used
  to design, document, and manage complex database structures
  — most often applied at the [conceptual and logical](#3--the-three-core-perspectives)
  stages, before a physical big-data platform is chosen.


### 8.4 📊 Visualization & Business Intelligence

* **Tableau:** a leading platform for complex 
  exploratory dashboards and visual analytics.

* **Microsoft Power BI:** a business intelligence 
  tool that integrates natively with cloud  and 
  spreadsheet ecosystems.

---

