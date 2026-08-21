# rdd_transformations

Lecture slides on RDD transformations (`map`, `filter`, `flatMap`, `mapValues`, `mapPartitions`, partitioning); diagrams in `images/`.

## Contents

| Name | Type | Description |
|---|---|---|
| [`images/`](images/) | folder | 4 items |
| [`5.0_data_abstraction_in_Spark.md`](5.0_data_abstraction_in_Spark.md) | md (1.8KB) | Data abstraction in Apache Spark |
| [`5.1_creating_rdds.pptx`](5.1_creating_rdds.pptx) | pptx (177.5KB) | Creating RDDs in PySpark; why Spark's in-memory model goes beyond classic MapReduce |
| [`5.2_map_transformation.pptx`](5.2_map_transformation.pptx) | pptx (336.0KB) | `RDD.map()` — the 1-to-1 transformation |
| [`5.3_filter_transformation.pptx`](5.3_filter_transformation.pptx) | pptx (250.9KB) | `RDD.filter(f)` — dropping elements with a Boolean predicate |
| [`5.4_flatmap_transformation.pptx`](5.4_flatmap_transformation.pptx) | pptx (259.4KB) | `RDD.flatMap(f)` — the 1-to-many transformation |
| [`5.5_mapvalues_transformation.pptx`](5.5_mapvalues_transformation.pptx) | pptx (156.4KB) | `RDD.mapValues(f)` — applying `f` to the value of `(K, V)` pairs |
| [`5.6_partitioning_in_spark.pptx`](5.6_partitioning_in_spark.pptx) | pptx (132.3KB) | What partitioning is and why it enables parallel processing |
| [`5.7_mappartitions.pptx`](5.7_mappartitions.pptx) | pptx (246.4KB) | `RDD.mapPartitions(f)` — the summarization design pattern (whole partition → single element) |
| [`5.8_transformations_API_and_tutorial_and_videos.pptx`](5.8_transformations_API_and_tutorial_and_videos.pptx) | pptx (229.5KB) | Pointers to the Spark Transformations API, docs, tutorials, and videos |
