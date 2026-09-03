# Big Data Modeling
### Course Information — Fall Quarter 2026

[Santa Clara University](http://scu.edu/) · [Big Data Modeling & Analytics](https://www.scu.edu/business/graduate-degrees/ms-programs/ms-information-systems/curriculum/)

---

## Table of Contents

* [Main Subjects](#main-subjects)
* [Course Description](#course-description)
* [Course Objectives](#course-objectives)
* [Required Books](#required-books)
* [Required Software, API, and Documentation](#required-software)
* [Tentative Course Outline](#tentative-outline)
* [Key Dates at a Glance](#key-dates)
* [Course Schedule Overview](#schedule-overview)
* [Session-by-Session Details](#session-details)

---

<a id="main-subjects"></a>
## Main Subjects

| # | Subject                             | Weight |
|---|--------------------------------------|-------:|
| 1 | Big Data Concepts                    |    10% |
| 2 | MapReduce Paradigm                   |    20% |
| 3 | Big Data Analytics by PySpark        |    50% |
| 4 | Data Partitioning and SQL Queries    |    20% |

---

<a id="course-description"></a>
## Course Description

* Understand the fundamentals of big data
* Understand the fundamentals of the MapReduce paradigm
* Use PySpark (Python API for Apache Spark) to solve big data problems
* Use SQL for NoSQL data (DataFrames in Spark and Amazon Athena)
* Understand Amazon Athena & Google BigQuery: access & analyze big data by SQL

---

<a id="course-objectives"></a>
## Course Objectives

At the completion of this course, students will be able to understand:

**Elements of Big Data**:

- Cluster computing
- Persistence, queries, analytics
- Data replication
- Distributed file system and fault tolerance
- Scale-out architecture vs. scale-up architecture

**What is the MapReduce paradigm?**

- Data partitioning and partitions
- Mapper function: `map()`
- Reducer function: `reduce()`
- Combiner function: `combine()`
- Sort & shuffle: SQL's `GROUP BY`
- Classic MapReduce algorithms
- Data design patterns

**Fundamentals of Spark and PySpark**

- Spark architecture
- Spark: engine for large-scale data analytics
- Data abstractions in Spark and PySpark
- RDDs and DataFrames
- Transformations and actions
- Running simple programs in PySpark

**NoSQL Databases & Serverless Architectures**

- SQL for NoSQL data & relational algebra
- Amazon Athena and SQL
- Google BigQuery and SQL

---

<a id="required-books"></a>
## Required Books

| # | Book | Used For |
|---|------|----------|
| 1 | [*Data-Intensive Text Processing with MapReduce*](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf) — Jimmy Lin & Chris Dyer | First 3 weeks of class |
| 2 | [*Data Algorithms with Spark*](https://www.oreilly.com/library/view/data-algorithms-with/9781492082378/) — Mahmoud Parsian | Remaining 7 weeks of class |

---

<a id="required-software"></a>
## Required Software, API, and Documentation

* [Apache Spark (main site)](http://spark.apache.org)
* [PySpark API and documentation](https://spark.apache.org/docs/latest/api/python/index.html)
* [RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
* [DataFrame Programming Guide](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html)

---

<a id="tentative-outline"></a>
## Tentative Course Outline

> **Note:** The weekly coverage below is tentative and may shift depending on class progress. Regardless of schedule changes, students must keep up with all reading and programming assignments.

---

<a id="key-dates"></a>
## Key Dates at a Glance

| Event | Date | Notes |
|-------|------|-------|
| 📝 Exam 1 | TBD (October 2026) | In-class · LockDown Browser required · closed book/notes/internet/AI/friends/phone/computer/software |
| 📝 Exam 2 | TBD (November 2026) | In-class · LockDown Browser required · closed book/notes/internet/AI/friends/phone/computer/software |
| 🦃 Thanksgiving Recess | November 23–27, 2026 | No classes · no office hours |
| 🎓 Final Exam | TBD, December 8–12, 2026 | In-class · LockDown Browser required · closed book/notes/internet/AI/friends/phone/computer/software |

---

<a id="schedule-overview"></a>
## Course Schedule Overview

| # | Day | Date | Topic |
|---|-----|------|-------|
| 1  | Monday    | Sep 21, 2026 | [Introduction to Big Data and Cluster Computing](#session-1) |
| 2  | Wednesday | Sep 23, 2026 | [Introduction to Big Data and MapReduce](#session-2) |
| 3  | Monday    | Sep 28, 2026 | [Introduction to MapReduce](#session-3) |
| 4  | Wednesday | Sep 30, 2026 | [Introduction to MapReduce & Join Operations](#session-4) |
| 5  | Monday    | Oct 5, 2026  | [Review: MapReduce Paradigm & PySpark](#session-5) |
| 6  | Wednesday | Oct 7, 2026  | 📝 [Exam 1 (in-class)](#session-6) |
| 7  | Monday    | Oct 12, 2026 | [Introduction to Spark & PySpark](#session-7) |
| 8  | Wednesday | Oct 14, 2026 | [Introduction to Spark and PySpark](#session-8) |
| 9  | Monday    | Oct 19, 2026 | [Spark's Nuts and Bolts](#session-9) |
| 10 | Wednesday | Oct 21, 2026 | [Data Design Patterns](#session-10) |
| 11 | Monday    | Oct 26, 2026 | [Data Design Patterns](#session-11) |
| 12 | Wednesday | Oct 28, 2026 | [RDD Design Patterns · Review for Exam 2](#session-12) |
| 13 | Monday    | Nov 2, 2026  | [Independent Study](#session-13) |
| 14 | Wednesday | Nov 4, 2026  | 📝 [Exam 2 (in-class)](#session-14) |
| 15 | Monday    | Nov 9, 2026  | [Spark's DataFrames (1)](#session-15) |
| 16 | Wednesday | Nov 11, 2026 | [Spark's DataFrames (2)](#session-16) |
| 17 | Monday    | Nov 16, 2026 | [Graph Data Structures & MapReduce Design Patterns](#session-17) |
| 18 | Wednesday | Nov 18, 2026 | [MapReduce Design Pattern: Graph Algorithms](#session-18) |
| —  | Mon–Fri   | Nov 23–27, 2026 | 🦃 [Thanksgiving Recess](#thanksgiving) |
| 19 | Monday    | Nov 30, 2026 | [Serverless Analytics & SQL Access to Big Data](#session-19) |
| 20 | Wednesday | Dec 2, 2026  | [Review for Final Exam](#session-20) |
| 21 | TBD       | 12/08–12/12/2026 | 🎓 [Final Exam](#session-21) |

---

<a id="session-details"></a>
## Session-by-Session Details

<a id="session-1"></a>
### Session 1: Monday, September 21, 2026

**Topic:** Introduction to Big Data and MapReduce

**Required:**
- [1. Introduction to Big Data and Solutions](../../slides/big_data/2_introduction_to_big_data_and_solutions.pdf)
- [2. MapReduce: Simplified Data Processing on Large Clusters - Google paper](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)
- [3. Chapter 1 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)
- [4. A Very Brief Introduction to MapReduce](http://hci.stanford.edu/courses/cs448g/a2/files/map_reduce_tutorial.pdf)

**Optional:**
- [1. Introduction to Big Data](https://lagesoft.files.wordpress.com/2018/11/bd-introduction-to-big-data.pdf)
- [2. Introduction to MapReduce](http://lsd.ls.fi.upm.es/lsd/nuevas-tendencias-en-sistemas-distribuidos/IntroToMapReduce_2.pdf)

[⇧ back to schedule](#schedule-overview)

---

<a id="session-2"></a>
### Session 2: Wednesday, September 23, 2026

**Topic:** Introduction to Big Data and MapReduce

**Required:**
- [1. MapReduce Tutorial Slides by Jimmy Lin](https://cs.uwaterloo.ca/~jimmylin/publications/WWW2013-MapReduce-tutorial-slides.pdf)
- [2. Chapter 2 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)
- [3. Introduction to MapReduce by Mahmoud Parsian](http://mapreduce4hackers.com/docs/Introduction-to-MapReduce.pdf)
- [4. MapReduce: Simplified Data Processing on Large Clusters - Google paper](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)

**Optional:**
- [1. MapReduce, Wikipedia](https://en.wikipedia.org/wiki/MapReduce)
- [2. Introduction to MapReduce and Hadoop by Matei Zaharia](https://github.com/mahmoudparsian/big-data-mapreduce-course/blob/master/slides/mapreduce/introduction_to_mapreduce/MapReduce_by_Matei_Zaharia.pdf)

[⇧ back to schedule](#schedule-overview)

---

<a id="session-3"></a>
### Session 3: Monday, September 28, 2026

**Topic:** Introduction to MapReduce

**Required:**
- [1. Introduction to MapReduce](https://kodu.ut.ee/~srirama/cloud/2011/L3_MapReduce.pdf)
- [2. Chapter 3 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)
- [3. Chapter 4 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)

**Optional:**
- [1. Introduction to MapReduce: Watch a Video](https://www.youtube.com/watch?v=ht3dNvdNDzI&t=250s)
- [2. The Future of Big Data by Matei Zaharia — Video](https://www.youtube.com/watch?v=oSj2vYw5RLs)
- [3. Introduction to MapReduce and Hadoop by Matei Zaharia](https://github.com/mahmoudparsian/big-data-mapreduce-course/blob/master/slides/mapreduce/introduction_to_mapreduce/MapReduce_by_Matei_Zaharia.pdf)

[⇧ back to schedule](#schedule-overview)

---

<a id="session-4"></a>
### Session 4: Wednesday, September 30, 2026

**Topic:** Introduction to MapReduce & Join Operations

**Required:**
- [Chapter 3 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)
- [Chapter 4 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)
- [Chapter 5 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)
- [Join Algorithms in Action Using MapReduce](../../slides/mapreduce/joins_in_mapreduce/join_operation_in_action_using_MapReduce.md)

**Optional:**
- [Simplifying Big Data Applications with Apache Spark 2.0 by Matei Zaharia](https://www.youtube.com/watch?v=Zb9YW8XjxnE)
- [Relational Operations Using MapReduce](https://medium.com/swlh/relational-operations-using-mapreduce-f49e8bd14e31)

[⇧ back to schedule](#schedule-overview)

---

<a id="session-5"></a>
### Session 5: Monday, October 5, 2026

**Topic:**
- Review of MapReduce paradigm with examples
- Review of MapReduce implementation with PySpark, with examples

[⇧ back to schedule](#schedule-overview)

---

<a id="session-6"></a>
### Session 6: Wednesday, October 7, 2026

**📝 Exam 1 — in-class** *(date tentative — see [`exam_dates.md`](../../course_information/exam_dates.md) for the confirmed date)*
- LockDown Browser is required
- Closed book/notes/internet/AI/friends/phone/computer/software

[⇧ back to schedule](#schedule-overview)

---

<a id="session-7"></a>
### Session 7: Monday, October 12, 2026

**Topic:** Introduction to Spark & PySpark

**Required:**
- [A Gentle Introduction to Apache Spark](https://pages.databricks.com/rs/094-YMS-629/images/A-Gentle-Introduction-to-Apache-Spark.pdf)
- [Chapters 1, 2 of Data Algorithms with Spark by Mahmoud Parsian](https://www.amazon.com/Data-Algorithms-Spark-Recipes-Patterns/dp/1492082384/ref=sr_1_1)
- [PySpark Tutorial](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/pyspark_tutorial)
- [Classic Word Count in PySpark](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/wordcount)

**Optional:**
- [Learning Spark (book)](https://pages.databricks.com/rs/094-YMS-629/images/LearningSpark2.0.pdf)
- [Introduction to Apache Spark](https://stanford.edu/~rezab/sparkclass/slides/itas_workshop.pdf)

[⇧ back to schedule](#schedule-overview)

---

<a id="session-8"></a>
### Session 8: Wednesday, October 14, 2026

**Topic:** Introduction to Spark and PySpark (Python API for Spark)

**Required:**
- [Classic Word Count in PySpark](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/wordcount)
- [A Gentle Introduction to Apache Spark](https://pages.databricks.com/rs/094-YMS-629/images/A-Gentle-Introduction-to-Apache-Spark.pdf)
- [Chapters 1, 2, 3, 4 of Data Algorithms with Spark by Mahmoud Parsian](https://www.amazon.com/Data-Algorithms-Spark-Recipes-Patterns/dp/1492082384/ref=sr_1_1)
- [Learning Spark (book)](https://pages.databricks.com/rs/094-YMS-629/images/LearningSpark2.0.pdf)

**Optional:**
- [Introduction to Spark](http://www.slideshare.net/jeykottalam/spark-sqlamp-camp2014)
- [Introduction to Spark by Shannon Quinn](https://web.archive.org/web/20230803213727/http://cobweb.cs.uga.edu/~squinn/mmd_s15/lectures/lecture13_mar3.pdf)

[⇧ back to schedule](#schedule-overview)

---

<a id="session-9"></a>
### Session 9: Monday, October 19, 2026

**Topic:** Spark's Nuts and Bolts

**Required:**
- [PySpark Tutorial](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/pyspark_tutorial)
- [Chapters 3, 4, 5 of Data Algorithms with Spark by Mahmoud Parsian](https://www.amazon.com/Data-Algorithms-Spark-Recipes-Patterns/dp/1492082384/ref=sr_1_1)
- [Learning Spark (book)](https://pages.databricks.com/rs/094-YMS-629/images/LearningSpark2.0.pdf)

**Optional:**
- [Introduction to Spark](http://www.slideshare.net/jeykottalam/spark-sqlamp-camp2014)
- [Parallel Programming With Spark by Matei Zaharia](https://web.archive.org/web/20191228201919/http://ampcamp.berkeley.edu:80/wp-content/uploads/2013/02/Parallel-Programming-With-Spark-Matei-Zaharia-Strata-2013.pptx)

[⇧ back to schedule](#schedule-overview)

---

<a id="session-10"></a>
### Session 10: Wednesday, October 21, 2026

**Topic:** Data Design Patterns

**Required:**
- [PySpark Tutorial](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/pyspark_tutorial)
- [MinMax Algorithm](https://github.com/mahmoudparsian/pyspark-tutorial/blob/master/tutorial/map-partitions/README.md)
- [Top-10 Algorithm](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/Top-N)

[⇧ back to schedule](#schedule-overview)

---

<a id="session-11"></a>
### Session 11: Monday, October 26, 2026

**Topic:** Data Design Patterns

**Required:**
- [Chapters 3, 4, 5 of Data Algorithms with Spark by Mahmoud Parsian](https://www.amazon.com/Data-Algorithms-Spark-Recipes-Patterns/dp/1492082384/ref=sr_1_1)
- Data Design Patterns: In-Mapper Combiner, `mapPartitions()`
- [Top-10 Algorithm](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/Top-N)
- [MinMax Algorithm](https://github.com/mahmoudparsian/pyspark-tutorial/blob/master/tutorial/map-partitions/README.md)

**Optional:**
- [Chapters 4, 6, 7, 12 of PySpark Algorithms by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms)

[⇧ back to schedule](#schedule-overview)

---

<a id="session-12"></a>
### Session 12: Wednesday, October 28, 2026

**Topic:** RDD Design Patterns

**Required:**
- Spark's RDD partitioning
- [Chapters 3, 4, 5 of Data Algorithms with Spark by Mahmoud Parsian](https://www.amazon.com/Data-Algorithms-Spark-Recipes-Patterns/dp/1492082384/ref=sr_1_1)
- Spark's `mapPartitions()` transformation
- [mapPartitions() Tutorial](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/mappartitions)
- Review reducers: `groupByKey()`, `reduceByKey()`, and `combineByKey()`

**Also:**
- Review for Exam 2
- Problem solving & Q/A session

[⇧ back to schedule](#schedule-overview)

---

<a id="session-13"></a>
### Session 13: Monday, November 2, 2026

**Topic:** Independent Study

[⇧ back to schedule](#schedule-overview)

---

<a id="session-14"></a>
### Session 14: Wednesday, November 4, 2026

**📝 Exam 2 — in-class** *(date tentative — see [`exam_dates.md`](../../course_information/exam_dates.md) for the confirmed date)*
- LockDown Browser is required
- Closed book/notes/internet/AI/friends/phone/computer/software

[⇧ back to schedule](#schedule-overview)

---

<a id="session-15"></a>
### Session 15: Monday, November 9, 2026

**Topic:** Spark's DataFrames (1)

- [Chapters 4, 6, 7, 12 of PySpark Algorithms by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms)
- [Video: Structuring Spark — SQL, DataFrames, Datasets and Streaming (28 min)](https://www.youtube.com/watch?v=1a4pgYzeFwE)

[⇧ back to schedule](#schedule-overview)

---

<a id="session-16"></a>
### Session 16: Wednesday, November 11, 2026

**Topic:** Spark's DataFrames (2)

- [Chapters 4, 6, 7, 12 of PySpark Algorithms by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms)
- [Video: Structuring Spark — SQL, DataFrames, Datasets and Streaming (28 min)](https://www.youtube.com/watch?v=1a4pgYzeFwE)

[⇧ back to schedule](#schedule-overview)

---

<a id="session-17"></a>
### Session 17: Monday, November 16, 2026

**Topic:** Introduction to graph data structures; MapReduce design pattern — graph algorithms

- [Chapter 6 of Data Algorithms with Spark by Mahmoud Parsian](https://www.amazon.com/Data-Algorithms-Spark-Recipes-Patterns/dp/1492082384/ref=sr_1_1)
- [Chapter 11 of PySpark Algorithms by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms)

[⇧ back to schedule](#schedule-overview)

---

<a id="session-18"></a>
### Session 18: Wednesday, November 18, 2026

**Topic:** MapReduce design pattern — graph algorithms

- [Chapter 6 of Data Algorithms with Spark by Mahmoud Parsian](https://www.amazon.com/Data-Algorithms-Spark-Recipes-Patterns/dp/1492082384/ref=sr_1_1)
- [Chapter 11 of PySpark Algorithms by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms)

[⇧ back to schedule](#schedule-overview)

---

<a id="thanksgiving"></a>
### Thanksgiving Recess: November 23-27, 2026

* 🦃 Academic holiday — no classes
* No office hours

[⇧ back to schedule](#schedule-overview)

---

<a id="session-19"></a>
### Session 19: Monday, November 30, 2026

**Topic:** Introduction to serverless analytics; SQL access to big data

- SQL access: Amazon Athena
- SQL access: Google BigQuery

[⇧ back to schedule](#schedule-overview)

---

<a id="session-20"></a>
### Session 20: Wednesday, December 2, 2026

**Topic:** Review for final exam; Q/A session

[⇧ back to schedule](#schedule-overview)

---

<a id="session-21"></a>
### Session 21: Final Exam

**🎓 In-class exam**
- LockDown Browser is required
- Closed book/notes/internet/AI/friends/phone/computer/software
- Date: TBD (December 8–12, 2026)
- Time: TBD

[⇧ back to schedule](#schedule-overview)
