# Fall 2022 Course Information

## [Santa Clara University](http://scu.edu/)

## [MSIS 2627: Big Data Modeling and Analytics](https://www.scu.edu/business/ms-information-systems/curriculum/msis-courses/)

-----------------

## Course Description & Objectives

* Understand the fundamentals of big data 
* Understand the fundamentals of MapReduce Paradigm
* Use PySpark (Python API to Spark) to solve big data problems
* Use SQL for NoSQL data (DataFrames in Spark and Amazon Athena)
* Understand Amazon Athena: Access Big Data by SQL

-----------------

## Course Objectives

At the completion of this course, students will be able to understand:

* What is MapReduce? Examples of MapReduce
* Elements of Big Data: Persistence, Queries, Analytics
* Distributed File System and Fault Tolerance
* Introduction to classic MapReduce Algorithms
* Understand Spark and Hadoop frameworks
* MapReduce algorithms and some design patterns
* NoSQL Databases
* Fundamentals of Spark and PySpark
* Running simple programs in PySpark
* Scale-out vs. Scale-up
* SQL for NoSQL data & Relational Algebra
* Amazon Athena and SQL

----------------
## Required books (all resources are online):

* [1. Data Algorithms with Spark by Mahmoud Parsian](https://www.oreilly.com/library/view/data-algorithms-with/9781492082378/)
* [2. Data-Intensive Text Processing with MapReduce by Jimmy Lin and Chris Dyer](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)

## Extra books (all resources are online):
* [3. Data Algorithms with Spark by Mahmoud Parsian](https://www.oreilly.com/library/view/data-algorithms-with/9781492082378/)

------------------

## Required Software

* [Apache Spark](http://spark.apache.org)

* [PySpark: Python API for Spark](http://spark.apache.org/docs/latest/api/python/index.html)


-------------------

## Tentative Course Outline

The weekly coverage might change as it depends 
on the progress of the class. However, you must 
keep up with the reading and programming assignments.

### Main Subjects

* Classic MapReduce (Jimmy Lin's Book)
	* Solve Big Data problems using `map()`, `combine()`, and `reduce()` functions
	* up to 25%

* PySpark and Spark (Mahmoud Parsian's book: Data Algorithms with Spark)
	* up to 65%

* Data Partitioning and Amazon Athena and Google BigQuery
	* up to 10%

----------------

### Session-1:  Tuesday, September 20, 2022
	* Introduction to Big Data
	* [Chapter 1 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf) 
	* [A Very Brief Introduction to MapReduce](http://hci.stanford.edu/courses/cs448g/a2/files/map_reduce_tutorial.pdf) 
	* [Introduction to MapReduce](http://lsd.ls.fi.upm.es/lsd/nuevas-tendencias-en-sistemas-distribuidos/IntroToMapReduce_2.pdf) 
	* [MapReduce: Simplified Data Processing on Large Clusters by Jeffrey Dean and Sanjay Ghemawat](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)

----------------

### Session-2: Thursday, September 22, 2022
	* Introduction to Big Data 
	* [MapReduce wikipedia](https://en.wikipedia.org/wiki/MapReduce)
	* [A Comprehensive Study on MapReduce Applications](https://github.com/mahmoudparsian/big-data-mapreduce-course/blob/master/slides/mapreduce/A_Comprehensive_Study_on_MapReduce_Applications.pdf)
	* [MapReduce Tutorial Slides by Jimmy Lin](https://cs.uwaterloo.ca/~jimmylin/publications/WWW2013-MapReduce-tutorial-slides.pdf)
	* [Chapter 2 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf) 
	* [Introduction to MapReduce by Mahmoud Parsian](http://mapreduce4hackers.com/docs/Introduction-to-MapReduce.pdf) 
	* [Introduction to MapReduce and Hadoop by Matei Zaharia](https://github.com/mahmoudparsian/big-data-mapreduce-course/blob/master/slides/mapreduce/MapReduce_by_Matei_Zaharia.pdf) 
	* [MapReduce: Simplified Data Processing on Large Clusters by Jeffrey Dean and Sanjay Ghemawat](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)

----------------

### Session-3: Tuesday, September 27, 2022
	* Introduction to MapReduce: Watch a Video 
	* [The Future of Big Data by Matei Zaharia -- Video](https://www.youtube.com/watch?v=oSj2vYw5RLs)   
	* [1st half of Chapter 3 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)   
	* [Introduction to MapReduce and Hadoop by Matei Zaharia](https://github.com/mahmoudparsian/big-data-mapreduce-course/blob/master/slides/mapreduce/MapReduce_by_Matei_Zaharia.pdf)  

----------------

### Session-4: Thursday, September 29, 2022
	* Introduction to MapReduce: Watch a Video    
	* [Simplifying Big Data Applications with Apache Spark 2.0 by Matei Zaharia](https://www.youtube.com/watch?v=Zb9YW8XjxnE)  
	* [2nd half of Chapter 3 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf) 
	* [Introduction to MapReduce and Hadoop by Matei Zaharia](https://github.com/mahmoudparsian/big-data-mapreduce-course/blob/master/slides/mapreduce/MapReduce_by_Matei_Zaharia.pdf)   
	* [NEW: Introduction to MapReduce](https://courses.cs.ut.ee/MTAT.08.011/2013_spring/uploads/Main/L3_MapReduce.pdf)
	* [NEW: MapReduce Algorithms](https://courses.cs.ut.ee/MTAT.08.027/2018_spring/uploads/Main/L5_MapReduceAlgorithms2018.pdf)
	* Classic Join in MapReduce

----------------

* **Session-5: Tuesday, April 12, 2022**
	* Introduction to MapReduce/Hadoop/Spark 
	* [Chapters 1, 2 of PySpark Algorithms Book by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms) 
	* [Chapter 4 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)  
	* [Introduction to MapReduce and Hadoop by Matei Zaharia](https://github.com/mahmoudparsian/big-data-mapreduce-course/blob/master/slides/mapreduce/MapReduce_by_Matei_Zaharia.pdf) 

----------------

* **Session-6: Thursday, April 14, 2022**
	* Practice MapReduce/Hadoop/Spark...
	* [Chapters 1, 2, 3 of PySpark Algorithms Book by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms) 
	* [Introduction to Spark](http://www.slideshare.net/jeykottalam/spark-sqlamp-camp2014)  
	* [Introduction to Spark by Shannon Quinn](http://cobweb.cs.uga.edu/~squinn/mmd_s15/lectures/lecture13_mar3.pdf)  

----------------

* **Session-7: Tuesday, April 19, 2022**
	* Spark's Nuts and Bolts    
	* [Chapters 4, 5 of PySpark Algorithms Book by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms)
	* [Making Big Data Simple: by Matei Zaharia](https://www.youtube.com/watch?v=Nev1s6fHwMI) 
	* [Introduction to Spark](http://www.slideshare.net/jeykottalam/spark-sqlamp-camp2014)  
	* [Parallel-Programming-With-Spark-Matei-Zaharia](http://ampcamp.berkeley.edu/wp-content/uploads/2013/02/Parallel-Programming-With-Spark-Matei-Zaharia-Strata-2013.pptx) 

----------------

* **Session-8: Thursday, April 21, 2022**
	* MapReduce Design Patterns
	* MinMax  
	* Top-10  
	* [Chapters 4, 5, 6 of PySpark Algorithms Book by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms) 
	* [Chapter 3, 4 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)  

----------------

* **Session-9: Tuesday, April 26, 2022**
	* MapReduce Design Patterns: InMapper Combiner, mapPartitions
	* Top-10 Algorithm   
	* MinMax Algorithm  
	* [Chapters 4, 6, 7, 12 of PySpark Algorithms Book by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms)

----------------

* **Session-10: Thursday, April 28, 2022**
	* Spark's mapPartitions() Transformation

----------------

* **Session-11: Tuesday, May 3, 2022**
	* Review mapPartitions(), reducers (groupByKey()  and reduceByKey())
 
----------------

* **Session-12: Thursday, May 5, 2022**
	* Review Midterm Exam 
	
----------------

* **Session-13: Tuesday, May 10, 2022**
	* Midterm Exam: closed book/notes/friend/internet

----------------

* **Session-14: Thursday, May 12, 2022**
	* Spark's DataFrames 
	* [Chapters 4, 6, 7, 12 of PySpark Algorithms Book by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms)
	* [Video: Structuring Spark: SQL, DataFrames, Datasets And Streaming - 28 mins](https://www.youtube.com/watch?v=1a4pgYzeFwE)


----------------

* **Session-15: Tuesday, May 17, 2022**
	* MapReduce Design Pattern: Graph Algorithms 
	* [Chapters 11 of PySpark Algorithms Book by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms)
 
----------------

* **Session-16: Thursday, May 19, 2022**
	* Introduction to Serverless Analytics  
	* SQL Access: Amazon Athena 
	* SQL Access: Google BigQuery 

----------------

* **Session-17: Tuesday, May 24, 2022**
	* Introduction to Serverless Analytics 
	* SQL Access: Apache Presto 
	* SQL Access: Amazon Athena 
	* SQL Access: Google BigQuery 

----------------

* **Session-18: Thursday, May 26, 2022**
	* Relational Algebra and Big Data  
	* SQL Access to Big Data  

----------------

* **Session-19: Tuesday, May 31, 2022**
	* Stream Processing 
	* [Spark Streaming](https://spark.apache.org/streaming/) 
	* [How to Perform Distributed Spark Streaming With PySpark](https://dzone.com/articles/distributed-spark-streaming-pyspark)
 
----------------

* **Session-20: Thursday, June 2, 2022**
	* Review for Final Exam 

----------------

* **Session-21: Final Exam**  
	* Date: June 6-9, 2022 (TBDL)


----------------
