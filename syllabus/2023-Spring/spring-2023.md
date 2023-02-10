<center>Course Information <br/> for <br/> Spring Quarter 2023</center>
----------------


## [Santa Clara University](http://scu.edu/)

## [MSIS 2527: Big Data Modeling & Analytics](https://www.scu.edu/business/graduate-degrees/ms-programs/ms-information-systems/curriculum/)

-----------------

## Course Description & Objectives

* Understand the fundamentals of big data 
* Understand the fundamentals of MapReduce Paradigm
* Use PySpark (Python API for Apache Spark) to solve big data problems
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

* [1. PySpark Algorithms by Mahmoud Parsian](https://www.amazon.com/PySpark-Algorithms-Version-Mahmoud-Parsian-ebook/dp/B07X4B2218)
* [2. Mining of Massive Datasets by Jure Leskovec, Anand Rajaraman, Jeffrey D. Ullman](http://infolab.stanford.edu/~ullman/mmds/book0n.pdf)

------------------

## Required Software, API, and Documentation

* [Apache Spark (main site)](http://spark.apache.org)
* [PySpark API and documentation](https://spark.apache.org/docs/latest/api/python/index.html)
* [RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
* [DataFrame Programming Guide](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html)

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

### Session-1:  Monday, April 3, 2023

TOPIC: Introduction to Big Data and MapReduce

* REQUIRED:
	* [MapReduce: Simplified Data Processing on Large Clusters by Jeffrey Dean and Sanjay Ghemawat](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)
	* [Chapter 1 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf) 
	* [A Very Brief Introduction to MapReduce](http://hci.stanford.edu/courses/cs448g/a2/files/map_reduce_tutorial.pdf) 
* OPTIONAL:
	* [Introduction to Big Data](https://lagesoft.files.wordpress.com/2018/11/bd-introduction-to-big-data.pdf)
	* [Introduction to MapReduce](http://lsd.ls.fi.upm.es/lsd/nuevas-tendencias-en-sistemas-distribuidos/IntroToMapReduce_2.pdf) 

----------------

### Session-2: Wednesday, April 5, 2023
	
TOPIC: Introduction to Big Data and MapReduce

* REQUIRED:
	* [MapReduce Tutorial Slides by Jimmy Lin](https://cs.uwaterloo.ca/~jimmylin/publications/WWW2013-MapReduce-tutorial-slides.pdf)
	* [Chapter 2 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf) 
	* [Introduction to MapReduce by Mahmoud Parsian](http://mapreduce4hackers.com/docs/Introduction-to-MapReduce.pdf) 
	* [MapReduce: Simplified Data Processing on Large Clusters by Jeffrey Dean and Sanjay Ghemawat](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)

* OPTIONAL:
	* [MapReduce wikipedia](https://en.wikipedia.org/wiki/MapReduce)
	* [A Comprehensive Study on MapReduce Applications](https://github.com/mahmoudparsian/big-data-mapreduce-course/blob/master/slides/mapreduce/A_Comprehensive_Study_on_MapReduce_Applications.pdf)
	* [Introduction to MapReduce and Hadoop by Matei Zaharia](https://github.com/mahmoudparsian/big-data-mapreduce-course/blob/master/slides/mapreduce/MapReduce_by_Matei_Zaharia.pdf) 

----------------

### Session-3: Monday, April 10, 2023

TOPIC: Introduction to MapReduce

* REQUIRED:
	* [Introduction to MapReduce](https://courses.cs.ut.ee/MTAT.08.011/2013_spring/uploads/Main/L3_MapReduce.pdf)
	* [Chapter 3 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)   
	* [Chapter 4 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)   

* OPTIONAL:
	* [Introduction to MapReduce: Watch a Video](https://www.youtube.com/watch?v=ht3dNvdNDzI&t=250s) 
	* [The Future of Big Data by Matei Zaharia -- Video](https://www.youtube.com/watch?v=oSj2vYw5RLs)   
	* [Introduction to MapReduce and Hadoop by Matei Zaharia](https://github.com/mahmoudparsian/big-data-mapreduce-course/blob/master/slides/mapreduce/MapReduce_by_Matei_Zaharia.pdf)  

----------------

### Session-4: Wednesday, April 12, 2023

TOPIC: Introduction to MapReduce

* REQUIRED:
	* [Chapter 3 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)   
	* [Chapter 4 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)   
	* [Chapter 5 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)   

	* [MapReduce Algorithms](https://courses.cs.ut.ee/MTAT.08.027/2018_spring/uploads/Main/L5_MapReduceAlgorithms2018.pdf)
	* Classic Join in MapReduce (inner join)
	* [Join Algorithms Using MapReduce](https://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.186.2140&rep=rep1&type=pdf)

* OPTIONAL:
	* [Simplifying Big Data Applications with Apache Spark 2.0 by Matei Zaharia](https://www.youtube.com/watch?v=Zb9YW8XjxnE)  
	* [Introduction to MapReduce and Hadoop by Matei Zaharia](https://github.com/mahmoudparsian/big-data-mapreduce-course/blob/master/slides/mapreduce/MapReduce_by_Matei_Zaharia.pdf)   
	* [Relational Operations Using MapReduce](https://medium.com/swlh/relational-operations-using-mapreduce-f49e8bd14e31)
	
----------------

### Session-5: Monday, April 17, 2023

TOPIC: Introduction to Spark 

* REQUIRED:
	* [A-Gentle-Introduction-to-Apache-Spark](pages.databricks.com/rs/094-YMS-629/images/A-Gentle-Introduction-to-Apache-Spark.pdf)
	* [Chapters 1, 2 of Data Algorithms with Spark Book by Mahmoud Parsian](https://www.amazon.com/Data-Algorithms-Spark-Recipes-Patterns/dp/1492082384/ref=sr_1_1) 
	* [PySpark Tutorial](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/pyspark_tutorial)  
	* [Classic Word Count in PySpark](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/wordcount)

* OPTIONAL:
	* [Learning Spark (book)](https://pages.databricks.com/rs/094-YMS-629/images/LearningSpark2.0.pdf)
	* [Introduction to Apache Spark](https://stanford.edu/~rezab/sparkclass/slides/itas_workshop.pdf)  

----------------

### Session-6: Wednesday, April 19, 2023

TOPIC: Introduction to Spark and PySpark (Python API for Spark)

* REQUIRED:
	* [Classic Word Count in PySpark](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/wordcount)
	* [A-Gentle-Introduction-to-Apache-Spark](pages.databricks.com/rs/094-YMS-629/images/A-Gentle-Introduction-to-Apache-Spark.pdf)
	* [Chapters 1, 2, 3, 4 of Data Algorithms with Spark Book by Mahmoud Parsian](https://www.amazon.com/Data-Algorithms-Spark-Recipes-Patterns/dp/1492082384/ref=sr_1_1) 
	* [Learning Spark (book)](https://pages.databricks.com/rs/094-YMS-629/images/LearningSpark2.0.pdf)

* OPTIONAL:
	* [Introduction to Spark](http://www.slideshare.net/jeykottalam/spark-sqlamp-camp2014)  
	* [Introduction to Spark by Shannon Quinn](http://cobweb.cs.uga.edu/~squinn/mmd_s15/lectures/lecture13_mar3.pdf)  

----------------

### Session-7: Monday, April 24, 2023

TOPIC: Spark's Nuts and Bolts

* REQUIRED:   
	* [PySpark Tutorial](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/pyspark_tutorial)  
	* [Chapters 3, 4, 5 of Data Algorithms with Spark Book by Mahmoud Parsian](https://www.amazon.com/Data-Algorithms-Spark-Recipes-Patterns/dp/1492082384/ref=sr_1_1) 
	* [Learning Spark (book)](https://pages.databricks.com/rs/094-YMS-629/images/LearningSpark2.0.pdf)

* OPTIONAL:
	* [Introduction to Spark](http://www.slideshare.net/jeykottalam/spark-sqlamp-camp2014)  
	* [Parallel-Programming-With-Spark-Matei-Zaharia](http://ampcamp.berkeley.edu/wp-content/uploads/2013/02/Parallel-Programming-With-Spark-Matei-Zaharia-Strata-2013.pptx) 

----------------

### Session-8: Wednesday, April 26, 2023

TOPIC: Data Design Patterns

* REQUIRED:     
	* [PySpark Tutorial](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/pyspark_tutorial)  
	* [MinMax Algorithm](https://github.com/mahmoudparsian/pyspark-tutorial/blob/master/tutorial/map-partitions/README.md)
	* [Top-10 Algorithm](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/Top-N) 

----------------

### Session-9: Monday, May 1, 2023

TOPIC: Data Design Patterns

* REQUIRED:  
	* [Chapters 3, 4, 5 of Data Algorithms with Spark Book by Mahmoud Parsian](https://www.amazon.com/Data-Algorithms-Spark-Recipes-Patterns/dp/1492082384/ref=sr_1_1)    
	* Data Design Patterns: InMapper Combiner, mapPartitions
	* [Top-10 Algorithm](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/Top-N) 
	* [MinMax Algorithm](https://github.com/mahmoudparsian/pyspark-tutorial/blob/master/tutorial/map-partitions/README.md)

* OPTIONAL:
	* [Chapters 4, 6, 7, 12 of PySpark Algorithms Book by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms)

----------------

### Session-10: Wednesday, May 3, 2023

TOPIC: RDD Design Patterns

* REQUIRED:     
	* Spark's RDD Partitioning
	* [Chapters 3, 4, 5 of Data Algorithms with Spark Book by Mahmoud Parsian](https://www.amazon.com/Data-Algorithms-Spark-Recipes-Patterns/dp/1492082384/ref=sr_1_1) 
	* Spark's `mapPartitions()` Transformation
	* [mapPartitions() Tutorial](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/mappartitions)
	* Review reducers: `groupByKey()`, `reduceByKey()`, and `combineByKey()`

----------------

### Session-11: Monday, May 8, 2023
	
* Review for Midterm Exam
* Problem solving & Q/A session 
	
----------------

### Session-12: Wednesday, May 10, 2023
	
* Midterm Exam
* closed book/notes/friend/internet/software

----------------

### Session-13: Monday, May 15, 2023
	
* Midterm Exam Discussion and Review

----------------

### Session-14: Wednesday, May 17, 2023
	
* Spark's DataFrames (1)
* [Chapters 4, 6, 7, 12 of PySpark Algorithms Book by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms)
* [Video: Structuring Spark: SQL, DataFrames, Datasets And Streaming - 28 mins](https://www.youtube.com/watch?v=1a4pgYzeFwE)

----------------
### Session-15: Monday, May 22, 2023
	
* Spark's DataFrames (2)
* [Chapters 4, 6, 7, 12 of PySpark Algorithms Book by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms)
* [Video: Structuring Spark: SQL, DataFrames, Datasets And Streaming - 28 mins](https://www.youtube.com/watch?v=1a4pgYzeFwE)

----------------
### Session-16: Wednesday, May 24, 2023
	
* MapReduce Design Pattern: Graph Algorithms 
* [Chapters 11 of PySpark Algorithms Book by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms)
 
----------------
### Session-17: Monday, May 29, 2023
	
* Memorial Day (Federal Holiday), NO CLASS
 
----------------

### Session-18: Wednesday, May 31, 2023
	
* Introduction to Serverless Analytics  
* SQL Access: Amazon Athena 
* SQL Access: Google BigQuery 	

----------------

### Session-19: Monday, June 5, 2023
	
* Introduction to Serverless Analytics  
* SQL Access: Amazon Athena 
* SQL Access: Google BigQuery 	
* Relational Algebra and Big Data  
* SQL Access to Big Data  


----------------

### Session-20: Wednesday, June 7, 2023
	
* Review for Final Exam 
* Q/A session 

----------------

### Session-21: Final Exam 
	
* Date: June 12-15, 2023
* closed book/notes/friend/internet/software


----------------
