# Syllabus

# Big Data Modeling

## Description

Big data analytics is among today’s fastest-growing and 
highest-paid jobs as organizations increasingly rely on 
data to drive strategic business decisions. This hands-on 
introduction to big data modeling provides a unique approach 
to help you act on data for real business gain. The focus 
will be on data-intensive text processing with MapReduce, 
Spark (a unified analytics engine for large-scale data 
processing), functional, and distributed programming. 
This course is about distributed algorithm design (using
MapReduce and Spark), particularly for text processing 
and related applications.

You will lean how to analyze massive amount of data with 
distributed computations using Spark's high-level data 
transformations. This class will teach scalable approaches 
to processing large amounts of text with MapReduce, Spark,
and Amazon Athena (an interactive query service that makes 
it easy to analyze data in Amazon S3 using standard SQL).

This class will introduce the concept of 

* Big Data analytics with MapReduce and Spark
* Graph algorithms
* Basic Machine Learning algorithms
	* Logistic Regression
	* Linear Regression
* Big Data Streaming
* Big data databases:
	* Google's BigTable
	* HBase (Hadoop Database)
	* ElasticSearch (a distributed, multitenant-capable full-text search engine).



## Learning objectives

Upon successful completion of the course, students will be able to:

* Understand elements of big data
	* Volume, Variety, Veracity, Velocity, Value
	* Scale-out vs. Scale-up
	* Distributed and cluster Computing
	
* Analyze big data by using MapReduce paradigm
  * Understand mappers, reducers, and combiners
  
* Analyze and transform data into a desired form
  by using Spark's transformations and actions:
  * Understand Spark's transformations such
  as mappers, reducers, filters, groupBykey, 
  reduceByKey, combineByKey, aggregateByKey, ...
  * Perform ETL and big data analysis
  
* Understand partitioning big data for faster queries
* Understand Spark's data abstractions: RDDs and DataFrames
* Understand basics of data streaming and processing
* Understand graph algorithms on big data such as Motif Finding
* How to use Amazon's Athena (an interactive query 
  service that makes it easy to analyze data in Amazon 
  S3 using standard SQL)
* Use basic Machine Learning algorithms on big data:
	* Logistic regression
	* classification algorithms


## Literature and Learning Material

### Required material:
* [1. PySpark in Action book by Mahmoud Parsian, to be published by Manning by end of 2019](https://www.manning.com)
* [2. Data-Intensive Text Processing with MapReduce by Jimmy Lin and Chris Dyer](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)
* [3. Apache Spark](http://spark.apache.org/)
* [4. Amazon Athena](https://aws.amazon.com/athena/)
* [5. GraphFrames](http://graphframes.github.io/graphframes/docs/_site/index.html)

### Additional material:
* [1. A Very Brief Introduction to MapReduce by Diana MacLean](http://hci.stanford.edu/courses/cs448g/a2/files/map_reduce_tutorial.pdf)
* [2. Introduction to MapReduce by Mahmoud Parsian](http://mapreduce4hackers.com/docs/Introduction-to-MapReduce.pdf)
* [3. Mining of Massive Datasets by Jure Leskovec, Anand Rajaraman, Jeffrey D. Ullman](http://infolab.stanford.edu/~ullman/mmds/book.pdf)


## Technology
* Python will be used to wrangle data. 
* MapReduce paradigm: map(), reduce(), combine()
* Apache Spark: 
	* Data Abstractions: RDDs, DataFrames
	* Transformations and Actions
	* Data Partitioning
	* Data Streaming
	* GraphFrames
* Amazon Athena
	* Serverless computing
	* Catalogs and Tables
	* Interactive SQL query using S3
* Big data databases:
	* Google's BigTable
	* HBase: Hadoop Database
	* ElasticSearch
	

# Course Structure

### Session-1:
* Introduction to big data 
* Elements of big data
* Introduction to data analytics
* Distributed and cluster computing

Required Reading:

1. [THE 4 V’S OF BIG DATA](https://www.dummies.com/careers/find-a-job/the-4-vs-of-big-data/)

2. [Chapter 1 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)

3. [A Very Brief Introduction to MapReduce](http://hci.stanford.edu/courses/cs448g/a2/files/map_reduce_tutorial.pdf)
 
4. [Introduction to MapReduce](http://lsd.ls.fi.upm.es/lsd/nuevas-tendencias-en-sistemas-distribuidos/IntroToMapReduce_2.pdf)


### Session-2
* Introduction to Big Data 
* Introduction to MapReduce paradigm

Required Reading:

1. [Introduction to MapReduce by Mahmoud Parsian](http://mapreduce4hackers.com/docs/Introduction-to-MapReduce.pdf)

2. [Chapter 2 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)

3. [Introduction to MapReduce and Hadoop by Matei Zaharia](https://github.com/mahmoudparsian/big-data-mapreduce-course/blob/master/slides/mapreduce/MapReduce_by_Matei_Zaharia.pdf)

    
### Session-3: 
* Elements of MapReduce Paradigm
* Data Partitioning
* Mappers as map()
* Sort and Shuffle
* Reducers as reduce() and combine()
* Mining of massive datasets

Required Reading:

1. [Introduction to MapReduce](https://www.youtube.com/watch?v=ht3dNvdNDzI)

2. [The Future of Big Data by Matei Zaharia](https://www.youtube.com/watch?v=oSj2vYw5RLs)

3. [Chapter 3 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)

4. [Introduction to MapReduce and Hadoop by Matei Zaharia](https://github.com/mahmoudparsian/big-data-mapreduce-course/blob/master/slides/mapreduce/MapReduce_by_Matei_Zaharia.pdf)

5. [Chapters 1 & 2 of Mining of Massive Datasets book](http://infolab.stanford.edu/~ullman/mmds/book.pdf)
 
 
### Session-4: 
* Introduction to Apache Spark
* Spark data abstractions: 
	* RDDs
	* DataFrames
* Spark's actions
* Data Partitioning


Required Reading:

1. [What is Spark? by Matei Zaharia](https://www.youtube.com/watch?v=2lypTlbjqHE)

2. [Simplifying Big Data Applications with Apache Spark by Matei Zaharia](https://www.youtube.com/watch?v=Zb9YW8XjxnE)

3. [Chapter 3 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)



### Session-5: 
* Spark RDDs 
* Data Partitioning 
* Transformations: mappers, filters, reducers, ...
* Creating RDDs from many different data sources
	* Text files
	* Relational databases
	* Collections
	* Amazon S3

Required Reading: 

1. [Manipulating Data by Spark](https://pages.databricks.com/201808-EB-Mini-eBook-4-Manipulating-Data_landing.html)

2. [Introduction to Spark](https://stanford.edu/~rezab/sparkclass/slides/itas_workshop.pdf)

3. [Spark API and Documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html)


### Session-6: 
* Spark RDDs 
* Data Partitioning 
* Transformations: reducers, aggrgators, combiners, ...
* Lazy Evaluation

Required Reading: 

1. [Manipulating Data by Spark](https://pages.databricks.com/201808-EB-Mini-eBook-4-Manipulating-Data_landing.html)

2. [Introduction to Spark](https://stanford.edu/~rezab/sparkclass/slides/itas_workshop.pdf)

3. [Chapter 4 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)  

4. [Introduction to MapReduce and Hadoop by Matei Zaharia](https://github.com/mahmoudparsian/big-data-mapreduce-course/blob/master/slides/mapreduce/MapReduce_by_Matei_Zaharia.pdf)

6. [Spark API and Documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html)
	
	 
### Session-7: 
* Spark DataFrames 
* Data Partitioning 
* SQL Transformations
* Relational Algebra and Big Data

Required Reading: 

1. [Introduction to DataFrames - Python](https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-python.html)

2. [Manipulating Data by Spark](https://pages.databricks.com/201808-EB-Mini-eBook-4-Manipulating-Data_landing.html)

3. [Introduction to Spark](https://stanford.edu/~rezab/sparkclass/slides/itas_workshop.pdf)

4. [Introduction to Spark](http://www.slideshare.net/jeykottalam/spark-sqlamp-camp2014)

5. [Introduction to Spark](http://cobweb.cs.uga.edu/~squinn/mmd_s15/lectures/lecture13_mar3.pdf)

6. [Spark API and Documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html)

	
### Session-8: 
* Spark Nuts and Bolts
* Spark DataFrames 
* Data Partitioning 
* SQL Transformations

Required Reading: 

1. [Making Big Data Simple: by Matei Zaharia](https://www.youtube.com/watch?v=Nev1s6fHwMI) 

2. [Introduction to Spark](http://www.slideshare.net/jeykottalam/spark-sqlamp-camp2014)

3. [Parallel-Programming-With-Spark-Matei-Zaharia](http://ampcamp.berkeley.edu/wp-content/uploads/2013/02/Parallel-Programming-With-Spark-Matei-Zaharia-Strata-2013.pptx)  

4. [Spark API and Documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html)

	   
### Session-9: Review for Midterm Exam
* Elements of big data
* Distributed computing and analysis
* MapReduce Paradigm
* Spark's RDDs and DataFrames


### Session-10: Midterm Exam


### Session-11: 
* MapReduce Design Pattern: mapPartitions()
* Data Design Patterns
* Data partitioning
* InMapper Combiner
* MinMax                     
* Top-10 

Required Reading:                     

1. [Chapter 3 & 4 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)

2. [MapReduce Design Patterns by Donald Miner](https://www.slideshare.net/DonaldMiner/mapreduce-design-patterns)


### Session-12: 
* Amazon Athena is an interactive query service
* Serverless computing with SQL
* Data catalogs and tables
* Partitioning data and storing in Parquet
* Smart joins with Smart partitioning data
* JDBC Access to Athena


Required Reading:

1. [Amazon Athena](https://aws.amazon.com/athena/)

2. [Performance with Athena](https://aws.amazon.com/blogs/big-data/top-10-performance-tuning-tips-for-amazon-athena/)


### Session-13: 
* Introduction to Machine Learning with Big Data
* Logistic Regression
* How to Create LR Model using Numeric data
* How to Create LR Model using Non-Numeric data
* How to Predict using LR Model


Required Reading:   
                
1. [Introduction to Logistic Regression](http://www-hsc.usc.edu/~eckel/biostat2/slides/lecture13.pdf)

2. [Spark API and Documentation](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html?highlight=logistic#pyspark.ml.classification.LogisticRegression)


### Session-14: 
* Introduction to Machine Learning with Big Data
* Linear Regression
* How to Create LR Model
* How to Predict using LR Model


Required Reading:                     

1. [Introduction to Linear Regression](https://databricks.com/spark/getting-started-with-apache-spark/machine-learning)

2. [Spark API and Documentation](https://spark.apache.org/docs/latest/ml-classification-regression.html#linear-regression)


### Session-15: 
* Graph Algorithms with Big Data
* How to build a graph (on social data, on patient data)
* Analysis of Graphs
	* Motif finding
	* Triangle finding

Required Reading:                     

1. [Introduction to graphs](http://pages.cs.wisc.edu/~paton/readings/Old/fall08/GRAPH.html)

2. [GraphFrames documentation and API](http://graphframes.github.io/graphframes/docs/_site/index.html)

	   
### Session-16: 
* Graph Algorithms with Big Data
* Graph algorithms:
	* PageRank algorithm
	* Connected components algorithm
   * Shortest path
    
Required Reading:                     

1. [Introduction to graphs](http://pages.cs.wisc.edu/~paton/readings/Old/fall08/GRAPH.html)

2. [GraphFrames documentation and API](http://graphframes.github.io/graphframes/docs/_site/index.html)

	   
### Session-17: 
* Introduction to data stream processing
* Data Streaming with Big Data
* Streaming data with Spark
    
Required Reading:                     

1. [A Gentle Introduction to Stream Processing](https://medium.com/stream-processing/what-is-stream-processing-1eadfca11b97)

2. [What is Streaming Data?](https://aws.amazon.com/streaming-data/)

3. [Streaming data with Spark](https://spark.apache.org/streaming/)
	   

### Session-18: 
* Big Data Databases: billions of rows, millions of columns
	* Google's BigTable
	* HBase: Hadoop database
	* ElasticSearch
    
Required Reading:                     

1. [Bigtable: A Distributed Storage System for Structured Data](https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf)

2. [What is HBase?](https://hbase.apache.org)

3. [HBase data model](https://www.netwoven.com/2013/10/10/hbase-overview-of-architecture-and-data-model/)



### Session-19: Review for Final Exam
* Elements of big data
* Distributed computing and analysis
* MapReduce Paradigm
* Spark's RDDs and DataFrames
* Big data databases
* Graph algorithms
* Streaming data
* Machine Learning algorithms


### Session-20: Final Exam


# Assignments
Match the above-stated learning objectives with the assignments used to grade students.

### Assignment #1: Learning objectives
* Understand MapReduce paradigm
* Use map() and reduce() functions to solve "word count" problem
* Understand Sort and Shuffle phase of MapReduce Paradigm
	
### Assignment #2: Learning objectives
* Understand ETL by using Spark: extended word count and bigrams
* Understand loading, reading, and saving data
* Use Spark's transformations: map(), filter(), reduceByKey(), ...
* Understand Lazy Evaluation

### Assignment #3: Learning objectives
* Understand Spark's DataFrames
* Use SQL on NoSQL data
* Perform data analytics (such as ETL, mapping, aggregation) 

### Assignment #4: Learning objectives
* How to build a social graph
* Use Motif Finding to solve a data problem
* Understand graph algorithms

### Assignment #5: Learning objectives
* Implement Logistic Regression for Spam/NoSpam Detection
* Build an LR Model with training data
* Use the built model for Prediction
* Measure accuracy of the built model


# Points:
````
Assignment #1: 12%
Assignment #2: 12%
Assignment #3: 12%
Assignment #4: 12%
Assignment #5: 12%
Midterm Exam:  20%
Final Exam:    20%
Bonus:          3%
````

#Grading Scale

````
Points Letter
100-94 A
<94-90 A-
<90-87 B+
<87-84 B
<84-80 B-
<80-77 C+
<77-74 C
<74-70 C-
<70    F
````

# Course Policies

## General
* Quizzes and exams are closed book, closed notes
* No makeup quizzes or exams will be given
* Grades in the C range represent performance that meets expectations
* Grades in the B range represent performance that is substantially better than the expectations
* Grades in the A range represent work that is excellent
* Grades will be maintained in the LMS course shell. 
* Students are responsible for tracking their progress by referring to the online gradebook.

## Labs and Assignments
* Students are expected to work independently. 
* Offering and accepting solutions from others is an act of plagiarism, 
which is a serious offense and all involved parties will be penalized 
according to the Academic Honesty Policy
* Discussion amongst students is encouraged, but when in doubt, 
direct your questions to the professor.
* No late assignments will be accepted under any circumstances

## Attendance and Absences
* Attendance is expected.
* Students are responsible for all missed work, regardless of the reason 
for absence.
* It is also the absentee's responsibility to get all missing notes 
or materials. 

## Instructor's Intended Purpose
* The student's work must match the instructor's intended purpose 
for an assignment. While the instructor will establish the intent 
of an assignment, each student must clarify outstanding questions 
of that intent for a given assignment.

* The student may not give or get any unauthorized or excessive 
assistance in the preparation of any work.

* The student must clearly establish authorship of a work. 
Referenced work must be clearly documented, cited, and attributed, 
regardless of media or distribution.


