# PySpark RDD Practice Questions

* For practice purposes, some scenarios reappear in a different form/shape
  elsewhere in this question set (and in
  [`mapreduce_questions.md`](mapreduce_questions.md), which has the classic
  MapReduce version of every MapReduce+PySpark question below).

* You may not post the solutions to these questions anywhere.

* Assume that the following variables are defined/created:

```code
    spark : an instance of SparkSession object
       sc : an instance of SparkContext object
```

* Created and Compiled by: Mahmoud Parsian

* Last updated: 9/4/2026 (split out of the combined
  `mapreduce_and_RDD_questions.md`)

---

## Question 1

*(Companion question — see [Question 9](mapreduce_questions.md#question-9)
in `mapreduce_questions.md` for the classic MapReduce version of this same
problem.)*

Consider the following input record format:

    <student_id><,><single-grade-in-range-of-0-to-100>

The goal is to find minimum and maximum of grades for all students. Write a
complete PySpark program (as a set of RDD transformations and actions) to
accomplish this task. Your output will be:

    <student_id> <minimum-grade> <maximum-grade>

The following rules must be implemented:

1. If a grade is over 100, then that record is dropped
2. If a grade is less than 10, then that record is dropped

---

## Question 2

*(Companion question — see [Question 10](mapreduce_questions.md#question-10)
in `mapreduce_questions.md` for the classic MapReduce version of this same
problem.)*

Consider the following input record format:

    <movie-name><,><rating-in-range-of-1-to-5>

The goal is to find the number of raters per movie. Write a complete
PySpark program (as a set of RDD transformations and actions) to accomplish
this task. Your output will be:

    <movie-name> <number-of-raters>

The following rules must be implemented:

1. If a rating is over 5, then that record is dropped
2. If a record does not have a proper format, then that record is dropped

---

## Question 3

*(Companion question — see [Question 11](mapreduce_questions.md#question-11)
in `mapreduce_questions.md` for the classic MapReduce version of this same
problem.)*

Assume the following input:

    <gene-ID><,><reference><,><gene-value>

where reference can be:

* `"r1"`: as normal
* `"r2"`: as cancer
* `"r3"`: as unknown

Write a complete PySpark program (as a set of RDD transformations and
actions) to keep only normal genes and finally count them for all genes.

---

## Question 4

*(Companion question — see [Question 12](mapreduce_questions.md#question-12)
in `mapreduce_questions.md` for the classic MapReduce version of this same
problem.)*

Let a bigram be defined as a sequence of two consecutive words. For example
for the following input: `"w1,w2,w3,w4"`, we can construct the following
three bigrams:

    w1, w2
    w2, w3
    w3, w4

Let your input be a huge text file (`x.dat`), where each record has the
following format (a record may have any number of words):

    <word1><,><word2><,><word3>...

Write a complete PySpark program (as a set of RDD transformations and
actions) to find the frequency of all unique bigrams.

---

## Question 5

*(Companion question — see [Question 16](mapreduce_questions.md#question-16)
in `mapreduce_questions.md` for the classic MapReduce version of this same
problem.)*

Given the following input, write a complete PySpark program (as a set of
RDD transformations and actions) to find the maximum and minimum of all
given keys and values for all given records. For the following input
(listed below), the output should be:

    min  10
    max  90

Input is given as a set of `(K, V)` pairs:

    90    20
    40    70
    10    40
    30    40
    40    90
    30    80
    20    30
    20    10

Is your PySpark solution efficient? Discuss in detail.

---

## Question 6

*(Companion question — see [Question 19](mapreduce_questions.md#question-19)
in `mapreduce_questions.md` for the classic MapReduce version of this same
problem.)*

Given large set of documents, we want to use PySpark RDDs to create an
"inverted index" for all documents.

For example, given the following input documents:

    Document1: fox jumped fast fox fast
    Document2: fox ran fox jumped fast
    Document3: hello hello hello fox
    ...

we want to generate the following "inverted index":

    fox    →  (Document1: 1, 4)(Document2: 1, 3)(Document3: 4)
    jumped →  (Document1: 2)(Document2: 4)
    fast   →  (Document1: 3, 5)(Document2: 5)
    ran    →  (Document2: 2)
    hello  →  (Document3: 1, 2, 3)
    ...

Write a complete PySpark program (using RDD transformations) to build the
same inverted index: generate a list of locations (word number in the
document and identifier for the document) for each word occurrence. An
identifier for each document is provided as the key, and the value is a
string of words (for example, "hello hello hello fox" will be the value for
"Document3").

---

## Question 7

*(Companion question — see [Question 33](mapreduce_questions.md#question-33)
in `mapreduce_questions.md` for the classic MapReduce version of this same
problem.)*

Given the following input (millions of (key, value) pairs), find average
rating per movie [note that ratings of less than 2 must be ignored]. The
same movie can be rated any number of times.

Input is given as: Key is a `movie_id` and Value is a rating between 1 and
5.

```text
Key  Value
m1    1
m1    3
m1    1
m1    5
...  ...
m2    5
m2    4
...  ...
```

a. Find average per movie by using `reduceByKey()`

b. Find average per movie by using `groupByKey()`

---

## Question 8

Assume that we have a MapReduce cluster with 41 nodes (one master node and
40 worker nodes and master does not store any data at all). Further assume
that the data replication factor is 4. Using this cluster, we are running a
single MapReduce program (job).

a. How many nodes can fail at a single point of time so that the whole
   single job will not fail?

b. Now we are running two MapReduce programs at the same time
   (concurrently), how many nodes can fail at a single point of time
   without any job failure?

c. If we are running a single MapReduce job and 20 worker nodes crash at
   the same time (while running a single MapReduce job), what is the
   probability (in the range of 0.0% to 100.00%) that this job will
   succeed?

Given the following rdd in pyspark:

```python
>>> # sc : as a SparkContext object

>>> data = ['k1', 'k2', 'k1', 'k2',
            'k1', 'k2', 'k3', 'k2']

>>> rdd = sc.parallelize(data)
```

write a sequence of pyspark transformations and actions to find frequencies
of all keys in data. For this example, your solution should
generate/output:

    ('k1', 3)
    ('k2', 4)

If a frequency is less than 2, then drop them.

---

## Question 9

Assume that all of the input is in a file called `"/dir/movies.txt"` and
each input record has the following format:

    <userID><,><movieID><,><rating-in-range-of-1-to-5>

Sample input:

    user1,movie1,3
    user1,movie1,1
    user1,movie2,5
    user2,movie1,4
    ...

Note that a user may rate the same movie any number of times. The goal is
to find the number of raters per movie. Write a PySpark program (as a set
of transformations and actions) to accomplish this task. Your output will
be:

    <movieID> <number-of-raters>

---

## Question 10

Consider the following in PySpark:

```python
>>> data = [1, 1, 2, 3, 1, 2, 3, 3, 3]
>>> rdd = sc.parallelize(data, 3)
>>> rdd2 = rdd.map(lambda x: (x, 2))
>>> groupedby = rdd2.groupByKey().collect()
>>> reducedby = rdd2.reduceByKey(lambda x, y: x * y).collect()
```

a. Show the content of `groupedby` in detail and show your work...

b. Show the content of `reducedby` in detail and show your work...

---

## Question 11

Consider the following in PySpark:

```python
>>> data = [ ("B", 2), ("A", 1), ("A", 4), ("B", 2), ("B", 3) ]
>>> rdd = sc.parallelize( data )
>>> rdd2 = rdd.combineByKey
... (lambda value: (value, value+2, 1),
...  lambda x, value: (x[0] + value, x[1] + value*value, x[2] + 1),
...  lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])
... )

>>> myoutput = rdd2.collect()
```

* Show the final content of `myoutput`
* Show how `myoutput` is calculated
* Show your work step by step.

---

## Question 12

Let `genes.txt` be a huge text file, where every record has the following
format (reference can be in {1, 2, 3} where 1 denotes cancer, 2 denotes
healthy, and 3 is undefined):

    <gene_id><,><reference><,><gene_value>

For example, a sample input might be:

```text
g1,1,2.3
g1,2,1.5
g1,3,2.5
g1,1,4.1
g2,1,1.3
g2,2,1.8
g2,3,3.5
g2,1,4.3
g2,1,2.9
...
```

a. Write a PySpark command/transformation to convert `genes.txt` file into
   an `RDD[String]` and then output the number of elements in that RDD

b. Let `rdd1` (as `RDD[String]`) represent the `genes.txt` file in Spark.
   Use `rdd1` and write a set of PySpark transformations to generate the
   following output per geneID:

       <gene_id> <C> <S>

   where `C` is the number of cancer genes (per `gene_id`) and `S` is the
   sum of values for the cancer gene.

c. Let `rdd1` represent the `genes.txt` file in Spark. Use `rdd1` and write
   a PySpark filter to remove all undefined genes.

---

## Question 13

Assume we have 100 billion numbers saved in a file called `big.txt` (one
number per record) and the goal is to find the number of zeros, positives,
and negatives for all of these numbers. Write an efficient PySpark program
to accomplish this task.

Your client has asked you to write an efficient program for this otherwise
he will not pay any money for your software!

---

## Question 14

Assume that all of the input is in a file called `"/tmp/movies.txt"` and
each input record has the following format:

    <userID><,><movieID><,><rating-in-range-of-1-to-5>

Sample input:

```text
user1,movie1,3
user1,movie1,1
user1,movie2,5
user2,movie1,4
...
```

Note that a user may rate the same movie any number of times.

a. The goal is to find the number of raters per movie. Write a complete
   PySpark program (as a set of transformations and actions) to accomplish
   this task. Your output will be:

       <movieID> <number-of-raters>

b. The goal is to find the number of unique movies rated by each user.
   Write a complete PySpark program (as a set of transformations and
   actions) to accomplish this task. Your output will be:

       <userID> <number-of-unique-movies>

---

## Question 15

Consider the following in PySpark:

```python
>>> # spark: an instance of SparkSession object
>>> data = [1, 1, 1, 2, 3, 1, 2, 3, 3, 3]
>>> rdd = spark.sparkContext.parallelize(data, 3)
>>> rdd2 = rdd.map(lambda x: (x, x))
>>> grouped_rdd = rdd2.groupByKey().mapValues(lambda x : sum(x)).collect()
```

* Show the content of `grouped_rdd` in detail.
* Show your work...

---

## Question 16

Consider the following in PySpark:

```python
>>> # spark: an instance of SparkSession object
>>> data = [1, 1, 1, 1, 2, 2, 3, 1, 2, 3, 3, 3]
>>> rdd = spark.sparkContext.parallelize(data)
>>> rdd2 = rdd.map(lambda x: (x+1, x))
>>> reduced_rdd = rdd2.reduceByKey(lambda x, y: x + y).collect()
```

* Show the content of `reduced_rdd` in detail.
* Show your work...

---

## Question 17

Consider the following in PySpark:

```python
>>> # spark: an instance of SparkSession object
>>> data = [1, -1, 1, 1, 0, 0, 1, -2,
            2, 3, 1, 2, -3, ...]
>>> rdd = spark.sparkContext.parallelize(data)
```

Write a series of spark transformations to split `rdd` into two RDDs:
`rddP` will hold only non-negative numbers and `rddN` will hold only
negative numbers.

---

## Question 18

Let `genes.txt` be a huge text file, where every record has the following
format (reference can be in {1, 2, 3} where 1 denotes cancer, 2 denotes
healthy, and 3 is undefined):

    <geneID><,><reference><,><geneValue>

For example, a sample input might be:

```text
g1,1,2.3
g1,2,1.5
g1,3,2.5
g1,1,4.1
g2,1,1.3
g2,2,1.8
g2,3,3.5
g2,1,4.3
g2,1,2.9
...
```

a. Write a PySpark command/transformation to convert `genes.txt` file into
   an `RDD[String]` and then output the number of elements in that RDD
   (the final result will be in `rdd1`)

b. Let `rdd1` represent the `genes.txt` file in Spark (as `RDD[String]`).
   Use `rdd1` and write a PySpark command/transformation to generate the
   following output per geneID:

       <geneID> <M> <N>

   where `M` is the number of cancer genes (for geneID) and `N` is the sum
   of values for the cancer genes.

c. Find sum of the values for all genes.

d. Let `rdd1` represent the `genes.txt` file (as `RDD[String]`) in Spark.
   Use `rdd1` and write a PySpark filter to keep only healthy genes.

---

## Question 19

Using MapReduce and PySpark, write a series of transformations and actions
to eliminate all duplicate records from a given big file called
`bigfile.txt`. Your output will be all of the unique records contained in
`bigfile.txt`.

---

## Question 20

Assume the following input:

    <Employee-ID><,><type>

where type can be:

* `"fulltime"`
* `"parttime"`
* `"contractor"`

The goal is to write a PySpark program to count "fulltime" and "parttime"
employees. Your output should be something like:

    fulltime: <number-of-fulltime-employees>
    parttime: <number-of-parttime-employees>

---

## Question 21

Given the following rdd in pyspark:

```python
>>> data = ['k1', 'k2', 'k1', 'k2',
            'k1', 'k2', 'k3', 'k2', 'k4']
>>> # spark: an instance of SparkSession object
>>> rdd = spark.sparkContext.parallelize(data)
```

write a sequence of pyspark transformations and actions to find frequencies
of all keys in data. Keep only the (key, frequency) pairs if the frequency
is greater than one.

For this example, your solution should generate/output:

    ('k1', 3)
    ('k2', 4)

---

## Question 22

Consider the following in PySpark:

```python
>>> # spark: an instance of SparkSession object
>>>
>>> data = [1, 1, 1, 1, 2, 2, 3, 1, 2, 3, 3, 3]
>>> rdd = spark.sparkContext.parallelize(data)
>>> rdd2 = rdd.map(lambda x: (x+1, x-1))
>>> my_output = rdd2.reduceByKey(lambda x, y: x + y).collect()
```

* Show the content of `my_output` in detail.
* Show your work...

---

## Question 23

Assume we have 100 billion numbers saved in a file called `big.txt` (one
number per record) and the goal is to find the number of positives
(numbers greater than zero) and negatives (numbers less than zero) for all
of these numbers. Write a Spark/PySpark program to accomplish this task.
Your client has asked you to write an efficient program for this otherwise
he will not pay any money for your software!

---

## Question 24

Assume that all of the input is in a file called "movies.txt" and each
input record has the following format:

    <userID><,><movieID><,><rating-in-range-of-1-to-5>

Sample input:

```text
user1,movie1,3
user1,movie1,1
user1,movie2,5
user2,movie1,4
...
```

Note that a user may rate the same movie any number of times. You may use
the following functions in your transformations:

    getUser("userX,movieY,ratingN")   returns "userX"
    getMovie("userX,movieY,ratingN")  returns "movieY"
    getRating("userX,movieY,ratingN") returns ratingN

MUST use the provided functions.

a. The goal is to find the number of raters per movie. Write a complete
   PySpark program (as a set of transformations and actions) to accomplish
   this task. Your output will be:

       <movieID> <number-of-raters>

b. The goal is to find the number of unique movies rated by each user.
   Write a complete PySpark program (as a set of transformations and
   actions) to accomplish this task. Your output will be:

       <userID> <number-of-unique-movies>

---

## Question 25

Consider the following in PySpark:

```python
>>> data = [1, 1, 1, 2, 3, 1, 2, 3, 3, 3]
>>> # sc : as SparkContext object
>>> rdd = sc.parallelize(data)
>>> rdd2 = rdd.map(lambda x: (x, x+2))
>>> grouped_rdd = rdd2.groupByKey().mapValues(lambda x : sum(x)).collect()
```

* Show the content of `grouped_rdd` in detail
* Show your work... step by step

---

## Question 26

Consider the following in PySpark:

```python
>>> data = [1, 1, 1, 1, 2, 2, 3, 1, 2, 3, 3, 3]
>>> # sc : as SparkContext object
>>> rdd = sc.parallelize(data)
>>> rdd2 = rdd.map(lambda x: (x+1, x))
>>> reduced_rdd = rdd2.reduceByKey(lambda x, y: x + y).collect()
```

* Show the content of `reduced_rdd` in detail.
* Show your work step-by-step

---

## Question 27

Consider the following in PySpark:

```python
>>> data = [1, -1, 1, 1, 0, 0, 1, -2, 2, 3, 1, 2, -3, ...]
>>> # sc : as SparkContext object
>>> rdd = sc.parallelize(data)
```

Write a series of spark transformations to split `rdd` into three separate
RDDs:

* `rddP` will hold only positive numbers
* `rddN` will hold only negative numbers
* `rddZ` will hold only zeros

---

## Question 28

Assume we have about 100 billion numbers saved in a file called `big.txt`
(one number per record) and the goal is to perform the following in order
(MUST USE PySpark):

a. create an RDD[Integer] as `rdd`

b. count the exact number of numbers in `rdd`

c. remove all negative numbers

d. count all remaining numbers

---

## Question 29

Consider the following in PySpark:

```python
>>> data = [0, 1, 0, 1, -1, 1, 0, 2, 3, 1, -2, -3, 3, 3]
>>> # sc : as SparkContext object
>>> rdd = sc.parallelize(data)
>>> rdd2 = rdd.filter(lambda v: v > 0)
>>> rdd3 = rdd2.map(lambda x: (x, x+2))
>>> reducedRDD = rdd3.reduceByKey(lambda x, y: x + y).collect()
```

* Show the content of `reducedRDD` in detail.
* Show your work step-by-step

---

## Question 30

Consider the following in PySpark:

```python
>>> # sc : as SparkContext object
>>> data = [("a", 1), ("a", 1), ("a", 3),
            ("b", 1), ("b", 1), ("b", 2),  ...]
>>> rdd = sc.parallelize(data)
```

* Write a series of spark transformations to find the average value per
  key.

* Write a series of spark transformations to find the maximum value per
  key.

* Write a series of spark transformations to find the median value per
  key.

* Write a series of spark transformations to find the mode value per key.

---

## Question 31

Consider the following in PySpark. Let `data` represent a set of records:

```python
>>> # sc : as SparkContext object
>>> data = ["abc", "abc", "xyz", "xyz", "xyz", ...]
>>> rdd = sc.parallelize(data)
```

Write a series of PySpark transformations to eliminate all duplicate
records. For this example, the output will be: `["abc", "xyz", ...]`. NOTE
that you can NOT use `unique()` and `distinct()` transformations.

---

## Question 32

Given the following input (file `big.txt`), using Spark's
`mapPartitions()`, write an efficient transformation to find minimum and
maximum of all given numbers. Note that every record (single line of input)
may have thousands of numbers.

Input is given as:

```text
10,4,50,40,30, ...
10,60,50,20, ...
20,20,30,40,50,2, ...
...
```

---

## Question 33

Given:

```python
>>> def myfunc(n):
...     if n < 0:
...             return [n, -n, -n]
...     else:
...             return []
...     #end-if
>>> #end-def
>>>
>>> data = [0, 1, 2, -3, -4]
>>> rdd = spark.sparkContext.parallelize(data)
>>> rdd.collect()
[0, 1, 2, -3, -4]
>>> rdd.count()
5
>>> rdd3 = rdd.flatMap(myfunc).flatMap(myfunc)
>>> rdd3.collect()
```

What is the output of this program?

---

## Question 34

Use PySpark to answer this question.

Assume that all of the input is in a file called "movies.txt" (with
millions of records) and each input record has the following format:

    <MOVIE-ID><,><rating-in-range-of-1-to-5>

Sample input:

```text
movie1,3
movie1,1
movie1,5
movie2,5
movie2,4
movie2,3
...
```

Note that a user may rate the same movie any number of times. You HAVE to
use the following Python functions in your transformations.

Note that, you MUST NOT use the Python `split()` function at all, but you
will use the following functions:

* `getMovie("movie,rating")`  returns "movie" as String
* `getRating("movie,rating")` returns rating as Integer

a. The goal is to find the number of raters per movie. Write a complete
   PySpark program (as a set of PySpark transformations and actions) to
   accomplish this task. Your output will be like:

       <MOVIE-ID> <number-of-raters>

b. The goal is to find the average rating per movie. Write a complete
   PySpark program (as a set of transformations and actions) to accomplish
   this task. Your output will be as:

       <MOVIE-ID> <average-rating-per-MOVIE-ID>

---

## Question 35

Consider the following in PySpark:

```python
>>> data = [0, 2, 2, -3, 1, -1, 3, -2, -4, 3]
>>> # spark : as a SparkSession object
>>> rdd = spark.sparkContext.parallelize(data)
>>> print("output-1: ", rdd.collect())
>>> rdd2 = rdd.filter(lambda v: v > 0)
>>> print("output-2: ", rdd2.collect())
>>> rdd3 = rdd2.map(lambda x: (x, x+2))
>>> print("output-3: ", rdd3.collect())
>>> rdd4 = rdd3.reduceByKey(lambda x, y: x + y)
>>> print("output-4: ", rdd4.collect())
```

* Show the output in detail.
* Show your work step-by-step

---

## Question 36

Consider the following in PySpark:

```python

>>> data = [("a", 1), ("a", 20), ("a", 3),
            ("b", 100), ("b", 1), ("b", 2), ...]

>>> # spark : as a SparkSession object
>>> rdd = spark.sparkContext.parallelize(data)
```

* Using `groupByKey()`: Write a series of spark transformations to find
  the (minimum, maximum) value per key.

* Using `reduceByKey()`: Write a series of spark transformations to find
  the (minimum, maximum) value per key.

* Using `combineByKey()`: Write a series of spark transformations to find
  the (minimum, maximum) value per key.

---

## Question 37

Consider the following PySpark shell program:

```python
def myfunc(n):
    if n == 0:
        return [n, -n]
    elif n > 0:
        return [n, -n, n]
    else:
        return []
    #end-if
#end-def

>>> data = [0, 3, 4, 0, -3, -4]
>>> # spark : as a SparkSession object
>>> rdd = spark.sparkContext.parallelize(data)
>>> print("output1 = ", rdd.collect())
>>> print("output2 = ", rdd.count())
>>> rdd3 = rdd.flatMap(myfunc).flatMap(myfunc)
>>> print("output3 = ", rdd3.collect())
```

---

## Question 38

Consider the following in PySpark:

```python
>>> def fun7(x):
>>>     if (x == 1):
>>>         return [x, 1]
>>>     if (x > 0):
>>>         return [x, x, -2]
>>>     return []
>>> #end-def
>>>
>>> data = [1, 1, -1, -2, 2, 2, -4]
>>>
>>> # spark : as a SparkSession object
>>> rdd = spark.sparkContext.parallelize(data, 3)
>>> rdd2 = rdd.flatMap(fun7)
>>> rdd2.collect()
>>> pairs = rdd2.map(lambda x: (x, 3))
                .groupByKey()
                .mapValues(lambda x : sum(x))
                .collect()
```

* Show the output.
* Show your work step-by-step

---

## Question 39

Consider the following (key, value) pairs in PySpark:

```python
>>> data = [('A', 4), ('A', 8),
            ('B', 5), ('B', 7), ...]
>>> # sc : as a SparkContext object
>>> rdd = sc.parallelize(data)
```

a. Using `groupByKey()`, write a set of Spark transformations to find the
   average (mean) value per key.

b. Using `reduceByKey()`, write a set of Spark transformations to find the
   average (mean) value per key.

c. Using `combineByKey()`, write a set of Spark transformations to find
   the average (mean) value per key.

---

## Question 40

Given the following rdd of pairs in PySpark:

```python
>>> data = [('k1', 5), ('k1', 6), ('k1', 7),
            ('k2', 7), ('k2', 8), ('k2', 7), ('k2', 8)]
>>> # spark : as a SparkSession object
>>> rdd = spark.sparkContext.parallelize(data)
```

1. Write an efficient sequence of pyspark transformations and actions to
   find unique list of keys: `{'k1', 'k2'}`

2. Write an efficient sequence of pyspark transformations and actions to
   find unique list of values: `{5, 6, 7, 8}`

---

## Question 41

Given the following rdd of pairs in PySpark:

```python
>>> data = [('k1', 5), ('k1', 6), ('k1', 7), ('k1', 5), ('k1', 6)
            ('k2', 7), ('k2', 8), ('k2', 7), ('k2', 8), ('k2', 9)]
>>> # spark : as a SparkSession object
>>> rdd = spark.sparkContext.parallelize(data)
```

1. Using `groupByKey()` write a sequence of pyspark transformations to
   find the (minimum, maximum) value per key.

2. Using `reduceByKey()` write a sequence of pyspark transformations to
   find the (minimum, maximum) value per key.

3. Using `combineByKey()` write a sequence of pyspark transformations to
   find the (minimum, maximum) value per key.

---

## Question 42

Given the following rdd of pairs in PySpark:

```python
>>> data = [('a', 2), ('b', 3), ('d', 2), ('x', 3), ('y', 1), ...]
>>> # spark : as a SparkSession object
>>> rdd = spark.sparkContext.parallelize(data)
```

Write a sequence of pyspark transformations to generate the following
output: MUST use `flatMap()`:

```text
[
 'a', 'a',
 'b', 'b', 'b',
 'd', 'd',
 'x', 'x', 'x',
 'y',
  ...
]
```

---

## Question 43

Consider the following RDD:

```python

>>> input = [("k1", "v1"), ("k1", "v1"), ("k1", "v2"), ...]
>>> # sc : as a SparkContext object
>>> rdd = sc.parallelize(input)
```

The goal is to write a set of Spark transformations to generate unique
(K, V) pairs [combination of K and V must be unique]. You may NOT use
Spark's `distinct()` function.

---

## Question 44

Consider the following RDD:

```python
>>> input = [("a", 2), ("a", -4), ("a", 9), ...
             ("b", 9), ("b", 7), ("b", -3), ...
             ("c", 2), ("c", 4), ...]
>>> # sc : as a SparkContext object
>>> rdd = sc.parallelize(input)
```

The goal is to write a set of Spark transformations (by using `rdd` --
represents the employees table as (key, value) pairs -- as your starting
point) to find the result for the following SQL statement:

```sql
   SELECT key, AVG(value), SUM(value)
     FROM employees WHERE value > 0
       GROUP BY key;
```

---

## Question 45

Consider the following file: `/home/data.txt`, which has 5 records:

```text
$ cat /home/data.txt
w11,w2
w1,w21,w3,w3
w1,w2,w31,w31,w3
w1,w1,w21,w2
w2,w21,w2,w1
```

and consider the following PySpark segment:

```python
# sc : as a SparkContext object
lines = sc.textFile("/home/data.txt")
rdd1 = lines.flatMap(lambda s: s.split(",")).filter(lambda x : len(x) < 3)
rdd1.count() # output 1
rdd2 = rdd1.map(lambda s : (1, s))
rdd3 = rdd2.map(lambda s: (s[1], s[0]))
rdd4 = rdd3.reduceByKey(lambda x, y: x+y).filter(lambda x : x[1] > 2)
rdd4.collect() # output 2
```

what will be the output?

---

## Question 46

Consider the following input:

```text
1
11
-1
2
12
3
-4
13
4
14
...
```

Suppose we want to count all positives, negatives, zeros, odd and even
numbers. Write an efficient PySpark program to accomplish this task.

---

## Question 47

1. Use Python, to create 1,000,000 random numbers in range of 1 to 100.

2. Then using PySpark, create an RDD for this one million numbers.

3. Then, find frequency of numbers.

4. Finally, find top-5 numbers (with highest frequencies)

---

## Question 48

*(Companion question — see
[Question 1](dataframes_questions.md#question-1) in
`dataframes_questions.md` for the PySpark DataFrame version of this same
SQL translation.)*

Consider the following SQL query:

```sql
SELECT COUNT(CustomerID) as count, Country
  FROM Customers
   GROUP BY Country;
```

If the Customers table dumped as a file (`dump.csv`) with the following
format:

    <CustomerID><,><Country>

How would you translate this SQL query by using PySpark RDDs?

---

## Question 49

*(Companion question — see
[Question 2](dataframes_questions.md#question-2) in
`dataframes_questions.md` for the PySpark DataFrame version of this same
SQL translation.)*

Consider the following SQL query:

```sql
SELECT COUNT(CustomerID) as COUNTED, Country
   FROM Customers
     GROUP BY Country
       ORDER BY COUNTED DESC
          LIMIT 5;
```

If the Customers table dumped as a file (`dump.csv`) with the following
format:

    <CustomerID><,><Country>

How would you translate this SQL query by using PySpark RDDs?

---

## Question 50

*(Companion question — see
[Question 3](dataframes_questions.md#question-3) in
`dataframes_questions.md` for the PySpark DataFrame version of this same
SQL translation.)*

Consider the following SQL query:

```sql
SELECT NAME, SUM(SALARY) FROM Employee
GROUP BY NAME
HAVING SUM(SALARY) > 3000;
```

If the Employee table dumped as file `dump.csv` with records:

    <NAME><,><SALARY>

How would you translate this SQL query by using PySpark RDDs?

---

## Question 51

*(Companion question — see
[Question 4](dataframes_questions.md#question-4) in
`dataframes_questions.md` for the PySpark DataFrame version of this same
SQL translation.)*

Consider the following SQL query:

```sql
SELECT COUNT(CustomerID) as COUNTED, Continent, Country
FROM Customers
GROUP BY Continent, Country
ORDER BY COUNTED DESC
LIMIT 5;
```

If the Customers table dumped as a file `dump.csv` with the following
records:

    <CustomerID><,><continent><,><Country>

How would you translate this SQL query by using PySpark RDDs?

---

## Question 52

Given two RDDs of pairs:

```python
>>> # sc : as a SparkContext object
>>> employees = sc.parallelize([(1, "alex"), (2, "jane"), (3, "ted"), (4, "dan")])
>>> departments = sc.parallelize([(1, "IT"), (2, "HR"), (3, "IT")])
```

where `employees` is `(emp_id, name)` and `departments` is `(emp_id,
dept)`.

a. Using `join()`, write a PySpark transformation to produce `(emp_id,
   (name, dept))` for employees that have a matching department. Show the
   output.

b. Using `leftOuterJoin()`, write a PySpark transformation so that
   employees with no department (like `dan`) still appear in the output,
   with `None` for `dept`. Show the output.

c. Without using `join()` or any `*OuterJoin()` function, write an
   equivalent solution using only `groupByKey()`/`cogroup()` and
   `flatMap()`/`map()`. Show your work step-by-step.

---

## Question 53

Given two RDDs:

```python
>>> # sc : as a SparkContext object
>>> rddA = sc.parallelize([1, 2, 3, 4, 5])
>>> rddB = sc.parallelize([4, 5, 6, 7, 8])
```

a. Write a PySpark transformation using `union()` to combine `rddA` and
   `rddB` (including duplicates), then show the output.

b. Write a PySpark transformation using `intersection()` to find elements
   present in both `rddA` and `rddB`. Show the output.

c. Write a PySpark transformation using `subtract()` to find elements
   present in `rddA` but NOT in `rddB`. Show the output.

d. `intersection()` and `subtract()` both trigger a shuffle internally.
   Explain, in at most 3 sentences, why that is unavoidable given how
   these operations are defined.

---

## Question 54

Given the following RDD of `(product_id, total_sales)` pairs:

```python
>>> # sc : as a SparkContext object
>>> data = [("p1", 4200), ("p2", 900), ("p3", 15000),
            ("p4", 3100), ("p5", 15000), ("p6", 50)]
>>> rdd = sc.parallelize(data)
```

a. Using `sortBy()` (or `sortByKey()` after an appropriate `map()`), write
   a PySpark transformation to sort products by `total_sales` descending.
   Show the output.

b. Using `takeOrdered()` (NOT `sortBy()` followed by `take()`), write a
   PySpark action to efficiently get the top-3 products by `total_sales`.
   Show the output.

c. `rdd.sortBy(...).take(3)` and `rdd.takeOrdered(3, key=...)` can produce
   the same result, but one is generally more efficient for large RDDs
   when you only need a small top-N. Explain why, in at most 3 sentences.

---

## Question 55

Assume `rddBig` is an RDD of 500 million `(user_id, event)` records, and
`lookup` is a small Python dictionary (10,000 entries) mapping
`country_code -> country_name`, needed to enrich `rddBig` by looking up
each event's `country_code`.

a. Write PySpark code that uses `sc.broadcast()` to distribute `lookup`
   efficiently to all executors, and a `map()` transformation that uses
   the broadcast variable to attach the full `country_name` to each
   record.

b. Explain what would go wrong (performance-wise) if you instead captured
   `lookup` directly inside the `map()` lambda's closure without using
   `sc.broadcast()`.

---

## Question 56

Assume `rdd` holds 1 billion raw text records read from a log file, some
of which are malformed (do not match the expected
`<timestamp>,<user_id>,<action>` format) and must be skipped during
parsing.

a. Write PySpark code that uses an `sc.accumulator(0)` to count the
   number of malformed records skipped, while still producing a clean RDD
   of parsed `(timestamp, user_id, action)` tuples from the valid records.

b. After the job runs, how do you retrieve the final count from the
   accumulator? Why is it unsafe, in general, to rely on an accumulator's
   value being read *inside* a transformation (as opposed to after an
   action has completed)?
