# MapReduce & PySpark  </br> Practice Questions

* For practice purposes, some of the questions 
  are repeated in a different form and shape.

* You may not post the solutions to these questions anywhere

* Created and Compiled by: Mahmoud Parsian

* Last updated: 9/25/2023

* For PySpark, assume that the following 
variables are defined/created:

~~~code
    spark : an instance of SparkSession object
       sc : an instance of SparkContext object
~~~

## Question 1

<font size="4">

Assume that we have about 100,000 `gene_id(s)`.

Assume we have billions of records and consider 
the following input record format:
 
	<gene_id><,><gene_value_as_float>
	
Sample records:

	g1,1.0
	g1,2.4
	g2,7.0
	g2,-1.5
	g2,3.0
	g3,2.3
	g1,4.0
	...

The goal is to find average and median of gene value(s)
for each `gene_id`.

The goal is to write a MapReduce solution (mapper and 
reducer) to solve this problem.

The following rules must be implemented:

1. If a gene value is less than 0, then that record is dropped

2. If a record does not have a proper format, then that record is dropped

3. If average of a `gene_id` is less than 1.5, then no output 
is created at all for that `gene_id`

4. Can we write combiners? How? Show your work, and justify your answer.

5. Discuss the number of mappers and reducers

</font>

-------

## Question 2

<font size="4">
Given billions of numbers (assume each 
record has a single number), find count, minimum, 
maximum, andd average for all numbers.  

Discuss the number of mappers and reducers

map(key, value) {
  emit("min", value)
  emit("max", value)
  emit("total", (value, 1))

}

</font>

-------

## Question 3

<font size="4">
Given billions of numbers (assume each 
record has a single number), find count of
zeros, positives, and negatives.

Discuss the number of mappers and reducers

</font>

-------

## Question 4

<font size="4">
Given the following input, 

`(key-as-string, value-as-integer)`

write a `map()` and `reduce()` functions 
to find average of all given input per key

Discuss the number of mappers and reducers

</font>

--------

## Question 5

<font size="4">
Given billions of records (assume each record 
is a string any value), write a MapReduce program 
to make sure that every record has only one duplicate.

</font>

---------

## Question 6

<font size="4">
Given billions of input records (each record is an 
String object), write a MapReduce program to remove 
all duplicate records. The result will be all distinct 
records.

</font>

## Question 7

<font size="4">
Explain how the MapReduce framework can 
be used to join two tables `R(B, A)` and `S(B, C)`. 
Here `R(B, A)` is a table with two attributes 
A and B. Similarly `S(B, C)` is a table with 
two attributes B and C.  The tables R and S 
are joined on the attribute B.

Input for R is expressed as:

~~~text
R,b7,a1
R,b2,a2
R,b3,a3
...
~~~

Input for S is expressed as:

~~~text
S,b1,c1
S,b1,c2
S,b3,c3
S,b7,c1
S,b7,c2
...
~~~


1. Write `map()` and `reduce()` functions to 
   perform **inner join** and show your work in detail

2. Write `map()` and `reduce()` functions to 
   perform **left join** and show your work in detail
   
3. Write `map()` and `reduce()` functions to 
   perform **right join** and show your work in detail


</font>

--------

## Question 8

<font size="4">

Given the following input, write a `map()`
and `reduce()` functions to find maximum of all 
given values for associated keys. For the 
following input (listed below),  

Input is given as:

	Key  Value
	k1    10
	k1     9
	k1     4
	k2    40
	k3    10
	k3    30
	k3    20
	...
	
the output (output of all reducers) will be:

	k1  10
	k2  40
	k3  30
	...


</font>

## Question 9

<font size="4">

Consider the following input record format:
 
	<student_id><,><single-grade-in-range-of-0-to-100>

The goal is to find minimum and maximum of grades 
for all students. Write a MapReduce and PySpark 
programs to accomplish this task. Your output will be 

	<student_id> <minimum-grade> <maximum-grade> 


The following rules must be implemented.

1. If a grade is over 100, then that record is dropped

2. If a grade is less than 10, then that record is dropped

</font>


## Question 10

<font size="4">

Consider the following input record format:

	<movie-name><,><rating-in-range-of-1-to-5>

The goal is to find the number of raters per movie.
Write a MapReduce and PySpark programs to accomplish 
this task.  Your output will be 

	<movie-name> <number-of-raters>

The following rules must be implemented.

1. If a rating is over 5, then that record is dropped

2. If a record does not have a proper format, then 
   that record is dropped

</font>


## Question 11

<font size="4">

Assume the following input

	<gene-ID><,><reference><,><gene-value>

where reference can be:

* `"r1"`: as normal
* `"r2"`: as cancer
* `"r3"`: as unknown

The goal is to write a MapReduce and PySpark programs 
to keep only normal genes and finally count them for 
all genes.

</font>

-------

## Question 12

<font size="4">
Let a bigram be defined as a sequence 
of two consecutive words. For example for 
the following input: `"w1,w2,w3,w4"`,
we can construct the following three bigrams:

	w1, w2
	w2, w3
	w3, w4

Let your input be a huge text file (x.dat), where 
each record has the following format (a record 
may have any number of words):

	<word1><,><word2><,><word3>...

Write a MapReduce abd PySpark program to find frequency 
of all unique bigrams.

</font>

-------

## Question 13

<font size="4">

In classic MapReduce, let `map()` and 
`reduce()` functions, and input defined as 
[note that function `even(x)` returns `True` 
if `x` is an even number, otherwise it 
returns `False`]:

Mapper:

~~~code
map(String K, Integer V) {
  if (even(V)) {
  	emit("k2", 2);
  }  
  emit(K, V+1);
}
~~~

Reducer:

~~~code
reduce(String K, Iterable<Integer> V) {
   integer sum = 0;
   for (integer n : V) {
      sum = sum + n;
   }
   emit (K, sum);
} 
~~~

Input to mappers as (Key, Value) pairs:

~~~data
k1	3
k2	2
k3	1
k1	1
k2	2
k2	4
k3  7
k3  5
~~~

a. Show all of the output emitted by all mappers

b. Show all of the input to all reducers

c. Show all of the output generated by all reducers

</font>

-------

## Question 14

<font size="4">

Assume our big data test cluster has only 4 
worker nodes/servers labeled as 

                {S0, S1, S2, S3, S4} 

The server `S0` is the master node and does not 
store any data at all.  Let the replication 
factor to be `2` and let's have the following big 
files: file1 and file2 as

    file1 = {f1, f2, f3} (3 blocks in Hadoop)
    file2 = {f4, f5} (2 blocks in Hadoop)

How Hadoop distributed file system will place these 
two files in our defined cluster. 

a. You need to show how these two files will be 
   placed at the cluster nodes. Show your answer 
   per node.

b. Which server or servers maintain the metadata 
   information about  all these files?
    
</font>

-------

## Question 15

<font size="4">


Classic MapReduce and PySpark  

Palindrome is a word, which reads the same 
backward or forward.  Let `PAL(x)` be a defined 
function (you are not required to implement this 
function, just use it), which return true if `x` 
is a Palindrome and false otherwise. The goal is 
to read a set of a documents (as a set of text 
files) and find frequencies of all palindromes. 
Write `map()` and `reduce()` functions to find 
frequencies of all palindromes. The input to 
`map()` will be a pair of `(K, V)`, where `K` 
a document ID and `V` is a single sentence as a 
String (comprised of many words).

</font>

-------

## Question 16

<font size="4">
Classic MapReduce and PySpark  

Provide solution in MapReduce and PySpark.

Given the following input, write a classic 
`map()` and `reduce()` functions to find the maximum 
and minimum of all given keys and values for all 
given records.  For the following input (listed 
below), the output (output of all reducers) will 
be:

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

a. Write a `map()` function: must identify Key and Value for the map()

b. Show output of all mappers

c. Write a reduce() function: must identify Key and Value for the reduce()

d. Show all input to all reducers

e. Is your MapReduce solution efficient? Discuss in detail.

f. Is your PySpark solution efficient? Discuss in detail.


</font>

-------

## Question 17

<font size="4">

Given the following rdd in pyspark:

~~~python
>>> # sc : as a SparkContext object

>>> data = ['k1', 'k2', 'k1', 'k2', 
            'k1', 'k2', 'k3', 'k2']
            
>>> rdd = sc.parallelize(data)
~~~

write a sequence of pyspark transformations and actions to 
find frequencies  of all keys in data. For this example, your 
solution should generate/output:

	('k1', 3)
	('k2', 4)

If a frequency is less than 2, then drop them.


</font>

-------

## Question 18

<font size="4">

Assume that we have a MapReduce cluster 
with 41 nodes (one master node and 40 worker 
nodes and master does not store any data at 
all). Further assume that the data replication 
factor is 4.  Using this cluster, we are running 
a single MapReduce program (job), 

a. how many nodes can fail at a single 
   point of time so that the whole single 
   job will not fail?

b . Now we are running two MapReduce 
    programs at the same time (concurrently), 
    how many nodes can fail at a single point 
    of time without any job failure?

c. If we are running a single MapReduce job
   and 20 worker nodes crash at the same time 
   (while running a single MapReduce job), 
   what is the probability (in the range of 
   0.0% to 100.00%) that this job will succeed?

</font>

-------

## Question 19

<font size="4">
Let String and Integer be basic data types. 
In classic MapReduce, let `map()` and 
`reduce()` functions defined as follows:

Mapper:

~~~code
map(Integer key, Integer value) {
  emit("key", value);
  emit("key", key);
  
  if (key > value) {
     emit("key1", 1);
  }
  else {
     emit("key2", 2);
  } 
}
~~~



Reduer:

~~~code
reduce(String key, Iterable<Integer> values) {
   Integer sum = 0;
   for (Integer n : values) {
      if (n > 1) {
         sum = sum + n;
      }
   }
   emit (key, sum);
} 
~~~

Let the input be the following (key, value) to mappers: 

~~~data
key	value
1	   2
5     3
3     2
1     1
4     1
~~~


a. Show all of the output emitted by all mappers:
For each input, show output.

b. Show output of MapReduce's sort and shuffle phase:

c. At most, how many reducers are needed and what are
the reducer's keys and values?

d. Show all of the output generated by all reducers


</font>

-------

## Question 20

<font size="4">

PySpark and MapReduce:

Given large set of documents, we want to use 
classic MapReduce and PySpark to create an 
"inverted index" for all documents. 

For example, given the following input documents:


	Document1: fox jumped fast fox fast
	Document2: fox ran fox jumped fast
	Document3: hello hello hello fox
	...


we want to generate the following "inverted index"


	fox →  (Document1: 1, 4)(Document2: 1, 3)(Document3: 4)
	jumped  → (Document1: 2)(Document2: 4)
	fast → (Document1: 3, 5)(Document2: 5)
	ran  → (Document2: 2)
	hello  → (Document3: 1, 2, 3)
	...


The goal is to develop a classic MapReduce and Pyspark
programs for inverted index creation: generate a list of 
locations (word number in the document and identifier 
for the document) for each word occurrence. An 
identifier for each document is provided as the key 
to the map() function and value is a string of words 
(for example, "hello hello hello fox" will be the value 
for the "Document3").

a. Write a `map()` function 
you must identify Key and Value for the `map()`

b. Write a `reduce()` function (NOT a PySpark function): 
you must identify Key and Value for the `reduce()`

</font>

-------

## Question 21

<font size="4">

Assume that all of the input is in a file 
called  `"/dir/movies.txt"` and each input record 
has the following format:

	<userID><,><movieID><,><rating-in-range-of-1-to-5>

Sample input:

	user1,movie1,3
	user1,movie1,1
	user1,movie2,5
	user2,movie1,4
	...


Note that a user may rate the same movie any 
number of times. The goal is to find the number 
of raters per movie.  Write a PySpark program 
(as a set of trasformations and actions) to 
accomplish this task. Your output will be

	<movieID> <number-of-raters>

</font>

-------

## Question 22

<font size="4">

Consider the following in PySpark:

~~~python
>>> data = [1, 1, 2, 3, 1, 2, 3, 3, 3]
>>> rdd = sc.parallelize(data, 3)
>>> rdd2 = rdd.map(lambda x: (x, 2))
>>> groupedby = rdd2.groupByKey().collect()
>>> reducedby = rdd2.reduceByKey(lambda x, y: x * y).collect()
~~~

a. Show the content of `groupedby` in detail
and show your work...

b. Show the content of `reducedby` in detail
and show your work...

</font>

-------

## Question 23

<font size="4">

Consider the following in PySpark:

~~~python
>>> data = [ ("B", 2), ("A", 1), ("A", 4), ("B", 2), ("B", 3) ]
>>> rdd = sc.parallelize( data )
>>> rdd2 = rdd.combineByKey
... (lambda value: (value, value+2, 1),
...  lambda x, value: (x[0] + value, x[1] + value*value, x[2] + 1),
...  lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])
... )

>>> myoutput = rdd2.collect()
~~~

* Show the final content of `myoutput`
* show how `myoutput` is calculated
*  show your work step by step.

</font>

-------

## Question 24

<font size="4">

Let `genes.txt` be a huge text file, where every 
record has the following format (reference can be 
in {1, 2, 3} where 1 denotes cancer, 2 denotes 
healthy, and 3 is undefined):

	<gene_id><,><reference><,><gene_value>

For example, a sample input might be:

~~~text
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
~~~


a. Write a PySpark command/tranformation to
convert genes.txt file into an `RDD[String]`
and then output the number of elements in 
that RDD

b. Let `rdd1` (as `RDD[String]`) represents the `genes.txt` 
file in Spark.  Use `rdd1` and write a set of PySpark 
tranformation to generate the following output 
per geneID

	<gene_id> <C> <S>

where `C` is the number of cancer genes (per `gene_id`) 
and `S` is the sum of values for the cancer gene.

c. Let `rdd1` represents the `genes.txt` file in Spark.
Use `rdd1` and write a PySpark filter to remove all 
undefined genes.

</font>

-------

## Question 25

<font size="4">

In classic MapReduce, let a `map()` 
function be defined as:

~~~code
map(integer key, integer value) {
  if (key == value) {
     emit(key, value);
  }
  else {
     emit(key, 1);
     emit(value, key);
  }
}
~~~


and consider the following (Key, Value) input to mappers: 

~~~code
Key   Value
1     1
2     2
3     1
4     4
2     4
5     5
~~~


a. Show all of the output emitted by all mappers:
show your work step-by-step and show what is generated 
per mapper input.

b. Show all of the input to all reducers:


</font>

-------

## Question 26

<font size="4">


For a classic MapReduce program, consider the 
following `(key, value)` pairs generated by 
all mappers:

~~~text
(a, -2), (b, 3), (c, 2), (a, 4), 
(b, 5), (c, 7), (a, -3), (c, -5)
(a, 2), (b, 3), (c, 4), (z, 1), 
(z, 2), (z, 3), (b, -2), (z, -4)
(z, 1), (z, 3), (a, 5), (z, 0), 
(z, 0), (z, 0), (z, 0), (z, 0)
~~~

a. Show the output of Sort and Shuffle phase for 
these input generated by all mappers (defined above):

b. Write a generic `reduce()` function and identify 
data type of key and value for a reducer, which will 
count the number of zeros, positives, and negatives 
for each key.

c. Show all of the output generated by all reducers

d. What is ideal maximum number of reducers for the 
data (defined above)


</font>

-------

## Question 27

<font size="4">


Assume we have 100 billion numbers saved in a 
file called `big.txt` (one number per record) and 
the goal is to find the number of zeros, positives, 
and negatives for all of these numbers. Write an
efficient PySpark program to accomplish this task. 

Your client has asked you to write an efficient 
program for this otherwise he will not pay any 
money for your software!


</font>

-------

## Question 28

<font size="4">


Assume that all of the input is in a file 
called  `"/tmp/movies.txt"` and each input 
record has the following format:

	<userID><,><movieID><,><rating-in-range-of-1-to-5>


Sample input:

~~~text
user1,movie1,3
user1,movie1,1
user1,movie2,5
user2,movie1,4
...
~~~


Note that a user may rate the same movie any 
number of times.

a. The goal is to find the number of raters 
per movie.  Write a complete PySpark program 
(as a set of trasformations and actions) to 
accomplish this task. Your output will be

	<movieID> <number-of-raters>

b. The goal is to find the number of unique 
movies rated by each user. Write a complete 
PySpark program (as a set of trasformations 
and actions) to accomplish this task. Your 
output will be

	<userID> <number-of-unique-movies>


</font>

-------

## Question 29

<font size="4">

Consider the following in PySpark:

~~~python
>>> # spark: an instance of SparkSession object
>>> data = [1, 1, 1, 2, 3, 1, 2, 3, 3, 3]
>>> rdd = spark.sparkContext.parallelize(data, 3)
>>> rdd2 = rdd.map(lambda x: (x, x))
>>> grouped_rdd = rdd2.groupByKey().mapValues(lambda x : sum(x)).collect()
~~~

* Show the content of `grouped_rdd` in detail. 
* show your work...



</font>

-------

## Question 30

<font size="4">

Consider the following in PySpark:

~~~python
>>> # spark: an instance of SparkSession object
>>> data = [1, 1, 1, 1, 2, 2, 3, 1, 2, 3, 3, 3]
>>> rdd = spark.sparkContext.parallelize(data)
>>> rdd2 = rdd.map(lambda x: (x+1, x))
>>> reduced_rdd = rdd2.reduceByKey(lambda x, y: x + y).collect()
~~~

* Show the content of `reduced_rdd` in detail. 
* Show your work...



</font>

-------

## Question 31

<font size="4">

Consider the following in PySpark:

~~~python
>>> # spark: an instance of SparkSession object
>>> data = [1, -1, 1, 1, 0, 0, 1, -2, 
            2, 3, 1, 2, -3, ...]
>>> rdd = spark.sparkContext.parallelize(data)
~~~

Write a series of spark transformations to split 
rdd into two RDDs: `rddP` will hold only non-negative 
numbers and `rddN` will hold only negative numbers.


</font>

-------

## Question 32

<font size="4">


Let `genes.txt` be a huge text file, where every 
record has the following format (reference can be 
in {1, 2, 3}  where 1 denotes cancer, 2 denotes 
healthy, and 3 is undefined):

	<geneID><,><reference><,><geneValue>

For example, a sample input might be:

~~~text
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
~~~

a. Write a PySpark command/tranformation to
convert genes.txt file into an RDD[String] and 
then output the number of elements in that RDD 
(the final result will be in rdd1)

b. Let rdd1 represents the genes.txt file 
in Spark ((as RDD[String]).  Use rdd1 and write 
a PySpark command/tranformation to generate the 
following output per geneID

	<geneID> <M> <N>

where M is the number of cancer genes (for geneID) 
and N is the sum of values for the cancer genes.

c. Find sum of the values for all genes.

d.  Let rdd1 represents the genes.txt file 
(as RDD[String]) in Spark.  Use rdd1 and write 
a PySpark filter to keep only healthy genes.


</font>

-------

## Question 33

<font size="4">


Using Classic MapReduce, let `map()` and `reduce()` 
functions defined as:

Mapper:

~~~code
map(String K, Integer V) {
  if (V > 0) {
     emit("P", 1);
  }
  else if (V < 0) {
     emit("N", 1);
  }
  else {
     emit("Z", 1);
  }
}
~~~

Reducer:

~~~code
reduce(String K, Iterable<Integer> values) {
   Integer sum = 0;
   for (Integer n : values) {
      sum = sum + n;
   }
   emit (K, sum);
} 
~~~

a. What does this MapReduce program do? 
Provide your answer in at MOST 2 lines.

Consider the following (Key, Value) input to mappers: 

~~~text
Key	Value
a	2
b	-1
c	1
d	-3
e	0
f	0
g	0
h	5
i	6
j	4
~~~

b. Show all of the output emitted by all mappers.

c. Show all of the input to all reducers.

d. Show all of the output generated by all reducers


</font>

-------

## Question 34

<font size="4">


PySpark and MapReduce solutions:

Given the following input 
(millions of (key, value) pairs), find average rating 
per movie [note that ratings of less than 2 must be 
ignored]. The same movie can be rated any number of times. 

Input is given as: Key is a `movie_id` and Value is 
a rating  between 1 and 5.

~~~text
Key  Value
m1    1
m1    3
m1    1
m1    5
...  ...
m2    5
m2    4
...  ...
~~~


a. Write a map() function: must identify 
Key and Value and their data types for the map()

b. Show output of all mappers for movies { m1, m2 }

c. Write a reduce() function: must identify Key 
and Value for the reduce()

d. Show all input to all reducers for movies { m1, m2 }

e. Show output of all reducers for movies { m1, m2 }

f. Find average per movie by using reduceByKey()

g. Find average per movie by using groupByKey()


</font>

-------

## Question 35

<font size="4">


Using MapReduce and PySpark, 

write a series of transformations and actions to 
eliminate all duplicate records from a given big 
file called bigfile.txt.  Your output will be all 
of unique records contained in bigfile.txt.


</font>

-------

## Question 36

<font size="4">


Assume the following input

	<Employee-ID><,><type>

where type can be:

* `"fulltime"`
* `"parttime"`
* `"contractor"`

The goal is to write a PySpark program to count  
"fulltime" and "parttime" employees. Your output 
should be something like:

	fulltime: <number-of-fulltime-employees>
	parttime: <number-of-parttime-employees>


</font>

-------


## Question 37

<font size="4">


Given the following rdd in pyspark:

~~~python
>>> data = ['k1', 'k2', 'k1', 'k2', 
            'k1', 'k2', 'k3', 'k2', 'k4']
>>> # spark: an instance of SparkSession object
>>> rdd = spark.sparkContext.parallelize(data)
~~~

write a sequence of pyspark transformations 
and actions to find frequencies of all keys 
in data. Keep only the (key, frequency) 
pairs if the frequency is greater than one.

For this example, your solution should generate/output:

	('k1', 3)
	('k2', 4)



</font>

-------

## Question 38

<font size="4">


Consider the following in PySpark:

~~~python
>>> # spark: an instance of SparkSession object
>>> 
>>> data = [1, 1, 1, 1, 2, 2, 3, 1, 2, 3, 3, 3]
>>> rdd = spark.sparkContext.parallelize(data)
>>> rdd2 = rdd.map(lambda x: (x+1, x-1))
>>> my_output = rdd2.reduceByKey(lambda x, y: x + y).collect()
~~~

* Show the content of `my_output` in detail. 
* Show your work...


</font>

-------

## Question 39

<font size="4">


In Classic MapReduce, 
let a map() function be defined as:

~~~code
map(integer key, integer value) {
  if (key > value) {
     emit(key, value);
  }
  else {
     emit(key, 2);
  }
}
~~~


and consider the following (Key, Value) input to mappers: 

~~~text
Key	 Value
1	 2
2	 3
2	 1
1	 3
3    1
3	 4
4    3
~~~


a. Show all of the output emitted 
by all mappers: show your work step-by-step 
and show what is generated per mapper input.

b. Show all of the input to all reducers:

</font>

-------

## Question 40

<font size="4">


For a Classic MapReduce program, consider 
the following (key, value) pairs generated by 
all mappers:

~~~text
(a, 1), (b, 3), (c, 2), (a, 4), 
(b, 5), (c, 7), (a, 3),
(a, 2), (b, 3), (c, 4), (z, 1), 
(z, 2), (z, 3), (z, 4)
(z, 1), (z, 3), (a, 5), (z, 0), 
(z, 0), (z, 0), (z, 0)
~~~


a. Show the output of "Sort and Shuffle" 
phase for these input generated by all 
mappers (defined above):

b. Write a generic reduce() function and 
identify data type of key and value for a 
reducer, which will count the number of  
positives (numbers greater than zero) for 
each key.

c. Show all of the output generated by all reducers

d. What is ideal maximum number of reducers for the 
data (defined above)



</font>

-------

## Question 41

<font size="4">

Assume we have 100 billion numbers saved
in a file called big.txt (one number per record) 
and the goal is to find the number of positives 
(numbers greater than zero) and negatives (numbers 
less than zero) for all of these numbers. Write a 
Spark/PySpark program to accomplish this task. Your 
client has asked you to write an efficient program 
for this otherwise he will not pay any money for 
your software!



</font>

-------

## Question 42

<font size="4">

Given the following input, using Classic MapReduce, 
write a generic `map()` and `reduce()` functions to find 
minimum and maximum of all given input key(s) [1st column] 
and value(s) [2nd column]. For input (listed below), the 
output will: 

	min: 10
	max: 700

Input is given as:

~~~text
Key  Value
400    10
100    10
200    20
100    30
700    40
 50   500
~~~


a. Write a map() function: must identify Key and 
Value for the map()

b. Show output of all mappers

c. Write a reduce() function: must identify 
Key and Value for the reduce()

d. Show all input to all reducers

e. How many reducers will you have?

f. Show output of all reducers

g. Is your solution scalable?


</font>

-------

## Question 43

<font size="4">

Assume that all of the input is in 
a file called  "movies.txt" and each 
input record has the following format:

	<userID><,><movieID><,><rating-in-range-of-1-to-5>

Sample input:

~~~text
user1,movie1,3
user1,movie1,1
user1,movie2,5
user2,movie1,4
...
~~~


Note that a user may rate the same movie any 
number of times. You may use the following 
functions in your transformations:

	getUser("userX,movieY,ratingN")   returns "userX"
	getMovie("userX,movieY,ratingN")  returns "movieY"
	getRating("userX,movieY,ratingN") returns ratingN

MUST use the provided functions.

a. The goal is to find the number of raters 
per movie.  Write a complete PySpark program 
(as a set of trasformations and actions) to 
accomplish this task. Your output will be

	<movieID> <number-of-raters>

b. The goal is to find the number of unique 
movies rated by each user. Write a complete 
PySpark program (as a set of trasformations 
and actions) to accomplish this task. 
Your output will be

	<userID> <number-of-unique-movies>


</font>

-------

## Question 44

<font size="4">

Consider the following in PySpark:

~~~python
>>> data = [1, 1, 1, 2, 3, 1, 2, 3, 3, 3]
>>> # sc : as SparkContext object
>>> rdd = sc.parallelize(data)
>>> rdd2 = rdd.map(lambda x: (x, x+2))
>>> grouped_rdd = rdd2.groupByKey()
                 .mapValues(lambda x : sum(x))
                 .collect()
~~~

* Show the content of `grouped_rdd` in detail
* Show your work... step by step


</font>

-------

## Question 45

<font size="4">

Consider the following in PySpark:

~~~python
>>> data = [1, 1, 1, 1, 2, 2, 3, 1, 2, 3, 3, 3]
>>> # sc : as SparkContext object
>>> rdd = sc.parallelize(data)
>>> rdd2 = rdd.map(lambda x: (x+1, x))
>>> reduced_rdd = rdd2.reduceByKey(lambda x, y: x + y).collect()
~~~

* Show the content of `reduced_rdd` in detail. 
* Show your work step-by-step


</font>

-------

## Question 46

<font size="4">

Consider the following in PySpark:

~~~python
>>> data = [1, -1, 1, 1, 0, 0, 1, -2, 2, 3, 1, 2, -3, ...]
>>> # sc : as SparkContext object
>>> rdd = sc.parallelize(data)
~~~

Write a series of spark transformations to split rdd into 
three separate RDDs: 

* rddP will hold only positive numbers
* rddN will hold only negative numbers
* rddZ will hold only zeros 



</font>

-------

## Question 47

<font size="4">

Assume we have about 100 billion  
numbers saved in a file called big.txt 
(one number  per record) and the goal is 
to perform the following
in order (MUST USE PySpark):

a. create an RDD[Integer] as rdd

b. count the exact number of numbers in rdd

c. remove all negative numbers

d. count all remaining numbers


</font>

-------

## Question 48

<font size="4">

Given the following input, using 
Classic MapReduce, write an efficient map() and 
reduce() functions to find maximum of all given 
values for unique key(s) [1st column].  Note that 
the value field can have any number of numbers.
The goal is to find the maximum value per key.

Input is given as:

~~~text
Key  Value
a    10,4,50,40,30
a    10,60,50,20
b    20,20,30,40,50,2
b    30,40,55,3,5,1,4,5
...
~~~

a. Write a map() function: must identify Key and Value for the map()

b. Show output of all mappers

c. Write a reduce() function: must identify 
Key and Value for the reduce()

d. Show all input to all reducers

e. How many reducers will you have?

f. Show output of all reducers

g. Is your solution scalable

</font>

-------

## Question 49

<font size="4">

Consider the following in PySpark:

~~~python
>>> data = [0, 1, 0, 1, -1, 1, 0, 2, 3, 1, -2, -3, 3, 3]
>>> # sc : as SparkContext object
>>> rdd = sc.parallelize(data)
>>> rdd2 = rdd.filter(lambda v: v > 0)
>>> rdd3 = rdd2.map(lambda x: (x, x+2))
>>> reducedRDD = rdd3.reduceByKey(lambda x, y: x + y).collect()
~~~

* Show the content of `reducedRDD` in detail. 
* Show your work step-by-step


</font>

-------

## Question 50

<font size="4">

Consider the following in PySpark:

~~~python
>>> # sc : as SparkContext object
>>> data = [("a", 1), ("a", 1), ("a", 3), 
            ("b", 1), ("b", 1), ("b", 2),  ...]
>>> rdd = sc.parallelize(data)
~~~

* Write a series of spark transformations to find 
the average value per key.

* Write a series of spark transformations to find 
the maximum value per key.

* Write a series of spark transformations to find 
the median value per key.

* Write a series of spark transformations to find 
the mode value per key.

</font>

-------

## Question 51

<font size="4">

Consider the following in PySpark:
Let data represent a set of records:

~~~python
>>> # sc : as SparkContext object
>>> data = ["abc", "abc", "xyz", "xyz", "xyz", ...]
>>> rdd = sc.parallelize(data)
~~~

Write a series of PySpark transformations to eliminate
all duplicate records. For this example, the output
will be : ["abc", "xyz", ...]. NOTE that you can NOT
use unique() and distinct() transformations.



</font>

-------

## Question 52

<font size="4">

Given the following (key, value) pairs 
(as input to map()):

	<string-key-as-ISBN-of-a-book> <128-bytes-hash-code-of-entire-book>

Using "classic MapReduce" paradigm, write map() and reduce()
functions to list/output the ISBN's, which have more than 3
duplicates. Identify the data type of map() and reduce()
functions.


</font>

-------

## Question 53

<font size="4">

For a Classic MapReduce 
program, consider the following (key, value) 
pairs generated by all mappers:

~~~text
(a, 1), (a, 3), (c, 2), (a, 7)
(b, 2), (c, 0), (b, 4), (b, 6)
(c, 0), (z, 3), (c, 5), (z, 0)
~~~

a. Show the output of "Sort and 
Shuffle" phase for these input generated by 
all mappers (defined above):

b. Write a generic reduce() function and identify 
data type of key and value for a reducer, which 
will compute the average of values for each key.

c. Show all of the output generated by all reducers

d. What is ideal optimal maximum number of  
reducers for the  data (defined above)


</font>

-------

## Question 54

<font size="4">

Given the following input (file
big.txt) , using Spark's `mapPartitions()` write 
an efficient transformation to find minimum and 
maximum of all given numbers. Note that every 
record (single line of input) may have thousands 
of numbers.

Input is given as:

~~~text
10,4,50,40,30, ...
10,60,50,20, ...
20,20,30,40,50,2, ...
...
~~~


</font>

-------

## Question 55

<font size="4">

Given

~~~python
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
~~~

What is the output of this program?


</font>

-------

## Question 56

<font size="4">

Classic MapReduce and PySpark:

Given the following (key, value) pairs 
(as input to map()):

	<key-as-string> <value-as-integer>

Write a complete map()  and reduce() functions to 
find the median per key. Only, Output medians, which 
are greater than 10. 


</font>

-------

## Question 57

<font size="4">


Use PySpark to answer this question.

Assume that all of the input is in a file called  
"movies.txt" (with millions of records) and each 
input record has the following format:

	<MOVIE-ID><,><rating-in-range-of-1-to-5>

Sample input:

~~~text
movie1,3
movie1,1
movie1,5
movie2,5
movie2,4
movie2,3
...
~~~


Note that a user may rate the same movie any number of 
times.  You HAVE to use the following Python functions 
in your transformations.

Note that, you MUST NOT use the Python split() 
function at all, but you will use the following functions:

* getMovie("movie,rating")  returns "movie" as String
* getRating("movie,rating") returns rating as Integer

a. The goal is to find the number of raters 
per movie.  Write a complete PySpark program (as a set of 
PySpark trasformations and actions) to accomplish this task. 
Your output will be like:

	<MOVIE-ID> <number-of-raters>

b. The goal is to find the average rating 
per movie. Write a complete PySpark program (as a set of 
trasformations and actions) to accomplish this task. Your 
output will be as:

<MOVIE-ID> <average-rating-per-MOVIE-ID>


</font>

-------

## Question 58

<font size="4">

Consider the following in PySpark:

~~~python
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
~~~

* Show the output in detail.  
* Show your work step-by-step


</font>

-------

## Question 59

<font size="4">

Consider the following in PySpark:

~~~python

>>> data = [("a", 1), ("a", 20), ("a", 3), 
            ("b", 100), ("b", 1), ("b", 2), ...]
            
>>> # spark : as a SparkSession object     
>>> rdd = spark.sparkContext.parallelize(data)
~~~

* Using `groupByKey()`: Write a series of spark transformations to find 
the (minimum, maximum) value per key.

* Using `reduceByKey()`: Write a series of spark transformations to find 
the (minimum, maximum) value per key.

* Using `combineByKey()`: Write a series of spark transformations to find 
the (minimum, maximum) value per key.

</font>

-------

## Question 60

<font size="4">

Consider the following PySpark shell program:

~~~python
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
~~~

</font>

-------

## Question 61

<font size="4">

In Classic MapReduce, 
let a map() function be defined as:

~~~code
map(Integer key, Integer value) {
  if (key > value) {
     emit(key, value);
  }
  if (value > key) {
     emit(value, key);
  }  
  emit(key, key);
}
~~~

and consider the following (key, value) input to mappers: 

~~~text
key	 value
1	 2
2	 3
5	 2
6	 3
4    4
~~~


a. Show all of the output emitted 
by all mappers: show your work step-by-step 
and show what is generated per mapper input.

b. Show all of the input to all reducers:


</font>

-------

## Question 62

<font size="4">

Consider the following in PySpark:


~~~python
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
~~~

* Show the output. 
* Show your work step-by-step


</font>

-------

## Question 63

<font size="4">

Consider the following 
(key, value) pairs in PySpark:

~~~python
>>> data = [('A', 4), ('A', 8), 
            ('B', 5), ('B', 7), ...]
>>> # sc : as a SparkContext object     
>>> rdd = sc.parallelize(data)
~~~

a. Using `groupByKey()`, write a set of Spark transformations to 
find the average (mean) value per key. 

b. Using `reduceByKey()`, write a set of Spark transformations to 
find the average (mean) value per key. 

c. Using `combineByKey()`, write a set of Spark transformations to 
find the average (mean) value per key. 




</font>

-------

## Question 64

<font size="4">

In classic MapReduce, let `map()` 
and `reduce()` functions, and input defined 
as below.  Assume that the function `EVEN(x)` 
returns True if `x` is an even number, otherwise 
it returns False.

Mapper:
~~~code
map(String key, Integer value) {
  if (EVEN(value)) {
  	emit("even", 1);
  }  
  emit(key, value+1);
}
~~~

Reducer:
~~~code
reduce(String key, Iterable<Integer> values) {
   Integer sum = 0;
   for (Integer n : values) {
      sum = sum + n;
   }
   emit (key, sum);
} 
~~~

Input to mappers are as (key, value) pairs:

~~~text
k1	3
k2	2
k3	1
k1	1
k2	6
k2	5
k3  7
~~~

a. Show all of the output emitted by 
all mappers (per mapper input):

b. Show all of the input to all reducers:

c. Show all of the output generated by all reducers


</font>

-------

## Question 65

<font size="4">


64. Given the following rdd of pairs 
in PySpark:

~~~python
>>> data = [('k1', 5), ('k1', 6), ('k1', 7), 
            ('k2', 7), ('k2', 8), ('k2', 7), ('k2', 8)]
>>> # spark : as a SparkSession object     
>>> rdd = spark.sparkContext.parallelize(data)
~~~

1. Write an efficient sequence of pyspark transformations and actions 
to find unique list of keys: `{'k1', 'k2'}`
 
2. Write an efficient sequence of pyspark transformations and actions 
to find unique list of values: `{5, 6, 7, 8}`
 
</font>

-------

## Question 66

<font size="4">


Given the following rdd of pairs in PySpark:

~~~python
>>> data = [('k1', 5), ('k1', 6), ('k1', 7), ('k1', 5), ('k1', 6)
            ('k2', 7), ('k2', 8), ('k2', 7), ('k2', 8), ('k2', 9)]
>>> # spark : as a SparkSession object     
>>> rdd = spark.sparkContext.parallelize(data)
~~~

1. Using `groupByKey()` write a sequence of pyspark transformations to 
find the (minimum, maximum) value per key.

2. Using `reduceByKey()` write a sequence of pyspark transformations to 
find the (minimum, maximum) value per key.

3. Using `combineByKey()` write a sequence of pyspark transformations to 
find the (minimum, maximum) value per key.

</font>

-------

## Question 67

<font size="4">


66. Given the following rdd of 
pairs in PySpark:

~~~python
>>> data = [('a', 2), ('b', 3), ('d', 2), ('x', 3), ('y', 1), ...]
>>> # spark : as a SparkSession object     
>>> rdd = spark.sparkContext.parallelize(data)
~~~

Write a sequence of pyspark transformations to 
generate the following output: MUST use flatMap():

~~~text
[
 'a', 'a', 
 'b', 'b', 'b', 
 'd', 'd', 
 'x', 'x', 'x', 
 'y',
  ...
]
~~~


</font>

-------

## Question 68

<font size="4">


Consider the following RDD:

~~~python

>>> input = [("k1", "v1"), ("k1", "v1"), ("k1", "v2"), ...]
>>> # sc : as a SparkContext object     
>>> rdd = sc.parallelize(input)
~~~

The goal is to write a set of Spark transformations to 
generate unique (K, V) pairs [combination of K and V must 
be unique]. You may NOT use Spark's distinct() function.


</font>

-------

## Question 69

<font size="4">


68. Consider the following RDD:

~~~python
>>> input = [("a", 2), ("a", -4), ("a", 9), ...
             ("b", 9), ("b", 7), ("b", -3), ...
             ("c", 2), ("c", 4), ...]
>>> # sc : as a SparkContext object     
>>> rdd = sc.parallelize(input)
~~~

The goal is to write a a set of Spark transformations
(by using `rdd` -- represents the employees table as
(key, value) pairs -- as your starting point) to find
the result for the following SQL statement:

~~~sql
   SELECT key, AVG(value), SUM(value)
     FROM employees WHERE value > 0
       GROUP BY key;
~~~

</font>

-------

## Question 70

<font size="4">


Assume that we have a MapReduce cluster with 
101 nodes (one master node and 100 worker
nodes and master does not store any data at all). 
Further assume that the data replication factor is 7.  

Using this cluster, we are running a single MapReduce 
program (job), at most, how many worker nodes can fail 
at a single point of time so that the whole single job 
will not fail.

A.  **99 nodes**

B.  **7 nodes**

C.  **8 nodes**

D.  **5 nodes**

E.  **6 nodes**


</font>

-------

## Question 71

<font size="4">


Given a graph as a list edges:

		<src_node_id><,><dst_node_id>

1. With using GraphFrames, find `inDegrees` 
and `outDegrees` of all nodes.

2. Without using GraphFrames, find `inDegrees` 
and `outDegrees` of all nodes.

The in-degree of each vertex in the graph, 
returned as a DataFame with two columns:

* “id”: the ID of the vertex
* “inDegree” (int) storing the in-degree of the vertex

Note that vertices with 0 in-edges are not returned 
in the result.

The out-degree of each vertex in the graph, returned 
as a DataFrame with two columns:

* “id”: the ID of the vertex
* “outDegree” (integer) storing the out-degree of the vertex 

Note that vertices with 0 out-edges are not returned 
in the result.


</font>

-------

## Question 72

<font size="4">


Given Credit Card data records as:

	<transaction_id><,><date><,><transaction_amount><,><item><,><customer_id>
where 

	<date>=<dd/mm/YYYY>


1. If we will query data by YYYY, how would you partition 
your data by using PySpark?

2. If we will query data by YYYY and mm, how would you 
partition your data by using PySpark?

3. If we will query data by YYYY and customer_id, how would 
you partition your data by using PySpark?


</font>

-------

## Question 73

<font size="4">

Consider the following file: `/home/data.txt`, 
which has 5 records:

~~~text
$ cat /home/data.txt
w11,w2
w1,w21,w3,w3
w1,w2,w31,w31,w3
w1,w1,w21,w2
w2,w21,w2,w1
~~~

and consider the following PySpark segment:

~~~python
# sc : as a SparkContext object     
lines = sc.textFile("/home/data.txt")
rdd1 = lines.flatMap(lambda s: s.split(",")).filter(lambda x : len(x) < 3)
rdd1.count() # output 1
rdd2 = rdd1.map(lambda s : (1, s))
rdd3 = rdd2.map(lambda s: (s[1], s[0]))
rdd4 = rdd3.reduceByKey(lambda x, y: x+y).filter(lambda x : x[1] > 2)
rdd4.collect() # output 2
~~~

what will be the output?


</font>

-------

## Question 74

<font size="4">


Consider the following input:

~~~text
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
~~~

Suppose, we want to  count all positives, negatives, 
zeros, odd and even numbers.  Write an efficient PySpark 
program to accomplish this task.


</font>

-------

## Question 75

<font size="4">

Consider the following SQL query:

~~~sql
SELECT COUNT(CustomerID) as count, Country
  FROM Customers
   GROUP BY Country;
~~~

If the Customers table dumped as a file (dump.csv) with the 
following format:

	<CustomerID><,><Country>

How would you translate this SQL query by


1. PySpark RDDs

2. PySpark DataFrames


</font>

-------

## Question 76

<font size="4">

Consider the following SQL query:

~~~sql
SELECT COUNT(CustomerID) as COUNTED, Country
   FROM Customers
     GROUP BY Country
       ORDER BY COUNTED DESC
          LIMIT 5;
~~~

If the Customers table dumped as a file (dump.csv) with the 
following format:

	<CustomerID><,><Country>

How would you translate this SQL query by


1. PySpark RDDs

2. PySpark DataFrames


</font>

-------

## Question 77

<font size="4">

Consider the following SQL query:

~~~sql
SELECT NAME, SUM(SALARY) FROM Employee 
GROUP BY NAME
HAVING SUM(SALARY) > 3000; 
~~~

If the Employee table dumped as file dump.csv with records:

	<NAME><,><SALARY>

How would you translate this SQL query by

1. PySpark RDDs

2. PySpark DataFrames


</font>

-------

## Question 78

<font size="4">


Create a DataFrame with the following columns:
(dept, name, salary), your data frame should have 
at least 4 rows.

77.1 Find average of salary per dept.
77.2 Find maximum of salary per dept.
77.3 Find minimum of salary per dept.
77.4 find (minimum, maximum) salary per dept.


</font>

-------

## Question 79

<font size="4">


Let `e` be a DataFrame representing edges
in a GraphFrame environment:

		e = (src, dst, weight)
 			
and `g` be a graph `(v, e)`
where v represent vertices as `(id, name)`.

Write a series of transformations to make this
graph undirected.


</font>

-------

## Question 80

<font size="4">


Let an rdd denote RDD[(String, String, Integer)]
to be of triplets (id, name, salary).

Then write a series of transformations to convert 
RDD into a DataFrame with 3 columns:  (id, name, salary)


</font>

-------

## Question 81

<font size="4">


Given a CSV file with the following fileds:

	continent, country, city, temperature


1. Create a DataFrame with 4 columns

2. Find average temperature per continent

3. Find average temperature per (continent, country)

4. Partition data in such a way that 80%
     of queries will analyze data by continent

5. Partition data in such a way that 70%
     of queries will analyze data by (continent, country).
     What will be table schema?

6. Partition data in such a way that 70%
     of queries will analyze data by 
     (continent) OR (continent, country, city)
     What will be table schema?
     

</font>

-------

## Question 82

<font size="4">


Let `e` be a DataFrame representing edges
in a GraphFrame environment:
 
 
		e = (src, dst, weight)


Yow want to build a graph by using GraphFrame.

Create `v` as vertices from a given `e`.
and then create a graph as `(v, e)`.


</font>

-------

## Question 83

<font size="4">


Consider the following SQL query:

~~~sql
SELECT COUNT(CustomerID) as COUNTED, Continent, Country
FROM Customers
GROUP BY Continent, Country
ORDER BY COUNTED DESC
LIMIT 5;
~~~

If the Customers table dumped as a file dump.csv with 
the following records:

	<CustomerID><,><continent><,><Country>

How would you translate this SQL query by

a. PySpark RDDs

b. PySpark DataFrames


</font>

-------

## Question 84

<font size="4">


Consider the following DataFrame:

~~~python
features = [('alex', 1), ('bob', 3), ('ali', 6), ('dave', 10)]
columns = ("name", "age")
# spark : as a SparkSession object     

samples = spark.createDataFrame(features, columns)
>>> samples.show()
+----+---+
|name|age|
+----+---+
|alex|  1|
| bob|  3|
| ali|  6|
|dave| 10|
+----+---+
~~~

How would you standardize the age column:
where 

	age_scaled = (age - mean_age) / standard_deviation_age

The output should be:

~~~text
+----+---+-------------------+
|name|age|age_scaled         |
+----+---+-------------------+
|alex|1  |-1.0215078369104984|
|bob |3  |-0.5107539184552492|
|ali |6  |0.2553769592276246 |
|dave|10 |1.276884796138123  |
+----+---+-------------------+
~~~

</font>

-------

## Question 85

<font size="4">


Let e be a DataFrame representing edges
in a GraphFrame environment:
 
	e = (src, dst, weight)

and `g` be a graph `(v, e)`
where `v` represent vertices as `(id, name, age)`.

Write a series of transformations to 
find name's of users, which are connected
bi-directionally and age difference is 5.

</font>


-------

## Question 86

<font size="4">


Let `df` be a Spark DataFrame representing `(name, age, salary)`.

1. Create a new DataFrame for teenagers.

2. Create a new DataFrame, to represnet `(name, avg_salary)`,
where `avg_salary` is an average salary per `name`.

3. Create a new DataFrame for baby boomers.

4. Create a new DataFrame `(age, count)`, 
where `count` is a frequncy per `age`.

</font>

-------

## Question 87

<font size="4">


1. Use Python, to create 1000,000 random numbers
in range of 1 to 100.

2. Then using PySpark, create an RDD for this one million numbers.

3. Then, find frequency of numbers.

4. Finally, find top-5 numbers (with highest frequencies)


</font>

-------

## Question 88

<font size="4">

Give a Spark DataFrame: 

		(emp_id, age, salary, year)
		

Each employee may have many records.	

1. Create a DataFrame with 10 reords and 2 `emp_id`(s)


2. Write a set of transformations to create the following DataFrame:

		(emp_id, salary, year)
	
where `(emp_id, year)` is distinct and `salary` is the maximum salary per year.


3. Using a DataFrame created in Step-2: write a set of transformations 
 to create the following DataFrame:

		(emp_id, average_salary, minimum_salary, maximum_salary)


</font>