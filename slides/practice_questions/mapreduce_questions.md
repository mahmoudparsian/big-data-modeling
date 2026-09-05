# MapReduce Practice Questions

* For practice purposes, some scenarios reappear in a different form/shape
  elsewhere in this question set (and in
  [`rdds_questions.md`](rdds_questions.md), which has the PySpark-RDD
  version of every MapReduce+PySpark question below).

* You may not post the solutions to these questions anywhere.

* Created and Compiled by: Mahmoud Parsian

* Last updated: 9/4/2026 (split out of the combined
  `mapreduce_and_RDD_questions.md`)

---

## Question 1

Assume that we have about 100,000 `gene_id(s)`.

Assume we have billions of records and consider the following input record
format:

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

The goal is to find average and median of gene value(s) for each `gene_id`.

Write a MapReduce solution (mapper and reducer) to solve this problem.

The following rules must be implemented:

1. If a gene value is less than 0, then that record is dropped
2. If a record does not have a proper format, then that record is dropped
3. If average of a `gene_id` is less than 1.5, then no output is created at
   all for that `gene_id`
4. Can we write combiners? How? Show your work, and justify your answer.
5. Discuss the number of mappers and reducers

---

## Question 2

Given billions of numbers (assume each record has a single number), find
count, minimum, maximum, and average for all numbers.

Discuss the number of mappers and reducers.

---

## Question 3

Given billions of numbers (assume each record has a single number), find
count of zeros, positives, and negatives.

Discuss the number of mappers and reducers.

---

## Question 4

Given the following input, `(key-as-string, value-as-integer)`, write a
`map()` and `reduce()` functions to find average of all given input per key.

Discuss the number of mappers and reducers.

---

## Question 5

Given billions of records (assume each record is a string of any value),
write a MapReduce program to make sure that every record has only one
duplicate.

---

## Question 6

Given billions of input records (each record is a String object), write a
MapReduce program to remove all duplicate records. The result will be all
distinct records.

---

## Question 7

Explain how the MapReduce framework can be used to join two tables `R(B, A)`
and `S(B, C)`. Here `R(B, A)` is a table with two attributes A and B.
Similarly `S(B, C)` is a table with two attributes B and C. The tables R and
S are joined on the attribute B.

Input for R is expressed as:

```text
R,b7,a1
R,b2,a2
R,b3,a3
...
```

Input for S is expressed as:

```text
S,b1,c1
S,b1,c2
S,b3,c3
S,b7,c1
S,b7,c2
...
```

1. Write `map()` and `reduce()` functions to perform **inner join** and show
   your work in detail
2. Write `map()` and `reduce()` functions to perform **left join** and show
   your work in detail
3. Write `map()` and `reduce()` functions to perform **right join** and show
   your work in detail

---

## Question 8

Given the following input, write a `map()` and `reduce()` functions to find
maximum of all given values for associated keys.

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

---

## Question 9

*(Companion question — see [Question 1](rdds_questions.md#question-1) in
`rdds_questions.md` for the PySpark version of this same problem.)*

Consider the following input record format:

    <student_id><,><single-grade-in-range-of-0-to-100>

The goal is to find minimum and maximum of grades for all students. Write a
MapReduce program (mapper and reducer) to accomplish this task. Your output
will be:

    <student_id> <minimum-grade> <maximum-grade>

The following rules must be implemented:

1. If a grade is over 100, then that record is dropped
2. If a grade is less than 10, then that record is dropped

---

## Question 10

*(Companion question — see [Question 2](rdds_questions.md#question-2) in
`rdds_questions.md` for the PySpark version of this same problem.)*

Consider the following input record format:

    <movie-name><,><rating-in-range-of-1-to-5>

The goal is to find the number of raters per movie. Write a MapReduce
program (mapper and reducer) to accomplish this task. Your output will be:

    <movie-name> <number-of-raters>

The following rules must be implemented:

1. If a rating is over 5, then that record is dropped
2. If a record does not have a proper format, then that record is dropped

---

## Question 11

*(Companion question — see [Question 3](rdds_questions.md#question-3) in
`rdds_questions.md` for the PySpark version of this same problem.)*

Assume the following input:

    <gene-ID><,><reference><,><gene-value>

where reference can be:

* `"r1"`: as normal
* `"r2"`: as cancer
* `"r3"`: as unknown

Write a MapReduce program (mapper and reducer) to keep only normal genes and
finally count them for all genes.

---

## Question 12

*(Companion question — see [Question 4](rdds_questions.md#question-4) in
`rdds_questions.md` for the PySpark version of this same problem.)*

Let a bigram be defined as a sequence of two consecutive words. For example
for the following input: `"w1,w2,w3,w4"`, we can construct the following
three bigrams:

    w1, w2
    w2, w3
    w3, w4

Let your input be a huge text file (`x.dat`), where each record has the
following format (a record may have any number of words):

    <word1><,><word2><,><word3>...

Write a MapReduce program (mapper and reducer) to find the frequency of all
unique bigrams.

---

## Question 13

In classic MapReduce, let `map()` and `reduce()` functions, and input
defined as [note that function `even(x)` returns `True` if `x` is an even
number, otherwise it returns `False`]:

Mapper:

```code
map(String K, Integer V) {
  if (even(V)) {
  	emit("k2", 2);
  }
  emit(K, V+1);
}
```

Reducer:

```code
reduce(String K, Iterable<Integer> V) {
   integer sum = 0;
   for (integer n : V) {
      sum = sum + n;
   }
   emit (K, sum);
}
```

Input to mappers as (Key, Value) pairs:

```data
k1	3
k2	2
k3	1
k1	1
k2	2
k2	4
k3  7
k3  5
```

a. Show all of the output emitted by all mappers

b. Show all of the input to all reducers

c. Show all of the output generated by all reducers

---

## Question 14

Assume our big data test cluster has only 4 worker nodes/servers labeled as

    {S0, S1, S2, S3, S4}

The server `S0` is the master node and does not store any data at all. Let
the replication factor be `2` and let's have the following big files: file1
and file2 as

    file1 = {f1, f2, f3} (3 blocks in Hadoop)
    file2 = {f4, f5} (2 blocks in Hadoop)

How would the Hadoop distributed file system place these two files in our
defined cluster?

a. You need to show how these two files will be placed at the cluster
   nodes. Show your answer per node.

b. Which server or servers maintain the metadata information about all
   these files?

---

## Question 15

Classic MapReduce:

Palindrome is a word, which reads the same backward or forward. Let
`PAL(x)` be a defined function (you are not required to implement this
function, just use it), which returns true if `x` is a Palindrome and false
otherwise. The goal is to read a set of documents (as a set of text files)
and find frequencies of all palindromes. Write `map()` and `reduce()`
functions to find frequencies of all palindromes. The input to `map()` will
be a pair of `(K, V)`, where `K` is a document ID and `V` is a single
sentence as a String (comprised of many words).

---

## Question 16

*(Companion question — see [Question 5](rdds_questions.md#question-5) in
`rdds_questions.md` for the PySpark version of this same problem.)*

Given the following input, write a generic `map()` and `reduce()` functions
to find the maximum and minimum of all given keys and values for all given
records. For the following input (listed below), the output (output of all
reducers) will be:

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

a. Write a `map()` function: must identify Key and Value for the `map()`

b. Show output of all mappers

c. Write a `reduce()` function: must identify Key and Value for the
   `reduce()`

d. Show all input to all reducers

e. Is your MapReduce solution efficient? Discuss in detail.

---

## Question 17

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

---

## Question 18

Let String and Integer be basic data types. In classic MapReduce, let
`map()` and `reduce()` functions be defined as follows:

Mapper:

```code
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
```

Reducer:

```code
reduce(String key, Iterable<Integer> values) {
   Integer sum = 0;
   for (Integer n : values) {
      if (n > 1) {
         sum = sum + n;
      }
   }
   emit (key, sum);
}
```

Let the input be the following (key, value) to mappers:

```data
key	value
1	   2
5     3
3     2
1     1
4     1
```

a. Show all of the output emitted by all mappers: for each input, show
   output.

b. Show output of MapReduce's sort and shuffle phase:

c. At most, how many reducers are needed and what are the reducer's keys
   and values?

d. Show all of the output generated by all reducers

---

## Question 19

PySpark and MapReduce:

Given large set of documents, we want to use classic MapReduce to create an
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

The goal is to develop a classic MapReduce program for inverted index
creation: generate a list of locations (word number in the document and
identifier for the document) for each word occurrence. An identifier for
each document is provided as the key to the `map()` function and value is a
string of words (for example, "hello hello hello fox" will be the value for
"Document3").

a. Write a `map()` function; you must identify Key and Value for the
   `map()`

b. Write a `reduce()` function (a classic MapReduce reducer, NOT a PySpark
   function); you must identify Key and Value for the `reduce()`

*(For the PySpark/RDD version of this same inverted-index problem, see
[Question 6](rdds_questions.md#question-6) in `rdds_questions.md`.)*

---

## Question 20

In classic MapReduce, let a `map()` function be defined as:

```code
map(integer key, integer value) {
  if (key == value) {
     emit(key, value);
  }
  else {
     emit(key, 1);
     emit(value, key);
  }
}
```

and consider the following (Key, Value) input to mappers:

```code
Key   Value
1     1
2     2
3     1
4     4
2     4
5     5
```

a. Show all of the output emitted by all mappers: show your work
   step-by-step and show what is generated per mapper input.

b. Show all of the input to all reducers.

---

## Question 21

For a classic MapReduce program, consider the following `(key, value)`
pairs generated by all mappers:

```text
(a, -2), (b, 3), (c, 2), (a, 4),
(b, 5), (c, 7), (a, -3), (c, -5)
(a, 2), (b, 3), (c, 4), (z, 1),
(z, 2), (z, 3), (b, -2), (z, -4)
(z, 1), (z, 3), (a, 5), (z, 0),
(z, 0), (z, 0), (z, 0), (z, 0)
```

a. Show the output of Sort and Shuffle phase for these input generated by
   all mappers (defined above).

b. Write a generic `reduce()` function and identify data type of key and
   value for a reducer, which will count the number of zeros, positives,
   and negatives for each key.

c. Show all of the output generated by all reducers.

d. What is the ideal maximum number of reducers for the data (defined
   above)?

---

## Question 22

Using Classic MapReduce, let `map()` and `reduce()` functions be defined
as:

Mapper:

```code
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
```

Reducer:

```code
reduce(String K, Iterable<Integer> values) {
   Integer sum = 0;
   for (Integer n : values) {
      sum = sum + n;
   }
   emit (K, sum);
}
```

a. What does this MapReduce program do? Provide your answer in at MOST 2
   lines.

Consider the following (Key, Value) input to mappers:

```text
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
```

b. Show all of the output emitted by all mappers.

c. Show all of the input to all reducers.

d. Show all of the output generated by all reducers.

---

## Question 23

In classic MapReduce, let a `map()` function be defined as:

```code
map(integer key, integer value) {
  if (key > value) {
     emit(key, value);
  }
  else {
     emit(key, 2);
  }
}
```

and consider the following (Key, Value) input to mappers:

```text
Key	 Value
1	 2
2	 3
2	 1
1	 3
3    1
3	 4
4    3
```

a. Show all of the output emitted by all mappers: show your work
   step-by-step and show what is generated per mapper input.

b. Show all of the input to all reducers.

---

## Question 24

For a Classic MapReduce program, consider the following (key, value) pairs
generated by all mappers:

```text
(a, 1), (b, 3), (c, 2), (a, 4),
(b, 5), (c, 7), (a, 3),
(a, 2), (b, 3), (c, 4), (z, 1),
(z, 2), (z, 3), (z, 4)
(z, 1), (z, 3), (a, 5), (z, 0),
(z, 0), (z, 0), (z, 0)
```

a. Show the output of "Sort and Shuffle" phase for these input generated by
   all mappers (defined above).

b. Write a generic `reduce()` function and identify data type of key and
   value for a reducer, which will count the number of positives (numbers
   greater than zero) for each key.

c. Show all of the output generated by all reducers.

d. What is the ideal maximum number of reducers for the data (defined
   above)?

---

## Question 25

Given the following input, using Classic MapReduce, write a generic
`map()` and `reduce()` functions to find minimum and maximum of all given
input key(s) [1st column] and value(s) [2nd column]. For input (listed
below), the output will be:

    min: 10
    max: 700

Input is given as:

```text
Key  Value
400    10
100    10
200    20
100    30
700    40
 50   500
```

a. Write a `map()` function: must identify Key and Value for the `map()`

b. Show output of all mappers

c. Write a `reduce()` function: must identify Key and Value for the
   `reduce()`

d. Show all input to all reducers

e. How many reducers will you have?

f. Show output of all reducers

g. Is your solution scalable?

---

## Question 26

Given the following input, using Classic MapReduce, write an efficient
`map()` and `reduce()` functions to find maximum of all given values for
unique key(s) [1st column]. Note that the value field can have any number
of numbers. The goal is to find the maximum value per key.

Input is given as:

```text
Key  Value
a    10,4,50,40,30
a    10,60,50,20
b    20,20,30,40,50,2
b    30,40,55,3,5,1,4,5
...
```

a. Write a `map()` function: must identify Key and Value for the `map()`

b. Show output of all mappers

c. Write a `reduce()` function: must identify Key and Value for the
   `reduce()`

d. Show all input to all reducers

e. How many reducers will you have?

f. Show output of all reducers

g. Is your solution scalable?

---

## Question 27

Given the following (key, value) pairs (as input to `map()`):

    <string-key-as-ISBN-of-a-book> <128-bytes-hash-code-of-entire-book>

Using "classic MapReduce" paradigm, write `map()` and `reduce()` functions
to list/output the ISBN's, which have more than 3 duplicates. Identify the
data type of `map()` and `reduce()` functions.

---

## Question 28

For a Classic MapReduce program, consider the following (key, value) pairs
generated by all mappers:

```text
(a, 1), (a, 3), (c, 2), (a, 7)
(b, 2), (c, 0), (b, 4), (b, 6)
(c, 0), (z, 3), (c, 5), (z, 0)
```

a. Show the output of "Sort and Shuffle" phase for these input generated by
   all mappers (defined above).

b. Write a generic `reduce()` function and identify data type of key and
   value for a reducer, which will compute the average of values for each
   key.

c. Show all of the output generated by all reducers.

d. What is the ideal optimal maximum number of reducers for the data
   (defined above)?

---

## Question 29

Classic MapReduce:

Given the following (key, value) pairs (as input to `map()`):

    <key-as-string> <value-as-integer>

Write a complete `map()` and `reduce()` functions to find the median per
key. Only output medians, which are greater than 10.

---

## Question 30

In Classic MapReduce, let a `map()` function be defined as:

```code
map(Integer key, Integer value) {
  if (key > value) {
     emit(key, value);
  }
  if (value > key) {
     emit(value, key);
  }
  emit(key, key);
}
```

and consider the following (key, value) input to mappers:

```text
key	 value
1	 2
2	 3
5	 2
6	 3
4    4
```

a. Show all of the output emitted by all mappers: show your work
   step-by-step and show what is generated per mapper input.

b. Show all of the input to all reducers.

---

## Question 31

In classic MapReduce, let `map()` and `reduce()` functions, and input
defined as below. Assume that the function `EVEN(x)` returns True if `x` is
an even number, otherwise it returns False.

Mapper:

```code
map(String key, Integer value) {
  if (EVEN(value)) {
  	emit("even", 1);
  }
  emit(key, value+1);
}
```

Reducer:

```code
reduce(String key, Iterable<Integer> values) {
   Integer sum = 0;
   for (Integer n : values) {
      sum = sum + n;
   }
   emit (key, sum);
}
```

Input to mappers are as (key, value) pairs:

```text
k1	3
k2	2
k3	1
k1	1
k2	6
k2	5
k3  7
```

a. Show all of the output emitted by all mappers (per mapper input):

b. Show all of the input to all reducers:

c. Show all of the output generated by all reducers

---

## Question 32

Assume that we have a MapReduce cluster with 101 nodes (one master node and
100 worker nodes and master does not store any data at all). Further assume
that the data replication factor is 7.

Using this cluster, we are running a single MapReduce program (job); at
most, how many worker nodes can fail at a single point of time so that the
whole single job will not fail?

A. **99 nodes**

B. **7 nodes**

C. **8 nodes**

D. **5 nodes**

E. **6 nodes**

---

## Question 33

*(Companion question — see [Question 34](rdds_questions.md#question-34) in
`rdds_questions.md` for the `reduceByKey()`/`groupByKey()` PySpark version
of this same problem.)*

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

a. Write a `map()` function: must identify Key and Value and their data
   types for the `map()`

b. Show output of all mappers for movies `{ m1, m2 }`

c. Write a `reduce()` function: must identify Key and Value for the
   `reduce()`

d. Show all input to all reducers for movies `{ m1, m2 }`

e. Show output of all reducers for movies `{ m1, m2 }`

---

## Question 34

Let a huge text file (`document.txt`) hold many lines of text, where each
line may contain any number of words separated by whitespace. The classic
"Word Count" problem asks for the frequency of every unique word across
the whole file.

Sample input:

```text
the quick brown fox
the fox jumped over the lazy dog
...
```

a. Write a `map()` function: identify the Key and Value type emitted by
   `map()`.

b. Write a `reduce()` function: identify the Key and Value type consumed
   and produced by `reduce()`.

c. For the sample input above, show the exact output emitted by all
   mappers.

d. Show the exact output produced by all reducers.

e. Can a combiner be used here? If yes, write the combiner function. If
   no, explain why not.

---

## Question 35

Given billions of `(gene_id, gene_value)` records, the goal is to compute
the **average** `gene_value` per `gene_id` using classic MapReduce.

a. A naive combiner that just re-applies the reducer's averaging logic
   locally (per mapper) and then averages those partial averages again in
   the reducer will, in general, produce the **wrong** final answer.
   Construct a small numeric counterexample (3-4 records for a single
   `gene_id`) that proves this.

b. Design a combiner that IS correct for this problem. What intermediate
   `(key, value)` shape must the combiner emit so that the reducer can
   still compute the exact, correct average? Write both the combiner and
   the (now different) reducer.

c. Why does `sum` behave differently than `average` with respect to
   combiner correctness? Answer in at most 3 sentences.

---

## Question 36

Assume 100 million `(customer_id, purchase_amount)` records are being
processed by a MapReduce job that sums `purchase_amount` per
`customer_id`. Assume further that 1% of all `customer_id` values (a
small set of "whale" customers) account for 60% of all records —
i.e., the key space is heavily skewed.

a. If you use Hadoop's default hash partitioner, what problem will you
   observe at the reduce phase? Be specific about which reducer(s) are
   affected and why.

b. Design a custom partitioner (describe its `getPartition(key,
   numReducers)` logic) that reduces this skew. You may change the
   `(key, value)` shape emitted by the mapper if it helps (e.g., salting).

c. If you salt the keys in part (b), describe the extra step needed after
   the reducers finish to produce one final total per `customer_id`.

---

## Question 37

Given billions of `(sensor_id, timestamp, reading)` records, the goal is
to produce, for each `sensor_id`, its readings **sorted by timestamp**
(a "secondary sort" — sorting the values within each key's group, not
just grouping by key).

a. Explain why a naive `reduce()` that calls `Collections.sort()` on the
   in-memory list of values for a key is a poor solution at scale.

b. Describe the "value-to-key conversion" (composite-key) secondary-sort
   pattern: what does the new map-output key look like, and what custom
   Partitioner and grouping comparator are needed so that all records for
   the same `sensor_id` still land in the same reducer, arriving already
   sorted by `timestamp`?

c. Write the `map()` function that emits the composite key described in
   part (b).
