# PySpark RDD Practice Questions — Set 02: flatMap, groupByKey, sortByKey

Nine practice questions on PySpark RDDs, three at each level, meant to be
solved **interactively** in the `bin/pyspark` shell. This set builds on
[Set 01](QUESTIONS_01.md) and adds `flatMap()`, `groupByKey()`, and
`sortByKey()`:

| # | Level | Topic | Input |
|---|---|---|---|
| [Q1](#q1-basic--map-vs-flatmap) | Basic | `map()` vs `flatMap()` | Python list |
| [Q2](#q2-basic--sorting-products) | Basic | Sorting products | Python list |
| [Q3](#q3-basic--grouping-employees-by-department) | Basic | Grouping employees by department | Python list |
| [Q4](#q4-intermediate--word-count-with-the-top-words) | Intermediate | Word count, with the top words | [`data/quotes.txt`](data/quotes.txt) |
| [Q5](#q5-intermediate--groupbykey-vs-reducebykey) | Intermediate | `groupByKey()` vs `reduceByKey()` | [`data/transactions.txt`](data/transactions.txt) |
| [Q6](#q6-intermediate--students-per-course) | Intermediate | Students per course | [`data/enrollments.txt`](data/enrollments.txt) |
| [Q7](#q7-intermediate--implement-distinct-yourself) | Intermediate+ | Implement `distinct()` yourself | Python list and [`data/web_logs.txt`](data/web_logs.txt) |
| [Q8](#q8-intermediate--build-an-inverted-index) | Intermediate+ | Build an inverted index | [`data/documents.txt`](data/documents.txt) |
| [Q9](#q9-intermediate--top-2-students-per-class) | Intermediate+ | Top 2 students per class | [`data/class_scores.txt`](data/class_scores.txt) |

Use **only** these RDD operations:

* **Creating RDDs:** `sc.parallelize(...)` and `sc.textFile(...)`
* **Transformations:** `map()`, `flatMap()`, `filter()`, `mapValues()`,
  `groupByKey()`, `reduceByKey()`, `sortByKey()`, `distinct()`
* **Actions:** `collect()`, `count()`, `take(n)`

Plain Python (`sorted()`, `len()`, `sum()`, string methods, and your own
`def` functions) is fine inside your lambdas and on the results you collect.

Try each question on your own before you open the
[answers](ANSWERS_02.md).

## Files

| File | Purpose |
|---|---|
| [QUESTIONS_02.md](QUESTIONS_02.md) | The nine questions (this page) |
| [ANSWERS_02.md](ANSWERS_02.md) | Step-by-step solutions, with real output from Spark 4.2.0 |
| [data/quotes.txt](data/quotes.txt) | Input for Q4: plain text sentences |
| [data/transactions.txt](data/transactions.txt) | Input for Q5: `customer,amount` |
| [data/enrollments.txt](data/enrollments.txt) | Input for Q6: `student:course1,course2,...` |
| [data/web_logs.txt](data/web_logs.txt) | Input for Q7 (also used in Set 01): `date,user,url,http_status` |
| [data/documents.txt](data/documents.txt) | Input for Q8: `doc_id\|text` |
| [data/class_scores.txt](data/class_scores.txt) | Input for Q9: `class,student,score` |

## Getting started

Start `$SPARK_HOME/bin/pyspark` **from this folder**, exactly as in
[Set 01](QUESTIONS_01.md#getting-started). The tips there still apply:
press Enter on an empty line to finish a multi-line `def`, and remember
that transformations are lazy.

Two new tips for this set:

* After `groupByKey()`, each value is a `ResultIterable`, not a list. Printing
  it only shows something like `<pyspark.resultiterable.ResultIterable object at 0x...>`.
  Use `mapValues(list)` or `mapValues(sorted)` to see what's inside.
* `sortByKey()` gives `collect()` and `take(n)` a **fixed** order, so you no
  longer need `sorted()` around the result. But it sorts only by the **key**.
  If two elements have the same key, their order is still not fixed.

---

## Q1 (Basic) — map() vs flatMap()

**Input** (a Python list of sentences):

```python
sentences = ["the cat sat", "the dog ran fast", "a cat ran"]
```

**Tasks:**

1. Create an RDD from `sentences`. Use `map()` to split each sentence into
   words. Collect it and count it.
2. Now do the same split with `flatMap()`. Collect it and count it.
3. Find the unique words, sorted.
4. Use `map()` to find the **number of words** in each sentence.

**Expected output:**

```
[['the', 'cat', 'sat'], ['the', 'dog', 'ran', 'fast'], ['a', 'cat', 'ran']]
3
['the', 'cat', 'sat', 'the', 'dog', 'ran', 'fast', 'a', 'cat', 'ran']
10
['a', 'cat', 'dog', 'fast', 'ran', 'sat', 'the']
[3, 4, 3]
```

*Think about it:* why does task 1 give a count of 3 but task 2 a count of 10?

---

## Q2 (Basic) — Sorting products

**Input** (a Python list of `(product, price)` pairs):

```python
products = [("laptop", 1200), ("mouse", 25), ("monitor", 300),
            ("keyboard", 75), ("phone", 900), ("cable", 10)]
```

**Tasks:**

1. Sort the products **by name**, A to Z.
2. Sort them by name, Z to A.
3. Find the **3 most expensive** products, as `(price, product)` pairs.
4. Find the names of the **2 cheapest** products.

**Hint:** `sortByKey()` can only sort by the **key**. To sort by price, first
use `map()` to make the price the key.

**Expected output:**

```
[('cable', 10), ('keyboard', 75), ('laptop', 1200), ('monitor', 300), ('mouse', 25), ('phone', 900)]
[('phone', 900), ('mouse', 25), ('monitor', 300), ('laptop', 1200), ('keyboard', 75), ('cable', 10)]
[(1200, 'laptop'), (900, 'phone'), (300, 'monitor')]
['cable', 'mouse']
```

---

## Q3 (Basic) — Grouping employees by department

**Input** (a Python list of `(department, name)` pairs):

```python
staff = [("eng", "ann"), ("sales", "raj"), ("eng", "li"), ("hr", "sam"),
         ("sales", "eva"), ("eng", "tom"), ("hr", "mia")]
```

**Tasks:**

1. Use `groupByKey()` to group the employees by department. How many
   groups are there?
2. Show each department with a **sorted list** of its employees, with the
   departments sorted too.
3. Show the **number of employees** in each department (departments sorted).
4. Which departments have **more than 2** employees?
5. Get the same counts as task 3 again, this time with `reduceByKey()`
   instead of `groupByKey()`.

**Expected output:**

```
3
[('eng', ['ann', 'li', 'tom']), ('hr', ['mia', 'sam']), ('sales', ['eva', 'raj'])]
[('eng', 3), ('hr', 2), ('sales', 2)]
[('eng', 3)]
[('eng', 3), ('hr', 2), ('sales', 2)]
```

---

## Q4 (Intermediate) — Word count, with the top words

**Input:** [`data/quotes.txt`](data/quotes.txt), 5 lines of plain text with
mixed case and punctuation:

```
Spark is fast. Spark is fun!
Data is big, and big data needs Spark.
...
```

**Tasks:**

1. Count the lines. Then use `flatMap()` to split them into words, and count the words.
2. Clean each word: change it to lowercase and remove the punctuation
   `. , ! ? ; :` from both ends. `"Spark."` and `"spark"` should become the same word.
   Show the first 6 clean words.
3. Count each word. How many **different** words are there? Show the first 5
   in alphabetical order.
4. Show the **top 5** words by count, as `(word, count)`. If two words have
   the same count, put them in alphabetical order.

**Expected output:**

```
5
40
['spark', 'is', 'fast', 'spark', 'is', 'fun']
15
[('and', 2), ('big', 4), ('data', 5), ('fast', 1), ('fun', 1)]
[('the', 6), ('data', 5), ('is', 5), ('big', 4), ('spark', 4)]
```

**Hint for task 4:** a key can be a tuple, and tuples sort field by field.
What key would put the largest count first and break ties by the word?

---

## Q5 (Intermediate) — groupByKey() vs reduceByKey()

**Input:** [`data/transactions.txt`](data/transactions.txt). Each line is one
purchase, as `customer,amount`:

```
c1,20
c2,15
c1,35
...
```

**Tasks:**

1. Read the file into `(customer, amount)` pairs, with the amount as an `int`.
   Count them.
2. Find the **total amount per customer** with `reduceByKey()`, sorted by customer.
3. Find the same totals with `groupByKey()` instead. Check that both answers
   are equal.
4. Show each customer's amounts as a **sorted list**.
5. Find the **median amount per customer**. The median is the middle value of
   the sorted list; for an even number of values, it is the average of the two
   middle values. Return it as a `float`.

**Expected output:**

```
12
[('c1', 110), ('c2', 75), ('c3', 150), ('c4', 100)]
[('c1', 110), ('c2', 75), ('c3', 150), ('c4', 100)]
True
[('c1', [10, 20, 35, 45]), ('c2', [5, 15, 25, 30]), ('c3', [40, 50, 60]), ('c4', [100])]
[('c1', 27.5), ('c2', 20.0), ('c3', 50.0), ('c4', 100.0)]
```

*Think about it:* tasks 2 and 3 give the same answer. Which one is better for
a very large file, and why? Can the median in task 5 be computed with
`reduceByKey()`?

---

## Q6 (Intermediate) — Students per course

**Input:** [`data/enrollments.txt`](data/enrollments.txt). Each line is one
student and the courses they take, as `student:course1,course2,...`:

```
alice:math,physics,art
bob:math,history
...
```

**Tasks:**

1. Count the students. Turn each line into a `(student, [courses])` pair
   and show the first 2.
2. Use `flatMap()` to make one `(course, student)` pair **per enrollment**.
   Count the pairs and show the first 4.
3. For each course (courses sorted), show the sorted list of its students.
4. Show the **number of students per course**, largest first. If two courses
   have the same number, put them in alphabetical order.
5. Which students take **3 or more** courses?

**Expected output:**

```
6
[('alice', ['math', 'physics', 'art']), ('bob', ['math', 'history'])]
14
[('math', 'alice'), ('physics', 'alice'), ('art', 'alice'), ('math', 'bob')]
[('art', ['alice', 'carol', 'erin']), ('cs', ['carol', 'dan', 'erin']), ('history', ['bob', 'erin']), ('math', ['alice', 'bob', 'carol', 'frank']), ('physics', ['alice', 'carol'])]
[('math', 4), ('art', 3), ('cs', 3), ('history', 2), ('physics', 2)]
['alice', 'carol', 'erin']
```

---

## Q7 (Intermediate+) — Implement distinct() yourself

`distinct()` removes duplicate elements from an RDD. **Suppose `distinct()`
was not available. How would you implement it** using only the other
transformations in this set?

**Input:**

```python
nums = sc.parallelize([3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5])
```

and, for task 4, the `(date, user)` pairs of the successful (status `200`)
requests in [`data/web_logs.txt`](data/web_logs.txt), the same pairs as in
[Set 01, Q7](QUESTIONS_01.md#q7-intermediate--distinct-visitors-per-day).

**Tasks:**

1. Write a function `distinct_v1(rdd)` that returns the distinct elements of
   any RDD, using `map()` and **`reduceByKey()`** (no `distinct()`). Test it on
   `nums` and compare with `nums.distinct()`.
2. Write a second version, `distinct_v2(rdd)`, using **`groupByKey()`** instead.
3. Write `distinct_sorted(rdd)`, which returns the distinct elements **in sorted
   order**, so you don't need Python's `sorted()`.
4. Test your functions on the `(date, user)` pairs from the web logs. There are
   11 pairs; how many are distinct? Show them in sorted order.
5. Which version is better for large data: `distinct_v1` or `distinct_v2`? Why?

**Expected output** (tasks 1–4):

```
[1, 2, 3, 4, 5, 6, 9]
[1, 2, 3, 4, 5, 6, 9]
[1, 2, 3, 4, 5, 6, 9]
[1, 2, 3, 4, 5, 6, 9]
11
7
7
[('2026-10-01', 'alex'), ('2026-10-01', 'bob'), ('2026-10-02', 'alex'), ('2026-10-02', 'bob'), ('2026-10-02', 'dana'), ('2026-10-02', 'erin'), ('2026-10-03', 'carol')]
```

(The 4 lists are, in order: `nums.distinct()` sorted, `distinct_v1(nums)`
sorted, `distinct_v2(nums)` sorted, and `distinct_sorted(nums)`. The 3 counts
are: all pairs, `distinct_v1` pairs, and `distinct()` pairs.)

**Hint:** `reduceByKey()` and `groupByKey()` already bring equal **keys**
together. What if every element became a key?

---

## Q8 (Intermediate+) — Build an inverted index

An **inverted index** maps every word to the list of documents that contain
it. It's the main data structure behind search engines.

**Input:** [`data/documents.txt`](data/documents.txt). Each line is one
document, as `doc_id|text`:

```
d1|spark makes big data simple
d2|big data needs a big cluster
...
```

**Tasks:**

1. Count the documents. Use `flatMap()` to make one `(word, doc_id)` pair for
   **each word** in each document, and count the pairs.
2. Remove duplicate pairs (`big` appears **twice** in `d2`). How many pairs are left?
3. Build the inverted index: `(word, [sorted doc_ids])`, sorted by word. How
   many words does it have? Show the first 5 entries.
4. "Search" the index: which documents contain the word `cluster`?
5. Which words appear in **2 or more** documents?

**Expected output:**

```
4
22
21
14
[('a', ['d2', 'd3']), ('and', ['d4']), ('big', ['d1', 'd2']), ('cluster', ['d2', 'd3']), ('data', ['d1', 'd2', 'd4'])]
[('cluster', ['d2', 'd3'])]
[('a', ['d2', 'd3']), ('big', ['d1', 'd2']), ('cluster', ['d2', 'd3']), ('data', ['d1', 'd2', 'd4']), ('spark', ['d1', 'd3', 'd4'])]
```

*Think about it:* what would the entry for `big` look like if you skipped task 2?

---

## Q9 (Intermediate+) — Top 2 students per class

**Input:** [`data/class_scores.txt`](data/class_scores.txt). Each line is one
student's score, as `class,student,score`:

```
A,ann,88
A,ben,92
A,cal,75
A,dee,92
...
```

**Tasks:**

1. Read the file into `(class, (student, score))` pairs, with the score as an
   `int`. Show the first 2.
2. For each class (classes sorted), find the **top 2 students** by score, as a
   list of `(student, score)`. If two students have the same score, put them in
   alphabetical order.
3. "Flatten" the result back into one row per student, as
   `((class, rank), (student, score))` with rank 1 or 2, sorted by class and rank.
4. Print the rows from task 3 as a small report, one line per student.
5. Find the **average score per class**, rounded to 2 decimals.

**Expected output:**

```
[('A', ('ann', 88)), ('A', ('ben', 92))]
[('A', [('ben', 92), ('dee', 92)]), ('B', [('gus', 95), ('fay', 81)]), ('C', [('ivy', 85), ('kim', 85)])]
[(('A', 1), ('ben', 92)), (('A', 2), ('dee', 92)), (('B', 1), ('gus', 95)), (('B', 2), ('fay', 81)), (('C', 1), ('ivy', 85)), (('C', 2), ('kim', 85))]
A 1 ben 92
A 2 dee 92
B 1 gus 95
B 2 fay 81
C 1 ivy 85
C 2 kim 85
[('A', 86.75), ('B', 75.5), ('C', 79.5)]
```

**Hint for task 3:** `flatMap()` can take one `(class, [top students])` element
and return **several** elements, one per student. Python's `enumerate()` gives
you the rank.
