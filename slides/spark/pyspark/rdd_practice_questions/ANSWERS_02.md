# PySpark RDD Practice Questions — Set 02: Answers

Solutions to the nine [Set 02 practice questions](QUESTIONS_02.md). Every
session below was run in the `bin/pyspark` shell (Spark 4.2.0), started from
this folder. The output is copied from those real runs.

Try the questions yourself first. There are often several correct
solutions, and yours doesn't need to match these exactly.

* [Q1 (Basic): map() vs flatMap()](#q1-basic--map-vs-flatmap)
* [Q2 (Basic): Sorting products](#q2-basic--sorting-products)
* [Q3 (Basic): Grouping employees by department](#q3-basic--grouping-employees-by-department)
* [Q4 (Intermediate): Word count, with the top words](#q4-intermediate--word-count-with-the-top-words)
* [Q5 (Intermediate): groupByKey() vs reduceByKey()](#q5-intermediate--groupbykey-vs-reducebykey)
* [Q6 (Intermediate): Students per course](#q6-intermediate--students-per-course)
* [Q7 (Intermediate+): Implement distinct() yourself](#q7-intermediate--implement-distinct-yourself)
* [Q8 (Intermediate+): Build an inverted index](#q8-intermediate--build-an-inverted-index)
* [Q9 (Intermediate+): Top 2 students per class](#q9-intermediate--top-2-students-per-class)

---

## Q1 (Basic) — map() vs flatMap()

```python
>>> sentences = ["the cat sat", "the dog ran fast", "a cat ran"]
>>> rdd = sc.parallelize(sentences)

>>> mapped = rdd.map(lambda s: s.split(" "))
>>> mapped.collect()
[['the', 'cat', 'sat'], ['the', 'dog', 'ran', 'fast'], ['a', 'cat', 'ran']]
>>> mapped.count()
3

>>> words = rdd.flatMap(lambda s: s.split(" "))
>>> words.collect()
['the', 'cat', 'sat', 'the', 'dog', 'ran', 'fast', 'a', 'cat', 'ran']
>>> words.count()
10

>>> sorted(words.distinct().collect())
['a', 'cat', 'dog', 'fast', 'ran', 'sat', 'the']

>>> rdd.map(lambda s: len(s.split(" "))).collect()
[3, 4, 3]
```

**Notes:**

* `map()` is **one-to-one**: 3 sentences in, 3 elements out. Each element is
  a *list* of words.
* `flatMap()` is **one-to-many**: the function returns a list, and Spark
  "flattens" it, so every word becomes its own element. 3 sentences in,
  10 words out. A sentence that splits into an empty list adds nothing.
* Use `map()` when you want one result per input (like the word count of
  each sentence in task 4). Use `flatMap()` when one input should become
  several elements (like the words of a sentence).

---

## Q2 (Basic) — Sorting products

```python
>>> products = [("laptop", 1200), ("mouse", 25), ("monitor", 300),
...             ("keyboard", 75), ("phone", 900), ("cable", 10)]
>>> prod = sc.parallelize(products)

>>> prod.sortByKey().collect()
[('cable', 10), ('keyboard', 75), ('laptop', 1200), ('monitor', 300), ('mouse', 25), ('phone', 900)]
>>> prod.sortByKey(ascending=False).collect()
[('phone', 900), ('mouse', 25), ('monitor', 300), ('laptop', 1200), ('keyboard', 75), ('cable', 10)]

>>> by_price = prod.map(lambda kv: (kv[1], kv[0]))
>>> by_price.sortByKey(ascending=False).take(3)
[(1200, 'laptop'), (900, 'phone'), (300, 'monitor')]
>>> by_price.sortByKey().map(lambda pn: pn[1]).take(2)
['cable', 'mouse']
```

**Notes:**

* `sortByKey()` sorts **pair RDDs by their key**, ascending by default.
  Pass `ascending=False` for the reverse order.
* To sort by the value, **swap** the pair with `map()` so the value becomes
  the key. This "swap, then sort" trick is used again in Q4 and Q6.
* `sortByKey()` followed by `take(n)` is the RDD way to say "the top *n*".
  `take(n)` returns only the first *n* elements to the driver, while
  `collect()` returns everything.

---

## Q3 (Basic) — Grouping employees by department

```python
>>> staff = [("eng", "ann"), ("sales", "raj"), ("eng", "li"), ("hr", "sam"),
...          ("sales", "eva"), ("eng", "tom"), ("hr", "mia")]
>>> staff_rdd = sc.parallelize(staff)

>>> grouped = staff_rdd.groupByKey()
>>> grouped.count()
3

>>> grouped.mapValues(sorted).sortByKey().collect()
[('eng', ['ann', 'li', 'tom']), ('hr', ['mia', 'sam']), ('sales', ['eva', 'raj'])]

>>> grouped.mapValues(len).sortByKey().collect()
[('eng', 3), ('hr', 2), ('sales', 2)]

>>> grouped.mapValues(len).filter(lambda kv: kv[1] > 2).collect()
[('eng', 3)]

>>> staff_rdd.mapValues(lambda n: 1).reduceByKey(lambda a, b: a + b).sortByKey().collect()
[('eng', 3), ('hr', 2), ('sales', 2)]
```

**Notes:**

* `groupByKey()` turns `(key, value)` pairs into **one** element per key:
  `(key, all values for that key)`. 7 pairs became 3 groups.
* The grouped values are a `ResultIterable`, not a list. `mapValues(sorted)`
  turns each one into a sorted list, and `mapValues(len)` counts it. The
  order of the values inside a group is **not** fixed, so sort them if
  you need a stable answer.
* Task 5 gives the same counts with `reduceByKey()`, the counting pattern
  from Set 01. Q5 explains when each one is the better choice.

---

## Q4 (Intermediate) — Word count, with the top words

```python
>>> lines = sc.textFile("data/quotes.txt")
>>> lines.count()
5

>>> words = lines.flatMap(lambda line: line.split())
>>> words.count()
40

>>> clean = words.map(lambda w: w.lower().strip(".,!?;:")).filter(lambda w: w != "")
>>> clean.take(6)
['spark', 'is', 'fast', 'spark', 'is', 'fun']

>>> counts = clean.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
>>> counts.count()
15
>>> counts.sortByKey().take(5)
[('and', 2), ('big', 4), ('data', 5), ('fast', 1), ('fun', 1)]

>>> top = (counts.map(lambda wc: ((-wc[1], wc[0]), wc))
...              .sortByKey()
...              .map(lambda kv: kv[1]))
>>> top.take(5)
[('the', 6), ('data', 5), ('is', 5), ('big', 4), ('spark', 4)]
```

**Notes:**

* This is the classic **word count**: `flatMap` (lines → words), `map`
  (word → `(word, 1)`), and `reduceByKey` (add the 1s).
* `line.split()` with no argument splits on any run of spaces, so you never
  get empty strings between words. The `filter(lambda w: w != "")` is still a good
  safety net: a "word" made only of punctuation becomes `""` after `strip()`.
* **Ties in task 4.** Just swapping to `(count, word)` and calling
  `sortByKey(ascending=False)` sorts by the count **only**. `data` and `is`
  (both 5) could then come out in either order. The fix is a **tuple key**
  `(-count, word)`. Tuples sort field by field, so `-count` puts the largest
  count first, and `word` breaks ties alphabetically. The final `map()`
  throws the sort key away and keeps the original `(word, count)`.

---

## Q5 (Intermediate) — groupByKey() vs reduceByKey()

```python
>>> tx = (sc.textFile("data/transactions.txt")
...         .map(lambda line: line.split(","))
...         .map(lambda f: (f[0], int(f[1]))))
>>> tx.count()
12

>>> t1 = tx.reduceByKey(lambda a, b: a + b).sortByKey()
>>> t1.collect()
[('c1', 110), ('c2', 75), ('c3', 150), ('c4', 100)]

>>> t2 = tx.groupByKey().mapValues(sum).sortByKey()
>>> t2.collect()
[('c1', 110), ('c2', 75), ('c3', 150), ('c4', 100)]
>>> t1.collect() == t2.collect()
True

>>> tx.groupByKey().mapValues(sorted).sortByKey().collect()
[('c1', [10, 20, 35, 45]), ('c2', [5, 15, 25, 30]), ('c3', [40, 50, 60]), ('c4', [100])]

>>> def median(values):
...     v = sorted(values)
...     n = len(v)
...     mid = n // 2
...     if n % 2 == 1:
...         return float(v[mid])
...     return (v[mid - 1] + v[mid]) / 2
...
>>> tx.groupByKey().mapValues(median).sortByKey().collect()
[('c1', 27.5), ('c2', 20.0), ('c3', 50.0), ('c4', 100.0)]
```

**Notes:**

* **For sums, counts, min, and max, prefer `reduceByKey()`.** It adds values up
  *inside each partition first*, and then sends only one partial total per key
  across the network (the **shuffle**). `groupByKey()` sends **every single
  value** across the network and only adds them up afterwards. With a big file, that's much
  more data to move, and one very common key can run out of memory.
* **Use `groupByKey()` when you really need all the values together.** A
  median needs the whole sorted list. You can't combine "the median of these
  values" with "the median of those values" to get the overall median, so
  `reduceByKey()` doesn't work here.
* `mapValues()` accepts any function that takes the grouped values,
  including built-ins like `sum`, `len`, and `sorted`, and your own `def`
  like `median`.
* For more on this topic, see the
  [groupByKey vs reduceByKey guide](../pyspark_groupbykey_vs_reducebykey_guide_2.md).

---

## Q6 (Intermediate) — Students per course

```python
>>> raw = sc.textFile("data/enrollments.txt")
>>> raw.count()
6

>>> student_courses = raw.map(lambda line: line.split(":")).map(lambda f: (f[0], f[1].split(",")))
>>> student_courses.take(2)
[('alice', ['math', 'physics', 'art']), ('bob', ['math', 'history'])]

>>> course_student = student_courses.flatMap(lambda kv: [(c, kv[0]) for c in kv[1]])
>>> course_student.count()
14
>>> course_student.take(4)
[('math', 'alice'), ('physics', 'alice'), ('art', 'alice'), ('math', 'bob')]

>>> course_student.groupByKey().mapValues(sorted).sortByKey().collect()
[('art', ['alice', 'carol', 'erin']), ('cs', ['carol', 'dan', 'erin']), ('history', ['bob', 'erin']), ('math', ['alice', 'bob', 'carol', 'frank']), ('physics', ['alice', 'carol'])]

>>> enroll = course_student.mapValues(lambda s: 1).reduceByKey(lambda a, b: a + b)
>>> (enroll.map(lambda kv: ((-kv[1], kv[0]), kv))
...        .sortByKey()
...        .map(lambda kv: kv[1])
...        .collect())
[('math', 4), ('art', 3), ('cs', 3), ('history', 2), ('physics', 2)]

>>> student_courses.filter(lambda kv: len(kv[1]) >= 3).map(lambda kv: kv[0]).collect()
['alice', 'carol', 'erin']
```

**Notes:**

* A `flatMap()` function can return **pairs**: one `(course, student)` for
  each course in the list. 6 students became 14 enrollments.
* This also **inverts** the relationship. The input is organized by student,
  and the output is organized by course. Q8 uses the same idea to build an
  inverted index.
* Task 4 uses the same `(-count, name)` tuple-key trick as Q4, so `art` and
  `cs` (both 3) come out alphabetically.
* Task 5 doesn't need a shuffle. Each student's courses are already together
  on one line, so `filter()` and `map()` are enough.

---

## Q7 (Intermediate+) — Implement distinct() yourself

The idea: `reduceByKey()` and `groupByKey()` already bring together all the
elements with the **same key**, and leave exactly **one** element per key.
So make **each element its own key**, with a dummy value such as `None`, let
Spark group the duplicates, and then keep just the keys.

```python
>>> nums = sc.parallelize([3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5])
>>> sorted(nums.distinct().collect())
[1, 2, 3, 4, 5, 6, 9]

>>> def distinct_v1(rdd):
...     return (rdd.map(lambda x: (x, None))
...                .reduceByKey(lambda a, b: a)
...                .map(lambda kv: kv[0]))
...
>>> sorted(distinct_v1(nums).collect())
[1, 2, 3, 4, 5, 6, 9]

>>> def distinct_v2(rdd):
...     return (rdd.map(lambda x: (x, None))
...                .groupByKey()
...                .map(lambda kv: kv[0]))
...
>>> sorted(distinct_v2(nums).collect())
[1, 2, 3, 4, 5, 6, 9]

>>> def distinct_sorted(rdd):
...     return (rdd.map(lambda x: (x, None))
...                .reduceByKey(lambda a, b: a)
...                .sortByKey()
...                .map(lambda kv: kv[0]))
...
>>> distinct_sorted(nums).collect()
[1, 2, 3, 4, 5, 6, 9]
```

Task 4, on the `(date, user)` pairs from the web logs:

```python
>>> logs = sc.textFile("data/web_logs.txt").map(lambda line: line.split(","))
>>> date_user = logs.filter(lambda f: f[3] == "200").map(lambda f: (f[0], f[1]))
>>> date_user.count()
11
>>> distinct_v1(date_user).count()
7
>>> date_user.distinct().count()
7
>>> distinct_sorted(date_user).collect()
[('2026-10-01', 'alex'), ('2026-10-01', 'bob'), ('2026-10-02', 'alex'), ('2026-10-02', 'bob'), ('2026-10-02', 'dana'), ('2026-10-02', 'erin'), ('2026-10-03', 'carol')]
```

**Notes:**

* **`distinct_v1` is better** (task 5). Each partition first reduces its own
  duplicates to one `(x, None)` before the shuffle, so an element repeated a
  million times crosses the network at most once per partition.
  `distinct_v2` (`groupByKey()`) sends **every** copy across the network, and
  then just throws the grouped values away. This is the same trade-off as in Q5.
* `distinct_v1` is exactly how PySpark itself implements `distinct()`. This
  is the code in `pyspark/core/rdd.py` (Spark 4.2.0):

  ```python
  return (
      self.map(lambda x: (x, None))
      .reduceByKey(lambda x, _: x, numPartitions)
      .map(lambda x: x[0])
  )
  ```

  That's also why `distinct()` causes a shuffle, and why its output order is
  not fixed.
* `distinct_sorted` makes the **whole element** the key, so `sortByKey()`
  sorts by the whole `(date, user)` tuple. If you call `distinct_v1(date_user).sortByKey()`
  instead, the sort uses the pair's own key, the **date** only, and the users
  within each date come out in no fixed order.
* Any element type that can be a key works: numbers, strings, and tuples
  (as in task 4). Lists can't be keys because they aren't hashable, so
  `distinct()` doesn't work on an RDD of lists either. Convert them to tuples first.

---

## Q8 (Intermediate+) — Build an inverted index

```python
>>> docs = sc.textFile("data/documents.txt").map(lambda line: line.split("|"))
>>> docs.count()
4

>>> word_doc = docs.flatMap(lambda d: [(w, d[0]) for w in d[1].split()])
>>> word_doc.count()
22

>>> unique_word_doc = word_doc.distinct()
>>> unique_word_doc.count()
21

>>> index = unique_word_doc.groupByKey().mapValues(sorted).sortByKey()
>>> index.count()
14
>>> index.take(5)
[('a', ['d2', 'd3']), ('and', ['d4']), ('big', ['d1', 'd2']), ('cluster', ['d2', 'd3']), ('data', ['d1', 'd2', 'd4'])]

>>> index.filter(lambda kv: kv[0] == "cluster").collect()
[('cluster', ['d2', 'd3'])]

>>> index.filter(lambda kv: len(kv[1]) >= 2).collect()
[('a', ['d2', 'd3']), ('big', ['d1', 'd2']), ('cluster', ['d2', 'd3']), ('data', ['d1', 'd2', 'd4']), ('spark', ['d1', 'd3', 'd4'])]
```

**Notes:**

* `line.split("|")` gives `[doc_id, text]`, and then `flatMap()` makes
  one `(word, doc_id)` pair for each word, the same "invert with flatMap" idea as in Q6.
* `distinct()` here is what keeps `d2` from showing up twice for `big`.
  Without it, the entry would be `('big', ['d1', 'd2', 'd2'])`:

  ```python
  >>> word_doc.groupByKey().mapValues(sorted).filter(lambda kv: kv[0] == "big").collect()
  [('big', ['d1', 'd2', 'd2'])]
  ```

  (Counting the duplicates is useful too. A real search engine uses it to rank
  documents. But it's not what an inverted index lists.)
* `groupByKey()` is the right tool here, because we really want the **full
  list** of documents for each word, not a single combined number.
* `index.filter(...)` is a full scan of the index, which is fine for a
  practice question. Real search engines store the index so that looking up
  one word is fast.

---

## Q9 (Intermediate+) — Top 2 students per class

```python
>>> rows = sc.textFile("data/class_scores.txt").map(lambda line: line.split(","))
>>> by_class = rows.map(lambda f: (f[0], (f[1], int(f[2]))))
>>> by_class.take(2)
[('A', ('ann', 88)), ('A', ('ben', 92))]

>>> def top2(students):
...     return sorted(students, key=lambda s: (-s[1], s[0]))[:2]
...
>>> tops = by_class.groupByKey().mapValues(top2).sortByKey()
>>> tops.collect()
[('A', [('ben', 92), ('dee', 92)]), ('B', [('gus', 95), ('fay', 81)]), ('C', [('ivy', 85), ('kim', 85)])]

>>> ranked = (tops.flatMap(lambda kv: [((kv[0], i + 1), s) for i, s in enumerate(kv[1])])
...               .sortByKey())
>>> ranked.collect()
[(('A', 1), ('ben', 92)), (('A', 2), ('dee', 92)), (('B', 1), ('gus', 95)), (('B', 2), ('fay', 81)), (('C', 1), ('ivy', 85)), (('C', 2), ('kim', 85))]

>>> for (cls, rank), (student, score) in ranked.collect():
...     print(cls, rank, student, score)
...
A 1 ben 92
A 2 dee 92
B 1 gus 95
B 2 fay 81
C 1 ivy 85
C 2 kim 85

>>> (by_class.mapValues(lambda s: (s[1], 1))
...          .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
...          .mapValues(lambda t: round(t[0] / t[1], 2))
...          .sortByKey()
...          .collect())
[('A', 86.75), ('B', 75.5), ('C', 79.5)]
```

**Notes:**

* **Top-N per group** is a classic pattern: `groupByKey()`, then `mapValues()`
  with a function that sorts the group and keeps the first N.
* `top2` sorts by `(-score, name)`: highest score first, and alphabetical among
  ties. With only `-score`, `ben` and `dee` (both 92) could come out in either
  order, depending on the order Spark happened to group them in.
* `flatMap()` "un-groups" the result. One `(class, [2 students])` element becomes
  2 rows, and `enumerate()` gives each row its rank. The key `(class, rank)` makes
  `sortByKey()` order the rows by class first, then by rank.
* The `for` loop runs in **plain Python** on the driver, over the list that
  `collect()` returned. That's fine for printing a small result.
* `groupByKey()` is fine here because each class is small. For huge groups,
  you'd use `reduceByKey()` with a function that merges two "top 2" lists into a
  new "top 2" list, so no group is ever held in memory all at once.
* Task 5 is the `(sum, count)` average pattern from
  [Set 01, Q5](ANSWERS_01.md#q5-intermediate--average-score-per-student).
