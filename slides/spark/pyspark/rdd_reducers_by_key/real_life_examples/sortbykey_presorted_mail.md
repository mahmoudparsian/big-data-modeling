# A Real-Life Case for `sortByKey()`: Printing Presorted Mail

## 1. The scenario

Every month, a credit-card company prints a paper statement for each of its
customers and mails it. The post office gives a **discount for presorted
mail**: if the envelopes arrive already in ZIP code order, the post office
doesn't have to sort them itself. For millions of statements a month, that
discount is real money.

The print shop has **3 printers**. Each printer should print one
**contiguous range** of ZIP codes, so that its stack of envelopes can go
straight into mail trays, in order: printer 1 prints the lowest ZIP codes,
printer 3 the highest.

Each statement is a `(zip_code, (customer, amount_due))` pair. The statements
come out of the billing system in **no particular order**.

| Input: statements, in billing order | Output: the **same** statements, in ZIP code order |
|---|---|
| `('95050', ('Chen', 120.0))` | `('02138', ('Murphy', 310.25))` |
| `('10001', ('Okafor', 45.1))` | `('10001', ('Okafor', 45.1))` |
| `('98101', ('Larsen', 9.5))` | `('10001', ('Bernstein', 19.99))` |
| `('02138', ('Murphy', 310.25))` | ... |
| ... | `('98101', ('Andersen', 99.0))` |

How is the order decided? ZIP codes are 5-character strings, so Python compares
them character by character, from left to right: `'02138' < '10001'` because
`'0' < '1'`, and `'95050' < '98101'` because `'5' < '8'` in the second
position. Since every ZIP code has exactly 5 digits, string order is the same
as numeric order.

**Notice what is different from all the reducer examples:** the output has
**every** input record, not one record per key. `'10001'` appears twice in the
output, because two customers live there. `sortByKey()` doesn't combine
anything; it only puts the records in order.


## 2. Why this is a job for `sortByKey()`

* **The sort key is already the key.** Each record is `(zip_code, statement)`,
  and we want to sort by `zip_code`. That is exactly what `sortByKey()` does.
* **Spark sorts in parallel, on all the machines**, so millions of statements
  never have to fit on one computer.
* **The output partitions are ranges.** `sortByKey()` puts the **smallest**
  keys in partition 0, the next ones in partition 1, and so on, and each
  partition is sorted. With `numPartitions=3`, each partition is the batch
  for one printer: **exactly** what the print shop needs.


## 3. Sample data

Start the PySpark shell (`./bin/pyspark`):

~~~python
>>> sc.version
'4.2.0'

>>> # (zip_code, (customer, amount_due))
>>> statements = [
...     ('95050', ('Chen',       120.00)),
...     ('10001', ('Okafor',      45.10)),
...     ('98101', ('Larsen',       9.50)),
...     ('02138', ('Murphy',     310.25)),
...     ('60601', ('Kowalski',    88.00)),
...     ('95050', ('Alvarez',     64.75)),
...     ('30301', ('Washington', 150.00)),
...     ('94105', ('Nakamura',   210.40)),
...     ('10001', ('Bernstein',   19.99)),
...     ('73301', ('Hernandez',   75.00)),
...     ('95050', ('Baker',       33.30)),
...     ('98101', ('Andersen',    99.00)),
... ]

>>> # statements_rdd : RDD[(zip_code, (customer, amount_due))]
>>> statements_rdd = sc.parallelize(statements, 2)
>>> statements_rdd.getNumPartitions()
2
~~~

(The ZIP codes are real: Cambridge MA, New York, Atlanta, Chicago, Austin,
San Francisco, Santa Clara, Seattle. The customers and amounts are made up.)


## 4. The `sortByKey()` solution

~~~python
>>> # by_zip : RDD[(zip_code, (customer, amount_due))], in ZIP code order
>>> by_zip = statements_rdd.sortByKey(numPartitions=3)
>>> for zip_code, (customer, amount) in by_zip.collect():
...     print(zip_code, f"{customer:<11} {amount:>7.2f}")
...
02138 Murphy       310.25
10001 Okafor        45.10
10001 Bernstein     19.99
30301 Washington   150.00
60601 Kowalski      88.00
73301 Hernandez     75.00
94105 Nakamura     210.40
95050 Chen         120.00
95050 Alvarez       64.75
95050 Baker         33.30
98101 Larsen         9.50
98101 Andersen      99.00
~~~

Here we did **not** need `sorted()` around `collect()`, as in all the reducer
examples: the RDD itself is in order, so `collect()` returns the records in
order.

And here are the 3 printers' batches. `glom()` shows each partition as a list:

~~~python
>>> for printer, batch in enumerate(by_zip.glom().collect(), start=1):
...     zips = [zip_code for zip_code, statement in batch]
...     print(f"printer {printer}: {len(batch)} statements, ZIP {zips[0]} to {zips[-1]}")
...
printer 1: 5 statements, ZIP 02138 to 60601
printer 2: 5 statements, ZIP 73301 to 95050
printer 3: 2 statements, ZIP 98101 to 98101
~~~

Each printer gets one contiguous range, and the ranges follow each other:
printer 1's last ZIP code (`60601`) is smaller than printer 2's first
(`73301`). If the job wrote its output with `by_zip.saveAsTextFile(...)`, the
files `part-00000`, `part-00001`, `part-00002` would be the three printers'
batches, in order.

Notice also that customers in the **same** ZIP code (`Okafor` and
`Bernstein`, or the three in `95050`) are in no particular order: only the
key is sorted. Section 6 fixes that.


## 5. How `sortByKey()` works

`sortByKey()` is a two-step job (`$SPARK_HOME/python/pyspark/core/rdd.py`):

1. **Find the boundaries.** Spark takes a sample of the keys, sorts the
   sample, and picks `numPartitions - 1` boundary keys that split the sample
   into roughly equal parts. (Our RDD is small, so the sample is every key.)
   Then every record is sent to the partition for its key range. This is a
   **shuffle**, with a *range partitioner* instead of the hash partitioner
   that the reducers use.
2. **Sort inside each partition.** Each partition is sorted on its own, in
   parallel.

Here is step 1 by hand, for our 12 keys and 3 partitions:

~~~python
>>> keys = sorted(zip_code for zip_code, statement in statements)
>>> keys
['02138', '10001', '10001', '30301', '60601', '73301', '94105', '95050', '95050', '95050', '98101', '98101']

>>> # the boundaries: the keys at 1/3 and 2/3 of the sorted sample
>>> bounds = [keys[int(len(keys) * 1 / 3)], keys[int(len(keys) * 2 / 3)]]
>>> bounds
['60601', '95050']
~~~

Keys up to `'60601'` go to partition 0, keys after that and up to `'95050'`
to partition 1, and the rest to partition 2. That's exactly the batches we
saw. The partitions are only **roughly** equal: all three `95050` statements
must go to the same partition, so printer 3 got only 2. With millions of
statements and thousands of ZIP codes, the batches come out much more even.


## 6. Sorting by more than one field, and in descending order

### Within a ZIP code, by customer name

To sort by ZIP code **and then** by customer name, make the key a **tuple**
`(zip_code, customer)`. Python compares tuples field by field, so
`sortByKey()` sorts by the first field, and breaks ties with the second:

~~~python
>>> # by_zip_name : RDD[((zip_code, customer), amount_due)]
>>> by_zip_name = (statements_rdd
...                .map(lambda kv: ((kv[0], kv[1][0]), kv[1][1]))
...                .sortByKey(numPartitions=3))
>>> for (zip_code, customer), amount in by_zip_name.collect():
...     print(zip_code, f"{customer:<11} {amount:>7.2f}")
...
02138 Murphy       310.25
10001 Bernstein     19.99
10001 Okafor        45.10
30301 Washington   150.00
60601 Kowalski      88.00
73301 Hernandez     75.00
94105 Nakamura     210.40
95050 Alvarez       64.75
95050 Baker         33.30
95050 Chen         120.00
98101 Andersen      99.00
98101 Larsen         9.50
~~~

Now `Bernstein` comes before `Okafor`, and `Alvarez`, `Baker`, `Chen` are in
order. (One side effect: with this key, a single ZIP code can be split
between two printers, because the boundaries are now `(zip, name)` pairs.
Check `by_zip_name.glom().collect()`: `95050` starts on printer 2 and ends on
printer 3. That's fine here, because the trays stay in order.)

### By amount due, largest first

The collections department wants a different report: all statements, from the
**largest** amount due to the smallest. Now the sort key is the amount, so
we put the amount in the key, and pass `ascending=False`:

~~~python
>>> by_amount = (statements_rdd
...              .map(lambda kv: (kv[1][1], (kv[1][0], kv[0])))   # (amount, (customer, zip))
...              .sortByKey(ascending=False, numPartitions=3))
>>> by_amount.take(3)
[(310.25, ('Murphy', '02138')), (210.4, ('Nakamura', '94105')), (150.0, ('Washington', '30301'))]
~~~

PySpark also has `sortBy(keyfunc)`, which sorts any RDD by a key **you
compute**, without building `(key, value)` pairs yourself:

~~~python
>>> top3 = statements_rdd.sortBy(lambda kv: kv[1][1], ascending=False, numPartitions=3).take(3)
>>> top3
[('02138', ('Murphy', 310.25)), ('94105', ('Nakamura', 210.4)), ('30301', ('Washington', 150.0))]
~~~

And `sortBy()` is just `sortByKey()` in disguise. In PySpark's source it is
`self.keyBy(keyfunc).sortByKey(ascending, numPartitions).values()`: build
the key, sort by it, and throw the key away again. So use `sortByKey()`
when your records are already `(sort_key, value)` pairs, and `sortBy()` when
they aren't.


## 7. Why not `collect()` + `sorted()`, or `takeOrdered()`?

### `collect()` + Python's `sorted()`

~~~python
>>> sorted(statements_rdd.collect()) == sorted(by_zip.collect())
True
~~~

For 12 statements, sure. But `collect()` brings **every** record to the
driver, one machine, and sorts there, alone. For 50 million statements
that's slow, and the driver will likely run out of memory. With
`sortByKey()`, the data stays spread over the cluster, every machine sorts
its part in parallel, and the result is still an RDD that you can save,
print in batches, or keep processing.

### `takeOrdered()`

`takeOrdered(n, key)` returns the **first `n`** records in order, without
sorting everything. It's the right tool for "the 3 largest amounts due":

~~~python
>>> statements_rdd.takeOrdered(3, key=lambda kv: -kv[1][1])
[('02138', ('Murphy', 310.25)), ('94105', ('Nakamura', 210.4)), ('30301', ('Washington', 150.0))]
~~~

But the print shop needs **all** statements in order, not the top 3. When the
whole RDD must be in order, use `sortByKey()`.

### The reducers

`reduceByKey()`, `groupByKey()`, and the others **do not sort**. Their result
comes out in hash-partition order, which looks random. That's why every
reducer example in this folder wrapped its results in `sorted(...)`. If a
reducer's result must be in key order, add a `sortByKey()` after it:

~~~python
>>> # number of statements per ZIP code, in ZIP code order
>>> counts = statements_rdd.mapValues(lambda s: 1).reduceByKey(lambda a, b: a + b)
>>> counts.sortByKey().collect()
[('02138', 1), ('10001', 2), ('30301', 1), ('60601', 1), ('73301', 1), ('94105', 1), ('95050', 3), ('98101', 2)]
~~~


## 8. A common mistake: sorting numbers that are stored as strings

Data often arrives as text, for example from a CSV file. If the amounts are
still **strings**, `sortByKey()` compares them as strings, character by
character, and the "largest first" report goes wrong:

~~~python
>>> as_text = statements_rdd.map(lambda kv: (f"{kv[1][1]:.2f}", kv[1][0]))   # ('120.00', 'Chen'), ...
>>> as_text.sortByKey(ascending=False, numPartitions=3).keys().take(5)
['99.00', '9.50', '88.00', '75.00', '64.75']
~~~

`'99.00'` comes first because `'9'` is the largest **first character**, and
`'310.25'` (the real largest amount) comes much later, because it starts with
`'3'`. The fix is to convert the key to a number before sorting:

~~~python
>>> as_text.map(lambda kv: (float(kv[0]), kv[1])).sortByKey(ascending=False, numPartitions=3).keys().take(5)
[310.25, 210.4, 150.0, 120.0, 99.0]
~~~

**But the opposite is true for ZIP codes**: keep them as strings. A ZIP code is
a label that happens to use digits, not a number. Converting it to an integer
throws away its leading zero:

~~~python
>>> int('02138')
2138
~~~

and `2138` is not a valid ZIP code (it has only 4 digits), so the envelope
for Cambridge, MA would be printed with the wrong ZIP code. Because all ZIP
codes have the same length (5), sorting them as strings already gives the
right order.

The rule: **sort by the type that has the order you want.** Amounts and ages
are numbers; ZIP codes, phone numbers, and account numbers are fixed-width
strings.


## 9. Takeaways

`sortByKey()` is not a reducer: it never combines records, it only orders
them. The output has exactly as many records as the input.

| Use ... | When ... | Example |
|---|---|---|
| `sortByKey()` | the **whole** RDD must be in order, and the records are already `(sort_key, value)` pairs | statements in ZIP code order (this example) |
| `sortBy(keyfunc)` | the whole RDD must be in order, by a key you compute from each record | statements by amount due |
| `takeOrdered(n, key)` | you only need the **first `n`** records in order | the 3 largest amounts due |
| `collect()` + `sorted()` | the result is small enough for one machine (for example, after a reducer) | the reducer examples in this folder |

Other real-life jobs with the same shape (all records, in key order, often
split into ranges):

* **Bank statement files** in account-number order, for a downstream system
  that reads them in order.
* **An audit log** of all transactions in time order (key: the timestamp),
  for the auditors.
* **A printed phone directory or catalog**, in alphabetical order, split
  into volumes (A to F, G to M, ...): one partition per volume.
* **A full leaderboard** of all players by score (descending), split into
  pages.

For the reducers (`reduceByKey()`, `foldByKey()`, `groupByKey()`,
`aggregateByKey()`, `combineByKey()`), see the other examples in
[this folder](README.md).
