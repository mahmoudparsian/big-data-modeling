# A Real-Life Case for `foldByKey()`: Election-Night Vote Totals

## 1. The scenario

On election night, every precinct (a neighborhood polling place) sends its
**tally** to the state election office: how many votes each candidate got
there. A tally is a dictionary `{candidate: votes}`. The office publishes a
running total for every county:

```
Santa Clara   Patel 1,385   Lee 1,235   Rivera 348   write-in 4
```

Each report is a `(county, tally)` pair. Reports arrive from many precincts
and are spread over many partitions. A precinct only lists the candidates who
got votes there, so some tallies have fewer names (or an extra `write-in`).

For example, here are **all three** precinct reports from Santa Clara
county, and the **one** county record we want:

| Input: one record per **precinct** | Output: one record per **county** |
|---|---|
| `('Santa Clara', {'Lee': 410, 'Patel': 385, 'Rivera': 120})` | `('Santa Clara', {'Lee': 1235, 'Patel': 1385, 'Rivera': 348, 'write-in': 4})` |
| `('Santa Clara', {'Lee': 530, 'Patel': 760, 'Rivera': 140})` | |
| `('Santa Clara', {'Lee': 295, 'Patel': 240, 'Rivera': 88, 'write-in': 4})` | |

| Output field | Value | How it is computed from the 3 precinct tallies |
|---|---|---|
| `Lee` | `1235` | `410 + 530 + 295` |
| `Patel` | `1385` | `385 + 760 + 240` |
| `Rivera` | `348` | `120 + 140 + 88` |
| `write-in` | `4` | only the third precinct reported write-in votes: `4` |

Notice that the input value and the output value are the **same kind of
thing**: a precinct tally and a county tally are both `{candidate: votes}`
dictionaries. A county total is just a bigger tally.


## 2. Why this is a job for `foldByKey()`

Two facts about this job point to `foldByKey()`:

1. **The value and the result have the same type** (a tally), and one rule
   combines any two tallies: *add the votes candidate by candidate*. That is
   what `reduceByKey()` needs too.
2. **There is a natural empty tally**: `{}`, "no votes counted yet". It is
   exactly what every county shows on TV before the first precinct reports.
   And adding the empty tally to any tally doesn't change it.

`foldByKey(zeroValue, func)` is built for exactly this: `reduceByKey()`
**plus** a starting value. The name comes from functional programming:
"folding" a list means starting from an initial value and combining the
elements into it one by one.

| Business rule | `foldByKey()` argument | Type |
|---|---|---|
| The empty tally, before any votes are counted | `zeroValue` | `V` |
| Add two tallies, candidate by candidate | `func` | `(V, V) → V` |

(Compare: `aggregateByKey()` also has a zero value, but it needs **two**
functions, because its result type differs from the value type. Here they
are the same, so one function is enough.)


## 3. Sample data

Start the PySpark shell (`./bin/pyspark`). We use **2 partitions** so the
output is the same on every machine:

~~~python
>>> sc.version
'4.2.0'

>>> # (county, {candidate: votes})
>>> reports = [
...     ('Santa Clara', {'Lee': 410, 'Patel': 385, 'Rivera': 120}),
...     ('Alameda',     {'Lee': 700, 'Patel': 640, 'Rivera': 300}),
...     ('Santa Clara', {'Lee': 530, 'Patel': 760, 'Rivera': 140}),
...     ('Alameda',     {'Lee': 510, 'Patel': 560, 'Rivera': 210, 'write-in': 7}),
...     ('Santa Clara', {'Lee': 295, 'Patel': 240, 'Rivera': 88, 'write-in': 4}),
...     ('San Mateo',   {'Lee': 220, 'Patel': 260, 'Rivera': 95}),
...     ('San Mateo',   {'Lee': 180, 'Patel': 150, 'Rivera': 60}),
...     ('San Mateo',   {'Lee': 140, 'Patel': 300}),
... ]

>>> # reports_rdd : RDD[(county, tally)]
>>> reports_rdd = sc.parallelize(reports, 2)
>>> for i, part in enumerate(reports_rdd.glom().collect()):
...     print("partition", i, ":", [county for county, tally in part])
...
partition 0 : ['Santa Clara', 'Alameda', 'Santa Clara', 'Alameda']
partition 1 : ['Santa Clara', 'San Mateo', 'San Mateo', 'San Mateo']
~~~

`Santa Clara` has reports in **both** partitions; `Alameda` only in
partition 0, and `San Mateo` only in partition 1. (All vote counts in this
example are made up.)


## 4. The `foldByKey()` solution

~~~python
>>> # Add tally2 into tally1, candidate by candidate: (V, V) -> V
>>> def add_tallies(tally1, tally2):
...     for candidate, votes in tally2.items():
...         tally1[candidate] = tally1.get(candidate, 0) + votes
...     return tally1
...

>>> # county_totals : RDD[(county, {candidate: votes})]
>>> county_totals = reports_rdd.foldByKey({}, add_tallies)
~~~

That's the whole job: one empty tally and one function. Now print the
results, with candidates ordered from most to fewest votes (as on TV):

~~~python
>>> def show(name, tally):
...     ranked = sorted(tally.items(), key=lambda cv: (-cv[1], cv[0]))
...     print(f"{name:12}", "   ".join(f"{c} {v:,}" for c, v in ranked))
...

>>> for county, tally in sorted(county_totals.collect()):
...     show(county, tally)
...
Alameda      Lee 1,210   Patel 1,200   Rivera 510   write-in 7
San Mateo    Patel 710   Lee 540   Rivera 155
Santa Clara  Patel 1,385   Lee 1,235   Rivera 348   write-in 4
~~~

### Bonus: the statewide total, with the same zero and the same function

The statewide total is the sum of the county tallies. `RDD.fold()` (the
version without keys) folds all values of an RDD into one result, and it
takes the **same** empty tally and the **same** `add_tallies`:

~~~python
>>> statewide = county_totals.values().fold({}, add_tallies)
>>> show("Statewide", statewide)
Statewide    Patel 3,295   Lee 2,985   Rivera 1,013   write-in 11
~~~


## 5. Step by step, for `Santa Clara`

`Santa Clara` has 2 reports in partition 0 and 1 report in partition 1.
**Each partition starts from its own copy of the empty tally**:

| Step | Partition 0 | Partition 1 |
|---|---|---|
| start (`zeroValue`) | `{}` | `{}` |
| `add_tallies` | `+ {Lee: 410, Patel: 385, Rivera: 120}` → `{Lee: 410, Patel: 385, Rivera: 120}` | `+ {Lee: 295, Patel: 240, Rivera: 88, write-in: 4}` → `{Lee: 295, Patel: 240, Rivera: 88, write-in: 4}` |
| `add_tallies` | `+ {Lee: 530, Patel: 760, Rivera: 140}` → `{Lee: 940, Patel: 1145, Rivera: 260}` | (no more reports) |
| `add_tallies` (after the shuffle) | `{Lee: 940, Patel: 1145, Rivera: 260}` `+ {Lee: 295, Patel: 240, Rivera: 88, write-in: 4}` → `{Lee: 1235, Patel: 1385, Rivera: 348, write-in: 4}` | |


## 6. Why `add_tallies` may safely change its first argument

`add_tallies` changes `tally1` **in place** instead of building a new
dictionary. With `foldByKey()`, that is always safe, and the source code
shows why (`$SPARK_HOME/python/pyspark/core/rdd.py`):

```python
# inside RDD.foldByKey(zeroValue, func) (simplified)
def createZero():
    return copy.deepcopy(zeroValue)

return self.combineByKey(lambda v: func(createZero(), v), func, func)
```

The **first** value of a key in a partition is added to a **fresh copy** of
the empty tally. So the first argument of `func` is always either that fresh
copy or a running total built from it, and **never** a precinct's own
report. Changing it changes no one's report.

With `reduceByKey()`, it's different. There, the first report of a county in
a partition **becomes** the running total (`createCombiner` is
`lambda x: x`). So `reduceByKey(add_tallies)` would add all the other
precincts' votes **into that precinct's own report object**.

You can see the difference with Python's own `functools.reduce()`, which
works like `reduceByKey()` without an initial value, and like `foldByKey()`
with one:

~~~python
>>> from functools import reduce
>>> santa_clara = [tally for county, tally in reports if county == 'Santa Clara']
>>> santa_clara[0]                         # the first precinct's report
{'Lee': 410, 'Patel': 385, 'Rivera': 120}

>>> # like foldByKey(): start from an empty tally
>>> reduce(add_tallies, santa_clara, {})
{'Lee': 1235, 'Patel': 1385, 'Rivera': 348, 'write-in': 4}
>>> santa_clara[0]                         # unchanged
{'Lee': 410, 'Patel': 385, 'Rivera': 120}

>>> # like reduceByKey(): the first report becomes the running total
>>> reduce(add_tallies, santa_clara)
{'Lee': 1235, 'Patel': 1385, 'Rivera': 348, 'write-in': 4}
>>> santa_clara[0]                         # the first precinct's report was overwritten!
{'Lee': 1235, 'Patel': 1385, 'Rivera': 348, 'write-in': 4}
~~~

The answer is the same both times, but the second call quietly destroyed the
first precinct's report. (In PySpark, each worker gets its own copy of the
data, so with `reduceByKey()` the damage usually stays hidden inside the
worker. But "usually" is not good enough. With `reduceByKey()`, the safe habit
is to build a **new** tally in every call.)

Let's restore the report we just overwrote:

~~~python
>>> reports = [
...     ('Santa Clara', {'Lee': 410, 'Patel': 385, 'Rivera': 120}),
...     ('Alameda',     {'Lee': 700, 'Patel': 640, 'Rivera': 300}),
...     ('Santa Clara', {'Lee': 530, 'Patel': 760, 'Rivera': 140}),
...     ('Alameda',     {'Lee': 510, 'Patel': 560, 'Rivera': 210, 'write-in': 7}),
...     ('Santa Clara', {'Lee': 295, 'Patel': 240, 'Rivera': 88, 'write-in': 4}),
...     ('San Mateo',   {'Lee': 220, 'Patel': 260, 'Rivera': 95}),
...     ('San Mateo',   {'Lee': 180, 'Patel': 150, 'Rivera': 60}),
...     ('San Mateo',   {'Lee': 140, 'Patel': 300}),
... ]
~~~


## 7. Why not `reduceByKey()`, `aggregateByKey()`, or `groupByKey()`?

All three give the same answer, but each is a weaker fit.

### `reduceByKey()`: works, but must build a new tally in every call

~~~python
>>> def new_total(tally1, tally2):        # never changes tally1 or tally2
...     return {c: tally1.get(c, 0) + tally2.get(c, 0)
...             for c in tally1.keys() | tally2.keys()}
...
>>> totals2 = reports_rdd.reduceByKey(new_total)
>>> sorted(totals2.collect()) == sorted(county_totals.collect())
True
~~~

This is perfectly correct, and if the tallies were plain numbers,
`reduceByKey()` would be the obvious choice. But for dictionaries, it builds
a brand-new dictionary for every report, and there is no "no votes yet"
starting point in the code.

### `aggregateByKey()`: the same function written twice

~~~python
>>> totals3 = reports_rdd.aggregateByKey({}, add_tallies, add_tallies)
>>> sorted(totals3.collect()) == sorted(county_totals.collect())
True
~~~

`seqFunc` and `combFunc` are **the same** function, because the value and
the result have the same type. `foldByKey()` is `aggregateByKey()` for exactly
this case: you write the function once.

### `groupByKey()`: ships every report, to add them up anyway

~~~python
>>> totals4 = reports_rdd.groupByKey().mapValues(lambda tallies: reduce(add_tallies, tallies, {}))
>>> sorted(totals4.collect()) == sorted(county_totals.collect())
True
~~~

Every precinct report crosses the network and all reports of a county are
held in memory together, only to be added up. With `foldByKey()`, each
partition sends at most **one** running total per county.


## 8. A common mistake: a `zeroValue` that is not empty

The PySpark documentation for `foldByKey()` says the `zeroValue` "may be added
to the result an arbitrary number of times, and must not change the result".
It is used **once per key per partition**. Suppose a careless (or dishonest!)
programmer starts every county with a "head start" of 100 votes for `Lee`:

~~~python
>>> rigged = reports_rdd.foldByKey({'Lee': 100}, add_tallies)
>>> for county, tally in sorted(rigged.collect()):
...     show(county, tally)
...
Alameda      Lee 1,310   Patel 1,200   Rivera 510   write-in 7
San Mateo    Patel 710   Lee 640   Rivera 155
Santa Clara  Lee 1,435   Patel 1,385   Rivera 348   write-in 4
~~~

`Alameda` and `San Mateo` (reports in one partition each) gave `Lee` **100**
extra votes, which was not enough to change who leads. But `Santa Clara`
(reports in two partitions) gave **200**, and that flipped the result: `Lee`
now "wins" Santa Clara, which `Patel` really won by 150 votes. If all of Santa
Clara's reports had happened to land in one partition, the head start would
have been only 100, and `Patel` would still lead. The size of the error
depends on how Spark happened to split the data. The `zeroValue`
must always be truly empty: `{}` for tallies, `0` for addition, `1` for
multiplication, an empty set for set union.


## 9. Takeaways

| Use ... | When ... | Example |
|---|---|---|
| `reduceByKey()` | the value and the result are the same kind of thing, and one rule combines any two of them in any order | [current scan](reducebykey_package_tracking.md) per package |
| `foldByKey()` | same as `reduceByKey()`, **and** there is a natural empty value of that same type | vote tally per county (this example) |
| `aggregateByKey()` | the result is different from the value, and has a natural empty value | [star-rating box](aggregatebykey_product_ratings.md) per product |
| `combineByKey()` | the result is different from the value, with no sensible empty value; the first value starts it | [customer profile](combinebykey_customer_profile.md) |
| `groupByKey()` | the answer needs **all** the values of a key together, and each group is small enough | [median home price](groupbykey_median_home_prices.md) per zip code |

Other real-life jobs with the same shape (the value is already a "partial
result", and there is a natural empty one):

* **Store inventory** per region: each store reports `{product: units in
  stock}`; the region's stock is the sum of the stores' reports (zero: `{}`).
* **Word counts** per document category: each document is already a
  `{word: count}` dictionary (zero: `{}`).
* **User permissions**: each group a user belongs to grants a set of
  permissions; the user's permissions are the union of the sets (zero: the
  empty set `set()`).
* **Hourly traffic** per highway: each sensor reports 24 hourly car counts;
  the highway's counts are the sum, hour by hour (zero: 24 zeros).

Two rules to remember:

* The `zeroValue` must be truly **empty**: combining it with any value must
  not change that value. Spark may use it many times per key.
* With `foldByKey()`, `func` may safely change and return its **first**
  argument (it is always a fresh copy of the `zeroValue` or a running total),
  but it must never change its **second** argument.

For the basics of the reducers on one simple problem, see
[Find Sum, Count, and Average Per Key](../find_sum_count_avg_per_key.md).

The same case with DataFrames (built-in aggregates, Spark SQL, and the
DataFrame-specific mistakes): [DataFrame version](foldbykey_election_results_dataframe.md).
