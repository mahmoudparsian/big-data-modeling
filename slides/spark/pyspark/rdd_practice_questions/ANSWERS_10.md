# PySpark RDD Practice Questions — Set 10: Answers

Solutions to the nine [Set 10 review questions](QUESTIONS_10.md). Every session
below was run in the `bin/pyspark` shell (Spark 4.2.0), started from this folder.
The output is copied from those real runs.

Each answer starts with the **toolbox** it uses, and links to the earlier set where that
tool was introduced. There are many correct solutions to these questions; where
another approach is just as good, the notes mention it.

* [Q1 (Basic): Trips, fares, and distances per city](#q1-basic--trips-fares-and-distances-per-city)
* [Q2 (Basic): Who drives where?](#q2-basic--who-drives-where)
* [Q3 (Basic): The longest trips and the busiest hours](#q3-basic--the-longest-trips-and-the-busiest-hours)
* [Q4 (Intermediate): Driver ratings](#q4-intermediate--driver-ratings)
* [Q5 (Intermediate): Revenue per car type, and the driver with no trips](#q5-intermediate--revenue-per-car-type-and-the-driver-with-no-trips)
* [Q6 (Intermediate): Suspicious fares](#q6-intermediate--suspicious-fares)
* [Q7 (Intermediate+): Regular riders and their favorite drivers](#q7-intermediate--regular-riders-and-their-favorite-drivers)
* [Q8 (Intermediate+): Cleaning the raw trip log](#q8-intermediate--cleaning-the-raw-trip-log)
* [Q9 (Intermediate+): The top 2 drivers in each city](#q9-intermediate--the-top-2-drivers-in-each-city)

Setup, shared by all the questions:

```python
>>> def parse(line):
...     f = line.split(",")
...     rating = int(f[7]) if f[7] else None
...     return (f[0], f[1], f[2], f[3], int(f[4]), float(f[5]), float(f[6]), rating)
...
>>> trips = sc.textFile("data/trips.txt", 2).map(parse).cache()
>>> trips.take(2)
[('t01', 'd1', 'r1', 'San Jose', 8, 5.2, 14.5, 5), ('t02', 'd2', 'r2', 'San Jose', 9, 12.0, 30.0, 4)]
```

Each trip is a tuple `(trip_id, driver, rider, city, hour, distance, fare, rating)`, so
`t[3]` is the city, `t[5]` the distance, `t[6]` the fare, and `t[7]` the rating
(`None` if the rider didn't rate the trip). `trips` is **cached**, because every question
uses it ([Set 08, Q9](ANSWERS_08.md#q9-intermediate--where-to-put-cache)).

---

## Q1 (Basic) — Trips, fares, and distances per city

**Toolbox:** `map()`, `count()`, a `(count, sum, sum)` tuple with `reduceByKey()`
([Set 01](ANSWERS_01.md#q5-intermediate--average-score-per-student)).

```python
>>> trips.count()
20
>>> round(sum(trips.map(lambda t: t[6]).collect()), 2)
476.25

>>> per_city = (trips.map(lambda t: (t[3], (1, t[5], t[6])))
...                  .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])))
>>> (per_city.mapValues(lambda v: (v[0], round(v[1] / v[0], 2), round(v[2], 2)))
...          .sortByKey()
...          .collect())
[('Oakland', (8, 8.11, 168.25)), ('San Jose', (12, 7.96, 308.0))]
```

**Notes:**

* One `reduceByKey()` computes **three** things per city at once, by carrying a tuple
  `(trips, total distance, total fare)`. The average distance is computed **after**
  the reduce, from the totals.
* `sum(... .collect())` is fine for a small result. For a big RDD, the action
  `trips.map(lambda t: t[6]).sum()` adds up the numbers on the executors instead.
* `per_city` is reused in Q6.

---

## Q2 (Basic) — Who drives where?

**Toolbox:** `distinct()` on pairs, then `groupByKey()` + `mapValues(sorted)`
([Set 02](ANSWERS_02.md#q3-basic--grouping-employees-by-department)).

```python
>>> city_driver = trips.map(lambda t: (t[3], t[1])).distinct()
>>> city_driver.groupByKey().mapValues(sorted).sortByKey().collect()
[('Oakland', ['d1', 'd3', 'd4']), ('San Jose', ['d1', 'd2', 'd3', 'd4'])]

>>> driver_cities = trips.map(lambda t: (t[1], t[3])).distinct().groupByKey().mapValues(sorted)
>>> driver_cities.sortByKey().collect()
[('d1', ['Oakland', 'San Jose']), ('d2', ['San Jose']), ('d3', ['Oakland', 'San Jose']), ('d4', ['Oakland', 'San Jose'])]
>>> driver_cities.filter(lambda kv: len(kv[1]) == 1).map(lambda kv: kv[0]).collect()
['d2']

>>> trips.map(lambda t: t[2]).distinct().count()
7
```

**Notes:**

* **`distinct()` first, then group.** Without it, `San Jose` would list `d1` once for
  every one of Ana's San Jose trips.
* The two questions are the same data **inverted**: `(city, driver)` vs.
  `(driver, city)`. Swapping the pair with `map()` is all it takes.
* Another way to get the same answer is a `set` combiner with `aggregateByKey(set(), ...)`
  ([Set 06, Q5](ANSWERS_06.md#q5-intermediate--unique-values-per-key-with-a-set)),
  which removes the duplicates before the shuffle.

---

## Q3 (Basic) — The longest trips and the busiest hours

**Toolbox:** "swap, then `sortByKey()`" and `take(n)`
([Set 02](ANSWERS_02.md#q2-basic--sorting-products)); the `(-count, key)` tie-break
([Set 02, Q4](ANSWERS_02.md#q4-intermediate--word-count-with-the-top-words)).

```python
>>> trips.map(lambda t: (t[5], t[0])).sortByKey(ascending=False).take(3)
[(20.5, 't13'), (15.3, 't05'), (14.8, 't20')]

>>> (trips.map(lambda t: (t[4], 1))
...       .reduceByKey(lambda a, b: a + b)
...       .map(lambda hc: ((-hc[1], hc[0]), hc))
...       .sortByKey()
...       .map(lambda kv: kv[1])
...       .take(3))
[(18, 4), (9, 3), (19, 3)]
```

**Notes:**

* To sort by the distance, make the distance the **key**. `take(3)` after the sort
  gives the top 3 without collecting everything.
* For the hours, `9` and `19` both have 3 trips. The `(-count, hour)` key puts the
  larger count first and breaks the tie by the hour, so the answer is the same on
  every run.
* For a global top-N, the `mapPartitions()` + `heapq.nlargest()` approach from
  [Set 03, Q8](ANSWERS_03.md#q8-intermediate--global-top-n) avoids the full sort.
  PySpark's `top(n)` does exactly that.

---

## Q4 (Intermediate) — Driver ratings

**Toolbox:** `filter()` for missing values; a `(sum, count)` accumulator with
`aggregateByKey()` ([Set 06, Q4](ANSWERS_06.md#q4-intermediate--average-per-key)).

```python
>>> rated = trips.filter(lambda t: t[7] is not None)
>>> trips.count() - rated.count()
3

>>> ratings = rated.map(lambda t: (t[1], t[7])).aggregateByKey(
...     (0, 0),
...     lambda acc, r: (acc[0] + r, acc[1] + 1),
...     lambda a, b: (a[0] + b[0], a[1] + b[1]))
>>> ratings.mapValues(lambda s: (s[1], round(s[0] / s[1], 2))).sortByKey().collect()
[('d1', (5, 4.6)), ('d2', (3, 3.33)), ('d3', (6, 4.33)), ('d4', (3, 3.33))]

>>> sorted(ratings.filter(lambda kv: kv[1][1] >= 3 and kv[1][0] / kv[1][1] >= 4.5)
...               .map(lambda kv: kv[0])
...               .collect())
['d1']
```

**Notes:**

* **Unrated trips must be removed, not counted as 0.** A `0` would pull every average
  down. Treating "no value" as `0` is a classic bug.
* The **minimum number of ratings** (3) protects against a driver with a single 5-star
  trip topping the list. Real rating systems use rules like this.
* The filter uses the **unrounded** average `s[0] / s[1]`. Round only for display.
* `combineByKey()` ([Set 04, Q4](ANSWERS_04.md#q4-intermediate--average-per-key-with-combinebykey))
  or `mapValues(lambda r: (r, 1))` + `reduceByKey()` work just as well.

---

## Q5 (Intermediate) — Revenue per car type, and the driver with no trips

**Toolbox:** **reduce before you join**
([Set 05, Q4](ANSWERS_05.md#q4-intermediate--orders-per-customer-and-per-city));
`leftOuterJoin()` to find missing matches
([Set 05, Q5](ANSWERS_05.md#q5-intermediate--data-quality-checks-with-outer-joins)).

```python
>>> drivers = (sc.textFile("data/drivers.txt", 2)
...              .map(lambda line: line.split(","))
...              .map(lambda f: (f[0], (f[1], f[2]))))
>>> revenue = trips.map(lambda t: (t[1], t[6])).reduceByKey(lambda a, b: a + b)

>>> (revenue.join(drivers)
...         .map(lambda kv: (kv[1][1][1], kv[1][0]))
...         .reduceByKey(lambda a, b: a + b)
...         .mapValues(lambda v: round(v, 2))
...         .sortByKey()
...         .collect())
[('electric', 185.25), ('sedan', 202.0), ('suv', 89.0)]

>>> everyone = drivers.leftOuterJoin(revenue)
>>> everyone.filter(lambda kv: kv[1][1] is None).map(lambda kv: kv[1][0][0]).collect()
['Eva']
>>> everyone.map(lambda kv: (kv[1][0][0], round(kv[1][1] or 0.0, 2))).sortByKey().collect()
[('Ana', 102.25), ('Ben', 89.0), ('Chloe', 99.75), ('Dan', 185.25), ('Eva', 0.0)]
```

**Notes:**

* **Reduce first:** 20 trips become 4 `(driver, revenue)` pairs, and only those 4 are
  joined. With millions of trips but hundreds of drivers, that makes the join tiny.
* A joined row is `(driver_id, (revenue, (name, car_type)))`, so the car type is
  `kv[1][1][1]`.
* **The inner join hides Eva.** She has no trips, so she's not in `revenue`, and
  `join()` drops her. Starting from `drivers` with `leftOuterJoin()` keeps every
  driver, with `None` for missing revenue.
* `drivers` is a **small** table, so a **broadcast join**
  ([Set 09, Q4](ANSWERS_09.md#q4-intermediate--a-broadcast-join)) would avoid the
  shuffle entirely: `sc.broadcast(dict(drivers.collect()))`.

---

## Q6 (Intermediate) — Suspicious fares

**Toolbox:** a per-city statistic from Q1, **broadcast** back to the trips
([Set 09, Q1](ANSWERS_09.md#q1-basic--a-broadcast-lookup-table)), then `map()` and `filter()`.

```python
>>> city_rate = dict(per_city.mapValues(lambda v: v[2] / v[1]).collect())
>>> {city: round(rate, 3) for city, rate in sorted(city_rate.items())}
{'Oakland': 2.592, 'San Jose': 3.225}

>>> rate_b = sc.broadcast(city_rate)
>>> def ratio(t):
...     return t[6] / t[5] / rate_b.value[t[3]]
...
>>> (trips.map(lambda t: (t[0], t[3], round(t[6] / t[5], 2), round(ratio(t), 2)))
...       .filter(lambda r: r[3] > 1.5)
...       .collect())
[('t13', 'San Jose', 5.37, 1.66)]

>>> sorted(trips.map(lambda t: (round(ratio(t), 2), t[0])).collect(), reverse=True)[:3]
[(1.66, 't13'), (1.21, 't03'), (1.06, 't19')]
```

**Notes:**

* **Two passes:** first compute each city's average fare per km (total fare ÷ total
  km, from Q1's `per_city`), then compare **every trip** with **its** city's average.
  The averages are tiny (2 numbers), so broadcasting them is the natural choice.
  A `join()` on the city would work too, but it would shuffle all the trips.
* Trip `t13` costs $5.37 per km, **1.66×** San Jose's average of $3.23. That's the only
  trip above the 1.5× threshold. It's also the 1-star trip (Q4): an unhappy rider.
* Use the **city's own** average, not the overall average. Fares differ by city, and a
  normal San Jose fare might look high next to Oakland's.

---

## Q7 (Intermediate+) — Regular riders and their favorite drivers

**Toolbox:** a **tuple key** `(rider, driver)`
([Set 01, Q9](ANSWERS_01.md#q9-intermediate--best-selling-product-per-category));
**re-keying** and a "keep the better one" `reduceByKey()` with a tie-break (same place).

```python
>>> pair_counts = trips.map(lambda t: ((t[2], t[1]), 1)).reduceByKey(lambda a, b: a + b)
>>> pair_counts.filter(lambda kv: kv[1] > 1).sortByKey().collect()
[(('r1', 'd1'), 3), (('r1', 'd3'), 3), (('r2', 'd2'), 3), (('r4', 'd4'), 3), (('r5', 'd3'), 2)]

>>> def better(a, b):
...     if a[1] != b[1]:
...         return a if a[1] > b[1] else b
...     return a if a[0] < b[0] else b
...
>>> favorite = (pair_counts.map(lambda kv: (kv[0][0], (kv[0][1], kv[1])))
...                        .reduceByKey(better)
...                        .sortByKey())
>>> favorite.collect()
[('r1', ('d1', 3)), ('r2', ('d2', 3)), ('r3', ('d1', 1)), ('r4', ('d4', 3)), ('r5', ('d3', 2)), ('r6', ('d1', 1)), ('r7', ('d2', 1))]

>>> sorted(trips.map(lambda t: (t[2], 1))
...             .reduceByKey(lambda a, b: a + b)
...             .filter(lambda kv: kv[1] == 1)
...             .map(lambda kv: kv[0])
...             .collect())
['r7']
```

**Notes:**

* This is the **same shape** as "best-selling product per category" in Set 01, Q9:
  count by a pair, move part of the key into the value, then keep the best value per key.
* **Ties:** `r1` rode 3 times with `d1` and 3 times with `d3`. `better()` breaks ties
  alphabetically, so the answer doesn't depend on the order the values arrive in.
  `r3` and `r6` also have ties (1 trip each with two drivers).
* `better()` is **associative and commutative** (it always keeps the same winner), so
  it's safe in `reduceByKey()` ([Set 04, Q9](ANSWERS_04.md#q9-intermediate--when-reducebykey-gives-the-wrong-answer)).
* Riders with **one** trip are a "first-time rider" list for a marketing email. It's
  just the counting pattern, with a `filter()`.

---

## Q8 (Intermediate+) — Cleaning the raw trip log

**Toolbox:** parse-and-filter in one `flatMap()`; a **broadcast** set of valid driver
ids ([Set 09, Q5](ANSWERS_09.md#q5-intermediate--broadcasting-a-set-of-stop-words)); a
**`Counter` accumulator** for the problems
([Set 09, Q7](ANSWERS_09.md#q7-intermediate--custom-accumulators)); `cache()` so the
counts don't double ([Set 09, Q6](ANSWERS_09.md#q6-intermediate--when-accumulators-count-twice)).

```python
>>> from collections import Counter
>>> from pyspark.accumulators import AccumulatorParam
>>> class CounterParam(AccumulatorParam):
...     def zero(self, initial):
...         return Counter()
...     def addInPlace(self, c1, c2):
...         c1.update(c2)
...         return c1
...
>>> known_drivers = sc.broadcast(set(drivers.map(lambda kv: kv[0]).collect()))
>>> issues = sc.accumulator(Counter(), CounterParam())

>>> def clean(line):
...     f = line.split(",")
...     if len(f) != 8:
...         issues.add(Counter({"wrong field count": 1}))
...         return []
...     if f[0] == "trip_id":
...         issues.add(Counter({"header": 1}))
...         return []
...     if f[1] not in known_drivers.value:
...         issues.add(Counter({"unknown driver": 1}))
...         return []
...     try:
...         distance, fare = float(f[5]), float(f[6])
...     except ValueError:
...         issues.add(Counter({"bad number": 1}))
...         return []
...     if distance <= 0:
...         issues.add(Counter({"bad distance": 1}))
...         return []
...     return [parse(line)]
...
>>> raw = sc.textFile("data/trips_raw.txt", 2)
>>> good = raw.flatMap(clean).cache()
>>> raw.count()
25
>>> good.count()
20
>>> sorted(issues.value.items())
[('bad distance', 1), ('bad number', 1), ('header', 1), ('unknown driver', 1), ('wrong field count', 1)]

>>> sorted(good.collect()) == sorted(trips.collect())
True
>>> sorted(issues.value.items())
[('bad distance', 1), ('bad number', 1), ('header', 1), ('unknown driver', 1), ('wrong field count', 1)]
```

**Notes:**

* **One pass** over the raw file gives both the clean RDD and a report of **what was
  dropped, and why**: a header, a blank line (1 field), a trip by the unknown driver
  `d9`, a missing fare (`float("")` fails), and a negative distance.
* **Order the checks** from cheapest to most specific. The field count comes first,
  because every later check reads `f[1]` or `f[5]`, which would crash on a short line.
* The cleaned data is **exactly** `trips.txt`. That is the check that the cleaning is
  right: it removes all the bad lines and keeps all the good ones.
* **`good` is cached**, so the second action (`collect()`) didn't re-run `clean`, and
  the counts didn't double.
* Real pipelines often **save** the bad lines too (a "dead-letter" file), so they can be
  inspected and fixed. A `filter()` with the opposite condition does that.

---

## Q9 (Intermediate+) — The top 2 drivers in each city

**Toolbox:** a tuple key `(city, driver)` for the revenue; **re-keying** by city; a
**bounded** top-2 accumulator with `aggregateByKey()`
([Set 04, Q8](ANSWERS_04.md#q8-intermediate--top-2-per-key-without-groupbykey),
[Set 06](ANSWERS_06.md)); a **broadcast** of the city totals for the shares.

```python
>>> driver_rev = trips.map(lambda t: ((t[3], t[1]), t[6])).reduceByKey(lambda a, b: a + b)
>>> driver_rev.mapValues(lambda v: round(v, 2)).sortByKey().collect()
[(('Oakland', 'd1'), 30.0), (('Oakland', 'd3'), 63.0), (('Oakland', 'd4'), 75.25), (('San Jose', 'd1'), 72.25), (('San Jose', 'd2'), 89.0), (('San Jose', 'd3'), 36.75), (('San Jose', 'd4'), 110.0)]

>>> def keep_top2(drivers):
...     return sorted(drivers, key=lambda d: (-d[1], d[0]))[:2]
...
>>> top2 = (driver_rev.map(lambda kv: (kv[0][0], (kv[0][1], kv[1])))
...                   .aggregateByKey([],
...                                   lambda acc, d: keep_top2(acc + [d]),
...                                   lambda a, b: keep_top2(a + b)))

>>> city_total = sc.broadcast(dict(driver_rev.map(lambda kv: (kv[0][0], kv[1]))
...                                          .reduceByKey(lambda a, b: a + b)
...                                          .collect()))
>>> {city: round(total, 2) for city, total in sorted(city_total.value.items())}
{'Oakland': 168.25, 'San Jose': 308.0}

>>> (top2.map(lambda kv: (kv[0], [(d, round(r, 2), f"{r / city_total.value[kv[0]]:.0%}")
...                               for d, r in kv[1]]))
...      .sortByKey()
...      .collect())
[('Oakland', [('d4', 75.25, '45%'), ('d3', 63.0, '37%')]), ('San Jose', [('d4', 110.0, '36%'), ('d2', 89.0, '29%')])]
```

**Notes:**

* **Three steps, three tools:**
  1. revenue per `(city, driver)`: a sum per **tuple key**;
  2. the top 2 per city: **re-key** by city, then a **bounded** accumulator that never
     keeps more than 2 drivers, so it works for a city with 100,000 drivers too.
     `groupByKey()` + `mapValues(keep_top2)` gives the same answer, but holds every
     driver of a city in one task;
  3. each driver's **share** of the city's revenue: the city totals are tiny, so
     **broadcast** them.
* The zero value `[]` is safe, even though `keep_top2(acc + [d])` builds a new list:
  `aggregateByKey()` deep-copies the zero value for every key
  ([Set 06, Q9](ANSWERS_06.md#q9-intermediate--implement-aggregatebykey-yourself)).
* Dan (`d4`) is the top driver in **both** cities. But in San Jose, **all** of his
  revenue ($110) comes from a single trip: the suspicious `t13` fare from Q6. Without
  it, Ben (`d2`) and Ana (`d1`) would be the San Jose top 2. A good analysis connects
  the findings.
* `f"{x:.0%}"` formats a fraction as a whole percentage.
