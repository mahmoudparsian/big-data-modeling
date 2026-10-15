# PySpark RDD Practice Questions — Set 10: Mixed Review

Nine review questions, three at each level, meant to be solved **interactively** in
the `bin/pyspark` shell. Unlike Sets 01–09, this set doesn't tell you which operations
to use. **Choosing the right tools is part of the question**, as in an exam or a real
project.

All the questions use one small, realistic data set: the trips of a ride-sharing
service in two cities.

| # | Level | Question |
|---|---|---|
| [Q1](#q1-basic--trips-fares-and-distances-per-city) | Basic | Trips, fares, and distances per city |
| [Q2](#q2-basic--who-drives-where) | Basic | Who drives where? |
| [Q3](#q3-basic--the-longest-trips-and-the-busiest-hours) | Basic | The longest trips and the busiest hours |
| [Q4](#q4-intermediate--driver-ratings) | Intermediate | Driver ratings |
| [Q5](#q5-intermediate--revenue-per-car-type-and-the-driver-with-no-trips) | Intermediate | Revenue per car type, and the driver with no trips |
| [Q6](#q6-intermediate--suspicious-fares) | Intermediate | Suspicious fares |
| [Q7](#q7-intermediate--regular-riders-and-their-favorite-drivers) | Intermediate+ | Regular riders and their favorite drivers |
| [Q8](#q8-intermediate--cleaning-the-raw-trip-log) | Intermediate+ | Cleaning the raw trip log |
| [Q9](#q9-intermediate--the-top-2-drivers-in-each-city) | Intermediate+ | The top 2 drivers in each city |

You may use **any operation** from [Sets 01–09](README.md): transformations, reducers,
joins, set operations, `mapPartitions()`, partitioning, `cache()`, broadcast variables,
and accumulators. Each question has a hidden **hint** that names the sets that help;
try without it first.

Try each question on your own before you open the [answers](ANSWERS_10.md). The answers
show one good solution, explain **why** those tools were chosen, and point out other good
choices.

## Files

| File | Purpose |
|---|---|
| [QUESTIONS_10.md](QUESTIONS_10.md) | The nine questions (this page) |
| [ANSWERS_10.md](ANSWERS_10.md) | Solutions, with real output from Spark 4.2.0 |
| [data/trips.txt](data/trips.txt) | 20 clean trips: `trip_id,driver_id,rider,city,hour,distance_km,fare,rating` |
| [data/drivers.txt](data/drivers.txt) | 5 drivers: `driver_id,name,car_type` |
| [data/trips_raw.txt](data/trips_raw.txt) | The **raw** trip log for Q8: the same 20 trips, plus a header and some bad lines |

In `trips.txt`, `hour` is the hour of day the trip started (0–23), and `rating` is 1 to 5
stars, or **empty** if the rider didn't rate the trip.

## Getting started

Start `$SPARK_HOME/bin/pyspark` **from this folder**, as in
[Set 01](QUESTIONS_01.md#getting-started), and do the questions **in order**, in one shell
session. Start with this setup, which every question uses:

```python
def parse(line):
    f = line.split(",")
    rating = int(f[7]) if f[7] else None
    return (f[0], f[1], f[2], f[3], int(f[4]), float(f[5]), float(f[6]), rating)

trips = sc.textFile("data/trips.txt", 2).map(parse).cache()
trips.take(2)
```

```
[('t01', 'd1', 'r1', 'San Jose', 8, 5.2, 14.5, 5), ('t02', 'd2', 'r2', 'San Jose', 9, 12.0, 30.0, 4)]
```

So in each trip `t`: `t[0]` trip id, `t[1]` driver, `t[2]` rider, `t[3]` city, `t[4]` hour,
`t[5]` distance (km), `t[6]` fare ($), `t[7]` rating (or `None`).

As in the earlier sets, sort your results (`sortByKey()` or Python's `sorted()`), so
they're easy to compare with the expected output.

---

## Q1 (Basic) — Trips, fares, and distances per city

1. How many trips are there, and what is the **total fare** of all trips (rounded to
   2 decimals)?
2. For each city, find `(number of trips, average distance, total fare)`, with the
   average and the total rounded to 2 decimals, sorted by city. Use **one** pass over
   the data.

**Expected output:**

```
20
476.25
[('Oakland', (8, 8.11, 168.25)), ('San Jose', (12, 7.96, 308.0))]
```

<details>
<summary>Hint</summary>

Carry a tuple through one reducer: [Set 01, Q5](QUESTIONS_01.md#q5-intermediate--average-score-per-student)
and [Q8](QUESTIONS_01.md#q8-intermediate--temperature-statistics-with-dirty-data).
Keep the per-city RDD, because Q6 needs it.

</details>

---

## Q2 (Basic) — Who drives where?

1. For each city, list the drivers who drove there (each driver once, sorted).
2. For each driver, list the cities they drove in (sorted). Which drivers drove in
   **only one** city?
3. How many **different riders** are there?

**Expected output:**

```
[('Oakland', ['d1', 'd3', 'd4']), ('San Jose', ['d1', 'd2', 'd3', 'd4'])]
[('d1', ['Oakland', 'San Jose']), ('d2', ['San Jose']), ('d3', ['Oakland', 'San Jose']), ('d4', ['Oakland', 'San Jose'])]
['d2']
7
```

<details>
<summary>Hint</summary>

Remove the duplicates before you group: [Set 01](QUESTIONS_01.md) (`distinct()`) and
[Set 02, Q3](QUESTIONS_02.md#q3-basic--grouping-employees-by-department) (`groupByKey()`).

</details>

---

## Q3 (Basic) — The longest trips and the busiest hours

1. Find the **3 longest** trips, as `(distance, trip_id)`.
2. Find the **3 busiest hours** of the day, as `(hour, number_of_trips)`, most trips
   first. If two hours have the same number of trips, the earlier hour comes first.

**Expected output:**

```
[(20.5, 't13'), (15.3, 't05'), (14.8, 't20')]
[(18, 4), (9, 3), (19, 3)]
```

<details>
<summary>Hint</summary>

"Swap, then sort", and the `(-count, key)` tie-break:
[Set 02, Q2 and Q4](QUESTIONS_02.md#q2-basic--sorting-products).

</details>

---

## Q4 (Intermediate) — Driver ratings

1. How many trips were **not** rated?
2. For each driver, find `(number of ratings, average rating)`, with the average rounded
   to 2 decimals. Unrated trips must **not** count.
3. Which drivers have **at least 3** ratings **and** an average of **4.5 or more**?

**Expected output:**

```
3
[('d1', (5, 4.6)), ('d2', (3, 3.33)), ('d3', (6, 4.33)), ('d4', (3, 3.33))]
['d1']
```

<details>
<summary>Hint</summary>

Filter out the `None` ratings, then compute a `(sum, count)` per driver:
[Set 06, Q4](QUESTIONS_06.md#q4-intermediate--average-per-key) (or Sets 01 and 04).

</details>

---

## Q5 (Intermediate) — Revenue per car type, and the driver with no trips

Use [`data/drivers.txt`](data/drivers.txt) (`driver_id,name,car_type`), read with **2** partitions.

1. Find the **total revenue (fares) per car type**, rounded to 2 decimals, sorted by car type.
2. Which driver had **no trips** at all? Show their name.
3. Show the revenue of **every** driver by name, with `0.0` for a driver with no trips.

**Expected output:**

```
[('electric', 185.25), ('sedan', 202.0), ('suv', 89.0)]
['Eva']
[('Ana', 102.25), ('Ben', 89.0), ('Chloe', 99.75), ('Dan', 185.25), ('Eva', 0.0)]
```

<details>
<summary>Hint</summary>

Reduce before you join, and use an outer join to find what's missing:
[Set 05, Q4 and Q5](QUESTIONS_05.md#q4-intermediate--orders-per-customer-and-per-city).
For a small table like `drivers`, a broadcast join
([Set 09, Q4](QUESTIONS_09.md#q4-intermediate--a-broadcast-join)) also works.

</details>

---

## Q6 (Intermediate) — Suspicious fares

A trip is **suspicious** if its fare per km is more than **1.5 times** the average fare
per km of **its own city**. (A city's average fare per km is its total fare divided by
its total distance.)

1. Compute each city's average fare per km, as a dict (values rounded to 3 decimals).
2. Find the suspicious trips, as `(trip_id, city, fare_per_km, ratio_to_city_average)`,
   with both numbers rounded to 2 decimals.
3. Show the 3 trips with the **highest** ratio, as `(ratio, trip_id)`.

**Expected output:**

```
{'Oakland': 2.592, 'San Jose': 3.225}
[('t13', 'San Jose', 5.37, 1.66)]
[(1.66, 't13'), (1.21, 't03'), (1.06, 't19')]
```

<details>
<summary>Hint</summary>

Two passes: a small per-city result (from Q1), sent back out to the trips with a
broadcast variable: [Set 09, Q1](QUESTIONS_09.md#q1-basic--a-broadcast-lookup-table).

</details>

---

## Q7 (Intermediate+) — Regular riders and their favorite drivers

1. Which `(rider, driver)` pairs rode together **more than once**? Show them with their
   number of trips, sorted.
2. For **each rider**, find their **favorite driver**: the driver they rode with most,
   as `(rider, (driver, trips))`. If there's a tie, take the driver id that comes first
   alphabetically.
3. Which riders took only **one** trip?

**Expected output:**

```
[(('r1', 'd1'), 3), (('r1', 'd3'), 3), (('r2', 'd2'), 3), (('r4', 'd4'), 3), (('r5', 'd3'), 2)]
[('r1', ('d1', 3)), ('r2', ('d2', 3)), ('r3', ('d1', 1)), ('r4', ('d4', 3)), ('r5', ('d3', 2)), ('r6', ('d1', 1)), ('r7', ('d2', 1))]
['r7']
```

<details>
<summary>Hint</summary>

Count by a tuple key, move part of the key into the value, then keep the best value per
key: the same shape as [Set 01, Q9](QUESTIONS_01.md#q9-intermediate--best-selling-product-per-category).

</details>

---

## Q8 (Intermediate+) — Cleaning the raw trip log

The raw log [`data/trips_raw.txt`](data/trips_raw.txt) (read with **2** partitions)
contains the 20 trips of `trips.txt`, plus 5 bad lines: a **header**, a **blank line**, a
trip by a driver who is **not** in `drivers.txt`, a trip with a **missing fare**, and a
trip with a **negative distance**.

1. In **one pass**, keep only the good trips (parsed with `parse()`), and **count the bad
   lines by type** with the labels `"header"`, `"wrong field count"`, `"unknown driver"`,
   `"bad number"`, and `"bad distance"`. Count the raw lines and the good trips, and show
   the problem counts, sorted.
2. Check that your cleaned trips are **exactly** the trips of `trips.txt`.
3. Show the problem counts again. They must **not** have changed.

**Expected output:**

```
25
20
[('bad distance', 1), ('bad number', 1), ('header', 1), ('unknown driver', 1), ('wrong field count', 1)]
True
[('bad distance', 1), ('bad number', 1), ('header', 1), ('unknown driver', 1), ('wrong field count', 1)]
```

<details>
<summary>Hint</summary>

A `flatMap()` that returns `[]` or `[trip]`, a broadcast set of driver ids, and a
`Counter` accumulator: [Set 09, Q5–Q7](QUESTIONS_09.md#q7-intermediate--custom-accumulators).
Task 3 is about [Set 09, Q6](QUESTIONS_09.md#q6-intermediate--when-accumulators-count-twice).

</details>

---

## Q9 (Intermediate+) — The top 2 drivers in each city

1. Find the revenue of each driver **in each city**, as `((city, driver), revenue)`,
   rounded to 2 decimals and sorted.
2. Find the **total revenue** of each city, as a dict (rounded to 2 decimals).
3. For each city, find its **top 2 drivers** by revenue, as
   `(driver, revenue, share_of_city_revenue)`, with the share as a whole percentage
   such as `'45%'`. Your solution must work for a city with 100,000 drivers, so it must
   **not** collect all of a city's drivers in one place.

**Expected output:**

```
[(('Oakland', 'd1'), 30.0), (('Oakland', 'd3'), 63.0), (('Oakland', 'd4'), 75.25), (('San Jose', 'd1'), 72.25), (('San Jose', 'd2'), 89.0), (('San Jose', 'd3'), 36.75), (('San Jose', 'd4'), 110.0)]
{'Oakland': 168.25, 'San Jose': 308.0}
[('Oakland', [('d4', 75.25, '45%'), ('d3', 63.0, '37%')]), ('San Jose', [('d4', 110.0, '36%'), ('d2', 89.0, '29%')])]
```

<details>
<summary>Hint</summary>

A bounded top-2 combiner instead of `groupByKey()`:
[Set 04, Q8](QUESTIONS_04.md#q8-intermediate--top-2-per-key-without-groupbykey) (with
`combineByKey()`) or [Set 06](QUESTIONS_06.md) (with `aggregateByKey()`). The city
totals are small: broadcast them. `f"{x:.0%}"` formats a fraction as a percentage.

</details>

*Think about it:* Dan (`d4`) is the top driver in both cities. Look at his San Jose
revenue again, together with your answer to Q6. What do you notice?
