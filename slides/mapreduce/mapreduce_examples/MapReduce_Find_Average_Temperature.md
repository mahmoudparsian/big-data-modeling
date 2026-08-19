# MapReduce Example: Average Temperature per City

	Author: Mahmoud Parsian
	Last updated: 8/19/2026

## 1. Introduction

MapReduce is a parallel programming model
and an associated implementation introduced
by Google. In the programming model, a user
specifies the computation by two functions,
`map()` and `reduce()`. The purpose of this
article is to present a problem and then
provide an associated solution in MapReduce.

Note that in classic MapReduce, both `map()`
and `reduce()` functions can emit any number
(0, 1, 2, 3, ...) of (key, value) pairs. What
does it mean when we say that a mapper can
emit zero (key, value) pairs? It means that
the mapper is *filtering out* its entire input
record — for example, because the record is
malformed (bad format), or because it simply
is not needed for the computation at hand.

## 2. Problem

Given temperature data for United States,
for the past 100 years, we want to find
the average temperature per city, where a
city is identified by "state" code (such
as "CA", "TX", "IA", ...) and "city" name.

## 3. Input Data Format

We assume that input is given in a CSV format
and each record has the following format:

```text
<date>,<time>,<state_code>,<city_name>,<temperature>
```

where date is in a format of "MM/DD/YYYY" and
time is in a format of "hour:minute" where hour
is a number in {0, 1, 2, ..., 23} and minute
is a number in {0, 1, 2, ..., 59}. Our assumption
is that one temperature is recorded per minute.

Example records will be:

```text
9/28/2022,13:42,CA,Sunnyvale,82
9/28/2022,16:20,CA,Sunnyvale,76
9/27/2022,14:11,CA,Cupertino,88
8/22/2000,4:45,TX,Dallas,70
8/24/2011,15:10,TX,Dallas,94
...
```

## 4. Is This a Big Data Problem?

Yes. If we have gathered data for 100 years,
for 50 states and 2,500 cities, with one
reading every minute, then the number of
records to process will be:

```text
100 * 2500 * 365 * 24 * 60 = 131,400,000,000
```

That's over 131 billion records — well beyond
what a single machine can process in a
reasonable amount of time, which is exactly
the kind of workload MapReduce was designed for.

## 5. Output Data Format

The goal is to create a set of (key, value) pairs
where key is combination of (state_code, city_name)
and value is the average temperature for the
given key.

Output example records will be:

```text
(CA-Sunnyvale, 79.0)
(TX-Dallas, 82.0)
...
```

## 6. Input to Mappers

We assume that input to mappers is provided as
(key, value) pairs, where key is a record number
(which will be ignored by our mappers) and value
is the actual record described above.

A (key, value) example to our mappers will be:

```text
(1, "9/28/2022,13:42,CA,Sunnyvale,82")
(2, "9/28/2022,16:20,CA,Sunnyvale,76")
...
```

## 7. Mapper

Next, we write a `map()` function, which accepts
a (key, value) pair and emits necessary outputs
to be able to calculate the average temperature
per city.

```text
# key: a record number, will be ignored
# value: a data record in format of:
#    <date>,<time>,<state_code>,<city_name>,<temperature>
#
map(key, value) {
   # step-1: tokenize input record
   # note that tokens[i] is a string object
   tokens = value.split(",")
   # date = tokens[0]
   # time = tokens[1]
   state_code = tokens[2]
   city_name = tokens[3]
   temperature = tokens[4]

   # step-2: create proper key
   output_key = state_code + "-" + city_name
   output_value = int(temperature)

   # step-3: emit (K, V) pair
   emit(output_key, output_value)
}
```

The mappers output will be (key, value) pairs,
where key is `"<state_code>-<city_name>"`
and value is temperature as an integer.

Sample of mappers output will be:

```text
("CA-Sunnyvale", 82)
("CA-Sunnyvale", 76)
...
```

## 8. Sort & Shuffle Phase

Sort & Shuffle Phase is the genie of the MapReduce
paradigm: it is done automagically on the
programmer's behalf. The shuffle phase transfers
the map output from mappers to reducers, and the
sort phase covers the merging and sorting of map
outputs. To simplify, the Sort & Shuffle phase
will create the output as (key, values) pairs,
where key is a unique `"<state_code>-<city_name>"`
and values is an `Iterable<Integer>` (a sequence
of temperature readings).

Sample output of Sort & Shuffle Phase will be:

```text
("CA-Sunnyvale", [82, 76, 56, 98, ...])
("TX-Dallas",    [70, 94, 88, 70, ...])
...
```

Output of Sort & Shuffle Phase is given as
input to reducers.

## 9. Reducer

Next, we write a `reduce()` function, which accepts
a (key, values) pair and emits the average of
values for a given key.

```text
# key: "<state_code>-<city_name>"
# values: Iterable<Integer>
#
reduce(key, values) {
   # step-1: find the sum and count of temperature values
   count = 0
   sum = 0
   for (v in values) {
      count += 1
      sum += v
   }

   # step-2: calculate average
   # NOTE: use floating-point division here — in languages
   # where `/` truncates on two integers (e.g., Java's `int / int`,
   # or Python 2's `/`), cast sum or count to a float/double first,
   # or the result will be silently wrong.
   average = sum / (1.0 * count)

   # step-3: emit (K, V) pair
   emit(key, average)
}
```

The reducers output will be (key, value) pairs,
where key is `"<state_code>-<city_name>"`
and value is the average temperature for the
given key.

Sample of reducers output will be:

```text
(CA-Sunnyvale, 79.0)
(TX-Dallas, 82.0)
...
```

## 10. Combiner — Why Averaging Needs Care

It's tempting to add a `combine()` function to cut
down the amount of data shuffled across the network
(as you would for Word Count). But averaging is
**not** naively combinable: the average of a set of
partial averages is not, in general, equal to the
average of all the underlying values.

For example, suppose one mapper emits three readings
for `CA-Sunnyvale`: `[82, 76, 56]` — a local average
of `71.33`. A second mapper emits one reading:
`[98]` — a local average of `98.0`. Naively averaging
those two partial averages gives
`(71.33 + 98.0) / 2 = 84.67`, which is wrong: the
true average of all four readings is
`(82 + 76 + 56 + 98) / 4 = 78.0`.

The fix is to have the combiner emit a **partial sum
and a partial count**, never a partial average:

```text
# key: "<state_code>-<city_name>"
# values: Iterable<Integer> (local to one mapper)
combine(key, values) {
   count = 0
   sum = 0
   for (v in values) {
      count += 1
      sum += v
   }
   # emit a (sum, count) pair -- NOT an average
   emit(key, (sum, count))
}
```

The reducer then changes shape slightly: instead of
raw temperatures, it receives `(sum, count)` pairs
and combines those:

```text
# key: "<state_code>-<city_name>"
# values: Iterable<(sum, count)>
reduce(key, values) {
   total_sum = 0
   total_count = 0
   for ((s, c) in values) {
      total_sum += s
      total_count += c
   }
   average = total_sum / (1.0 * total_count)
   emit(key, average)
}
```

Because integer addition (of both the sums and the
counts) is **associative** and **commutative**, this
combiner is safe to apply zero, one, or many times,
in any order — the final result is unaffected.

## 11. Food for Thought

1. If we want to find average temperature per state
   (not by city name), how would you write `map()`
   and `reduce()` functions?

2. If we want to drop records that do not have proper
   state codes, how would you implement this filter?

3. If we want to drop records that do not have proper
   city names (either null or empty), how would you
   implement this filter?

4. If we want to output only records where average
   temperature is more than 20.00, how would you
   implement this filter?

5. If we want to find the median temperature per
   city, how would you implement this functionality?
   (Hint: unlike sum/count, the median cannot be
   computed incrementally with a simple combiner —
   why not?)

6. If we want to find (minimum, maximum, average)
   temperature per city, how would you implement
   this functionality?

7. Extend the `combine()`/`reduce()` pair from
   Section 10 so the final output is
   `(average, count)` — i.e., also report how many
   readings went into each city's average.

## 12. Comments

Comments and suggestions are welcome!

## 13. References

1. [Data-Intensive Text Processing with MapReduce by Jimmy Lin and Chris Dyer](https://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)

2. [A Very Brief Introduction to MapReduce by Diana MacLean](https://hci.stanford.edu/courses/cs448g/a2/files/map_reduce_tutorial.pdf)

3. [Introduction to MapReduce by Mahmoud Parsian](http://mapreduce4hackers.com/docs/Introduction-to-MapReduce.pdf)
