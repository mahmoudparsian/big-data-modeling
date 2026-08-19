# Classic Word Count

	Author: Mahmoud Parsian
	Last updated: 8/19/2026

## Goal

Build a complete MapReduce job that counts how many
times each word appears across a set of documents,
then extend it with filters at both the mapper and
the reducer.

## Complete MapReduce Job for Word Count

   1. INPUT: set of documents
   2. OUTPUT: frequency of words
   3. `map()` function
   4. `reduce()` function
   5. `combine()` function (OPTIONAL, controlled by the programmer — see Homework)

## Filters in MapReduce

   1. When to apply a filter to a mapper (see "REVISED map() with a filter" below)
   2. When to apply a filter to a reducer (see "REVISED reduce() with a filter" below)


## INPUT

~~~text
fox jumped
fox jumped and fox jumped
fox is here
fox is there
~~~

## Passing input to mappers

1. Input is split into chunks (records) and passed to mappers.
2. We assume that the partitioner passes each record to a
   mapper as a `(key, value)` pair, where:
   - `key`: the record number of the input, as an integer
   - `value`: the entire record, as a string

Therefore:

| Record # | Input line | Passed to a mapper as |
|---|---|---|
| 1 | `fox jumped` | `(1, "fox jumped")` |
| 2 | `fox jumped and fox jumped` | `(2, "fox jumped and fox jumped")` |
| 3 | `fox is here` | `(3, "fox is here")` |
| 4 | `fox is there` | `(4, "fox is there")` |


## Mapper function

~~~text
# THIS IS PSEUDO CODE
# key is a record number and is ignored
# value is the entire record of input
map(key, value) {
  # tokenize the given input record
  words = value.split(" ")

  # for each word generate (word, 1)
  for word in words {
     emit(word, 1)
  }
}
~~~

## Output of Mappers

~~~text
fox jumped =>
  (fox, 1),
  (jumped, 1)

fox jumped and fox jumped =>
  (fox, 1),
  (jumped, 1),
  (and, 1),
  (fox, 1),
  (jumped, 1)

fox is here =>
  (fox, 1),
  (is, 1),
  (here, 1)

fox is there =>
  (fox, 1),
  (is, 1),
  (there, 1)
~~~

## Output of Sort & Shuffle

The framework groups all values by key and hands each
reducer a `(key, Iterable<Integer>)` pair:

~~~text
(fox,    [1, 1, 1, 1, 1])   # i.e., (fox, Iterable<Integer>)
(jumped, [1, 1, 1])
(and,    [1])
(is,     [1, 1])
(here,   [1])
(there,  [1])
~~~


## Reducer function: LONGER VERSION

~~~text
# THIS IS PSEUDO CODE
# key is a word
# values : Iterable<Integer>
reduce(key, values) {
  total = 0

  # for each element in values
  for count in values {
     total += count
     # total = total + count
  }

  emit(key, total)
}
~~~

## Reducer function: SHORTER VERSION

~~~text
# key is a word
# values : Iterable<Integer>
reduce(key, values) {
  total = sum(values)
  emit(key, total)
}
~~~


## Output of Reducers

~~~text
(fox, 5)
(jumped, 3)
(and, 1)
(is, 2)
(here, 1)
(there, 1)
~~~


## REVISED map() with a filter

Filter: ignore words with fewer than 3 characters.

~~~text
# key is a record number and is ignored
# value is the entire record of input
map(key, value) {
  # tokenize the given input record
  words = value.split(" ")

  # for each word generate (word, 1)
  for word in words {
     if (len(word) > 2) {
        emit(word, 1)
     }
  }
}
~~~

With this filter, `is` (2 characters) is dropped by every
mapper before the shuffle even happens — less data crosses
the network.

## REVISED reduce() with a filter

Filter: drop a word from the output if its total frequency
is less than 2 (i.e., keep only words that repeat).

~~~text
# key is a word
# values : Iterable<Integer>
reduce(key, values) {
  total = sum(values)
  if (total > 1) {
     emit(key, total)
  }
}
~~~

## Output after both filters are applied

~~~text
(fox, 5)
(jumped, 3)
~~~

`and`, `here`, and `there` disappear because each occurs
only once; `is` never even reaches the reducer because the
mapper filter removed it first.

## Mapper filter vs. reducer filter — why it matters

| | Mapper filter | Reducer filter |
|---|---|---|
| Decision is based on | a single word (local, no context) | the *aggregated* count across all documents (global) |
| Effect | reduces data volume early, before shuffle | reduces output volume, after aggregation |
| Can it decide "top N" / "frequency >= k"? | No — a mapper never sees the global count | Yes — the reducer sees every value for the key |

Rule of thumb: filter in the **mapper** when the decision
only needs the record/word itself (e.g., stop-word removal,
length checks); filter in the **reducer** when the decision
depends on an aggregate (e.g., minimum frequency, top-K).

## Homework

   1. Write a `combine()` function for word count.
      (Hint: word count is the *easy* case for combining —
      contrast it with the Average Temperature example,
      where naively combining partial *averages* produces
      the wrong answer; see the "Combiner" section of
      `MapReduce_Find_Average_Temperature.md`. Why does
      summing not have that problem?)
   2. Argue why your combiner is correct: since integer
      addition is both **associative** and **commutative**,
      partial sums computed by a combiner on a mapper's
      local output can be safely re-summed by the reducer
      and still produce the same final total.
   3. Does the reducer filter (`total > 1`) still work
      correctly if a combiner is used? Why or why not?
   4. The mapper tokenizes with `value.split(" ")`, which
      treats `"Fox"` and `"fox"` as different words and
      breaks on multiple consecutive spaces or punctuation
      (e.g., `"fox,"` would never match `"fox"`). Revise
      `map()` to lowercase each token and strip punctuation
      before emitting it. Would that change the counts in
      this example's output? Why or why not?
