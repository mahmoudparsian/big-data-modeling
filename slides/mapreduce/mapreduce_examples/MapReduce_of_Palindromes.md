# MapReduce of Palindromes

	Author: Mahmoud Parsian
	Last updated: 8/19/2026

## Problem

Given a set of text documents, the goal is to
find the frequency of palindromes in these
documents. What is a palindrome? A palindrome
is a word that reads the same backward as
forward, e.g., "madam" or "refer".

## Python function `is_palindrome()`

Given a string, we write a Python function to
check whether it is a palindrome. A string is
a palindrome if the reverse of the string is
the same as the string. For example, "radar"
is a palindrome, but "radix" is not.

```python
# find the reverse of the string and check
# whether the reverse and the original are the same
def is_palindrome(s):
    if s is None:
        return False
    return s == s[::-1]
```

Note: this simple check assumes the token is
already lowercase and free of punctuation
(e.g., "Madam," would *not* be recognized as a
palindrome, because of the capital "M" and the
trailing comma). For real-world text you would
normalize each token — strip punctuation and
lowercase it — before calling `is_palindrome()`.
See Homework question 3 below.

## Sample Input

```text
today level ok dont civic madam is madam
tomorrow level madam civic yes level
there is no palindromes in this record except madam
```

## Mapper

```text
# pseudo-code:
# assume that k is a record number of the input file, ignored
# assume that v is the entire input record
map(k, v) {
    # split input record by space
    words = v.split(" ")
    for (w in words) {
        if (is_palindrome(w)) {
            # w is a palindrome
            emit(w, 1)
        }
    }
}
```

## Output of Mappers

```text
(level, 1)
(civic, 1)
(madam, 1)
(madam, 1)
(level, 1)
(madam, 1)
(civic, 1)
(level, 1)
(madam, 1)
```

## Output of Sort and Shuffle

```text
(level, [1, 1, 1])
(civic, [1, 1])
(madam, [1, 1, 1, 1])
```

## Reducer

```text
# pseudo-code:
# key is a unique palindrome
# values is an Iterable<Integer>
reduce(key, values) {
    count = 0
    for (v in values) {
        # sum of the counts for this palindrome
        count += v
    }
    # emit the palindrome and its total count
    emit(key, count)
}
```

## Output of Reducers

```text
(level, 3)
(civic, 2)
(madam, 4)
```

## Homework

1. Write a `combine()` function for this
   MapReduce job.

2. Argue why your combiner is correct. (Hint:
   since integer addition is both
   **associative** and **commutative**, partial
   counts computed by a combiner on a mapper's
   local output can be safely re-summed by the
   reducer, in any order, and still produce the
   same final total — the same argument used for
   Word Count.)

3. `is_palindrome()` above is case-sensitive and
   punctuation-sensitive, and it also treats the
   empty string and every single-character word
   as a (trivial) palindrome. Rewrite the mapper
   so that it: (a) lowercases each token and
   strips leading/trailing punctuation before
   testing it, and (b) ignores tokens shorter
   than 2 characters. Would this change the
   sample output above? Why or why not?
