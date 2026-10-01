# MapReduce of Palindromes

	Author: Mahmoud Parsian
	Last updated: 8/19/2026

## 1. Problem

* Given a set of text documents, the goal is to
find the frequency of palindromes in these
documents. 
* What is a palindrome? A palindrome
is a word that reads the same backward as
forward, e.g., "madam" or "refer".

## 2. Application of Palindromes

WWhile palindromes often seem like just a word 
game, they play critical roles across several 
real-world fields:

### 1. Genetics & Molecular Biology (Biotech)

In biology, a palindromic sequence occurs when 
a double-stranded DNA sequence reads the same 
forwards on one strand as it does backwards 
on the complementary strand.

* **Restriction Enzymes:** Bacteria produce restriction enzymes that recognize specific palindromic DNA sequences to cut foreign viral DNA. Genetic engineering relies heavily on this mechanism to cut and edit DNA segments precisely.
* **CRISPR-Cas9:** The "PR" in CRISPR stands for **P**alindromic **R**epeats. These repeated palindromic sequences act as an immune memory system in bacteria, forming the basis of modern gene editing.
* **RNA Structure:** Single-stranded RNA can fold back on itself at palindromic points to form "hairpin loops," which dictate how the RNA functions and stabilizes inside a cell.

### 2. Computer Science & Software Engineering

Palindromes are foundational concepts for testing and building computational algorithms:

* **Text Processing & Search Engines:** Detecting palindromic structures helps test string manipulation algorithms, dynamic programming, and sliding-window techniques.
* **Data Integrity & Compression:** Certain data structures (like suffix trees and Manacher’s Algorithm) use palindrome detection to identify symmetric repeating patterns in massive strings, which is used in biological sequence analysis and data compression.
* **Formal Language Theory:** Palindromes are classic examples used to demonstrate the limits of *Context-Free Grammars* (CFGs) in theoretical computer science.

### 3. Cryptography & Security

* **Hash Collisions & Symmetric Ciphers:** Palindromic properties are analyzed in cryptographic hashing and block cipher design to identify structural weaknesses or unwanted symmetries in encryption algorithms.

---


## 3. Python function

Given a string, we write a Python function to
check whether it is a palindrome. A string is
a palindrome if the reverse of the string is
the same as the string. For example, "radar"
is a palindrome, but "radix" is not.

```python
# find the reverse of the string and check
# whether the reverse and the original are the same
def is_palindrome(s: str) -> bool:
    """Check if a string is a palindrome, ignoring case."""
    if s is None:
        return False
    lowercased = s.lower()
    return lowercased == lowercased[::-1]
```

Note: this simple check assumes the token is
already lowercase and free of punctuation
(e.g., "Madam," would *not* be recognized as a
palindrome, because of the capital "M" and the
trailing comma). For real-world text you would
normalize each token — strip punctuation and
lowercase it — before calling `is_palindrome()`.
See Homework question 3 below.

## 4. Sample Input

```text
today level ok dont civic madam is madam
tomorrow level madam civic yes level
there is no palindromes in this record except madam
```

## 5. Mapper

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

## 6. Output of Mappers

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

## 7. Output of Sort and Shuffle

```text
(level, [1, 1, 1])
(civic, [1, 1])
(madam, [1, 1, 1, 1])
```

## 8. Reducer

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

## 9. Output of Reducers

```text
(level, 3)
(civic, 2)
(madam, 4)
```

## 10. Homework

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
