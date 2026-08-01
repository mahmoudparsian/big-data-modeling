# Associativity, Commutativity, and Reducers

> **A practical algebra of MapReduce reducers**
> Why some functions parallelize for free, why others quietly return wrong
> answers, and how to repair the ones that don't.

---

## Table of Contents

1. [Why This Matters](#1-why-this-matters)
2. [Notation and Setup](#2-notation-and-setup)
3. [The Laws, Formalized](#3-the-laws-formalized)
4. [Algebraic Structures: From Magma to Commutative Monoid](#4-algebraic-structures-from-magma-to-commutative-monoid)
5. [Two Fundamental Theorems](#5-two-fundamental-theorems)
6. [Why MapReduce Needs Both Laws](#6-why-mapreduce-needs-both-laws)
7. [The Combiner Correctness Theorem](#7-the-combiner-correctness-theorem)
8. [A Catalog of Reduction Functions](#8-a-catalog-of-reduction-functions)
9. [The Lifting Recipe: Repairing a Broken Reducer](#9-the-lifting-recipe-repairing-a-broken-reducer)
10. [Master Summary Table](#10-master-summary-table)
11. [Algebraic, Distributive, and Holistic Aggregates](#11-algebraic-distributive-and-holistic-aggregates)
12. [The Floating-Point Caveat](#12-the-floating-point-caveat)
13. [Testing Your Reducer](#13-testing-your-reducer)
14. [Pitfalls and a Design Checklist](#14-pitfalls-and-a-design-checklist)
15. [Exercises](#15-exercises)

---

## 1. Why This Matters

In MapReduce, you never control **how** your values are combined. You only
supply a binary function; the framework decides:

* **how to group** them — combiners run on mappers, partial merges run during
  the shuffle, the final merge runs on the reducer. Every run may group
  differently.
* **in what order** they arrive — mapper outputs cross the network
  concurrently, spill files merge in arbitrary sequence, and speculative
  execution may produce a duplicate attempt that finishes first.

So the framework silently reserves the right to evaluate your reduction as

```
f(f(f(v1, v2), v3), v4)     

 or      

f(f(v1, v2), f(v3, v4))
```

or, after a reshuffle,

```
f(f(v3, v1), f(v4, v2))
```

If all of these must yield the same answer, then your function **must** be
associative (grouping doesn't matter) and commutative (order doesn't matter).
That is the entire subject of this document.

**The punchline up front:**

> A reduction is safe to run in parallel **iff** the pair `(S, f)` forms a
> **commutative semigroup** — and safe to use with a combiner and an empty
> partition **iff** it forms a **commutative monoid**.

Everything else here is elaboration: the definitions, the proofs, a catalog of
which real functions qualify, and a mechanical recipe for fixing the ones that
don't.

### The dataflow that forces the requirement

```
              ┌──────────┐   ┌──────────┐   ┌──────────┐
   Input  ──▶ │  map()   │   │  map()   │   │  map()   │     (parallel, N nodes)
              └────┬─────┘   └────┬─────┘   └────┬─────┘
                   │              │              │
              ┌────▼─────┐   ┌────▼─────┐   ┌────▼─────┐
              │combine() │   │combine() │   │combine() │     ← f applied EARLY,
              └────┬─────┘   └────┬─────┘   └────┬─────┘       on a SUBSET
                   │              │              │
                   └──────┬───────┴──────┬───────┘
                          │   SHUFFLE    │                   ← ARBITRARY ORDER
                     ┌────▼─────┐   ┌────▼─────┐
                     │ reduce() │   │ reduce() │              ← f applied to
                     └──────────┘   └──────────┘                partial results
```

Notice that `f` is applied at three different levels to three different
subsets, in an order nobody promised you. Associativity licenses the
re-grouping; commutativity licenses the re-ordering.

---

## 2. Notation and Setup

Let:

* $S$ — the set of values being reduced (integers, floats, tuples, sets, …)
* $f : S \times S \to S$ — a **binary operation**, written infix as $a \ast b$
* $V = [v_1, v_2, \ldots, v_n]$ — the list of values arriving for one key

A **reducer** computes a fold of $f$ over $V$:

$$
\text{reduce}(f, V) \;=\; v_1 \ast v_2 \ast \cdots \ast v_n
$$

This expression is **only well-defined** once we know the laws below hold —
otherwise it is ambiguous, because we have not said where the parentheses go
or which order the $v_i$ appear in.

**Two failure modes to keep separate in your head:**

| Failure | Symptom | Reproducible? |
|---|---|---|
| Not associative | Answer depends on how the framework *grouped* the data (partition count, combiner on/off) | Sometimes — changes when you change cluster size |
| Not commutative | Answer depends on the *order* values arrived over the network | **No** — nondeterministic, different on every run |

The second is the nastier bug: it passes your unit tests and fails in
production, differently each time.

---

## 3. The Laws, Formalized

### 3.1 Closure (Law 0)

$$
\forall a, b \in S : \quad a \ast b \in S
$$

The operation never leaves the set. This is easy to overlook and it is what
breaks the naive average: `avg: ℝ × ℝ → ℝ` is closed, but the *semantic* type
"the average of a list" is not preserved by pairwise averaging. Closure is why
we will later switch the carrier set from `ℝ` to `ℝ × ℕ`.

```python
# Closed:      max: int × int -> int
# NOT closed:  len: str × str -> int      (result can't be fed back in)
```

### 3.2 Associativity

$$
\forall a, b, c \in S : \quad (a \ast b) \ast c \;=\; a \ast (b \ast c)
$$

**Read it as:** *grouping* is irrelevant. Parentheses may be moved freely as
long as the left-to-right sequence of operands is preserved.

```
   (a * b) * c   ==   a * (b * c)

        *                  *
       / \                / \
      *   c      ==      a   *
     / \                    / \
    a   b                  b   c
```

**Examples:**

$$(2 + 3) + 4 = 9 = 2 + (3 + 4) \qquad (5 \times 2) \times 3 = 30 = 5 \times (2 \times 3)$$

**Counterexample (subtraction):**

$$(10 - 3) - 2 = 5 \qquad\text{but}\qquad 10 - (3 - 2) = 9$$

### 3.3 Commutativity

$$
\forall a, b \in S : \quad a \ast b \;=\; b \ast a
$$

**Read it as:** *order* is irrelevant. Operands may be swapped.

**Counterexample (string concatenation):**

```python
"ab" + "cd"  # 'abcd'
"cd" + "ab"  # 'cdab'   ← different
```

Note that string concatenation **is** associative — this shows the two laws are
genuinely independent, which is the point of the next table.

### 3.4 The two laws are independent

| | Commutative | Not commutative |
|---|---|---|
| **Associative** | `+`, `×`, `max`, `min`, `∪`, `∩`, `AND`, `OR`, `XOR`, `gcd` | string `+`, list `+`, matrix `×`, function `∘` |
| **Not associative** | `avg(a,b)=(a+b)/2`, `a*b = a²+b²`, NAND | `−`, `÷`, `**`, `a*b = a` (first) |

Every quadrant is populated. You must check **both** laws; neither implies the
other.

### 3.5 Identity element

An element $e \in S$ is an **identity** (neutral element) if

$$
\forall a \in S : \quad e \ast a \;=\; a \ast e \;=\; a
$$

**Why a reducer cares:** the identity is what a combiner returns for an
**empty partition**, and it is the seed of `fold` / `aggregate` /
`aggregateByKey`. Without one you must use `reduce` (which fails on empty
input) instead of `fold` (which does not).

| Operation | Identity $e$ |
|---|---|
| `+` | `0` |
| `×` | `1` |
| `max` | `-∞` (`float('-inf')`, or `Integer.MIN_VALUE`) |
| `min` | `+∞` (`float('inf')`, or `Integer.MAX_VALUE`) |
| `∪` (set union) | `∅` |
| `AND` | `True` |
| `OR` | `False` |
| string `+` | `""` |
| `(sum, count)` for average | `(0, 0)` |

The identity must be **two-sided** and it is **unique** when it exists:
if $e_1, e_2$ are both identities, then $e_1 = e_1 \ast e_2 = e_2$. ∎

### 3.6 Idempotence (a useful bonus law)

$$
\forall a \in S : \quad a \ast a \;=\; a
$$

`max`, `min`, `AND`, `OR`, `∪`, `∩` are idempotent. `+`, `×`, `count` are not.

**Why it matters in practice:** idempotent reducers are **immune to duplicate
delivery**. If speculative execution or an at-least-once message channel
delivers the same value twice, `max` still returns the right answer but `sum`
silently over-counts. This is the algebraic reason `max`-style aggregations are
so much easier to operate at scale than `sum`-style ones.

### 3.7 Summary of the axioms

```
Closure:        a * b ∈ S
Associativity:  (a * b) * c = a * (b * c)
Commutativity:  a * b = b * a
Identity:       e * a = a * e = a
Idempotence:    a * a = a               (optional; buys duplicate-safety)
Inverse:        a * a⁻¹ = e             (optional; buys incremental deletion)
```

**Inverse** deserves a note: if every element has an inverse, `(S, *)` is a
**group**, and you can *remove* a value from an aggregate without recomputing
it. `sum` has inverses (subtract), so a running sum supports deletions;
`max` does not, which is why deleting the current maximum from a windowed
aggregate forces a full recomputation.

---

## 4. Algebraic Structures: From Magma to Commutative Monoid

The laws stack into a hierarchy. Each level buys you a specific capability in
a distributed system.

```
  Magma                (closure only)
    │  + associativity
    ▼
  Semigroup            ✅ safe to re-group  → parallel tree reduction OK
    │  + identity
    ▼
  Monoid               ✅ safe on empty input → combiners & fold OK
    │  + commutativity
    ▼
  Commutative Monoid   ✅ safe in any order  → MapReduce-safe. THE TARGET.
    │  + inverses
    ▼
  Abelian Group        ✅ safe to subtract   → incremental / windowed updates
```

| Structure | Axioms | What it unlocks in MapReduce |
|---|---|---|
| **Magma** | closure | Nothing. You must fix the evaluation order yourself. |
| **Semigroup** | + associative | Split the work into contiguous chunks and merge. Balanced tree reduction in $O(\log n)$ depth. |
| **Monoid** | + identity | Empty partitions are legal; `combiner` can emit a neutral value; `fold`/`aggregate` with a zero seed. |
| **Commutative monoid** | + commutative | **Fully order-independent.** Shuffle can deliver in any order; combiner is optional and can run 0, 1, or many times. |
| **Abelian group** | + inverse | Incremental maintenance: add and *remove* values from a running aggregate. |

> **Design rule of thumb:** *Aim your reducer at "commutative monoid." If you
> can't get there directly, change the value type until you can* (§9).

See also `monoids/Monoids_as_a_Design_Principle_for_Efficient_MapReduce_Algorithms.pdf`
in this repository.

---

## 5. Two Fundamental Theorems

These are the two results that make MapReduce work. Everything else is
bookkeeping.

### Theorem 1 (Generalized Associativity Law)

> **If $\ast$ is associative on $S$, then for any $v_1, \ldots, v_n \in S$, all
> ways of parenthesizing $v_1 \ast v_2 \ast \cdots \ast v_n$ yield the same
> value** (the operand *sequence* being fixed).

**Proof (strong induction on $n$).**

*Base cases.* $n = 1, 2$: only one parenthesization exists. $n = 3$: the two
parenthesizations agree by the associativity axiom.

*Inductive step.* Assume the claim for all lengths $< n$. Any parenthesized
product $P$ of $v_1 \ldots v_n$ has a topmost operation splitting it at some
$k$:

$$P = L_k \ast R_k, \quad L_k = (v_1 \cdots v_k), \; R_k = (v_{k+1} \cdots v_n)$$

By the inductive hypothesis $L_k$ and $R_k$ each have a unique value. Define
the **left-nested normal form**

$$N = (\cdots((v_1 \ast v_2) \ast v_3) \cdots \ast v_n)$$

It suffices to show $P = N$ for every $k$. Induct on $n - k$. If $k = n-1$,
then $R_k = v_n$ and $L_k = N_{n-1}$ by the inductive hypothesis, so
$P = N_{n-1} \ast v_n = N$. If $k < n-1$, write $R_k = R' \ast v_n$ (again by
the inductive hypothesis, $R_k$ equals its own left-nested form). Then

$$
P = L_k \ast (R' \ast v_n) \;\overset{\text{assoc}}{=}\; (L_k \ast R') \ast v_n
$$

and $L_k \ast R'$ is a product of $v_1 \ldots v_{n-1}$, which by the inductive
hypothesis equals $N_{n-1}$. Hence $P = N_{n-1} \ast v_n = N$. ∎

**Consequence.** Writing $v_1 \ast v_2 \ast \cdots \ast v_n$ *without*
parentheses is legitimate, and a reducer may build the answer as a balanced
binary tree of depth $\lceil \log_2 n \rceil$ instead of a linear chain — the
entire basis of parallel reduction.

```
Linear fold (depth n-1)          Tree reduction (depth ⌈log₂ n⌉)
  ((((v1+v2)+v3)+v4)+v5)          ((v1+v2) + (v3+v4)) + v5

  Sequential: O(n) steps          Parallel: O(log n) steps
```

### Theorem 2 (Order Independence)

> **If $\ast$ is associative *and* commutative, then for every permutation
> $\sigma$ of $\{1, \ldots, n\}$:**
> $$v_1 \ast v_2 \ast \cdots \ast v_n \;=\; v_{\sigma(1)} \ast v_{\sigma(2)} \ast \cdots \ast v_{\sigma(n)}$$

**Proof (induction on $n$).** For $n \le 2$ this is exactly commutativity.
Assume it for $n-1$. Let $\sigma$ be a permutation of $n$ elements and let
$j = \sigma^{-1}(n)$ be the position holding $v_n$. Using Theorem 1 to drop
parentheses freely:

$$
v_{\sigma(1)} \cdots v_{\sigma(n)}
= \underbrace{(v_{\sigma(1)} \cdots v_{\sigma(j-1)})}_{A} \ast v_n \ast \underbrace{(v_{\sigma(j+1)} \cdots v_{\sigma(n)})}_{B}
$$

By commutativity $v_n \ast B = B \ast v_n$, so the whole expression equals
$(A \ast B) \ast v_n$. Now $A \ast B$ is a product of $v_1, \ldots, v_{n-1}$ in
some order, which by the inductive hypothesis equals
$v_1 \ast \cdots \ast v_{n-1}$. Therefore the expression equals
$(v_1 \ast \cdots \ast v_{n-1}) \ast v_n$, which is the identity permutation's
value. ∎

**Corollary (the MapReduce guarantee).** If $(S, \ast)$ is a commutative
semigroup, then for *any* partition of the multiset $V$ into disjoint blocks
$B_1, \ldots, B_m$ and *any* order within and among blocks:

$$
\bigoplus_{v \in V} v \;=\; \Big(\bigoplus_{v \in B_1} v\Big) \ast \cdots \ast \Big(\bigoplus_{v \in B_m} v\Big)
$$

This is precisely what a combiner does. It is correct **because of, and only
because of, these two theorems.**

**Practical restatement:** the reduction result is a function of the
**multiset** of values, not of the sequence.

$$
\text{reduce}(f, V) = g(\text{multiset}(V))
$$

---

## 6. Why MapReduce Needs Both Laws

Four independent sources of nondeterminism, each demanding one of the laws:

| Source | Which law rescues you |
|---|---|
| **Combiners** run on a mapper-local subset — you don't know which values got pre-combined | Associativity |
| **Partition count** changes between runs (10 reducers vs. 100) → different groupings | Associativity |
| **Shuffle arrival order** — mapper outputs race across the network | Commutativity |
| **Speculative execution / retries** — a duplicate attempt may win, and spill files merge in arbitrary order | Commutativity (+ idempotence for duplicate-safety) |

### Concrete demonstration

```python
from functools import reduce
import itertools

def order_independent(f, xs):
    """Brute-force: is reduce(f, ·) invariant under all permutations?"""
    base = reduce(f, xs)
    return all(reduce(f, p) == base for p in itertools.permutations(xs))

xs = [3, 1, 4, 1, 5]

order_independent(lambda a, b: max(a, b),  xs)   # True   ✅ safe
order_independent(lambda a, b: a + b,      xs)   # True   ✅ safe
order_independent(lambda a, b: a - b,      xs)   # False  ❌ unsafe
order_independent(lambda a, b: a,          xs)   # False  ❌ unsafe ("first")
order_independent(lambda a, b: (a + b) / 2, xs)  # False  ❌ unsafe (naive avg)
```

Those last three are exactly the reducers that "work on my laptop" and then
return a different number on every cluster run.

### Note on Spark's `reduceByKey` and `aggregateByKey`

Spark's documentation and Hadoop's `Reducer` contract both **assume without
checking** that your function is associative and commutative. Nothing validates
it. A non-commutative `reduceByKey` compiles, runs, produces a number, and is
wrong — nondeterministically. The type system will not save you; the algebra
must.

> **On decidability.** Deciding whether an *arbitrary* user-supplied reducer is
> commutative is undecidable in general (a consequence of Rice's theorem: it is
> a non-trivial semantic property of a program). Practical tools therefore rely
> on (a) restricting reducers to a known-good algebraic vocabulary, (b)
> property-based / randomized testing (§13), or (c) symbolic analysis of a
> restricted reducer language.

---

## 7. The Combiner Correctness Theorem

A combiner is an optimization the framework may apply **zero, one, or many
times**, on **arbitrary subsets**. So its correctness condition is stronger
than a reducer's.

> **Theorem (Combiner Safety).** Let `reduce` be the reducer and `combine` the
> combiner. It is safe to install `combine` as a combiner **iff** for every
> multiset $V$ and every partition $V = B_1 \uplus \cdots \uplus B_m$:
> $$\text{reduce}(V) \;=\; \text{reduce}\big(\{\,\text{combine}(B_1), \ldots, \text{combine}(B_m)\,\}\big)$$

**Corollary.** The condition holds automatically when `combine == reduce` and
$(S, \ast)$ is a **commutative monoid**. (Proof: immediate from the Corollary
to Theorem 2; the identity handles $B_i = \emptyset$.)

**When `combine != reduce`.** For non-monoid aggregations you split into three
functions — this is exactly the shape of Spark's `combineByKey` /
`aggregateByKey` and of the classic **map → combine → reduce** pattern:

| Function | Type | Role |
|---|---|---|
| `lift` (createCombiner) | $A \to M$ | map a raw value into the monoid |
| `merge` (mergeValue/mergeCombiners) | $M \times M \to M$ | the **commutative monoid** operation |
| `finalize` (mapValues) | $M \to B$ | project the accumulator to the answer |

This is a **homomorphism** into a monoid $M$ followed by a projection. The
monoid $M$ — not the answer type $B$ — is what must satisfy the laws. That
single observation solves average, variance, top-N, and most everything else in
§8 and §9.

```
   raw values  ──lift──▶  M  ──merge (assoc + commut)──▶  M  ──finalize──▶  answer
      (A)                                                            (B)
                          ↑
                 ALL the algebra lives here
```

**Aggressive test for combiner safety:** run the combiner **twice** and on
**singleton** partitions. If `reduce([f(v)]) != f(v)`, or applying the combiner
to already-combined output changes the result, your combiner is unsafe.

---

## 8. A Catalog of Reduction Functions

For each function: the operation, whether it satisfies each law, its identity,
and working code. ✅ = holds, ❌ = fails.

---

### 8.1 SUM — the canonical commutative monoid

$$a \ast b = a + b \qquad e = 0$$

| Assoc | Commut | Identity | Idempotent | Inverse | Structure |
|:---:|:---:|:---:|:---:|:---:|---|
| ✅ | ✅ | ✅ `0` | ❌ | ✅ | Abelian **group** |

**Proof of associativity:** $(a+b)+c = a+b+c = a+(b+c)$ by the field axioms of
$\mathbb{R}$. **Commutativity:** $a+b = b+a$. ∎

```python
# Plain Python
from functools import reduce
reduce(lambda a, b: a + b, [1, 2, 3, 4, 5], 0)      # 15

# PySpark
rdd.reduceByKey(lambda a, b: a + b)
rdd.foldByKey(0, lambda a, b: a + b)                 # identity 0 makes fold legal
```

**Because it has inverses**, a running sum supports deletion: `total -= v`.
**Because it is not idempotent**, duplicate delivery corrupts it.

---

### 8.2 MAX — associative, commutative, idempotent

$$a \ast b = \max(a, b) \qquad e = -\infty$$

| Assoc | Commut | Identity | Idempotent | Inverse | Structure |
|:---:|:---:|:---:|:---:|:---:|---|
| ✅ | ✅ | ✅ `-∞` | ✅ | ❌ | Idempotent commutative monoid (a **semilattice**) |

**Proof of associativity.** For a totally ordered set,
$\max(\max(a,b),c)$ and $\max(a,\max(b,c))$ both equal the unique largest of
$\{a,b,c\}$: each is $\ge$ all three elements and is itself one of them, and in
a total order such an element is unique. **Commutativity** is immediate since
$\max$ depends only on the set $\{a,b\}$. **Idempotence:**
$\max(a,a)=a$. ∎

```python
# Plain Python
reduce(lambda a, b: max(a, b), [3, 9, 2, 7], float('-inf'))   # 9

# PySpark
rdd = sc.parallelize([("alice", 8000), ("bob", 12000),
                      ("alice", 10000), ("bob", 9000), ("carol", 9500)])

max_by_key = rdd.reduceByKey(lambda a, b: max(a, b))
# [('alice', 10000), ('bob', 12000), ('carol', 9500)]
```

**Note the identity choice.** `-∞` works for floats. For 64-bit integers use
`Long.MIN_VALUE`; for strings use `""` (with lexicographic order); for a
general poset, no identity may exist — then use `Option`/`None` as a
synthetic identity:

```python
def max_opt(a, b):
    if a is None: return b
    if b is None: return a
    return max(a, b)
# (S ∪ {None}, max_opt) is a commutative monoid with identity None — always.
```

That `Option` trick works for **any** semigroup: it freely adjoins an identity,
turning any semigroup into a monoid. Formally, $S^1 = S \cup \{e\}$.

**Idempotence pays off:** `max` is safe under at-least-once delivery,
speculative execution, and re-processing the same input file twice.

---

### 8.3 MIN — the dual of MAX

$$a \ast b = \min(a, b) \qquad e = +\infty$$

| Assoc | Commut | Identity | Idempotent | Inverse | Structure |
|:---:|:---:|:---:|:---:|:---:|---|
| ✅ | ✅ | ✅ `+∞` | ✅ | ❌ | Semilattice |

Everything in §8.2 applies with the order reversed.

```python
rdd.reduceByKey(lambda a, b: min(a, b))
reduce(lambda a, b: min(a, b), [3, 9, 2, 7], float('inf'))     # 2
```

**Combining MIN and MAX in one pass** — the pair monoid
$(\mathbb{R} \times \mathbb{R}, (\min, \max))$ is itself a commutative monoid,
because **the product of two commutative monoids is a commutative monoid**
(componentwise). This is a general and extremely useful fact:

```python
# lift:  v -> (v, v)
# merge: componentwise min and max
minmax = (rdd.mapValues(lambda v: (v, v))
             .reduceByKey(lambda a, b: (min(a[0], b[0]), max(a[1], b[1]))))
# identity: (+inf, -inf)
```

> **Product Monoid Theorem.** If $(M_1, \ast_1, e_1)$ and $(M_2, \ast_2, e_2)$
> are commutative monoids, then $(M_1 \times M_2, \ast, (e_1, e_2))$ with
> $(a_1,a_2) \ast (b_1,b_2) = (a_1 \ast_1 b_1,\; a_2 \ast_2 b_2)$ is a
> commutative monoid. **Proof:** each axiom is checked componentwise. ∎
>
> **This is your main tool.** Need five aggregates in one pass? Take the
> product of five monoids.

---

### 8.4 COUNT — sum of ones

$$\text{lift}(v) = 1, \qquad a \ast b = a + b, \qquad e = 0$$

| Assoc | Commut | Identity | Idempotent | Structure |
|:---:|:---:|:---:|:---:|---|
| ✅ | ✅ | ✅ `0` | ❌ | Commutative monoid $(\mathbb{N}, +, 0)$ |

`count` is not a binary operation on the *values*; it is a **homomorphism**
$\text{lift}: A \to \mathbb{N}$ followed by the sum monoid. This is the
simplest instance of the §7 pattern and the template for everything harder.

```python
rdd.mapValues(lambda v: 1).reduceByKey(lambda a, b: a + b)
```

---

### 8.5 AVERAGE — ❌ the classic trap, and its fix

**Naive version (WRONG):**

$$a \ast b = \frac{a+b}{2}$$

| Assoc | Commut | Identity | Structure |
|:---:|:---:|:---:|---|
| ❌ | ✅ | ❌ | Commutative **magma** only |

**Counterexample (disproof of associativity):**

$$
\text{avg}(\text{avg}(1,2),\,3) = \text{avg}(1.5, 3) = \mathbf{2.25}
$$
$$
\text{avg}(1,\,\text{avg}(2,3)) = \text{avg}(1, 2.5) = \mathbf{1.75}
$$

$2.25 \ne 1.75$, so $\ast$ is **not associative**. The true mean of
$\{1,2,3\}$ is $2$ — *neither* answer is right. ∎

Intuitively: pairwise averaging **implicitly re-weights** the data. The value
`3` gets weight $\tfrac12$ while `1` and `2` get $\tfrac14$ each. Averaging
loses the count, and the count is exactly the information the merge step needs.

**The fix — lift to the pair monoid $(\mathbb{R} \times \mathbb{N}, +, (0,0))$:**

$$
\text{lift}(v) = (v, 1), \qquad
(s_1, c_1) \ast (s_2, c_2) = (s_1 + s_2,\; c_1 + c_2), \qquad
\text{finalize}(s, c) = s / c
$$

| Assoc | Commut | Identity | Structure |
|:---:|:---:|:---:|---|
| ✅ | ✅ | ✅ `(0, 0)` | Commutative monoid (product of two sum monoids) |

**Proof:** it is the product of $(\mathbb{R},+,0)$ and $(\mathbb{N},+,0)$;
apply the Product Monoid Theorem. ∎

Note that `finalize` sits **outside** the monoid — division is neither
associative nor commutative, which is precisely why it must not appear in the
merge function.

```python
# ---- Plain Python ----
from functools import reduce
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
s, c = reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]),
              [(v, 1) for v in data],
              (0, 0))
s / c                                  # 5.5   ✅ correct for any grouping/order

# ---- PySpark: combineByKey ----
sum_count = rdd.combineByKey(
    lambda v:        (v, 1),                            # lift
    lambda acc, v:   (acc[0] + v,      acc[1] + 1),     # merge value into acc
    lambda a, b:     (a[0] + b[0],     a[1] + b[1]),    # merge two accs
)
avg_by_key = sum_count.mapValues(lambda x: x[0] / x[1])  # finalize

# ---- PySpark: aggregateByKey (identity makes this legal) ----
avg_by_key = (rdd.aggregateByKey(
                  (0, 0),                                       # identity e
                  lambda acc, v: (acc[0] + v, acc[1] + 1),
                  lambda a, b:   (a[0] + b[0], a[1] + b[1]))
                 .mapValues(lambda x: x[0] / x[1]))

# ---- PySpark: mapValues + reduceByKey (equivalent, often clearest) ----
avg_by_key = (rdd.mapValues(lambda v: (v, 1))
                 .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
                 .mapValues(lambda sc: sc[0] / sc[1]))
```

**Verified:** with `[("alice",8000),("bob",12000),("alice",10000),("bob",9000),
("carol",9500),("alice",7000)]` split across two partitions with a combiner,
the monoid version yields `alice: 8333.33, bob: 10500.0, carol: 9500.0` —
identical to the single-machine ground truth, and identical for every
partitioning.

> **⚠️ Beware "mean of means."** Averaging per-partition averages is the same
> error in disguise. It is correct **only** when all partitions have equal
> size — which the framework never guarantees.

---

### 8.6 VARIANCE and STANDARD DEVIATION

Two monoid formulations. Both work; they trade numerical stability for
simplicity.

#### (a) Sufficient-statistics monoid (simple, less stable)

$$
\text{lift}(v) = (1,\; v,\; v^2), \qquad
(n_1,s_1,q_1) \ast (n_2,s_2,q_2) = (n_1{+}n_2,\; s_1{+}s_2,\; q_1{+}q_2)
$$
$$
e = (0,0,0), \qquad
\text{finalize}(n,s,q) = \frac{q}{n} - \left(\frac{s}{n}\right)^{\!2}
$$

It is the product of three sum monoids ⟹ commutative monoid. ✅

```python
vals = [2, 4, 4, 4, 5, 5, 7, 9]
n, s, q = reduce(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]),
                 [(1, v, v*v) for v in vals], (0, 0, 0))
pop_var = q/n - (s/n)**2          # 4.0   ✅ matches Σ(v-μ)²/n
pop_std = pop_var ** 0.5          # 2.0
samp_var = (q - s*s/n) / (n - 1)  # Bessel-corrected
```

```python
# PySpark
stats = (rdd.mapValues(lambda v: (1, v, v*v))
            .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+b[2]))
            .mapValues(lambda t: (t[1]/t[0], t[2]/t[0] - (t[1]/t[0])**2)))  # (mean, var)
```

⚠️ `q/n - (s/n)²` **catastrophically cancels** when the mean is large relative
to the spread (e.g. values near $10^9$ with variance 1). Use (b) for
production.

#### (b) Chan–Golub–LeVeque merge monoid (numerically stable)

Carry $(n, \mu, M_2)$ where $M_2 = \sum (v_i - \mu)^2$:

$$
\begin{aligned}
n &= n_A + n_B \\
\delta &= \mu_B - \mu_A \\
\mu &= \mu_A + \delta \cdot \frac{n_B}{n} \\
M_2 &= M_{2,A} + M_{2,B} + \delta^2 \cdot \frac{n_A \, n_B}{n}
\end{aligned}
$$

$$\text{lift}(v) = (1, v, 0), \qquad e = (0, 0, 0), \qquad \text{finalize} = M_2/n$$

**This merge is associative and commutative** (the formula is symmetric in
$A \leftrightarrow B$: swapping negates $\delta$, and $\delta$ appears only
squared or multiplied by the *other* side's weight — the resulting $\mu$ is
$\frac{n_A\mu_A + n_B\mu_B}{n}$, manifestly symmetric).

```python
def chan_merge(A, B):
    nA, mA, M2A = A
    nB, mB, M2B = B
    n = nA + nB
    if n == 0:
        return (0, 0.0, 0.0)                 # identity, and empty-safe
    d = mB - mA
    return (n,
            mA + d * nB / n,
            M2A + M2B + d * d * nA * nB / n)

n, mean, M2 = reduce(chan_merge, [(1, float(v), 0.0) for v in vals])
# n=8, mean=5.0, population variance = M2/n = 4.0     ✅ matches (a)
```

This is exactly what Spark's `StatCounter` and `DataFrame.stddev` implement
internally.

---

### 8.7 TOP-N — ❌ naive, ✅ as a bounded-list monoid

There is no binary "top-N" on scalars. Lift to **sorted lists of length ≤ N**:

$$
\text{lift}(v) = [v], \qquad
A \ast B = \text{take}_N(\text{sortDesc}(A \mathbin{+\!\!+} B)), \qquad e = [\;]
$$

| Assoc | Commut | Identity | Idempotent | Structure |
|:---:|:---:|:---:|:---:|---|
| ✅ | ✅ | ✅ `[]` | ✅ | Commutative monoid on lists of length $\le N$ |

**Proof sketch of associativity.** For any multisets $A, B, C$,
$\text{top}_N(A \uplus B \uplus C)$ is determined by the $N$ largest elements
overall. Discarding elements outside the top $N$ of any sub-multiset can never
remove an element that belongs to the global top $N$ — if $x$ is in the global
top $N$ it is in the top $N$ of every sub-multiset containing it. Hence
truncation commutes with the merge, and both parenthesizations equal
$\text{top}_N(A \uplus B \uplus C)$. **Commutativity** follows because $\uplus$
is commutative and the result depends only on the multiset. ∎

```python
def topn_merge(a, b, n=3):
    return sorted(a + b, reverse=True)[:n]

parts = [[5, 3, 9], [12, 1, 7], [8, 2, 11]]
tops  = [sorted(p, reverse=True)[:3] for p in parts]   # local combiners
reduce(topn_merge, tops)                               # [12, 11, 9]
sorted(sum(parts, []), reverse=True)[:3]               # [12, 11, 9]  ✅ same
```

```python
# PySpark — top-3 salaries per key, bounded memory
top3 = (rdd.mapValues(lambda v: [v])
           .reduceByKey(lambda a, b: sorted(a + b, reverse=True)[:3]))
```

Use a bounded min-heap for large $N$ to keep the merge $O(|A|+|B|)$ rather
than $O(m \log m)$. Note the crucial property: **the accumulator stays bounded
at $N$**, so the combiner actually reduces network traffic. Compare with §8.9.

---

### 8.8 BOOLEANS, SETS, AND BITWISE OPERATIONS

All are textbook idempotent commutative monoids — the "free wins" of
distributed aggregation.

| Operation | $\ast$ | Identity | Assoc | Commut | Idem |
|---|---|---|:---:|:---:|:---:|
| Logical AND (`∀`) | `a and b` | `True` | ✅ | ✅ | ✅ |
| Logical OR (`∃`) | `a or b` | `False` | ✅ | ✅ | ✅ |
| Logical XOR | `a ^ b` | `False` | ✅ | ✅ | ❌ |
| Set union | `a \| b` | `set()` | ✅ | ✅ | ✅ |
| Set intersection | `a & b` | universe `U` | ✅ | ✅ | ✅ |
| Bitwise OR / AND | `a \| b`, `a & b` | `0` / `~0` | ✅ | ✅ | ✅ |
| GCD / LCM | `gcd(a,b)` | `0` / `1` | ✅ | ✅ | ✅ |

```python
# "Did any transaction for this account exceed $10,000?"
rdd.mapValues(lambda v: v > 10000).reduceByKey(lambda a, b: a or b)

# "Are ALL sensors for this device reporting healthy?"
rdd.mapValues(lambda v: v == 'OK').reduceByKey(lambda a, b: a and b)

# Set of distinct categories per key
rdd.mapValues(lambda v: {v}).reduceByKey(lambda a, b: a | b)
```

XOR is the interesting one: associative and commutative but **not** idempotent
(`a ^ a = 0`) — and it is its own inverse, making $(\{0,1\}^k, \oplus, 0)$ an
abelian group. That is why XOR checksums support incremental
add-and-remove.

---

### 8.9 DISTINCT COUNT — exact vs. approximate

**Exact** = the set-union monoid (§8.8) followed by `len`:

$$\text{lift}(v) = \{v\}, \qquad A \ast B = A \cup B, \qquad e = \emptyset, \qquad \text{finalize} = |\cdot|$$

✅ Algebraically perfect. ❌ Operationally dangerous: the accumulator grows with
cardinality, so the combiner no longer bounds memory or shuffle size. This is
the difference between an algebra that is *correct* and one that is *scalable*.

```python
rdd.mapValues(lambda v: {v}).reduceByKey(lambda a, b: a | b).mapValues(len)
```

**Approximate** — HyperLogLog. Its register array merges by **componentwise
max**, which is §8.2 lifted to vectors:

$$(R_A \ast R_B)[i] = \max(R_A[i],\, R_B[i])$$

✅ Associative, ✅ commutative, ✅ identity (all-zero registers),
✅ **idempotent**, and **fixed size** (a few KB regardless of cardinality).

```python
# PySpark
df.agg(approx_count_distinct("user_id", rsd=0.02))
```

The same trick underlies the whole family of mergeable sketches:

| Sketch | Answers | Merge operation |
|---|---|---|
| HyperLogLog | distinct count | componentwise `max` |
| Bloom filter | membership | bitwise `OR` |
| Count–Min | frequency | componentwise `+` |
| t-digest / KLL | quantiles | centroid/level merge |
| Theta sketch | set cardinality + union/intersect | keep smallest-$k$ hashes |

**Every one of them is engineered to be a commutative monoid.** That is not a
coincidence — it is the design requirement for anything that must run under
MapReduce.

---

### 8.10 MEDIAN and PERCENTILES — ❌ holistic

$$\text{median}(A \uplus B) \;\ne\; \text{some function of } \text{median}(A), \text{median}(B)$$

**Counterexample.** $A = [1,2,3]$, $B = [100, 200, 300000]$:

* $\text{median}(A) = 2$, $\text{median}(B) = 200$
* any symmetric combination of $2$ and $200$ (mean $= 101$, min $= 2$,
  max $= 200$) is wrong
* the true median of $[1,2,3,100,200,300000]$ is $(3+100)/2 = \mathbf{51.5}$

The failure is not about ordering — it is that a fixed-size summary
$(\text{median})$ is **not a sufficient statistic** for the merged median. ∎

| Approach | Structure | Cost |
|---|---|---|
| Full sort / `sortByKey` + index | — | exact, $O(n \log n)$, shuffles everything |
| Collect all values per key into a list, then median | free monoid (list concat) — associative but **not commutative**, and unbounded | exact, memory-bound |
| **t-digest / KLL sketch** | commutative monoid, bounded size | approximate, one pass ✅ |
| Histogram with fixed buckets | commutative monoid (vector `+`) | approximate, bounded ✅ |

```python
# Practical: approximate quantiles in Spark
df.approxQuantile("salary", [0.5, 0.9, 0.99], relativeError=0.01)
```

**Lesson:** when the exact answer has no bounded sufficient statistic, either
pay the full-shuffle price or accept a sketch. There is no third option.

---

### 8.11 FIRST / LAST — ❌ nondeterministic by construction

$$a \ast \text{first} \; b = a$$

| Assoc | Commut | Identity | Structure |
|:---:|:---:|:---:|---|
| ✅ | ❌ | — | **Left-zero semigroup** |

Associative but **not commutative**: `first(a,b) = a ≠ b = first(b,a)`. The
result therefore depends entirely on shuffle arrival order — it is
nondeterministic and will differ between runs.

```python
reduce(lambda a, b: a, [3, 1, 4])      # 3
reduce(lambda a, b: a, [4, 1, 3])      # 4   ← same multiset, different answer
```

**Fix:** make "first" well-defined by carrying the ordering key, which turns it
into an `argmin` — a genuine commutative monoid:

```python
# lift: (timestamp, value); merge: keep the smaller timestamp (argmin)
def earliest(a, b):
    return a if a[0] <= b[0] else b      # ✅ assoc + commut (total order on ts)

(rdd.mapValues(lambda r: (r.ts, r.value))
    .reduceByKey(earliest)
    .mapValues(lambda tv: tv[1]))
```

Ties must be broken deterministically (e.g. by a secondary id) or you are back
to nondeterminism.

**Argmax/argmin generally:** the pair $(\text{key}, \text{payload})$ with
"keep the larger key" is a commutative monoid **provided the tie-break is
total**. This is how you answer "which product had the highest revenue?"
rather than merely "what was the highest revenue?"

---

### 8.12 STRING / LIST CONCATENATION — ✅ associative, ❌ commutative

$$a \ast b = a \mathbin{+\!\!+} b, \qquad e = \texttt{""} \text{ or } []$$

| Assoc | Commut | Identity | Structure |
|:---:|:---:|:---:|---|
| ✅ | ❌ | ✅ `""` | **Free monoid** — the archetype of a non-commutative monoid |

```python
"ab" + "cd"      # 'abcd'
"cd" + "ab"      # 'cdab'    ← ❌ order-dependent
```

Safe under a *combiner* (associativity licenses re-grouping **within** an
ordered sequence) but **not** safe under a MapReduce shuffle, which reorders.

**Fix:** either sort by an explicit key before concatenating (Hadoop *secondary
sort*, or Spark `sortWithinPartitions` / a window function), or replace
concatenation with an order-free structure such as a **multiset/bag**:

```python
# ❌ order-dependent
rdd.reduceByKey(lambda a, b: a + "," + b)

# ✅ commutative: collect into a bag, sort at the end
from collections import Counter
(rdd.mapValues(lambda v: Counter([v]))
    .reduceByKey(lambda a, b: a + b)               # Counter + is commutative
    .mapValues(lambda c: ",".join(sorted(c.elements()))))
```

`Counter` addition is a commutative monoid — the **free commutative monoid**
on the alphabet, which is the order-free replacement for the free monoid.

---

### 8.13 SUBTRACTION, DIVISION, EXPONENTIATION — ❌❌

| Op | Assoc | Commut | Counterexample |
|---|:---:|:---:|---|
| `−` | ❌ | ❌ | $(10-3)-2 = 5 \ne 9 = 10-(3-2)$; $0-1 \ne 1-0$ |
| `÷` | ❌ | ❌ | $(8/4)/2 = 1 \ne 4 = 8/(4/2)$; $1/2 \ne 2/1$ |
| `**` | ❌ | ❌ | $(2^3)^2 = 64 \ne 512 = 2^{(3^2)}$; $2^3 \ne 3^2$ |

Subtraction is more precisely **anti-commutative**: $a - b = -(b - a)$. That is
not good enough — anti-commutativity does not give order independence.

**Never** use these directly as reducers. Express the intent through a monoid
instead:

```python
# ❌ WRONG: net = credits - debits via a subtracting reducer
rdd.reduceByKey(lambda a, b: a - b)

# ✅ RIGHT: sign the values at map time, then SUM (a commutative monoid)
(rdd.mapValues(lambda t: t.amount if t.kind == 'credit' else -t.amount)
    .reduceByKey(lambda a, b: a + b))
```

This is the general repair for subtraction: **push the sign into the lift, keep
the merge additive.**

---

## 9. The Lifting Recipe: Repairing a Broken Reducer

Every fix in §8 followed the same four steps. Here is the procedure.

```
┌─ 1. IDENTIFY the accumulator M ────────────────────────────────────┐
│    Ask: "What is the SUFFICIENT STATISTIC?"                        │
│    What is the minimum information about a subset that lets me     │
│    merge it with any other subset without seeing the raw data?     │
└────────────────────────────────────────────────────────────────────┘
                              ▼
┌─ 2. DEFINE lift : A → M ───────────────────────────────────────────┐
│    How does ONE raw value become an accumulator?                   │
└────────────────────────────────────────────────────────────────────┘
                              ▼
┌─ 3. DEFINE merge : M × M → M ──────────────────────────────────────┐
│    PROVE associativity, commutativity, and find the identity e.    │
│    Usually free: build M as a PRODUCT of known monoids.            │
└────────────────────────────────────────────────────────────────────┘
                              ▼
┌─ 4. DEFINE finalize : M → B ───────────────────────────────────────┐
│    All the non-associative arithmetic (division, sqrt, ratios)     │
│    goes HERE — outside the merge, applied exactly once.            │
└────────────────────────────────────────────────────────────────────┘
```

Applying it:

| Goal | Accumulator $M$ | `lift` | `merge` | `finalize` |
|---|---|---|---|---|
| average | $(\text{sum}, \text{count})$ | `(v, 1)` | componentwise `+` | `s/c` |
| variance | $(n, \sum v, \sum v^2)$ | `(1, v, v²)` | componentwise `+` | `q/n − (s/n)²` |
| stddev | same | same | same | `sqrt(...)` |
| min & max & avg | $(\min,\max,\text{sum},\text{cnt})$ | `(v,v,v,1)` | componentwise | `(m, M, s/c)` |
| top-N | sorted list, len ≤ N | `[v]` | merge + truncate | identity |
| argmax | $(\text{score}, \text{payload})$ | `(f(v), v)` | keep larger score | `.payload` |
| distinct count | set (or HLL registers) | `{v}` | `∪` (or `max`) | `len` (or estimate) |
| "first by time" | $(\text{ts}, \text{value})$ | `(v.ts, v)` | keep smaller ts | `.value` |
| ratio A/B | $(\text{numer}, \text{denom})$ | `(a, b)` | componentwise `+` | `n/d` |

**The universal move:** *never divide, never take a square root, never compute
a ratio inside the merge.* Carry the numerator and denominator separately and
divide exactly once, at the end.

### Worked example: combining five aggregates in one pass

```python
# M = (count, sum, sumsq, min, max) — a product of five commutative monoids
IDENTITY = (0, 0.0, 0.0, float('inf'), float('-inf'))

def lift(v):
    return (1, v, v*v, v, v)

def merge(a, b):
    return (a[0]+b[0], a[1]+b[1], a[2]+b[2],
            min(a[3], b[3]), max(a[4], b[4]))

def finalize(m):
    n, s, q, lo, hi = m
    mean = s / n
    return {"count": n, "mean": mean, "var": q/n - mean**2,
            "std": (q/n - mean**2) ** 0.5, "min": lo, "max": hi}

# PySpark
stats = (rdd.mapValues(lift)
            .reduceByKey(merge)          # ✅ commutative monoid: safe anywhere
            .mapValues(finalize))

# or, with the explicit identity:
stats = (rdd.aggregateByKey(IDENTITY,
                            lambda acc, v: merge(acc, lift(v)),
                            merge)
            .mapValues(finalize))
```

One shuffle, bounded accumulator, provably correct under any partitioning and
any arrival order.

---

## 10. Master Summary Table

| # | Function | Assoc | Commut | Identity | Idem | Inverse | MapReduce-safe? | Fix if not |
|---|---|:---:|:---:|:---:|:---:|:---:|:---:|---|
| 1 | `sum` | ✅ | ✅ | `0` | ❌ | ✅ | ✅ | — |
| 2 | `product` | ✅ | ✅ | `1` | ❌ | ✅* | ✅ | watch overflow / use logs |
| 3 | `max` | ✅ | ✅ | `-∞` | ✅ | ❌ | ✅ | — |
| 4 | `min` | ✅ | ✅ | `+∞` | ✅ | ❌ | ✅ | — |
| 5 | `count` | ✅ | ✅ | `0` | ❌ | ✅ | ✅ | lift `v ↦ 1` |
| 6 | `AND` / `OR` | ✅ | ✅ | `True`/`False` | ✅ | ❌ | ✅ | — |
| 7 | `XOR` | ✅ | ✅ | `False` | ❌ | ✅ | ✅ | — |
| 8 | set `∪` / `∩` | ✅ | ✅ | `∅` / `U` | ✅ | ❌ | ✅ | unbounded memory ⚠️ |
| 9 | `gcd` / `lcm` | ✅ | ✅ | `0` / `1` | ✅ | ❌ | ✅ | — |
| 10 | **`avg` (naive)** | ❌ | ✅ | ❌ | ✅ | ❌ | ❌ | lift to `(sum, count)` |
| 11 | `avg` (monoid) | ✅ | ✅ | `(0,0)` | ❌ | ✅ | ✅ | — |
| 12 | `variance`/`stddev` | ✅ | ✅ | `(0,0,0)` | ❌ | ✅ | ✅ | lift to `(n, Σv, Σv²)` or Chan |
| 13 | `top-N` | ✅ | ✅ | `[]` | ✅ | ❌ | ✅ | lift to bounded sorted list |
| 14 | `argmax` / `argmin` | ✅ | ✅ | sentinel | ✅ | ❌ | ✅ | lift to `(key, payload)`, total tie-break |
| 15 | `distinct count` (exact) | ✅ | ✅ | `∅` | ✅ | ❌ | ✅⚠️ | set union; memory grows |
| 16 | `distinct count` (HLL) | ✅ | ✅ | zeros | ✅ | ❌ | ✅ | componentwise `max` |
| 17 | **`median` / percentile** | ❌ | — | ❌ | — | ❌ | ❌ | t-digest / KLL / histogram |
| 18 | **`first` / `last`** | ✅ | ❌ | — | ✅ | ❌ | ❌ | carry timestamp → `argmin` |
| 19 | **string / list `++`** | ✅ | ❌ | `""` / `[]` | ❌ | ❌ | ❌ | secondary sort, or use a bag |
| 20 | **`−` (subtract)** | ❌ | ❌ | `0`(right) | ❌ | — | ❌ | sign at map time, then `sum` |
| 21 | **`÷` (divide)** | ❌ | ❌ | `1`(right) | ❌ | — | ❌ | carry `(numer, denom)`, divide last |
| 22 | **exponentiation** (`a ** b`) | ❌ | ❌ | ❌ | ❌ | — | ❌ | use `sum` of logs |
| 23 | matrix `×` | ✅ | ❌ | `I` | ❌ | ✅* | ❌ | preserve index order explicitly |
| 24 | float `+` (IEEE 754) | ⚠️ | ✅ | `0.0` | ❌ | ⚠️ | ⚠️ | Kahan / pairwise sum — see §12 |

\* inverses exist only for non-zero / non-singular elements.

**Bold rows are the ones that bite people in production.**

---

## 11. Algebraic, Distributive, and Holistic Aggregates

Gray et al.'s **data-cube taxonomy** (1997) is the same idea from the database
side, and it predicts exactly which aggregates parallelize.

| Class | Definition | Accumulator size | Examples | MapReduce |
|---|---|---|---|---|
| **Distributive** | $F(A \uplus B) = G(F(A), F(B))$ for some $G$; the sub-aggregate has the **same type** as the answer | $O(1)$ | `count`, `sum`, `min`, `max` | ✅ trivial |
| **Algebraic** | expressible as a **bounded** $M$-tuple of distributive aggregates plus a finalize step | $O(1)$, $M$ fixed | `avg` (2), `variance` (3), `stddev` (3), `top-N` ($N$), `MaxN` | ✅ after lifting |
| **Holistic** | **no** bounded sufficient statistic exists | $O(n)$ | `median`, `percentile`, `mode`, `rank`, exact `count distinct` | ❌ needs sort or sketch |

**The correspondence to this document:**

```
Distributive  ⟺  the values themselves form a commutative monoid
Algebraic     ⟺  a BOUNDED commutative monoid exists after lifting  (§9)
Holistic      ⟺  no bounded commutative monoid exists — sketch or sort
```

So the practical question *"can I use a combiner?"* is really the theoretical
question *"is this aggregate algebraic?"*, which is really *"does a bounded
commutative monoid exist for it?"* — three phrasings of one thing.

### A note on in-mapper combining

Because the target is a commutative monoid, you can also do the combining
**inside the mapper** with a local hash map, which avoids the serialization
round-trip a combiner pays:

```python
# in-mapper combining (mapPartitions) — same algebra, less I/O
def combine_partition(rows):
    acc = {}
    for k, v in rows:
        acc[k] = merge(acc[k], lift(v)) if k in acc else lift(v)
    return acc.items()

rdd.mapPartitions(combine_partition).reduceByKey(merge)
```

This is valid for exactly the same reason a combiner is: the Corollary to
Theorem 2. Trade-off: memory proportional to distinct keys per partition.

---

## 12. The Floating-Point Caveat

> **IEEE 754 floating-point addition is commutative but *not* associative.**

```python
a, b, c = 1e16, -1e16, 1.0
(a + b) + c      # 1.0     ✅ what you expect
a + (b + c)      # 0.0     ❌ the 1.0 is annihilated by rounding
```

So `sum` over floats is a commutative **magma** at machine precision, not a
monoid. Consequences you must accept or mitigate:

* A distributed float sum is **reproducible only if the partitioning and merge
  order are fixed.** Change the cluster size and the last few digits change.
* Do **not** write exact-equality assertions against distributed float
  aggregates. Compare within a tolerance.
* Integer and `Decimal` arithmetic **are** exactly associative — for money,
  always use fixed-point (`decimal.Decimal`, or integer cents). This is not
  pedantry; it is the difference between a ledger that balances and one that
  does not.

**Mitigations, in increasing order of cost:**

| Technique | Error bound | Notes |
|---|---|---|
| Naive left fold | $O(n \varepsilon)$ | worst |
| Pairwise / tree summation | $O(\varepsilon \log n)$ | free with tree reduction — you get it by default in Spark |
| Kahan / Neumaier compensated sum | $O(\varepsilon)$ | carry a compensation term in the accumulator |
| Sort by magnitude, then sum | good | requires a full sort |
| `Decimal` / integer | exact | slower, but exact |

```python
# Kahan summation as a commutative-ish monoid: accumulator (sum, compensation)
def kahan_merge(a, b):
    s = a[0] + b[0]
    c = a[1] + b[1] + ((a[0] - (s - b[0])) + (b[0] - (s - a[0])))
    return (s, c)

def kahan_finalize(acc):
    return acc[0] + acc[1]
```

The honest framing: floating point makes associativity **approximate**. The
algebra tells you the answer is order-independent *in $\mathbb{R}$*; the
hardware tells you it is order-independent *up to rounding*. Design your tests
accordingly.

---

## 13. Testing Your Reducer

Since the laws cannot be decided statically (§6), test them empirically.
Property-based testing catches the overwhelming majority of real violations.

```python
import itertools, random
from functools import reduce

def check_associative(f, domain, trials=1000):
    for _ in range(trials):
        a, b, c = (random.choice(domain) for _ in range(3))
        if f(f(a, b), c) != f(a, f(b, c)):
            return False, (a, b, c)
    return True, None

def check_commutative(f, domain, trials=1000):
    for _ in range(trials):
        a, b = random.choice(domain), random.choice(domain)
        if f(a, b) != f(b, a):
            return False, (a, b)
    return True, None

def check_identity(f, e, domain, trials=1000):
    return all(f(e, a) == a and f(a, e) == a
               for a in random.sample(domain, min(trials, len(domain))))

def check_order_independence(f, xs):
    """Exhaustive over permutations — use for small xs only."""
    base = reduce(f, xs)
    return all(reduce(f, p) == base for p in itertools.permutations(xs))
```

### The partition-invariance test — the one that matters most

```python
def check_partition_invariance(lift, merge, finalize, values, trials=200):
    """The result must not depend on HOW the data is split."""
    truth = finalize(reduce(merge, map(lift, values)))
    for _ in range(trials):
        vs = values[:]
        random.shuffle(vs)                                  # random ORDER
        k = random.randint(1, len(vs))
        cuts = sorted(random.sample(range(1, len(vs)), min(k, len(vs)-1)))
        blocks, prev = [], 0
        for cut in cuts + [len(vs)]:
            blocks.append(vs[prev:cut]); prev = cut
        partials = [reduce(merge, map(lift, b)) for b in blocks if b]
        if finalize(reduce(merge, partials)) != truth:       # random GROUPING
            return False, blocks
    return True, None
```

This single test simulates every source of nondeterminism in §6 at once:
random order, random partition count, random block sizes. If a reducer passes
it over a few hundred trials, it is almost certainly a commutative semigroup.

**Also test explicitly:**

* the **empty** partition (does `finalize(e)` make sense?)
* a **singleton** partition (`merge(e, lift(v)) == lift(v)`)
* **duplicate** values (exposes accidental idempotence assumptions)
* running the **combiner twice** on its own output
* extreme magnitudes (exposes §12 numerical issues)

---

## 14. Pitfalls and a Design Checklist

### Top pitfalls

1. **Averaging averages.** Correct only for equal-size partitions, which never
   happens. → §8.5
2. **`reduceByKey(lambda a, b: a - b)`.** Neither law holds. → §8.13
3. **Dividing inside the merge.** Any division, `sqrt`, or ratio inside the
   merge breaks associativity. Move it to `finalize`. → §9
4. **Unbounded accumulators.** `set` union and `list` concat are algebraically
   fine but destroy the combiner's purpose — the shuffle no longer shrinks. → §8.9
5. **Assuming the combiner runs.** It may run zero times. Your reducer must be
   correct on raw values *and* on combined values. → §7
6. **Assuming the combiner runs once.** It may run repeatedly on its own
   output. Test double application. → §7
7. **`first`/`last` without an ordering key.** Nondeterministic. → §8.11
8. **Integer overflow in `sum`/`product`** at scale. Use 64-bit, `Decimal`, or
   log-space.
9. **Float equality assertions** on distributed aggregates. → §12
10. **Non-total tie-breaking in `argmax`.** Ties resolved by arrival order
    reintroduce nondeterminism. → §8.11

### Design checklist

Before shipping any reducer, answer all seven:

- [ ] **Closure** — does `merge` return the same type it consumes?
- [ ] **Associativity** — proved, or property-tested over ≥1000 random triples?
- [ ] **Commutativity** — proved, or property-tested?
- [ ] **Identity** — what does an empty partition produce? Is `e` two-sided?
- [ ] **Boundedness** — does the accumulator stay $O(1)$ as input grows?
- [ ] **Finalize isolation** — is every division / sqrt / ratio outside `merge`?
- [ ] **Numerics** — overflow, underflow, catastrophic cancellation considered?

If all seven are ✅, your reducer is a bounded commutative monoid and is correct
under any partitioning, any ordering, any combiner schedule, and any number of
speculative re-executions. That is the whole goal.

---

## 15. Exercises

1. **Prove or disprove** that $a \ast b = |a - b|$ is associative. Commutative?
   Give a counterexample for whichever fails.

2. **Design the monoid** for computing, in one pass per key: the count, the
   mean, and the **harmonic mean**. What must the accumulator carry?
   *(Hint: what is the sufficient statistic for $n / \sum (1/v_i)$?)*

3. **Weighted average.** Values arrive as $(v_i, w_i)$. Give `lift`, `merge`,
   `finalize`, and the identity. Prove `merge` is a commutative monoid.

4. **Mode (most frequent value).** Is it distributive, algebraic, or holistic?
   Justify using §11. What is the exact accumulator, and why is it not bounded?

5. **Second-largest distinct value.** Design a bounded commutative monoid. Be
   careful about duplicates — what is the identity?

6. **Skewness** requires $\sum v^3$. Extend §8.6(a) and give `finalize`. Then
   argue why the Chan-style stable merge is preferable at scale.

7. **Longest string per key.** Is `lambda a,b: a if len(a) >= len(b) else b`
   commutative? What breaks, and how do you make the tie-break total?

8. **Sliding-window sums.** Explain, using §3.7, why `sum` supports removing an
   expiring value from a running window but `max` does not. What structure does
   `max` need to be given to support it?

9. **Run the checker.** Implement `check_partition_invariance` from §13 against
   your own production reducer. Report the first violation, if any.

10. **Float experiment.** Sum $10^7$ values of `0.1` using (a) a naive left
    fold, (b) tree reduction, (c) `math.fsum`. Explain the differences using
    §12.

---

## References and Further Reading

* Dean, J. and Ghemawat, S. — *MapReduce: Simplified Data Processing on Large
  Clusters*, OSDI 2004. (See `google_mapreduce_paper/` in this repository.)
* Gray, J. et al. — *Data Cube: A Relational Aggregation Operator Generalizing
  Group-By, Cross-Tab, and Sub-Totals*, 1997. (Distributive / algebraic /
  holistic taxonomy.)
* Lin, J. and Dyer, C. — *Data-Intensive Text Processing with MapReduce*.
  (In-mapper combining, the local aggregation patterns.)
* Parsian, M. — *Data Algorithms* and *Data Algorithms with Spark*, O'Reilly.
* Chan, T. F., Golub, G. H., LeVeque, R. J. — *Algorithms for Computing the
  Sample Variance: Analysis and Recommendations*, 1983.
* Flajolet, P. et al. — *HyperLogLog: the analysis of a near-optimal
  cardinality estimation algorithm*, 2007.
* `monoids/Monoids_as_a_Design_Principle_for_Efficient_MapReduce_Algorithms.pdf`
  (this repository)
* `combiners/combiners_in_mapreduce.md` (this repository)
* `reducebykey_max_avg_filter.md` (this repository)

---

## One-Paragraph Summary

A MapReduce reducer is handed a **multiset**, not a list: the framework may
group the values however it likes (combiners, variable partition counts) and
deliver them in whatever order the network produces (concurrent shuffle,
speculative execution). A reduction is therefore well-defined **iff** its
binary operation is **associative** — grouping-independent — and
**commutative** — order-independent; adding an **identity** makes it a
**commutative monoid**, which additionally makes empty partitions and optional
combiners safe. Functions like `sum`, `max`, `min`, `count`, `∪`, `AND`, and
`OR` are commutative monoids out of the box. Functions like `avg`, `variance`,
and `top-N` are *not*, but become so once you **lift** the value type to a
bounded sufficient statistic — `(sum, count)`, `(n, Σv, Σv²)`, a length-$N$
sorted list — and push all non-associative arithmetic into a `finalize` step
applied exactly once. Functions like `median` are **holistic**: no bounded
sufficient statistic exists, so you must either sort everything or accept a
mergeable sketch. Design your reducers toward a bounded commutative monoid, and
correctness under any partitioning, any ordering, and any number of retries
follows as a theorem rather than a hope.
