# Join Operation in Action <br> using MapReduce

	Author: Mahmoud Parsian
	Last updated: 9/3/2026

## Table of Contents

1. Introduction
2. Problem
3. Input Data Format
4. Why Is This a Join Problem?
5. The Reduce-Side Join: Tag, Group, Match
6. Worked Example, Goal 1 — Transaction Count and Total per Customer
7. Worked Example, Goal 2 — Customers Who Have Not Purchased Anything
8. Worked Example, Goal 3 — Top-N Customers by Spending
9. Generalized Join Algorithms: INNER, LEFT OUTER, RIGHT OUTER
10. Worked Join Example — Set A and Set B
11. Why a Combiner Can't Do the Join Itself
12. From MapReduce to PySpark: join() on RDDs and DataFrames
13. Food for Thought
14. Comments
15. References

## 1. Introduction

A **join** combines records from two (or more) datasets
that share a common key. In a single-machine world this
is a database `JOIN`; in MapReduce there is no shared
memory or shared index across mappers, so a join has to
be expressed as a `map()`/`reduce()` pair like any other
computation. This article works through a concrete join
problem end to end — real sample data, every mapper call,
the shuffle output, every reducer call — and then lifts
the same technique into three general-purpose join
algorithms: **INNER**, **LEFT OUTER**, and **RIGHT OUTER**.

## 2. Problem

Let's make two datasets: one with bank customer data,
and the other with their credit transaction data — both
having `customer_id` as a common key.

The attributes in the datasets are:

**Customer Details:**

```text
customer_id,customer_name,age
```

**Credit Transaction Details:**

```text
transaction_id,date,customer_id,transaction_amount
```

Three goals for this article:

1. Calculate the number of credit transactions made by
   each customer, along with the total transaction amount.
   The output after joining is:

   ```text
   customer_name,transactions_count,total_transaction_amount
   ```

2. Find customers who have **not** made any purchases —
   i.e., customers with zero matching transaction records.

3. Find the top-N (N = 2, 3, 5, 10) customers by total
   spending amount — built directly on top of Goal 1's
   per-customer totals rather than a fresh join. The output
   after ranking is:

   ```text
   rank,customer_name,total_transaction_amount
   ```

## 3. Input Data Format

Two small sample datasets, used throughout this article
so every mapper and reducer call can be traced by hand.

**Customers** (4 records):

```text
customer_id,customer_name,age
C1,Alice,34
C2,Bob,29
C3,Carol,41
C4,Dave,25
```

**Transactions** (6 records):

```text
transaction_id,date,customer_id,transaction_amount
T1,1/2/2024,C1,120.50
T2,1/5/2024,C2,45.00
T3,1/9/2024,C1,75.25
T4,2/1/2024,C3,200.00
T5,2/3/2024,C1,10.00
T6,2/10/2024,C2,60.00
```

Notice `C4` (Dave) never appears in the transactions
file — that's on purpose, so Goal 2 has something to find.
Also notice `C1` (Alice) appears **three** times, `C2`
(Bob) **twice**, and `C3` (Carol) **once** — so Goal 1's
counts and totals won't all be trivial.

## 4. Why Is This a Join Problem?

`customer_name` lives only in the customer file;
`transaction_amount` lives only in the transaction file.
Neither file, read on its own, can produce
`customer_name,transactions_count,total_transaction_amount`
— we need one record from customers **joined** with zero
or more records from transactions, matched on
`customer_id`. And at real bank scale — millions of
customers, billions of transactions per year — neither
file fits in one machine's memory, so we can't just load
the small side into a hash map and stream the big side
past it (a "map-side join"). That leaves a **reduce-side
join**: ship both datasets through the shuffle, grouped by
`customer_id`, and let the reducer do the matching.

## 5. The Reduce-Side Join: Tag, Group, Match

The trick is that MapReduce's shuffle already does the
"group by key" work for free — we just need both datasets
to emit the *same* key (`customer_id`) so the shuffle puts
matching records from **both** files into one reducer
call. Since a reducer receives a flat
`Iterable<value>` for a key, with no indication of which
file each value came from, every value must be **tagged**
with its source before it's emitted:

```text
# customer record -> tag "CUST"
map_customer(key, value) {
   # value = "customer_id,customer_name,age"
   customer_id, customer_name, age = value.split(",")
   emit(customer_id, ("CUST", customer_name))
}

# transaction record -> tag "TXN"
map_transaction(key, value) {
   # value = "transaction_id,date,customer_id,transaction_amount"
   transaction_id, date, customer_id, transaction_amount = value.split(",")
   emit(customer_id, ("TXN", float(transaction_amount)))
}
```

**Aside — could `map()` tell the two files apart without
a separate function per file?** In this particular
dataset, yes: customer lines split into 3 tokens and
transaction lines split into 4, so one shared `map()`
could dispatch on `len(value.split(","))` instead of
routing each file to its own mapper function. That
simplifies *reading* the raw line — but it doesn't remove
the need to tag the *emitted* value. By the time `map()`
calls `emit(customer_id, customer_name)` or
`emit(customer_id, transaction_amount)`, the field count
that told the two records apart is gone; the reducer sees
only a flat list of bare values and still needs some way
to know which came from which file. Here the two values
even happen to differ by runtime type (`customer_name` is
a string, `transaction_amount` is a number), so a reducer
*could* dispatch on type instead of an explicit tag — but
that's a coincidence of this toy example, not a general
technique: it breaks the moment two datasets share a value
type (as most do), which is exactly why the generalized
join in Section 9 uses an explicit `"A"`/`"B"` tag instead
of inferring the source from shape.

In real Hadoop this is wired up with
`MultipleInputs.addInputPath(job, customersPath,
TextInputFormat.class, CustomerMapper.class)` and a
second call for the transactions path with
`TransactionMapper.class` — one job, two mapper classes,
one shared reducer, and the tag comes for free from which
mapper class ran, no `len()` check needed. Conceptually
it's still a single `map()` that looks at which file a
record came from and tags accordingly; that's exactly the
pattern the generic `set A` / `set B` version in Section 9
makes explicit with a literal `"A"` / `"B"` tag.

After shuffle & sort, every reducer call receives one
`customer_id` and the full list of tagged values from
**both** files:

```text
(C1, [("CUST","Alice"), ("TXN",120.50), ("TXN",75.25), ("TXN",10.00)])
(C2, [("CUST","Bob"),   ("TXN",45.00),  ("TXN",60.00)])
(C3, [("CUST","Carol"), ("TXN",200.00)])
(C4, [("CUST","Dave")])
```

This shuffled shape is the input to *both* worked
examples below — Goal 1 and Goal 2 differ only in what
their `reduce()` functions do with it.

## 6. Worked Example, Goal 1 — Transaction Count and Total per Customer

This reducer is an **INNER JOIN + aggregate in one pass**:
for each `customer_id`, split its values back into the
`CUST` tag (at most one — a customer file has one row per
customer) and the `TXN` tags (zero or more), then emit
only when there is at least one transaction.

```text
# key = customer_id
# values = Iterable<(tag, value)>
reduce_goal1(key, values) {
   customer_name = NULL
   count = 0
   total = 0.0
   for (tag, v) in values {
      if tag == "CUST":
         customer_name = v
      else:  # tag == "TXN"
         count += 1
         total += v
   }

   if count == 0:
      # inner join: no transactions -> nothing to emit
      return

   emit(customer_name, (count, total))
}
```

Applied to the four shuffled groups from Section 5:

```text
reduce_goal1(C1, [("CUST","Alice"), ("TXN",120.50), ("TXN",75.25), ("TXN",10.00)])
   -> (Alice, (3, 205.75))

reduce_goal1(C2, [("CUST","Bob"), ("TXN",45.00), ("TXN",60.00)])
   -> (Bob, (2, 105.00))

reduce_goal1(C3, [("CUST","Carol"), ("TXN",200.00)])
   -> (Carol, (1, 200.00))

reduce_goal1(C4, [("CUST","Dave")])
   -> nothing emitted (count == 0)
```

Final output:

```text
customer_name,transactions_count,total_transaction_amount
Alice,3,205.75
Bob,2,105.00
Carol,1,200.00
```

`Dave` is correctly absent — an inner join only keeps keys
present on **both** sides, and `C4` never appears in the
transactions file.

## 7. Worked Example, Goal 2 — Customers Who Have Not Purchased Anything

This is the mirror image: keep exactly the `customer_id`s
that have a `CUST` tag but **no** `TXN` tag at all — an
**anti-join** (a LEFT OUTER JOIN filtered down to the
unmatched rows only). It reuses the identical shuffled
input from Section 5 — only the `reduce()` logic changes:

```text
# key = customer_id
# values = Iterable<(tag, value)>
reduce_goal2(key, values) {
   customer_name = NULL
   txn_count = 0
   for (tag, v) in values {
      if tag == "CUST":
         customer_name = v
      else:  # tag == "TXN"
         txn_count += 1
   }

   if txn_count > 0:
      # this customer has purchased -- not what we're looking for
      return

   if customer_name == NULL:
      # defensive: a transaction referencing an unknown customer_id
      # (an orphan foreign key) -- nothing to report here either
      return

   emit(customer_name, "NO PURCHASES")
}
```

Applied to the same four shuffled groups:

```text
reduce_goal2(C1, [("CUST","Alice"), ("TXN",120.50), ("TXN",75.25), ("TXN",10.00)])
   -> nothing emitted (txn_count == 3)

reduce_goal2(C2, [("CUST","Bob"), ("TXN",45.00), ("TXN",60.00)])
   -> nothing emitted (txn_count == 2)

reduce_goal2(C3, [("CUST","Carol"), ("TXN",200.00)])
   -> nothing emitted (txn_count == 1)

reduce_goal2(C4, [("CUST","Dave")])
   -> (Dave, "NO PURCHASES")
```

Final output:

```text
Dave,NO PURCHASES
```

Sections 6 and 7 are the same shuffle, read by two
different reducers — proof that the tag-and-group step in
Section 5 is doing all of the actual "join" work; everything
after that is ordinary per-key filtering and aggregation.

## 8. Worked Example, Goal 3 — Top-N Customers by Spending

Goal 3 doesn't need a fresh join — it needs Goal 1's
*output*. Section 6 already joined customers with
transactions and produced exactly the ranking key we need:

```text
customer_name,transactions_count,total_transaction_amount
Alice,3,205.75
Bob,2,105.00
Carol,1,200.00
```

So Goal 3 is a **second MapReduce job**, chained after
Goal 1's, whose input is Goal 1's output and whose task is:
given `(customer_name, total)` pairs, find the global
top-N by `total`. Customers with zero transactions, like
Dave, were already dropped by Goal 1's inner join and
correctly never enter the ranking — a customer who spent
nothing can't be a top spender.

### Why not just sort everything at one reducer?

The simplest possible reducer — route every
`(customer_name, total)` pair to one reducer under a
constant key, sort the whole list there, keep the first
N — is correct, but it defeats the purpose of MapReduce at
scale: every record in the dataset is forced through one
reducer's memory, no matter how small `N` is. The standard
fix is **local top-N, then merge**:

1. **Map + in-mapper combine.** Each mapper keeps a bounded
   min-heap of capacity `N` as it reads its split. For
   every record, push `(total, customer_name)` onto the
   heap; if the heap now holds more than `N` entries, evict
   the smallest. By the time the mapper's split is
   exhausted, the heap holds only that mapper's *local*
   top-N — everything else was discarded early, in the
   mapper, before a single byte crosses the shuffle.
2. **Shuffle.** Every mapper emits its (at most `N`)
   survivors under one shared key (e.g. `NULL`), so they
   all land at the same single reducer.
3. **Reduce.** That one reducer now sees at most
   `(number of mappers) x N` candidates — not the whole
   dataset — sorts just that small set, and keeps the top
   `N`.

This is correct because the global top-N can never contain
a record that lost its own mapper's local top-N cut: if a
record wasn't among the `N` largest within its own split,
at least `N` larger records already exist elsewhere in the
dataset, so it cannot be among the `N` largest overall
either.

```text
# in-mapper combining: each mapper keeps a size-N min-heap,
# keyed by total_transaction_amount
map_topN(key, value) {
   # value = "customer_name,transactions_count,total_transaction_amount"
   customer_name, count, total = value.split(",")
   heap.push((total, customer_name))
   if heap.size() > N:
      heap.pop_min()   # local top-N cut -- discard early, before shuffle
}

close_mapper() {
   # end of this mapper's split: emit its surviving local
   # top-N to the ONE reducer that merges every mapper's list
   for (total, customer_name) in heap:
      emit(NULL, (customer_name, total))
}

# single reducer: merges every mapper's local top-N
reduce_topN(key, values) {
   candidates = list(values)               # (customer_name, total) pairs
   candidates.sort(by total, descending)
   rank = 1
   for (customer_name, total) in candidates[0 : N]:
      emit(rank, (customer_name, total))
      rank += 1
}
```

### Trace it on the sample data, N = 2

Split Goal 1's 3-row output across two mapper tasks, to
show the local-cut-then-merge behavior instead of a
trivial one-mapper case:

**Mapper M1** (split = [Alice, Bob]), heap capacity N = 2:

```text
map_topN(_, "Alice,3,205.75")  -> heap.push((205.75,Alice))  heap=[(205.75,Alice)]
map_topN(_, "Bob,2,105.00")    -> heap.push((105.00,Bob))    heap=[(205.75,Alice),(105.00,Bob)]
                                   size == 2, no evict
close_mapper() -> emit(NULL,(Alice,205.75)), emit(NULL,(Bob,105.00))
```

**Mapper M2** (split = [Carol]), heap capacity N = 2:

```text
map_topN(_, "Carol,1,200.00")  -> heap.push((200.00,Carol))  heap=[(200.00,Carol)]
close_mapper() -> emit(NULL,(Carol,200.00))    # heap never filled past 1 -- nothing evicted
```

**Reducer** receives all three survivors under key `NULL`
(no candidate was large enough, on either mapper, to force
an eviction, so all 3 of the original 3 records reach the
reducer here — the local cut only *starts* discarding once
a split holds more than `N` candidates):

```text
reduce_topN(NULL, [(Alice,205.75), (Bob,105.00), (Carol,200.00)])
   sorted descending by total: Alice(205.75), Carol(200.00), Bob(105.00)
   -> (1, (Alice, 205.75))
   -> (2, (Carol, 200.00))
```

**Top-2 output:**

```text
rank,customer_name,total_transaction_amount
1,Alice,205.75
2,Carol,200.00
```

### N = 2, 3, 5, 10 side by side

There are only 3 ranked customers in this sample (Dave was
dropped by Goal 1's inner join), so N = 5 and N = 10 can't
produce 5 or 10 rows — the output simply stops at 3:

| N | Output |
|---|---|
| 2 | Alice (205.75), Carol (200.00) |
| 3 | Alice (205.75), Carol (200.00), Bob (105.00) |
| 5 | Alice (205.75), Carol (200.00), Bob (105.00) — only 3 candidates exist |
| 10 | Alice (205.75), Carol (200.00), Bob (105.00) — only 3 candidates exist |

At real bank scale — millions of customers — the same
algorithm holds without modification: only the
`close_mapper()` heap size (`N`, a small constant like 10)
and the final reducer's candidate count
(`(number of mappers) x N`, still small) determine memory
use, never the size of the full dataset.

## 9. Generalized Join Algorithms: INNER, LEFT OUTER, RIGHT OUTER

Now generalize away from customers/transactions. Given
two arbitrary datasets:

```text
Set A:
<key>,<value>

Set B:
<key>,<value>
```

tag each one on its way out of the mapper:

```text
Set A (tagged):
<key>,<value>,"A"

Set B (tagged):
<key>,<value>,"B"
```

A single shared `map()` handles both (in practice, one
mapper class per input path, exactly as in Section 5):

```text
# k is ignored
# v is <key>,<value>,"A" OR <key>,<value>,"B"
map(k, v) {
   key, value, tag = v.split(",")
   emit(key, (value, tag))
}
```

All three joins below start from the *same* grouping
step — split the shuffled values back into an `A_list` and
a `B_list` — and differ only in what they do when one list
is empty:

```text
# shared helper, inlined into every reduce() below
# values = Iterable<(value, tag)>
split(values) {
   A_list = []
   B_list = []
   for (v, t) in values {
      if t == "A":
         A_list.append(v)
      else:
         B_list.append(v)
   }
   return (A_list, B_list)
}
```

### The General Reduce-Side Join Algorithm

Stepping back from the three specific reducers below:
every reduce-side join in MapReduce — whatever its
semantics — follows the same four-step recipe. Sections
5-7 already showed one instance of it (with "CUST"/"TXN"
tags instead of "A"/"B"); what follows names the recipe
explicitly so it's clear it's an *algorithm*, not just
three snippets of code that happen to work:

1. **Tag.** Every mapper, whichever input file it's
   reading, emits `(key, (value, source_tag))` — the tag
   survives the shuffle even though the reducer eventually
   sees both datasets merged into one flat list.
2. **Group.** The MapReduce shuffle groups all tagged
   values for a given key at one reducer, by key, for
   free — this step is provided by the framework, not
   written by the join's author.
3. **Split.** The reducer partitions its
   `Iterable<value>` back into per-source lists (`A_list`,
   `B_list`, ...) using the tag — this is the `split()`
   helper above, and it's identical for every join type.
4. **Match.** A join-type-specific rule looks only at the
   *sizes* of `A_list` and `B_list` and decides whether to
   emit anything for this key, and what: the full cross
   product `A_list x B_list` when both are non-empty, plus
   a join-type-specific fallback (drop the key, or pad with
   `NULL`) for whichever side is allowed to survive
   unmatched.

Steps 1-3 never change — only step 4's rule changes across
INNER / LEFT OUTER / RIGHT OUTER (and FULL OUTER, in the
Section 13 exercise). That's the whole difference between
them, and it's also why Goal 2's anti-join (Section 7)
could reuse Section 5's tagging completely unmodified: same
recipe, a different step 4.

Two properties fall out of this recipe and are worth naming,
because they're what make it a real *algorithm* — with a
known cost model — and not just code that happens to
produce the right rows:

- **Cost per key is `O(|A_list| x |B_list|)`.** The cross
  product in step 4 means a key with 1,000 rows on each
  side emits 1,000,000 output rows. This is exactly what
  makes join **skew** dangerous: one oversized key (a "hot
  key" — e.g. one wildly popular product in an
  orders-joined-with-reviews job) can dominate a single
  reducer's running time and memory even though every other
  key is tiny, because that reducer alone pays the
  quadratic cost for its key. (Section 11 revisits why a
  combiner can't pre-filter this away — the decision needs
  *global* information a combiner doesn't have.)
- **Memory is bounded by both lists together, unless the
  algorithm is written more carefully.** The pseudocode
  below buffers all of `A_list` and `B_list` in memory
  before emitting anything — the simplest correct
  implementation, and the one used throughout this article,
  but it costs `O(|A_list| + |B_list|)` memory per key. A
  more careful **streaming** variant buffers only the
  smaller of the two lists and streams the larger side's
  values past it one at a time without ever holding all of
  it in memory — the classic trick (see Lin & Dyer,
  Section 15, reference 1) is to make the reducer's
  secondary sort put the smaller-cardinality tag first, so
  its list is already fully buffered by the time the larger
  side's values start arriving.

With that backbone in place, the three join types below
differ in exactly one `if` — what step 4 does when a list
comes up empty.

### INNER JOIN

Emit a pair for every `(a, b)` combination — nothing at
all if either side is missing:

```text
# key is a key
# values = Iterable<(value, tag)>
reduce_inner(key, values) {
   A_list, B_list = split(values)

   if size(A_list) < 1 OR size(B_list) < 1:
      # no match on one side -- inner join drops this key entirely
      return

   for a in A_list:
      for b in B_list:
         emit(key, (a, b))
      # for
   # for
}
```

This is exactly Section 6's `reduce_goal1()`, generalized:
`A_list` there was always `[customer_name]` (size 0 or 1)
and the double loop degenerated to counting/summing
`B_list` instead of emitting cross pairs — but the "drop
the key when one side is empty" rule is identical.

### LEFT OUTER JOIN

Keep every row of **A**, matched with **B** when a match
exists, or paired with `NULL` when it doesn't:

```text
reduce_left_outer(key, values) {
   A_list, B_list = split(values)

   if size(A_list) < 1:
      # nothing from A for this key -- LEFT OUTER JOIN has
      # nothing to anchor on, so there's nothing to emit
      return

   if size(B_list) < 1:
      # A has no match in B -- emit A padded with NULL
      for a in A_list:
         emit(key, (a, NULL))
      return

   for a in A_list:
      for b in B_list:
         emit(key, (a, b))
      # for
   # for
}
```

Section 7's `reduce_goal2()` is a LEFT OUTER JOIN with an
extra filter bolted on: instead of emitting `(a, NULL)`
for every unmatched `a`, it emits nothing when `B_list` is
non-empty, and emits just `a` (dropping the `NULL`
placeholder, since there's nothing useful to pair it with)
when `B_list` is empty. Swap that filter out and
`reduce_left_outer()` above is what's left.

### RIGHT OUTER JOIN

The mirror image of LEFT OUTER JOIN — swap which side is
allowed to survive unmatched:

```text
reduce_right_outer(key, values) {
   A_list, B_list = split(values)

   if size(B_list) < 1:
      # nothing from B for this key -- RIGHT OUTER JOIN has
      # nothing to anchor on, so there's nothing to emit
      return

   if size(A_list) < 1:
      # B has no match in A -- emit B padded with NULL
      for b in B_list:
         emit(key, (NULL, b))
      return

   for a in A_list:
      for b in B_list:
         emit(key, (a, b))
      # for
   # for
}
```

Note `reduce_right_outer(A, B)` is identical to
`reduce_left_outer(B, A)` with the tuple order flipped —
LEFT OUTER JOIN and RIGHT OUTER JOIN are the same
algorithm, called with the two datasets in opposite roles.
In our bank example, "customers LEFT OUTER JOIN
transactions" (every customer, matched transactions or
`NULL`) and "transactions RIGHT OUTER JOIN customers"
describe the exact same result set.

## 10. Worked Join Example — Set A and Set B

Section 9's `map()`, `split()`, `reduce_inner()`,
`reduce_left_outer()`, and `reduce_right_outer()` are
generic — they never mention customers or transactions.
This section runs all of them, by hand, over one small
concrete dataset so the difference between the three join
types is visible in the actual output, not just in the
`if` statements.

**Set A** — 2 keys, 6 records:

```text
K1,a1,"A"
K1,a2,"A"
K2,a3,"A"
K2,a4,"A"
K2,a5,"A"
K2,a6,"A"
```

**Set B** — 3 keys, 7 records:

```text
K1,b1,"B"
K1,b2,"B"
K3,b3,"B"
K3,b4,"B"
K3,b5,"B"
K4,b6,"B"
K4,b7,"B"
```

Note the shape on purpose: `K1` is in **both** sets (so it
can show a matched cross product), `K2` is **only in A**
(so it can show a LEFT OUTER `NULL`), and `K3`/`K4` are
**only in B** (so they can show RIGHT OUTER `NULL`s — two
B-only keys, to show it isn't a one-off).

### Step 1 — `map()` output (all 13 calls)

```text
map(_, "K1,a1,A") -> (K1, (a1, A))
map(_, "K1,a2,A") -> (K1, (a2, A))
map(_, "K2,a3,A") -> (K2, (a3, A))
map(_, "K2,a4,A") -> (K2, (a4, A))
map(_, "K2,a5,A") -> (K2, (a5, A))
map(_, "K2,a6,A") -> (K2, (a6, A))
map(_, "K1,b1,B") -> (K1, (b1, B))
map(_, "K1,b2,B") -> (K1, (b2, B))
map(_, "K3,b3,B") -> (K3, (b3, B))
map(_, "K3,b4,B") -> (K3, (b4, B))
map(_, "K3,b5,B") -> (K3, (b5, B))
map(_, "K4,b6,B") -> (K4, (b6, B))
map(_, "K4,b7,B") -> (K4, (b7, B))
```

6 calls come from Set A, 7 from Set B — 13 total, matching
the 6 + 7 = 13 input records.

### Step 2 — shuffle & sort (group by key)

```text
(K1, [(a1,A), (a2,A), (b1,B), (b2,B)])   # 4 values: 2 from A, 2 from B
(K2, [(a3,A), (a4,A), (a5,A), (a6,A)])   # 4 values: all from A
(K3, [(b3,B), (b4,B), (b5,B)])           # 3 values: all from B
(K4, [(b6,B), (b7,B)])                   # 2 values: all from B
```

Running `split()` from Section 9 on each group gives the
`A_list`/`B_list` every reducer below starts from:

```text
K1 -> A_list=[a1,a2],          B_list=[b1,b2]
K2 -> A_list=[a3,a4,a5,a6],    B_list=[]
K3 -> A_list=[],               B_list=[b3,b4,b5]
K4 -> A_list=[],               B_list=[b6,b7]
```

### Step 3a — `reduce_inner()` on all four keys

```text
reduce_inner(K1, ...)  A_list=[a1,a2], B_list=[b1,b2]
   -> (K1,(a1,b1)), (K1,(a1,b2)), (K1,(a2,b1)), (K1,(a2,b2))

reduce_inner(K2, ...)  B_list=[]  -> nothing emitted
reduce_inner(K3, ...)  A_list=[]  -> nothing emitted
reduce_inner(K4, ...)  A_list=[]  -> nothing emitted
```

**INNER JOIN final output (4 rows):**

```text
(K1,(a1,b1))
(K1,(a1,b2))
(K1,(a2,b1))
(K1,(a2,b2))
```

### Step 3b — `reduce_left_outer()` on all four keys

```text
reduce_left_outer(K1, ...)  A_list=[a1,a2], B_list=[b1,b2]
   -> (K1,(a1,b1)), (K1,(a1,b2)), (K1,(a2,b1)), (K1,(a2,b2))

reduce_left_outer(K2, ...)  A_list=[a3,a4,a5,a6], B_list=[]
   -> (K2,(a3,NULL)), (K2,(a4,NULL)), (K2,(a5,NULL)), (K2,(a6,NULL))

reduce_left_outer(K3, ...)  A_list=[]  -> nothing emitted
reduce_left_outer(K4, ...)  A_list=[]  -> nothing emitted
```

**LEFT OUTER JOIN final output (8 rows):**

```text
(K1,(a1,b1))
(K1,(a1,b2))
(K1,(a2,b1))
(K1,(a2,b2))
(K2,(a3,NULL))
(K2,(a4,NULL))
(K2,(a5,NULL))
(K2,(a6,NULL))
```

### Step 3c — `reduce_right_outer()` on all four keys

```text
reduce_right_outer(K1, ...)  A_list=[a1,a2], B_list=[b1,b2]
   -> (K1,(a1,b1)), (K1,(a1,b2)), (K1,(a2,b1)), (K1,(a2,b2))

reduce_right_outer(K2, ...)  B_list=[]  -> nothing emitted

reduce_right_outer(K3, ...)  A_list=[], B_list=[b3,b4,b5]
   -> (K3,(NULL,b3)), (K3,(NULL,b4)), (K3,(NULL,b5))

reduce_right_outer(K4, ...)  A_list=[], B_list=[b6,b7]
   -> (K4,(NULL,b6)), (K4,(NULL,b7))
```

**RIGHT OUTER JOIN final output (9 rows):**

```text
(K1,(a1,b1))
(K1,(a1,b2))
(K1,(a2,b1))
(K1,(a2,b2))
(K3,(NULL,b3))
(K3,(NULL,b4))
(K3,(NULL,b5))
(K4,(NULL,b6))
(K4,(NULL,b7))
```

### Side by side

| Join type | Output rows | Keys in output | Dropped keys |
|---|---|---|---|
| INNER | 4 | K1 | K2, K3, K4 |
| LEFT OUTER | 8 | K1, K2 | K3, K4 |
| RIGHT OUTER | 9 | K1, K3, K4 | K2 |

All three reducers see the *exact same* shuffled input from
Step 2 — every row of that table is produced by the `if`
branches in Section 9's `reduce_inner()` /
`reduce_left_outer()` / `reduce_right_outer()` alone,
nothing else changes. `K1` (matched on both sides) is the
only key that survives every join type, and its 4 rows
are identical in all three outputs — matching keys are
unaffected by which join you choose; only the unmatched
keys (`K2`, `K3`, `K4`) are treated differently. As a
sanity check for the `reduce_full_outer()` exercise in
Section 13: a FULL OUTER JOIN over this same data would be
the union of the LEFT OUTER and RIGHT OUTER outputs above
— 4 (K1) + 4 (K2, padded) + 3 (K3, padded) + 2 (K4,
padded) = **13 rows**, covering all four keys.

## 11. Why a Combiner Can't Do the Join Itself

It's tempting to add a `combine()` step to cut down
shuffle traffic, the way Word Count does. But a combiner
only ever sees values from **one mapper's local output** —
and a join needs to know whether the *other* dataset has a
matching key at all, which is global information no single
mapper has. A combiner can't decide "drop this `A` row, it
has no match in `B`" (INNER JOIN) or "this `A` row has no
match, pad it with `NULL`" (LEFT OUTER JOIN) — that
decision can only be made once *all* of a key's tagged
values, from every mapper, have arrived at one reducer.

There is one safe partial optimization: within Goal 1's
aggregate (Section 6), a combiner *could* pre-sum the
`("TXN", amount)` values that share a `customer_id` within
one mapper's partition — sum/count are associative and
commutative, same as in
[`word_count_in_mapreduce.md`](../word_count_in_mapreduce/word_count_in_mapreduce.md)
— as long as it leaves the `("CUST", customer_name)` value
untouched and simply passes it through. That trims data
volume; it does not, and cannot, decide the join itself.

## 12. From MapReduce to PySpark: join() on RDDs and DataFrames

Everything in Sections 5-11 was hand-rolled — tag, shuffle,
split, match, written out explicitly because that's what
raw MapReduce forces you to do. PySpark runs on the same
shuffle-based execution model, but exposes the reduce-side
join as a built-in operation on both of its APIs. The
mechanism underneath doesn't change; only the amount of
code you write to invoke it does.

### RDDs — `join()` is Section 9's algorithm, pre-written

Spark's pair-RDD API ships INNER/LEFT/RIGHT/FULL directly,
each corresponding to one of Section 9's reducers:

```python
# (customer_id, customer_name)
customers = sc.textFile("customers.csv") \
    .map(lambda line: line.split(",")) \
    .map(lambda f: (f[0], f[1]))

# (customer_id, transaction_amount)
transactions = sc.textFile("transactions.csv") \
    .map(lambda line: line.split(",")) \
    .map(lambda f: (f[2], float(f[3])))

customers.join(transactions)           # INNER  -> reduce_inner()
customers.leftOuterJoin(transactions)  # LEFT   -> reduce_left_outer()
customers.rightOuterJoin(transactions) # RIGHT  -> reduce_right_outer()
customers.fullOuterJoin(transactions)  # FULL   -> reduce_full_outer() (Section 13, exercise 3)
```

Each call shuffles both RDDs by key and groups them — that's
Section 9's steps 1-3 (tag, group, split) done for you — then
applies exactly the size-based rule Section 9's `reduce_*()`
functions spell out by hand: `leftOuterJoin()` wraps the `B`
side in `Optional`/`None` for the same reason
`reduce_left_outer()` emits `NULL` — there's nothing on that
side to pair with.

Goal 1's aggregate (Section 6) collapses to:

```python
transactions.map(lambda kv: (kv[0], (1, kv[1]))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .join(customers) \
    .map(lambda kv: (kv[1][1], kv[1][0][0], kv[1][0][1]))
# -> (customer_name, count, total) -- same output as reduce_goal1()
```

### DataFrames — `join()` reads like SQL

```python
customers_df.join(transactions_df, on="customer_id", how="inner")
customers_df.join(transactions_df, on="customer_id", how="left")
customers_df.join(transactions_df, on="customer_id", how="right")
customers_df.join(transactions_df, on="customer_id", how="full")
```

Goal 1 becomes one chained expression:

```python
transactions_df.groupBy("customer_id") \
    .agg(count("*").alias("transactions_count"),
         sum("transaction_amount").alias("total_transaction_amount")) \
    .join(customers_df, on="customer_id", how="inner") \
    .select("customer_name", "transactions_count", "total_transaction_amount")
```

Goal 3 (top-N, Section 8) is just
`.orderBy(desc("total_transaction_amount")).limit(N)` on that
same result — Spark's Catalyst optimizer plans a `limit()`
after a sort with essentially the local-top-N-then-merge
strategy Section 8 built by hand (a bounded structure per
partition, merged at the end), so that hand-written
heap-and-merge logic is close to what actually runs.

### Quick reference

| Join type | Section 9 reducer | RDD method | DataFrame `how=` |
|---|---|---|---|
| INNER | `reduce_inner()` | `.join()` | `"inner"` |
| LEFT OUTER | `reduce_left_outer()` | `.leftOuterJoin()` | `"left"` |
| RIGHT OUTER | `reduce_right_outer()` | `.rightOuterJoin()` | `"right"` |
| FULL OUTER | `reduce_full_outer()` (exercise) | `.fullOuterJoin()` | `"full"` |

### Why Sections 5-11 still matter

Two reasons the hand-rolled version isn't just busywork now
that `df1.join(df2)` is one line:

1. **The cost model doesn't go away.** Skewed keys, the
   `O(|A_list| x |B_list|)` cross product, and shuffle cost
   (Section 9's "General Reduce-Side Join Algorithm") are
   exactly what makes a Spark join slow or blow out executor
   memory in production. Knowing the algorithm underneath is
   what lets you read a Spark UI stage and understand *why*
   one task is stuck long after the others finish.
2. **The map-side join escape hatch is the same idea.**
   Section 13's "replicated join" exercise is
   `pyspark.sql.functions.broadcast(small_df)` on DataFrames
   (or a broadcast variable on RDDs) — Spark just gives it a
   one-word spelling.

## 13. Food for Thought

1. Rewrite `reduce_goal1()` (Section 6) using the
   generic `reduce_inner()` from Section 9 as a starting
   point, by feeding its emitted `(customer_name,
   transaction_amount)` pairs into a second aggregation
   pass. What does that second pass's `map()`/`reduce()`
   look like?

2. What happens to Goal 1's output if a `customer_id` in
   the transactions file does not exist in the customers
   file (an orphan foreign key)? Trace it through
   `reduce_inner()` and through `reduce_goal1()` — do they
   agree?

3. Write `reduce_full_outer()` — emit every key present in
   **either** `A_list` or `B_list`, padding whichever side
   is missing with `NULL`. (Hint: it's the union of the
   LEFT OUTER and RIGHT OUTER cases, with the inner
   double-loop shared between them.)

4. Goal 2 finds customers with **zero** transactions. How
   would you adapt `reduce_goal2()` to instead find
   customers with **fewer than 2** transactions?

5. At bank scale, one side of a join is sometimes small
   enough to fit in memory (say, a few thousand VIP
   customers) while the other is huge (billions of
   transactions). How could you avoid the shuffle
   entirely for that case? (This is a **map-side join** —
   look up "replicated join" / Hadoop's
   `DistributedCache`.)

## 14. Comments

Comments and suggestions are welcome!

## 15. References

1. [Data-Intensive Text Processing with MapReduce by Jimmy Lin and Chris Dyer](../mapreduce_by_Jimmy_Lin/MapReduce-algorithms-by-Jimmy-Lin.pdf) (this repository) — see the chapter on relational joins
2. [`word_count_in_mapreduce/word_count_in_mapreduce.md`](../word_count_in_mapreduce/word_count_in_mapreduce.md) — companion worked example, same map/shuffle/reduce trace style
3. [PySpark `pyspark.RDD.join()` API docs](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.join.html) — RDD join/leftOuterJoin/rightOuterJoin/fullOuterJoin
4. [PySpark `pyspark.sql.DataFrame.join()` API docs](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.join.html) — DataFrame join with the `how=` parameter
