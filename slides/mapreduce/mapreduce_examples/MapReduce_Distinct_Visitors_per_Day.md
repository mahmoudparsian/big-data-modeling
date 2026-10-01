# MapReduce Example: <br> Distinct Website Visitors per Day

## 1. Problem

* A website records one event for every page visit. 
* A person can visit several pages, and their events can appear in different input splits.
* Find the number of **distinct users per day**, not the number of visits.

This example uses **two MapReduce jobs**:

```text
Visit events
  → Job 1: deduplicate (date, user_id) pairs
  → Job 2: count the unique users for each date
  → Daily distinct visitor counts
```

Unlike ordinary Word Count, summing local distinct counts is incorrect
when the same user appears in more than one mapper's input.

## 2. Input Format and Assumptions

```text
event_id,date,user_id,page
```

- Dates are already normalized to the same reporting time zone.
- A nonempty `user_id` consistently identifies one user for this exercise.
- Each input record is valid CSV; use a CSV parser, not a naive comma split.
- We count authenticated user IDs, not necessarily distinct human beings.
- The toy data has no header. A real input reader must skip headers.
- Output includes dates with at least one valid visit. Dates with no visits
  require a separate calendar dataset if zero-count rows are needed.

For large event logs, distributing the work can be useful. The tiny
sample below makes every intermediate result easy to inspect; it does
not itself require a distributed system.

## 3. Sample Dataset: Two Mapper Partitions

**Partition A — Mapper 1:**

```csv
E01,2026-09-01,U1,/home
E02,2026-09-01,U2,/home
E03,2026-09-01,U1,/pricing
E04,2026-09-02,U1,/home
E05,2026-09-02,U3,/docs
E06,2026-09-02,U3,/pricing
```

**Partition B — Mapper 2:**

```csv
E07,2026-09-01,U2,/docs
E08,2026-09-01,U3,/home
E09,2026-09-01,U3,/pricing
E10,2026-09-02,U3,/home
E11,2026-09-02,U4,/docs
E12,2026-09-02,U4,/pricing
E13,2026-09-02,U5,/home
```

There are six visits on September 1 and seven on September 2. The distinct users are:

| Date | Distinct users | Desired count |
|---|---|---:|
| 2026-09-01 | U1, U2, U3 | 3 |
| 2026-09-02 | U1, U3, U4, U5 | 4 |

## 4. Why Adding Local Distinct Counts Fails

| Date | Mapper 1's users | Mapper 2's users | Sum of local counts | Correct global count |
|---|---|---|---:|---:|
| 2026-09-01 | U1, U2 | U2, U3 | 2 + 2 = 4 | 3 |
| 2026-09-02 | U1, U3 | U3, U4, U5 | 2 + 3 = 5 | 4 |

The local counts lose the identities needed to detect overlap. Job 1
keeps `(date, user_id)` as the key until global deduplication is complete.

## 5. Job 1: Mapper

The input key is an offset or record identifier; we ignore it.
The output key is a **tuple**, not an ambiguously concatenated string.
The integer `1` is only a presence marker.

```text
map1(input_key, csv_line):
    event_id, date, user_id, page = parse_csv(csv_line)
    emit((date, user_id), 1)
```

Every mapper call is shown below:

| Mapper | Event | Emitted key | Value |
|---|---|---|---:|
| 1 | E01 | (2026-09-01, U1) | 1 |
| 1 | E02 | (2026-09-01, U2) | 1 |
| 1 | E03 | (2026-09-01, U1) | 1 |
| 1 | E04 | (2026-09-02, U1) | 1 |
| 1 | E05 | (2026-09-02, U3) | 1 |
| 1 | E06 | (2026-09-02, U3) | 1 |
| 2 | E07 | (2026-09-01, U2) | 1 |
| 2 | E08 | (2026-09-01, U3) | 1 |
| 2 | E09 | (2026-09-01, U3) | 1 |
| 2 | E10 | (2026-09-02, U3) | 1 |
| 2 | E11 | (2026-09-02, U4) | 1 |
| 2 | E12 | (2026-09-02, U4) | 1 |
| 2 | E13 | (2026-09-02, U5) | 1 |

## 6. Job 1: Optional Combiner

Keep one presence marker per key in each local group:

```text
# date_user : (date, userID)
# markers : Iterable<Integer> (list of 1's)
combine1(date_user, markers):
    emit(date_user, max(markers))
```

Every marker is `1`, so the combined value is also `1`. This is safe
with zero, one, or multiple combiner passes: `max` is associative and
commutative, and the output type matches the mapper's output type.
Correctness must not depend on the framework running a combiner.

If one complete local combining pass runs per mapper, the outputs are:

```text
Mapper 1 after combining:
((2026-09-01, U1), 1)
((2026-09-01, U2), 1)
((2026-09-02, U1), 1)
((2026-09-02, U3), 1)

Mapper 2 after combining:
((2026-09-01, U2), 1)
((2026-09-01, U3), 1)
((2026-09-02, U3), 1)
((2026-09-02, U4), 1)
((2026-09-02, U5), 1)
```

This illustrative pass reduces 13 records to 9. Actual savings depend
on how the framework schedules combining and on local duplicate rates.
It cannot remove duplicates shared by different mappers.

## 7. Job 1: Shuffle and Sort

The partitioner must route equal **full `(date, user_id)` keys** to the
same reducer. Different users on the same date may go to different
reducers. The framework groups values by the full key.

| Key | Values without a combiner | Values after the illustrated combiner pass |
|---|---|---|
| (2026-09-01, U1) | [1, 1] | [1] |
| (2026-09-01, U2) | [1, 1] | [1, 1] |
| (2026-09-01, U3) | [1, 1] | [1] |
| (2026-09-02, U1) | [1] | [1] |
| (2026-09-02, U3) | [1, 1, 1] | [1, 1] |
| (2026-09-02, U4) | [1, 1] | [1] |
| (2026-09-02, U5) | [1] | [1] |

The table is displayed in sorted order for readability. Do not assume
values arrive in a particular order or that separate reducer output
files are globally sorted.

## 8. Job 1: Reducer and Output

Emit exactly one record for every distinct pair:

**Option 1: not optimized:**

```text
# date_user : (date, userID)
# markers : Iterable<Integer> (list of 1's)
reduce1(date_user, markers):
    emit(date_user, max(markers))
```

**Better revised reducer: optimized**

```text
# date_user : (date, userID)
# markers : Iterable<Integer> (list of 1's)
reduce1(date_user, markers):
    emit(date_user, 1)
```

All seven reducer calls return `1`, regardless of whether combining ran:

```text
((2026-09-01, U1), 1)
((2026-09-01, U2), 1)
((2026-09-01, U3), 1)
((2026-09-02, U1), 1)
((2026-09-02, U3), 1)
((2026-09-02, U4), 1)
((2026-09-02, U5), 1)
```

The reducer can compute `max` while streaming the markers; it does not
need a set of all users. Each invocation already represents one user
on one date.

## 9. Job 2: Mapper

Job 2 reads **all successfully committed output from Job 1**. These are
seven deduplicated records, not the original thirteen events.

```text
# date_user : (date, userID)
# presence : Integer as 1
map2(date_user, presence):
    date, user_id = date_user
    emit(date, 1)
```

The seven mapper outputs are:

```text
(2026-09-01, 1)  # U1
(2026-09-01, 1)  # U2
(2026-09-01, 1)  # U3
(2026-09-02, 1)  # U1
(2026-09-02, 1)  # U3
(2026-09-02, 1)  # U4
(2026-09-02, 1)  # U5
```

The annotations explain where each row came from; they are not part of
the emitted record. Job 2's input splits need not match Job 1's splits.

## 10. Job 2: Combiner, Shuffle, and Reducer

Now ordinary summation is correct because each input row represents a
unique `(date, user_id)` pair.

```text
combine2(date, counts):
    emit(date, sum(counts))

reduce2(date, counts):
    emit(date, sum(counts))
```

Without a combiner, the grouped input and reducer results are:

```text
reduce2(2026-09-01, [1, 1, 1]) → (2026-09-01, 3)
reduce2(2026-09-02, [1, 1, 1, 1]) → (2026-09-02, 4)
```

For one possible Job 2 split, Mapper A receives U1 and U2 for the first
date and U1 for the second; Mapper B receives the remaining four rows.
Their local combiners emit:

```text
Mapper A: (2026-09-01, 2), (2026-09-02, 1)
Mapper B: (2026-09-01, 1), (2026-09-02, 4)
```

After shuffling, the reducers sum `[2, 1]` and `[1, 3]`, yielding
`3` for September 1 and `4` for September 2. Partial counts remain integers, so repeated combining
also works. Use a sufficiently wide integer type for large counts.

## 11. Final Output

```text
(2026-09-01, 3)
(2026-09-02, 4)
```

These are **daily unique users**. Adding them gives seven user-days, not
seven unique users across the whole period: only U1, U2, U3, U4, and U5 appear
in the complete dataset.

## 12. Runnable Python Simulation

Save the following as `distinct_visitors.py` and run it with Python 3:

```text
python3 distinct_visitors.py
```

On Windows, `py distinct_visitors.py` is another option if the Python
launcher is installed. No third-party packages are needed.

This is an in-memory teaching simulation, not a distributed execution
engine. Its lists model intermediate records and its dictionary models
shuffle grouping; production MapReduce uses partitioning, disk spill,
network transfer, and task scheduling.

```python
import csv
from collections import defaultdict

PARTITIONS = [
    [
        "E01,2026-09-01,U1,/home",
        "E02,2026-09-01,U2,/home",
        "E03,2026-09-01,U1,/pricing",
        "E04,2026-09-02,U1,/home",
        "E05,2026-09-02,U3,/docs",
        "E06,2026-09-02,U3,/pricing",
    ],
    [
        "E07,2026-09-01,U2,/docs",
        "E08,2026-09-01,U3,/home",
        "E09,2026-09-01,U3,/pricing",
        "E10,2026-09-02,U3,/home",
        "E11,2026-09-02,U4,/docs",
        "E12,2026-09-02,U4,/pricing",
        "E13,2026-09-02,U5,/home",
    ],
]


def grouped(records):
    groups = defaultdict(list)
    for key, value in records:
        groups[key].append(value)
    return groups


def aggregate(records, operation):
    return [(key, operation(values))
            for key, values in sorted(grouped(records).items())]


def local_passes(records, operation, passes):
    for _ in range(passes):
        records = aggregate(records, operation)
    return records


def run(partitions, job1_passes=0, job2_passes=0):
    mapped1 = []
    for partition in partitions:
        local = []
        for event_id, date, user_id, page in csv.reader(partition):
            local.append(((date, user_id), 1))
        mapped1.extend(local_passes(local, max, job1_passes))

    unique_pairs = aggregate(mapped1, max)  # Job 1 shuffle + reduce

    # Choose two illustrative input splits for Job 2.
    mapped2 = []
    for partition in (unique_pairs[::2], unique_pairs[1::2]):
        local = [(date, 1) for (date, user_id), marker in partition]
        mapped2.extend(local_passes(local, sum, job2_passes))
    return dict(aggregate(mapped2, sum))   # Job 2 shuffle + reduce


expected = {"2026-09-01": 3, "2026-09-02": 4}
for passes1 in (0, 1, 2):
    for passes2 in (0, 1, 2):
        assert run(PARTITIONS, passes1, passes2) == expected

# Moving duplicates to different splits must not change the result.
all_events = [line for partition in PARTITIONS for line in partition]
assert run([all_events]) == expected
assert run([[line] for line in reversed(all_events)], 1, 1) == expected
assert run([]) == {}
assert run([[all_events[0]], [all_events[0]]]) == {"2026-09-01": 1}

for date, count in sorted(run(PARTITIONS).items()):
    print(date, count)
print("All checks passed.")
```

Expected output:

```text
2026-09-01 3
2026-09-02 4
All checks passed.
```

The simulation checks representative schedules; the correctness
argument is the key grouping and the associative, commutative merge
operations, not merely the number of test cases.

## 13. Design Tradeoffs and Common Mistakes

- **Why two jobs?** 
	- Job 1 groups by `(date, user_id)`; 
	- Job 2 regroups by `date`. 
	- Job 1 & Job 2 require different grouping keys and a job boundary.
- **Could one job work?** Yes: map to `(date, user_id)` as separate key
  and value, then build a set of users in each date's reducer. That
  simple approach needs memory proportional to the users for a date.
  The two-job design avoids that per-date set. Secondary sorting is
  another, more advanced design option.
- **What does a combiner guarantee?** Nothing about whether it runs.
  Correctness must hold if it is skipped or applied to partial groups.
  The record-count savings illustrated above are not guaranteed.
- **Where can skew occur?** Job 1 distributes full composite keys;
  Job 2 still sends all records for a date to one reducer. Large dates
  can be bottlenecks. Additional sharded aggregation stages can help.
- **Does deduplication eliminate all data-quality problems?** No.
  Inconsistent IDs, missing IDs, and inconsistent time zones need an
  explicit policy before the mapper emits records.
- **Do retries mean sums should be idempotent?** No. Correct framework
  output-commit behavior prevents successful task attempts from being
  counted twice. A combiner merges disjoint input contributions; it
  must not arbitrarily replay a previously counted contribution.

## 14. Practice Questions

1. Add `E14,2026-09-01,U4,/home` to Partition B. Trace both jobs.
   Which output changes?
2. Find distinct visitors **per page per day**. What is the key in
   Job 1, and what is the key in Job 2?
3. Find distinct visitors across the **entire period**. Why is summing
   the daily counts wrong, and how should the grouping keys change?
4. Filter out visits to `/health` before counting. In which mapper
   should the filter run, and why must it precede deduplication?
5. Return only dates with at least four distinct users. Why should
   this filter run after Job 2's final aggregation, not in a combiner?

**Answer checks:** <br>

* (1) September 1 becomes 4; September 2 remains 4.
* (2) Deduplicate `(date, page, user_id)`, then count by `(date, page)`.
* (3) Deduplicate by `user_id`, then count under a constant key: 5 users.
* (4) Filter the original events in Job 1's mapper while `page` is present.
* (5) Partial counts below four can still combine into a final count of four
or more.

## 15. Related Worked Examples

- [Word Count](MapReduce_Word_Count.md) — summation after choosing the key.
- [Total Sales per Store](MapReduce_Total_Sales_per_Store.md) — combining
  several aggregates for a key.
- [Inverted Index](MapReduce_Inverted_Index.md) — another use of composite
  information and deduplication in a distributed computation.
