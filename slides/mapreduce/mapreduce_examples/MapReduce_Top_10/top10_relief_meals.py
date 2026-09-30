"""In-memory simulation of two MapReduce jobs; Python 3, no dependencies."""
import csv
import heapq
from collections import defaultdict
from pathlib import Path

K = 10


def top_k(records, k):
    """Records are (hub_id, total); higher total, then smaller ID wins."""
    if k <= 0:
        raise ValueError('k must be positive')
    heap = []
    for hub_id, total in records:
        # IDs are H followed by a fixed-width numeric suffix. Larger quality
        # tuples are better, so the root is the worst retained candidate.
        item = (total, -int(hub_id[1:]), hub_id)
        if len(heap) < k:
            heapq.heappush(heap, item)
        elif item > heap[0]:
            heapq.heapreplace(heap, item)
    return [(hub_id, total) for total, _, hub_id in
            sorted(heap, reverse=True)]


def run(rows, k=K, reducers=3, combine=True):
    if reducers <= 0:
        raise ValueError('reducers must be positive')
    # Job 1: map each batch to (hub_id, meals), optionally combine by hub.
    mapped = defaultdict(list)
    for row in rows:
        mapped[row['mapper_partition']].append(
            (row['hub_id'], int(row['meals_delivered'])))
    shuffled = defaultdict(list)
    for records in mapped.values():
        if combine:
            local = defaultdict(int)
            for hub_id, meals in records:
                local[hub_id] += meals
            records = local.items()
        for hub_id, meals in records:
            shuffled[hub_id].append(meals)
    # All contributions for a hub reach one reducer. Top-k runs only after
    # that hub's complete total is known, across ALL keys of each reducer.
    partitions = [[] for _ in range(reducers)]
    for hub_id, values in shuffled.items():
        partitions[int(hub_id[1:]) % reducers].append((hub_id, sum(values)))
    candidates = [record for part in partitions
                  for record in top_k(part, k)]
    # Job 2: map candidates to one constant key; one reducer merges them.
    return top_k(candidates, k)


def main():
    with Path(__file__).with_name('relief_meals.csv').open(newline='') as f:
        rows = list(csv.DictReader(f))
    reference = defaultdict(int)
    for row in rows:
        reference[row['hub_id']] += int(row['meals_delivered'])
    expected = sorted(reference.items(), key=lambda x: (-x[1], x[0]))[:K]
    for reducers in (1, 2, 3, 7, 30):
        for combine in (False, True):
            assert run(rows, reducers=reducers, combine=combine) == expected
            assert run(list(reversed(rows)), reducers=reducers,
                       combine=combine) == expected
    assert run([]) == []
    assert run(rows, k=30) == sorted(reference.items(),
                                    key=lambda x: (-x[1], x[0]))
    assert top_k([('H02', 5), ('H01', 5)], 1) == [('H01', 5)]
    # Demonstrate the tempting, incorrect early-pruning algorithm.
    local = defaultdict(list)
    for row in rows:
        local[row['mapper_partition']].append(
            (row['hub_id'], int(row['meals_delivered'])))
    early_candidates = [record for part in local.values()
                        for record in top_k(part, K)]
    assert 'H21' not in {hub for hub, _ in early_candidates}
    assert expected[0] == ('H21', 120000)
    names = {row['hub_id']: row['hub_name'] for row in rows}
    for rank, (hub, total) in enumerate(run(rows), 1):
        print(f'{rank:2d}. {hub} {names[hub]:8s} {total:,}')
    print('All checks passed; early mapper pruning loses the winner H21.')


if __name__ == '__main__':
    main()
