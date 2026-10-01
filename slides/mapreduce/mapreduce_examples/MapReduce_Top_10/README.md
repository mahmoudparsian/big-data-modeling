# MapReduce Top 10

Worked example: finding the top 10 disaster-relief 
hubs by meals delivered — a two-job MapReduce algorithm 
(aggregate first, then bounded-heap selection) over 
synthetic data, with a runnable Python simulation.

| Name | Description |
|---|---|
| [`MapReduce_Top_10_Design_Pattern.md`](MapReduce_Top_10_Design_Pattern.md) | Explain Top-10 Design Pattern |
: full map/combine/shuffle/reduce traces, why naive local top-k pruning before summation is wrong, bounded-heap selection, deterministic tie-breaking, a correctness proof, cost analysis, and practice questions |
| [`MapReduce_Top_10_Example.md`](MapReduce_Top_10_Example.md) | Exact two-job top-10 algorithm: full map/combine/shuffle/reduce traces, why naive local top-k pruning before summation is wrong, bounded-heap selection, deterministic tie-breaking, a correctness proof, cost analysis, and practice questions |
| [`top10_relief_meals.py`](top10_relief_meals.py) | Pure-Python, dependency-free in-memory simulation of the two jobs; run with `python3 top10_relief_meals.py` |
| [`relief_meals.csv`](relief_meals.csv) | Synthetic input dataset (32 delivery batches across 21 hubs — 10 hubs receive a second batch) used by the simulation |
