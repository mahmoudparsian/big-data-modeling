# Word Count, Classic

The word-count problem, worked from first principles: why it matters
in practice, then solved twice — once in plain Python, once as a
real MapReduce job — as the "before/after" pair students see before
moving on to `mapreduce_examples/`, `combiners/`, and the rest of
`mapreduce/`.

## Folder Contents

| Name | Description |
|---|---|
| [`word_count_applications.md`](./word_count_applications.md) | Why word count matters beyond the classroom: real product uses (reading-time estimates, search, moderation, ...), and why it's a natural first example of distributed processing |
| [`word_count_in_python/`](./word_count_in_python/) | Word count in **plain Python** — no Hadoop, no Spark — the "before MapReduce" baseline. See its own README |
| [`word_count_in_mapreduce/`](./word_count_in_mapreduce/) | The same problem solved as a real MapReduce job: `(filename, record)` pairs → a Python `mapper()` → shuffle/sort → a Python `reducer()`, with a preview of the PySpark port. See its own README |

## Suggested order

1. [`word_count_applications.md`](./word_count_applications.md) — motivation.
2. [`word_count_in_python/`](./word_count_in_python/) — solve it without MapReduce first.
3. [`word_count_in_mapreduce/`](./word_count_in_mapreduce/) — the same solution reframed as `mapper()`/`reducer()`.
