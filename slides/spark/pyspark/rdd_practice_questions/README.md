# PySpark RDD Practice Questions

Practice questions on PySpark RDDs, meant to be solved interactively in
the `bin/pyspark` shell. They are grouped into numbered **sets**, and each
set focuses on a group of RDD transformations. Each set has a questions
file and a matching answers file. Try the questions before you open the answers.

| Set | Questions | Answers | Covers |
|---|---|---|---|
| 01 | [QUESTIONS_01.md](QUESTIONS_01.md) | [ANSWERS_01.md](ANSWERS_01.md) | 9 questions (3 basic, 3 intermediate, 3 intermediate+) using `parallelize()`/`textFile()`, `map()`, `mapValues()`, `filter()`, `distinct()`, `reduceByKey()`, `collect()`, `count()` |
| 02 | [QUESTIONS_02.md](QUESTIONS_02.md) | [ANSWERS_02.md](ANSWERS_02.md) | 9 questions (3 basic, 3 intermediate, 3 intermediate+) adding `flatMap()`, `groupByKey()`, `sortByKey()`, including "implement `distinct()` yourself" |
| 03 | [QUESTIONS_03.md](QUESTIONS_03.md) | [ANSWERS_03.md](ANSWERS_03.md) | The power of `mapPartitions()` (and `mapPartitionsWithIndex()`): once-per-partition setup, local combining, batching, one-pass statistics, CSV parsing, global top-N, running totals |
| 04 | [QUESTIONS_04.md](QUESTIONS_04.md) | [ANSWERS_04.md](ANSWERS_04.md) | Reducers only: `groupByKey()`, `reduceByKey()`, `combineByKey()`, including when each one is right, and when `reduceByKey()` gives a wrong answer |
| 05 | [QUESTIONS_05.md](QUESTIONS_05.md) | [ANSWERS_05.md](ANSWERS_05.md) | Joins: `join()`, `leftOuterJoin()`, `cogroup()`, including data quality checks, join size, 3-way joins, self-joins, and building the joins yourself from `cogroup()` |

| Folder | Purpose |
|---|---|
| [data/](data) | Small text input files used by the questions |
