# PySpark DataFrame Practice Questions

Practice questions on PySpark **DataFrames**, meant to be solved interactively in the
`bin/pyspark` shell. They are grouped into numbered **sets**, and each set focuses on one
part of the DataFrame API. Each set has a questions file and a matching answers file,
with real output from Spark 4.2.0. Try the questions before you open the answers.

These sets are the DataFrame companion to the
[RDD practice questions](../rdd_practice_questions/README.md), and they use some of the
same data (as CSV and JSON files with headers), so you can compare the two APIs. For the
concepts behind each set, see the [DataFrames tutorial](../dataframes_tutorial/README.md).

| Set | Questions | Answers | Covers |
|---|---|---|---|
| 01 | [QUESTIONS_01.md](QUESTIONS_01.md) | [ANSWERS_01.md](ANSWERS_01.md) | Creating and inspecting DataFrames: `createDataFrame()`, schemas (DDL strings and `StructType`), `Row`s, reading CSV and nested JSON, read modes for bad rows, RDD ↔ DataFrame, `describe()`/`summary()`, profiling |
| 02 | [QUESTIONS_02.md](QUESTIONS_02.md) | [ANSWERS_02.md](ANSWERS_02.md) | Working with columns: `select`/`selectExpr`, `withColumn(s)`, `cast` vs. `try_cast` (ANSI mode), `when`/`otherwise`, string, date, struct, and array functions, many columns at once, cleaning a messy file |
| 03 | [QUESTIONS_03.md](QUESTIONS_03.md) | [ANSWERS_03.md](ANSWERS_03.md) | Filtering, sorting, nulls, and duplicates: `filter`/`where`, `&`, `\|`, `~`, text and list filters, `orderBy` with `NULL` placement, `distinct`/`dropDuplicates`, `na.drop`/`na.fill`, top N and paging, three-valued logic, rule-based validation |
| 04 | [QUESTIONS_04.md](QUESTIONS_04.md) | [ANSWERS_04.md](ANSWERS_04.md) | Grouping and aggregation: `groupBy`/`agg`, whole-table aggregates, `collect_list`/`collect_set`, conditional counts, `WHERE` vs. `HAVING`, `pivot`, `rollup`/`cube` subtotals, medians and approximate aggregates, the average-of-ratios trap |
| 05 | [QUESTIONS_05.md](QUESTIONS_05.md) | [ANSWERS_05.md](ANSWERS_05.md) | Joins: inner, left, right, full, semi, and anti joins, key-name and ambiguity problems, four-table joins, broadcast joins in `explain()`, range joins, reconciliation with a full outer join, duplicate and `NULL` keys, self-joins |
| 06 | [QUESTIONS_06.md](QUESTIONS_06.md) | [ANSWERS_06.md](ANSWERS_06.md) | Window functions: `row_number`/`rank`/`dense_rank`, group totals on every row, top N per group, keeping the first record per key, `lag`/`lead`, running totals and window frames (the default-frame trap), `ntile`/`percent_rank`, gaps and islands |
| 07 | [QUESTIONS_07.md](QUESTIONS_07.md) | [ANSWERS_07.md](ANSWERS_07.md) | Spark SQL: temporary views and `spark.sql()`, SQL vs. the DataFrame API, `F.expr()`, joins and subqueries (and the `NOT IN` + `NULL` trap), CTEs, window functions and `QUALIFY`, named parameters and SQL injection, view lifetimes, a full pipeline in SQL |
| 08 | [QUESTIONS_08.md](QUESTIONS_08.md) | [ANSWERS_08.md](ANSWERS_08.md) | UDFs: Python UDFs and their traps (return types, `NULL`s), UDFs in SQL, pandas UDFs, the Python vs. Spark rounding difference, why built-in functions are faster (`EvalPython` in the plan), struct-returning UDFs, `applyInPandas`/`mapInPandas`, fuzzy matching |
| 09 | [QUESTIONS_09.md](QUESTIONS_09.md) | [ANSWERS_09.md](ANSWERS_09.md) | Reading, writing, and performance: CSV, JSON, and Parquet, save modes, `partitionBy` and partition pruning, controlling the number of output files, reading `explain()` (pushdown, column pruning), caching DataFrames, adaptive query execution and join strategies, an end-to-end ETL job |
| 10 | [QUESTIONS_10.md](QUESTIONS_10.md) | [ANSWERS_10.md](ANSWERS_10.md) | **Mixed review**: 9 exam-style questions on the ride-sharing data, where choosing the right tools from Sets 01–09 is part of the question (the DataFrame companion to [RDD Set 10](../rdd_practice_questions/QUESTIONS_10.md)) |

| Folder | Purpose |
|---|---|
| [data/](data) | Small CSV and JSON input files used by the questions |
| `output/` | Created by the Set 09 answers when you run them; ignored by git (see [`.gitignore`](.gitignore)) |
