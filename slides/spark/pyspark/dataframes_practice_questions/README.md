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

| Folder | Purpose |
|---|---|
| [data/](data) | Small CSV and JSON input files used by the questions |
