# DataFrame Notebooks (marimo)

Eight notebooks (00–07) that teach PySpark **DataFrames** to students who already know **RDDs**. Each topic
is shown next to the RDD code you already know (`map`, `filter`, `reduceByKey`, `aggregateByKey`, `join`, ...).
All of them use the same made-up coffee chain, *Bean & Leaf* (stores, products, sales, customer reviews).
Start with 00–02 (short warm-ups on the same 8 sales), then work through 03–07.

| # | Notebook | Topics |
|---|---|---|
| 0 | [`00_create_dataframe_python.py`](./00_create_dataframe_python.py) | Warm-up (a plain Python script, not a marimo notebook): create a DataFrame from a Python list, `show()`, `printSchema()`. Run it with `python 00_create_dataframe_python.py` |
| 1 | [`01_create_dataframe_then_use_SQL.py`](./01_create_dataframe_then_use_SQL.py) | Warm-up: write a CSV file, read it with `header` / `inferSchema`, register a temporary view, then query it with **Spark SQL** (`SELECT`, `WHERE`, calculated columns, `GROUP BY`). Each SQL query is followed by the same query with the DataFrame API |
| 2 | [`02_create_dataframe.py`](./02_create_dataframe.py) | Warm-up: the same sales as a DataFrame built from a Python list, then the **DataFrame API** (`filter`, `withColumn`, `groupBy` / `agg`, `orderBy`). Each query is followed by the same query in Spark SQL |
| 3 | [`03_dataframe_basics.py`](03_dataframe_basics.py) | RDD vs. DataFrame · `SparkSession` · creating DataFrames (lists, `Row`, explicit schema / DDL string, from an RDD and back, CSV, JSON) · inspecting (`show`, `printSchema`, `dtypes`, `count`, `describe`, `Row` objects) · lazy evaluation · `select`, `withColumn`, `withColumnRenamed`, `drop`, `selectExpr` · `filter` / `where` · `orderBy`, `distinct`, `dropDuplicates` · `when` / `otherwise` · **cleaning messy data** step by step (`trim`, `initcap`, `regexp_replace`, `try_cast`, `try_to_date`, `fillna`, `dropna`), and the same cleaning as one SQL `WITH` query |
| 4 | [`04_dataframe_aggregations.py`](04_dataframe_aggregations.py) | `agg()` on the whole table · `groupBy()` vs. `reduceByKey()` / `aggregateByKey()` · several aggregates at once · grouping by several columns · filter before (`WHERE`) vs. after (`HAVING`) · `NULL`s in aggregates · conditional aggregation (`sum(when(...))`, `count_if`) · `collect_list` / `collect_set` · `median`, `max_by` · `pivot` (SQL `PIVOT`) · `rollup` (SQL `GROUP BY ROLLUP`) · a first look at window functions (`OVER (PARTITION BY ...)`) · Spark SQL |
| 5 | [`05_dataframe_joins.py`](05_dataframe_joins.py) | pair-RDD `join` vs. DataFrame `join` · `on=` name / list / condition (SQL `USING` vs. `ON`), ambiguous columns, aliases · `inner`, `left`, `right`, `full`, `left_semi`, `left_anti`, `cross` · three-table join + aggregation · joins on several columns · range (non-equi) join · self-join · traps: duplicate keys, `NULL` keys (`eqNullSafe`, SQL `<=>`) · broadcast joins (`F.broadcast`, SQL `/*+ BROADCAST */`) · joins in Spark SQL |
| 6 | [`06_dataframes_with_claude.py`](06_dataframes_with_claude.py) | **Spark + Claude:** API key from a `.env` file · first API call · structured output (a Pydantic schema → guaranteed JSON columns) · labeling review text on the driver (`collect` → Claude → `createDataFrame` → `join`) and on the executors (`mapPartitions`, one client per partition) · analyzing the labels with `groupBy` / `pivot` and checking them against star ratings · Claude writes Spark SQL from an English question (with safety checks) · Spark aggregates, Claude writes the manager's report · cost, privacy, and good practices |
| 7 | [`07_dataframe_advanced.py`](07_dataframe_advanced.py) | arrays: `split`, `explode` (word count vs. `flatMap`), `explode_outer`, `posexplode`, `size`, `array_contains`, `get` · nested data: structs, arrays of structs, `read.json`, `from_json` · window functions: `row_number` / `rank` / `dense_rank`, top-N per group, `lag` / `lead`, running totals and moving averages with `rowsBetween` · set operations: the `union` trap, `unionByName`, `intersect`, `subtract` · dates (`to_timestamp`, `date_format`, `date_trunc`, `datediff`, `date_add`) and strings (`regexp_extract`, `substring`, `lpad`) · writing Parquet with `partitionBy` (partition pruning), `cache`, `repartition` vs. `coalesce` · `withColumns`, `unpivot`, `transform`, `replace`, `sample`, `randomSplit` |

**DataFrame API and SQL side by side (Notebooks 1–5):** every query is shown twice, once with the
DataFrame API (`select`, `filter`, `groupBy`, `join`, ...) and once in Spark SQL (`spark.sql("SELECT ...")`),
in two cells next to each other that print the same result. The exercise solutions show both versions too.

Notebooks 3, 4, 5 and 7 each end with an **RDD → DataFrame cheat sheet**, an **interactive widget**
(dropdowns/sliders that re-run the query), and **four exercises** with fold-out solutions. Notebook 6 has a
"type your own question" box and three exercises.

### DuckDB → PySpark

Two extra notebooks show how to move data from [DuckDB](https://duckdb.org) into Spark:

| Notebook | What it shows |
|---|---|
| [`duckdb_to_pyspark.py`](duckdb_to_pyspark.py) | Query DuckDB (in memory), pass the result through Apache Arrow, and turn it into a PySpark DataFrame |
| [`auto_insurance_analysis_pyspark.py`](auto_insurance_analysis_pyspark.py) | Export the `insurance` table of `auto_insurance_db.duckdb` to Parquet with DuckDB, read it with `spark.read.parquet()`, then answer 17 questions, each in Spark SQL **and** with the DataFrame API (the notebook checks that both give the same rows): 4 business questions (loss ratio, renewal conversion, top-3 outliers per state, tenure cohorts), 3 simple, 5 intermediate (`HAVING`, `CASE WHEN` buckets, `PIVOT`, scalar subquery, conditional aggregation) and 5 intermediate+ (percent of total, compare with the group average, `ROLLUP`, best offer per state, `NTILE` deciles with a running total). It writes the Parquet file to `./tmp_spark_staging/`, so run it from this folder |

### Other files

| File / folder | Contents |
|---|---|
| [`data/`](data) | The CSV and JSON files read by Notebooks 3 and 7 |
| [`auto_insurance_db.duckdb`](auto_insurance_db.duckdb) | The DuckDB database used by `auto_insurance_analysis_pyspark.py` (needs DuckDB 2.0+) |
| [`marimo_setup_guide_macbook.md`](marimo_setup_guide_macbook.md) | Step-by-step setup on macOS: Python, Java, a virtual environment, marimo, PySpark, DuckDB |
| [`marimo_setup_guide_windows.md`](marimo_setup_guide_windows.md) | The same setup on Windows |
| [`.env.example`](.env.example) | Template for Notebook 6's settings: copy it to `.env` and add your Anthropic API key |

## Running

```bash
pip install marimo pyspark      # Spark 4.x needs Java 17 or 21
marimo edit 03_dataframe_basics.py
```

`marimo edit` opens the notebook in your browser. `marimo run 03_dataframe_basics.py` shows it read-only, and
`python 03_dataframe_basics.py` runs it as a plain script. The output of `df.show()` appears below each cell.
(For the full setup, see the macOS and Windows guides above.)

Notebook 1 writes the file `sales_transactions.csv` into the folder you run it from.

**Notebook 6 needs an Anthropic API key** (and makes about 30 small API calls per run, a few cents):

```bash
pip install anthropic python-dotenv
cp .env.example .env              # then edit .env: ANTHROPIC_API_KEY=sk-ant-...
marimo edit 06_dataframes_with_claude.py
```

The notebook reads `.env` from **this folder**. `.env` is git-ignored, so the key is never committed. Without a
key, the notebook shows a warning and skips the cells that call Claude.

Written and tested with Spark 4.3.0 and marimo 0.23. Spark 4 runs in ANSI mode by default, so Notebook 3 uses
`try_cast()` and `try_to_date()` when cleaning (a plain `cast()` of bad text fails the job).

## See also

* [`../dataframes_practice_questions/`](../dataframes_practice_questions): 10 practice sets with answers
* [`../dataframes_udf/`](../dataframes_udf): user-defined functions (not covered in these notebooks)
* [`../dataframes_tutorial/`](../dataframes_tutorial): step-by-step DataFrame modules (text)
* [`../marimo/`](../marimo): a short intro to marimo with PySpark
* [`../rdd_transformations_review/`](../rdd_transformations_review): the RDD review these notebooks build on
* [`../dataframes_transformations_review/`](../dataframes_transformations_review): the DataFrame exam review (same data as the RDD review)
