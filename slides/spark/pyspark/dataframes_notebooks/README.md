# DataFrame Notebooks (marimo)

Five [marimo](https://marimo.io) notebooks that teach PySpark **DataFrames** to students who already know
**RDDs**. Each topic is shown next to the RDD code you already know (`map`, `filter`, `reduceByKey`,
`aggregateByKey`, `join`, ...). All five use the same made-up coffee chain, *Bean & Leaf* (stores, products, sales, customer reviews).

| # | Notebook | Topics |
|---|---|---|
| 0 | [`00_create_dataframe.py`](./00_create_dataframe.py) | Basics: How to Create a DataFrame|
| 1 | [`01_create_dataframe.py`](./01_create_dataframe.py) | Basics: How to Create a DataFrame|
| 2 | [`02_dataframe_basics.py`](02_dataframe_basics.py) | RDD vs. DataFrame · `SparkSession` · creating DataFrames (lists, `Row`, explicit schema / DDL string, from an RDD and back, CSV, JSON) · inspecting (`show`, `printSchema`, `dtypes`, `count`, `describe`, `Row` objects) · lazy evaluation · `select`, `withColumn`, `withColumnRenamed`, `drop`, `selectExpr` · `filter` / `where` · `orderBy`, `distinct`, `dropDuplicates` · `when` / `otherwise` · **cleaning messy data** step by step (`trim`, `initcap`, `regexp_replace`, `try_cast`, `try_to_date`, `fillna`, `dropna`) |
| 3 | [`03_dataframe_aggregations.py`](03_dataframe_aggregations.py) | `agg()` on the whole table · `groupBy()` vs. `reduceByKey()` / `aggregateByKey()` · several aggregates at once · grouping by several columns · filter before (`WHERE`) vs. after (`HAVING`) · `NULL`s in aggregates · conditional aggregation (`sum(when(...))`, `count_if`) · `collect_list` / `collect_set` · `median`, `max_by` · `pivot` · `rollup` · a first look at window functions · Spark SQL |
| 4 | [`04_dataframe_joins.py`](04_dataframe_joins.py) | pair-RDD `join` vs. DataFrame `join` · `on=` name / list / condition, ambiguous columns, aliases · `inner`, `left`, `right`, `full`, `left_semi`, `left_anti`, `cross` · three-table join + aggregation · joins on several columns · range (non-equi) join · self-join · traps: duplicate keys, `NULL` keys (`eqNullSafe`) · broadcast joins · joins in Spark SQL |
| 5 | [`05_dataframes_with_claude.py`](05_dataframes_with_claude.py) | **Spark + Claude:** API key from a `.env` file · first API call · structured output (a Pydantic schema → guaranteed JSON columns) · labeling review text on the driver (`collect` → Claude → `createDataFrame` → `join`) and on the executors (`mapPartitions`, one client per partition) · analyzing the labels with `groupBy` / `pivot` and checking them against star ratings · Claude writes Spark SQL from an English question (with safety checks) · Spark aggregates, Claude writes the manager's report · cost, privacy, and good practices |
| 6 | [`06_dataframe_advanced.py`](06_dataframe_advanced.py) | arrays: `split`, `explode` (word count vs. `flatMap`), `explode_outer`, `posexplode`, `size`, `array_contains`, `get` · nested data: structs, arrays of structs, `read.json`, `from_json` · window functions: `row_number` / `rank` / `dense_rank`, top-N per group, `lag` / `lead`, running totals and moving averages with `rowsBetween` · set operations: the `union` trap, `unionByName`, `intersect`, `subtract` · dates (`to_timestamp`, `date_format`, `date_trunc`, `datediff`, `date_add`) and strings (`regexp_extract`, `substring`, `lpad`) · writing Parquet with `partitionBy` (partition pruning), `cache`, `repartition` vs. `coalesce` · `withColumns`, `unpivot`, `transform`, `replace`, `sample`, `randomSplit` |

Notebooks 2–5 each end with an **RDD → DataFrame cheat sheet**, an **interactive widget** (dropdowns/sliders that
re-run the query), and **four exercises** with fold-out solutions. Notebook 4 has a "type your own question" box
and three exercises.

| Folder | Contents |
|---|---|
| [`data/`](data) | The CSV and JSON files read by Notebooks 1 and 5 |
| [`.env.example`](.env.example) | Template for Notebook 4's settings: copy it to `.env` and add your Anthropic API key |

## Running

```bash
pip install marimo pyspark      # Spark 4.x needs Java 17 or 21
marimo edit 01_dataframe_basics.py
```

`marimo edit` opens the notebook in your browser. `marimo run 01_dataframe_basics.py` shows it read-only, and
`python 01_dataframe_basics.py` runs it as a plain script. The output of `df.show()` appears below each cell.

**Notebook 4 needs an Anthropic API key** (and makes about 30 small API calls per run, a few cents):

```bash
pip install anthropic python-dotenv
cp .env.example .env              # then edit .env: ANTHROPIC_API_KEY=sk-ant-...
marimo edit 04_dataframes_with_claude.py
```

The notebook reads `.env` from **this folder**. `.env` is git-ignored, so the key is never committed. Without a
key, the notebook shows a warning and skips the cells that call Claude.

Written and tested with Spark 4.3.0 and marimo 0.23. Spark 4 runs in ANSI mode by default, so Notebook 1 uses
`try_cast()` and `try_to_date()` when cleaning (a plain `cast()` of bad text fails the job).

## See also

* [`../dataframes_practice_questions/`](../dataframes_practice_questions): 10 practice sets with answers
* [`../dataframes_udf/`](../dataframes_udf): user-defined functions (not covered in these notebooks)
* [`../dataframes_tutorial/`](../dataframes_tutorial): step-by-step DataFrame modules (text)
* [`../marimo/`](../marimo): a short intro to marimo with PySpark
* [`../rdd_transformations_review/`](../rdd_transformations_review): the RDD review these notebooks build on
* [`../dataframes_transformations_review/`](../dataframes_transformations_review): the DataFrame exam review (same data as the RDD review)
