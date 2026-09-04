# Jupyter + PySpark

## What Is Jupyter?

[Jupyter Notebook](https://jupyter.org) is the classic interactive
Python notebook: a browser-based UI where you write and run code in
cells and see results (text, tables, plots) inline, right below the
cell that produced them. It's the most widely used notebook tool in
data science and a common way to explore data with PySpark before
writing it up as a batch job.

A couple of things worth knowing going in, especially if you're
comparing it with [`../marimo/`](../marimo):

* **Notebooks are stored as JSON** (the `.ipynb` file format), with
  cell code, outputs, and execution counters all in one file. That
  makes for noisy diffs in git compared to a plain `.py` file.
* **Execution order is up to you.** Cells run only when you run them,
  in whatever order you run them — it's possible to run cells out of
  order and end up with results on screen that don't match the code
  above them. Re-running "top to bottom" (Restart & Run All) is the
  usual fix.
* **A running kernel holds your Spark session.** The `SparkSession`
  you create lives in the notebook's Python kernel process until you
  either call `.stop()` or shut the kernel down.

Install it with:

```bash
pip install jupyter
```

## Files in This Folder

| Name | Description |
|---|---|
| [`pyspark_jupyter_example.ipynb`](./pyspark_jupyter_example.ipynb) | **Basic:** a simple, executed notebook — one DataFrame, filtered by a threshold |
| [`pyspark_jupyter_intermediate.ipynb`](./pyspark_jupyter_intermediate.ipynb) | **Intermediate:** joining two DataFrames, `groupBy().agg()`, `when()`/`otherwise()`, filtered by two conditions |
| [`run_pyspark_jupyter.sh`](./run_pyspark_jupyter.sh) | Shell script that launches Jupyter with a PySpark-ready kernel via `$SPARK_HOME/bin/pyspark` |

## Running a Notebook

The simplest way — no `$SPARK_HOME` setup needed — is to just open it
with a `pip install`ed Jupyter and PySpark:

```bash
pip install jupyter pyspark
jupyter notebook pyspark_jupyter_example.ipynb
```

(Substitute `pyspark_jupyter_intermediate.ipynb` for the intermediate
notebook.)

Alternatively, if you have a full Spark installation, you can launch
Jupyter through `$SPARK_HOME/bin/pyspark` with
[`run_pyspark_jupyter.sh`](./run_pyspark_jupyter.sh):

```bash
export SPARK_HOME=/opt/spark
./run_pyspark_jupyter.sh
```

## What the Basic Notebook Does

This is the same example used in
[`../marimo/pyspark_marimo_example.py`](../marimo/pyspark_marimo_example.py),
so you can compare the two side by side:

1. Starts a local `SparkSession`.
2. Creates a small sample DataFrame (name, age, salary).
3. Filters it by a `MIN_SALARY` threshold.

The difference: in the marimo version, dragging a slider automatically
re-runs the filter cell. Here, changing `MIN_SALARY` means manually
re-running that cell (and anything after it) yourself.

## What the Intermediate Notebook Does

This mirrors
[`../marimo/pyspark_marimo_intermediate.py`](../marimo/pyspark_marimo_intermediate.py):

1. Starts a local `SparkSession`.
2. Creates `employees` and `departments` DataFrames and **joins** them
   on `dept_id`.
3. Adds a `salary_band` column with `when()`/`otherwise()` (the
   inline-if/else way to derive a column, as an alternative to a UDF
   — see [`../UDF/`](../UDF)).
4. Builds a per-department summary with `groupBy().agg()` — headcount,
   average salary, max salary — sorted with `orderBy()`.
5. Filters by both `DEPT_FILTER` and `MIN_SALARY` — change either
   variable and re-run that cell to see a different result.

## See Also

* [`marimo/`](../marimo) — a reactive-notebook alternative, for
  comparison
* [`spark-submit-example/`](../spark-submit-example) — running a
  PySpark program as a batch job instead of interactively
