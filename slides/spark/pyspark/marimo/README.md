# Marimo + PySpark

## What Is Marimo?

[marimo](https://marimo.io) is an open-source **reactive notebook**
for Python. It looks similar to Jupyter on the surface, but works
differently in ways that matter for teaching and for real projects:

* **It's a plain `.py` file, not JSON.** A marimo notebook is regular
  Python source, so it diffs and merges cleanly in git — no more
  meaningless whole-file JSON diffs on every run.
* **It's reactive.** When you change a cell — including moving a UI
  widget like a slider — marimo automatically re-runs every other
  cell that depends on it, in the correct dependency order. You never
  have to remember to "restart and run all," and cells can't silently
  get out of sync with what's on screen (a classic Jupyter footgun).
* **It runs as a script, too.** Because it's just Python, you can run
  a marimo notebook directly (`python notebook.py`) or import it like
  any other module, not just through the browser UI.

Install it with:

```bash
pip install marimo
```

## Files in This Folder

| Name | Description |
|---|---|
| [`pyspark_marimo_example.py`](./pyspark_marimo_example.py) | **Basic:** a simple marimo notebook — one DataFrame, filtered interactively with a slider |
| [`pyspark_marimo_intermediate.py`](./pyspark_marimo_intermediate.py) | **Intermediate:** joining two DataFrames, `groupBy().agg()`, `when()`/`otherwise()`, filtered interactively with a dropdown *and* a slider |

## Running a Notebook

```bash
pip install marimo pyspark
marimo edit pyspark_marimo_example.py
```

`marimo edit` opens the notebook in your browser with live editing.
To just run it once and view the output (no editing UI), use:

```bash
marimo run pyspark_marimo_example.py
```

or run it as a plain script:

```bash
python pyspark_marimo_example.py
```

(Substitute `pyspark_marimo_intermediate.py` for the intermediate
notebook.)

## What the Basic Notebook Does

1. Starts a local `SparkSession`.
2. Creates a small sample DataFrame (name, age, salary).
3. Shows a `mo.ui.slider()` for a minimum-salary threshold.
4. Filters the DataFrame with `df.filter(col("salary") >= min_salary.value)`
   — dragging the slider re-runs this cell automatically and shows
   the new result, no manual re-run needed.

## What the Intermediate Notebook Does

1. Starts a local `SparkSession`.
2. Creates `employees` and `departments` DataFrames and **joins** them
   on `dept_id`.
3. Adds a `salary_band` column with `when()`/`otherwise()` (the
   inline-if/else way to derive a column, as an alternative to a UDF
   — see [`../UDF/`](../UDF)).
4. Builds a per-department summary with `groupBy().agg()` — headcount,
   average salary, max salary — sorted with `orderBy()`.
5. Filters interactively by **both** a department dropdown and a
   salary slider at once; moving either widget re-runs the filter.

## See Also

* [`jupyter/`](../jupyter) — the more traditional way to run PySpark
  interactively, for comparison
* [`spark-submit-example/`](../spark-submit-example) — running a
  PySpark program as a batch job instead of interactively
* [marimo.io](https://marimo.io) — official marimo documentation
