import marimo

__generated_with = "0.23.10"
app = marimo.App(width="medium", app_title="PySpark + Marimo — Intermediate Example")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 🔥 PySpark + Marimo: An Intermediate Example

    This notebook builds on
    [`pyspark_marimo_example.py`](./pyspark_marimo_example.py) (a single
    DataFrame + one slider) with a few more everyday PySpark moves:

    * joining two DataFrames
    * `groupBy().agg()` with multiple aggregates, then `orderBy()`
    * a derived column with `when()` / `otherwise()`
    * **two** interactive widgets (a dropdown *and* a slider) combined
      into one filter — moving either one reactively re-runs the cell
      below it

    **Setup:**
    ```bash
    pip install marimo pyspark
    marimo edit pyspark_marimo_intermediate.py
    ```
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 1 — Start a SparkSession
    """)
    return


@app.cell
def _():
    import marimo as mo
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import avg, col, count, max as spark_max, when

    spark = (
        SparkSession.builder
        .appName("pyspark-marimo-intermediate")
        .master("local[*]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return SparkSession, avg, col, count, mo, spark, spark_max, when


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 2 — Create Two DataFrames and Join Them

    `employees` only knows each person's `dept_id`; `departments` maps
    that id to a human-readable name. Joining them is the usual way to
    bring related data together in Spark.
    """)
    return


@app.cell
def _(spark):
    some_employees = [
        (1, "alex", 10, 12000),
        (2, "jane", 20, 45000),
        (3, "rafa", 10, 56000),
        (4, "ted",  20, 145000),
        (5, "xo2",  30, 1332000),
        (6, "mary", 30, 555000),
        (7, "coco", 10, 33000),
        (8, "lee",  20, 78000),
    ]
    employees = spark.createDataFrame(
        some_employees, ["id", "name", "dept_id", "salary"]
    )

    some_departments = [
        (10, "Engineering"),
        (20, "Sales"),
        (30, "Executive"),
    ]
    departments = spark.createDataFrame(some_departments, ["dept_id", "dept_name"])

    joined = employees.join(departments, on="dept_id", how="inner")
    joined.show()
    return departments, employees, joined, some_departments, some_employees


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 3 — Add a Derived Column with `when()` / `otherwise()`

    `when()`/`otherwise()` is PySpark's inline if/else for building a
    new column out of existing ones — here, bucketing salary into
    bands without writing a UDF (see [`../UDF/`](../UDF) for the UDF
    way to do this kind of thing).
    """)
    return


@app.cell
def _(col, joined, when):
    banded = joined.withColumn(
        "salary_band",
        when(col("salary") < 50_000, "Low")
        .when(col("salary") < 200_000, "Medium")
        .otherwise("High"),
    )
    banded.show()
    return (banded,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 4 — `groupBy().agg()` and `orderBy()`

    A per-department summary: headcount, average salary, and max
    salary, sorted by average salary descending.
    """)
    return


@app.cell
def _(avg, count, joined, spark_max):
    summary = (
        joined.groupBy("dept_name")
        .agg(
            count("*").alias("num_employees"),
            avg("salary").alias("avg_salary"),
            spark_max("salary").alias("max_salary"),
        )
        .orderBy("avg_salary", ascending=False)
    )
    summary.show()
    return (summary,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 5 — Filter Interactively with Two Widgets

    Pick a department *and* drag the salary slider. Both widgets feed
    the same filter cell below, and moving either one re-runs it
    automatically.
    """)
    return


@app.cell
def _(mo):
    dept_choice = mo.ui.dropdown(
        options=["All", "Engineering", "Sales", "Executive"],
        value="All",
        label="Department",
    )
    min_salary = mo.ui.slider(
        start=0, stop=1_500_000, value=40_000, step=10_000, label="Minimum salary"
    )
    mo.hstack([dept_choice, min_salary])
    return dept_choice, min_salary


@app.cell
def _(banded, col, dept_choice, min_salary):
    filtered = banded.filter(col("salary") >= min_salary.value)
    if dept_choice.value != "All":
        filtered = filtered.filter(col("dept_name") == dept_choice.value)
    filtered.show()
    return (filtered,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 6 — Stop Spark

    Uncomment the line below when you're done experimenting, to
    release the local Spark cluster's resources.
    """)
    return


@app.cell
def _():
    # spark.stop()
    return


if __name__ == "__main__":
    app.run()
