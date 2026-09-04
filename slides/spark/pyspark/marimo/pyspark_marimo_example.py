import marimo

__generated_with = "0.23.10"
app = marimo.App(width="medium", app_title="PySpark + Marimo — Simple Example")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 🔥 PySpark + Marimo: A Simple Example

    This notebook creates a small PySpark DataFrame and lets you
    filter it interactively with a slider — moving the slider
    automatically re-runs the Spark filter below it.

    **Setup:**
    ```bash
    pip install marimo pyspark
    marimo edit pyspark_marimo_example.py
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
    from pyspark.sql.functions import col

    spark = (
        SparkSession.builder
        .appName("pyspark-marimo-example")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return col, mo, spark


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 2 — Create a Sample DataFrame
    """)
    return


@app.cell
def _(spark):
    some_data = [
        ("alex", 20, 12000),
        ("jane", 30, 45000),
        ("rafa", 40, 56000),
        ("ted",  30, 145000),
        ("xo2",  10, 1332000),
        ("mary", 44, 555000),
    ]
    column_names = ["name", "age", "salary"]
    df = spark.createDataFrame(some_data, column_names)
    df.show()
    return (df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 3 — Filter Interactively

    Drag the slider below. Because marimo is **reactive**, moving
    it re-runs the Spark filter in the next cell automatically —
    there's no "restart and run all" step like in Jupyter.
    """)
    return


@app.cell
def _(mo):
    min_salary = mo.ui.slider(
        start=0, stop=1_500_000, value=50_000, step=10_000, label="Minimum salary"
    )
    min_salary
    return (min_salary,)


@app.cell
def _(col, df, min_salary):
    filtered = df.filter(col("salary") >= min_salary.value)
    filtered.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 4 — Stop Spark

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
