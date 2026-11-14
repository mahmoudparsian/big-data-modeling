import marimo

__generated_with = "0.24.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import duckdb
    from pyspark.sql import SparkSession

    return SparkSession, duckdb, mo


@app.cell
def _(mo):
    mo.md("""
    # DuckDB to PySpark DataFrame
    This notebook initializes a local Spark session, queries a DuckDB database using Apache Arrow, and converts the result into a PySpark DataFrame.
    """)
    return


@app.cell
def _(SparkSession):
    # Initialize local SparkSession with Arrow optimization enabled
    spark = (
        SparkSession.builder.appName("DuckDB-to-PySpark")
        .master("local[*]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    print("spark.version=", spark.version)
    return (spark,)


@app.cell
def _(duckdb):
    # Connect to DuckDB (replace ':memory:' with 'path/to/your_database.duckdb')
    con = duckdb.connect(":memory:")

    # Create a sample table for demonstration
    con.execute("""
        CREATE OR REPLACE TABLE sales_data AS 
        SELECT * FROM (VALUES 
            (1, 'Laptop', 1200.00, 'Electronics'),
            (2, 'Desk Chair', 350.50, 'Furniture'),
            (3, 'Monitor', 425.00, 'Electronics'),
            (4, 'Coffee Maker', 85.00, 'Appliances')
        ) AS t(id, product_name, price, category)
    """)
    return (con,)


@app.cell
def _(con, mo, sales_data):
    _df = mo.sql(
        f"""
        SELECT *
        FROM sales_data;
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo, spark):
    # 1. Read the DuckDB table as a PyArrow Table
    my_table = con.execute("""
    SELECT * FROM sales_data
    """).df()

    # 2. Convert the Arrow table to a Pandas DataFrame, then to a PySpark DataFrame
    # (With Arrow enabled in Spark config, toPandas() -> createDataFrame() is fast and preserves types)
    spark_df = spark.createDataFrame(my_table)

    # Display schema and preview using marimo's UI
    mo.vstack(
        [
            mo.md("### PySpark DataFrame Created"),
            mo.plain_text(spark_df._jdf.schema().treeString()),
            mo.ui.table(spark_df.toPandas(), label="PySpark DataFrame Preview"),
        ]
    )
    return (spark_df,)


@app.cell
def _(spark_df):
    # Example PySpark transformation
    from pyspark.sql import functions as F

    category_summary = spark_df.groupBy("category").agg(
        F.count("id").alias("item_count"),
        F.sum("price").alias("total_revenue"),
    )

    category_summary.show()
    return


if __name__ == "__main__":
    app.run()
