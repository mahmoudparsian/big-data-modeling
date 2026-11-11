import marimo

__generated_with = "0.23.10"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""
    # PySpark Sales DataFrame & <br> Basic Transformations Tutorial
    This Marimo notebook covers creating a sales DataFrame and performing basic operations like filtering, calculated columns, groupings, and sorting.
    """)
    return


@app.cell
def _():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, round, sum, count, desc

    spark = SparkSession.builder.appName("SalesDataTutorial").getOrCreate()
    return col, count, desc, round, spark, sum


@app.cell
def _(spark):
    sales_rows = [
        (1, "S01", "Latte",      "Coffee", 2, 4.50),
        (2, "S01", "Espresso",   "Coffee", 1, 3.00),
        (3, "S02", "Croissant",  "Bakery", 3, 3.25),
        (4, "S03", "Cold Brew",  "Coffee", 1, 4.00),
        (5, "S02", "Latte",      "Coffee", 4, 4.50),
        (6, "S03", "Green Tea",  "Tea",    2, 3.50),
        (7, "S04", "Muffin",     "Bakery", 5, 2.75),
        (8, "S01", "Chai Latte", "Tea",    1, 4.25),
    ]

    columns = ["sale_id", "store_id", "product", "category", "quantity", "unit_price"]

    sales_df = spark.createDataFrame(sales_rows, schema=columns)

    # Display in marimo
    return (sales_df,)


@app.cell
def _(mo, sales_df):
    mo.md("### sales_df DataFrame Schema")
    sales_df.printSchema()
    return


@app.cell
def _(mo, sales_df):
    mo.md("### Original Sales DataFrame")
    sales_df.show()
    return


@app.cell
def _(col, mo, sales_df):
    mo.md("### 1. Filtering Rows (Coffee Only)")
    coffee_df = sales_df.filter(col("category") == "Coffee")
    coffee_df.show()
    return


@app.cell
def _(col, mo, round, sales_df):
    mo.md("### 2. Adding Calculated Columns (Total Revenue)")
    revenue_df = sales_df.withColumn("total_revenue", round(col("quantity") * col("unit_price"), 2))
    revenue_df.show()
    return


@app.cell
def _(col, count, desc, mo, sales_df, sum):
    mo.md("### 3. Grouping and Aggregating by Store")
    store_summary_df = (
        sales_df
        .withColumn("total_revenue", col("quantity") * col("unit_price"))
        .groupBy("store_id")
        .agg(
            count("sale_id").alias("total_transactions"),
            sum("quantity").alias("items_sold"),
            sum("total_revenue").alias("gross_revenue")
        )
        .orderBy(desc("gross_revenue"))
    )
    store_summary_df.show()
    return


if __name__ == "__main__":
    app.run()
