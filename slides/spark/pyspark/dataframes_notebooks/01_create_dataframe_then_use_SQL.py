import marimo

__generated_with = "0.24.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, round, sum, count, desc

    # Initialize SparkSession with DuckDB-like / ANSI SQL capabilities
    spark = SparkSession.builder.appName("PySparkSQLCSVAnalysis").getOrCreate()
    print("spark.version=", spark.version)
    return mo, spark


@app.cell
def _(mo):
    mo.md(r"""
    # PySpark SQL CSV Data Analysis Tutorial

    Welcome to this interactive notebook! In this guide, we will:
    1. **Create/Load a CSV file** containing retail sales data.
    2. **Read the CSV into PySpark** with headers and schema inference enabled.
    3. **Register the DataFrame as a Temporary SQL View** (`sales_data`).
    4. **Execute SQL Queries** to filter, aggregate, calculate metrics, and order results.
    """)
    return


@app.cell
def _(mo):
    # Cell 1: Sample CSV File Creation
    # Writing sample sales data to a local CSV file to simulate reading from an external data source.
    sample_csv_data = """sale_id,store_id,product,category,quantity,unit_price
    1,S01,Latte,Coffee,2,4.50
    2,S01,Espresso,Coffee,1,3.00
    3,S02,Croissant,Bakery,3,3.25
    4,S03,Cold Brew,Coffee,1,4.00
    5,S02,Latte,Coffee,4,4.50
    6,S03,Green Tea,Tea,2,3.50
    7,S04,Muffin,Bakery,5,2.75
    8,S01,Chai Latte,Tea,1,4.25"""

    csv_file_path = "sales_transactions.csv"
    with open(csv_file_path, "w") as f:
        f.write(sample_csv_data)

    mo.md(f"**Step 1:** CSV file successfully written to `{csv_file_path}`.")
    return (csv_file_path,)


@app.cell
def _(csv_file_path, mo, spark):
    # Cell 2: Reading the CSV File into PySpark
    # We set header=True to read the column names from line 1 and inferSchema=True to automatically detect data types.
    sales_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csv_file_path)
    )

    mo.md("### Step 2: Loaded CSV Data into PySpark DataFrame")
    sales_df.show()
    return (sales_df,)


@app.cell
def _(sales_df):
    sales_df.printSchema()
    return


@app.cell
def _(mo, sales_df):
    # Cell 3: Registering DataFrame as a Temp View
    # Registering the view allows us to write standard SQL queries directly against the dataset.
    sales_df.createOrReplaceTempView("sales_data")

    mo.md("### Step 3: Temporary SQL View `sales_data` Created")
    return


@app.cell
def _(mo, spark):
    # Cell 4: SQL Query 1 - Selecting & Previewing Raw Data
    # Basic SELECT query with ORDER BY to view the dataset sorted by sale_id.
    query1 = """
    SELECT sale_id, store_id, product, category, quantity, unit_price
    FROM sales_data
    ORDER BY sale_id DESC
    """
    df_raw = spark.sql(query1)

    mo.md("### Cell 4: Inspecting All Records (`SELECT ... ORDER BY`)")
    df_raw.show()
    return


@app.cell
def _(mo, spark):
    # Cell 5: SQL Query 2 - Filtering Rows
    # Filtering items belonging exclusively to the 'Coffee' category using WHERE.
    query2 = """
    SELECT sale_id, store_id, product, quantity, unit_price
    FROM sales_data
    WHERE category = 'Coffee'
    ORDER BY unit_price DESC
    """
    df_filtered = spark.sql(query2)

    mo.md("### Cell 5: Filtering Data (`WHERE category = 'Coffee'`)")
    df_filtered.show()
    return


@app.cell
def _(mo, spark):
    # Cell 5.1: SQL Query 2 - Filtering Rows
    # Filtering items belonging exclusively to the 'Coffee' category using WHERE.
    query22 = """
    SELECT sale_id, store_id, product, quantity, unit_price
    FROM sales_data
    WHERE category = 'Coffee'
    ORDER BY unit_price DESC
    """
    df_filtered_22 = spark.sql(query22)

    mo.md("### Cell 5: Filtering Data (`WHERE category = 'Coffee'`)")
    print("df_filtered_22.collect()=", df_filtered_22.collect())

    """
    df_filtered_22.collect()= 
    [
      Row(sale_id=1, store_id='S01', product='Latte', quantity=2, unit_price=4.5),
      Row(sale_id=5, store_id='S02', product='Latte', quantity=4, unit_price=4.5),
      Row(sale_id=4, store_id='S03', product='Cold Brew', quantity=1, unit_price=4.0),
      Row(sale_id=2, store_id='S01', product='Espresso', quantity=1, unit_price=3.0)
    ]
    """
    return


@app.cell
def _(mo, spark):
    # Cell 6: SQL Query 3 - Calculated Columns & Expressions
    # Calculating line-item revenue (quantity * unit_price) directly in standard SQL.
    query3 = """
    SELECT 
        sale_id,
        store_id,
        product,
        quantity,
        unit_price,
        ROUND(quantity * unit_price, 2) AS line_item_revenue
    FROM sales_data
    ORDER BY line_item_revenue DESC
    """
    df_calculated = spark.sql(query3)

    mo.md("### Cell 6: Calculated Column (`quantity * unit_price AS line_item_revenue`)")
    df_calculated.show()

    return


@app.cell
def _(mo, spark):
    # Cell 7: SQL Query 4 - Aggregating & Summarizing by Group
    # Grouping by store_id to compute total transactions, total units sold, and total sales revenue.
    query7 = """
    SELECT 
        store_id,
        COUNT(sale_id) AS total_transactions,
        SUM(quantity) AS total_items_sold,
        ROUND(SUM(quantity * unit_price), 2) AS total_store_revenue
    FROM sales_data
    GROUP BY store_id
    ORDER BY total_store_revenue DESC
    """
    df_aggregated = spark.sql(query7)

    mo.md("### Cell 7: Grouping & Aggregating (`GROUP BY store_id`)")
    df_aggregated.show()

    return


if __name__ == "__main__":
    app.run()
