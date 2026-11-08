import marimo

__generated_with = "0.23.10"
app = marimo.App(width="medium", app_title="DataFrames 1 — Basics")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # DataFrames, Part 1: The Basics

    **Notebook 1 of 5** · next: `02_dataframe_aggregations.py`, `03_dataframe_joins.py`, `04_dataframes_with_claude.py`, `05_dataframe_advanced.py`

    You already know **RDDs**: a distributed collection of Python objects that you
    transform with `map()`, `filter()`, `reduceByKey()`, and so on. This notebook
    introduces the **DataFrame**, the API you will use for most Spark work.

    **Our story:** *Bean & Leaf* is a small (made-up) coffee chain with stores in the
    Bay Area. We have its sales data, and we want to load it, look at it, ask simple
    questions, and clean it up.

    **What you will learn**

    1. What a DataFrame is, and how it differs from an RDD
    2. Creating DataFrames (from Python lists, from RDDs, from CSV and JSON files)
    3. Inspecting a DataFrame: `show()`, `printSchema()`, `count()`, `describe()`, ...
    4. Choosing columns, adding columns, filtering rows, and sorting
    5. Cleaning messy real-world data, step by step

    **Run it:** `marimo edit 01_dataframe_basics.py` (or `python 01_dataframe_basics.py`).
    Output of `df.show()` appears under each cell.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1. RDD vs. DataFrame

    An **RDD** is a list of Python objects spread over the cluster. Spark does not know
    what is *inside* each object; a tuple `(1, "S01", "Latte", 2, 4.5)` is just a blob to it.
    You remember that position `3` is the quantity.

    A **DataFrame** is a table: rows with **named, typed columns** (like a SQL table or a
    spreadsheet). Because Spark knows the structure (the **schema**), it can optimize your
    query before running it (the *Catalyst* optimizer), and you write `col("quantity")`
    instead of `t[3]`.

    | | RDD | DataFrame |
    |---|---|---|
    | Data | Python objects (tuples, dicts, ...) | Rows with named columns |
    | Structure known to Spark? | No | Yes, the **schema** (column names + types) |
    | You write | Python functions (`lambda t: t[3] * t[4]`) | Column expressions (`col("quantity") * col("unit_price")`) |
    | Optimized by Spark? | No, runs your functions as written | Yes, Catalyst rewrites the plan |
    | Lazy? | Yes | Yes (same idea: transformations vs. actions) |
    | Immutable? | Yes | Yes (every operation returns a *new* DataFrame) |
    | Entry point | `SparkContext` (`sc`) | `SparkSession` (`spark`) |

    Good news: **lazy evaluation and immutability work exactly as with RDDs.** And a
    DataFrame is built on top of RDDs, so you can always go back with `df.rdd`.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2. Start a `SparkSession`

    With RDDs you used a `SparkContext` (`sc`). For DataFrames the entry point is a
    `SparkSession` (`spark`). It still contains the old context: `spark.sparkContext`.

    We import the functions module as **`F`** (`F.col`, `F.sum`, `F.round`, ...). This is the
    usual style, because names like `sum`, `max`, `min` and `round` would otherwise hide
    Python's built-in functions.
    """)
    return


@app.cell
def _():
    import marimo as mo
    from pyspark.sql import Row, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    spark = (
        SparkSession.builder
        .appName("dataframes-01-basics")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")  # small data: few partitions
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext  # the SparkContext you know from RDDs
    print("Spark version:", spark.version)
    return (
        DoubleType,
        F,
        IntegerType,
        Row,
        StringType,
        StructField,
        StructType,
        mo,
        sc,
        spark,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 3. Your first DataFrame

    The simplest way: a Python list of tuples plus a list of column names. Compare it with
    the RDD you would have built from the same list.
    """)
    return


@app.cell
def _(sc, spark):
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

    # The RDD way: Spark sees 8 anonymous tuples
    sales_rdd = sc.parallelize(sales_rows)
    print("RDD first element:", sales_rdd.first())

    # The DataFrame way: the same data, but with column names
    sales = spark.createDataFrame(
        sales_rows,
        ["sale_id", "store_id", "product", "category", "quantity", "unit_price"],
    )
    sales.show()
    return sales, sales_rdd


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    `show()` prints the first 20 rows as a table. It is an **action** (like `collect()` or
    `take()` on an RDD): it makes Spark actually run the job.

    `printSchema()` shows the **schema**. Spark *inferred* the types from the Python values:
    Python `int` → `long`, `float` → `double`, `str` → `string`. `nullable = true` means
    the column may contain missing values (`NULL`, which is Python's `None`).
    """)
    return


@app.cell
def _(sales):
    sales.printSchema()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **The same question, both ways:** *What is the revenue (quantity × price) of each sale?*

    With the RDD you must remember positions (`t[4]` is the quantity). With the DataFrame
    you use column names, and the result is again a table.
    """)
    return


@app.cell
def _(F, sales, sales_rdd):
    # RDD: positions, Python lambda
    print(sales_rdd.map(lambda t: (t[0], t[4] * t[5])).take(3))

    # DataFrame: names, column expressions
    sales.select("sale_id", (F.col("quantity") * F.col("unit_price")).alias("revenue")).show(3)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 4. Other ways to create a DataFrame

    ### 4a. From `Row` objects

    A `Row` is like a named tuple. Spark takes the column names from the field names.
    """)
    return


@app.cell
def _(Row, spark):
    menu = spark.createDataFrame([
        Row(product="Latte", category="Coffee", price=4.50),
        Row(product="Muffin", category="Bakery", price=2.75),
        Row(product="Green Tea", category="Tea", price=3.50),
    ])
    menu.show()
    menu.printSchema()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 4b. With an explicit schema

    Inferring types is convenient, but in real programs you usually **declare** the schema.
    It is faster (Spark does not have to scan the data) and safer (you get exactly the
    types you want, e.g. `int` instead of `long`).

    There are two ways to write a schema: a `StructType` object, or a short **DDL string**
    (the same syntax as in SQL `CREATE TABLE`). Both give the same result.
    """)
    return


@app.cell
def _(DoubleType, IntegerType, StringType, StructField, StructType, spark):
    staff_schema = StructType([
        StructField("emp_id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("store_id", StringType(), nullable=True),
        StructField("hourly_wage", DoubleType(), nullable=True),
    ])
    staff_rows = [
        (101, "Ana", "S01", 21.50),
        (102, "Ben", "S02", 19.75),
        (103, "Chloe", "S01", None),  # wage not known yet -> NULL
    ]
    staff = spark.createDataFrame(staff_rows, schema=staff_schema)
    staff.printSchema()
    staff.show()

    # The same schema as a DDL string
    staff_ddl = spark.createDataFrame(
        staff_rows, schema="emp_id INT, name STRING, store_id STRING, hourly_wage DOUBLE"
    )
    print(staff_ddl.dtypes)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 4c. From an RDD, and back

    You can turn an RDD of tuples into a DataFrame with `toDF(column names)` (or
    `spark.createDataFrame(rdd, ...)`). Going the other way, `df.rdd` gives you an RDD of
    `Row` objects, so all your RDD skills still apply.
    """)
    return


@app.cell
def _(sc):
    pairs_rdd = sc.parallelize([("S01", 3), ("S02", 7), ("S03", 5)])
    pairs_df = pairs_rdd.toDF(["store_id", "cups"])
    pairs_df.show()

    back_to_rdd = pairs_df.rdd  # an RDD of Row objects
    print(back_to_rdd.collect())
    print(back_to_rdd.map(lambda r: (r.store_id, r.cups * 2)).collect())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 4d. From files: CSV and JSON

    In practice, data comes from files. `spark.read` has a reader for each format:
    `spark.read.csv(...)`, `spark.read.json(...)`, `spark.read.parquet(...)`, ...

    The file `data/coffee_sales_raw.csv` holds raw sales exported from the stores' cash
    registers. Let's read it three ways and compare the schemas.
    """)
    return


@app.cell
def _(mo):
    DATA_DIR = mo.notebook_dir() / "data"
    raw_csv_path = str(DATA_DIR / "coffee_sales_raw.csv")
    stores_json_path = str(DATA_DIR / "stores.json")
    print(open(raw_csv_path).read())
    return raw_csv_path, stores_json_path


@app.cell
def _(raw_csv_path, spark):
    # (1) No options: the header line is treated as data, columns are _c0, _c1, ...
    #     and every column is a string.
    raw_no_options = spark.read.csv(raw_csv_path)
    raw_no_options.show(3)
    raw_no_options.printSchema()
    return


@app.cell
def _(raw_csv_path, spark):
    # (2) header=True uses the first line as column names;
    #     inferSchema=True reads the data once more to guess the types.
    raw_sales = spark.read.csv(raw_csv_path, header=True, inferSchema=True)
    raw_sales.printSchema()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Look closely: `sale_id` became an integer, but `quantity` and `unit_price` are still
    **strings**. Why? Because one quantity is the word `two`, one is empty, and every price
    starts with a dollar sign. A single bad value makes the whole column a string.
    We will fix that in **Section 10 (Cleaning)**.

    (3) Declaring the schema yourself with `schema=` skips the guessing. Here we keep the
    messy columns as strings on purpose, so we can clean them later.

    JSON files (one JSON object per line) work the same way. Note that Spark sorts the
    JSON fields **alphabetically** when it infers the schema, and reads whole numbers as
    `long`.
    """)
    return


@app.cell
def _(raw_csv_path, spark, stores_json_path):
    raw_with_schema = spark.read.csv(
        raw_csv_path,
        header=True,
        schema="sale_id INT, sale_date STRING, store_id STRING, product STRING, "
               "quantity STRING, unit_price STRING, payment STRING",
    )
    raw_with_schema.printSchema()

    stores = spark.read.json(stores_json_path)
    stores.printSchema()
    stores.show()
    return raw_with_schema, stores


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 5. Inspecting a DataFrame

    Before you query data, look at it. These are the tools you will use every day:

    | Method | Returns | Action? |
    |---|---|---|
    | `df.columns` | list of column names | no (just the schema) |
    | `df.dtypes` | list of `(name, type)` pairs | no |
    | `df.schema` | the `StructType` object | no |
    | `df.printSchema()` | prints the schema as a tree | no |
    | `df.show(n, truncate, vertical)` | prints `n` rows (default 20) | **yes** |
    | `df.count()` | number of rows | **yes** |
    | `df.first()`, `df.head(n)`, `df.take(n)` | `Row` objects | **yes** |
    | `df.collect()` | **all** rows as a Python list (careful with big data!) | **yes** |
    | `df.describe()`, `df.summary()` | a DataFrame of statistics | lazy, then `show()` |

    `collect()`, `take()` and `first()` behave just like their RDD versions, but give you
    `Row` objects instead of tuples.
    """)
    return


@app.cell
def _(sales):
    print("columns:", sales.columns)
    print("dtypes: ", sales.dtypes)
    print("rows:   ", sales.count())

    first_sale = sales.first()
    print("first():", first_sale)
    # Three ways to read a field of a Row
    print(first_sale.product, first_sale["quantity"], first_sale[5])
    print(first_sale.asDict())
    return


@app.cell
def _(sales):
    # Only 2 rows, and print each row vertically (useful for wide tables)
    sales.show(2, vertical=True)
    return


@app.cell
def _(sales):
    # Quick statistics for numeric columns (count, mean, stddev, min, max)
    sales.describe("quantity", "unit_price").show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 6. Lazy evaluation: same as RDDs

    Methods like `select()`, `filter()`, `withColumn()` and `orderBy()` are
    **transformations**: they return a new DataFrame immediately and run *nothing*. Spark
    only records the plan. The work happens when you call an **action** (`show()`,
    `count()`, `collect()`, writing a file, ...).

    The difference from RDDs: before running, Spark **optimizes the whole plan**. For
    example, it can push a filter down so it runs before a join, or read only the columns
    you actually use.
    """)
    return


@app.cell
def _(F, sales):
    # This line runs no job, it only builds a plan:
    big_coffee_sales = sales.filter(F.col("category") == "Coffee").filter(F.col("quantity") >= 2)
    print(type(big_coffee_sales))

    # The action triggers the work:
    print("big coffee sales:", big_coffee_sales.count())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 7. Working with columns

    ### 7a. `select()`: choose columns (and compute new ones)

    `select()` is the DataFrame version of `map()`: for each row, it produces a new row.
    You can refer to a column in several ways; they all mean the same thing:

    * a string: `"product"`
    * `F.col("product")`: the most common way, works everywhere
    * `sales.product` or `sales["product"]`: tied to that DataFrame

    Use `.alias("name")` to name a computed column.
    """)
    return


@app.cell
def _(F, sales):
    sales.select("product", "quantity").show(3)

    sales.select(
        F.col("product"),
        F.upper(F.col("category")).alias("CATEGORY"),
        (F.col("quantity") * F.col("unit_price")).alias("revenue"),
    ).show(3)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 7b. Add, rename, and drop columns

    * `withColumn(name, expression)`: add a new column (or replace one with the same name)
    * `withColumnRenamed(old, new)`: rename a column
    * `drop(name, ...)`: remove columns

    Remember: DataFrames are **immutable**. These methods return a *new* DataFrame;
    `sales` itself never changes. Chain the calls and assign the result.
    """)
    return


@app.cell
def _(F, sales):
    sales_revenue = (
        sales
        .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
        .withColumnRenamed("unit_price", "price")
        .drop("category")
    )
    sales_revenue.show()
    print("sales still has:", sales.columns)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 7c. `selectExpr()`: SQL snippets

    If you know SQL, `selectExpr()` lets you write each column as a SQL expression.
    """)
    return


@app.cell
def _(sales):
    sales.selectExpr("product", "quantity * unit_price AS revenue", "quantity > 2 AS big_order").show(3)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 8. Filtering rows

    `filter()` (or its alias `where()`) keeps the rows where a condition is true, just like
    `rdd.filter(lambda ...)`, but the condition is a **column expression**.

    **Combining conditions:** use `&` (and), `|` (or), `~` (not), **not** Python's
    `and` / `or` / `not`. And put **parentheses around each condition**, because `&` binds
    more tightly than `==` or `>`:

    ```python
    F.col("category") == "Coffee" & F.col("quantity") > 1      # WRONG: error
    (F.col("category") == "Coffee") & (F.col("quantity") > 1)  # right
    ```
    """)
    return


@app.cell
def _(F, sales):
    # RDD:       sales_rdd.filter(lambda t: t[3] == "Coffee")
    # DataFrame:
    sales.filter(F.col("category") == "Coffee").show()

    # Two conditions with & and parentheses
    sales.filter((F.col("category") == "Coffee") & (F.col("quantity") > 1)).show()
    return


@app.cell
def _(F, sales):
    # A few more useful conditions
    sales.where(F.col("store_id").isin("S01", "S04")).show()        # value in a list
    sales.where(F.col("unit_price").between(3.0, 4.0)).show()        # inclusive range
    sales.where(F.col("product").contains("Latte")).show()           # substring
    sales.where(~(F.col("category") == "Coffee")).show()             # NOT
    sales.where("quantity >= 3 AND category <> 'Coffee'").show()     # a SQL string works too
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 9. Sorting, distinct values, and a few handy functions

    * `orderBy()` (or `sort()`) sorts rows. Use `F.desc("col")` or `F.col("col").desc()`
      for descending order. Give **several** columns to break ties.
    * `distinct()` removes duplicate rows; `dropDuplicates(["col", ...])` removes rows that
      repeat the given columns (and keeps one of them).
    * `limit(n)` keeps the first `n` rows (a transformation, unlike `take(n)`).
    """)
    return


@app.cell
def _(F, sales):
    # Highest quantity first; ties broken by sale_id
    sales.orderBy(F.desc("quantity"), "sale_id").show()

    sales.select("category").distinct().orderBy("category").show()

    # Keep one sale per store (the dropped rows are not guaranteed, so we sort after)
    sales.dropDuplicates(["store_id"]).select("store_id").orderBy("store_id").show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### `when()` / `otherwise()`: if/else for columns

    With RDDs you would write `if` inside a `map()` lambda. With DataFrames you build the
    condition as an expression. `F.lit(value)` turns a plain Python value into a column.
    """)
    return


@app.cell
def _(F, sales):
    sales.select(
        "product",
        "quantity",
        F.when(F.col("quantity") >= 4, "large")
         .when(F.col("quantity") >= 2, "medium")
         .otherwise("small")
         .alias("order_size"),
        F.lit("Bean & Leaf").alias("brand"),
        F.concat_ws(" - ", "store_id", "product").alias("label"),
        F.round(F.col("unit_price") * 1.1, 2).alias("price_after_10pct"),
    ).show(truncate=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 10. Cleaning messy data: a worked case

    Real data is never clean. Here is the raw cash-register export again
    (`raw_with_schema`, every messy column read as a string). Find the problems before you
    read the list below.
    """)
    return


@app.cell
def _(raw_with_schema):
    raw_with_schema.show(truncate=False)
    print("rows:", raw_with_schema.count())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Problems in this data**

    | Problem | Example | Fix |
    |---|---|---|
    | Exact duplicate row | sale `1006` appears twice | `dropDuplicates()` |
    | Extra spaces, mixed case | `" espresso "`, `CROISSANT`, `latte` | `trim()`, `initcap()` |
    | Inconsistent labels | `card`, `Card`, `CASH`, `cash` | `lower()` |
    | Price stored as text with a dollar sign | `"$4.50"` | `regexp_replace()`, then cast to `double` |
    | Quantity that is not a number, or missing | `two`, empty (Spark already reads an empty field as `NULL`) | `try_cast("int")` turns `two` into `NULL` too |
    | Two date formats, one garbage date | `2026-10-01`, `10/02/2026`, `not a date` | `try_to_date()` with each format |
    | Missing payment method | empty | `fillna()` |
    | Impossible value | quantity `-1` | `filter()` |

    We fix them one step at a time. Each step is a new DataFrame, so you can `show()` any
    step to check it.
    """)
    return


@app.cell
def _(raw_with_schema):
    # Step 1: remove exact duplicate rows
    step1 = raw_with_schema.dropDuplicates()
    print("before:", raw_with_schema.count(), " after:", step1.count())
    return (step1,)


@app.cell
def _(F, step1):
    # Step 2: tidy the text columns
    step2 = (
        step1
        .withColumn("product", F.initcap(F.trim(F.col("product"))))
        .withColumn("payment", F.lower(F.trim(F.col("payment"))))
    )
    step2.select("sale_id", "product", "payment").orderBy("sale_id").show()
    return (step2,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Step 3: turn text into numbers.**

    Spark 4 runs in **ANSI mode** by default: a plain `cast("int")` of the text `two`
    **fails the whole job** with a `CAST_INVALID_INPUT` error. That is usually what you
    want in production, but while cleaning we prefer `try_cast()`, which turns a bad value
    into `NULL` so we can find and handle it.

    For the price, we first remove the dollar sign with `regexp_replace()`.
    """)
    return


@app.cell
def _(F, step2):
    step3 = (
        step2
        .withColumn("quantity", F.col("quantity").try_cast("int"))
        .withColumn("unit_price", F.regexp_replace("unit_price", r"\$", "").try_cast("double"))
    )
    step3.select("sale_id", "quantity", "unit_price").orderBy("sale_id").show()
    step3.printSchema()
    return (step3,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Step 4: parse the dates.** `try_to_date(column, format)` returns `NULL` when the text
    does not match the format. We try both formats and keep the first one that works with
    `coalesce()` (it returns the first non-`NULL` value).
    """)
    return


@app.cell
def _(F, step3):
    step4 = step3.withColumn(
        "sale_date",
        F.coalesce(
            F.try_to_date("sale_date", "yyyy-MM-dd"),
            F.try_to_date("sale_date", "MM/dd/yyyy"),
        ),
    )
    step4.select("sale_id", "sale_date").orderBy("sale_id").show()
    return (step4,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Step 5: find the missing values.** `isNull()` / `isNotNull()` test for `NULL`.
    (Never write `== None`: in SQL, comparing with `NULL` is neither true nor false.)
    """)
    return


@app.cell
def _(F, step4):
    step4.filter(
        F.col("quantity").isNull() | F.col("sale_date").isNull() | F.col("payment").isNull()
    ).orderBy("sale_id").show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Step 6: decide what to do with each missing value.** This is a *business* decision,
    not a technical one:

    * A sale without a **payment method** is still a sale → fill in `"unknown"`
      with `fillna()` (also written `df.na.fill()`).
    * A sale without a **quantity** or a **date** is useless for reporting → drop the row
      with `dropna(subset=[...])` (also written `df.na.drop()`).

    **Step 7: remove impossible values.** A quantity of `-1` is a data-entry error.
    """)
    return


@app.cell
def _(F, step4):
    step6 = step4.fillna({"payment": "unknown"}).dropna(subset=["quantity", "sale_date"])
    clean_sales = step6.filter(F.col("quantity") > 0)
    print("after step 6:", step6.count(), " after step 7:", clean_sales.count())
    return (clean_sales,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **The result:** 14 messy rows became 9 clean, correctly typed rows. Now we can safely
    add a `revenue` column.
    """)
    return


@app.cell
def _(F, clean_sales):
    sales_clean = clean_sales.withColumn(
        "revenue", F.round(F.col("quantity") * F.col("unit_price"), 2)
    ).orderBy("sale_id")
    sales_clean.show()
    sales_clean.printSchema()
    return (sales_clean,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    In a real job you would write this as **one chain**, and save the result
    (e.g. `sales_clean.write.parquet("clean_sales/")`) so the next program does not have
    to clean the data again:

    ```python
    sales_clean = (
        spark.read.csv(path, header=True, schema=raw_schema)
        .dropDuplicates()
        .withColumn("product", F.initcap(F.trim("product")))
        .withColumn("payment", F.lower(F.trim("payment")))
        .withColumn("quantity", F.col("quantity").try_cast("int"))
        .withColumn("unit_price", F.regexp_replace("unit_price", r"\$", "\").try_cast("double"))
        .withColumn("sale_date", F.coalesce(F.try_to_date("sale_date", "yyyy-MM-dd"),
                                            F.try_to_date("sale_date", "MM/dd/yyyy")))
        .fillna({"payment": "unknown"})
        .dropna(subset=["quantity", "sale_date"])
        .filter(F.col("quantity") > 0)
        .withColumn("revenue", F.round(F.col("quantity") * F.col("unit_price"), 2))
    )
    ```
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 11. Try it: an interactive filter

    Pick a payment method and a minimum revenue. The cell below re-runs automatically and
    shows the matching clean sales. (This is marimo's reactivity: the filter cell depends
    on the two widgets.)
    """)
    return


@app.cell
def _(mo):
    payment_choice = mo.ui.dropdown(
        options=["all", "card", "cash", "unknown"], value="all", label="Payment"
    )
    min_revenue = mo.ui.slider(start=0, stop=12, step=1, value=0, label="Minimum revenue")
    mo.hstack([payment_choice, min_revenue], justify="start")
    return min_revenue, payment_choice


@app.cell
def _(F, min_revenue, payment_choice, sales_clean):
    chosen = sales_clean.filter(F.col("revenue") >= min_revenue.value)
    if payment_choice.value != "all":
        chosen = chosen.filter(F.col("payment") == payment_choice.value)
    print(f"{chosen.count()} matching sales")
    chosen.orderBy("sale_id").show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 12. Cheat sheet: from RDD to DataFrame

    | Task | RDD | DataFrame |
    |---|---|---|
    | Create | `sc.parallelize(rows)` | `spark.createDataFrame(rows, ["a", "b"])` |
    | Read a CSV file | `sc.textFile(p).map(lambda l: l.split(","))` | `spark.read.csv(p, header=True, inferSchema=True)` |
    | Look at data | `rdd.take(5)` | `df.show(5)`, `df.printSchema()` |
    | Transform each row | `rdd.map(lambda t: (t[0], t[1] * 2))` | `df.select("a", (F.col("b") * 2).alias("b2"))` |
    | Add a field | `rdd.map(lambda t: t + (t[1] * 2,))` | `df.withColumn("b2", F.col("b") * 2)` |
    | Keep some rows | `rdd.filter(lambda t: t[1] > 3)` | `df.filter(F.col("b") > 3)` |
    | Sort | `rdd.sortBy(lambda t: -t[1])` | `df.orderBy(F.desc("b"))` |
    | Remove duplicates | `rdd.distinct()` | `df.distinct()` / `df.dropDuplicates(["a"])` |
    | Count | `rdd.count()` | `df.count()` |
    | Convert | `rdd.toDF(["a", "b"])` | `df.rdd` |

    **Next notebook:** `02_dataframe_aggregations.py`: `groupBy()`, `agg()`, and how they
    replace `reduceByKey()`.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 13. Exercises

    Use the `sales` DataFrame from Section 3 (and `stores` from Section 4d). Try each one
    in a new cell first, then open the solution.

    1. Show `product` and `unit_price` of every **Tea** sale, most expensive first.
    2. Add a column `total` (quantity × unit_price) and keep only the sales with
       `total` greater than 9. Show `sale_id`, `product`, `total`, ordered by `sale_id`.
    3. How many **different products** appear in `sales`?
    4. From `stores`, show the stores in **San Jose** that opened in **2020 or later**,
       with columns `name` and `opened` only.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    import contextlib
    import io
    import textwrap

    def solution(code, namespace):
        """Run the solution code, capture what it prints, and show both in a fold-out."""
        code = textwrap.dedent(code).strip()
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            exec(code, dict(namespace))
        return mo.accordion({
            "Show solution": mo.vstack([
                mo.md(f"```python\n{code}\n```"),
                mo.plain_text(printed.getvalue()),
            ])
        })

    return (solution,)


@app.cell(hide_code=True)
def _(F, sales, solution):
    solution('''
        # Exercise 1
        (sales.filter(F.col("category") == "Tea")
              .select("product", "unit_price")
              .orderBy(F.desc("unit_price"))
              .show())
    ''', {"F": F, "sales": sales})
    return


@app.cell(hide_code=True)
def _(F, sales, solution):
    solution('''
        # Exercise 2
        (sales.withColumn("total", F.col("quantity") * F.col("unit_price"))
              .filter(F.col("total") > 9)
              .select("sale_id", "product", "total")
              .orderBy("sale_id")
              .show())
    ''', {"F": F, "sales": sales})
    return


@app.cell(hide_code=True)
def _(F, sales, solution):
    solution('''
        # Exercise 3
        print(sales.select("product").distinct().count())
    ''', {"F": F, "sales": sales})
    return


@app.cell(hide_code=True)
def _(F, solution, stores):
    solution('''
        # Exercise 4
        (stores.filter((F.col("city") == "San Jose") & (F.col("opened") >= 2020))
               .select("name", "opened")
               .show())
    ''', {"F": F, "stores": stores})
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Stop Spark

    Uncomment the line below when you are done, to release the local Spark resources.
    """)
    return


@app.cell
def _():
    # spark.stop()
    return


if __name__ == "__main__":
    app.run()
