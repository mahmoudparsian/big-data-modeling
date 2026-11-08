import marimo

__generated_with = "0.23.10"
app = marimo.App(width="medium", app_title="DataFrames 2 — Aggregations")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # DataFrames, Part 2: Aggregations

    **Notebook 2 of 5** · before: `01_dataframe_basics.py` · next: `03_dataframe_joins.py`

    With RDDs you summarized data with **reducers**: `reduceByKey()`, `groupByKey()`,
    `aggregateByKey()`, `combineByKey()`. You first had to build `(key, value)` pairs,
    and for something as simple as an *average* you had to carry `(sum, count)` pairs
    around yourself.

    With DataFrames, one pattern covers almost everything:

    ```python
    df.groupBy("key column(s)").agg(F.sum("x"), F.avg("y"), F.count("*"), ...)
    ```

    **Our story:** the *Bean & Leaf* coffee chain (from Notebook 1) wants a sales report
    for October 1–3, 2026: revenue per store, per city, per category, tips, payment mix,
    and the best-selling products.

    **What you will learn**

    1. Aggregating a whole table: `agg()` with `sum`, `avg`, `min`, `max`, `count`
    2. `groupBy()` on one or more columns, and how it replaces `reduceByKey()`
    3. Filtering before grouping (SQL `WHERE`) vs. after grouping (SQL `HAVING`)
    4. How `NULL` values behave in aggregates
    5. Conditional aggregation, collecting values into lists, and more functions
    6. Pivot tables, subtotals (`rollup`), and a first look at window functions
    7. The same queries in Spark SQL
    """)
    return


@app.cell
def _():
    import marimo as mo
    from operator import add
    from pyspark.sql import SparkSession, Window
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder
        .appName("dataframes-02-aggregations")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")  # small data: few partitions
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print("Spark version:", spark.version)
    return F, Window, add, mo, spark


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1. The data

    18 clean sales from four stores over three days. Each row already carries the store's
    `city` and the product's `category` (in Notebook 3 you will see how a **join** builds
    such a table from separate tables). `tip` is `NULL` when the customer did not leave a
    tip.
    """)
    return


@app.cell
def _(F, spark):
    sales_rows = [
        (1,  "2026-10-01", "S01", "San Jose",      "Latte",      "Coffee", 2, 4.50, "card", 1.00),
        (2,  "2026-10-01", "S01", "San Jose",      "Croissant",  "Bakery", 1, 3.25, "cash", None),
        (3,  "2026-10-01", "S02", "Santa Clara",   "Espresso",   "Coffee", 3, 3.00, "card", 0.50),
        (4,  "2026-10-01", "S03", "San Francisco", "Cold Brew",  "Coffee", 2, 4.00, "card", 1.50),
        (5,  "2026-10-01", "S04", "San Jose",      "Green Tea",  "Tea",    1, 3.50, "cash", None),
        (6,  "2026-10-02", "S01", "San Jose",      "Latte",      "Coffee", 3, 4.50, "card", 2.00),
        (7,  "2026-10-02", "S02", "Santa Clara",   "Muffin",     "Bakery", 4, 2.75, "card", None),
        (8,  "2026-10-02", "S02", "Santa Clara",   "Latte",      "Coffee", 1, 4.50, "cash", None),
        (9,  "2026-10-02", "S03", "San Francisco", "Chai Latte", "Tea",    3, 4.25, "card", 1.00),
        (10, "2026-10-02", "S03", "San Francisco", "Croissant",  "Bakery", 2, 3.25, "card", 0.50),
        (11, "2026-10-02", "S04", "San Jose",      "Espresso",   "Coffee", 2, 3.00, "card", None),
        (12, "2026-10-03", "S01", "San Jose",      "Cold Brew",  "Coffee", 1, 4.00, "cash", None),
        (13, "2026-10-03", "S01", "San Jose",      "Green Tea",  "Tea",    2, 3.50, "card", 1.00),
        (14, "2026-10-03", "S02", "Santa Clara",   "Latte",      "Coffee", 5, 4.50, "card", 3.00),
        (15, "2026-10-03", "S03", "San Francisco", "Latte",      "Coffee", 2, 4.50, "card", 1.00),
        (16, "2026-10-03", "S03", "San Francisco", "Muffin",     "Bakery", 3, 2.75, "cash", None),
        (17, "2026-10-03", "S04", "San Jose",      "Chai Latte", "Tea",    1, 4.25, "card", 0.50),
        (18, "2026-10-03", "S04", "San Jose",      "Croissant",  "Bakery", 2, 3.25, "card", None),
    ]
    sales_schema = (
        "sale_id INT, sale_date STRING, store_id STRING, city STRING, product STRING, "
        "category STRING, quantity INT, unit_price DOUBLE, payment STRING, tip DOUBLE"
    )
    sales = (
        spark.createDataFrame(sales_rows, sales_schema)
        .withColumn("sale_date", F.to_date("sale_date"))
        .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
    )
    sales.show(truncate=False)
    sales.printSchema()
    return sales, sales_rows, sales_schema


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2. Aggregating the whole table: `agg()`

    `df.agg(...)` collapses the **whole DataFrame into one row**. Each argument is an
    aggregate function from `F`. Always give the result a readable name with `.alias()`.

    | Function | Meaning |
    |---|---|
    | `F.count("*")` | number of rows |
    | `F.count("col")` | number of rows where `col` is **not** `NULL` |
    | `F.countDistinct("col")` | number of different non-`NULL` values |
    | `F.sum`, `F.avg` (= `F.mean`), `F.min`, `F.max` | the usual |
    | `F.round(expr, 2)` | round the result to 2 decimals |
    """)
    return


@app.cell
def _(F, sales):
    totals = sales.agg(
        F.count("*").alias("num_sales"),
        F.sum("quantity").alias("items_sold"),
        F.sum("revenue").alias("total_revenue"),
        F.round(F.avg("revenue"), 2).alias("avg_sale"),
        F.min("revenue").alias("smallest_sale"),
        F.max("revenue").alias("largest_sale"),
        F.countDistinct("product").alias("num_products"),
    )
    totals.show()
    return (totals,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The result is still a **DataFrame** (with one row). To use a value in Python, take
    the row out with `first()` and read a field:
    """)
    return


@app.cell
def _(totals):
    total_revenue = totals.first()["total_revenue"]
    print("Total revenue for Oct 1-3:", total_revenue)
    return (total_revenue,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 3. `groupBy()`: one result row per group

    **Question:** *What is the total revenue of each store?*

    With RDDs you would build `(store_id, revenue)` pairs and call `reduceByKey()`.
    With DataFrames you say which column is the key (`groupBy`) and what to compute
    (`agg`). Spark does the shuffle and the combining for you, and, just like
    `reduceByKey()`, it adds up partial results on each partition before the shuffle.
    """)
    return


@app.cell
def _(F, add, sales):
    # RDD way (df.rdd gives Row objects)
    store_revenue_rdd = (
        sales.rdd
        .map(lambda r: (r.store_id, r.revenue))
        .reduceByKey(add)
        .sortByKey()
    )
    print(store_revenue_rdd.collect())

    # DataFrame way
    store_revenue = (
        sales.groupBy("store_id")
        .agg(F.sum("revenue").alias("revenue"))
        .orderBy("store_id")
    )
    store_revenue.show()
    return store_revenue, store_revenue_rdd


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Important:** the groups come out in **no particular order** (they are spread over
    partitions). Always end with `orderBy()` if the order matters.

    ### Where DataFrames really shine: the average

    **Question:** *What is the average sale amount in each store?* With RDDs, `reduceByKey`
    cannot average directly (an average of averages is wrong), so you carried
    `(sum, count)` pairs, e.g. with `aggregateByKey()`. With DataFrames it is just
    `F.avg()`.
    """)
    return


@app.cell
def _(F, sales):
    # RDD way: carry (sum, count), then divide
    avg_rdd = (
        sales.rdd
        .map(lambda r: (r.store_id, r.revenue))
        .aggregateByKey(
            (0.0, 0),                                     # zero value: (sum, count)
            lambda acc, v: (acc[0] + v, acc[1] + 1),      # add one value in a partition
            lambda a, b: (a[0] + b[0], a[1] + b[1]),      # merge two partitions
        )
        .mapValues(lambda p: round(p[0] / p[1], 2))
        .sortByKey()
    )
    print(avg_rdd.collect())

    # DataFrame way
    sales.groupBy("store_id").agg(F.round(F.avg("revenue"), 2).alias("avg_sale")).orderBy("store_id").show()
    return (avg_rdd,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Several aggregates at once

    One `agg()` can compute as many results as you like, in a single pass over the data.

    There are also shortcuts like `groupBy(...).count()` or `groupBy(...).sum("revenue")`,
    but they give columns names like `sum(revenue)` and allow no rounding, so `agg()` with
    `alias()` is the better habit.
    """)
    return


@app.cell
def _(F, sales):
    store_report = (
        sales.groupBy("store_id")
        .agg(
            F.count("*").alias("num_sales"),
            F.sum("quantity").alias("items"),
            F.sum("revenue").alias("revenue"),
            F.round(F.avg("revenue"), 2).alias("avg_sale"),
            F.max("revenue").alias("biggest_sale"),
        )
        .orderBy(F.desc("revenue"), "store_id")
    )
    store_report.show()

    # The shortcut, for comparison
    sales.groupBy("store_id").count().orderBy("store_id").show()
    return (store_report,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 4. Grouping by several columns

    Pass several columns to `groupBy()`: you get one row per **combination** of values.
    (With RDDs, the key would be a tuple: `((city, category), revenue)`.)
    """)
    return


@app.cell
def _(F, sales):
    city_category = (
        sales.groupBy("city", "category")
        .agg(
            F.sum("quantity").alias("items"),
            F.sum("revenue").alias("revenue"),
        )
        .orderBy("city", "category")
    )
    city_category.show()
    return (city_category,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 5. Filter before or after grouping?

    * **Before** grouping (`filter` → `groupBy`): choose which **rows** take part.
      In SQL this is `WHERE`.
    * **After** grouping (`groupBy` → `agg` → `filter`): choose which **groups** to keep,
      using the aggregated columns. In SQL this is `HAVING`.

    **Question A:** revenue per store from **card** payments only (filter rows first).
    **Question B:** which stores made **more than 30** in total (filter groups after).
    """)
    return


@app.cell
def _(F, sales):
    # A: WHERE -- only card sales are summed
    (sales.filter(F.col("payment") == "card")
          .groupBy("store_id")
          .agg(F.sum("revenue").alias("card_revenue"))
          .orderBy("store_id")
          .show())

    # B: HAVING -- sum everything, then keep the big groups
    (sales.groupBy("store_id")
          .agg(F.sum("revenue").alias("revenue"))
          .filter(F.col("revenue") > 30)
          .orderBy("store_id")
          .show())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 6. `NULL` values in aggregates

    Aggregate functions **skip `NULL`s** (except `count("*")`, which counts rows). That is
    often right, but you must know which question you are answering. Our `tip` column is
    `NULL` when there was no tip:

    * `F.count("tip")` counts only sales **with** a tip.
    * `F.avg("tip")` is the average tip **among customers who tipped**.
    * To get the average tip **per sale**, treat "no tip" as `0` first with
      `F.coalesce(F.col("tip"), F.lit(0))`.
    """)
    return


@app.cell
def _(F, sales):
    (sales.groupBy("store_id")
          .agg(
              F.count("*").alias("sales"),
              F.count("tip").alias("sales_with_tip"),
              F.sum("tip").alias("total_tips"),
              F.round(F.avg("tip"), 2).alias("avg_tip_if_tipped"),
              F.round(F.avg(F.coalesce(F.col("tip"), F.lit(0))), 2).alias("avg_tip_per_sale"),
          )
          .orderBy("store_id")
          .show())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 7. Conditional aggregation

    Sometimes you want several filtered totals **side by side** in one row per group, e.g.
    card revenue *and* cash revenue per store. Put a `when()` **inside** the aggregate:
    rows that do not match become `NULL` and are skipped.

    `F.count_if(condition)` counts the rows where the condition is true.
    """)
    return


@app.cell
def _(F, sales):
    payment_mix = (
        sales.groupBy("store_id")
        .agg(
            F.sum(F.when(F.col("payment") == "card", F.col("revenue"))).alias("card_revenue"),
            F.sum(F.when(F.col("payment") == "cash", F.col("revenue"))).alias("cash_revenue"),
            F.count_if(F.col("payment") == "cash").alias("cash_sales"),
            F.round(F.avg((F.col("payment") == "card").cast("int")) * 100, 1).alias("pct_card"),
        )
        .orderBy("store_id")
    )
    payment_mix.show()
    return (payment_mix,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The last column uses a small trick: `(payment == "card").cast("int")` is `1` for card
    and `0` for cash, so its **average** is the share of card sales.

    ## 8. Collecting values into a list: the `groupByKey()` of DataFrames

    `F.collect_list("col")` gathers all values of a group into an array (with duplicates);
    `F.collect_set("col")` keeps each value once. The order inside the array is not
    guaranteed, so sort it with `F.array_sort()` when you want a stable result.
    `F.size()` gives the length of an array.

    Use these on small groups only: like `groupByKey()`, they move every value to one place.
    """)
    return


@app.cell
def _(F, sales):
    (sales.groupBy("store_id")
          .agg(
              F.array_sort(F.collect_set("product")).alias("products"),
              F.size(F.collect_set("product")).alias("num_products"),
          )
          .orderBy("store_id")
          .show(truncate=False))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 9. More aggregate functions

    | Function | Meaning |
    |---|---|
    | `F.median("col")` | the middle value |
    | `F.stddev("col")` | standard deviation |
    | `F.approx_count_distinct("col")` | fast, approximate distinct count (for big data) |
    | `F.first("col")`, `F.any_value("col")` | some value of the group (not a "first" in any order!) |
    | `F.max_by("x", "y")` | the value of `x` in the row where `y` is largest |
    | `F.min_by("x", "y")` | the value of `x` in the row where `y` is smallest |

    **Question:** for each category, what is the median sale, and which single sale
    (`sale_id`) was the biggest?
    """)
    return


@app.cell
def _(F, sales):
    (sales.groupBy("category")
          .agg(
              F.count("*").alias("sales"),
              F.median("revenue").alias("median_sale"),
              F.round(F.stddev("revenue"), 2).alias("stddev_sale"),
              F.max("revenue").alias("biggest_sale"),
              F.max_by("sale_id", "revenue").alias("biggest_sale_id"),
          )
          .orderBy("category")
          .show())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    (Watch out: if two rows tie for the largest value, `max_by()` picks either one. In our
    data each category's biggest sale is unique.)

    ## 10. Pivot tables

    `pivot("col")` turns the **values** of a column into **new columns**, like a
    spreadsheet pivot table. **Question:** revenue per store (rows) by category (columns).

    Listing the values yourself, `pivot("category", ["Coffee", "Tea", "Bakery"])`, fixes the
    column order and saves Spark an extra job to find the distinct values. A store with no
    sales in a category gets `NULL`; `fillna(0)` turns that into `0`.
    """)
    return


@app.cell
def _(F, sales):
    pivoted = (
        sales.groupBy("store_id")
        .pivot("category", ["Coffee", "Tea", "Bakery"])
        .agg(F.sum("revenue"))
        .orderBy("store_id")
    )
    pivoted.show()
    pivoted.fillna(0).show()
    return (pivoted,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 11. Subtotals and grand totals: `rollup()`

    A management report often needs revenue **per store, per city, and overall**.
    `rollup("city", "store_id")` computes all three levels at once:

    * `(city, store_id)`: one row per store
    * `(city, NULL)`: a subtotal for each city
    * `(NULL, NULL)`: the grand total

    The `NULL`s here mean "all". `F.grouping("col")` is `1` on such rows, which helps to
    label them. (`cube()` is similar, but gives subtotals for **every** combination of the
    columns.)
    """)
    return


@app.cell
def _(F, sales):
    rolled = (
        sales.rollup("city", "store_id")
        .agg(
            F.sum("revenue").alias("revenue"),
            F.grouping("store_id").alias("is_subtotal"),
        )
        .orderBy(F.col("city").asc_nulls_last(), F.col("store_id").asc_nulls_last())
    )
    rolled.show()
    return (rolled,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 12. A first look at window functions: aggregate *without* collapsing rows

    `groupBy()` gives **one row per group**. Sometimes you want to keep **every row** and
    add a group result next to it, e.g. *"what share of its store's revenue is each sale?"*
    or *"rank the products inside each city"*. That is a **window function**: an aggregate
    computed over a *window* of related rows.

    ```python
    w = Window.partitionBy("store_id")              # the "group" for each row
    df.withColumn("store_total", F.sum("revenue").over(w))
    ```
    """)
    return


@app.cell
def _(F, Window, sales):
    by_store = Window.partitionBy("store_id")
    (sales.select("sale_id", "store_id", "product", "revenue")
          .withColumn("store_total", F.sum("revenue").over(by_store))
          .withColumn("pct_of_store", F.round(F.col("revenue") / F.col("store_total") * 100, 1))
          .filter(F.col("store_id") == "S01")
          .orderBy("sale_id")
          .show())
    return (by_store,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Top product per city:** first aggregate revenue per `(city, product)` with
    `groupBy`, then rank the products inside each city with a window that is also
    **ordered**. `F.dense_rank()` gives `1` to the best product (ties get the same rank).
    """)
    return


@app.cell
def _(F, Window, sales):
    product_by_city = sales.groupBy("city", "product").agg(F.sum("revenue").alias("revenue"))
    rank_in_city = Window.partitionBy("city").orderBy(F.desc("revenue"))
    top_products = (
        product_by_city
        .withColumn("rank", F.dense_rank().over(rank_in_city))
        .filter(F.col("rank") == 1)
        .orderBy("city", "product")
    )
    top_products.show()
    return product_by_city, rank_in_city, top_products


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 13. The same thing in Spark SQL

    Register a DataFrame as a temporary **view**, and you can query it with plain SQL.
    `spark.sql()` returns a DataFrame, and it runs on the same engine, so speed is the
    same. Use whichever reads better to you.
    """)
    return


@app.cell
def _(sales, spark):
    sales.createOrReplaceTempView("sales")
    spark.sql("""
        SELECT   store_id,
                 COUNT(*)              AS num_sales,
                 SUM(revenue)          AS revenue,
                 ROUND(AVG(revenue), 2) AS avg_sale
        FROM     sales
        WHERE    payment = 'card'
        GROUP BY store_id
        HAVING   SUM(revenue) > 20
        ORDER BY revenue DESC, store_id
    """).show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 14. Try it: build your own report

    Choose what to group by and what to measure. The table below re-computes itself.
    """)
    return


@app.cell
def _(mo):
    group_choice = mo.ui.dropdown(
        options=["store_id", "city", "category", "product", "payment", "sale_date"],
        value="store_id",
        label="Group by",
    )
    metric_choice = mo.ui.dropdown(
        options=["total revenue", "number of sales", "items sold", "average sale", "total tips"],
        value="total revenue",
        label="Measure",
    )
    mo.hstack([group_choice, metric_choice], justify="start")
    return group_choice, metric_choice


@app.cell
def _(F, group_choice, metric_choice, sales):
    metrics = {
        "total revenue":   F.sum("revenue"),
        "number of sales": F.count("*"),
        "items sold":      F.sum("quantity"),
        "average sale":    F.round(F.avg("revenue"), 2),
        "total tips":      F.sum("tip"),
    }
    report = (
        sales.groupBy(group_choice.value)
        .agg(metrics[metric_choice.value].alias("value"))
        .orderBy(F.desc("value"), group_choice.value)
    )
    report.show()
    return metrics, report


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 15. Cheat sheet: RDD reducers → DataFrame aggregations

    | Task | RDD | DataFrame |
    |---|---|---|
    | Sum per key | `pairs.reduceByKey(add)` | `df.groupBy("k").agg(F.sum("v"))` |
    | Count per key | `pairs.mapValues(lambda v: 1).reduceByKey(add)` or `countByKey()` | `df.groupBy("k").count()` |
    | Average per key | `aggregateByKey((0, 0), ...)` then divide | `df.groupBy("k").agg(F.avg("v"))` |
    | Max per key | `pairs.reduceByKey(max)` | `df.groupBy("k").agg(F.max("v"))` |
    | All values per key | `pairs.groupByKey().mapValues(list)` | `df.groupBy("k").agg(F.collect_list("v"))` |
    | Distinct values per key | `groupByKey().mapValues(set)` | `F.collect_set("v")` / `F.countDistinct("v")` |
    | Composite key | key is a tuple `((a, b), v)` | `df.groupBy("a", "b")` |
    | Whole-data total | `rdd.map(...).sum()` | `df.agg(F.sum("v"))` |
    | Keep only big groups | `.filter(lambda kv: kv[1] > 30)` after reducing | `.filter(F.col("total") > 30)` after `agg` |

    **Next notebook:** `03_dataframe_joins.py`, combining several tables.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 16. Exercises

    Use the `sales` DataFrame. Try each one in a new cell first, then open the solution.

    1. For each **product**, show the number of items sold and the total revenue, best
       seller (by revenue) first.
    2. For each **day** (`sale_date`), show the revenue and the number of **different
       stores** that made a sale.
    3. Which **products** were sold in **at least 3 different stores**?
    4. For each **city**, show the total tips and the share of sales that had a tip
       (in percent, 1 decimal).
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
        (sales.groupBy("product")
              .agg(F.sum("quantity").alias("items"), F.sum("revenue").alias("revenue"))
              .orderBy(F.desc("revenue"), "product")
              .show())
    ''', {"F": F, "sales": sales})
    return


@app.cell(hide_code=True)
def _(F, sales, solution):
    solution('''
        # Exercise 2
        (sales.groupBy("sale_date")
              .agg(F.sum("revenue").alias("revenue"),
                   F.countDistinct("store_id").alias("stores"))
              .orderBy("sale_date")
              .show())
    ''', {"F": F, "sales": sales})
    return


@app.cell(hide_code=True)
def _(F, sales, solution):
    solution('''
        # Exercise 3 -- a HAVING-style filter after the aggregation
        (sales.groupBy("product")
              .agg(F.countDistinct("store_id").alias("stores"))
              .filter(F.col("stores") >= 3)
              .orderBy("product")
              .show())
    ''', {"F": F, "sales": sales})
    return


@app.cell(hide_code=True)
def _(F, sales, solution):
    solution('''
        # Exercise 4 -- count("tip") skips NULLs, count("*") does not
        (sales.groupBy("city")
              .agg(F.sum("tip").alias("total_tips"),
                   F.round(F.count("tip") / F.count("*") * 100, 1).alias("pct_tipped"))
              .orderBy("city")
              .show())
    ''', {"F": F, "sales": sales})
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
