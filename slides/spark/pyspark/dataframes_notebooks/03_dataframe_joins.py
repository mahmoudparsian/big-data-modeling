import marimo

__generated_with = "0.23.10"
app = marimo.App(width="medium", app_title="DataFrames 3 — Joins")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # DataFrames, Part 3: Joins

    **Notebook 3 of 5** · before: `01_dataframe_basics.py`, `02_dataframe_aggregations.py` · next: `04_dataframes_with_claude.py`, `05_dataframe_advanced.py`

    Real data lives in **several tables**. A sale records only a `store_id` and a
    `product_id`; the store's city and the product's price live in other tables. A
    **join** brings matching rows together.

    With RDDs you joined **pair RDDs**: you first reshaped each RDD into `(key, value)`
    pairs, then `a.join(b)` gave you `(key, (value_a, value_b))`, and you unpacked the
    nested tuples by hand. With DataFrames you name the key column, and the result is a
    flat table:

    ```python
    sales.join(stores, on="store_id", how="inner")
    ```

    **Our story:** *Bean & Leaf* (the coffee chain from Notebooks 1 and 2) keeps its data
    in three tables: `stores`, `products`, and `sales`. We want revenue by city and
    category, and we want to find data problems: sales for unknown stores or products,
    stores with no sales, products nobody buys.

    **What you will learn**

    1. Inner joins: RDD vs. DataFrame
    2. The three ways to write the join condition, and the *ambiguous column* problem
    3. All join types: `inner`, `left`, `right`, `full`, `left_semi`, `left_anti`, `cross`
    4. Joining three tables, then aggregating
    5. Joins on several columns, on a range (non-equi joins), and self-joins
    6. Traps: duplicate keys, `NULL` keys
    7. Broadcast joins, and joins in Spark SQL
    """)
    return


@app.cell
def _():
    import marimo as mo
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder
        .appName("dataframes-03-joins")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")  # small data: few partitions
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print("Spark version:", spark.version)
    return F, mo, spark


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1. The three tables

    Look for the deliberate problems; they make the join types interesting:

    * Store **S05** (Airport) opened recently and has **no sales** yet.
    * Product **P08** (Matcha) is on the menu but **nobody bought it**.
    * Sale **8** comes from store **S09**, a pop-up kiosk that is **not** in `stores`.
    * Sale **9** is for product **P99**, a seasonal drink that is **not** in `products`.
    """)
    return


@app.cell
def _(spark):
    stores = spark.createDataFrame(
        [
            ("S01", "Downtown", "San Jose"),
            ("S02", "Campus",   "Santa Clara"),
            ("S03", "Harbor",   "San Francisco"),
            ("S04", "Mall",     "San Jose"),
            ("S05", "Airport",  "Oakland"),
        ],
        "store_id STRING, store_name STRING, city STRING",
    )
    products = spark.createDataFrame(
        [
            ("P01", "Latte",      "Coffee", 4.50),
            ("P02", "Espresso",   "Coffee", 3.00),
            ("P03", "Cold Brew",  "Coffee", 4.00),
            ("P04", "Green Tea",  "Tea",    3.50),
            ("P05", "Chai Latte", "Tea",    4.25),
            ("P06", "Croissant",  "Bakery", 3.25),
            ("P07", "Muffin",     "Bakery", 2.75),
            ("P08", "Matcha",     "Tea",    5.00),
        ],
        "product_id STRING, product STRING, category STRING, price DOUBLE",
    )
    sales = spark.createDataFrame(
        [
            (1,  "2026-10-01", "S01", "P01", 2),
            (2,  "2026-10-01", "S01", "P06", 1),
            (3,  "2026-10-01", "S02", "P02", 3),
            (4,  "2026-10-01", "S03", "P03", 2),
            (5,  "2026-10-02", "S04", "P04", 1),
            (6,  "2026-10-02", "S02", "P07", 4),
            (7,  "2026-10-02", "S03", "P05", 2),
            (8,  "2026-10-02", "S09", "P01", 1),   # unknown store
            (9,  "2026-10-03", "S01", "P99", 1),   # unknown product
            (10, "2026-10-03", "S02", "P01", 5),
            (11, "2026-10-03", "S04", "P06", 2),
            (12, "2026-10-03", "S03", "P01", 2),
        ],
        "sale_id INT, sale_date STRING, store_id STRING, product_id STRING, quantity INT",
    )
    stores.show()
    products.show()
    sales.show()
    return products, sales, stores


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2. Your first join: RDD vs. DataFrame

    **Question:** *In which city did each sale happen?* We need `sales` and `stores`,
    matched on `store_id`.

    **The RDD way:** turn both into `(store_id, ...)` pairs, join, then unpack
    `(key, (left, right))`.
    """)
    return


@app.cell
def _(sales, stores):
    sales_pairs = sales.rdd.map(lambda r: (r.store_id, (r.sale_id, r.quantity)))
    store_pairs = stores.rdd.map(lambda r: (r.store_id, r.city))

    joined_rdd = sales_pairs.join(store_pairs)       # (store_id, ((sale_id, qty), city))
    print(sorted(joined_rdd.collect())[:3])

    flat = joined_rdd.map(lambda kv: (kv[1][0][0], kv[0], kv[1][1], kv[1][0][1]))
    print(sorted(flat.collect())[:3])                # (sale_id, store_id, city, qty)
    return flat, joined_rdd, sales_pairs, store_pairs


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **The DataFrame way:** one line, and the result keeps all column names. Joining on
    the column **name** (`on="store_id"`) keeps a single `store_id` column.
    """)
    return


@app.cell
def _(sales, stores):
    sales_with_city = sales.join(stores, on="store_id", how="inner")
    sales_with_city.orderBy("sale_id").show()
    print("sales:", sales.count(), " after inner join:", sales_with_city.count())
    return (sales_with_city,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Where did sale 8 go?** Its store `S09` has no match in `stores`, so an **inner**
    join drops it, silently. And `S05` is missing too, because it has no sales. Keep this
    in mind: an inner join keeps **only rows that match on both sides**. Section 4 shows
    the joins that keep unmatched rows.

    ## 3. Three ways to write the join condition

    | Style | Example | Key columns in the result |
    |---|---|---|
    | One column name | `on="store_id"` | one `store_id` |
    | List of names | `on=["store_id", "product_id"]` | one of each |
    | A condition (expression) | `on=sales.store_id == stores.store_id` | **both** `store_id` columns! |

    Use a **condition** when the key columns have different names, or when the match is
    not a simple equality (Section 5). With a condition, both key columns are kept, and
    `select("store_id")` would then fail with an `AMBIGUOUS_REFERENCE` error, because
    Spark does not know which one you mean. Two fixes:

    * refer to a specific side, e.g. `sales.store_id` or `stores["store_id"]`, and
      `drop()` the copy you do not need;
    * or give each DataFrame a short **alias** and write `F.col("s.store_id")`.
    """)
    return


@app.cell
def _(F, sales, stores):
    # A condition keeps both key columns ...
    by_condition = sales.join(stores, sales.store_id == stores.store_id, "inner")
    print(by_condition.columns)

    # ... fix 1: drop the copy that came from stores
    by_condition.drop(stores.store_id).select("sale_id", "store_id", "city").orderBy("sale_id").show(3)

    # ... fix 2: aliases
    (sales.alias("s")
          .join(stores.alias("t"), F.col("s.store_id") == F.col("t.store_id"))
          .select("s.sale_id", "s.store_id", "t.city")
          .orderBy("sale_id")
          .show(3))
    return (by_condition,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 4. Join types

    The `how=` argument decides what happens to rows **without** a match.
    Think of `sales` as the **left** table and `stores` as the **right** table:

    ```
         left (sales)        right (stores)
       ┌─────────────┬──────┬─────────────┐
       │ only left   │ both │ only right  │
       │ (sale 8,S09)│      │ (S05)       │
       └─────────────┴──────┴─────────────┘
       inner      = both
       left       = only left + both        (missing right columns -> NULL)
       right      = both + only right       (missing left columns  -> NULL)
       full       = everything
       left_semi  = left rows that HAVE a match   (left columns only)
       left_anti  = left rows with NO match       (left columns only)
    ```
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### `left` (left outer): keep every sale

    **Question:** list **all** sales with their city, and keep the sales from unknown
    stores, so we can investigate them. The store columns are `NULL` where there is no
    match. (RDD: `leftOuterJoin()`, where a missing right side was `None`.)
    """)
    return


@app.cell
def _(sales, stores):
    sales.join(stores, on="store_id", how="left").orderBy("sale_id").show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### `right` (right outer): keep every store

    The mirror image: every store appears, even S05 with no sales. In practice people
    usually swap the two tables and write a `left` join instead, which reads more
    naturally: `stores.join(sales, "store_id", "left")`.
    """)
    return


@app.cell
def _(F, sales, stores):
    (sales.join(stores, on="store_id", how="right")
          .select("store_id", "city", "sale_id", "quantity")
          .orderBy("store_id", F.col("sale_id").asc_nulls_last())
          .show())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### `full` (full outer): keep everything

    Every sale and every store, matched where possible. Handy for **reconciling** two
    data sources: rows with `NULL` on one side are exactly the mismatches. Because we
    joined on the column **name**, `store_id` is filled in from whichever side has it.
    """)
    return


@app.cell
def _(F, sales, stores):
    full_join = sales.join(stores, on="store_id", how="full")
    (full_join.select("store_id", "sale_id", "store_name", "city")
              .orderBy("store_id", F.col("sale_id").asc_nulls_last())
              .show())

    # Just the mismatches
    (full_join.filter(F.col("sale_id").isNull() | F.col("store_name").isNull())
              .select("store_id", "sale_id", "store_name")
              .orderBy("store_id")
              .show())
    return (full_join,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### `left_semi` and `left_anti`: filtering by another table

    These two do not add any columns. They only **filter** the left table:

    * `left_semi` keeps the left rows that **have** a match (SQL: `WHERE EXISTS` / `IN`).
      Each left row appears at most once, even if it matches many right rows.
    * `left_anti` keeps the left rows that have **no** match (SQL: `WHERE NOT EXISTS`).
      This is the classic **data-quality** check.

    **Questions:** Which stores made at least one sale? Which stores made none? Which
    sales refer to an unknown product? Which products were never sold?
    """)
    return


@app.cell
def _(products, sales, stores):
    print("Stores with at least one sale (left_semi):")
    stores.join(sales, on="store_id", how="left_semi").orderBy("store_id").show()

    print("Stores with no sales (left_anti):")
    stores.join(sales, on="store_id", how="left_anti").show()

    print("Sales with an unknown product (left_anti):")
    sales.join(products, on="product_id", how="left_anti").show()

    print("Products never sold (left_anti):")
    products.join(sales, on="product_id", how="left_anti").show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Compare: `stores.join(sales, "store_id", "inner")` would list store **S01 three
    times** (once per sale). `left_semi` lists it once and adds no sales columns.

    ### `cross`: every combination

    `crossJoin()` pairs **every** left row with **every** right row (a Cartesian product,
    like `rdd.cartesian()`). 5 stores × 8 products = 40 rows. It is rarely what you want
    on big tables, but useful to build a complete grid, e.g. *every store–product
    combination*, and then find which combinations never sold with a `left_anti` join.
    """)
    return


@app.cell
def _(products, sales, stores):
    grid = stores.select("store_id").crossJoin(products.select("product_id", "product"))
    print("grid rows:", grid.count())

    never_sold = grid.join(sales, on=["store_id", "product_id"], how="left_anti")
    print("store-product pairs with no sales:", never_sold.count())
    never_sold.filter("store_id = 'S01'").orderBy("product_id").show()
    return grid, never_sold


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Summary of row counts

    The same `sales` (12 rows) and `stores` (5 rows), every join type:
    """)
    return


@app.cell
def _(sales, spark, stores):
    join_counts = [
        (how, sales.join(stores, on="store_id", how=how).count())
        for how in ["inner", "left", "right", "full", "left_semi", "left_anti"]
    ]
    join_counts.append(("cross", sales.crossJoin(stores).count()))
    spark.createDataFrame(join_counts, "join_type STRING, rows LONG").show()
    return (join_counts,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 5. Joining three tables, then aggregating

    **Question:** *revenue by city and category.* Revenue needs the `price` (in
    `products`); the city is in `stores`. Chain two joins, compute the revenue, then
    `groupBy` (Notebook 2).
    """)
    return


@app.cell
def _(F, products, sales, stores):
    sales_full = (
        sales
        .join(stores, on="store_id", how="inner")
        .join(products, on="product_id", how="inner")
        .withColumn("revenue", F.col("quantity") * F.col("price"))
    )
    (sales_full.groupBy("city", "category")
               .agg(F.sum("quantity").alias("items"), F.sum("revenue").alias("revenue"))
               .orderBy("city", "category")
               .show())
    print("sales used in the report:", sales_full.count(), "of", sales.count())
    return (sales_full,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Careful:** the report silently left out 2 of the 12 sales (sale 8, unknown store;
    sale 9, unknown product). With **left** joins we keep them, and label the gaps with
    `coalesce()`. A sale with an unknown product has no price, so its revenue stays
    `NULL` (and `sum()` skips it); the report now shows that the data is incomplete
    instead of hiding it.
    """)
    return


@app.cell
def _(F, products, sales, stores):
    sales_all = (
        sales
        .join(stores, on="store_id", how="left")
        .join(products, on="product_id", how="left")
        .withColumn("city", F.coalesce("city", F.lit("(unknown store)")))
        .withColumn("category", F.coalesce("category", F.lit("(unknown product)")))
        .withColumn("revenue", F.col("quantity") * F.col("price"))
    )
    (sales_all.groupBy("city", "category")
              .agg(F.count("*").alias("sales"), F.sum("revenue").alias("revenue"))
              .orderBy("city", "category")
              .show())
    return (sales_all,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 6. Joining on several columns

    Some tables have a **composite key**. This week's `promotions` give a discount for a
    specific product **in a specific store**, so we must match on **both** `store_id` and
    `product_id`. (With RDDs the key would be a tuple: `((store_id, product_id), ...)`.)

    We use a `left` join because most sales have no promotion; `coalesce(..., 0)` turns
    "no promotion" into a 0 % discount.
    """)
    return


@app.cell
def _(F, sales_full, spark):
    promotions = spark.createDataFrame(
        [("S01", "P01", 10), ("S02", "P01", 20), ("S03", "P05", 15), ("S04", "P06", 50)],
        "store_id STRING, product_id STRING, discount_pct INT",
    )
    with_promo = (
        sales_full
        .join(promotions, on=["store_id", "product_id"], how="left")
        .withColumn("discount_pct", F.coalesce("discount_pct", F.lit(0)))
        .withColumn("net_revenue", F.round(F.col("revenue") * (1 - F.col("discount_pct") / 100), 2))
    )
    (with_promo.select("sale_id", "store_id", "product", "revenue", "discount_pct", "net_revenue")
               .orderBy("sale_id")
               .show())
    return promotions, with_promo


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Note that sale 1 (S01, Latte) gets 10 % but sale 12 (S03, Latte) gets nothing: the
    promotion for Latte is only in S01 and S02. Joining on `product_id` alone would have
    been wrong.

    ## 7. Joining on a range (non-equi join)

    The condition does not have to be an equality. **Question:** label each sale as a
    *single*, *small*, or *bulk* order, using a table of quantity bands. A sale matches
    the band whose range contains its quantity:
    """)
    return


@app.cell
def _(F, sales, spark):
    bands = spark.createDataFrame(
        [("single", 1, 1), ("small", 2, 3), ("bulk", 4, 99)],
        "band STRING, min_qty INT, max_qty INT",
    )
    (sales.join(bands, F.col("quantity").between(F.col("min_qty"), F.col("max_qty")), "left")
          .select("sale_id", "quantity", "band")
          .orderBy("sale_id")
          .show())
    return (bands,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 8. Self-join: a table joined with itself

    **Question:** *which pairs of stores are in the same city?* (Maybe they can share a
    delivery truck.) We join `stores` with itself. The aliases `a` and `b` are required,
    otherwise every column name would be ambiguous. The condition `a.store_id < b.store_id`
    removes pairs of a store with itself, and lists each pair only once.
    """)
    return


@app.cell
def _(F, stores):
    (stores.alias("a")
           .join(stores.alias("b"),
                 (F.col("a.city") == F.col("b.city")) & (F.col("a.store_id") < F.col("b.store_id")))
           .select(F.col("a.city"),
                   F.col("a.store_name").alias("store_1"),
                   F.col("b.store_name").alias("store_2"))
           .show())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 9. Two classic traps

    ### Trap 1: duplicate keys multiply rows

    A join produces **one output row for every matching pair**. If the right table has
    the same key twice, every matching left row appears twice, and your totals are
    inflated. Suppose someone added a new Latte price **without removing the old row**:
    """)
    return


@app.cell
def _(F, products, sales, spark):
    products_dup = products.union(
        spark.createDataFrame([("P01", "Latte", "Coffee", 4.75)], products.schema)
    )

    def revenue_with(prod):
        return (sales.join(prod, on="product_id", how="inner")
                     .agg(F.count("*").alias("rows"),
                          F.sum(F.col("quantity") * F.col("price")).alias("revenue")))

    print("correct products table:")
    revenue_with(products).show()
    print("products table with a duplicate P01:")
    revenue_with(products_dup).show()

    # The check to run BEFORE joining: is the key really unique?
    products_dup.groupBy("product_id").count().filter(F.col("count") > 1).show()
    return products_dup, revenue_with


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The 4 Latte sales became 8 rows. **Before a join, check that the key of the lookup
    table is unique** (`groupBy(key).count()` with `count > 1` should be empty).

    ### Trap 2: `NULL` keys never match

    In SQL logic, `NULL == NULL` is **not true** (it is `NULL`, "unknown"), so rows with a
    `NULL` key never match anything, not even another `NULL`. Usually that is what you
    want. If you really mean "missing matches missing", use the null-safe comparison
    `eqNullSafe()` (SQL: `<=>`).
    """)
    return


@app.cell
def _(spark):
    left_t = spark.createDataFrame([("A", 1), (None, 2)], "k STRING, left_val INT")
    right_t = spark.createDataFrame([("A", "x"), (None, "y")], "k STRING, right_val STRING")

    print("Normal equality: the NULL keys do not match")
    left_t.join(right_t, left_t.k == right_t.k).show()

    print("eqNullSafe: NULL matches NULL")
    left_t.join(right_t, left_t.k.eqNullSafe(right_t.k)).orderBy("left_val").show()
    return left_t, right_t


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 10. Broadcast joins: no shuffle for small tables

    A normal join **shuffles** both tables so that equal keys meet on the same machine,
    exactly like `rdd.join()`. But when one table is **small** (like `stores` or
    `products`), Spark can instead send a full copy of it to every executor and join
    locally, with no shuffle of the big table. (You may have done this by hand with RDDs:
    a broadcast variable holding a dictionary, used inside `map()`.)

    Spark does this **automatically** for tables below
    `spark.sql.autoBroadcastJoinThreshold` (10 MB by default). You can also ask for it
    with `F.broadcast(df)`. In the physical plan, look for `BroadcastHashJoin`
    (broadcast) instead of `SortMergeJoin` (shuffle).
    """)
    return


@app.cell
def _(F, sales, stores):
    sales.join(F.broadcast(stores), on="store_id").explain()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 11. Joins in Spark SQL

    The same joins in SQL, after registering the DataFrames as views. `LEFT ANTI JOIN`
    and `LEFT SEMI JOIN` are Spark SQL keywords.
    """)
    return


@app.cell
def _(products, sales, spark, stores):
    sales.createOrReplaceTempView("sales")
    stores.createOrReplaceTempView("stores")
    products.createOrReplaceTempView("products")

    spark.sql("""
        SELECT   t.city, p.category, SUM(s.quantity * p.price) AS revenue
        FROM     sales s
        JOIN     stores   t ON s.store_id   = t.store_id
        JOIN     products p ON s.product_id = p.product_id
        GROUP BY t.city, p.category
        ORDER BY t.city, p.category
    """).show()

    spark.sql("""
        SELECT * FROM products p
        LEFT ANTI JOIN sales s ON p.product_id = s.product_id
    """).show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 12. Try it: compare the join types

    Pick a join type for `sales` (left) and `stores` (right) on `store_id`. Watch the row
    count and the `NULL`s change.
    """)
    return


@app.cell
def _(mo):
    how_choice = mo.ui.dropdown(
        options=["inner", "left", "right", "full", "left_semi", "left_anti"],
        value="inner",
        label="how=",
    )
    how_choice
    return (how_choice,)


@app.cell
def _(F, how_choice, sales, stores):
    picked = sales.join(stores, on="store_id", how=how_choice.value)
    print(f'sales.join(stores, on="store_id", how="{how_choice.value}") -> {picked.count()} rows')
    picked.orderBy(F.col("sale_id").asc_nulls_last(), "store_id").show()
    return (picked,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 13. Cheat sheet: RDD joins → DataFrame joins

    | Task | RDD (pair RDDs) | DataFrame |
    |---|---|---|
    | Prepare | `a.map(lambda r: (key, value))` for each side | nothing: name the key column |
    | Inner join | `a.join(b)` → `(k, (va, vb))` | `a.join(b, "k")` → flat columns |
    | Left outer | `a.leftOuterJoin(b)` → `vb` may be `None` | `a.join(b, "k", "left")` → `NULL` columns |
    | Right outer | `a.rightOuterJoin(b)` | `a.join(b, "k", "right")` |
    | Full outer | `a.fullOuterJoin(b)` | `a.join(b, "k", "full")` |
    | Keep left rows with a match | `a.join(b.keys().distinct().map(lambda k: (k, 1))).mapValues(lambda p: p[0])` | `a.join(b, "k", "left_semi")` |
    | Keep left rows without a match | `a.subtractByKey(b)` | `a.join(b, "k", "left_anti")` |
    | Every pair | `a.cartesian(b)` | `a.crossJoin(b)` |
    | Composite key | key is a tuple | `on=["k1", "k2"]` |
    | Small lookup table | broadcast variable + `map()` | `a.join(F.broadcast(b), "k")` |

    ### Checklist before you join

    1. What is the key, and is it **unique** in the lookup table?
    2. Which rows must survive? (all left rows → `left`; only matches → `inner`)
    3. Are there `NULL` keys, and what should happen to them?
    4. After the join: did the row count change the way you expected?

    **Next notebooks:** `04_dataframes_with_claude.py` (Spark + an LLM) and
    `05_dataframe_advanced.py` (arrays, nested data, window functions, set operations, dates, writing data).
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 14. Exercises

    Use `sales`, `stores`, and `products`. Try each one in a new cell first, then open
    the solution.

    1. Show every sale with its `product` name and `price`, **including** sales of unknown
       products. Order by `sale_id`.
    2. Total quantity sold per **store name**, including stores with no sales (show `0`
       for them). Order by store name.
    3. Which **categories** were sold in **San Jose**? Show each category once.
    4. Which sales have **both** a known store **and** a known product? Show just their
       `sale_id`s, using semi joins only.
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
def _(F, products, sales, solution, stores):
    exercise_ns = {"F": F, "sales": sales, "stores": stores, "products": products}
    solution('''
        # Exercise 1 -- a left join keeps sale 9 (unknown product) with NULLs
        (sales.join(products, on="product_id", how="left")
              .select("sale_id", "product_id", "product", "price")
              .orderBy("sale_id")
              .show())
    ''', exercise_ns)
    return (exercise_ns,)


@app.cell(hide_code=True)
def _(exercise_ns, solution):
    solution('''
        # Exercise 2 -- stores on the left; S05 has no sales, so its sum is NULL -> 0
        (stores.join(sales, on="store_id", how="left")
               .groupBy("store_name")
               .agg(F.coalesce(F.sum("quantity"), F.lit(0)).alias("items"))
               .orderBy("store_name")
               .show())
    ''', exercise_ns)
    return


@app.cell(hide_code=True)
def _(exercise_ns, solution):
    solution('''
        # Exercise 3
        (sales.join(stores, on="store_id")
              .join(products, on="product_id")
              .filter(F.col("city") == "San Jose")
              .select("category")
              .distinct()
              .orderBy("category")
              .show())
    ''', exercise_ns)
    return


@app.cell(hide_code=True)
def _(exercise_ns, solution):
    solution('''
        # Exercise 4 -- two semi joins act as two filters
        (sales.join(stores, on="store_id", how="left_semi")
              .join(products, on="product_id", how="left_semi")
              .select("sale_id")
              .orderBy("sale_id")
              .show())
    ''', exercise_ns)
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
