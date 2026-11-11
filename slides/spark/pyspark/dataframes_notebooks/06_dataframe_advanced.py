import marimo

__generated_with = "0.23.10"
app = marimo.App(width="medium", app_title="DataFrames 5 — Advanced")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # DataFrames, Part 5: Advanced Transformations

    **Notebook 5 of 5** · before: `01_dataframe_basics.py` … `04_dataframes_with_claude.py`

    Notebooks 1–3 covered the transformations you use every day. This notebook fills in
    the rest of the toolbox, again comparing each idea with the RDD code you know:

    1. **Arrays and `explode()`**: the DataFrame version of `flatMap()` (word count!)
    2. **Nested data**: structs, arrays of structs, and JSON
    3. **Window functions**: top-N per group, previous/next row, running totals
    4. **Set operations**: `union`, `unionByName`, `intersect`, `exceptAll`
    5. **Dates and strings**: timestamps, date math, regular expressions
    6. **Writing data and performance**: Parquet, `partitionBy`, `cache`, `repartition` vs. `coalesce`
    7. **Handy extras**: `withColumns`, `unpivot`, `transform`, `replace`, `sample`

    **Our story:** the *Bean & Leaf* coffee chain again. This time: orders from the
    mobile app (nested JSON), a week of daily sales per store, and loyalty sign-ups.

    For **user-defined functions (UDFs)**, see [`../dataframes_udf/`](../dataframes_udf)
    and practice Set 08 in [`../dataframes_practice_questions/`](../dataframes_practice_questions).
    """)
    return


@app.cell
def _():
    import contextlib
    import io
    import os
    import tempfile

    import marimo as mo
    from pyspark.sql import SparkSession, Window
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder
        .appName("dataframes-05-advanced")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")  # small data: few partitions
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext
    print("Spark version:", spark.version)
    return F, Window, contextlib, io, mo, os, sc, spark, tempfile


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1. Arrays and `explode()`: the DataFrame `flatMap()`

    A column can hold an **array** (a list of values) in each row. Two functions turn text
    into rows of words, exactly what `flatMap()` did for RDDs:

    * `F.split(column, pattern)` cuts a string into an **array** (the pattern is a regular
      expression)
    * `F.explode(array_column)` makes **one row per array element**

    ### Word count, both ways

    The classic first Spark program. Here are five customer reviews.
    """)
    return


@app.cell
def _(sc, spark):
    review_lines = [
        "Best latte in San Jose",
        "The latte was lukewarm and the line was long",
        "Great cold brew and friendly staff",
        "Friendly staff but the music was too loud",
        "Cold brew was great, the croissant was stale",
    ]

    # RDD way: flatMap splits each line into words, reduceByKey adds the 1s
    word_counts_rdd = (
        sc.parallelize(review_lines)
        .flatMap(lambda line: line.lower().replace(",", "").split())
        .map(lambda w: (w, 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda kv: (-kv[1], kv[0]))
    )
    print(word_counts_rdd.take(5))

    lines_df = spark.createDataFrame([(l,) for l in review_lines], "line STRING")
    lines_df.show(truncate=False)
    return lines_df, review_lines, word_counts_rdd


@app.cell
def _(F, lines_df):
    # DataFrame way: split -> array, explode -> one row per word, groupBy -> count
    words = (
        lines_df
        .select(F.split(F.lower("line"), r"[^a-z]+").alias("words"))   # an array per line
    )
    words.show(truncate=False)

    word_counts = (
        words
        .select(F.explode("words").alias("word"))                      # one row per word
        .filter(F.col("word") != "")
        .groupBy("word").count()
        .orderBy(F.desc("count"), "word")
    )
    word_counts.show(5)
    return word_counts, words


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Working with array columns

    Each app order has a list of items. Useful functions:

    | Function | Result |
    |---|---|
    | `F.size(arr)` | number of elements |
    | `F.array_contains(arr, value)` | `true` if the value is in the array |
    | `F.array_distinct(arr)`, `F.array_sort(arr)` | remove duplicates / sort |
    | `F.get(arr, 0)` | one element, counting from 0; `NULL` if there is no such element |
    | `F.explode(arr)` | one row per element; rows with an **empty or `NULL`** array disappear |
    | `F.explode_outer(arr)` | like `explode`, but keeps those rows (with `NULL`) |
    | `F.posexplode(arr)` | also returns the position of each element |
    | `F.collect_list(col)` | the opposite of `explode`: rows back into an array (Notebook 2) |
    """)
    return


@app.cell
def _(F, spark):
    orders = spark.createDataFrame(
        [
            (101, "S01", ["Latte", "Croissant"]),
            (102, "S01", ["Espresso"]),
            (103, "S02", ["Latte", "Latte", "Muffin"]),
            (104, "S02", []),          # cancelled order: no items
            (105, "S03", None),        # items unknown
        ],
        "order_id INT, store_id STRING, items ARRAY<STRING>",
    )
    orders.select(
        "order_id",
        "items",
        F.size("items").alias("n_items"),
        F.array_contains("items", "Latte").alias("has_latte"),
        F.array_distinct("items").alias("distinct_items"),
        F.get("items", 0).alias("first_item"),
    ).orderBy("order_id").show(truncate=False)
    return (orders,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Note what Spark does with the empty and the `NULL` array: `size()` gives `0` for `[]`
    and `NULL` for `NULL` (Spark 4's default ANSI mode; older versions returned `-1`), and
    `F.get()` gives `NULL` when there is no first element.

    **Careful:** the shorter `F.col("items")[0]` (or `F.element_at`) **fails the whole job**
    on an empty array in Spark 4 (`INVALID_ARRAY_INDEX`), the same strictness as `cast()` in
    Notebook 1. Use `F.get()` (or `F.try_element_at()`) when an array may be too short.

    Now compare `explode`, `explode_outer` and `posexplode`. Orders 104 and 105 **vanish**
    with `explode`, just like an empty list produces nothing in `flatMap()`. If you count
    orders after exploding, use `explode_outer` or you lose them.
    """)
    return


@app.cell
def _(F, orders):
    print("explode:")
    orders.select("order_id", F.explode("items").alias("item")).orderBy("order_id", "item").show()

    print("explode_outer:")
    orders.select("order_id", F.explode_outer("items").alias("item")).orderBy("order_id", "item").show()

    print("posexplode:")
    orders.select("order_id", F.posexplode("items").alias("pos", "item")).orderBy("order_id", "pos").show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2. Nested data: structs and JSON

    The mobile app sends each order as one JSON object. A **struct** is a column made of
    named fields (like a small row inside a row); `items` is an **array of structs**:

    ```json
    {"order_id": "A1", "ts": "2026-10-01 08:15:00",
     "customer": {"id": "C1", "tier": "gold"},
     "items": [{"product": "Latte", "qty": 2}, {"product": "Croissant", "qty": 1}]}
    ```

    `spark.read.json()` infers the whole nested schema. Look at `printSchema()`.
    """)
    return


@app.cell
def _(mo, spark):
    app_orders = spark.read.json(str(mo.notebook_dir() / "data" / "app_orders.json"))
    app_orders.printSchema()
    app_orders.orderBy("order_id").show(truncate=False)
    return (app_orders,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Reading nested fields:** use a dot, `F.col("customer.tier")`, or `"customer.*"` for all
    fields of a struct. To get **one row per item**, explode the array of structs, then read
    the fields of each item struct. The result is a flat table that `groupBy` can use.
    """)
    return


@app.cell
def _(F, app_orders):
    order_items = (
        app_orders
        .select(
            "order_id",
            F.col("customer.id").alias("customer_id"),
            F.col("customer.tier").alias("tier"),
            F.explode("items").alias("item"),
        )
        .select("order_id", "customer_id", "tier", "item.product", "item.qty")
    )
    order_items.orderBy("order_id", "product").show()

    # Items per loyalty tier. Order A4 has no items, so explode() dropped it,
    # and the silver tier is missing here! (explode_outer would keep it.)
    order_items.groupBy("tier").agg(F.sum("qty").alias("items")).orderBy("tier").show()
    return (order_items,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **JSON stored as text:** sometimes JSON arrives as a plain **string column** (e.g. inside
    a CSV file or a database table). `F.from_json(column, schema)` parses it into a struct;
    `F.to_json()` goes the other way.
    """)
    return


@app.cell
def _(F, spark):
    events = spark.createDataFrame(
        [(1, '{"device": "ios", "version": "3.2"}'),
         (2, '{"device": "android", "version": "3.1"}')],
        "event_id INT, payload STRING",
    )
    (events
     .withColumn("p", F.from_json("payload", "device STRING, version STRING"))
     .select("event_id", "p.device", "p.version")
     .show())
    return (events,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 3. Window functions

    Notebook 2 gave a first look. A **window function** computes a value for each row from
    a group of **related rows** (its *window*), **without** collapsing them like `groupBy`.

    ```python
    w = Window.partitionBy("store_id").orderBy("day")
    F.lag("revenue", 1).over(w)
    ```

    * `partitionBy(...)`: which rows belong together (like the key of `groupBy`)
    * `orderBy(...)`: the order inside each group (needed for ranking, previous/next, running totals)
    * `rowsBetween(start, end)`: optional frame, e.g. "the 2 rows before up to this row"

    With RDDs, all of this meant `groupByKey()`, then sorting each group's list in Python
    by hand. Here is one week of daily revenue for two stores:
    """)
    return


@app.cell
def _(F, spark):
    _days = ["2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04",
             "2026-10-05", "2026-10-06", "2026-10-07"]
    _s01 = [120, 135, 135, 150, 210, 240, 180]
    _s02 = [90, 95, 80, 110, 160, 175, 130]
    daily_sales = (
        spark.createDataFrame(
            [("S01", d, r) for d, r in zip(_days, _s01)] + [("S02", d, r) for d, r in zip(_days, _s02)],
            "store_id STRING, day STRING, revenue INT",
        )
        .withColumn("day", F.to_date("day"))
    )
    daily_sales.orderBy("store_id", "day").show()
    return (daily_sales,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Ranking: `row_number`, `rank`, `dense_rank`

    **Question:** rank each store's days from best to worst. S01 has a **tie** (135 on Oct 2
    and Oct 3), which shows the difference:

    * `row_number()`: 1, 2, 3, … always unique (ties are broken arbitrarily, so add a tie-breaker)
    * `rank()`: ties share a rank, then a **gap** (… 5, 5, 7)
    * `dense_rank()`: ties share a rank, **no gap** (… 5, 5, 6)
    """)
    return


@app.cell
def _(F, Window, daily_sales):
    best_first = Window.partitionBy("store_id").orderBy(F.desc("revenue"), "day")
    ranked = daily_sales.select(
        "store_id", "day", "revenue",
        F.row_number().over(best_first).alias("row_number"),
        F.rank().over(Window.partitionBy("store_id").orderBy(F.desc("revenue"))).alias("rank"),
        F.dense_rank().over(Window.partitionBy("store_id").orderBy(F.desc("revenue"))).alias("dense_rank"),
    )
    ranked.filter("store_id = 'S01'").orderBy("row_number").show()
    return best_first, ranked


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Top-N per group

    The most common window pattern: **number the rows inside each group, then keep the first
    N**. *Question:* the 2 best days of each store.

    The same trick removes duplicates by keeping **the latest record per key** (e.g. each
    customer's most recent order): order by date descending and keep `row_number == 1`.
    """)
    return


@app.cell
def _(F, best_first, daily_sales):
    (daily_sales
     .withColumn("rn", F.row_number().over(best_first))
     .filter(F.col("rn") <= 2)
     .orderBy("store_id", "rn")
     .show())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Previous and next row: `lag()` and `lead()`

    **Question:** how did revenue change from the day before? `F.lag("revenue", 1)` reads the
    value **one row earlier** in the window (`NULL` for the first day); `F.lead()` reads the
    next row.
    """)
    return


@app.cell
def _(F, Window, daily_sales):
    by_day = Window.partitionBy("store_id").orderBy("day")
    (daily_sales
     .withColumn("prev_day", F.lag("revenue", 1).over(by_day))
     .withColumn("change", F.col("revenue") - F.col("prev_day"))
     .withColumn("pct_change", F.round((F.col("revenue") / F.col("prev_day") - 1) * 100, 1))
     .withColumn("next_day", F.lead("revenue", 1).over(by_day))
     .filter("store_id = 'S01'")
     .orderBy("day")
     .show())
    return (by_day,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Running totals and moving averages: `rowsBetween()`

    A **frame** says which rows around the current one to use:

    * `rowsBetween(Window.unboundedPreceding, Window.currentRow)`: from the first row up to
      this one → **running total** (week-to-date revenue)
    * `rowsBetween(-2, Window.currentRow)`: this row and the 2 before → **3-day moving average**
    * no `orderBy` at all → the whole group → **share of the store's weekly total**
    """)
    return


@app.cell
def _(F, Window, by_day, daily_sales):
    running = by_day.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    last_3 = by_day.rowsBetween(-2, Window.currentRow)
    whole_store = Window.partitionBy("store_id")

    windowed = daily_sales.select(
        "store_id", "day", "revenue",
        F.sum("revenue").over(running).alias("week_to_date"),
        F.round(F.avg("revenue").over(last_3), 1).alias("avg_3_days"),
        F.round(F.col("revenue") / F.sum("revenue").over(whole_store) * 100, 1).alias("pct_of_week"),
    )
    windowed.orderBy("store_id", "day").show(14)
    return last_3, running, whole_store, windowed


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Try it: window function explorer

    Pick a store and a calculation; the table re-computes.
    """)
    return


@app.cell
def _(mo):
    store_pick = mo.ui.dropdown(options=["S01", "S02"], value="S01", label="Store")
    calc_pick = mo.ui.dropdown(
        options=["running total", "3-day moving average", "change vs. previous day", "rank (best = 1)"],
        value="running total",
        label="Calculation",
    )
    mo.hstack([store_pick, calc_pick], justify="start")
    return calc_pick, store_pick


@app.cell
def _(F, Window, by_day, calc_pick, daily_sales, last_3, running, store_pick):
    calculations = {
        "running total":           F.sum("revenue").over(running),
        "3-day moving average":    F.round(F.avg("revenue").over(last_3), 1),
        "change vs. previous day": F.col("revenue") - F.lag("revenue", 1).over(by_day),
        "rank (best = 1)":         F.rank().over(Window.partitionBy("store_id").orderBy(F.desc("revenue"))),
    }
    (daily_sales
     .withColumn("result", calculations[calc_pick.value])
     .filter(F.col("store_id") == store_pick.value)
     .orderBy("day")
     .show())
    return (calculations,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 4. Set operations

    You know `union()`, `intersection()` and `subtract()` from RDDs. DataFrames have the
    same ideas, with one **big trap**: `union()` matches columns **by position**, not by name.

    Two stores collected loyalty sign-ups, but the Downtown file has its columns in a
    different order:
    """)
    return


@app.cell
def _(spark):
    campus_members = spark.createDataFrame(
        [("C1", "Ana"), ("C2", "Ben"), ("C3", "Chloe")], "customer_id STRING, name STRING"
    )
    downtown_members = spark.createDataFrame(
        [("Ben", "C2"), ("Dev", "C4")], "name STRING, customer_id STRING"
    )

    print("union(): columns matched by POSITION -- names and ids get mixed up!")
    campus_members.union(downtown_members).show()

    print("unionByName(): columns matched by NAME")
    all_members = campus_members.unionByName(downtown_members)
    all_members.orderBy("customer_id").show()
    return all_members, campus_members, downtown_members


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Both `union` and `unionByName` keep **duplicates** (Ben appears twice), like SQL
    `UNION ALL` and like RDD `union()`. Add `.distinct()` for SQL `UNION`.
    If one side has extra columns, `unionByName(other, allowMissingColumns=True)` fills them
    with `NULL`.

    | DataFrame | Meaning | RDD |
    |---|---|---|
    | `a.unionByName(b)` | all rows of both (duplicates kept) | `a.union(b)` |
    | `a.intersect(b)` | rows in both (distinct) | `a.intersection(b)` |
    | `a.subtract(b)` / `a.exceptAll(b)` | rows of `a` not in `b` (distinct / keeping duplicates) | `a.subtract(b)` |

    Set operations compare **whole rows**, so first `select()` the columns that should count.
    """)
    return


@app.cell
def _(campus_members, downtown_members, spark):
    campus_ids = campus_members.select("customer_id")
    downtown_ids = downtown_members.select("customer_id")

    print("members of both stores (intersect):")
    campus_ids.intersect(downtown_ids).show()

    print("campus only (subtract):")
    campus_ids.subtract(downtown_ids).orderBy("customer_id").show()

    print("unionByName with an extra column (allowMissingColumns=True):")
    online_members = spark.createDataFrame([("C5", "Eve", "eve@example.com")],
                                           "customer_id STRING, name STRING, email STRING")
    campus_members.unionByName(online_members, allowMissingColumns=True).orderBy("customer_id").show()
    return campus_ids, downtown_ids, online_members


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 5. Dates, times, and strings

    ### Dates and timestamps

    The app orders have a timestamp string `ts`. Turn it into a real `timestamp` with
    `F.to_timestamp()`, then the date functions work:

    | Function | Example result |
    |---|---|
    | `F.to_date(ts)`, `F.hour(ts)` | `2026-10-01`, `8` |
    | `F.date_format(ts, "EEEE")` | `Thursday` (any pattern: `"yyyy-MM"`, `"HH:mm"`, …) |
    | `F.dayofweek(ts)` | `1` = Sunday … `7` = Saturday |
    | `F.date_trunc("week", ts)` | the Monday that starts that week |
    | `F.datediff(end, start)` | days between two dates |
    | `F.date_add(date, n)` | `n` days later |
    """)
    return


@app.cell
def _(F, app_orders):
    order_times = (
        app_orders
        .withColumn("ts", F.to_timestamp("ts"))
        .select(
            "order_id",
            "ts",
            F.to_date("ts").alias("date"),
            F.hour("ts").alias("hour"),
            F.date_format("ts", "EEEE").alias("weekday"),
            F.dayofweek("ts").alias("dow"),
            F.date_trunc("week", "ts").cast("date").alias("week_start"),
            F.datediff(F.lit("2026-10-31"), F.to_date("ts")).alias("days_to_oct31"),
            F.date_add(F.to_date("ts"), 30).alias("coupon_expires"),
        )
    )
    order_times.orderBy("order_id").show()
    return (order_times,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Strings and regular expressions

    The cash registers print products like `Latte (L)`. **Question:** split that into the
    product name and the cup size. `F.regexp_extract(col, pattern, group)` returns the part
    of the text matched by one group `( … )` of a regular expression.
    """)
    return


@app.cell
def _(F, spark):
    register = spark.createDataFrame(
        [("Latte (L)",), ("Espresso (S)",), ("Cold Brew (M)",), ("Croissant",)], "label STRING"
    )
    register.select(
        "label",
        F.trim(F.regexp_replace("label", r"\(.*\)", "")).alias("product"),
        F.regexp_extract("label", r"\((S|M|L)\)", 1).alias("size"),
        F.length("label").alias("length"),
        F.substring("label", 1, 3).alias("first_3"),
        F.lpad(F.length("label").cast("string"), 3, "0").alias("padded"),
        F.upper(F.split("label", " ")[0]).alias("first_word_upper"),
    ).show()
    return (register,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Note: when the pattern does not match (`Croissant`), `regexp_extract` returns an empty
    string `""`, not `NULL`.

    ## 6. Writing data, and performance basics

    ### Writing Parquet, with `partitionBy`

    **Parquet** is the standard file format for Spark: columnar, compressed, and it stores
    the schema. `df.write` mirrors `spark.read`:

    ```python
    df.write.mode("overwrite").partitionBy("store_id").parquet(path)
    ```

    * `mode`: `"errorifexists"` (default), `"overwrite"`, `"append"`, `"ignore"`
    * `partitionBy("store_id")` writes **one folder per store**. A later query that filters
      on `store_id` reads **only that folder** (*partition pruning*).

    We write to a temporary folder so nothing is left in your project.
    """)
    return


@app.cell
def _(daily_sales, os, tempfile):
    out_dir = os.path.join(tempfile.mkdtemp(prefix="beanleaf_"), "daily_sales")
    daily_sales.write.mode("overwrite").partitionBy("store_id").parquet(out_dir)

    # one sub-folder per store, each holding parquet files (how many depends on your CPU count)
    for _name in sorted(os.listdir(out_dir)):
        if _name.startswith("store_id="):
            print(f"daily_sales/{_name}/")
    return (out_dir,)


@app.cell
def _(F, contextlib, io, out_dir, spark):
    sales_back = spark.read.parquet(out_dir)
    sales_back.printSchema()        # store_id came back from the folder names

    s02_only = sales_back.filter(F.col("store_id") == "S02")
    _plan = io.StringIO()
    with contextlib.redirect_stdout(_plan):
        s02_only.explain()
    print("Only the store_id=S02 folder is read (partition pruning):",
          "PartitionFilters: [isnotnull(store_id" in _plan.getvalue())
    print("rows:", s02_only.count())
    return s02_only, sales_back


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### `cache()`: reuse a result instead of recomputing it

    Remember lazy evaluation: every action recomputes the DataFrame **from the start**. If
    you use the same DataFrame in several actions (e.g. an expensive join you count, then
    group, then write), `cache()` keeps it in memory after the first action, exactly like
    `rdd.cache()`. Call `unpersist()` when done. Don't cache everything: memory is limited,
    and caching something used only once just costs time.
    """)
    return


@app.cell
def _(daily_sales):
    weekly = daily_sales.groupBy("store_id").sum("revenue")
    weekly.cache()
    print("is_cached:", weekly.is_cached, "| level:", weekly.storageLevel)
    print("first action fills the cache, rows =", weekly.count())
    weekly.orderBy("store_id").show()     # served from memory
    weekly.unpersist()
    print("after unpersist, is_cached:", weekly.is_cached)
    return (weekly,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### `repartition()` vs. `coalesce()`

    Same as for RDDs:

    * `repartition(n)` (or `repartition(n, "col")`) does a full **shuffle**; it can increase
      or decrease the number of partitions, and spreads rows evenly.
    * `coalesce(n)` only **decreases** the number, by merging partitions, **without a
      shuffle** (cheap). Typical use: write fewer, bigger files at the end of a job.
    """)
    return


@app.cell
def _(spark):
    numbers = spark.range(0, 1000, 1, numPartitions=8)
    print("start:          ", numbers.rdd.getNumPartitions())
    print("coalesce(2):    ", numbers.coalesce(2).rdd.getNumPartitions())
    print("repartition(16):", numbers.repartition(16).rdd.getNumPartitions())
    print("coalesce(16):   ", numbers.coalesce(16).rdd.getNumPartitions(), "(cannot increase)")
    return (numbers,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 7. Handy extras

    | Method | What it does |
    |---|---|
    | `withColumns({...})` | add or replace several columns in one call |
    | `unpivot(...)` | the opposite of `pivot`: wide columns → rows ("melt") |
    | `transform(func)` | chain your own function like a built-in method |
    | `replace({...}, subset=[...])` | replace specific values |
    | `sample(fraction, seed)` | a random sample of about `fraction` of the rows |
    | `randomSplit([0.8, 0.2], seed)` | split into random parts (e.g. train/test) |
    """)
    return


@app.cell
def _(F, daily_sales):
    # withColumns: several new columns at once
    daily_sales.withColumns({
        "weekday": F.date_format("day", "EEE"),
        "weekend": F.dayofweek("day").isin(1, 7),
        "with_tax": F.round(F.col("revenue") * 1.0925, 2),
    }).filter("store_id = 'S01'").orderBy("day").show()
    return


@app.cell
def _(spark):
    # unpivot: a wide pivot table back to long format
    wide = spark.createDataFrame(
        [("S01", 26.5, 7.0, 3.25), ("S02", 36.0, 0.0, 11.0)],
        "store_id STRING, Coffee DOUBLE, Tea DOUBLE, Bakery DOUBLE",
    )
    wide.show()
    (wide.unpivot("store_id", ["Coffee", "Tea", "Bakery"], "category", "revenue")
         .orderBy("store_id", "category")
         .show())
    return (wide,)


@app.cell
def _(F, all_members):
    # transform: reusable cleaning steps that read like built-in methods
    def with_clean_names(df):
        return df.withColumn("name", F.upper(F.trim("name")))

    def only_new_ids(df):
        return df.filter(F.col("customer_id") > "C2")

    (all_members
     .transform(with_clean_names)
     .transform(only_new_ids)
     .replace({"DEV": "DEVON"}, subset=["name"])
     .distinct()
     .orderBy("customer_id")
     .show())
    return only_new_ids, with_clean_names


@app.cell
def _(numbers):
    # sample / randomSplit: random, but repeatable with a seed
    print("sample(0.1):", numbers.sample(fraction=0.1, seed=42).count(), "rows (about 100)")
    train, test = numbers.randomSplit([0.8, 0.2], seed=42)
    print("randomSplit:", train.count(), "+", test.count(), "=", train.count() + test.count())
    return test, train


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 8. Cheat sheet: RDD → DataFrame (advanced)

    | Task | RDD | DataFrame |
    |---|---|---|
    | One input → many outputs | `rdd.flatMap(lambda l: l.split())` | `df.select(F.explode(F.split("line", " ")))` |
    | Word count | `flatMap` → `map((w, 1))` → `reduceByKey(add)` | `split` → `explode` → `groupBy("word").count()` |
    | Keep empty lists | (flatMap drops them) | `F.explode_outer(...)` |
    | Values back into a list | `groupByKey().mapValues(list)` | `groupBy(k).agg(F.collect_list(v))` |
    | Field of a nested record | `r["customer"]["tier"]` in a lambda | `F.col("customer.tier")` |
    | Top-N per key | `groupByKey()` + sort each list in Python | `row_number().over(Window.partitionBy(k).orderBy(...))` ≤ N |
    | Previous element | (very awkward) | `F.lag(col, 1).over(w)` |
    | Running total | (very awkward) | `F.sum(col).over(w.rowsBetween(Window.unboundedPreceding, 0))` |
    | Union | `a.union(b)` | `a.unionByName(b)` (not `union`!) |
    | Intersection / difference | `a.intersection(b)`, `a.subtract(b)` | `a.intersect(b)`, `a.subtract(b)` / `a.exceptAll(b)` |
    | Save | `rdd.saveAsTextFile(p)` | `df.write.mode("overwrite").partitionBy(c).parquet(p)` |
    | Keep in memory | `rdd.cache()` | `df.cache()` |
    | Fewer partitions | `rdd.coalesce(n)` | `df.coalesce(n)` |
    | Sample | `rdd.sample(False, 0.1, 42)` | `df.sample(fraction=0.1, seed=42)` |
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 9. Exercises

    Try each one in a new cell first, then open the solution.

    1. From `app_orders`, compute the **total quantity sold per product** (explode the items).
       Most sold first.
    2. From `daily_sales`, show **each store's single best day** and its revenue.
    3. From `daily_sales`, list the days on which revenue **dropped** compared with the
       previous day, with the size of the drop.
    4. Which customer ids are members of **exactly one** of the two stores
       (`campus_members`, `downtown_members`)?
    """)
    return


@app.cell(hide_code=True)
def _(contextlib, io, mo):
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
    return solution, textwrap


@app.cell(hide_code=True)
def _(F, Window, app_orders, campus_members, daily_sales, downtown_members):
    exercise_ns = {
        "F": F, "Window": Window, "app_orders": app_orders, "daily_sales": daily_sales,
        "campus_members": campus_members, "downtown_members": downtown_members,
    }
    return (exercise_ns,)


@app.cell(hide_code=True)
def _(exercise_ns, solution):
    solution('''
        # Exercise 1 -- explode the array of structs, then group by a struct field
        (app_orders
            .select(F.explode("items").alias("item"))
            .groupBy("item.product")
            .agg(F.sum("item.qty").alias("qty"))
            .orderBy(F.desc("qty"), "product")
            .show())
    ''', exercise_ns)
    return


@app.cell(hide_code=True)
def _(exercise_ns, solution):
    solution('''
        # Exercise 2 -- top-1 per group with row_number (day as tie-breaker)
        w = Window.partitionBy("store_id").orderBy(F.desc("revenue"), "day")
        (daily_sales
            .withColumn("rn", F.row_number().over(w))
            .filter("rn = 1")
            .select("store_id", "day", "revenue")
            .orderBy("store_id")
            .show())
    ''', exercise_ns)
    return


@app.cell(hide_code=True)
def _(exercise_ns, solution):
    solution('''
        # Exercise 3 -- lag, then keep the negative changes
        w = Window.partitionBy("store_id").orderBy("day")
        (daily_sales
            .withColumn("change", F.col("revenue") - F.lag("revenue", 1).over(w))
            .filter(F.col("change") < 0)
            .orderBy("store_id", "day")
            .show())
    ''', exercise_ns)
    return


@app.cell(hide_code=True)
def _(exercise_ns, solution):
    solution('''
        # Exercise 4 -- symmetric difference: (A - B) union (B - A)
        a = campus_members.select("customer_id")
        b = downtown_members.select("customer_id")
        a.subtract(b).unionByName(b.subtract(a)).orderBy("customer_id").show()
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
