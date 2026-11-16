import marimo

__generated_with = "0.24.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import duckdb
    import os
    from pathlib import Path
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import Window

    return F, Path, SparkSession, Window, duckdb, mo


@app.cell
def _(mo):
    mo.md(r"""
    # Auto Insurance Analytics: DuckDB $\rightarrow$ PySpark

    This notebook extracts the `insurance` table from DuckDB using native multi-threaded **Parquet** export (bypassing Pandas and single-threaded JDBC bottlenecks) and loads it into **PySpark** for distributed SQL analysis.

    **How the notebook is organized**

    | Part | Queries | What you practice |
    |---|---|---|
    | Business questions | 1–4 | loss ratio, renewal conversion, top-3 outliers per state, tenure cohorts |
    | Simple | 5–7 | whole-table aggregates, `WHERE` + `ORDER BY` + `LIMIT`, `GROUP BY` one column |
    | Intermediate | 8–12 | `HAVING`, `CASE WHEN` buckets, `PIVOT`, scalar subqueries, conditional aggregation |
    | Intermediate+ | 13–17 | window functions: percent of total, compare with the group average, `ROLLUP`, best row per group, deciles and running totals |

    **Every query is written twice:** first in **Spark SQL** (`spark.sql("SELECT ...")`), then with the
    **DataFrame API** (`df.filter(...).groupBy(...).agg(...)`). Both versions return the same rows in the
    same order, and each DataFrame API cell checks this for you (✅ under its table).
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Why this architecture scales for large DuckDB tables:

    ### No Python Heap Bottleneck:

        By running COPY ... TO ... (FORMAT PARQUET)
        inside DuckDB, data moves directly from DuckDB's
        storage engine to disk in C++, completely bypassing
        Python objects and Pandas DataFrames.

    ### Parallel Multi-File Sharding (For 10M+ Rows):

        If your insurance table grows to tens of gigabytes,
        change the COPY statement to export a directory of
        partitioned files (PER_THREAD_OUTPUT TRUE). Spark
        will automatically read all generated .parquet files
        concurrently across worker executors.

    ### Spark Vectorized Reader:

        spark.read.parquet() uses a vectorized columnar
        reader that loads Parquet row groups straight into
        JVM off-heap memory, allowing spark.sql(...) to
        execute queries with full Catalyst optimizer
        predicate pushdown.

    ### Safe UI Rendering:

        toPandas() is only called at the very end of each
        query cell after Spark has reduced millions of
        rows down to small summary tables for mo.ui.table().
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 1: export the DuckDB table to Parquet

    The next cell opens `auto_insurance_db.duckdb` **read-only** and asks DuckDB to write the whole
    `insurance` table to one Parquet file, `./tmp_spark_staging/insurance.parquet`. DuckDB does the work
    itself (in C++), so no rows pass through Python. Parquet is a good hand-over format: it is columnar,
    compressed (ZSTD), and it keeps the column types, so Spark does not have to guess them.

    Run the notebook from this folder: the paths are relative to the current folder.
    """)
    return


@app.cell
def _(Path, duckdb):
    # 1. Efficient Zero-Pandas Extraction via DuckDB Native Parquet Export
    db_path = "./auto_insurance_db.duckdb"
    staging_dir = Path("./tmp_spark_staging")
    staging_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = staging_dir / "insurance.parquet"

    # Connect in read-only mode so we don't lock the DB for other processes
    with duckdb.connect(db_path, read_only=True) as con:
        # DuckDB executes COPY in parallel C++ threads directly to disk.
        # For massive datasets (>10GB), use:
        # COPY insurance TO 'tmp_spark_staging/insurance' (FORMAT PARQUET, PER_THREAD_OUTPUT TRUE);
        con.execute(f"""
            COPY (SELECT * FROM insurance)
            TO '{parquet_path.as_posix()}'
            (FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000);
        """)

    parquet_str = str(parquet_path.resolve())
    return (parquet_str,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 2: start Spark and read the Parquet file

    The next cell creates the `SparkSession` (the entry point to Spark), reads the Parquet file into the
    DataFrame `df`, and gives the DataFrame a SQL name with `createOrReplaceTempView("insurance")`.
    From then on we can use the same data in two ways:

    * **Spark SQL:** `spark.sql("SELECT ... FROM insurance ...")`
    * **DataFrame API:** `df.select(...)`, `df.filter(...)`, `df.groupBy(...).agg(...)`, ...

    Both are translated by the same optimizer (Catalyst) into the same kind of plan, so neither is faster:
    pick the one that is easier to read for the question. `df.cache()` keeps the table in memory, because
    we will query it many times.
    """)
    return


@app.cell
def _(SparkSession, parquet_str):
    # 2. Initialize Spark & Load Parquet into a Spark DataFrame
    spark = (
        SparkSession.builder
        .appName("AutoInsuranceDuckDBAnalysis")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    # Spark reads Parquet natively in parallel without touching Python memory
    df = spark.read.parquet(parquet_str)

    # Register as a SQL temporary view for Spark SQL queries
    df.createOrReplaceTempView("insurance")

    # Cache if you plan to run multiple iterative SQL queries on the dataset
    df.cache()
    return df, spark


@app.cell
def _(df, mo):
    mo.md(f"""
    ### Loaded `{df.count():,}` rows into Spark DataFrame
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### The columns we use

    One row = one customer (`customer` is unique, and no column has NULLs).

    | Column | Meaning | Example values |
    |---|---|---|
    | `customer`, `state` | customer id, US state | `BU79786`, `Arizona`, `California`, `Nevada`, `Oregon`, `Washington` |
    | `coverage`, `policy_type`, `policy` | what is insured | `Basic` / `Extended` / `Premium`; `Personal Auto`, ...; `Personal L3`, ... |
    | `vehicle_class`, `vehicle_size` | the car | `SUV`, `Luxury Car`, ...; `Small` / `Medsize` / `Large` |
    | `monthly_premium_auto` | what the customer pays per month (integer, $) | 61 – 298 |
    | `total_claim_amount` | what the insurer paid out ($) | 0.1 – 2,893 |
    | `claim_to_premium_ratio` | `total_claim_amount / monthly_premium_auto` | |
    | `customer_lifetime_value` (CLV), `clv_group` | expected value of the customer | `Low CLV` ... `Very High CLV` |
    | `income`, `income_group`, `education`, `employmentstatus`, `gender`, `marital_status`, `location_code` | the customer | `Rural` / `Suburban` / `Urban`, ... |
    | `renew_offer_type`, `sales_channel`, `response` | the last renewal offer and the answer | `Offer1`–`Offer4`; `Agent`, `Branch`, `Call Center`, `Web`; `Yes` / `No` |
    | `months_since_policy_inception`, `number_of_policies`, `number_of_open_complaints` | history | |

    The next cell defines a small helper, `check_same(sql_df, api_df)`. Every DataFrame API cell calls it to
    compare its result with the SQL result: same column names, same rows, same order.
    """)
    return


@app.cell
def _(mo):
    def check_same(sql_df, api_df):
        # collect() brings the (small) results to the driver as lists of Row objects
        same = sql_df.columns == api_df.columns and sql_df.collect() == api_df.collect()
        if same:
            return mo.md("✅ Same columns, same rows, same order as the Spark SQL version.")
        return mo.md("❌ **Different result from the Spark SQL version!**")

    return (check_same,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    # Part 1: Business questions (Queries 1–4)

    ### Query 1: Which coverage / vehicle class combinations lose money?

    The **loss ratio** is claims paid divided by premiums earned. `monthly_premium_auto * 12` turns the
    monthly premium into a yearly one. A loss ratio above 1 means the insurer paid out more than it collected.

    * `GROUP BY coverage, vehicle_class` makes one row per combination.
    * `NULLIF(x, 0)` returns NULL when `x` is 0, so we never divide by zero (dividing by NULL gives NULL).
    * DataFrame API: `groupBy(...).agg(...)`, with `F.nullif` for `NULLIF`. In the API the alias is given with
      `.alias("name")` instead of `AS name`.
    * Both versions sort by the loss ratio and then by the two group columns, so rows with the same ratio
      always come out in the same order (a **tie-breaker**).
    """)
    return


@app.cell
def _(mo, spark):
    # Query 1: Loss Ratio & Profitability by Coverage and Vehicle Class
    q1_df = spark.sql("""
        SELECT
            coverage,
            vehicle_class,
            COUNT(customer) AS policy_count,
            ROUND(AVG(monthly_premium_auto), 2) AS avg_monthly_premium,
            ROUND(AVG(total_claim_amount), 2) AS avg_claim_amount,
            ROUND(SUM(total_claim_amount) / NULLIF(SUM(monthly_premium_auto * 12), 0), 3) AS annualized_loss_ratio
        FROM insurance
        GROUP BY coverage, vehicle_class
        ORDER BY annualized_loss_ratio DESC, coverage, vehicle_class
    """)

    mo.vstack([
        mo.md("### 1. Annualized Loss Ratio by Coverage & Vehicle Class"),
        mo.ui.table(q1_df.toPandas()) # Safe to collect small aggregated results for UI display
    ])
    return (q1_df,)


@app.cell
def _(F, check_same, df, mo, q1_df):
    # The same query with the DataFrame API
    q1_api = (
        df.groupBy("coverage", "vehicle_class")
        .agg(
            F.count("customer").alias("policy_count"),
            F.round(F.avg("monthly_premium_auto"), 2).alias("avg_monthly_premium"),
            F.round(F.avg("total_claim_amount"), 2).alias("avg_claim_amount"),
            F.round(
                F.sum("total_claim_amount")
                / F.nullif(F.sum(F.col("monthly_premium_auto") * 12), F.lit(0)),
                3,
            ).alias("annualized_loss_ratio"),
        )
        .orderBy(F.desc("annualized_loss_ratio"), "coverage", "vehicle_class")
    )

    mo.vstack([
        mo.md("**1. The same query with the DataFrame API**"),
        mo.ui.table(q1_api.toPandas()),
        check_same(q1_df, q1_api),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Query 2: Which renewal offer works best in which sales channel?

    The query is built in two steps with a **CTE** (`WITH offer_stats AS (...)`): first count the customers and
    the "Yes" answers per offer and channel, then compute the conversion rate and keep groups with more than
    50 customers (small groups give unreliable rates).

    * `SUM(CASE WHEN response = 'Yes' THEN 1 ELSE 0 END)` counts the rows that match a condition
      (**conditional aggregation**). In the API: `F.sum(F.when(cond, 1).otherwise(0))`.
    * A CTE becomes a Python variable in the API (`offer_stats`), and the outer query continues from it.
    * `WHERE` on the outer query filters *groups* here, because the CTE already aggregated.
    """)
    return


@app.cell
def _(mo, spark):
    # Query 2: Marketing Renew Offer Conversion & CLV Impact using CTEs
    q2_df = spark.sql("""
        WITH offer_stats AS (
            SELECT
                renew_offer_type,
                sales_channel,
                COUNT(*) AS total_customers,
                SUM(CASE WHEN response = 'Yes' THEN 1 ELSE 0 END) AS accepted_count,
                AVG(customer_lifetime_value) AS avg_clv
            FROM insurance
            GROUP BY renew_offer_type, sales_channel
        )
        SELECT
            renew_offer_type,
            sales_channel,
            total_customers,
            ROUND((accepted_count / total_customers) * 100, 2) AS conversion_rate_pct,
            ROUND(avg_clv, 2) AS avg_clv
        FROM offer_stats
        WHERE total_customers > 50
        ORDER BY conversion_rate_pct DESC, avg_clv DESC
    """)

    mo.vstack([
        mo.md("### 2. Renewal Offer Conversion Rate by Sales Channel"),
        mo.ui.table(q2_df.toPandas())
    ])
    return (q2_df,)


@app.cell
def _(F, check_same, df, mo, q2_df):
    # The same query with the DataFrame API
    # Step 1 (the CTE): one row per offer and channel
    offer_stats = (
        df.groupBy("renew_offer_type", "sales_channel")
        .agg(
            F.count("*").alias("total_customers"),
            F.sum(F.when(F.col("response") == "Yes", 1).otherwise(0)).alias("accepted_count"),
            F.avg("customer_lifetime_value").alias("avg_clv"),
        )
    )

    # Step 2 (the outer SELECT): rate, filter, sort
    q2_api = (
        offer_stats
        .filter(F.col("total_customers") > 50)
        .select(
            "renew_offer_type",
            "sales_channel",
            "total_customers",
            F.round(F.col("accepted_count") / F.col("total_customers") * 100, 2).alias("conversion_rate_pct"),
            F.round("avg_clv", 2).alias("avg_clv"),
        )
        .orderBy(F.desc("conversion_rate_pct"), F.desc("avg_clv"))
    )

    mo.vstack([
        mo.md("**2. The same query with the DataFrame API**"),
        mo.ui.table(q2_api.toPandas()),
        check_same(q2_df, q2_api),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Query 3: Who are the top-3 riskiest customers in each state?

    A **window function** computes a value for each row from a group of rows (the *window*) without
    collapsing the rows the way `GROUP BY` does.

    * `DENSE_RANK() OVER (PARTITION BY state ORDER BY claim_to_premium_ratio DESC)` numbers the customers
      1, 2, 3, ... inside each state, highest ratio first. Equal ratios get the same rank, so a state can
      show more than 3 rows: in California, three customers tie at rank 3. (`ROW_NUMBER()` would keep exactly 3.)
    * A window result cannot be used in the `WHERE` of the same `SELECT`, so the rank is computed in a CTE and
      filtered (`<= 3`) outside.
    * In the API, the window is a `Window` object: `Window.partitionBy("state").orderBy(F.desc(...))`, used with
      `F.dense_rank().over(w)`.
    * `customer` is added to the final `ORDER BY` as a tie-breaker.
    """)
    return


@app.cell
def _(mo, spark):
    # Query 3: Window Functions - Top High-Risk / High-Claim Outliers per State
    q3_df = spark.sql("""
        WITH ranked_claims AS (
            SELECT
                state,
                customer,
                policy_type,
                vehicle_class,
                ROUND(customer_lifetime_value, 2) AS clv,
                ROUND(total_claim_amount, 2) AS total_claim,
                ROUND(claim_to_premium_ratio, 2) AS claim_to_premium_ratio,
                DENSE_RANK() OVER (
                    PARTITION BY state
                    ORDER BY claim_to_premium_ratio DESC
                ) AS state_risk_rank
            FROM insurance
            WHERE claim_to_premium_ratio IS NOT NULL
        )
        SELECT *
        FROM ranked_claims
        WHERE state_risk_rank <= 3
        ORDER BY state, state_risk_rank, customer
    """)

    mo.vstack([
        mo.md("### 3. Top 3 Highest Claim-to-Premium Ratio Outliers per State"),
        mo.ui.table(q3_df.toPandas())
    ])
    return (q3_df,)


@app.cell
def _(F, Window, check_same, df, mo, q3_df):
    # The same query with the DataFrame API
    # Note: in SQL, the rank is computed on the *unrounded* ratio (the ORDER BY inside OVER
    # sees the column of the table, not the alias), so we rank first and round afterwards.
    w_state = Window.partitionBy("state").orderBy(F.desc("claim_to_premium_ratio"))

    q3_api = (
        df.filter(F.col("claim_to_premium_ratio").isNotNull())
        .withColumn("state_risk_rank", F.dense_rank().over(w_state))
        .filter(F.col("state_risk_rank") <= 3)
        .select(
            "state",
            "customer",
            "policy_type",
            "vehicle_class",
            F.round("customer_lifetime_value", 2).alias("clv"),
            F.round("total_claim_amount", 2).alias("total_claim"),
            F.round("claim_to_premium_ratio", 2).alias("claim_to_premium_ratio"),
            "state_risk_rank",
        )
        .orderBy("state", "state_risk_rank", "customer")
    )

    mo.vstack([
        mo.md("**3. The same query with the DataFrame API**"),
        mo.ui.table(q3_api.toPandas()),
        check_same(q3_df, q3_api),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Query 4: Do loyal customers complain less and claim less?

    `CASE WHEN ... THEN ... ELSE ... END` puts each customer in a **tenure cohort** (new, established, loyal).
    The labels start with `1.`, `2.`, `3.` so that sorting them alphabetically also sorts them by tenure.

    * `GROUP BY 1, 2` means "group by the 1st and 2nd column of the `SELECT`". In the API we first create the
      cohort column with `withColumn(...)` and then group by its name.
    * `CASE WHEN` becomes a chain `F.when(...).when(...).otherwise(...)`; `BETWEEN 12 AND 48` becomes
      `.between(12, 48)` (both ends included).
    """)
    return


@app.cell
def _(mo, spark):
    # Query 4: Customer Tenure vs. Open Complaints Cohort Analysis
    q4_df = spark.sql("""
        SELECT
            CASE
                WHEN months_since_policy_inception < 12 THEN '1. New (< 1 yr)'
                WHEN months_since_policy_inception BETWEEN 12 AND 48 THEN '2. Established (1-4 yrs)'
                ELSE '3. Loyal (4+ yrs)'
            END AS tenure_cohort,
            clv_group,
            COUNT(*) AS customers,
            ROUND(AVG(number_of_policies), 2) AS avg_policies,
            ROUND(AVG(number_of_open_complaints), 3) AS avg_open_complaints,
            ROUND(AVG(total_claim_amount), 2) AS avg_claim
        FROM insurance
        GROUP BY 1, 2
        ORDER BY tenure_cohort, clv_group
    """)

    mo.vstack([
        mo.md("### 4. Tenure Cohort & CLV Group Risk Profile"),
        mo.ui.table(q4_df.toPandas())
    ])
    return (q4_df,)


@app.cell
def _(F, check_same, df, mo, q4_df):
    # The same query with the DataFrame API
    tenure = F.col("months_since_policy_inception")

    q4_api = (
        df.withColumn(
            "tenure_cohort",
            F.when(tenure < 12, "1. New (< 1 yr)")
            .when(tenure.between(12, 48), "2. Established (1-4 yrs)")
            .otherwise("3. Loyal (4+ yrs)"),
        )
        .groupBy("tenure_cohort", "clv_group")
        .agg(
            F.count("*").alias("customers"),
            F.round(F.avg("number_of_policies"), 2).alias("avg_policies"),
            F.round(F.avg("number_of_open_complaints"), 3).alias("avg_open_complaints"),
            F.round(F.avg("total_claim_amount"), 2).alias("avg_claim"),
        )
        .orderBy("tenure_cohort", "clv_group")
    )

    mo.vstack([
        mo.md("**4. The same query with the DataFrame API**"),
        mo.ui.table(q4_api.toPandas()),
        check_same(q4_df, q4_api),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    # Part 2: Simple queries (Queries 5–7)

    ### Query 5: The portfolio at a glance

    Aggregate functions **without** `GROUP BY` summarize the whole table into **one row**.

    | Spark SQL | DataFrame API |
    |---|---|
    | `COUNT(*)` | `F.count("*")` |
    | `COUNT(DISTINCT state)` | `F.countDistinct("state")` |
    | `AVG(x)`, `SUM(x)`, `MIN(x)`, `MAX(x)` | `F.avg("x")`, `F.sum("x")`, `F.min("x")`, `F.max("x")` |
    | `ROUND(x, 2)` | `F.round(x, 2)` |

    In the API, `df.agg(...)` (with no `groupBy`) is the same as a `SELECT` with only aggregates.
    """)
    return


@app.cell
def _(mo, spark):
    # Query 5: whole-table summary (one row)
    q5_sql = spark.sql("""
        SELECT
            COUNT(*)                                AS customers,
            COUNT(DISTINCT state)                   AS states,
            MIN(monthly_premium_auto)               AS min_monthly_premium,
            MAX(monthly_premium_auto)               AS max_monthly_premium,
            ROUND(AVG(monthly_premium_auto), 2)     AS avg_monthly_premium,
            ROUND(SUM(total_claim_amount), 2)       AS total_claims,
            ROUND(AVG(customer_lifetime_value), 2)  AS avg_clv
        FROM insurance
    """)

    mo.vstack([
        mo.md("### 5. Portfolio summary (Spark SQL)"),
        mo.ui.table(q5_sql.toPandas()),
    ])
    return (q5_sql,)


@app.cell
def _(F, check_same, df, mo, q5_sql):
    # The same query with the DataFrame API
    q5_api = df.agg(
        F.count("*").alias("customers"),
        F.countDistinct("state").alias("states"),
        F.min("monthly_premium_auto").alias("min_monthly_premium"),
        F.max("monthly_premium_auto").alias("max_monthly_premium"),
        F.round(F.avg("monthly_premium_auto"), 2).alias("avg_monthly_premium"),
        F.round(F.sum("total_claim_amount"), 2).alias("total_claims"),
        F.round(F.avg("customer_lifetime_value"), 2).alias("avg_clv"),
    )

    mo.vstack([
        mo.md("**5. The same query with the DataFrame API**"),
        mo.ui.table(q5_api.toPandas()),
        check_same(q5_sql, q5_api),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Query 6: The 10 luxury-car customers who pay the most per year

    Pick rows (`WHERE`), add a calculated column, sort, and keep the first 10.

    * `vehicle_class IN ('Luxury Car', 'Luxury SUV')` becomes `F.col("vehicle_class").isin(...)`.
    * The calculated column `monthly_premium_auto * 12 AS annual_premium` becomes
      `(F.col("monthly_premium_auto") * 12).alias("annual_premium")` inside `select`.
    * `ORDER BY annual_premium DESC, customer` becomes `orderBy(F.desc("annual_premium"), "customer")`.
      Many customers pay the same premium, so `customer` is the tie-breaker: without it, the "top 10" could be a
      different set of rows on each run.
    * `LIMIT 10` becomes `.limit(10)`.
    """)
    return


@app.cell
def _(mo, spark):
    # Query 6: WHERE + calculated column + ORDER BY + LIMIT
    q6_sql = spark.sql("""
        SELECT
            customer,
            state,
            vehicle_class,
            coverage,
            monthly_premium_auto,
            monthly_premium_auto * 12 AS annual_premium
        FROM insurance
        WHERE vehicle_class IN ('Luxury Car', 'Luxury SUV')
        ORDER BY annual_premium DESC, customer
        LIMIT 10
    """)

    mo.vstack([
        mo.md("### 6. Top 10 luxury-vehicle customers by annual premium (Spark SQL)"),
        mo.ui.table(q6_sql.toPandas()),
    ])
    return (q6_sql,)


@app.cell
def _(F, check_same, df, mo, q6_sql):
    # The same query with the DataFrame API
    q6_api = (
        df.filter(F.col("vehicle_class").isin("Luxury Car", "Luxury SUV"))
        .select(
            "customer",
            "state",
            "vehicle_class",
            "coverage",
            "monthly_premium_auto",
            (F.col("monthly_premium_auto") * 12).alias("annual_premium"),
        )
        .orderBy(F.desc("annual_premium"), "customer")
        .limit(10)
    )

    mo.vstack([
        mo.md("**6. The same query with the DataFrame API**"),
        mo.ui.table(q6_api.toPandas()),
        check_same(q6_sql, q6_api),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Query 7: How big is each state?

    `GROUP BY state` makes one output row per state; every other column must be an aggregate.

    * SQL: `SELECT state, COUNT(*) ... GROUP BY state`
    * API: `df.groupBy("state").agg(F.count("*").alias(...), ...)`

    This is the DataFrame version of `rdd.map(lambda r: (r.state, 1)).reduceByKey(add)`, but with several
    aggregates computed in the same pass.
    """)
    return


@app.cell
def _(mo, spark):
    # Query 7: GROUP BY one column
    q7_sql = spark.sql("""
        SELECT
            state,
            COUNT(*)                            AS customers,
            ROUND(AVG(monthly_premium_auto), 2) AS avg_monthly_premium,
            ROUND(AVG(total_claim_amount), 2)   AS avg_claim,
            ROUND(MAX(total_claim_amount), 2)   AS max_claim
        FROM insurance
        GROUP BY state
        ORDER BY customers DESC
    """)

    mo.vstack([
        mo.md("### 7. Customers, premium and claims per state (Spark SQL)"),
        mo.ui.table(q7_sql.toPandas()),
    ])
    return (q7_sql,)


@app.cell
def _(F, check_same, df, mo, q7_sql):
    # The same query with the DataFrame API
    q7_api = (
        df.groupBy("state")
        .agg(
            F.count("*").alias("customers"),
            F.round(F.avg("monthly_premium_auto"), 2).alias("avg_monthly_premium"),
            F.round(F.avg("total_claim_amount"), 2).alias("avg_claim"),
            F.round(F.max("total_claim_amount"), 2).alias("max_claim"),
        )
        .orderBy(F.desc("customers"))
    )

    mo.vstack([
        mo.md("**7. The same query with the DataFrame API**"),
        mo.ui.table(q7_api.toPandas()),
        check_same(q7_sql, q7_api),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    # Part 3: Intermediate queries (Queries 8–12)

    ### Query 8: Claims by education and employment status (only large groups)

    Grouping by **two** columns gives one row per combination. `HAVING` filters **groups after** aggregation;
    `WHERE` filters **rows before** it.

    * SQL: `GROUP BY education, employmentstatus HAVING COUNT(*) >= 100`
    * API: there is no `having()`: it is simply a `filter()` placed **after** `agg()`. Because the filter
      comes after the aggregation, it sees the aggregated column `customers`.
    """)
    return


@app.cell
def _(mo, spark):
    # Query 8: GROUP BY two columns + HAVING
    q8_sql = spark.sql("""
        SELECT
            education,
            employmentstatus,
            COUNT(*)                          AS customers,
            ROUND(AVG(income), 0)             AS avg_income,
            ROUND(AVG(total_claim_amount), 2) AS avg_claim
        FROM insurance
        GROUP BY education, employmentstatus
        HAVING COUNT(*) >= 100
        ORDER BY avg_claim DESC, education, employmentstatus
    """)

    mo.vstack([
        mo.md("### 8. Average claim by education & employment status, groups with 100+ customers (Spark SQL)"),
        mo.ui.table(q8_sql.toPandas()),
    ])
    return (q8_sql,)


@app.cell
def _(F, check_same, df, mo, q8_sql):
    # The same query with the DataFrame API
    q8_api = (
        df.groupBy("education", "employmentstatus")
        .agg(
            F.count("*").alias("customers"),
            F.round(F.avg("income"), 0).alias("avg_income"),
            F.round(F.avg("total_claim_amount"), 2).alias("avg_claim"),
        )
        .filter(F.col("customers") >= 100)          # HAVING = filter after agg
        .orderBy(F.desc("avg_claim"), "education", "employmentstatus")
    )

    mo.vstack([
        mo.md("**8. The same query with the DataFrame API**"),
        mo.ui.table(q8_api.toPandas()),
        check_same(q8_sql, q8_api),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Query 9: How are claims spread over size buckets?

    `CASE WHEN` turns a number into a **category** (a bucket), then we group by the bucket.

    * The conditions are tested from top to bottom, and the first true one wins, so `< 500` only sees claims
      that are already `>= 200`.
    * The label is written once in a CTE (`bucketed`) and reused in `GROUP BY` and `ORDER BY`. In the API, the
      same thing is a `withColumn("claim_bucket", F.when(...)...)` before `groupBy`.
    * The labels start with `1.` ... `4.` so they sort in size order.
    """)
    return


@app.cell
def _(mo, spark):
    # Query 9: CASE WHEN buckets + GROUP BY
    q9_sql = spark.sql("""
        WITH bucketed AS (
            SELECT
                CASE
                    WHEN total_claim_amount < 200  THEN '1. under $200'
                    WHEN total_claim_amount < 500  THEN '2. $200 - $499'
                    WHEN total_claim_amount < 1000 THEN '3. $500 - $999'
                    ELSE '4. $1,000 and more'
                END AS claim_bucket,
                total_claim_amount,
                monthly_premium_auto,
                customer_lifetime_value
            FROM insurance
        )
        SELECT
            claim_bucket,
            COUNT(*)                                AS customers,
            ROUND(SUM(total_claim_amount), 2)       AS total_claims,
            ROUND(AVG(monthly_premium_auto), 2)     AS avg_monthly_premium,
            ROUND(AVG(customer_lifetime_value), 2)  AS avg_clv
        FROM bucketed
        GROUP BY claim_bucket
        ORDER BY claim_bucket
    """)

    mo.vstack([
        mo.md("### 9. Customers per claim-size bucket (Spark SQL)"),
        mo.ui.table(q9_sql.toPandas()),
    ])
    return (q9_sql,)


@app.cell
def _(F, check_same, df, mo, q9_sql):
    # The same query with the DataFrame API
    claim = F.col("total_claim_amount")

    q9_api = (
        df.withColumn(
            "claim_bucket",
            F.when(claim < 200, "1. under $200")
            .when(claim < 500, "2. $200 - $499")
            .when(claim < 1000, "3. $500 - $999")
            .otherwise("4. $1,000 and more"),
        )
        .groupBy("claim_bucket")
        .agg(
            F.count("*").alias("customers"),
            F.round(F.sum("total_claim_amount"), 2).alias("total_claims"),
            F.round(F.avg("monthly_premium_auto"), 2).alias("avg_monthly_premium"),
            F.round(F.avg("customer_lifetime_value"), 2).alias("avg_clv"),
        )
        .orderBy("claim_bucket")
    )

    mo.vstack([
        mo.md("**9. The same query with the DataFrame API**"),
        mo.ui.table(q9_api.toPandas()),
        check_same(q9_sql, q9_api),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Query 10: Average claim by vehicle size (rows) and location (columns)

    A **pivot** turns the values of one column (`location_code`: Rural, Suburban, Urban) into **column
    names**: a cross-table that is easy to read.

    * SQL: `... PIVOT (AVG(total_claim_amount) FOR location_code IN ('Rural', 'Suburban', 'Urban'))`.
      The inner `SELECT` lists only the columns the pivot needs; every column that is not pivoted or aggregated
      (here `vehicle_size`) becomes a row key. We round in the outer `SELECT`.
    * API: `groupBy("vehicle_size").pivot("location_code", [...]).agg(...)`.
    * Listing the values (`[...]`) is optional in the API, but it saves Spark an extra job to find them, and it
      fixes the column order.
    """)
    return


@app.cell
def _(mo, spark):
    # Query 10: PIVOT
    q10_sql = spark.sql("""
        SELECT
            vehicle_size,
            ROUND(Rural, 2)    AS Rural,
            ROUND(Suburban, 2) AS Suburban,
            ROUND(Urban, 2)    AS Urban
        FROM (
            SELECT vehicle_size, location_code, total_claim_amount
            FROM insurance
        )
        PIVOT (
            AVG(total_claim_amount)
            FOR location_code IN ('Rural' AS Rural, 'Suburban' AS Suburban, 'Urban' AS Urban)
        )
        ORDER BY vehicle_size
    """)

    mo.vstack([
        mo.md("### 10. Average claim: vehicle size × location (Spark SQL)"),
        mo.ui.table(q10_sql.toPandas()),
    ])
    return (q10_sql,)


@app.cell
def _(F, check_same, df, mo, q10_sql):
    # The same query with the DataFrame API
    locations = ["Rural", "Suburban", "Urban"]

    q10_api = (
        df.groupBy("vehicle_size")
        .pivot("location_code", locations)
        .agg(F.avg("total_claim_amount"))
        .select("vehicle_size", *[F.round(loc, 2).alias(loc) for loc in locations])
        .orderBy("vehicle_size")
    )

    mo.vstack([
        mo.md("**10. The same query with the DataFrame API**"),
        mo.ui.table(q10_api.toPandas()),
        check_same(q10_sql, q10_api),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Query 11: Which vehicle classes claim more than the company average?

    A **scalar subquery** is a `SELECT` that returns one value; it can be used anywhere a number can.
    Here `(SELECT AVG(total_claim_amount) FROM insurance)` is the company-wide average claim, used both in
    `HAVING` (to keep the classes above it) and in `SELECT` (to show it).

    * API: compute the single number first with `df.agg(F.avg(...)).first()[0]` (this runs a small Spark job
      and returns a Python `float`), then use it with `F.lit(...)` like any constant.
    * `avg_claim / company_avg` shows *how much* above average each class is (1.50 = 50% more).
    """)
    return


@app.cell
def _(mo, spark):
    # Query 11: scalar subquery in HAVING and SELECT
    q11_sql = spark.sql("""
        SELECT
            vehicle_class,
            COUNT(*)                                                         AS customers,
            ROUND(AVG(total_claim_amount), 2)                                AS avg_claim,
            ROUND((SELECT AVG(total_claim_amount) FROM insurance), 2)        AS company_avg,
            ROUND(AVG(total_claim_amount)
                  / (SELECT AVG(total_claim_amount) FROM insurance), 2)      AS times_company_avg
        FROM insurance
        GROUP BY vehicle_class
        HAVING AVG(total_claim_amount) > (SELECT AVG(total_claim_amount) FROM insurance)
        ORDER BY avg_claim DESC
    """)

    mo.vstack([
        mo.md("### 11. Vehicle classes with an above-average claim (Spark SQL)"),
        mo.ui.table(q11_sql.toPandas()),
    ])
    return (q11_sql,)


@app.cell
def _(F, check_same, df, mo, q11_sql):
    # The same query with the DataFrame API
    # 1) the scalar subquery: one number, brought back to Python
    company_avg = df.agg(F.avg("total_claim_amount")).first()[0]

    # 2) group, keep the classes above the company average, compare
    q11_api = (
        df.groupBy("vehicle_class")
        .agg(
            F.count("*").alias("customers"),
            F.avg("total_claim_amount").alias("raw_avg"),
        )
        .filter(F.col("raw_avg") > company_avg)
        .select(
            "vehicle_class",
            "customers",
            F.round("raw_avg", 2).alias("avg_claim"),
            F.round(F.lit(company_avg), 2).alias("company_avg"),
            F.round(F.col("raw_avg") / company_avg, 2).alias("times_company_avg"),
        )
        .orderBy(F.desc("avg_claim"))
    )

    mo.vstack([
        mo.md(f"**11. The same query with the DataFrame API** (company average claim = `{company_avg:,.2f}`)"),
        mo.ui.table(q11_api.toPandas()),
        check_same(q11_sql, q11_api),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Query 12: Which channel / policy type has the most unhappy customers?

    **Conditional aggregation** again, this time to compute a *percentage*: the share of customers with at
    least one open complaint.

    * `SUM(CASE WHEN number_of_open_complaints > 0 THEN 1 ELSE 0 END)` counts the customers with complaints.
      Spark SQL also has `COUNT_IF(condition)` for the same thing (`F.count_if(...)` in the API).
    * `100 * with_complaints / customers`: in Spark, `/` always returns a decimal number (a `double`), even for
      two integers, so we do not lose the fraction.
    * In the API, an aggregated column can be reused in a later `withColumn`, just like the outer `SELECT` of a CTE.
    """)
    return


@app.cell
def _(mo, spark):
    # Query 12: conditional aggregation -> percentage
    q12_sql = spark.sql("""
        WITH complaint_counts AS (
            SELECT
                sales_channel,
                policy_type,
                COUNT(*) AS customers,
                SUM(CASE WHEN number_of_open_complaints > 0 THEN 1 ELSE 0 END) AS with_complaints
            FROM insurance
            GROUP BY sales_channel, policy_type
        )
        SELECT
            sales_channel,
            policy_type,
            customers,
            with_complaints,
            ROUND(100 * with_complaints / customers, 2) AS complaint_rate_pct
        FROM complaint_counts
        ORDER BY complaint_rate_pct DESC, sales_channel, policy_type
    """)

    mo.vstack([
        mo.md("### 12. Share of customers with open complaints, by channel & policy type (Spark SQL)"),
        mo.ui.table(q12_sql.toPandas()),
    ])
    return (q12_sql,)


@app.cell
def _(F, check_same, df, mo, q12_sql):
    # The same query with the DataFrame API
    q12_api = (
        df.groupBy("sales_channel", "policy_type")
        .agg(
            F.count("*").alias("customers"),
            F.sum(F.when(F.col("number_of_open_complaints") > 0, 1).otherwise(0)).alias("with_complaints"),
        )
        .withColumn(
            "complaint_rate_pct",
            F.round(100 * F.col("with_complaints") / F.col("customers"), 2),
        )
        .orderBy(F.desc("complaint_rate_pct"), "sales_channel", "policy_type")
    )

    mo.vstack([
        mo.md("**12. The same query with the DataFrame API**"),
        mo.ui.table(q12_api.toPandas()),
        check_same(q12_sql, q12_api),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    # Part 4: Intermediate+ queries (Queries 13–17)

    These queries combine aggregation with **window functions**. Remember the order in which a query runs:
    `FROM` → `WHERE` → `GROUP BY` → `HAVING` → **window functions** → `SELECT` → `ORDER BY` → `LIMIT`.
    Window functions run *after* `GROUP BY`, so they can work on the aggregated rows.

    ### Query 13: Does each vehicle class bring its "fair share" of claims?

    We compare each class's share of **policies** with its share of **claims**. A class with 5% of the
    policies but 10% of the claims costs twice its "fair share".

    * `SUM(COUNT(*)) OVER ()` = the total of the `COUNT(*)` column over **all** groups. The empty `OVER ()`
      means "the window is the whole result". So `COUNT(*) / SUM(COUNT(*)) OVER ()` is the group's share.
    * API: first `groupBy().agg()`, then `F.sum("policies").over(Window.partitionBy())`. An empty
      `partitionBy()` is the same as `OVER ()`.
    * Spark logs a warning for a window without `PARTITION BY` ("moving all data to a single partition").
      That is fine here: there are only 6 aggregated rows.
    """)
    return


@app.cell
def _(mo, spark):
    # Query 13: percent of total with SUM(...) OVER ()
    q13_sql = spark.sql("""
        SELECT
            vehicle_class,
            COUNT(*)                                                               AS policies,
            ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)                       AS pct_of_policies,
            ROUND(SUM(total_claim_amount), 2)                                      AS total_claims,
            ROUND(100 * SUM(total_claim_amount) / SUM(SUM(total_claim_amount)) OVER (), 2) AS pct_of_claims
        FROM insurance
        GROUP BY vehicle_class
        ORDER BY total_claims DESC
    """)

    mo.vstack([
        mo.md("### 13. Share of policies vs. share of claims per vehicle class (Spark SQL)"),
        mo.ui.table(q13_sql.toPandas()),
    ])
    return (q13_sql,)


@app.cell
def _(F, Window, check_same, df, mo, q13_sql):
    # The same query with the DataFrame API
    w_all = Window.partitionBy()          # empty window = OVER ()

    q13_api = (
        df.groupBy("vehicle_class")
        .agg(
            F.count("*").alias("policies"),
            F.sum("total_claim_amount").alias("raw_claims"),
        )
        .select(
            "vehicle_class",
            "policies",
            F.round(100 * F.col("policies") / F.sum("policies").over(w_all), 2).alias("pct_of_policies"),
            F.round("raw_claims", 2).alias("total_claims"),
            F.round(100 * F.col("raw_claims") / F.sum("raw_claims").over(w_all), 2).alias("pct_of_claims"),
        )
        .orderBy(F.desc("total_claims"))
    )

    mo.vstack([
        mo.md("**13. The same query with the DataFrame API**"),
        mo.ui.table(q13_api.toPandas()),
        check_same(q13_sql, q13_api),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Query 14: Customers whose claim is more than 3 times their state's average

    Fraud and pricing teams look for claims far above "normal". "Normal" depends on the state, so we compare
    each customer with **the average of their own state**.

    * `AVG(total_claim_amount) OVER (PARTITION BY state)` puts the state average **on every row**, next to the
      customer's own claim. Without a window you would need a `GROUP BY state` subquery and a `JOIN` back to
      the table; the window does it in one step.
    * The window value cannot be filtered in the same `SELECT`, so it is computed in a CTE first.
    * API: `F.avg("total_claim_amount").over(Window.partitionBy("state"))`.
    """)
    return


@app.cell
def _(mo, spark):
    # Query 14: compare each row with its group average (window AVG)
    q14_sql = spark.sql("""
        WITH with_state_avg AS (
            SELECT
                customer,
                state,
                vehicle_class,
                total_claim_amount,
                AVG(total_claim_amount) OVER (PARTITION BY state) AS state_avg_claim
            FROM insurance
        )
        SELECT
            customer,
            state,
            vehicle_class,
            ROUND(total_claim_amount, 2)                   AS claim,
            ROUND(state_avg_claim, 2)                      AS state_avg_claim,
            ROUND(total_claim_amount / state_avg_claim, 2) AS times_state_avg
        FROM with_state_avg
        WHERE total_claim_amount > 3 * state_avg_claim
        ORDER BY times_state_avg DESC, customer
        LIMIT 10
    """)

    mo.vstack([
        mo.md("### 14. Top 10 claims compared with the state average (Spark SQL)"),
        mo.ui.table(q14_sql.toPandas()),
    ])
    return (q14_sql,)


@app.cell
def _(F, Window, check_same, df, mo, q14_sql):
    # The same query with the DataFrame API
    w_state_all = Window.partitionBy("state")

    q14_api = (
        df.withColumn("state_avg_claim", F.avg("total_claim_amount").over(w_state_all))
        .filter(F.col("total_claim_amount") > 3 * F.col("state_avg_claim"))
        .select(
            "customer",
            "state",
            "vehicle_class",
            F.round("total_claim_amount", 2).alias("claim"),
            F.round("state_avg_claim", 2).alias("state_avg_claim"),
            F.round(F.col("total_claim_amount") / F.col("state_avg_claim"), 2).alias("times_state_avg"),
        )
        .orderBy(F.desc("times_state_avg"), "customer")
        .limit(10)
    )

    mo.vstack([
        mo.md("**14. The same query with the DataFrame API**"),
        mo.ui.table(q14_api.toPandas()),
        check_same(q14_sql, q14_api),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Query 15: Yearly premium by state and coverage, with subtotals and a grand total

    `GROUP BY ROLLUP(state, coverage)` computes three levels in one query:
    (state, coverage) rows, a **subtotal** per state, and one **grand total**. In the subtotal rows the
    rolled-up column is NULL.

    * `GROUPING(coverage)` is 1 when `coverage` was rolled up (a subtotal row) and 0 otherwise. We use it to
      write `'(all coverages)'` / `'ALL STATES'` instead of NULL, and to put each subtotal right after its state.
    * API: `df.rollup("state", "coverage").agg(...)`, with `F.grouping("coverage")`.
    * Why `GROUPING()` and not `COALESCE(coverage, 'ALL')`? `COALESCE` would also relabel a real NULL value in
      the data. This table has no NULLs, but `GROUPING()` is the safe habit.
    """)
    return


@app.cell
def _(mo, spark):
    # Query 15: GROUP BY ROLLUP + GROUPING()
    q15_sql = spark.sql("""
        WITH rolled AS (
            SELECT
                state,
                coverage,
                GROUPING(state)    AS g_state,
                GROUPING(coverage) AS g_coverage,
                COUNT(*)                       AS customers,
                SUM(monthly_premium_auto * 12) AS annual_premium
            FROM insurance
            GROUP BY ROLLUP (state, coverage)
        )
        SELECT
            CASE WHEN g_state = 1    THEN 'ALL STATES'      ELSE state    END AS state,
            CASE WHEN g_coverage = 1 THEN '(all coverages)' ELSE coverage END AS coverage,
            customers,
            annual_premium
        FROM rolled
        ORDER BY g_state, rolled.state, g_coverage, rolled.coverage
    """)

    mo.vstack([
        mo.md("### 15. Annual premium by state & coverage, with subtotals (Spark SQL)"),
        mo.ui.table(q15_sql.toPandas()),
    ])
    return (q15_sql,)


@app.cell
def _(F, check_same, df, mo, q15_sql):
    # The same query with the DataFrame API
    q15_api = (
        df.rollup("state", "coverage")
        .agg(
            F.grouping("state").alias("g_state"),
            F.grouping("coverage").alias("g_coverage"),
            F.count("*").alias("customers"),
            F.sum(F.col("monthly_premium_auto") * 12).alias("annual_premium"),
        )
        .orderBy("g_state", "state", "g_coverage", "coverage")
        .select(
            F.when(F.col("g_state") == 1, "ALL STATES").otherwise(F.col("state")).alias("state"),
            F.when(F.col("g_coverage") == 1, "(all coverages)").otherwise(F.col("coverage")).alias("coverage"),
            "customers",
            "annual_premium",
        )
    )

    mo.vstack([
        mo.md("**15. The same query with the DataFrame API**"),
        mo.ui.table(q15_api.toPandas()),
        check_same(q15_sql, q15_api),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Query 16: The best renewal offer in each state

    "Best row per group" in three steps:

    1. **Aggregate**: response rate per (state, offer).
    2. **Rank inside each state** with `ROW_NUMBER() OVER (PARTITION BY state ORDER BY response_rate_pct DESC, ...)`.
       `ROW_NUMBER` always gives 1, 2, 3, ... (no ties), so exactly one offer gets number 1. The extra sort keys
       (`customers DESC`, `renew_offer_type`) decide ties in a fixed way.
    3. **Keep** `rn = 1`.

    We also put the state's overall response rate next to the winner, using two more windows over the
    *aggregated* rows: `SUM(accepted) OVER (PARTITION BY state)` and `SUM(customers) OVER (PARTITION BY state)`.
    The `lift` column shows how much better the best offer does than the state as a whole.
    """)
    return


@app.cell
def _(mo, spark):
    # Query 16: aggregate, then ROW_NUMBER() per group, keep the first row
    q16_sql = spark.sql("""
        WITH offer_rates AS (
            SELECT
                state,
                renew_offer_type,
                COUNT(*) AS customers,
                SUM(CASE WHEN response = 'Yes' THEN 1 ELSE 0 END) AS accepted
            FROM insurance
            GROUP BY state, renew_offer_type
        ),
        ranked AS (
            SELECT
                state,
                renew_offer_type,
                customers,
                accepted,
                100 * accepted / customers AS response_rate_pct,
                100 * SUM(accepted)  OVER (PARTITION BY state)
                    / SUM(customers) OVER (PARTITION BY state) AS state_rate_pct,
                ROW_NUMBER() OVER (
                    PARTITION BY state
                    ORDER BY 100 * accepted / customers DESC, customers DESC, renew_offer_type
                ) AS rn
            FROM offer_rates
        )
        SELECT
            state,
            renew_offer_type AS best_offer,
            customers,
            accepted,
            ROUND(response_rate_pct, 2)                  AS response_rate_pct,
            ROUND(state_rate_pct, 2)                     AS state_rate_pct,
            ROUND(response_rate_pct / state_rate_pct, 2) AS lift
        FROM ranked
        WHERE rn = 1
        ORDER BY state
    """)

    mo.vstack([
        mo.md("### 16. Best renewal offer per state (Spark SQL)"),
        mo.ui.table(q16_sql.toPandas()),
    ])
    return (q16_sql,)


@app.cell
def _(F, Window, check_same, df, mo, q16_sql):
    # The same query with the DataFrame API
    # Step 1: aggregate
    offer_rates = (
        df.groupBy("state", "renew_offer_type")
        .agg(
            F.count("*").alias("customers"),
            F.sum(F.when(F.col("response") == "Yes", 1).otherwise(0)).alias("accepted"),
        )
        .withColumn("response_rate_pct", 100 * F.col("accepted") / F.col("customers"))
    )

    # Step 2: windows over the aggregated rows
    w_state_rows = Window.partitionBy("state")
    w_state_best = Window.partitionBy("state").orderBy(
        F.desc("response_rate_pct"), F.desc("customers"), "renew_offer_type"
    )

    # Step 3: keep the first row of each state
    q16_api = (
        offer_rates
        .withColumn(
            "state_rate_pct",
            100 * F.sum("accepted").over(w_state_rows) / F.sum("customers").over(w_state_rows),
        )
        .withColumn("rn", F.row_number().over(w_state_best))
        .filter(F.col("rn") == 1)
        .select(
            "state",
            F.col("renew_offer_type").alias("best_offer"),
            "customers",
            "accepted",
            F.round("response_rate_pct", 2).alias("response_rate_pct"),
            F.round("state_rate_pct", 2).alias("state_rate_pct"),
            F.round(F.col("response_rate_pct") / F.col("state_rate_pct"), 2).alias("lift"),
        )
        .orderBy("state")
    )

    mo.vstack([
        mo.md("**16. The same query with the DataFrame API**"),
        mo.ui.table(q16_api.toPandas()),
        check_same(q16_sql, q16_api),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Query 17: Do a few customers cause most of the claims? (deciles and a running total)

    The **Pareto** question: sort customers from the largest claim to the smallest, cut them into 10 equal
    groups (**deciles**), and see what share of all claim dollars each decile causes.

    1. `NTILE(10) OVER (ORDER BY total_claim_amount DESC, customer)` gives each customer a decile 1–10
       (decile 1 = the 10% largest claims). `customer` breaks ties, so every run gives the same deciles.
    2. `GROUP BY decile` sums the claims per decile.
    3. Two windows over the 10 aggregated rows:
       * `SUM(SUM(total_claim_amount)) OVER ()`: all claims, for the percentage.
       * `SUM(SUM(total_claim_amount)) OVER (ORDER BY decile ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`:
         a **running total** (decile 1, then 1+2, then 1+2+3, ...).

    In the API, `rowsBetween(Window.unboundedPreceding, Window.currentRow)` is the same frame.
    Read the last column: "the top 10% / 20% / 30% of customers cause X% of all claim dollars".
    """)
    return


@app.cell
def _(mo, spark):
    # Query 17: NTILE deciles + percent of total + running total
    q17_sql = spark.sql("""
        WITH deciles AS (
            SELECT
                total_claim_amount,
                NTILE(10) OVER (ORDER BY total_claim_amount DESC, customer) AS decile
            FROM insurance
        )
        SELECT
            decile,
            COUNT(*)                                AS customers,
            ROUND(MIN(total_claim_amount), 2)       AS smallest_claim,
            ROUND(SUM(total_claim_amount), 2)       AS decile_claims,
            ROUND(100 * SUM(total_claim_amount)
                  / SUM(SUM(total_claim_amount)) OVER (), 2) AS pct_of_claims,
            ROUND(100 * SUM(SUM(total_claim_amount)) OVER (
                      ORDER BY decile ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                  / SUM(SUM(total_claim_amount)) OVER (), 2) AS cumulative_pct
        FROM deciles
        GROUP BY decile
        ORDER BY decile
    """)

    mo.vstack([
        mo.md("### 17. Claim dollars by customer decile, with a running total (Spark SQL)"),
        mo.ui.table(q17_sql.toPandas()),
    ])
    return (q17_sql,)


@app.cell
def _(F, Window, check_same, df, mo, q17_sql):
    # The same query with the DataFrame API
    w_rank = Window.orderBy(F.desc("total_claim_amount"), "customer")
    w_total = Window.partitionBy()
    w_running = Window.orderBy("decile").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    q17_api = (
        df.withColumn("decile", F.ntile(10).over(w_rank))
        .groupBy("decile")
        .agg(
            F.count("*").alias("customers"),
            F.min("total_claim_amount").alias("raw_min"),
            F.sum("total_claim_amount").alias("raw_sum"),
        )
        .select(
            "decile",
            "customers",
            F.round("raw_min", 2).alias("smallest_claim"),
            F.round("raw_sum", 2).alias("decile_claims"),
            F.round(100 * F.col("raw_sum") / F.sum("raw_sum").over(w_total), 2).alias("pct_of_claims"),
            F.round(
                100 * F.sum("raw_sum").over(w_running) / F.sum("raw_sum").over(w_total), 2
            ).alias("cumulative_pct"),
        )
        .orderBy("decile")
    )

    mo.vstack([
        mo.md("**17. The same query with the DataFrame API**"),
        mo.ui.table(q17_api.toPandas()),
        check_same(q17_sql, q17_api),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    # Summary: Spark SQL ↔ DataFrame API

    | Spark SQL | DataFrame API | Query |
    |---|---|---|
    | `SELECT a, b * 12 AS c` | `select("a", (F.col("b") * 12).alias("c"))` | 6 |
    | `WHERE x IN (...)` | `filter(F.col("x").isin(...))` | 6 |
    | `ORDER BY a DESC, b` · `LIMIT n` | `orderBy(F.desc("a"), "b")` · `limit(n)` | 6 |
    | aggregates without `GROUP BY` | `df.agg(...)` | 5 |
    | `GROUP BY a, b` | `groupBy("a", "b").agg(...)` | 7, 8 |
    | `HAVING cond` | `.filter(cond)` **after** `.agg()` | 8 |
    | `CASE WHEN ... THEN ... ELSE ... END` | `F.when(...).when(...).otherwise(...)` | 4, 9 |
    | `SUM(CASE WHEN cond THEN 1 ELSE 0 END)` | `F.sum(F.when(cond, 1).otherwise(0))` | 2, 12 |
    | `NULLIF(x, 0)` | `F.nullif(x, F.lit(0))` | 1 |
    | `WITH name AS (...)` (CTE) | a Python variable holding a DataFrame | 2, 16 |
    | scalar subquery `(SELECT AVG(x) FROM t)` | `df.agg(F.avg("x")).first()[0]`, then `F.lit(...)` | 11 |
    | `PIVOT (AGG(x) FOR c IN (...))` | `groupBy(...).pivot("c", [...]).agg(...)` | 10 |
    | `GROUP BY ROLLUP (a, b)` · `GROUPING(a)` | `rollup("a", "b")` · `F.grouping("a")` | 15 |
    | `f() OVER (PARTITION BY a ORDER BY b)` | `f().over(Window.partitionBy("a").orderBy("b"))` | 3, 14, 16 |
    | `SUM(...) OVER ()` | `F.sum(...).over(Window.partitionBy())` | 13, 17 |
    | `ROW_NUMBER()` / `DENSE_RANK()` / `NTILE(n)` | `F.row_number()` / `F.dense_rank()` / `F.ntile(n)` | 3, 16, 17 |
    | `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` | `.rowsBetween(Window.unboundedPreceding, Window.currentRow)` | 17 |

    **Good habits seen in this notebook**

    * Always add a **tie-breaker** to `ORDER BY` (and to a window's `ORDER BY`) when values can repeat;
      otherwise "top 10" or "rank 1" can change from one run to the next.
    * Round only at the **end**: compute with full precision, then `ROUND` the numbers you show.
    * Collect (`toPandas()`, `collect()`, `first()`) only **small, aggregated** results.
    """)
    return


if __name__ == "__main__":
    app.run()
