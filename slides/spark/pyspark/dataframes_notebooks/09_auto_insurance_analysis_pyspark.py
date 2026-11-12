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

    return Path, SparkSession, duckdb, mo


@app.cell
def _(mo):
    mo.md("""
    # Auto Insurance Analytics: DuckDB $\rightarrow$ PySpark

    This notebook extracts the `insurance` table from DuckDB using native multi-threaded **Parquet** export (bypassing Pandas and single-threaded JDBC bottlenecks) and loads it into **PySpark** for distributed SQL analysis.
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
        ORDER BY annualized_loss_ratio DESC
    """)

    mo.vstack([
        mo.md("### 1. Annualized Loss Ratio by Coverage & Vehicle Class"),
        mo.ui.table(q1_df.toPandas()) # Safe to collect small aggregated results for UI display
    ])
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
        ORDER BY state, state_risk_rank
    """)

    mo.vstack([
        mo.md("### 3. Top 3 Highest Claim-to-Premium Ratio Outliers per State"),
        mo.ui.table(q3_df.toPandas())
    ])
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
    return


if __name__ == "__main__":
    app.run()
