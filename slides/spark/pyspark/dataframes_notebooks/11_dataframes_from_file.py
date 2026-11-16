import marimo

__generated_with = "0.24.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    from pathlib import Path
    from pyspark.sql import SparkSession, Row
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        IntegerType,
        DoubleType,
    )

    return (
        DoubleType,
        F,
        IntegerType,
        Path,
        Row,
        SparkSession,
        StringType,
        StructField,
        StructType,
        Window,
        mo,
    )


@app.cell
def _(mo):
    mo.md("""
    # 🎓 PySpark DataFrames: From A to Z (CSV Ingestion & RDD Interoperability)

    ### Why Move from RDDs to DataFrames?
    In an **RDD** (`RDD[String]` or `RDD[Tuple]`), reading a CSV meant calling `sc.textFile(...)`, filtering out the header row manually, splitting strings on commas, and writing `try/except` blocks to cast numbers. Spark could not see inside your Python lambdas, preventing query optimization.

    With **DataFrames**, Spark parses CSV rows directly inside the JVM using an explicit `StructType` schema and compiles every query through:
    1. **Catalyst Optimizer:** Pushes filters down to the CSV reader, prunes unused columns, and optimizes join strategies.
    2. **Tungsten Execution Engine:** Stores rows in compact off-heap binary memory and generates whole-stage JVM bytecode.

    **Classroom Rule:** We never call `.toPandas()` or `.collect()` on large datasets. All transformations remain lazy in the JVM until an **Action** (`.show()`, `.count()`, `.write`) executes.
    """)
    return


@app.cell
def _(SparkSession):
    # =========================================================================
    # SECTION A: Initializing the SparkSession
    # =========================================================================
    spark = (
        SparkSession.builder
        .appName("PySparkDataFramesCSV_AtoZ")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    return (spark,)


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Part 1: Reading `employees_1000.csv` with an Explicit Schema
    Notice two crucial real-world CSV details handled in the cell below:
    1. **Zero-Inference Schema (`StructType`):** Passing `.schema(csv_schema)` prevents Spark from scanning the CSV twice just to guess column types.
    2. **Parsing Delimited Strings into Arrays (`F.split`):** CSVs are flat text files and cannot natively store arrays. `skills` arrives as `"Python|Spark|Scala"`, which we immediately parse into a structured `ArrayType(StringType())` using `F.split()`.
    """)
    return


@app.cell
def _(
    DoubleType,
    F,
    IntegerType,
    Path,
    StringType,
    StructField,
    StructType,
    spark,
):
    EMPLOYEE_DATA_PATH = "./data/employees_1000.csv"
    csv_path = Path(EMPLOYEE_DATA_PATH).resolve()
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Could not find {csv_path}. Place 'employees_1000.csv' in the same directory as this notebook."
        )

    # 1. Define the flat CSV schema matching the 7 columns in employees_1000.csv
    csv_schema = StructType([
        StructField("emp_id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("dept_id", StringType(), nullable=True),
        StructField("role", StringType(), nullable=True),
        StructField("salary", DoubleType(), nullable=True),
        StructField("years_exp", IntegerType(), nullable=True),
        StructField("skills_raw", StringType(), nullable=True),
    ])

    # 2. Read CSV directly into JVM Tungsten memory & convert pipe-delimited skills into an ArrayType
    emp_df = (
        spark.read
        .option("header", "true")
        .option("nullValue", "")
        .schema(csv_schema)
        .csv(str(csv_path))
        .withColumn("skills", F.split(F.col("skills_raw"), "\\|"))
        .drop("skills_raw")
    )

    emp_df.cache()
    emp_df.createOrReplaceTempView("employees")

    print(f"Loaded {emp_df.count():,} rows from {csv_path.name}")
    emp_df.printSchema()
    emp_df.show(10, truncate=False)
    return (emp_df,)


@app.cell
def _(StringType, StructField, StructType, spark):
    # Create the department dimension DataFrame for Join demonstrations
    dept_schema = StructType([
        StructField("dept_id", StringType(), nullable=False),
        StructField("dept_name", StringType(), nullable=False),
        StructField("location", StringType(), nullable=False),
    ])

    raw_depts = [
        ("ENG", "Engineering", "Santa Clara"),
        ("DAT", "Data Science", "San Francisco"),
        ("MKT", "Marketing", "New York"),
        ("FIN", "Finance", "Chicago"),
        ("HR", "Human Resources", "Austin"),
        ("OPS", "Operations", "Seattle"),
    ]

    dept_df = spark.createDataFrame(data=raw_depts, schema=dept_schema)
    dept_df.createOrReplaceTempView("departments")
    dept_df.show(truncate=False)
    return (dept_df,)


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Part 2: Transformations — Projection, Filtering & Conditional Columns
    Just like RDD `.map()` and `.filter()`, DataFrame transformations are **lazy**—they build a Logical Plan without moving data until an **Action** (`.show()`, `.count()`) is called.
    * **SQL:** `SELECT`, `WHERE`, `CASE WHEN`
    * **Functional API:** `.select()`, `.filter()` (or `.where()`), `.withColumn()`, `F.col()`, `F.when()`
    """)
    return


@app.cell
def _(spark):
    # [2A - SPARK SQL] Projection, Filtering & Conditional Logic
    sql_proj_df = spark.sql("""
        SELECT 
            emp_id,
            name,
            dept_id,
            salary,
            ROUND(salary * 1.10, 2) AS projected_salary,
            CASE 
                WHEN years_exp >= 12 THEN 'Principal/Staff Tier'
                WHEN years_exp >= 6 THEN 'Senior Tier'
                ELSE 'Mid/Junior Tier'
            END AS career_tier
        FROM employees
        WHERE salary >= 150000 AND dept_id IS NOT NULL
        ORDER BY salary DESC
    """)

    print("=== [SQL] Filtered & Projected High Earners ===")
    sql_proj_df.show(10, truncate=False)
    return (sql_proj_df,)


@app.cell
def _(F, emp_df):
    # [2B - FUNCTIONAL DATAFRAME API] Exact Equivalent of 2A
    func_proj_df = (
        emp_df
        .filter((F.col("salary") >= 150000) & F.col("dept_id").isNotNull())
        .withColumn("projected_salary", F.round(F.col("salary") * 1.10, 2))
        .withColumn(
            "career_tier",
            F.when(F.col("years_exp") >= 12, "Principal/Staff Tier")
             .when(F.col("years_exp") >= 6, "Senior Tier")
             .otherwise("Mid/Junior Tier")
        )
        .select("emp_id", "name", "dept_id", "salary", "projected_salary", "career_tier")
        .orderBy(F.col("salary").desc())
    )

    print("=== [Functional API] Filtered & Projected High Earners ===")
    func_proj_df.show(10, truncate=False)
    return (func_proj_df,)


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Part 3: Under the Hood — Catalyst Execution Plans (`.explain()`)
    Students coming from RDDs often ask whether Spark SQL is faster than the Functional DataFrame API.
    Inspecting `.explain()` proves that both APIs compile down to the **exact same Catalyst Physical Plan and Tungsten bytecode**.
    """)
    return


@app.cell
def _(func_proj_df, sql_proj_df):
    print("--- Catalyst Physical Plan (SQL) ---")
    sql_proj_df.explain()

    print("--- Catalyst Physical Plan (Functional API) ---")
    func_proj_df.explain()
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Part 4: Handling Missing Values (`NULL`) & Exploding Arrays
    About 3% of rows in `employees_1000.csv` have a `NULL` `dept_id`. Below we coalesce missing departments and explode each employee's `skills` array into one row per skill to find the most common skills across the company.
    """)
    return


@app.cell
def _(spark):
    # [4A - SPARK SQL] Null Coalescing & Exploding Arrays
    sql_skills_df = spark.sql("""
        SELECT 
            EXPLODE(skills) AS skill,
            COUNT(*) AS employee_count
        FROM employees
        GROUP BY 1
        ORDER BY employee_count DESC
    """)

    print("=== [SQL] Top Skills Across 1,000 Employees ===")
    sql_skills_df.show(10, truncate=False)
    return


@app.cell
def _(F, emp_df):
    # [4B - FUNCTIONAL DATAFRAME API] .na.fill(), F.explode(), and .groupBy()
    func_skills_df = (
        emp_df
        .na.fill({"dept_id": "UNASSIGNED"})
        .select(F.explode(F.col("skills")).alias("skill"))
        .groupBy("skill")
        .agg(F.count("*").alias("employee_count"))
        .orderBy(F.col("employee_count").desc())
    )

    print("=== [Functional API] Top Skills Across 1,000 Employees ===")
    func_skills_df.show(10, truncate=False)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Part 5: Aggregations (`GROUP BY` & `HAVING`)
    In RDDs, computing averages required `.mapValues(lambda v: (v, 1)).reduceByKey(...)` to avoid `groupByKey()` memory spikes.
    In DataFrames, `.groupBy().agg(...)` automatically performs **partial map-side aggregation** before shuffling across the network.
    """)
    return


@app.cell
def _(spark):
    # [5A - SPARK SQL] Departmental Salary & Experience Aggregations
    sql_agg_df = spark.sql("""
        SELECT 
            COALESCE(dept_id, 'UNASSIGNED') AS dept_id,
            COUNT(emp_id) AS headcount,
            ROUND(AVG(salary), 2) AS avg_salary,
            ROUND(MIN(salary), 2) AS min_salary,
            ROUND(MAX(salary), 2) AS max_salary,
            ROUND(AVG(years_exp), 1) AS avg_years_exp
        FROM employees
        GROUP BY COALESCE(dept_id, 'UNASSIGNED')
        HAVING COUNT(emp_id) >= 20
        ORDER BY avg_salary DESC
    """)

    print("=== [SQL] Department Aggregations ===")
    sql_agg_df.show(truncate=False)
    return


@app.cell
def _(F, emp_df):
    # [5B - FUNCTIONAL DATAFRAME API] .groupBy().agg() + Post-Aggregation Filter
    func_agg_df = (
        emp_df
        .withColumn("clean_dept", F.coalesce(F.col("dept_id"), F.lit("UNASSIGNED")))
        .groupBy("clean_dept")
        .agg(
            F.count("emp_id").alias("headcount"),
            F.round(F.avg("salary"), 2).alias("avg_salary"),
            F.round(F.min("salary"), 2).alias("min_salary"),
            F.round(F.max("salary"), 2).alias("max_salary"),
            F.round(F.avg("years_exp"), 1).alias("avg_years_exp"),
        )
        .filter(F.col("headcount") >= 20)
        .orderBy(F.col("avg_salary").desc())
    )

    print("=== [Functional API] Department Aggregations ===")
    func_agg_df.show(truncate=False)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Part 6: Joins (`LEFT OUTER` & Broadcast Hash Joins)
    Catalyst automatically detects when one table (`departments`, 6 rows) is small and executes a **Broadcast Hash Join**—sending a read-only copy of `departments` to every executor so the larger `employees` DataFrame never has to be shuffled.
    """)
    return


@app.cell
def _(spark):
    # [6A - SPARK SQL] Left Outer Join
    sql_join_df = spark.sql("""
        SELECT 
            e.emp_id,
            e.name,
            e.role,
            COALESCE(d.dept_name, 'Unassigned') AS department,
            COALESCE(d.location, 'Remote') AS office_location,
            e.salary
        FROM employees e
        LEFT JOIN departments d
            ON e.dept_id = d.dept_id
        ORDER BY e.emp_id
    """)

    print("=== [SQL] Left Outer Join ===")
    sql_join_df.show(10, truncate=False)
    return


@app.cell
def _(F, dept_df, emp_df):
    # [6B - FUNCTIONAL DATAFRAME API] Left Outer Join with Explicit Broadcast Hint
    func_join_df = (
        emp_df.alias("e")
        .join(
            F.broadcast(dept_df).alias("d"),
            on=F.col("e.dept_id") == F.col("d.dept_id"),
            how="left",
        )
        .select(
            F.col("e.emp_id"),
            F.col("e.name"),
            F.col("e.role"),
            F.coalesce(F.col("d.dept_name"), F.lit("Unassigned")).alias("department"),
            F.coalesce(F.col("d.location"), F.lit("Remote")).alias("office_location"),
            F.col("e.salary"),
        )
        .orderBy("emp_id")
    )

    print("=== [Functional API] Broadcast Left Join ===")
    func_join_df.show(10, truncate=False)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Part 7: Window Functions (Top-N Per Group Without Collapsing Rows)
    Unlike `GROUP BY`, **Window Functions** compute rankings and departmental benchmarks while keeping every employee row intact—making it effortless to extract the **Top 3 highest-paid employees per department**.
    """)
    return


@app.cell
def _(spark):
    # [7A - SPARK SQL] Top 3 Highest-Paid Employees per Department
    sql_win_df = spark.sql("""
        WITH ranked AS (
            SELECT 
                name,
                dept_id,
                role,
                salary,
                DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS dept_salary_rank,
                ROUND(AVG(salary) OVER (PARTITION BY dept_id), 2) AS dept_avg_salary
            FROM employees
            WHERE dept_id IS NOT NULL
        )
        SELECT 
            dept_id,
            dept_salary_rank,
            name,
            role,
            salary,
            dept_avg_salary,
            ROUND(salary - dept_avg_salary, 2) AS above_dept_avg
        FROM ranked
        WHERE dept_salary_rank <= 3
        ORDER BY dept_id, dept_salary_rank
    """)

    print("=== [SQL] Top 3 Earners per Department ===")
    sql_win_df.show(18, truncate=False)
    return


@app.cell
def _(F, Window, emp_df):
    # [7B - FUNCTIONAL DATAFRAME API] Window Specification + .over(window_spec)
    dept_salary_window = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())
    dept_partition_only = Window.partitionBy("dept_id")

    func_win_df = (
        emp_df
        .filter(F.col("dept_id").isNotNull())
        .withColumn("dept_salary_rank", F.dense_rank().over(dept_salary_window))
        .withColumn("dept_avg_salary", F.round(F.avg("salary").over(dept_partition_only), 2))
        .filter(F.col("dept_salary_rank") <= 3)
        .withColumn("above_dept_avg", F.round(F.col("salary") - F.col("dept_avg_salary"), 2))
        .select("dept_id", "dept_salary_rank", "name", "role", "salary", "dept_avg_salary", "above_dept_avg")
        .orderBy("dept_id", "dept_salary_rank")
    )

    print("=== [Functional API] Top 3 Earners per Department ===")
    func_win_df.show(18, truncate=False)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Part 8: User-Defined Functions (UDFs) & Serialization Trade-Offs
    * **Performance Note:** Standard Python UDFs (`@F.udf`) serialize rows out of Tungsten JVM memory into a Python worker process and back. Always prefer native `pyspark.sql.functions` (`F.*`) when possible, and reserve UDFs for domain logic that SQL expressions cannot represent cleanly.
    """)
    return


@app.cell
def _(F, StringType, emp_df, spark):
    @F.udf(returnType=StringType())
    def skill_badge_udf(skills_list):
        if not skills_list:
            return "Generalist"
        if "Spark" in skills_list and "Python" in skills_list:
            return "Big Data Stack"
        if "Spark" in skills_list:
            return "Distributed Systems"
        return "Domain Specialist"

    spark.udf.register("skill_badge", skill_badge_udf)

    # [8A - SPARK SQL]
    sql_udf_df = spark.sql("""
        SELECT name, dept_id, skills, skill_badge(skills) AS badge
        FROM employees
    """)
    print("=== [SQL] Custom UDF ===")
    sql_udf_df.show(8, truncate=False)

    # [8B - FUNCTIONAL DATAFRAME API]
    func_udf_df = emp_df.select(
        F.col("name"),
        F.col("dept_id"),
        F.col("skills"),
        skill_badge_udf(F.col("skills")).alias("badge"),
    )
    print("=== [Functional API] Custom UDF ===")
    func_udf_df.show(8, truncate=False)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Part 9: Interoperability — DataFrame $\leftrightarrow$ RDD Round-Trip Conversions
    Because DataFrames sit on top of RDDs, you can drop down to the RDD layer whenever you need custom unstructured parsing or fine-grained partition control, and lift the result back into a structured DataFrame:

    1. **DataFrame $
    ightarrow$ RDD (`df.rdd`):** Yields an `RDD[Row]`. Each `Row` supports both attribute access (`row.salary`) and positional indexing (`row[4]`).
    2. **RDD $
    ightarrow$ DataFrame (Two Ways):**
       * **Quick Inference (`.toDF(["col1", "col2"])`):** Convenient for interactive exploration; Spark scans a sample of the RDD to infer types.
       * **Explicit Schema (`spark.createDataFrame(rdd, schema)`):** Production best practice; enforces exact types without scanning data twice.
    """)
    return


@app.cell
def _(emp_df):
    # [9A - DataFrame -> RDD] Extracting the underlying RDD[Row]
    emp_rdd = emp_df.rdd

    print(f"1. RDD Type: {type(emp_rdd)}")
    print(f"2. Number of Partitions: {emp_rdd.getNumPartitions()}")

    first_row = emp_rdd.first()
    print(f"3. First Element (Row): {first_row}")
    print(f"   -> Access by field name: name={first_row.name}, salary={first_row.salary}")
    print(f"   -> Access by index:      row[1]={first_row[1]}, row[4]={first_row[4]}")
    return (emp_rdd,)


@app.cell
def _(emp_rdd):
    # [9B - RDD Transformations] Performing classic Pair-RDD reduceByKey() operations
    dept_payroll_rdd = (
        emp_rdd
        .filter(lambda row: row.dept_id is not None)
        .map(lambda row: (row.dept_id, (row.salary, 1)))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .map(lambda kv: (kv[0], kv[1][1], round(kv[1][0], 2), round(kv[1][0] / kv[1][1], 2)))
    )

    print("=== [RDD] Computed Tuple Output (dept_id, headcount, total_payroll, avg_salary) ===")
    for record in dept_payroll_rdd.take(6):
        print("  ", record)
    return (dept_payroll_rdd,)


@app.cell
def _(
    DoubleType,
    IntegerType,
    Row,
    StringType,
    StructField,
    StructType,
    dept_payroll_rdd,
    spark,
):
    # [9C - RDD -> DataFrame] Converting the RDD back into a DataFrame

    # Method 1: Quick conversion using .toDF(column_names) -> Spark infers column types
    inferred_df = dept_payroll_rdd.toDF(["dept_id", "headcount", "total_payroll", "avg_salary"])
    print("=== [RDD -> DF Method 1: .toDF() with Type Inference] ===")
    inferred_df.printSchema()
    inferred_df.show()

    # Method 2: Production conversion using Row objects + explicit StructType schema (No inference scan)
    payroll_schema = StructType([
        StructField("dept_id", StringType(), nullable=False),
        StructField("headcount", IntegerType(), nullable=False),
        StructField("total_payroll", DoubleType(), nullable=False),
        StructField("avg_salary", DoubleType(), nullable=False),
    ])

    row_rdd = dept_payroll_rdd.map(
        lambda t: Row(dept_id=t[0], headcount=t[1], total_payroll=t[2], avg_salary=t[3])
    )
    explicit_df = spark.createDataFrame(row_rdd, schema=payroll_schema)

    print("=== [RDD -> DF Method 2: createDataFrame() with Explicit Schema] ===")
    explicit_df.printSchema()
    explicit_df.show()

    # Optional: Export summary DataFrame to Parquet
    # explicit_df.write.mode("overwrite").parquet("./output/dept_payroll_summary.parquet")
    return


if __name__ == "__main__":
    app.run()
