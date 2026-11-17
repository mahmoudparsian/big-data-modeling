import marimo

__generated_with = "0.24.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    from pyspark.sql import SparkSession, Row
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        IntegerType,
        DoubleType,
        ArrayType,
    )

    return (
        ArrayType,
        DoubleType,
        F,
        IntegerType,
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
    # 🎓 PySpark DataFrames: From A to Z (Transitioning from RDDs)

    ### Why Move from RDDs to DataFrames?
    In an **RDD** (`RDD[Row]` or `RDD[Tuple]`), Spark treats your data as opaque Python/JVM objects. Spark cannot see inside a Python lambda function, which prevents query optimization and forces expensive serialization between Python and the JVM.

    A **DataFrame** is a distributed collection of data organized into **named columns** with a strict **Schema**—conceptually equivalent to a relational database table. Because Spark knows the exact column names and data types upfront, every query passes through two engines before executing:
    1. **Catalyst Optimizer:** Rewrites your logical plan (pushing filters down early, pruning unused columns, reordering joins).
    2. **Tungsten Execution Engine:** Stores data in compact off-heap binary memory (avoiding JVM garbage collection) and generates whole-stage JVM bytecode.

    **Golden Rule of this Notebook:**

    1. We never call `.toPandas()` or `.collect()` on large datasets.

    2. All transformations stay lazily evaluated in the JVM until an **Action** (`.show()`, `.count()`, `.write`) is triggered.
    """)
    return


@app.cell
def _(SparkSession):
    # =========================================================================
    # SECTION A: Initializing the SparkSession
    # =========================================================================
    # 1. In the RDD era, we created a SparkContext (sc).
    #
    # 2. In modern Spark, SparkSession is the 
    #    unified entry point for DataFrames
    spark = (
        SparkSession.builder
        .appName("PySparkDataFramesAtoZ")
        .master("local[*]")  # Uses all available CPU cores locally
        .config("spark.sql.shuffle.partitions", "4")  # Small partition count for fast classroom demos
        .getOrCreate()
    )

    print("spark.version=", spark.version)
    return (spark,)


@app.cell
def _():
    return


@app.cell
def _(spark):
    print("spark=", spark)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Part 1: Schemas & DataFrame Creation (Explicit vs. Inferred)
    With RDDs, parsing a CSV required `.map(lambda line: line.split(","))` and manual type casting (`int(x[2])`).
    With DataFrames, we define a `StructType` schema so Spark validates types at read time in C++/JVM memory without scanning the dataset twice.
    """)
    return


@app.cell
def _(
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    spark,
):
    # Define an explicit schema (Best Practice for production & large datasets)
    employee_schema = StructType([
        StructField("emp_id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("dept_id", StringType(), nullable=True),
        StructField("role", StringType(), nullable=True),
        StructField("salary", DoubleType(), nullable=True),
        StructField("years_exp", IntegerType(), nullable=True),
        StructField("skills", ArrayType(StringType()), nullable=True),
    ])

    raw_employees = [
        (101, "Alice Chen", "ENG", "Staff Engineer", 185000.0, 9, ["Python", "Spark", "Scala"]),
        (102, "Bob Martinez", "ENG", "Senior Engineer", 155000.0, 6, ["Java", "Spark", "Kafka"]),
        (103, "Charlie Kim", "ENG", "Software Engineer", 125000.0, 2, ["Python", "SQL"]),
        (104, "Diana Patel", "DAT", "Principal Data Scientist", 195000.0, 11, ["Python", "PyTorch", "Spark"]),
        (105, "Evan Wright", "DAT", "Data Analyst", 98000.0, 3, ["SQL", "Tableau"]),
        (106, "Fiona Lopez", "MKT", "Marketing Director", 145000.0, 8, ["Strategy", "Analytics"]),
        (107, "George Wu", "MKT", "Growth Specialist", 92000.0, 2, ["SEO", "SQL"]),
        (108, "Hannah Abbott", "ENG", "Senior Engineer", 160000.0, 7, ["Go", "Kubernetes", "Spark"]),
        (109, "Ian Malcolm", None, "Consultant", 130000.0, 10, ["Chaos Theory"]),
    ]

    # Create the DataFrame 
    emp_df = spark.createDataFrame(data=raw_employees, schema=employee_schema)

    print("emp_df=", emp_df)
    return (emp_df,)


@app.cell
def _(emp_df):
    # Inspect schema tree and display first 10 rows
    emp_df.printSchema()
    emp_df.show(truncate=False)
    return


@app.cell
def _(emp_df):
    # Register a Temporary View for Spark SQL
    emp_df.createOrReplaceTempView("employees")
    return


@app.cell
def _(StringType, StructField, StructType, spark):
    # Create a second lookup DataFrame for Join demonstrations later
    dept_schema = StructType([
        StructField("dept_id", StringType(), nullable=False),
        StructField("dept_name", StringType(), nullable=False),
        StructField("location", StringType(), nullable=False),
    ])

    raw_depts = [
        ("ENG", "Engineering", "Santa Clara"),
        ("DAT", "Data Science", "San Francisco"),
        ("MKT", "Marketing", "New York"),
        ("HR", "Human Resources", "Austin"),
    ]

    dept_df = spark.createDataFrame(data=raw_depts, schema=dept_schema)
    dept_df.createOrReplaceTempView("departments")
    dept_df.show()
    return (dept_df,)


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Part 2: Transformations — Projection, Filtering & Adding Columns
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
                WHEN years_exp >= 8 THEN 'Principal/Staff Tier'
                WHEN years_exp >= 5 THEN 'Senior Tier'
                ELSE 'Mid/Junior Tier'
            END AS career_tier
        FROM employees
        WHERE salary >= 120000 AND dept_id IS NOT NULL
        ORDER BY salary DESC
    """)

    print("=== [SQL] Filtered & Projected Employees ===")
    sql_proj_df.show(truncate=False)
    return (sql_proj_df,)


@app.cell
def _(F, emp_df):
    # [2B - FUNCTIONAL DATAFRAME API] Exact Equivalent of 2A
    # Notice how F.col("...") creates a Column expression object that Catalyst optimizes.
    func_proj_df = (
        emp_df
        .filter((F.col("salary") >= 120000) & F.col("dept_id").isNotNull())
        .withColumn("projected_salary", F.round(F.col("salary") * 1.10, 2))
        .withColumn(
            "career_tier",
            F.when(F.col("years_exp") >= 8, "Principal/Staff Tier")
             .when(F.col("years_exp") >= 5, "Senior Tier")
             .otherwise("Mid/Junior Tier")
        )
        .select("emp_id", "name", "dept_id", "salary", "projected_salary", "career_tier")
        .orderBy(F.col("salary").desc())
    )

    print("=== [Functional API] Filtered & Projected Employees ===")
    func_proj_df.show(truncate=False)
    return (func_proj_df,)


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Part 3: Under the Hood — Catalyst Execution Plans (`.explain()`)
    Students coming from RDDs often worry whether SQL is faster than the Functional API or vice versa.
    Let's inspect the **Physical Plan** of both `sql_proj_df` and `func_proj_df` using `.explain()`. Both compile to the **exact same Catalyst plan and Tungsten bytecode**.
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
    ## Part 4: Handling Missing Values (`NULL`) & Complex Types (`ArrayType`)
    Unlike flat CSVs or basic RDD tuples, DataFrames natively support nested types (`ArrayType`, `MapType`, `StructType`) and dedicated `na` (null-handling) operators.
    """)
    return


@app.cell
def _(spark):
    # [4A - SPARK SQL] Null Coalescing & Exploding Arrays
    sql_complex_df = spark.sql("""
        SELECT 
            name,
            COALESCE(dept_id, 'UNASSIGNED') AS clean_dept,
            EXPLODE(skills) AS individual_skill
        FROM employees
    """)

    print("=== [SQL] Null Coalescing & Exploded Skills ===")
    sql_complex_df.show(12, truncate=False)
    return


@app.cell
def _(F, emp_df):
    # [4B - FUNCTIONAL DATAFRAME API] .na.fill() and F.explode()
    func_complex_df = (
        emp_df
        .na.fill({"dept_id": "UNASSIGNED"})
        .select(
            F.col("name"),
            F.col("dept_id").alias("clean_dept"),
            F.explode(F.col("skills")).alias("individual_skill")
        )
    )

    print("=== [Functional API] Null Fill & Exploded Skills ===")
    func_complex_df.show(12, truncate=False)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Part 5: Aggregations (`GROUP BY` & `HAVING`)
    In RDDs, computing averages required `.mapValues(lambda v: (v, 1)).reduceByKey(...)` to avoid `groupByKey()` out-of-memory errors.
    In DataFrames, `.groupBy().agg(...)` automatically performs **partial map-side aggregation** (like `combineByKey`) before shuffling across the network!
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
            MAX(salary) AS max_salary,
            ROUND(AVG(years_exp), 1) AS avg_years_exp
        FROM employees
        GROUP BY COALESCE(dept_id, 'UNASSIGNED')
        HAVING COUNT(emp_id) >= 2
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
            F.max("salary").alias("max_salary"),
            F.round(F.avg("years_exp"), 1).alias("avg_years_exp"),
        )
        .filter(F.col("headcount") >= 2)  # Equivalent to SQL HAVING
        .orderBy(F.col("avg_salary").desc())
    )

    print("=== [Functional API] Department Aggregations ===")
    func_agg_df.show(truncate=False)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Part 6: Joins (`INNER`, `LEFT OUTER`, and Broadcast Hints)
    In RDDs, joining two Pair RDDs required `rdd1.join(rdd2)` and manual key alignment.
    In DataFrames, Catalyst automatically detects when one table (`departments`) is small and executes a **Broadcast Hash Join**—sending a copy of `departments` to every executor so `employees` never needs to be shuffled!
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
    sql_join_df.show(truncate=False)
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
    func_join_df.show(truncate=False)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Part 7: Window Functions (Analytics Without Collapsing Rows)
    Unlike `GROUP BY` (which collapses many rows into one summary row per group), **Window Functions** compute rankings, running totals, and group comparisons while preserving every individual employee row.
    """)
    return


@app.cell
def _(spark):
    # [7A - SPARK SQL] Departmental Salary Ranking & Comparison to Dept Average
    sql_win_df = spark.sql("""
        SELECT 
            name,
            dept_id,
            salary,
            RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) 
                AS dept_salary_rank,
            DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) 
                AS dept_salary_dense_rank,            
            ROUND(AVG(salary) OVER (PARTITION BY dept_id), 2) AS dept_avg_salary,
            ROUND(salary - AVG(salary) OVER (PARTITION BY dept_id), 2) AS diff_from_dept_avg
        FROM employees
        WHERE dept_id IS NOT NULL
        ORDER BY dept_id, dept_salary_dense_rank
    """)

    print("=== [SQL] Window Functions ===")
    sql_win_df.show(truncate=False)
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
        .withColumn("diff_from_dept_avg", F.round(F.col("salary") - F.col("dept_avg_salary"), 2))
        .select("name", "dept_id", "salary", "dept_salary_rank", "dept_avg_salary", "diff_from_dept_avg")
        .orderBy("dept_id", "dept_salary_rank")
    )

    print("=== [Functional API] Window Functions ===")
    func_win_df.show(truncate=False)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Part 8: User-Defined Functions (UDFs) & Why Native Functions Win
    When students transition from RDDs, their first instinct is to write custom Python functions (`@F.udf`) for everything.
    * **Performance Warning:** Standard Python UDFs force Spark to serialize rows out of Tungsten JVM memory into a separate Python worker process and back. Always prefer built-in `pyspark.sql.functions` (`F.*`) first, and only use UDFs when built-in expressions cannot express the logic.
    """)
    return


@app.cell
def _(F, StringType, emp_df, spark):
    # Define a custom Python UDF
    @F.udf(returnType=StringType())
    def skill_badge_udf(skills_list):
        if not skills_list:
            return "Generalist"
        if "Spark" in skills_list and "Python" in skills_list:
            return "Big Data Stack"
        if "Spark" in skills_list:
            return "Distributed Systems"
        return "Domain Specialist"

    # Register UDF so Spark SQL can also invoke it
    spark.udf.register("skill_badge", skill_badge_udf)

    # [8A - SPARK SQL] Using the registered UDF
    sql_udf_df = spark.sql("""
        SELECT name, skills, skill_badge(skills) AS badge
        FROM employees
    """)
    print("=== [SQL] Custom UDF ===")
    sql_udf_df.show(truncate=False)

    # [8B - FUNCTIONAL DATAFRAME API] Using the Python UDF object directly
    func_udf_df = emp_df.select(
        F.col("name"),
        F.col("skills"),
        skill_badge_udf(F.col("skills")).alias("badge")
    )
    print("=== [Functional API] Custom UDF ===")
    func_udf_df.show(truncate=False)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Part 9: Interoperability — DataFrame $\leftrightarrow$ RDD Round-Trip Conversions
    Because DataFrames are built directly on top of RDDs, you can drop down to the RDD layer whenever you need custom unstructured parsing or fine-grained partition control, and then lift the result back into a structured DataFrame:

    1. **DataFrame $
    ightarrow$ RDD (`df.rdd`):** Yields an `RDD[Row]`. Each `Row` supports both attribute access (`row.salary`) and positional indexing (`row[4]`). Note that accessing `.rdd` deserializes Tungsten off-heap bytes back into Python `Row` objects.
    2. **RDD $
    ightarrow$ DataFrame (Two Ways):**
       * **Quick Inference (`.toDF(["col1", "col2"])`):** Convenient for interactive exploration; Spark scans a sample of the RDD to infer data types.
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
    # Goal: Compute total payroll and headcount per department using low-level RDD API
    dept_payroll_rdd = (
        emp_rdd
        .filter(lambda row: row.dept_id is not None)
        .map(lambda row: (row.dept_id, (row.salary, 1)))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .map(lambda kv: (kv[0], kv[1][1], round(kv[1][0], 2), round(kv[1][0] / kv[1][1], 2)))
    )

    print("=== [RDD] Computed Tuple Output (dept_id, headcount, total_payroll, avg_salary) ===")
    for record in dept_payroll_rdd.take(5):
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

    # Writing a DataFrame out to columnar Parquet format (commented out so disk stays clean)
    # explicit_df.write.mode("overwrite").parquet("./output/dept_payroll_summary.parquet")
    return


if __name__ == "__main__":
    app.run()
