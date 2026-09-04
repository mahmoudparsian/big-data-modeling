# User-Defined Functions (UDF) in PySpark

This article shows how to use Python user-defined functions (UDFs)
in PySpark applications. To use a UDF, there are three basic steps:

1. Create a UDF in Python.
2. Turn it into something Spark can call — wrap it with `udf()` for
   the DataFrame API (or combine steps 1 and 2 with the `@udf`
   decorator), and/or register it with `spark.udf.register()` for use
   in Spark SQL.
3. Use the UDF in a DataFrame expression or a SQL query.

## Files in This Folder

| Name | Description |
|---|---|
| [`dataframe_UDF_example.py`](./dataframe_UDF_example.py) | Runnable PySpark script demonstrating all three UDF styles |
| [`dataframe_UDF_example.log`](./dataframe_UDF_example.log) | Sample output log from running the script with `spark-submit` |

## 1. Define a UDF in Python

A UDF starts as an ordinary Python function. For example, a function
that triples its input:

```python
# n : integer
def tripled(n):
    return 3 * n
#end-def
```

## 2. Register the UDF

To register a UDF for use in Spark SQL, use
`SparkSession.udf.register()`. It takes three arguments:

* 1st: the desired name for the UDF to be used in SQL
* 2nd: the Python function to register
* 3rd: the return data type of the function (if omitted, PySpark
  assumes `StringType()`)

```python
# "tripled_udf" : desired name to use in SQL
# tripled      : the Python function defined above
# IntegerType() : the return type of the UDF
from pyspark.sql.types import IntegerType
spark.udf.register("tripled_udf", tripled, IntegerType())
```

## 3. Use the UDF

### Example A: `tripled_udf` in a SQL query

This walkthrough is illustrative — it shows the interactive-shell
session for the `tripled_udf` above, rather than a script kept in
this folder.

Create a sample DataFrame:

```python
>>> data = [('alex', 20, 12000), ('jane', 30, 45000),
            ('rafa', 40, 56000), ('ted', 30, 145000),
            ('xo2', 10, 1332000), ('mary', 44, 555000)]
>>>
>>> column_names = ['name', 'age', 'salary']
>>> df = spark.createDataFrame(data, column_names)
>>>
>>> df
DataFrame[name: string, age: bigint, salary: bigint]
>>> df.printSchema()
root
 |-- name: string (nullable = true)
 |-- age: long (nullable = true)
 |-- salary: long (nullable = true)

>>> df.show()
+----+---+-------+
|name|age| salary|
+----+---+-------+
|alex| 20|  12000|
|jane| 30|  45000|
|rafa| 40|  56000|
| ted| 30| 145000|
| xo2| 10|1332000|
|mary| 44| 555000|
+----+---+-------+

>>> df.count()
6
>>> df.createOrReplaceTempView("people")
>>> df2 = spark.sql("select * from people where salary > 67000")
>>> df2.show()
+----+---+-------+
|name|age| salary|
+----+---+-------+
| ted| 30| 145000|
| xo2| 10|1332000|
|mary| 44| 555000|
+----+---+-------+
```

Now use `tripled_udf` in a SQL query:

```python
>>> df2 = spark.sql("select name, age, salary, tripled_udf(salary) as tripled_salary from people")
>>> df2.show()
+----+---+-------+--------------+
|name|age| salary|tripled_salary|
+----+---+-------+--------------+
|alex| 20|  12000|         36000|
|jane| 30|  45000|        135000|
|rafa| 40|  56000|        168000|
| ted| 30| 145000|        435000|
| xo2| 10|1332000|       3996000|
|mary| 44| 555000|       1665000|
+----+---+-------+--------------+
```

### Example B: a runnable script

[`dataframe_UDF_example.py`](./dataframe_UDF_example.py) is a
complete, runnable script (output verified in
[`dataframe_UDF_example.log`](./dataframe_UDF_example.log)) that
defines two more functions:

```python
def convert_case(name):
    if name is None: return None
    if len(name) < 1: return ""
    result_string = ""
    arr = name.split(" ")
    for x in arr:
       result_string += x[0:1].upper() + x[1:len(x)] + " "
    #end-for
    return result_string.strip()
#end-def

def to_upper_case(name):
    if name is None: return None
    if len(name) < 1: return ""
    return name.upper()
#end-def
```

`convert_case()` title-cases each word (`"john jones"` →
`"John Jones"`); `to_upper_case()` upper-cases the whole string. Both
handle `None` and empty strings so they behave sensibly on nulls in
a DataFrame column.

**Wrap a function with `udf()`** to use it in the DataFrame API:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

convert_case_udf = udf(lambda p: convert_case(p))
upper_case_udf = udf(lambda p: to_upper_case(p), StringType())
```

**Or use the `@udf` decorator** to define and wrap a function as a
UDF in one step, instead of writing a plain function and calling
`udf()` on it afterward:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def to_upper_case_udf(name):
    if name is None: return None
    if len(name) < 1: return ""
    return name.upper()
```

This defines a UDF equivalent to `upper_case_udf` above — it's used
the same way, e.g. `to_upper_case_udf(col("Name"))`. The decorator
only makes the function usable from the DataFrame API, though; to
call it from Spark SQL you still need to register the plain function
with `spark.udf.register()`, as shown next.

**Register a function** to use it from Spark SQL:

```python
spark.udf.register("convert_UDF", convert_case, StringType())
```

Create a sample DataFrame:

```python
column_names = ["ID", "Name"]
some_data = [("100", "john jones"),
             ("200", "tracey smith"),
             ("300", "amy sanders"),
             ("400", None)]
df = spark.createDataFrame(data=some_data, schema=column_names)
df.show(truncate=False)
```
```
+---+------------+
|ID |Name        |
+---+------------+
|100|john jones  |
|200|tracey smith|
|300|amy sanders |
|400|null        |
+---+------------+
```

Apply the DataFrame-API UDF in a `select`:

```python
df.select(col("ID"), convert_case_udf(col("Name")).alias("Name")).show(truncate=False)
```
```
+---+------------+
|ID |Name        |
+---+------------+
|100|John Jones  |
|200|Tracey Smith|
|300|Amy Sanders |
|400|null        |
+---+------------+
```

...or with `withColumn`:

```python
df.withColumn("Upper Name", upper_case_udf(col("Name"))).show(truncate=False)
```
```
+---+------------+------------+
|ID |Name        |Upper Name  |
+---+------------+------------+
|100|john jones  |JOHN JONES  |
|200|tracey smith|TRACEY SMITH|
|300|amy sanders |AMY SANDERS |
|400|null        |null        |
+---+------------+------------+
```

Finally, use the SQL-registered UDF from a Spark SQL query:

```python
df.createOrReplaceTempView("NAME_TABLE")
spark.sql("select ID, convert_UDF(Name) as Name from NAME_TABLE").show(truncate=False)
```
```
+---+------------+
|ID |Name        |
+---+------------+
|100|John Jones  |
|200|Tracey Smith|
|300|Amy Sanders |
|400|null        |
+---+------------+
```

## Running the Script

```
export SPARK_HOME=/opt/spark
$SPARK_HOME/bin/spark-submit dataframe_UDF_example.py
```

Full expected output is in [`dataframe_UDF_example.log`](./dataframe_UDF_example.log).
See [`spark-submit-example/`](../spark-submit-example/) for a deeper
look at `spark-submit` itself.

## A Note on Performance

A Python UDF runs row-by-row in a separate Python process, which adds
serialization overhead compared to Spark's built-in
`pyspark.sql.functions` column expressions. Prefer a built-in when one
exists — e.g. `upper()` instead of a custom `to_upper_case` UDF —
and reach for a UDF only when the logic genuinely can't be expressed
that way. For heavier row-at-a-time or vectorized work, consider a
Pandas UDF (`@pandas_udf`), which batches rows through Arrow and is
typically much faster than a plain UDF.
