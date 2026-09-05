# PySpark DataFrame Practice Questions

* For practice purposes, some scenarios reappear in a different form/shape
  elsewhere in this question set (and in
  [`rdds_questions.md`](rdds_questions.md), which has the PySpark-RDD
  version of the SQL-translation questions below).

* You may not post the solutions to these questions anywhere.

* Assume that the following variables are defined/created:

```code
    spark : an instance of SparkSession object
       sc : an instance of SparkContext object
```

* Created and Compiled by: Mahmoud Parsian

* Last updated: 9/4/2026 (split out of the combined
  `mapreduce_and_RDD_questions.md` and `dataframe_questions.txt`)

---

## Question 1

*(Companion question — see [Question 48](rdds_questions.md#question-48) in
`rdds_questions.md` for the PySpark RDD version of this same SQL
translation.)*

Consider the following SQL query:

```sql
SELECT COUNT(CustomerID) as count, Country
  FROM Customers
   GROUP BY Country;
```

If the Customers table dumped as a file (`dump.csv`) with the following
format:

    <CustomerID><,><Country>

How would you translate this SQL query by using PySpark DataFrames?

---

## Question 2

*(Companion question — see [Question 49](rdds_questions.md#question-49) in
`rdds_questions.md` for the PySpark RDD version of this same SQL
translation.)*

Consider the following SQL query:

```sql
SELECT COUNT(CustomerID) as COUNTED, Country
   FROM Customers
     GROUP BY Country
       ORDER BY COUNTED DESC
          LIMIT 5;
```

If the Customers table dumped as a file (`dump.csv`) with the following
format:

    <CustomerID><,><Country>

How would you translate this SQL query by using PySpark DataFrames?

---

## Question 3

*(Companion question — see [Question 50](rdds_questions.md#question-50) in
`rdds_questions.md` for the PySpark RDD version of this same SQL
translation.)*

Consider the following SQL query:

```sql
SELECT NAME, SUM(SALARY) FROM Employee
GROUP BY NAME
HAVING SUM(SALARY) > 3000;
```

If the Employee table dumped as file `dump.csv` with records:

    <NAME><,><SALARY>

How would you translate this SQL query by using PySpark DataFrames?

---

## Question 4

*(Companion question — see [Question 51](rdds_questions.md#question-51) in
`rdds_questions.md` for the PySpark RDD version of this same SQL
translation.)*

Consider the following SQL query:

```sql
SELECT COUNT(CustomerID) as COUNTED, Continent, Country
FROM Customers
GROUP BY Continent, Country
ORDER BY COUNTED DESC
LIMIT 5;
```

If the Customers table dumped as a file `dump.csv` with the following
records:

    <CustomerID><,><continent><,><Country>

How would you translate this SQL query by using PySpark DataFrames?

---

## Question 5

Given a graph as a list of edges:

    <src_node_id><,><dst_node_id>

1. With using GraphFrames, find `inDegrees` and `outDegrees` of all nodes.

2. Without using GraphFrames, find `inDegrees` and `outDegrees` of all
   nodes.

The in-degree of each vertex in the graph, returned as a DataFrame with two
columns:

* "id": the ID of the vertex
* "inDegree" (int) storing the in-degree of the vertex

Note that vertices with 0 in-edges are not returned in the result.

The out-degree of each vertex in the graph, returned as a DataFrame with
two columns:

* "id": the ID of the vertex
* "outDegree" (integer) storing the out-degree of the vertex

Note that vertices with 0 out-edges are not returned in the result.

---

## Question 6

Given Credit Card data records as:

    <transaction_id><,><date><,><transaction_amount><,><item><,><customer_id>

where

    <date>=<dd/mm/YYYY>

1. If we will query data by YYYY, how would you partition your data by
   using PySpark?

2. If we will query data by YYYY and mm, how would you partition your data
   by using PySpark?

3. If we will query data by YYYY and customer_id, how would you partition
   your data by using PySpark?

---

## Question 7

Create a DataFrame with the following columns: `(dept, name, salary)`; your
DataFrame should have at least 4 rows.

a. Find average of salary per dept.

b. Find maximum of salary per dept.

c. Find minimum of salary per dept.

d. Find (minimum, maximum) salary per dept.

---

## Question 8

Let `e` be a DataFrame representing edges in a GraphFrame environment:

    e = (src, dst, weight)

and `g` be a graph `(v, e)` where `v` represents vertices as `(id, name)`.

Write a series of transformations to make this graph undirected.

---

## Question 9

Let `rdd` denote `RDD[(String, String, Integer)]` to be of triplets `(id,
name, salary)`.

Write a series of transformations to convert `rdd` into a DataFrame with 3
columns: `(id, name, salary)`.

---

## Question 10

Given a CSV file with the following fields:

    continent, country, city, temperature

1. Create a DataFrame with 4 columns

2. Find average temperature per continent

3. Find average temperature per (continent, country)

4. Partition data in such a way that 80% of queries will analyze data by
   continent

5. Partition data in such a way that 70% of queries will analyze data by
   (continent, country). What will be the table schema?

6. Partition data in such a way that 70% of queries will analyze data by
   (continent) OR (continent, country, city). What will be the table
   schema?

---

## Question 11

Let `e` be a DataFrame representing edges in a GraphFrame environment:

    e = (src, dst, weight)

You want to build a graph by using GraphFrame.

Create `v` as vertices from a given `e`, and then create a graph as
`(v, e)`.

---

## Question 12

Consider the following DataFrame:

```python
features = [('alex', 1), ('bob', 3), ('ali', 6), ('dave', 10)]
columns = ("name", "age")
# spark : as a SparkSession object

samples = spark.createDataFrame(features, columns)
>>> samples.show()
+----+---+
|name|age|
+----+---+
|alex|  1|
| bob|  3|
| ali|  6|
|dave| 10|
+----+---+
```

How would you standardize the age column, where:

    age_scaled = (age - mean_age) / standard_deviation_age

The output should be:

```text
+----+---+-------------------+
|name|age|age_scaled         |
+----+---+-------------------+
|alex|1  |-1.0215078369104984|
|bob |3  |-0.5107539184552492|
|ali |6  |0.2553769592276246 |
|dave|10 |1.276884796138123  |
+----+---+-------------------+
```

---

## Question 13

Let `e` be a DataFrame representing edges in a GraphFrame environment:

    e = (src, dst, weight)

and `g` be a graph `(v, e)` where `v` represents vertices as `(id, name,
age)`.

Write a series of transformations to find the names of users, which are
connected bi-directionally and age difference is 5.

---

## Question 14

Let `df` be a Spark DataFrame representing `(name, age, salary)`.

1. Create a new DataFrame for teenagers.

2. Create a new DataFrame, to represent `(name, avg_salary)`, where
   `avg_salary` is an average salary per `name`.

3. Create a new DataFrame for baby boomers.

4. Create a new DataFrame `(age, count)`, where `count` is a frequency per
   `age`.

---

## Question 15

Given a Spark DataFrame:

    (emp_id, age, salary, year)

Each employee may have many records.

1. Create a DataFrame with 10 records and 2 `emp_id`(s)

2. Write a set of transformations to create the following DataFrame:

       (emp_id, salary, year)

   where `(emp_id, year)` is distinct and `salary` is the maximum salary
   per year.

3. Using the DataFrame created in Step 2: write a set of transformations
   to create the following DataFrame:

       (emp_id, average_salary, minimum_salary, maximum_salary)

---

## Question 16

Create the following DataFrame with 2 columns and 3 rows:

```text
df.show()

+---------+-----------+
| name    | tricks    |
+---------+-----------+
| alex    | [1, 2]    |
| jane    | []        |
| ted     | [1, 2, 3] |
+---------+-----------+
```

---

## Question 17

Given the following DataFrame:

```text
+---------+-----------+
| name    | tricks    |
+---------+-----------+
| alex    | [1, 2]    |
| jane    | []        |
| ted     | [1, 2, 3] |
+---------+-----------+
```

write a set of general transformations to produce the following DataFrame,
where the last column is the number of tricks:

```text
+---------+-----------+--------------+
| name    | tricks    | num_tricks   |
+---------+-----------+--------------+
| alex    | [1, 2]    |     2        |
| jane    | []        |     0        |
| ted     | [1, 2, 3] |     3        |
+---------+-----------+--------------+
```

---

## Question 18

Given the following DataFrame:

```text
+---------+-----------+--------------+
| name    | tricks    | num_tricks   |
+---------+-----------+--------------+
| alex    | [1, 2]    |     2        |
| jane    | []        |     0        |
| ted     | [1, 2, 3] |     3        |
+---------+-----------+--------------+
```

write a set of general transformations to keep the rows if the number of
tricks is more than 1.

---

## Question 19

Given the following DataFrame:

```text
+---------+-----------+--------------+
| name    | tricks    | num_tricks   |
+---------+-----------+--------------+
| alex    | [1, 2]    |     2        |
| jane    | []        |     0        |
| ted     | [1, 2, 3] |     3        |
+---------+-----------+--------------+
```

write a set of general transformations to find the top-5 tricks.

---

## Question 20

Given billions of records, where each record has the following format:

    <record_number><,><gene_id><,><gene_value>

Assume that your input files reside in the `/tmp/data/` directory.

a. Create a DataFrame to represent our input as
   `DataFrame(record_number, gene_id, gene_value)`

b. Write a set of transformations to find average of gene_values per
   gene_id

c. Write a set of transformations to find (min, max) of gene_values per
   gene_id

d. Write a set of transformations to keep only positive gene_values.

---

## Question 21

Given billions of records, where each record has the following format:

    <record_number><,><gene_id><,><gene_value>

Assume that your input files reside in the `/tmp/data/` directory.

a. Create a DataFrame to represent our input as `DataFrame(gene_id,
   gene_value)`

b. Write a set of transformations to find median of gene_values per
   gene_id

c. Write a set of transformations to find (N, P) of gene_values per
   gene_id, where P denotes positive gene_values and N denotes negative
   gene values

---

## Question 22

Given the following DataFrame:

```text
+---------+-----------+--------------+
| name    | tricks    | num_tricks   |
+---------+-----------+--------------+
| alex    | [1, 2]    |     2        |
| jane    | []        |     0        |
| ted     | [1, 2, 3] |     3        |
+---------+-----------+--------------+
```

a. Keep records if name is not null.

b. Find unique names, with no tricks.

c. Find unique names, with more than 1 trick.

---

## Question 23

Let `genes.txt` be a huge text file, where every record has the following
format (reference can be in {1, 2, 3} where 1 denotes cancer, 2 denotes
healthy, and 3 is undefined):

    <geneID><,><reference><,><geneValue>

For example, a sample input might be:

```text
g1,1,2.3
g1,2,1.5
g1,3,2.5
g1,1,4.1
g2,1,1.3
g2,2,1.8
g2,3,3.5
g2,1,4.3
g2,1,2.9
...
```

a. Create a DataFrame from the input.

b. Write a PySpark command/transformation to convert `genes.txt` file into
   an `RDD<String>` and then output the number of elements in that RDD.

c. Let `df` represent the `genes.txt` file in a Spark DataFrame. Use `df`
   and write a set of transformations to generate the following output per
   geneID:

       <geneID> <C> <S>

   where `C` is the number of cancer genes (for geneID) and `S` is the sum
   of values for the cancer gene.

d. Use `df` and write a PySpark filter to remove all undefined genes.

---

## Question 24

Assume the following input:

    <Employee-ID><,><type>

where type can be:

* `"fulltime"`
* `"parttime"`
* `"contractor"`

a. Create a DataFrame from input.

b. Drop all duplicate records.

c. Find the total number of "fulltime" and "parttime" employees.

---

## Question 25

Since a DataFrame is a table of rows and named columns (similar to a
relational table), therefore we should not care about partitioning a
DataFrame into partitions. Justify your answer.

---

## Question 26

Assume that all of the input is in a file called "movies.txt" and each
input record has the following format:

    <userID><,><movieID><,><rating-in-range-of-1-to-5>

Sample input:

```text
user1,movie1,3
user1,movie1,1
user1,movie2,5
user2,movie1,4
...
```

Note that a user may rate the same movie any number of times.

a. Represent `movies.txt` file as a DataFrame (`df`).

b. The goal is to find the number of raters per movie. Write a complete
   PySpark program (as a set of transformations and actions) to accomplish
   this task. Your output will be:

       <movieID> <number-of-raters>

c. The goal is to find the number of unique movies rated by each user.
   Write a complete PySpark program (as a set of transformations and
   actions) to accomplish this task. Your output will be:

       <userID> <number-of-unique-movies>

---

## Question 27

Use PySpark DataFrames to answer this question.

Assume that all of the input is in a file called "movies.txt" (with
millions of records) and each input record has the following format:

    <MOVIE-ID><,><rating-in-range-of-1-to-5>

Sample input:

```text
movie1,3
movie1,1
movie1,5
movie2,5
movie2,4
movie2,3
...
```

Note that a user may rate the same movie any number of times. You HAVE to
use the following Python functions in your transformations (NOTE, you MUST
NOT use the `split()` function at all).

a. Represent `movies.txt` file as a DataFrame (`df`).

b. The goal is to find the number of raters per movie. Write a complete
   PySpark program (as a set of PySpark transformations and actions) to
   accomplish this task. Your output will be like:

       <MOVIE-ID> <number-of-raters>

c. The goal is to find the average rating per movie. Write a complete
   PySpark program (as a set of transformations and actions) to accomplish
   this task. Your output will be as:

       <MOVIE-ID> <average-rating-per-MOVIE-ID>

---

## Question 28

Given two DataFrames:

```python
employees = spark.createDataFrame(
    [(1, "alex", "IT"), (2, "jane", "HR"),
     (3, "ted", "IT"), (4, "dan", "Sales")],
    ["emp_id", "name", "dept"]
)

departments = spark.createDataFrame(
    [("IT", "Engineering"), ("HR", "Human Resources")],
    ["dept", "dept_name"]
)
```

a. Write a DataFrame `inner` join on `dept` to produce `(emp_id, name,
   dept, dept_name)`. Show the output.

b. Write a `left` join so that employees with no matching department
   (like `dan`, whose dept is `"Sales"`) still appear, with `null` for
   `dept_name`. Show the output.

c. Write a `left_anti` join to find employees whose `dept` has NO match in
   `departments`. Show the output.

---

## Question 29

Given the following DataFrame:

```python
df = spark.createDataFrame(
    [("alex", "IT", 90000), ("bob", "IT", 80000),
     ("carol", "HR", 70000), ("dan", "HR", 75000),
     ("eve", "HR", 85000)],
    ["name", "dept", "salary"]
)
```

a. Using a `Window` partitioned by `dept` and ordered by `salary`
   descending, add a `rank` column using `row_number()`. Show the output.

b. Using the same window, keep only the top-2 highest-paid employees per
   `dept`.

c. Add a column `dept_avg_salary` holding the average salary of each
   employee's department (without collapsing rows via `groupBy`) using a
   window function.

---

## Question 30

Given a CSV file `employees.csv` with columns `(id, name, salary)`, where
`id` and `salary` should be treated as integers, not strings.

a. Define an explicit `StructType` schema for this file, and use it (via
   `spark.read.schema(...)`) instead of relying on `inferSchema`. Why is
   an explicit schema generally preferable to schema inference for large,
   recurring production jobs? Answer in at most 3 sentences.

b. Write the DataFrame out to `/tmp/employees.parquet`, partitioned by
   `name`'s first letter (create a new column for this first).

c. Read `/tmp/employees.parquet` back into a new DataFrame and confirm
   (via `printSchema()`) that the types from your explicit schema were
   preserved.

---

## Question 31

Given the following DataFrame:

```python
df = spark.createDataFrame(
    [(1, "Alice"), (2, "bob"), (3, "CHARLIE")],
    ["id", "name"]
)
```

a. Write a Python UDF `title_case(s)` that converts a name to title case
   (e.g., `"CHARLIE"` → `"Charlie"`), register it, and use it in a
   `withColumn()` call to add a `name_titlecase` column.

b. Rewrite part (a) WITHOUT a UDF, using only PySpark's built-in
   `pyspark.sql.functions`. Which version does Catalyst optimize better,
   and why should built-ins generally be preferred over UDFs?

---

## Question 32

Given the following DataFrame, which has both duplicate rows and null
values:

```python
df = spark.createDataFrame(
    [(1, "alex", 90000), (1, "alex", 90000), (2, "jane", None),
     (3, None, 70000), (4, "dan", 75000)],
    ["id", "name", "salary"]
)
```

a. Remove exact duplicate rows (keep one copy of `(1, "alex", 90000)`).

b. Drop rows where `name` is null.

c. Replace null `salary` values with `0` instead of dropping those rows.

d. What is the difference between using `dropDuplicates()` with no
   arguments versus `dropDuplicates(["id"])`? Give an example using the
   DataFrame above where the two would produce different results.

---

## Question 33

Given the following DataFrame:

```python
df = spark.createDataFrame(
    [("alex", "IT", 90000), ("bob", "IT", 80000),
     ("carol", "HR", 70000), ("dan", "HR", 75000)],
    ["name", "dept", "salary"]
)
```

Using `groupBy("dept").pivot("name").agg(...)`, produce a DataFrame with
one row per `dept` and one column per `name`, holding each employee's
`salary` (`null` where a name doesn't belong to that department). Show the
output.
