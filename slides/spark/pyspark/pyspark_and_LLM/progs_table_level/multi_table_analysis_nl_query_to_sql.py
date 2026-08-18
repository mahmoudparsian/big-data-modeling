# Natural Language Query of PySpark DataFrames using OpenAI Integration
#
# Multiple DataFrames

"""
You’ll now get a **complete, working example** that:

✅ Loads **multiple CSV files** as **separate Spark DataFrames**
✅ Registers them as **SQL tables** in Spark
✅ Sends **their schemas and sample data** to OpenAI GPT-4
✅ GPT translates a **complex natural language question** into 
   a **Spark SQL query** across multiple tables
✅ The query is executed and the results are shown

🛑 Still no Pandas and **no LangChain** involved — just raw Spark + OpenAI.

---

## ✅ Use Case: Multi-Table Analysis

Imagine a company has two datasets:

### 1. `employees.csv`

```csv
id,name,age,department_id,salary
1,Alice,30,1,100000
2,Bob,35,2,80000
3,Charlie,25,1,70000
4,Diana,40,3,90000
5,Evan,38,2,85000
6,Fay,28,3,95000
7,George,45,1,110000
8,Helen,33,2,78000
```

### 2. `departments.csv`

```csv
id,name
1,Engineering
2,HR
3,Marketing
```

---

## 🧠 Natural Language Question

> “Which department has the highest average salary for employees older than 30?”

This requires a **join** between employees and departments.


## 🔐 Tips

* GPT handles multi-table joins well if schema and sample rows are clear.
* Use `LIMIT 5` for data preview to stay within token limits.
* In real apps, always **validate GPT-generated SQL** before execution.

---
"""

### 🧠 Python Script 

import sys
import os
from pyspark.sql import SparkSession
from openai import OpenAI

#----------------------
# create OpenAI client 
#----------------------
def create_client(openai_api_key):
    return OpenAI(api_key=openai_api_key) 
#end-def

#-----------------------------------------------
# Extract Schema & Sample Data for Prompt ======
#-----------------------------------------------
def extract_schema_and_sample(df, table_name):
    schema_str = f"Table `{table_name}`: " + ", ".join([f"{field.name} ({field.dataType.simpleString()})" for field in df.schema.fields])
    rows = df.limit(5).collect()
    sample_str = "\n".join([str(row.asDict()) for row in rows])
    return schema_str, sample_str


#-----------------------------------------------------
# generate SQL for a given NL Query by calling LLM API
# ====== 6. Call OpenAI ======
#-----------------------------------------------------
def generate_sql(client, nl_query_prompt):

    response = client.chat.completions.create(
        model="gpt-4",  # or another chat model like "gpt-3.5-turbo"
        messages=[
            {"role": "system", "content": "You are a Spark SQL expert."},
            {"role": "user", "content": nl_query_prompt}
        ],
        temperature=0,
        max_tokens=200
    )
    
    sql_query = response.choices[0].message.content.strip()
    return sql_query
#end-def


# ========== 1. Setup Spark and OpenAI ==========
# Set your OpenAI API key securely
openai_api_key = "sk-..."

# ========== 2. create a SparkSession object
spark = SparkSession.builder.getOrCreate()

# ========== 3. Load CSV into Spark DataFrame ==========
employees_csv = sys.argv[1]
print("employees_csv=", employees_csv)
#
departments_csv = sys.argv[2]
print("departments_csv=", departments_csv)
#
# ====== 2. Load Multiple CSVs as DataFrames ======
employees_df = spark.read.csv(employees_csv, header=True, inferSchema=True)
departments_df = spark.read.csv(departments_csv, header=True, inferSchema=True)

print("=== Sample Data ===")
print("=== Sample Data: Employees ===")
employees_df.show(truncate=False)
print("=== Sample Data: Departments ===")
departments_df.show(truncate=False)


# ========== 4. create a view of a DataFrame 
# Register as SQL tables
employees_df.createOrReplaceTempView("employees")
departments_df.createOrReplaceTempView("departments")


# create table schemas and sample rows of data
emp_schema, emp_samples = extract_schema_and_sample(employees_df, "employees")
dept_schema, dept_samples = extract_schema_and_sample(departments_df, "departments")

# ====== 4. Natural Language Question ======
#user_question = "Which department has the highest average salary for employees older than 30?"
user_question = "Which departments have no employees? Give detailed information."

# ====== 5. Build GPT Prompt ======
prompt = f"""
You are a data analyst that converts natural language questions into Spark SQL queries.

There are two tables:

{emp_schema}
Sample rows:
{emp_samples}

{dept_schema}
Sample rows:
{dept_samples}

Write only the Spark SQL query (no explanation) to answer the following question:

Question: {user_question}
SQL:
"""

# ========== 8. Ask OpenAI to Translate to SQL ==========
# create an OpenAI client
client = create_client(openai_api_key)

sql_query = generate_sql(client, prompt)
print("\n=== 🔍 GPT-Generated SQL ===")
print("sql_query=", sql_query)

# NOTE: the generated SQL has a special format as: ```sql<sql-query>```
# therefore, we have to get rid of special characters from the 
# beginning and end of the generated sql query
cleaned_sql_query = sql_query.replace("```sql", "").replace("```", "")
print("cleaned_sql_query=", cleaned_sql_query)

# ====== 7. Execute GPT-Generated SQL in Spark ======
try:
    result_df = spark.sql(cleaned_sql_query)
    print("\n=== ✅ Query Result ===")
    result_df.show()
except Exception as e:
    print("\n❌ Error executing SQL:")
    print(e)


"""
## ✅ Expected SQL Generated by GPT

```sql
SELECT d.name AS department, AVG(e.salary) AS avg_salary
FROM employees e
JOIN departments d ON e.department_id = d.id
WHERE e.age > 30
GROUP BY d.name
ORDER BY avg_salary DESC
LIMIT 1;
```

---

## 🧪 Sample Output

```
=== ✅ Query Result ===
+-----------+----------+
|department |avg_salary|
+-----------+----------+
|Engineering|  110000.0|
+-----------+----------+
"""

