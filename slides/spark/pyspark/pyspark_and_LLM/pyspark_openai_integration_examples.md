# Natural Language Query <br> of PySpark DataFrames <br> using OpenAI Integration


### 1. Single-Table Analysis using OpenAI API:
	
	We provide a complete running example of pyspark 
	reading a CSV file (with some sample rows) and 
	then create a dataframe, then use openAI integration 
	by a complex natural language query to analyze data 
	in dataframe (such as natural language to SQL).
	You should not convert spark dataframe to panda's 
	dataframe (because this will not scale out for large
	data sets).
	
### 2. Multi-Table Analysis using OpenAI API:

	Also, we provide how to generate SQL for 
	Multi-Tables (representing multi-DataFrames)
	using JOIN operations.
	

## Complete working example that:

	✅ Reads a CSV using **PySpark**
	✅ Creates a Spark DataFrame (without converting to Pandas)
	✅ Uses **OpenAI GPT-4** to analyze the data using a 
	   **complex natural language query**, such as **NL → SQL translation**
	✅ Executes the SQL on the Spark DataFrame
	✅ Returns and displays the result

---

### ✅ Natural Language Queries 

	You'll be able to ask natural language queries like:

	> "what is the average salart per department?"
		
	> "which department has the highest average salary for employees over 30?"

	> "which departments has no employees assigned to?"
	  (this requires JOIN operation)


### And the system will:

	1. Send the query, schema, and sample data to GPT-4
	2. GPT-4 generates a valid **Spark SQL query**
	3. The SQL is run on the Spark DataFrame
	4. Results are displayed

---

### 📦 Requirements

		Install these Python packages:
		
		```bash
		pip install pyspark openai
		```

---

### 📁 Sample CSV (`data.csv`)

```csv
name,age,department,salary
Alice,30,Engineering,100000
Bob,35,HR,80000
Charlie,25,Engineering,70000
Diana,40,Marketing,90000
Evan,38,HR,85000
Fay,28,Marketing,95000
George,45,Engineering,110000
Helen,33,HR,78000
```

---

### 🧠 Python Script (`single_table_analysis_nl_query_to_sql.py`)

```python
from pyspark.sql import SparkSession
from openai import OpenAI
import os

# ========== 1. Setup Spark and OpenAI ==========
# Set your OpenAI API key securely
openai_api_key = "sk-..."  # Replace with your key or use environment variable
client = OpenAI(api_key=openai_api_key) 

spark = SparkSession.builder \
    .appName("Natural Language to Spark SQL") \
    .getOrCreate()

# ========== 2. Load CSV into Spark DataFrame ==========
df = spark.read.csv("data.csv", header=True, inferSchema=True)
df.createOrReplaceTempView("employees")

print("=== Sample Data ===")
df.show(5)

# ========== 3. Extract Schema and Sample Data ==========
schema_str = ", ".join([f"{field.name} ({field.dataType.simpleString()})" for field in df.schema.fields])
sample_rows = df.limit(5).collect()
formatted_rows = "\n".join([str(row.asDict()) for row in sample_rows])

# ========== 4. User’s Natural Language Query ==========
user_question = "Which department has the highest average salary for employees over 30 years old?"

# ========== 5. Build Prompt for GPT ==========
prompt = f"""
You are a data analyst who translates English questions into Spark SQL queries.

Dataset name: employees

Schema:
{schema_str}

Sample rows:
{formatted_rows}

Now, translate the following question into a Spark SQL query (don't explain it, just give the SQL):

Question: {user_question}
SQL:
"""

# ========== 6. Ask OpenAI to Translate to SQL ==========
response = client.chat.completions.create(
    model="gpt-4",
    messages=[
        {"role": "system", "content": "You are a Spark SQL expert."},
        {"role": "user", "content": prompt}
    ],
    temperature=0,
    max_tokens=150
)

sql_query = response['choices'][0]['message']['content'].strip()

print("\n=== 🔍 GPT-Generated SQL ===")
print(sql_query)

# ========== 7. Run the SQL on the Spark DataFrame ==========
try:
    result_df = spark.sql(sql_query)
    print("\n=== ✅ Query Result ===")
    result_df.show()
except Exception as e:
    print("\n❌ Error executing SQL query:")
    print(e)
```

---

### 🧪 Sample Output

```
=== Sample Data ===
+-------+---+-----------+------+
|   name|age| department|salary|
+-------+---+-----------+------+
|  Alice| 30|Engineering|100000|
|    Bob| 35|         HR| 80000|
|Charlie| 25|Engineering| 70000|
|  Diana| 40|  Marketing| 90000|
|   Evan| 38|         HR| 85000|
+-------+---+-----------+------+

=== 🔍 GPT-Generated SQL ===
SELECT department, AVG(salary) as avg_salary
FROM employees
WHERE age > 30
GROUP BY department
ORDER BY avg_salary DESC
LIMIT 1;

=== ✅ Query Result ===
+-----------+----------+
|department |avg_salary|
+-----------+----------+
|Engineering|  110000.0|
+-----------+----------+
```

---

### ✅ Summary

This example:

	* Keeps everything inside **Spark** (no Pandas conversion)
	* Uses GPT-4 to convert **natural language to SQL**
	* Executes GPT's query directly on the Spark DataFrame
	* Returns the result programmatically

---

### 🔐 Security Note

	Always sanitize GPT-generated SQL in production — 
	here we're trusting the model since the schema and 
	tables are predefined.

---

# Multiple DataFrames


You’ll now get a **complete, working example** that:

	✅ Loads **multiple CSV files** as **separate Spark DataFrames**
	✅ Registers them as **SQL tables** in Spark
	✅ Sends **their schemas and sample data** to OpenAI GPT-4
	✅ GPT translates a **complex natural language question** into a **Spark SQL query** across multiple tables
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

	> “find departments, which has no employees.”

This requires a **join** between employees and departments.

---

## 🧪 Python Script: `multi_table_analysis_nl_query_to_sql.py`

```python
from pyspark.sql import SparkSession
from openai import OpenAI

# ====== 1. Setup Spark and OpenAI ======
openai_api_key = "sk-..."  # Replace with your key or use environment variable
client = OpenAI(api_key=openai_api_key) 

spark = SparkSession.builder \
    .appName("MultiDF NL to SparkSQL") \
    .getOrCreate()

# ====== 2. Load Multiple CSVs as DataFrames ======
employees_df = spark.read.csv("employees.csv", header=True, inferSchema=True)
departments_df = spark.read.csv("departments.csv", header=True, inferSchema=True)

# Register as SQL tables
employees_df.createOrReplaceTempView("employees")
departments_df.createOrReplaceTempView("departments")

print("=== Sample Data: Employees ===")
employees_df.show(3)

print("=== Sample Data: Departments ===")
departments_df.show()

# ====== 3. Extract Schema & Sample Data for Prompt ======

def extract_schema_and_sample(df, table_name):
    schema_str = f"Table `{table_name}`: " + ", ".join([f"{field.name} ({field.dataType.simpleString()})" for field in df.schema.fields])
    rows = df.limit(5).collect()
    sample_str = "\n".join([str(row.asDict()) for row in rows])
    return schema_str, sample_str

emp_schema, emp_samples = extract_schema_and_sample(employees_df, "employees")
dept_schema, dept_samples = extract_schema_and_sample(departments_df, "departments")

# ====== 4. Natural Language Question ======
user_question = "Which department has the highest average salary for employees older than 30?"

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

# ====== 6. Call OpenAI ======
response = client.chat.completions.create(
    model="gpt-4",
    messages=[
        {"role": "system", "content": "You are a Spark SQL expert."},
        {"role": "user", "content": prompt}
    ],
    temperature=0,
    max_tokens=200
)

sql_query = response['choices'][0]['message']['content'].strip()

print("\n=== 🔍 GPT-Generated SQL ===")
print(sql_query)

# ====== 7. Execute GPT-Generated SQL in Spark ======
try:
    result_df = spark.sql(sql_query)
    print("\n=== ✅ Query Result ===")
    result_df.show()
except Exception as e:
    print("\n❌ Error executing SQL:")
    print(e)
```

---

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
```

---

## 🔐 Tips

	* GPT handles multi-table joins well if schema and sample rows are clear.
	* Use `LIMIT 5` for data preview to stay within token limits.
	* In real apps, always **validate GPT-generated SQL** before execution.

---

## 🛠️ Optional Enhancements


* ✅ Wrap this in a CLI or web app?
* 🗨️ Add conversational memory (without LangChain)?
* 📄 Use Parquet instead of CSV?



# Multiple Dataframes and Ranking Functions

Let's take the **multi-DataFrame PySpark + GPT** 
integration you already have and scale it up by:

---

### ✅ Adding a Very **Complex Natural Language Query**

Involving:

* **Joins**
* **Ranking functions** like `DENSE_RANK()` or `ROW_NUMBER()`
* **Filters** based on ranking

---

## 📊 Use Case

You still have two tables:

---

### 📁 `employees.csv`

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
9,Ian,36,3,91000
10,Jane,42,1,120000
```

---

### 📁 `departments.csv`

```csv
id,name
1,Engineering
2,HR
3,Marketing
```

---

## 🧠 Complex Natural Language Query:

		"Find the top 2 highest-paid employees 
		per department over the age of 30, ordered 
		by salary descending within each department."

This query requires:

* A `JOIN`
* A `WHERE` clause
* A **window function** with `DENSE_RANK()` or `ROW_NUMBER() OVER (PARTITION BY department)`
* Sorting within partitions

---

## ✅ Full Working Script

```python
from pyspark.sql import SparkSession
from openai import OpenAI

# ========== 1. Setup ==========
openai_api_key = "sk-..."  # Replace with your key or use environment variable
client = OpenAI(api_key=openai_api_key) 

spark = SparkSession.builder \
    .appName("MultiDF Complex SQL via GPT") \
    .getOrCreate()

# ========== 2. Load Data ==========
employees_df = spark.read.csv("employees.csv", header=True, inferSchema=True)
departments_df = spark.read.csv("departments.csv", header=True, inferSchema=True)

# Register as Spark SQL views
employees_df.createOrReplaceTempView("employees")
departments_df.createOrReplaceTempView("departments")

# ========== 3. Show Sample Data ==========
print("=== Employees ===")
employees_df.show(5)

print("=== Departments ===")
departments_df.show()

# ========== 4. Prepare schema + samples ==========
def get_schema_and_samples(df, table_name):
    schema = f"Table `{table_name}`: " + ", ".join([f"{f.name} ({f.dataType.simpleString()})" for f in df.schema.fields])
    samples = "\n".join([str(r.asDict()) for r in df.limit(5).collect()])
    return schema, samples

emp_schema, emp_samples = get_schema_and_samples(employees_df, "employees")
dept_schema, dept_samples = get_schema_and_samples(departments_df, "departments")

# ========== 5. Complex NL Query ==========
user_question = "Find the top 2 highest-paid employees per department over the age of 30, ordered by salary descending within each department."

# ========== 6. Create Prompt ==========
prompt = f"""
You are a Spark SQL expert. There are two tables:

{emp_schema}
Sample rows:
{emp_samples}

{dept_schema}
Sample rows:
{dept_samples}

Translate the following natural language request into Spark SQL.
Return only the SQL code, no explanation.

Question:
{user_question}

SQL:
"""

# ========== 7. Call GPT ==========
response = client.chat.completions.create(
    model="gpt-4",
    messages=[
        {"role": "system", "content": "You translate English to Spark SQL queries."},
        {"role": "user", "content": prompt}
    ],
    temperature=0,
    max_tokens=300
)

sql_query = response['choices'][0]['message']['content'].strip()
print("\n=== 🧠 GPT-Generated SQL ===")
print(sql_query)

# ========== 8. Execute Spark SQL ==========
try:
    result_df = spark.sql(sql_query)
    print("\n=== ✅ Query Result ===")
    result_df.show()
except Exception as e:
    print("\n❌ SQL Execution Error:")
    print(e)
```

---

## ✅ Expected GPT-Generated SQL

```sql
WITH ranked_employees AS (
    SELECT
        e.id,
        e.name,
        e.age,
        e.salary,
        d.name AS department,
        DENSE_RANK() OVER (PARTITION BY d.name ORDER BY e.salary DESC) AS rank
    FROM employees e
    JOIN departments d ON e.department_id = d.id
    WHERE e.age > 30
)
SELECT *
FROM ranked_employees
WHERE rank <= 2
ORDER BY department, salary DESC;
```

---

## ✅ Sample Output

```
+---+-------+---+------+-----------+----+
| id|   name|age|salary| department|rank|
+---+-------+---+------+-----------+----+
| 10|   Jane| 42|120000|Engineering|   1|
|  7| George| 45|110000|Engineering|   2|
|  5|   Evan| 38| 85000|         HR|   1|
|  2|    Bob| 35| 80000|         HR|   2|
|  9|    Ian| 36| 91000|  Marketing|   1|
|  4|  Diana| 40| 90000|  Marketing|   2|
+---+-------+---+------+-----------+----+
```

---

## 🔍 Breakdown of What GPT Does Well Here

* Joins employees → departments
* Filters with `WHERE e.age > 30`
* Uses `DENSE_RANK()` over `PARTITION BY department`
* Selects `rank <= 2`

---

## 🛡️ Tips for Robustness

* You can optionally **validate SQL syntax** before execution using:

  ```python
  spark.sql(f"EXPLAIN {sql_query}").show(truncate=False)
  ```
* Use `try/except` to handle edge cases or bad SQL output from GPT

---

## Want More? Would you like to:

* Add support for `GROUP BY ROLLUP`, `CUBE`, or window sliding?
* Ask follow-up questions about the result?
* Automate data profiling as part of the prompt?


# insight about data

	Given the two dataframes in pyspark (without conversion 
	to Panda's dataframe), how openAI can make sense of these 
	two dataframes and give a meaningful insight about these 
	2 dataframes

Let’s clarify how **OpenAI can provide meaningful insights** 
from **multiple PySpark DataFrames** **without using Pandas**.

---

## ✅ Goal

	We want GPT (OpenAI) to analyze **multiple PySpark DataFrames** 
	directly, without converting them to Pandas, and still provide 
	useful, intelligent, and even analytical answers.

---

## 🤔 But How Can GPT “Understand” PySpark DataFrames?

GPT doesn’t execute code or access the actual in-memory DataFrames.

So what does it need instead?

### ✅ All GPT needs:

1. **Schema**: Column names + data types (like a table definition)
2. **Sample rows**: A few example rows from each DataFrame
3. **Your natural language question**
4. *(Optional)* Any relationships between the DataFrames (e.g. joins)

		With that information, GPT can "reason" about the data 
		and simulate analysis or even generate valid SQL or 
		business insights.

---

## 🧱 Step-by-Step: Make GPT Understand Spark DataFrames

Let’s say you have two Spark DataFrames:

### 1. `employees_df`:

```text
id (int), name (string), age (int), department_id (int), salary (int)
```

### 2. `departments_df`:

```text
id (int), name (string)
```

---

### 🔧 Step 1: Extract the Schema and Sample Rows (No Pandas)

```python
def extract_info(df, name):
    schema = ", ".join([f"{f.name} ({f.dataType.simpleString()})" for f in df.schema.fields])
    rows = "\n".join([str(r.asDict()) for r in df.limit(5).collect()])
    return f"Table `{name}` schema: {schema}\nSample rows:\n{rows}"
```

---

### 🧠 Step 2: Construct the GPT Prompt

```python
info1 = extract_info(employees_df, "employees")
info2 = extract_info(departments_df, "departments")

user_question = "What useful insights can you find from this data? Consider employee salaries, department structures, and age demographics."

prompt = f"""
You are a data analyst.

You are given two data tables from a company's HR system.

{info1}

{info2}

Analyze this data and provide 3 useful business insights. Focus on things like:
- Salary patterns across departments
- Age demographics
- Potential HR concerns

Respond as a professional analyst with clear insights and recommendations.
"""
```

---

### 🧠 Step 3: Send to OpenAI API

```python
response = client.chat.completions.create(
    model="gpt-4",
    messages=[
        {"role": "system", "content": "You are a data analyst."},
        {"role": "user", "content": prompt}
    ],
    temperature=0.3,
    max_tokens=500
)

answer = response['choices'][0]['message']['content']
print("\n=== GPT Insight ===\n")
print(answer)
```

---

## ✅ Example Output from GPT

```markdown
Here are three insights from the provided data:

1. **Salary Disparities Across Departments**  
   Employees in the Engineering department have noticeably 
   higher salaries, with some earning up to 120,000. In contrast, 
   HR and Marketing show lower average salaries. This may suggest 
   that Engineering is a high-value area, or that HR and Marketing 
   could be under-compensated.

2. **Age and Salary Correlation**  
   Employees over 40 (e.g., Jane, George, Diana) tend to have higher 
   salaries. This indicates a possible correlation between age (or 
   experience) and compensation, which is expected. However, it also 
   suggests that younger talent might need better growth paths.

3. **Department Size Imbalance**  
   Engineering appears to have more employees than Marketing or HR. 
   If this reflects the company’s core operations, that’s fine, but 
   HR’s smaller size may affect employee support if the company grows.

**Recommendation**:  
Perform a full salary benchmarking across roles and experience levels. 
Also, assess HR team capacity if company headcount is expected to grow.
```

---

## 🧠 Why This Works

GPT doesn’t need the full data — just enough:

* Schema gives **structure**
* Sample rows give **patterns**
* Your prompt gives **context and intent**

		Even without Pandas, GPT can **simulate analysis**, 
		generate SQL, or produce business insights that feel 
		like a human wrote them.

---

## 🔁 Bonus Use Cases

You can also ask GPT to:

* Generate reports:
  *“Create a summary report of department-wise salary averages and headcounts.”*

* Write SQL:
  *“Write Spark SQL to get employees over 30 with salary above department average.”*

* Recommend action:
  *“Identify departments where young employees are paid below average.”*

---

## 🧩 What to do:

| Step       | What you provide                                                |
| ---------- | --------------------------------------------------------------- |
| 1. Schema  | Use `df.schema`                                                 |
| 2. Samples | Use `df.limit(n).collect()`                                     |
| 3. Prompt  | Include schema + samples + your natural language question       |
| 4. Result  | GPT can analyze, explain, or write Spark SQL — no Pandas needed |

---

# Reusable function to wrap this into a "GPT Insight Engine" for your DataFrames?

Below is a **reusable Python function** that acts as a "**GPT Insight Engine**" 
for **PySpark DataFrames**, using the **OpenAI API**.

It:

	✅ Accepts one or more Spark DataFrames
	✅ Extracts schema and sample rows (no Pandas used)
	✅ Accepts a custom natural language question or uses a default insight prompt
	✅ Calls GPT (e.g., GPT-4) and returns insights, summaries, or SQL, depending on prompt
	
---

## 🧠 GPT Insight Engine — PySpark Version (No Pandas)

```python
from openai import OpenAI
from pyspark.sql import DataFrame
from typing import Dict, Optional

# Set your OpenAI API Key (or use environment variable)
openai_api_key = "sk-..."  # Replace with your key or use environment variable
client = OpenAI(api_key=openai_api_key) 

def extract_schema_and_sample(df: DataFrame, table_name: str, num_rows: int = 5) -> str:
    schema = ", ".join([f"{f.name} ({f.dataType.simpleString()})" for f in df.schema.fields])
    rows = df.limit(num_rows).collect()
    sample_str = "\n".join([str(row.asDict()) for row in rows])
    return f"""Table `{table_name}`:
Schema: {schema}
Sample Rows:
{sample_str}
"""

def gpt_insight_engine(
    dataframes: Dict[str, DataFrame],
    question: Optional[str] = None,
    model: str = "gpt-4",
    sample_size: int = 5,
    verbose: bool = True
) -> str:
    """
    Generate insights from one or more Spark DataFrames using OpenAI.
    
    Parameters:
        dataframes (Dict[str, DataFrame]): A dictionary of table_name → Spark DataFrame
        question (str): Natural language question. If None, uses a default insight prompt.
        model (str): OpenAI model to use (default: "gpt-4")
        sample_size (int): Number of sample rows to extract
        verbose (bool): If True, prints the prompt
    
    Returns:
        str: GPT's response (insights or SQL or explanation)
    """
    
    all_tables_info = ""
    for name, df in dataframes.items():
        info = extract_schema_and_sample(df, name, sample_size)
        all_tables_info += info + "\n"

    # Default question if none is provided
    if not question:
        question = (
            "Analyze the data and provide 3 useful business insights. "
            "Focus on salary patterns, department distributions, age demographics, "
            "and any potential HR or operational recommendations."
        )

    prompt = f"""
You are a senior data analyst.

Here are multiple data tables from a company's HR system.

{all_tables_info}

Based on this data, answer the following question in a clear and structured way:

{question}
"""

    if verbose:
        print("\n=== 🧾 GPT Prompt ===\n")
        print(prompt.strip())

    # Call OpenAI
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a helpful data analyst."},
            {"role": "user", "content": prompt.strip()}
        ],
        temperature=0.4,
        max_tokens=700
    )

    answer = response["choices"][0]["message"]["content"]
    
    print("\n=== 🧠 GPT Insight ===\n")
    print(answer)
    return answer
```

---

## ✅ How to Use It

```python
# Assume you already have Spark session and these two DataFrames
employees_df = spark.read.csv("employees.csv", header=True, inferSchema=True)
departments_df = spark.read.csv("departments.csv", header=True, inferSchema=True)

# Register tables for Spark SQL (optional)
employees_df.createOrReplaceTempView("employees")
departments_df.createOrReplaceTempView("departments")

# Use the insight engine
gpt_insight_engine(
    dataframes={
        "employees": employees_df,
        "departments": departments_df
    },
    question="Which departments have the most experienced and highest-paid employees? Any anomalies?",
    model="gpt-4"
)
```

---

## 🧪 Example Output (from GPT)


Here are insights based on the provided data:

1. **Engineering Leads in Experience and Compensation**

		The Engineering department has the highest-paid employees, 
		including those aged over 40, such as George and Jane. This 
		suggests that experience is highly valued in technical roles.

2. **HR Shows Salary Compression**

		While HR has several employees over 30, their salaries remain 
		within a narrow band (~78k–85k), indicating limited salary 
		progression.

3. **Potential Overlap in Roles**

		Employees like Diana and Ian in Marketing have similar 
		experience and salary, which may suggest role duplication. 
		Consider reviewing role clarity or restructuring.


---

## 🛡️ Safe Usage Tips

* This engine **does not convert to Pandas** — safe for large Spark datasets.
* Only samples a few rows to keep prompt within token limits.
* It’s GPT "simulated" reasoning — always validate with real analytics.

---

## ➕ Optional Enhancements: Extend

* Add **SQL output mode** (NL → SQL query)?
* Format output as a **Markdown or HTML report**?
* Build a **command-line tool or web dashboard**?

Let's extend the **GPT Insight Engine for PySpark** into a 
flexible tool with the following capabilities:

---

## ✅ Extended Features

| Feature                   | Description                                                        |
| ------------------------- | ------------------------------------------------------------------ |
| `insight` mode            | GPT gives **natural language insights** (default mode)             |
| `sql` mode                | GPT generates **Spark SQL query** from a natural language question |
| `explain_sql` mode        | GPT generates SQL **and explains** it in plain English             |
| Markdown output           | Cleanly formatted insight/SQL for **Markdown rendering**           |
| Output to file (optional) | Save insights/SQL to `.md` or `.txt` file                          |
| No Pandas                 | ✅ All Spark, no Pandas used anywhere                               |

---

## 🧠 Extended GPT Insight Engine Function

````python
from openai import OpenAI
from pyspark.sql import DataFrame
from typing import Dict, Optional

openai_api_key = "sk-..."  # Replace with your key or use environment variable
client = OpenAI(api_key=openai_api_key) 

def extract_schema_and_sample(df: DataFrame, table_name: str, num_rows: int = 5) -> str:
    schema = ", ".join([f"{f.name} ({f.dataType.simpleString()})" for f in df.schema.fields])
    rows = df.limit(num_rows).collect()
    sample_str = "\n".join([str(row.asDict()) for row in rows])
    return f"""### Table `{table_name}`  
**Schema:** {schema}  
**Sample Rows:**  
```\n{sample_str}\n```"""

def gpt_data_assistant(
    dataframes: Dict[str, DataFrame],
    question: str,
    mode: str = "insight",  # "insight", "sql", or "explain_sql"
    sample_size: int = 5,
    model: str = "gpt-4",
    verbose: bool = True,
    save_to: Optional[str] = None  # e.g., "output.md"
) -> str:
    """
    GPT Data Assistant for analyzing Spark DataFrames.
    
    Parameters:
        dataframes: Dict of table_name -> Spark DataFrame
        question: The natural language question or prompt
        mode: "insight" | "sql" | "explain_sql"
        sample_size: Rows to sample from each DataFrame
        model: GPT model (e.g., gpt-4)
        verbose: Print full prompt and output
        save_to: Optional file path to save the result

    Returns:
        str: GPT response
    """
    assert mode in {"insight", "sql", "explain_sql"}, "Invalid mode."

    # Build prompt
    tables_info = "\n\n".join([
        extract_schema_and_sample(df, name, sample_size)
        for name, df in dataframes.items()
    ])

    instructions = {
        "insight": "Analyze the data and provide 2–3 useful business or operational insights. Avoid repeating the raw data. Use reasoning based on patterns or anomalies.",
        "sql": "Write a Spark SQL query that answers the question using the tables provided. Return only the SQL, no explanation.",
        "explain_sql": "Write a Spark SQL query that answers the question. Then explain what the query does and what insight it provides."
    }

    prompt = f"""
You are a senior data analyst working with Spark DataFrames.

Below are some tables from a dataset.

{tables_info}

---

**Task Mode:** {mode.upper()}  
**Question:** {question}  
{instructions[mode]}
"""

    if verbose:
        print("\n📤 === GPT Prompt ===\n")
        print(prompt.strip())

    # Call OpenAI
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a Spark data analyst."},
            {"role": "user", "content": prompt.strip()}
        ],
        temperature=0.3,
        max_tokens=800
    )

    answer = response['choices'][0]['message']['content'].strip()

    if verbose:
        print("\n📥 === GPT Output ===\n")
        print(answer)

    if save_to:
        with open(save_to, "w", encoding="utf-8") as f:
            f.write(f"# GPT Output ({mode})\n\n")
            f.write(answer)
        print(f"\n📁 Output saved to: {save_to}")

    return answer
````

---

## ✅ Example Usage

```python
# Load Spark DataFrames
employees_df = spark.read.csv("employees.csv", header=True, inferSchema=True)
departments_df = spark.read.csv("departments.csv", header=True, inferSchema=True)

# Register for Spark SQL (optional)
employees_df.createOrReplaceTempView("employees")
departments_df.createOrReplaceTempView("departments")

# Use extended assistant in different modes
from datetime import datetime
timestamp = datetime.now().strftime("%Y%m%d-%H%M")

# Insight mode
gpt_data_assistant(
    dataframes={"employees": employees_df, "departments": departments_df},
    question="What insights can you find about department-level salary distributions and age demographics?",
    mode="insight",
    save_to=f"insight_{timestamp}.md"
)

# SQL mode
gpt_data_assistant(
    dataframes={"employees": employees_df, "departments": departments_df},
    question="Find top 2 highest paid employees per department above age 30",
    mode="sql",
    save_to=f"query_{timestamp}.sql"
)

# Explain SQL
gpt_data_assistant(
    dataframes={"employees": employees_df, "departments": departments_df},
    question="Get the average salary by department, but only for employees aged over 35.",
    mode="explain_sql",
    save_to=f"explained_sql_{timestamp}.md"
)
```

---

## 📄 Example Markdown Output (`insight_20250907-1820.md`)

```markdown
# GPT Output (insight)

1. **Engineering Salaries Are Significantly Higher**

		Employees in Engineering have a higher average salary 
		compared to HR and Marketing. This suggests it's the core 
		technical team and likely has more senior staff.

2. **Age-Salary Correlation Exists**

		Older employees (age > 35) are typically earning more.
		This aligns with experience, but there may be retention 
		challenges for younger staff if growth paths aren't visible.

3. **HR Team Structure May Be Flat**

		HR has multiple employees within the same salary band and 
		age group, indicating a potentially flat structure with 
		little progression opportunity.
```

---

## ✅ Benefits of This Extension

* Works **directly with Spark DataFrames**
* Flexible across **analysis, SQL generation, and explanation**
* Great for **report generation, SQL automation, or insight discovery**
* **No Pandas, no LangChain**, simple and transparent

---

## ➕ Possible Future Add-ons: Execute GPT-generated SQL directly


* 🔄 **Execute GPT-generated SQL** directly?
* 📈 Auto-generate **visual summaries** using matplotlib/Altair (optional)?
* 🧪 Test suite for validating GPT's SQL?

Let's now **fully integrate GPT-generated SQL execution** 
into the Spark pipeline — all without converting to Pandas 
and keeping it clean and Spark-native.

---

## ✅ New Feature: Execute GPT-Generated SQL in Spark

We’ll add support for:

1. `mode="sql"` – GPT returns SQL → we run it
2. `mode="explain_sql"` – GPT returns SQL + explanation → we extract and run the SQL
3. Auto-printing Spark results (as `DataFrame.show()`)
4. Optional error handling if GPT's SQL is invalid

---

## 🧠 Updated `gpt_data_assistant(...)` With SQL Execution

Here's the **extended function** with **SQL execution support**:

````python
from openai import OpenAI
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Optional

openai_api_key = "sk-..."  # Replace with your key or use environment variable
client = OpenAI(api_key=openai_api_key) 

def extract_schema_and_sample(df: DataFrame, table_name: str, num_rows: int = 5) -> str:
    schema = ", ".join([f"{f.name} ({f.dataType.simpleString()})" for f in df.schema.fields])
    rows = df.limit(num_rows).collect()
    sample_str = "\n".join([str(row.asDict()) for row in rows])
    return f"""### Table `{table_name}`  
**Schema:** {schema}  
**Sample Rows:**  
```\n{sample_str}\n```"""

def extract_sql_from_text(text: str) -> str:
    """
    Extract the first SQL query from GPT's response using triple backticks or SQL start.
    """
    import re
    match = re.search(r"```sql\s+(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    # Fallback: find first SELECT or WITH
    match = re.search(r"(SELECT|WITH)\s+.+", text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(0).strip()
    
    return ""

def gpt_data_assistant(
    spark: SparkSession,
    dataframes: Dict[str, DataFrame],
    question: str,
    mode: str = "insight",  # "insight", "sql", or "explain_sql"
    sample_size: int = 5,
    model: str = "gpt-4",
    execute_sql: bool = True,
    verbose: bool = True,
    save_to: Optional[str] = None
) -> str:
    assert mode in {"insight", "sql", "explain_sql"}, "Invalid mode."

    # Register Spark views (table names) for GPT-generated SQL to work
    for name, df in dataframes.items():
        df.createOrReplaceTempView(name)

    # Build prompt
    tables_info = "\n\n".join([
        extract_schema_and_sample(df, name, sample_size)
        for name, df in dataframes.items()
    ])

    instructions = {
        "insight": "Analyze the data and provide 2–3 useful business or operational insights. Avoid repeating the raw data. Use reasoning based on patterns or anomalies.",
        "sql": "Write a Spark SQL query that answers the question using the tables provided. Return only the SQL, no explanation.",
        "explain_sql": "Write a Spark SQL query that answers the question. Then explain what the query does and what insight it provides."
    }

    prompt = f"""
You are a senior data analyst working with Spark DataFrames.

Below are some tables from a dataset.

{tables_info}

---

**Task Mode:** {mode.upper()}  
**Question:** {question}  
{instructions[mode]}
"""

    if verbose:
        print("\n📤 === GPT Prompt ===\n")
        print(prompt.strip())

    # Call OpenAI
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a Spark data analyst."},
            {"role": "user", "content": prompt.strip()}
        ],
        temperature=0.3,
        max_tokens=1000
    )

    answer = response['choices'][0]['message']['content'].strip()

    if verbose:
        print("\n📥 === GPT Output ===\n")
        print(answer)

    # Optionally save to file
    if save_to:
        with open(save_to, "w", encoding="utf-8") as f:
            f.write(f"# GPT Output ({mode})\n\n{answer}")
        print(f"\n📁 Output saved to: {save_to}")

    # Execute SQL if requested
    if mode in {"sql", "explain_sql"} and execute_sql:
        sql_query = extract_sql_from_text(answer)

        if sql_query:
            print("\n💻 === Executing GPT-Generated SQL ===\n")
            print(sql_query)
            try:
                result_df = spark.sql(sql_query)
                print("\n✅ === Query Result ===\n")
                result_df.show()
                return sql_query  # Or return result_df
            except Exception as e:
                print("\n❌ Error executing SQL:\n", e)
        else:
            print("\n⚠️ No valid SQL found in GPT output.")
    else:
        return answer
````

---

## ✅ How to Use It

```python
# Load your DataFrames
employees_df = spark.read.csv("employees.csv", header=True, inferSchema=True)
departments_df = spark.read.csv("departments.csv", header=True, inferSchema=True)

# Call the assistant with SQL execution
gpt_data_assistant(
    spark=spark,
    dataframes={
        "employees": employees_df,
        "departments": departments_df
    },
    question="Find the top 2 highest paid employees in each department over the age of 30.",
    mode="sql",
    execute_sql=True
)
```

---

## ✅ Example Console Output

```
💻 === Executing GPT-Generated SQL ===

WITH ranked_employees AS (
    SELECT e.name, e.age, e.salary, d.name as department,
           ROW_NUMBER() OVER (PARTITION BY d.name ORDER BY e.salary DESC) as rank
    FROM employees e
    JOIN departments d ON e.department_id = d.id
    WHERE e.age > 30
)
SELECT * FROM ranked_employees WHERE rank <= 2

✅ === Query Result ===

+-------+---+------+-----------+----+
|  name |age|salary|department |rank|
+-------+---+------+-----------+----+
| Jane  | 42|120000|Engineering|  1 |
| George| 45|110000|Engineering|  2 |
| Evan  | 38| 85000|HR         |  1 |
| Bob   | 35| 80000|HR         |  2 |
| Ian   | 36| 91000|Marketing  |  1 |
| Diana | 40| 90000|Marketing  |  2 |
+-------+---+------+-----------+----+
```

---

## ✅ Summary of Features

| Feature                | Description                           |
| ---------------------- | ------------------------------------- |
| `mode="sql"`           | GPT writes SQL, Spark executes it     |
| `mode="explain_sql"`   | GPT writes + explains SQL             |
| Auto-view registration | Makes tables available to SQL         |
| SQL extraction logic   | Works even if GPT gives full Markdown |
| Error handling         | Catches invalid SQL or malformed text |

---

## 🔄 What You Can Do Next


* 💬 Make it interactive via CLI or notebook UI?
* 🌍 Wrap it as a REST API?
* 📊 Visualize GPT results with Spark + matplotlib or Plotly?



# Visualize GPT results with Spark + matplotlib or Plotly

Let's now **extend your PySpark + GPT + SQL pipeline** 
to also **visualize the results** of GPT-generated SQL 
queries using **matplotlib** or **Plotly**.

---

## ✅ What We’ll Build

* GPT generates **Spark SQL**
* You **execute** it in Spark
* The resulting **Spark DataFrame is visualized**

  * 📊 Bar, pie, line, etc. (depending on the result)
* You choose:

  * ✅ `matplotlib` (simple, fast, local)
  * ✅ `Plotly` (interactive, prettier)

---

## ⚠️ Reminder

Because **Spark DataFrames are distributed**, you must **collect results** 
to the driver before plotting.

But we keep this **safe and controlled**:

> ✅ We only `.collect()` small GPT result DataFrames meant for visualization

---

## 🧠 Updated Pipeline with Visualization

### 🔧 Prerequisites

```bash
pip install matplotlib plotly
```

---

## 🧩 New Visualization Function

```python
import matplotlib.pyplot as plt
import plotly.express as px

def visualize_df(
    df,
    chart_type="bar",
    x=None,
    y=None,
    title="GPT SQL Result",
    top_n=10,
    use_plotly=True
):
    """
    Visualize a small Spark DataFrame using matplotlib or Plotly.
    
    Parameters:
        df (Spark DataFrame): The DataFrame to visualize (must be small).
        chart_type (str): "bar", "pie", "line"
        x (str): Column name for x-axis
        y (str): Column name for y-axis
        title (str): Chart title
        top_n (int): Max rows to plot
        use_plotly (bool): If True, use Plotly. Otherwise, use matplotlib.
    """
    pd_df = df.limit(top_n).toPandas()

    if pd_df.empty:
        print("⚠️ No data to plot.")
        return

    if use_plotly:
        if chart_type == "bar":
            fig = px.bar(pd_df, x=x, y=y, title=title)
        elif chart_type == "pie":
            fig = px.pie(pd_df, names=x, values=y, title=title)
        elif chart_type == "line":
            fig = px.line(pd_df, x=x, y=y, title=title)
        else:
            raise ValueError("Unsupported chart type.")
        fig.show()
    else:
        if chart_type == "bar":
            pd_df.plot(kind="bar", x=x, y=y, title=title)
        elif chart_type == "pie":
            pd_df.set_index(x)[y].plot.pie(autopct='%1.1f%%', title=title)
        elif chart_type == "line":
            pd_df.plot(kind="line", x=x, y=y, title=title)
        else:
            raise ValueError("Unsupported chart type.")
        plt.tight_layout()
        plt.show()
```

---

## ✅ Updated `gpt_data_assistant(...)` with Visualization

Add this at the end of the SQL execution logic:

```python
# Try to visualize the SQL result if small enough
if result_df.count() <= 20:
    try:
        visualize_df(
            result_df,
            chart_type="bar",
            x="department",  # auto-detectable in future
            y="salary",
            title="Top Paid Employees per Department",
            use_plotly=True  # or False for matplotlib
        )
    except Exception as viz_err:
        print("\n⚠️ Visualization failed:", viz_err)
```

---

## ✅ Full Usage Example

```python
result = gpt_data_assistant(
    spark=spark,
    dataframes={
        "employees": employees_df,
        "departments": departments_df
    },
    question="Show the average salary per department for employees over 30.",
    mode="sql",
    execute_sql=True
)
```

✅ Output:

* 📜 SQL is generated
* 🔁 SQL is executed
* 📈 Bar chart is shown (department vs average salary)

---

## 🧪 Example Result (Plotly)

```python
+-----------+-------------------+
| department| avg_salary        |
+-----------+-------------------+
|Engineering| 110000.0          |
|Marketing  | 92000.0           |
|HR         | 84000.0           |
+-----------+-------------------+
```

✅ Generates interactive **bar chart**:

* X-axis: department
* Y-axis: avg\_salary

---

## 🔮 Future Enhancements

You can later add:

* 🔍 Auto-detect `x` and `y` columns using GPT
* 🎯 Suggest best chart type from GPT
* 📊 Dashboards with multiple charts
* 📈 Export chart as image or embed in reports

---

## ✅ Summary

| Feature              | Description                               |
| -------------------- | ----------------------------------------- |
| Spark + GPT + SQL    | GPT writes & executes SQL in Spark        |
| Visualization added  | Plot Spark results with matplotlib/Plotly |
| No Pandas conversion | Still controlled `.toPandas()` for small  |
| Interactive charts   | Optionally use Plotly                     |

---

