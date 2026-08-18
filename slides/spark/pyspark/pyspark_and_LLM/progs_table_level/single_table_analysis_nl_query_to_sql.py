# Natural Language Query of PySpark DataFrames using OpenAI Integration

"""
### ✅ Description

We provide a complete running example of pyspark 
reading a CSV file (with some sample rows) and 
then create a dataframe, then use openAI integration 
by a complex natural language query to analyze data 
in dataframe (such as natural language to SQL).
You should not convert spark dataframe to panda's 
dataframe (because this will not scale out for large
data sets).

### 🔐 Security Note

Always sanitize GPT-generated SQL in production — 
here we're trusting the model since the schema and 
tables are predefined.


**complete working example**  that:

✅ Reads a CSV using **PySpark**
✅ Creates a Spark DataFrame (without converting to Pandas)
✅ Uses **OpenAI GPT-4** to analyze the data using a 
   **complex natural language query**, 
   such as **NL → SQL translation**
✅ Executes the SQL on the Spark DataFrame
✅ Returns and displays the result

---

### ✅ What You'll Get

You'll be able to ask natural language queries like:

> "Which department has the highest average salary for employees over 30?"

And the system will:

1. Send the query, schema, and sample data to GPT-4
2. GPT-4 generates a valid **Spark SQL query**
3. The SQL is run on the Spark DataFrame
4. Results are displayed


### ✅ Summary

This example:

* Keeps everything inside **Spark** (no Pandas conversion)
* Uses GPT-4 to convert **natural language to SQL**
* Executes GPT's query directly on the Spark DataFrame
* Returns the result programmatically


### 📦 Requirements

Install these Python packages:

pip install pyspark openai

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

#-----------------------------------------------------
# generate SQL for a given NL Query by calling LLM API
#-----------------------------------------------------
def generate_sql(client, nl_query_prompt):

    response = client.chat.completions.create(
        model="gpt-4",  # or another chat model like "gpt-3.5-turbo"
        messages=[
            {"role": "system", "content": "You are a Spark SQL expert."},
            {"role": "user", "content": nl_query_prompt}
        ],
        temperature=0,
        max_tokens=150
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
csv_filename = sys.argv[1]
print("csv_filename=", csv_filename)
# df = spark.read.csv("data.csv", header=True, inferSchema=True)
df = spark.read.csv(csv_filename, header=True, inferSchema=True)
print("=== Sample Data ===")
df.show(truncate=False)


# ========== 4. create a view of a DataFrame 
df.createOrReplaceTempView("employees")


# ========== 5. Extract Schema and Sample Data ==========
schema_str = ", ".join([f"{field.name} ({field.dataType.simpleString()})" for field in df.schema.fields])
sample_rows = df.limit(5).collect()
formatted_rows = "\n".join([str(row.asDict()) for row in sample_rows])

# ========== 6. User’s Natural Language Query ==========
user_question = "Which department has the highest average salary for employees over 30 years old?"

# ========== 7. Build Prompt for GPT ==========
prompt = f"""
You are a data analyst who translates English questions into Spark SQL queries.

Dataset name: employees

Schema:
{schema_str}

Sample rows:
{formatted_rows}

Now, translate the following question into a Spark SQL query (do not explain it, just give the SQL):

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

# ========== 9. Run the SQL on the Spark DataFrame ==========
try:
    result_df = spark.sql(cleaned_sql_query)
    print("\n=== ✅ Query Result ===")
    result_df.show(truncate=False)
except Exception as e:
    print("\n❌ Error executing SQL query:")
    print(e)


"""

### 🧪 Sample Output

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


"""

