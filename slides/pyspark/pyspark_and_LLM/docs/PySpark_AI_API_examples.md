# pyspark-ai


✅ [PySpark AI](https://github.com/pyspark-ai/pyspark-ai) 
lets you query Spark DataFrames **directly using natural 
language**, and it handles schema extraction, prompt formatting, 
and query execution.


✅ Provide a complete working example of pyspark-ai


---

# ✅ **Key Capabilities of PySpark AI**

* Natural language → Spark SQL
* LLMs supported: OpenAI, Azure OpenAI, Anthropic, Hugging Face, Cohere
* Works with **in-memory Spark DataFrames**
* Automatic schema extraction
* Spark SQL execution and display

---

# 🔧 **1. Install PySpark AI**

```bash
pip install pyspark-ai openai pyspark
```

---

# ✅ **2. Complete Example: Natural Language to Spark SQL**

```python
from pyspark.sql import SparkSession
from pyspark_ai import SparkAI

# -----------------------------------------
# 1. Initialize Spark Session
# -----------------------------------------
spark = SparkSession.builder \
    .appName("PySpark AI Example") \
    .master("local[*]") \
    .getOrCreate()

# -----------------------------------------
# 2. Initialize Spark AI with OpenAI
# -----------------------------------------
spark_ai = SparkAI(spark,
                   llm_type="openai",
                   openai_api_key="sk-your-api-key",
                   verbose=True)

# -----------------------------------------
# 3. Example DataFrame
# -----------------------------------------
df = spark.createDataFrame([
    (1, "John", "Engineering", 90000),
    (2, "Alice", "HR", 75000),
    (3, "Bob", "Engineering", 80000),
], ["emp_id", "name", "department", "salary"])

df.createOrReplaceTempView("employees")

# -----------------------------------------
# 4. Natural Language to Spark SQL
# -----------------------------------------

# Simple question
nl_query = "Show all employee names and salaries in the Engineering department."

# Generate Spark SQL and run
result = spark_ai.sql(nl_query)

# Show the results
result.show()

# -----------------------------------------
# 5. Get the Generated SQL (Optional)
# -----------------------------------------
print("\nGenerated SQL query:")
print(spark_ai.get_last_query())

```

---

# ✅ **3. Example Output**

### Console Output

```
[PySpark AI]: Generated Spark SQL query:
SELECT name, salary FROM employees WHERE department = 'Engineering'

+----+------+
|name|salary|
+----+------+
|John|90000 |
| Bob|80000 |
+----+------+
```

---

# 🔑 **4. Supported LLM Backends**

In the initialization:

```python
SparkAI(spark,
        llm_type="openai",           # or azure_openai, anthropic, huggingface, cohere
        openai_api_key="sk-your-api-key")
```

You can swap out OpenAI with Azure, Anthropic, etc.

---

# ✅ **5. Notes About Usage**

* SparkAI uses **schema introspection** from registered DataFrames.
* Behind the scenes, it prompts the LLM to generate Spark SQL.
* Queries are run using `spark.sql()`.
* You can **get the raw SQL query** with `spark_ai.get_last_query()`.

---

# 🔧 **6. Troubleshooting**

| Issue               | Fix                                              |
| ------------------- | ------------------------------------------------ |
| OpenAI auth error   | Set correct `openai_api_key`                     |
| DataFrame not found | Ensure you run `createOrReplaceTempView()`       |
| Long query times    | Optimize your OpenAI usage, or use `gpt-4-turbo` |
| Verbose output      | Set `verbose=False` if too noisy                 |

---

# ✅ **7. Next Steps**

✔️ Add **Streamlit frontend**
✔️ Add **LangChain agent compatibility**
✔️ Run on Databricks Spark clusters
✔️ Use **custom prompts** with SparkAI for better query generation

---

Would you like me to:

* Add a Streamlit or FastAPI frontend?
* Dockerize this example?
* Expand with **multi-table joins** and complex queries?


yes, Add a Streamlit or FastAPI frontend

Great! Here’s a **complete working solution with a Streamlit frontend** that lets you type natural language queries, and see the results from **PySpark AI**.
I'll also explain how you could switch to FastAPI if you prefer a REST API.

---

# ✅ **Streamlit + PySpark AI: Natural Language Query App**

---

## 🛠️ **1. Final Folder Structure**

```
pyspark_ai_app/
├── app.py               # Streamlit frontend
└── spark_ai_engine.py   # PySpark AI backend
```

---

## 🔧 **2. Install Required Libraries**

```bash
pip install streamlit pyspark-ai openai pyspark
```

---

## 🧠 **3. `spark_ai_engine.py`: Backend Logic**

```python
# spark_ai_engine.py
from pyspark.sql import SparkSession
from pyspark_ai import SparkAI

# Initialize Spark Session (Singleton Pattern)
spark = SparkSession.builder \
    .appName("PySpark AI App") \
    .master("local[*]") \
    .getOrCreate()

# Initialize Spark AI
spark_ai = SparkAI(spark,
                   llm_type="openai",
                   openai_api_key="sk-your-api-key",
                   verbose=True)

# Example data
def load_sample_data():
    employees = spark.createDataFrame([
        (1, "John", "Engineering", 90000),
        (2, "Alice", "HR", 75000),
        (3, "Bob", "Engineering", 80000),
    ], ["emp_id", "name", "department", "salary"])
    employees.createOrReplaceTempView("employees")

def query_with_nl(nl_query):
    df = spark_ai.sql(nl_query)
    sql = spark_ai.get_last_query()
    return df, sql

# Load example data on module import
load_sample_data()
```

---

## 🌐 **4. `app.py`: Streamlit Frontend**

```python
# app.py
import streamlit as st
from spark_ai_engine import query_with_nl

st.set_page_config(page_title="PySpark AI NL Query", layout="centered")
st.title("🔍 Natural Language Query with PySpark AI")

# Text input
user_query = st.text_input("Ask a question about your data:")

# Submit button
if st.button("Run Query") and user_query:
    try:
        with st.spinner("🤖 Generating Spark SQL and running query..."):
            df, generated_sql = query_with_nl(user_query)

        st.subheader("🔎 Generated SQL:")
        st.code(generated_sql, language="sql")

        st.subheader("📊 Results:")
        st.dataframe(df.toPandas())

    except Exception as e:
        st.error(f"Error: {e}")
```

---

## ▶️ **5. Run the App**

```bash
streamlit run app.py
```

Then open your browser at:

```
http://localhost:8501
```

---

## ✅ **Example Usage**

| **Natural Language Query**                       | **Generated Spark SQL**                                   |
| ------------------------------------------------ | --------------------------------------------------------- |
| Show all employees in Engineering.               | SELECT \* FROM employees WHERE department = 'Engineering' |
| List employee names and salaries.                | SELECT name, salary FROM employees                        |
| Show employees with a salary greater than 80000. | SELECT \* FROM employees WHERE salary > 80000             |

---

# ✅ **Optional: FastAPI Backend Version**

If you want a **FastAPI REST API** instead:

```bash
pip install fastapi uvicorn
```

Example FastAPI app:

```python
# api_app.py
from fastapi import FastAPI, HTTPException
from spark_ai_engine import query_with_nl

app = FastAPI()

@app.get("/query")
def run_query(q: str):
    try:
        df, generated_sql = query_with_nl(q)
        return {
            "sql_query": generated_sql,
            "data": df.toPandas().to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
```

Run with:

```bash
uvicorn api_app:app --reload
```

API Example:

```
GET http://localhost:8000/query?q=Show all employees in HR
```

---

# ✅ **What Next?**

✔️ Add **auth / API keys** to the frontend
✔️ Switch OpenAI model (`gpt-4o`, `gpt-4-turbo`, etc.)
✔️ Support your **real Spark datasets (CSV, Parquet, JDBC)**
✔️ Deploy on a server (Docker, EC2, Azure, Databricks)

---

## References

[ 1. PySpark AI (pyspark-ai) – English SDK Comprehensive Guide](https://sparkbyexamples.com/pyspark/pyspark-ai-comprehensive-guide/)

[ 2. Simplify Distributed ML with PySparkAI](https://www.kaggle.com/discussions/general/430038)


