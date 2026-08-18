Provided: 3 basic and 3 intermediate complete working examples of PySpark integration with LLM


---

## 🔧 PREREQUISITES (For All Examples)

```bash
pip install pyspark openai pandas
```

Set your OpenAI API key before running examples:

```python
import openai
openai.api_key = "your_openai_api_key"
```

Import base Spark session and required modules:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("LLMIntegration").getOrCreate()
```

---

## ✅ BASIC EXAMPLES

---

### 🟢 Example 1: Text Summarization with GPT

```python
data = [("Article about climate change and global warming effects",),
        ("Long post discussing new features in Python 3.11",)]
df = spark.createDataFrame(data, ["text"])

def summarize(text):
    prompt = f"Summarize the following text in 1 sentence:\n{text}"
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message["content"].strip()
    except Exception as e:
        return "Error"

summary_udf = udf(summarize, StringType())
df_result = df.withColumn("summary", summary_udf("text"))
df_result.show(truncate=False)
```

---

### 🟢 Example 2: Sentiment Classification of Reviews

```python
data = [("I love this product!",), ("Worst service ever.",)]
df = spark.createDataFrame(data, ["review"])

def classify_sentiment(review):
    prompt = f"Classify this text as Positive, Negative, or Neutral:\n{review}"
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message["content"].strip()
    except:
        return "Error"

sentiment_udf = udf(classify_sentiment, StringType())
df_result = df.withColumn("sentiment", sentiment_udf("review"))
df_result.show(truncate=False)
```

---

### 🟢 Example 3: Product Description Beautifier

```python
data = [("Basic white t-shirt, cotton",), ("Black running shoes with foam sole",)]
df = spark.createDataFrame(data, ["description"])

def beautify(desc):
    prompt = f"Rewrite this product description to be more appealing:\n{desc}"
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message["content"].strip()
    except:
        return "Error"

beautify_udf = udf(beautify, StringType())
df_result = df.withColumn("enhanced_description", beautify_udf("description"))
df_result.show(truncate=False)
```

---

## 🔶 INTERMEDIATE EXAMPLES

---

### 🟡 Example 4: Extract Entities (NER) from Text

```python
data = [("Apple launched a new iPhone in California.",),
        ("Elon Musk met with the German Chancellor.",)]
df = spark.createDataFrame(data, ["sentence"])

def extract_entities(text):
    prompt = f"Extract named entities (persons, locations, organizations) from this text:\n{text}"
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message["content"].strip()
    except:
        return "Error"

ner_udf = udf(extract_entities, StringType())
df_result = df.withColumn("entities", ner_udf("sentence"))
df_result.show(truncate=False)
```

---

### 🟡 Example 5: Generate SQL from Natural Language

```python
data = [("Get the average salary per department",),
        ("List top 5 products by revenue",)]
df = spark.createDataFrame(data, ["query_prompt"])

def to_sql(prompt):
    system_msg = "You are a SQL expert. Convert natural language to SQL using a table named 'sales_data'."
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": prompt}
            ]
        )
        return response.choices[0].message["content"].strip()
    except:
        return "Error"

sql_udf = udf(to_sql, StringType())
df_result = df.withColumn("generated_sql", sql_udf("query_prompt"))
df_result.show(truncate=False)
```

---

### 🟡 Example 6: Code Comment Generator for Spark Code

```python
data = [(
    """df = spark.read.csv("data.csv", header=True)
df = df.groupBy("category").agg({"sales": "sum"})"""
,)]
df = spark.createDataFrame(data, ["code_snippet"])

def generate_comment(code):
    prompt = f"Explain this PySpark code in plain English:\n{code}"
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message["content"].strip()
    except:
        return "Error"

comment_udf = udf(generate_comment, StringType())
df_result = df.withColumn("code_comment", comment_udf("code_snippet"))
df_result.show(truncate=False)
```

---

## ✅ Summary

| Level        | Task                         | Functionality               |
| ------------ | ---------------------------- | --------------------------- |
| Basic        | Summarize text               | NLP + PySpark               |
| Basic        | Classify sentiment           | Classification              |
| Basic        | Improve product descriptions | Text generation             |
| Intermediate | Extract named entities       | NER task with GPT           |
| Intermediate | Convert prompts to SQL       | Text-to-SQL generation      |
| Intermediate | Explain PySpark code         | Code understanding with LLM |

---

