# PySpark Integration with LLM using OpenAI Examples (Intro Slides)

Date: December 3, 2025

### 🧭 Suggested Classroom Flow

	1.	Explain slides 1–6: Concepts + setup
	2.	Run small live demo: Slides 7–10
	3.	Discuss performance constraints: Slides 11–14
	4.	Brainstorm use cases: Slide 15
	5.	Walk through best practices: Slides 17–19

## Slide 1 — Title
**PySpark Integration with LLM using OpenAI**
Introductory module with hands-on examples

---

## Slide 2 — Motivation
- **Many analytics workflows require both:**
  - Distributed computing (PySpark)
  - Natural language intelligence (LLMs)
- **Integration enables:**
  - Data summarization at scale
  - Automated insights
  - Text enrichment

---

## Slide 3 — Architecture Overview
- PySpark for distributed ETL + data processing
- OpenAI for intelligent text operations
- Typical data flow:
  1. Load data into Spark DataFrame
  2. Select text fragments
  3. Call LLM API
  4. Append enriched results

---

## Slide 4 — Setup Requirements
- Python 3.9+
- PySpark 3.x
- OpenAI Python SDK
- Env variables:
  ```bash
  export OPENAI_API_KEY="your_key"
  ```

---

## Slide 5 — Install Dependencies
```bash
pip install pyspark openai
```

---

## Slide 6 — Import Libraries
```python
from pyspark.sql import SparkSession
from openai import OpenAI

client = OpenAI()
spark = SparkSession.builder.appName("LLM").getOrCreate()
```

---

## Slide 7 — Sample Dataset
```python
data = [
    (1, "PySpark is a Python API for Apache Spark."),
    (2, "Models like GPT can generate insights from text.")
]
df = spark.createDataFrame(data, ["id", "text"])
df.show()
```

---

## Slide 8 — UDF-Based LLM Call (Simple)
```python
def summarize(text):
    resp = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[{"role":"user","content":f"Summarize: {text}"}]
    )
    return resp.choices[0].message.content
```

---

## Slide 9 — Register UDF
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

summarize_udf = udf(summarize, StringType())
```

---

## Slide 10 — Apply UDF
```python
result = df.withColumn("summary", summarize_udf(df.text))
result.show(truncate=False)
```

---

## Slide 11 — Notes on Performance
- UDF is executed row-by-row
- Each row triggers API call
- Very expensive
- Good for small datasets

---

## Slide 12 — Batch Processing Strategy
- Extract N rows
- Convert to local Pandas
- Send batch to LLM
- Merge back to Spark

---

## Slide 13 — Example Batch Workflow
```python
rows = df.limit(5).toPandas()
texts = list(rows["text"])
prompt = "Summarize each item:
" + "
".join(texts)
```

---

## Slide 14 — Send Batch to LLM
```python
resp = client.chat.completions.create(
    model="gpt-4.1",
    messages=[{"role":"user","content":prompt}]
)
print(resp.choices[0].message.content)
```

---

## Slide 15 — Use Cases
- Summarization
- Sentiment analysis
- Classification
- Entity extraction
- Code generation

---

## Slide 16 — Example: Sentiment Classification
```python
resp = client.chat.completions.create(
    model="gpt-4.1-mini",
    messages=[{"role":"user","content":"rate sentiment: great product"}]
)
```

---

## Slide 17 — Token + Cost Awareness
- Every token costs money
- Use small models when possible
- Avoid full documents

---

## Slide 18 — Common Pitfalls
- Timeouts
- Rate limits
- High cost for loops
- Using too-large prompts

---

## Slide 19 — Best Practice Checklist
- Limit rows
- Batch requests
- Cache results
- Monitor cost

---

## Slide 20 — Summary
- PySpark + LLM enables scalable text analytics
- Start simple, scale batch operations
- Focus on performance, cost, correctness
