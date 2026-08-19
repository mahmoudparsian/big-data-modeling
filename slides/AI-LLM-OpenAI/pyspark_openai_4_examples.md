# 4 Examples of PySpark Integration <br> with openAI LLM

	Here are **4 solid working examples** of 
	integrating PySpark with **OpenAI LLMs** 
	(e.g., GPT-4) to leverage the power of 
	distributed computing alongside advanced 
	language models. 
	
	Each example shows a practical use case in 
	a PySpark pipeline, and includes core code 
	and explanations.

---

## **Example 1: Text Enrichment in a Spark DataFrame using GPT**

**Use Case:** Enhance or summarize text fields (e.g., customer 
	reviews, support tickets) using GPT.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import openai

# Set up Spark and OpenAI
spark = SparkSession.builder.appName("GPTTextEnrichment").getOrCreate()
openai.api_key = "your-api-key"

# Sample DataFrame
data = [("The product is okay but arrived late.",),
        ("Excellent quality and fast delivery.",)]
df = spark.createDataFrame(data, ["review"])

# UDF to summarize text using GPT
def summarize_review(text):
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": f"Summarize this review: {text}"}],
        max_tokens=50
    )
    return response['choices'][0]['message']['content'].strip()

summarize_udf = udf(summarize_review, StringType())
df_enriched = df.withColumn("summary", summarize_udf("review"))

df_enriched.show(truncate=False)
```

---

## **Example 2: Content Classification with GPT inside PySpark**

**Use Case:** Classify textual data using GPT (e.g., categorize support requests).

```python
def classify_text(text):
    prompt = f"Classify the following support message into one category (Billing, Technical, Account):\n\n{text}"
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=20
    )
    return response["choices"][0]["message"]["content"].strip()

classify_udf = udf(classify_text, StringType())

support_data = [("I need help updating my payment method.",),
                ("My app keeps crashing when I open it.",)]
support_df = spark.createDataFrame(support_data, ["message"])

classified_df = support_df.withColumn("category", classify_udf("message"))
classified_df.show(truncate=False)
```

---

## **Example 3: Named Entity Recognition (NER) using GPT in a Data Pipeline**

**Use Case:** Extract named entities (people, companies, locations) from large text fields.

```python
def extract_entities(text):
    prompt = f"Extract named entities from the following text and return as a comma-separated list:\n\n{text}"
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=50
    )
    return response['choices'][0]['message']['content'].strip()

entity_udf = udf(extract_entities, StringType())

documents = [("Elon Musk announced a new Tesla factory in Germany.",),
             ("Microsoft and OpenAI signed a strategic partnership.",)]
docs_df = spark.createDataFrame(documents, ["document"])

ner_df = docs_df.withColumn("entities", entity_udf("document"))
ner_df.show(truncate=False)
```

---

## **Example 4: Translating Data at Scale using GPT in PySpark**

**Use Case:** Translate text in a multilingual dataset using GPT.

```python
def translate_to_english(text):
    prompt = f"Translate this text to English:\n\n{text}"
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=100
    )
    return response['choices'][0]['message']['content'].strip()

translate_udf = udf(translate_to_english, StringType())

foreign_texts = [("Hola, necesito ayuda con mi pedido.",),
                 ("Bonjour, j’ai un problème avec mon compte.",)]
foreign_df = spark.createDataFrame(foreign_texts, ["original_text"])

translated_df = foreign_df.withColumn("translated", translate_udf("original_text"))
translated_df.show(truncate=False)
```

---

### ⚠️ Notes:

* **API Calls**: These UDFs make HTTP requests to OpenAI's API, 
  which can be **slow** and **rate-limited** if not carefully managed.
  
* **Parallelization Caution**: PySpark UDFs do not naturally handle 
  external rate-limited services well. For production, consider:

  * Batching requests.
  * Using `foreachPartition` for grouped processing.
  * Throttling or queue-based architecture.
  
* **Alternatives**: For large-scale tasks, consider using GPT for 
  pre-processing outside Spark or on sampled subsets.

---

# Optimized Version using Batching 

To avoid hitting rate limits, you may use `foreachPartition` functions/transformations.

Here's an **optimized approach using `foreachPartition`** 
in PySpark to safely integrate with OpenAI's API without 
overwhelming it.

---

## ✅ Why `foreachPartition`?

Using `foreachPartition` allows:

* Fewer API sessions (vs. per-row UDFs).
* Controlled batching or throttling within each Spark partition.
* Better performance and rate-limit compliance.

---

## 🔧 Optimized Example: **Batch Summarization of Reviews**

Let's say you want to summarize customer reviews using GPT, 
but avoid hitting OpenAI's rate limit.

### ⚙️ Step-by-Step Code:

```python
from pyspark.sql import SparkSession
import openai
import time

# Initialize Spark session
spark = SparkSession.builder.appName("GPT_ForeachPartition").getOrCreate()
openai.api_key = "your-openai-api-key"

# Sample data
data = [
    ("The product was fine but arrived two days late.",),
    ("Absolutely loved the quality and the packaging.",),
    ("Terrible experience, the product broke in 3 days.",),
    ("Support was helpful but slow to respond.",),
]
df = spark.createDataFrame(data, ["review"])

# Create a temp directory to store results
import os
import uuid

TEMP_OUTPUT_DIR = f"/tmp/gpt_summaries_{uuid.uuid4()}"
os.makedirs(TEMP_OUTPUT_DIR, exist_ok=True)

# Function to process a partition and save locally
def process_partition(partition):
    import json

    results = []
    for row in partition:
        text = row.review
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[{"role": "user", "content": f"Summarize this review: {text}"}],
                max_tokens=50
            )
            summary = response['choices'][0]['message']['content'].strip()
        except Exception as e:
            summary = f"Error: {str(e)}"

        results.append({
            "original": text,
            "summary": summary
        })

        # Optional: delay between requests to respect rate limits
        time.sleep(1.2)

    # Save results to local file (1 per partition)
    partition_id = uuid.uuid4()
    file_path = os.path.join(TEMP_OUTPUT_DIR, f"part_{partition_id}.json")
    with open(file_path, 'w') as f:
        json.dump(results, f)

# Apply function to each partition
df.rdd.foreachPartition(process_partition)
```

---

## 📄 Step 2: Collecting and Reading the Results

After `foreachPartition`, all results are saved as local 
JSON files (1 per partition). Now, read and combine them.

```python
import json
import glob

def load_results(directory):
    files = glob.glob(os.path.join(directory, "*.json"))
    all_data = []

    for file in files:
        with open(file, 'r') as f:
            data = json.load(f)
            all_data.extend(data)

    return all_data

summarized_reviews = load_results(TEMP_OUTPUT_DIR)

# Display results
for item in summarized_reviews:
    print(f"Review: {item['original']}")
    print(f"Summary: {item['summary']}")
    print("-" * 60)
```

---

## ✅ Benefits of This Method

| Advantage              | Description                                             |
| ---------------------- | ------------------------------------------------------- |
| ✅ Rate-Limit Friendly | Easily add throttling or retries within a partition.    |
| ✅ Scalable            | Partitions handle multiple rows, reducing API calls.    |
| ✅ Robust              | Can log or store intermediate results.                  |
| ✅ Parallel            | Utilizes Spark’s parallelism without hammering the API. |

---

## 🧠 Want Even Smarter Optimization?

You can further enhance this:

* Add **retry logic** with exponential backoff.
* Implement **batching** (multiple reviews in one prompt).
* Use **asynchronous requests** with `aiohttp` 
  (if you run async I/O per partition).

---

This is a **batching version** that sends 3–5 reviews in a 
single prompt for **higher throughput**.

YES, batching multiple rows into a single GPT prompt is a 
**smart optimization** to reduce API calls and cost, and 
better handle rate limits.

---

## 🔄 Optimized Example: **Batching 3–5 Reviews per GPT Call**

Instead of summarizing each review individually, we’ll:

1. Group 3–5 reviews together.
2. Send them as a single prompt to GPT.
3. Parse the summarized output back.

---

### 🛠️ Setup

You’ll batch rows inside each Spark partition and call OpenAI with a prompt like:

> "Summarize the following reviews. Return each summary on a new line..."

---

### ✅ Full Working Code

```python
from pyspark.sql import SparkSession
import openai
import time
import os
import uuid
import json

# Set up Spark and OpenAI
spark = SparkSession.builder.appName("GPT_Batching").getOrCreate()
openai.api_key = "your-api-key"

# Sample Data
data = [
    ("The product was fine but arrived two days late.",),
    ("Absolutely loved the quality and the packaging.",),
    ("Terrible experience, the product broke in 3 days.",),
    ("Support was helpful but slow to respond.",),
    ("Great price and fast delivery!",),
    ("Wrong item delivered. Not happy.",),
    ("Very durable and well-designed.",),
    ("Customer service resolved my issue quickly.",),
]
df = spark.createDataFrame(data, ["review"])

# Temp directory to save results
TEMP_OUTPUT_DIR = f"/tmp/gpt_batch_output_{uuid.uuid4()}"
os.makedirs(TEMP_OUTPUT_DIR, exist_ok=True)

# GPT batching function
def process_partition_batched(partition, batch_size=4):
    partition = list(partition)
    batches = [partition[i:i+batch_size] for i in range(0, len(partition), batch_size)]
    results = []

    for batch in batches:
        reviews = [row.review for row in batch]
        joined_reviews = "\n".join([f"{i+1}. {text}" for i, text in enumerate(reviews)])
        prompt = f"Summarize the following reviews. Return each summary as a numbered list:\n\n{joined_reviews}"

        try:
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=300
            )
            summaries_raw = response['choices'][0]['message']['content'].strip()

            # Try to split response into lines
            summaries = summaries_raw.split('\n')
            for original, summary in zip(reviews, summaries):
                results.append({
                    "original": original,
                    "summary": summary.strip()
                })

        except Exception as e:
            for original in reviews:
                results.append({
                    "original": original,
                    "summary": f"Error: {str(e)}"
                })

        # Optional: Delay for rate limiting
        time.sleep(2)

    # Save batch results
    partition_id = uuid.uuid4()
    file_path = os.path.join(TEMP_OUTPUT_DIR, f"part_{partition_id}.json")
    with open(file_path, 'w') as f:
        json.dump(results, f)

# Apply batching per partition
df.rdd.foreachPartition(lambda partition: process_partition_batched(partition, batch_size=4))
```

---

### 📥 Collect Results

```python
import glob

def load_batch_results(directory):
    files = glob.glob(os.path.join(directory, "*.json"))
    all_data = []
    for file in files:
        with open(file, 'r') as f:
            all_data.extend(json.load(f))
    return all_data

batch_results = load_batch_results(TEMP_OUTPUT_DIR)

# Display summaries
for item in batch_results:
    print(f"Review: {item['original']}")
    print(f"Summary: {item['summary']}")
    print("-" * 60)
```

---

### 🔍 Sample GPT Prompt Sent

```text
Summarize the following reviews. Return each summary as a numbered list:

1. The product was fine but arrived two days late.
2. Absolutely loved the quality and the packaging.
3. Terrible experience, the product broke in 3 days.
4. Support was helpful but slow to respond.
```

### ✅ Expected GPT Response

```text
1. Good product, but delivery was delayed.
2. High-quality product with great packaging.
3. Poor quality; product failed quickly.
4. Support was useful but not timely.
```

---

## 🧠 Benefits of Batching

| Feature            | Benefit                                           |
| ------------------ | ------------------------------------------------- |
| 🧾 Fewer API Calls | Summarize 3–5 reviews in one call.                |
| 💸 Lower Cost      | Save tokens and money.                            |
| ⚡ Faster           | Parallelize per partition + reduce I/O.           |
| 📏 More Control    | Can format prompt and parse outputs consistently. |

---

# Write this to a **final Spark DataFrame** instead of JSON:

Let's now modify the batching process so that **each partition returns data** 
back to Spark directly, and you can **collect or save it as a Spark DataFrame** 
(instead of writing to disk as JSON files).

---

## ✅ Goal:

* Run GPT summarization in **batches** per partition.
* Return the results as a **new Spark DataFrame** with 
  `original` and `summary` columns.

---

## ⚙️ Plan:

We'll use `mapPartitions` instead of `foreachPartition`, so each partition 
returns rows to the driver or next stage in the pipeline.

---

### ✅ Updated Code Using `mapPartitions`

```python
from pyspark.sql import SparkSession
import openai
import time

# Initialize Spark session
spark = SparkSession.builder.appName("GPT_Batched_To_DataFrame").getOrCreate()
openai.api_key = "your-api-key"

# Sample review data
data = [
    ("The product was fine but arrived two days late.",),
    ("Absolutely loved the quality and the packaging.",),
    ("Terrible experience, the product broke in 3 days.",),
    ("Support was helpful but slow to respond.",),
    ("Great price and fast delivery!",),
    ("Wrong item delivered. Not happy.",),
    ("Very durable and well-designed.",),
    ("Customer service resolved my issue quickly.",),
]
df = spark.createDataFrame(data, ["review"])
```

---

### 🧠 GPT Batching Function (Using `mapPartitions`)

```python
def process_partition_to_rows(partition, batch_size=4):
    import openai
    import time

    openai.api_key = "your-api-key"
    partition = list(partition)
    batches = [partition[i:i+batch_size] for i in range(0, len(partition), batch_size)]

    for batch in batches:
        reviews = [row.review for row in batch]
        prompt = "\n".join([f"{i+1}. {review}" for i, review in enumerate(reviews)])
        full_prompt = f"Summarize the following reviews. Return each summary as a numbered list:\n\n{prompt}"

        try:
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[{"role": "user", "content": full_prompt}],
                max_tokens=300
            )
            summaries_raw = response['choices'][0]['message']['content'].strip()
            summaries = summaries_raw.split("\n")

            for original, summary in zip(reviews, summaries):
                yield (original, summary.strip())

        except Exception as e:
            for original in reviews:
                yield (original, f"Error: {str(e)}")

        time.sleep(2)  # Respect rate limits
```

---

### 🔁 Apply `mapPartitions` and Convert to DataFrame

```python
from pyspark.sql.types import StructType, StructField, StringType

# Apply mapPartitions
rdd_results = df.rdd.mapPartitions(lambda partition: process_partition_to_rows(partition, batch_size=4))

# Define schema for the new DataFrame
schema = StructType([
    StructField("original", StringType(), True),
    StructField("summary", StringType(), True),
])

# Create new DataFrame
summary_df = spark.createDataFrame(rdd_results, schema)

# Show results
summary_df.show(truncate=False)
```

---

### ✅ Output Sample

```
+------------------------------------------------------------+----------------------------------------------------+
|original                                                    |summary                                             |
+------------------------------------------------------------+----------------------------------------------------+
|The product was fine but arrived two days late.             |Good product, but delivery was delayed.             |
|Absolutely loved the quality and the packaging.             |High-quality product with great packaging.          |
|Terrible experience, the product broke in 3 days.           |Poor quality; product failed quickly.               |
|Support was helpful but slow to respond.                    |Support was useful but not timely.                  |
|...                                                         |...                                                 |
+------------------------------------------------------------+----------------------------------------------------+
```

---

## ✅ What You Can Do Next

* 💾 Save to file: `summary_df.write.csv(...)` or `.parquet(...)`
* 📈 Use for analytics: join with other data, send to BI tools
* 🧪 Evaluate GPT summaries using manual or automatic QA

---
