# Batch Calls to OpenAI API


	In pyspark, let's say I have a DataFrame with 2 columns: 
	(id: int, review: string), using OpenAI API, I want to 
	summarize the "review" column, but I do not want to call 
	OpenAI API for every row of data. I want to batch 100 rows, 
	and then send 100 rows to OpenAI API (rhater than one by one). 


Here is a detailed working solution with sample I/O. 



# Detailed PySpark + OpenAI Batch Summarization


## 👌 Perfect use case

	Batching OpenAI API calls is exactly what you want in 
	order to avoid token explosion and rate limit issues 
	when summarizing text at scale. Let’s walk through a 
	detailed PySpark + OpenAI example that:

	1.	Creates a sample DataFrame with (id, review)
	2.	Groups reviews into batches of 100
	3.	Calls the OpenAI API once per batch
	4.	Returns summarized results back into a Spark DataFrame

⸻

## 📝 Detailed PySpark + OpenAI Batch Summarization

~~~python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, monotonically_increasing_id, floor
import openai

# ---------------------------
# 1. Initialize Spark
# ---------------------------
spark = SparkSession.builder.appName("BatchOpenAISummarization").getOrCreate()

# ---------------------------
# 2. Sample DataFrame
# ---------------------------
data = [
    (1, "The product was excellent, delivery was quick."),
    (2, "I didn’t like the service, it was very slow."),
    (3, "Amazing experience, will buy again."),
    # ... imagine thousands of rows ...
]
df = spark.createDataFrame(data, ["id", "review"])

df.show()
~~~


# Sample Input DataFrame

```
id	review
1	The product was excellent, delivery was quick.
2	I didn’t like the service, it was very slow.
3	Amazing experience, will buy again.
```

⸻

~~~python
# ---------------------------
# 3. Create batch IDs (every 100 rows = 1 batch)
# ---------------------------
df_with_batch = df.withColumn("batch_id", floor(monotonically_increasing_id() / 100))

# Group reviews into lists per batch
batched_df = df_with_batch.groupBy("batch_id").agg(collect_list("review").alias("reviews"))

batched_df.show(truncate=False)
~~~

# Example Batched DataFrame

```
batch_id	reviews (array of 100 strings)
0	[“The product was excellent…”, “I didn’t like…”, “Amazing experience…”]
1	[next 100 reviews…]
```

⸻

~~~python
# ---------------------------
# 4. Define OpenAI batch summarizer
# ---------------------------
openai.api_key = "YOUR_OPENAI_API_KEY"

def summarize_batch(reviews):
    """
    Summarize a list of reviews into a single short summary.
    """
    # Join reviews into one string with separators
    joined_reviews = "\n".join([f"- {r}" for r in reviews])

    prompt = f"""
    You are a helpful assistant. Summarize the following {len(reviews)} customer reviews into a concise paragraph:

    {joined_reviews}
    """

    response = openai.ChatCompletion.create(
        model="gpt-4o-mini",   # or gpt-4.1 / gpt-3.5-turbo
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        max_tokens=250
    )

    return response["choices"][0]["message"]["content"].strip()
~~~

⸻


~~~python
# ---------------------------
# 5. Apply summarization per batch
# ---------------------------
# Collect batched data to driver (safe if few batches; scale requires UDF+mapPartitions)
batched_data = batched_df.collect()

results = []
for row in batched_data:
    summary = summarize_batch(row["reviews"])
    results.append((row["batch_id"], summary))

# Create Spark DataFrame with summaries
summary_df = spark.createDataFrame(results, ["batch_id", "summary"])

summary_df.show(truncate=False)
~~~

⸻

# Sample Output DataFrame

```
batch_id	summary
0	“Customers generally liked the product quality and fast delivery, though some reported slow service. Overall sentiment is positive with repeat purchase intent.”
1	“Most reviews in this batch highlight …”
```

⸻

~~~python
# ---------------------------
# 6. (Optional) Join summaries back to original rows
# ---------------------------
final_df = df_with_batch.join(summary_df, on="batch_id", how="left") \
                        .select("id", "review", "summary")

final_df.show(truncate=False)
~~~

# Final DataFrame (reviews + batch-level summary)

```
id	review	summary
1	The product was excellent, delivery was quick.	Customers generally liked the product quality and fast delivery…
2	I didn’t like the service, it was very slow.	Customers generally liked the product quality and fast delivery…
3	Amazing experience, will buy again.	Customers generally liked the product quality and fast delivery…
```

⸻

# ⚡ Notes on Scaling
	•	This approach calls OpenAI once per 100 reviews instead of once per row.
	•	If you have millions of reviews, don’t use .collect() — instead, apply mapPartitions with API calls inside (streaming batches per partition).
	•	Watch OpenAI rate limits → you might need retries + exponential backoff if handling large volumes.
	•	You can adjust batch size (50, 100, 200) depending on token budget.

⸻

# 👉 Scalable version using mapPartitions()  

So it can run on large clusters without pulling everything to the driver)?

Let’s make this scalable for large datasets using mapPartitions.

```
Instead of collecting everything to the driver, we’ll:
	1.	Split the DataFrame into partitions (Spark handles this).
	2.	In each partition, group rows into batches of 100.
	3.	Call the OpenAI API inside the partition (parallel across executors).
	4.	Return (id, review, summary) per row.
```

# ⚡ Scalable PySpark + OpenAI Batch Summarization (mapPartitions)

~~~python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import openai

# ---------------------------
# 1. Spark setup
# ---------------------------
spark = SparkSession.builder.appName("ScalableBatchSummarization").getOrCreate()

# ---------------------------
# 2. Sample data
# ---------------------------
data = [
    (1, "The product was excellent, delivery was quick."),
    (2, "I didn’t like the service, it was very slow."),
    (3, "Amazing experience, will buy again."),
    # ... imagine millions of rows ...
]
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("review", StringType(), True),
])
df = spark.createDataFrame(data, schema)

df.show()

# ---------------------------
# 3. OpenAI summarizer function
# ---------------------------
openai.api_key = "YOUR_OPENAI_API_KEY"

def summarize_batch(reviews):
    """Summarize a list of reviews into a concise paragraph."""
    joined_reviews = "\n".join([f"- {r}" for r in reviews])
    prompt = f"""
    You are a helpful assistant. Summarize the following {len(reviews)} customer reviews into a concise paragraph:

    {joined_reviews}
    """

    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",   # or gpt-4.1
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=250
        )
        return response["choices"][0]["message"]["content"].strip()
    except Exception as e:
        return f"[Error calling API: {e}]"
        
# ---------------------------
# 4. mapPartitions implementation
# ---------------------------
def process_partition(iterator):
    buffer = []
    results = []

    for row in iterator:
        buffer.append((row.id, row.review))

        # If batch reaches 100 reviews, summarize
        if len(buffer) == 100:
            reviews = [r[1] for r in buffer]
            summary = summarize_batch(reviews)

            # Assign same summary to all rows in batch
            for r in buffer:
                results.append((r[0], r[1], summary))

            buffer = []  # reset buffer

    # Process remaining rows (if <100 at end)
    if buffer:
        reviews = [r[1] for r in buffer]
        summary = summarize_batch(reviews)
        for r in buffer:
            results.append((r[0], r[1], summary))

    return iter(results)

# ---------------------------
# 5. Apply to DataFrame
# ---------------------------
from pyspark.sql import Row

processed_rdd = df.rdd.mapPartitions(process_partition)

final_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("review", StringType(), True),
    StructField("summary", StringType(), True)
])

final_df = spark.createDataFrame(processed_rdd, schema=final_schema)

final_df.show(truncate=False)
~~~

⸻

✅ Example Output

```
id	review	summary
1	The product was excellent, delivery was quick.	Customers liked the product quality and fast delivery, though some complained of slow service.
2	I didn’t like the service, it was very slow.	Customers liked the product quality and fast delivery, though some complained of slow service.
3	Amazing experience, will buy again.	Customers liked the product quality and fast delivery, though some complained of slow service.
```
⸻

## ⚡ Notes
	•	This runs fully distributed — each Spark executor 
	    calls OpenAI for its partition.
	•	Batch size is set to 100 (tuneable depending 
	    on token limits).
	•	Make sure executors have internet access + OpenAI 
	    API key configured (via environment variables or 
	    Spark config).
	•	Add rate limiting + retry logic if running at scale 
	    to avoid hitting API limits.

⸻

## To DO Next
👉 Extend this so that all summaries are written per 
batch into a separate table (instead of repeating the 
same summary on every row)? That’s useful if you only 
need batch-level summaries, not per-row copies.

