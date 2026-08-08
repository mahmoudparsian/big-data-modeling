---
marp: true
theme: default
paginate: true
style: |
  :root {
    --color-navy:   #1B2A4A;
    --color-orange: #E8601C;
    --color-teal:   #1A7A8A;
    --color-slate:  #3D4F61;
    --color-lgray:  #8A9BB0;
    --color-llgray: #E8EDF3;
    --color-offwh:  #F7F9FC;
  }
  section {
    font-family: 'Calibri', 'Arial', sans-serif;
    background: #ffffff;
    color: var(--color-navy);
    font-size: 18px;
    padding: 48px 56px;
  }
  h1 {
    font-family: 'Cambria', 'Georgia', serif;
    color: var(--color-navy);
    font-size: 36px;
    margin-bottom: 0.15em;
    border-bottom: 2px solid var(--color-llgray);
    padding-bottom: 0.2em;
  }
  h2 {
    font-family: 'Cambria', 'Georgia', serif;
    color: var(--color-teal);
    font-size: 22px;
    margin: 0.5em 0 0.2em 0;
  }
  h3 {
    color: var(--color-orange);
    font-size: 16px;
    margin: 0.4em 0 0.1em 0;
  }
  code {
    font-family: 'Courier New', monospace;
    background: #F0F4F8;
    color: var(--color-navy);
    padding: 1px 5px;
    border-radius: 3px;
    font-size: 0.88em;
  }
  pre {
    background: #F0F4F8;
    border-left: 4px solid var(--color-orange);
    border-radius: 6px;
    padding: 14px 18px;
    font-size: 14px;
    line-height: 1.55;
    overflow: hidden;
  }
  pre code {
    background: none;
    padding: 0;
    font-size: inherit;
  }
  ul, ol { margin: 0.3em 0; padding-left: 1.4em; }
  li { margin: 0.22em 0; line-height: 1.45; }
  strong { color: var(--color-navy); }
  em { color: var(--color-teal); }
  table {
    border-collapse: collapse;
    width: 100%;
    font-size: 15px;
  }
  th {
    background: var(--color-navy);
    color: white;
    padding: 7px 12px;
    text-align: left;
  }
  td { padding: 6px 12px; border-bottom: 1px solid var(--color-llgray); }
  tr:nth-child(even) { background: var(--color-offwh); }
  .tag-orange {
    background: var(--color-orange);
    color: white;
    padding: 2px 10px;
    border-radius: 10px;
    font-size: 13px;
    font-weight: bold;
  }
  .tag-teal {
    background: var(--color-teal);
    color: white;
    padding: 2px 10px;
    border-radius: 10px;
    font-size: 13px;
  }
  .callout {
    background: var(--color-offwh);
    border-left: 4px solid var(--color-orange);
    border-radius: 6px;
    padding: 10px 16px;
    margin: 10px 0;
    font-size: 16px;
  }
  .callout-teal {
    background: #E8F4FD;
    border-left: 4px solid var(--color-teal);
    border-radius: 6px;
    padding: 10px 16px;
    margin: 10px 0;
    font-size: 16px;
  }
  .callout-warn {
    background: #FFF3CD;
    border-left: 4px solid #E8A000;
    border-radius: 6px;
    padding: 10px 16px;
    margin: 10px 0;
    font-size: 16px;
  }
  section.title-slide {
    background: var(--color-navy);
    color: white;
    display: flex;
    flex-direction: column;
    justify-content: center;
  }
  section.title-slide h1 {
    color: white;
    border-bottom: 2px solid var(--color-orange);
    font-size: 42px;
  }
  section.title-slide p { color: #CADCFC; font-size: 20px; }
  section.section-divider {
    background: var(--color-navy);
    color: white;
    display: flex;
    flex-direction: column;
    justify-content: center;
  }
  section.section-divider h1 {
    color: var(--color-orange);
    font-size: 72px;
    border: none;
    margin: 0;
  }
  section.section-divider h2 {
    color: white;
    font-size: 32px;
    margin-top: 0.1em;
  }
  section.section-divider p { color: #CADCFC; }
  footer {
    font-size: 11px;
    color: var(--color-lgray);
  }
---

<!-- _class: title-slide -->

# Integrating PySpark with AI / LLM
## The Correct and Efficient Way

*DataFrame Enrichment with Anthropic Claude · Patterns, Templates, and Anti-patterns*

---
Big Data Modeling · PySpark + LLM Module

---

# Why Integrate PySpark with an LLM?

PySpark processes data at scale. LLMs understand language. Together:

| PySpark Alone | LLM Alone | PySpark + LLM |
|---|---|---|
| Fast, parallel, structured | Slow, sequential, unstructured | Best of both |
| No language understanding | Can't process millions of rows | Millions of rows with language understanding |
| Schema-aware | Schema-oblivious | Schema-aware + semantic |

<div class="callout">

**The use case:** You have a Spark DataFrame with text columns — product reviews, support tickets, log messages, user bios — and you want to add **LLM-generated columns**: sentiment, category, summary, extracted entities, risk score.

</div>

### Concrete examples
- `reviews_df` → add `sentiment`, `summary` columns via Claude
- `tickets_df` → add `priority`, `root_cause` columns via Claude
- `products_df` → add `description_quality_score` column via Claude

---

# The Core Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     PySpark DataFrame                           │
│   ┌──────────┬──────────┬──────────────────────────────────┐   │
│   │ id       │ text     │  ← LLM enrichment columns →      │   │
│   │ 1        │ "Great!" │  sentiment  category  summary    │   │
│   │ 2        │ "Awful"  │  (empty)    (empty)   (empty)    │   │
│   │ ...      │ ...      │                                   │   │
│   └──────────┴──────────┴──────────────────────────────────┘   │
└──────────────────────────┬──────────────────────────────────────┘
                           │  pandas_udf  (the bridge)
                    ┌──────▼──────┐
                    │   Executor  │   ← one per partition
                    │  (Worker)   │   ← batches rows locally
                    └──────┬──────┘
                           │  HTTP  (batched)
                    ┌──────▼──────────────┐
                    │  Anthropic Claude   │
                    │  claude-sonnet-4-6  │
                    └─────────────────────┘
```

**Key principle:** the LLM call lives inside a `pandas_udf` so Spark can distribute it across partitions — each worker calls Claude independently and in parallel.

---

# The Two Integration Mechanisms

<div class="callout-warn">

⚠️ **Never put LLM calls inside a regular Python UDF.** Row-by-row UDFs serialize data to Python for every single record. For 1M rows that means 1M individual API calls with no batching.

</div>

## ✅ Use `pandas_udf` (the right way)

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
import pandas as pd

@pandas_udf(StringType())
def llm_sentiment(texts: pd.Series) -> pd.Series:
    # texts is a BATCH of rows, not one row
    results = [call_claude(t) for t in texts]
    return pd.Series(results)

df = df.withColumn("sentiment", llm_sentiment("review_text"))
```

## ✅ Use `mapInPandas` for multi-column output

```python
def enrich_partition(iterator):
    for pdf in iterator:
        pdf["sentiment"] = pdf["text"].apply(call_claude_sentiment)
        pdf["category"]  = pdf["text"].apply(call_claude_category)
        yield pdf

df = df.mapInPandas(enrich_partition, schema=new_schema)
```

---

# What Happens Inside the Executor

```
Partition 0 (Worker 0)           Partition 1 (Worker 1)
┌──────────────────────┐         ┌──────────────────────┐
│ rows 0–999           │         │ rows 1000–1999        │
│                      │         │                       │
│  pandas_udf called   │         │  pandas_udf called   │
│  with pd.Series of   │         │  with pd.Series of   │
│  1000 texts          │         │  1000 texts           │
│         │            │         │         │             │
│    [loop + batch]    │         │    [loop + batch]    │
│         │            │         │         │             │
│  Claude API ×        │         │  Claude API ×         │
│  ceil(1000/batch)    │         │  ceil(1000/batch)     │
└──────────────────────┘         └──────────────────────┘
         ↑                                ↑
   Runs in parallel            Runs in parallel
   (different cores/nodes)     (different cores/nodes)
```

<div class="callout">

`batch_size` controls how many rows are sent to Claude per API call.
Too small = too many API round-trips.  Too large = hit token limits.
**Recommended starting point: 10–25 rows per batch.**

</div>

---
<!-- _class: section-divider -->

# 01
## The Template

*The reusable pattern every PySpark + LLM job should follow*

---

---

# The `.env` File — All Secrets and Tunables in One Place

<div class="callout-warn">

⚠️ **Never hard-code API keys or model names in source code.**  
Use a `.env` file. Add `.env` to `.gitignore` — commit only `.env.example`.

</div>

## `.env.example` (safe to commit)

```ini
# Anthropic
ANTHROPIC_API_KEY=sk-ant-your-key-here
ANTHROPIC_MODEL=claude-sonnet-4-6

# Token & rate limits
LLM_MAX_TOKENS=300
LLM_RATE_LIMIT_RPM=50
LLM_BATCH_SIZE=5
LLM_MAX_CHARS=1500

# Spark
SPARK_MASTER=local[*]
SPARK_PARTITIONS=4
```

## Loading in every notebook

```python
import os
from dotenv import load_dotenv          # pip install python-dotenv

load_dotenv(override=False)             # reads .env; won't overwrite CI secrets

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL             = os.environ.get("ANTHROPIC_MODEL",    "claude-sonnet-4-6")
MAX_TOKENS        = int(os.environ.get("LLM_MAX_TOKENS", "300"))
```

> `override=False` means CI/CD secrets injected into the environment
> take precedence over the local `.env` file — the safe default.

---

# The Standard Template (Part 1 of 2)

```python
# ── pyspark_llm_template.py ──────────────────────────────────────────────
import os, re, time, anthropic
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import StringType, StructType, StructField

# ── 1. Configuration — load from .env (never hard-code secrets) ──────────
from dotenv import load_dotenv
load_dotenv()                           # reads .env from project root

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL             = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6")
MAX_TOKENS        = int(os.environ.get("LLM_MAX_TOKENS",      "256"))
BATCH_SIZE        = int(os.environ.get("LLM_BATCH_SIZE",      "20"))
RATE_LIMIT_RPM    = int(os.environ.get("LLM_RATE_LIMIT_RPM",  "50"))
SLEEP_BETWEEN     = 60 / RATE_LIMIT_RPM   # seconds between calls

# ── 2. Prompt factory (pure function — easy to test and swap) ────────────
def build_prompt(text: str, task: str) -> str:
    prompts = {
        "sentiment": (
            f"Classify the sentiment of this review as POSITIVE, NEGATIVE, "
            f"or NEUTRAL. Reply with exactly one word.\n\nReview: {text}"
        ),
        "category": (
            f"Classify this support ticket into one of: BILLING, TECHNICAL, "
            f"SHIPPING, OTHER. Reply with exactly one word.\n\nTicket: {text}"
        ),
    }
    return prompts[task]
```

---

# The Standard Template (Part 2 of 2)

```python
# ── 3. Single-record Claude call (handles errors + rate limiting) ─────────
def call_claude(text: str, task: str) -> str:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            messages=[{"role": "user",
                        "content": build_prompt(text, task)}]
        )
        time.sleep(SLEEP_BETWEEN)          # naive rate limiting
        return response.content[0].text.strip()
    except anthropic.RateLimitError:
        time.sleep(30)                     # back off and retry once
        return call_claude(text, task)
    except Exception as e:
        return f"ERROR: {e}"

# ── 4. pandas_udf factory (returns a typed UDF for any task) ─────────────
def make_llm_udf(task: str):
    @pandas_udf(StringType())
    def _udf(texts: pd.Series) -> pd.Series:
        return texts.apply(lambda t: call_claude(t, task))
    return _udf

# ── 5. Use it ─────────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("LLM-Enrichment").getOrCreate()
df    = spark.read.parquet("s3://my-bucket/reviews/")

sentiment_udf = make_llm_udf("sentiment")
category_udf  = make_llm_udf("category")

result = (df
    .withColumn("sentiment", sentiment_udf(col("review_text")))
    .withColumn("category",  category_udf(col("review_text")))
)
result.write.parquet("s3://my-bucket/reviews-enriched/")
```

---

# The Critical Configuration Decisions

| Decision | Naive Choice | Correct Choice | Why |
|---|---|---|---|
| **API key location** | Hardcoded in script | `os.environ["KEY"]` | Security; never in repo |
| **Client instantiation** | Once globally | Inside UDF (per executor) | Global objects don't serialize across workers |
| **Batch size** | 1 (row by row) | 10–25 rows | Amortize HTTP overhead |
| **Rate limiting** | None | `time.sleep()` + retry | Avoid 429 errors killing the job |
| **Error handling** | None | Try/catch → `"ERROR:..."` | One bad row must not fail the partition |
| **Token budget** | `max_tokens=4096` | Task-appropriate (64–512) | Cost and latency |
| **Partition count** | Default | Match to API concurrency | Too many = rate limit; too few = slow |
| **Output type** | `StringType` | Specific type or JSON | Downstream schema safety |

<div class="callout-warn">

⚠️ **The serialization trap:** If you instantiate `anthropic.Anthropic()` at module level, Python will try to pickle it and send it to executors. It will fail silently or with a cryptic error. **Always instantiate the client inside the UDF function body.**

</div>

---
<!-- _class: section-divider -->

# 02
## Anti-Patterns

*What NOT to do — and why it will hurt you at scale*

---

# Anti-Pattern 1: The Row-by-Row UDF

<div class="callout-warn">

❌ **Never do this at scale**

</div>

```python
# WRONG — regular UDF, called once per row, no batching
from pyspark.sql.functions import udf

@udf(returnType=StringType())
def bad_sentiment_udf(text):
    client = anthropic.Anthropic()     # also wrong: per-row client creation
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=256,
        messages=[{"role": "user", "content": text}]
    )
    return response.content[0].text

df.withColumn("sentiment", bad_sentiment_udf("review_text"))
```

### What goes wrong
- **1M rows = 1M API calls** with no batching — 10–100× slower
- **Client created per row** — TLS handshake overhead on every call
- **No rate limiting** — your job will be 429'd within seconds
- **No error handling** — one timeout kills the whole partition

---

# Anti-Pattern 2: The Global Client

```python
# WRONG — global client, not serializable across workers
import anthropic
client = anthropic.Anthropic(api_key="sk-...")   # ← module level

@pandas_udf(StringType())
def bad_udf(texts: pd.Series) -> pd.Series:
    # 'client' cannot be pickled and sent to executors
    return texts.apply(lambda t: client.messages.create(...))
```

```python
# CORRECT — client created inside the UDF, on the executor
@pandas_udf(StringType())
def good_udf(texts: pd.Series) -> pd.Series:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    return texts.apply(lambda t: call_with_client(client, t))
```

# Anti-Pattern 3: Ignoring Output Schema

```python
# WRONG — Claude returns JSON but you store it as a raw string
df.withColumn("llm_out", llm_udf("text"))   # col is '{"a":1,"b":2}' string

# CORRECT — parse the JSON and expand into typed columns
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("sentiment",  StringType(),  True),
    StructField("confidence", IntegerType(), True),
])
df.withColumn("llm_out", from_json(llm_udf("text"), schema)) \
  .select("*", "llm_out.*")
```

---

# Anti-Pattern 4: Wrong Partition Count

```python
# PROBLEM: Default partitions for a small file = 200 (Spark default)
# 200 executors all hammering Claude simultaneously → rate limit storm

spark.conf.set("spark.sql.shuffle.partitions", "200")  # dangerous for LLM jobs

df = spark.read.parquet("reviews.parquet")
print(df.rdd.getNumPartitions())   # → 200  ← too many for API rate limits
```

```python
# SOLUTION: Right-size partitions to your API rate limit
# If rate limit = 50 RPM and each partition takes ~30s,
# max concurrent partitions = 50 * 30/60 = 25

TARGET_PARTITIONS = 20   # conservative, tune per your tier

df = (spark.read.parquet("reviews.parquet")
          .repartition(TARGET_PARTITIONS))

# Or coalesce if you just want to shrink without full shuffle:
df = df.coalesce(TARGET_PARTITIONS)
```

<div class="callout">

**Rule of thumb:** `num_partitions ≈ (rate_limit_rpm × avg_partition_time_minutes)`

If Claude can do 50 requests/min and each partition takes 2 minutes: `50 × 2 = 100` max concurrent — but start conservative at 20–30 and monitor.

</div>

---
<!-- _class: section-divider -->

# 03
## Advanced Patterns

*Multi-output, structured JSON, caching, and cost control*

---

# Pattern: Multi-Column Enrichment in One Pass

Instead of calling Claude once per column (expensive), call it once and return JSON:

```python
import json

def build_multi_prompt(text: str) -> str:
    return f"""Analyze this product review and respond with ONLY a JSON object.
No explanation, no markdown, just the JSON.

Review: {text}

Required JSON format:
{{
  "sentiment":  "POSITIVE" | "NEGATIVE" | "NEUTRAL",
  "rating":     1-5 (integer),
  "topics":     ["topic1", "topic2"],
  "summary":    "one sentence summary"
}}"""

@pandas_udf(StringType())          # returns raw JSON string
def multi_enrich_udf(texts: pd.Series) -> pd.Series:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    def enrich_one(text):
        try:
            r = client.messages.create(model="claude-sonnet-4-6",
                max_tokens=256,
                messages=[{"role":"user","content":build_multi_prompt(text)}])
            json.loads(r.content[0].text)   # validate it's real JSON
            return r.content[0].text
        except Exception as e:
            return json.dumps({"sentiment":"ERROR","rating":0,
                               "topics":[],"summary":str(e)})
    return texts.apply(enrich_one)
```

---

# Pattern: Expanding JSON into Typed Columns

```python
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (StructType, StructField,
    StringType, IntegerType, ArrayType)

# Step 1: Define the expected schema
llm_schema = StructType([
    StructField("sentiment", StringType(),           nullable=True),
    StructField("rating",    IntegerType(),          nullable=True),
    StructField("topics",    ArrayType(StringType()), nullable=True),
    StructField("summary",   StringType(),           nullable=True),
])

# Step 2: Call LLM once, get raw JSON column
df_raw = df.withColumn("llm_json", multi_enrich_udf(col("review_text")))

# Step 3: Parse and expand — zero additional API calls
df_enriched = (df_raw
    .withColumn("llm", from_json(col("llm_json"), llm_schema))
    .select(
        col("id"),
        col("review_text"),
        col("llm.sentiment").alias("sentiment"),
        col("llm.rating").alias("rating"),
        col("llm.topics").alias("topics"),
        col("llm.summary").alias("summary"),
    )
)

df_enriched.show(5, truncate=40)
```

**Result:** 4 enriched columns from 1 Claude call per row.

---

# Pattern: Batch API Calls (True Batching)

Instead of one Claude call per row inside `apply()`, group rows into API batches:

```python
@pandas_udf(StringType())
def batched_sentiment_udf(texts: pd.Series) -> pd.Series:
    client  = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    results = []
    items   = texts.tolist()
    BATCH   = 10                             # rows per API call

    for i in range(0, len(items), BATCH):
        batch = items[i : i + BATCH]

        # Ask Claude to classify all rows in one prompt
        numbered = "\n".join(f"{j+1}. {t}" for j, t in enumerate(batch))
        prompt   = (
            f"Classify each review below as POSITIVE, NEGATIVE, or NEUTRAL.\n"
            f"Respond with exactly {len(batch)} lines, each containing ONLY "
            f"the classification for that review, in order.\n\n{numbered}"
        )
        r    = client.messages.create(model="claude-sonnet-4-6",
                   max_tokens=len(batch)*10,
                   messages=[{"role":"user","content":prompt}])
        lines = r.content[0].text.strip().split("\n")

        # Guard against malformed responses
        for k in range(len(batch)):
            results.append(lines[k].strip() if k < len(lines) else "UNKNOWN")

        time.sleep(1.2)                      # rate limit

    return pd.Series(results)
```

---

# Pattern: Cost and Token Guardrails

```python
# ── Guardrail 1: Skip rows that are too short to be meaningful ───────────
from pyspark.sql.functions import length, when

df_filtered = df.withColumn(
    "sentiment",
    when(length(col("text")) < 20, "TOO_SHORT")
    .otherwise(sentiment_udf(col("text")))
)

# ── Guardrail 2: Truncate long texts before sending ──────────────────────
MAX_CHARS = 2000   # ~500 tokens; well within Claude's window

@pandas_udf(StringType())
def safe_sentiment_udf(texts: pd.Series) -> pd.Series:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    def process(text):
        truncated = text[:MAX_CHARS]         # never send more than needed
        r = client.messages.create(model="claude-sonnet-4-6", max_tokens=16,
            messages=[{"role":"user","content":
                f"Classify as POSITIVE/NEGATIVE/NEUTRAL:\n{truncated}"}])
        return r.content[0].text.strip()
    return texts.apply(process)

# ── Guardrail 3: Estimate cost before running ─────────────────────────────
AVG_INPUT_TOKENS  = 150   # per row (prompt + text)
AVG_OUTPUT_TOKENS = 10    # per row (short classification)
PRICE_IN          = 3.0   # $ per million input tokens (claude-sonnet)
PRICE_OUT         = 15.0  # $ per million output tokens

row_count = df.count()
est_cost  = (row_count * AVG_INPUT_TOKENS  / 1_000_000 * PRICE_IN  +
             row_count * AVG_OUTPUT_TOKENS / 1_000_000 * PRICE_OUT)
print(f"Estimated cost for {row_count:,} rows: ${est_cost:.2f}")
```

---
<!-- _class: section-divider -->

# 04
## Scaling to Billions of Rows

*Why row-by-row LLM calls break — and what to do instead*

---

# The Scale Problem

A single Claude API call takes **0.5–3 seconds**. What happens at scale?

| Rows | 1 call/row (no batching) | Time (serial) | API cost (est.) |
|------|--------------------------|---------------|-----------------|
| 100 | 100 calls | ~2 min | $0.05 |
| 10,000 | 10,000 calls | ~3 hours | $5 |
| 1,000,000 | 1,000,000 calls | ~12 days | $500 |
| 1,000,000,000 | 1,000,000,000 calls | **~33 years** | **$500,000** |

<div class="callout-warn">

One API call per row does not scale. At billions of rows, you need fundamentally different strategies — not just more workers.

</div>

### The three bottlenecks
1. **Latency** — HTTP round-trip per call (~500ms minimum)
2. **Rate limits** — Claude caps requests per minute per API key
3. **Cost** — input + output tokens billed per call

---

# Strategy 1: Batch Multiple Rows per Call

Send N rows in a single prompt. Claude processes them together in one round-trip.

```
WITHOUT BATCHING                  WITH BATCHING (batch=20)
─────────────────                 ──────────────────────────
Row 1  → API call → result        Rows 1–20  → 1 API call → 20 results
Row 2  → API call → result        Rows 21–40 → 1 API call → 20 results
Row 3  → API call → result        Rows 41–60 → 1 API call → 20 results
...                                ...
Row 60 → API call → result

60 API calls                       3 API calls (20× fewer)
```

```python
# Send batch_size rows in one prompt, get a JSON array back
numbered = "\n".join(f"{i+1}. {t}" for i, t in enumerate(batch))
prompt   = f"Classify each of these {len(batch)} reviews...\n{numbered}"
```

<div class="callout">

**Impact:** Batch size of 20 → **20× fewer API calls**, **20× less HTTP overhead**, **~40% token savings** (shared prompt prefix paid once).

</div>

---

# Strategy 2: Filter Before Calling the LLM

Not every row needs an LLM. Use Spark operations to eliminate rows first.

```python
# ── BEFORE: call Claude on all 1B rows ────────────────────────────
df.withColumn("sentiment", llm_udf(col("text")))       # 1B API calls

# ── AFTER: filter first, call Claude only on what you need ─────────
df_needs_llm = (
    df
    .filter(col("text").isNotNull())                     # drop nulls
    .filter(length(col("text")) > 20)                    # skip tiny text
    .filter(col("existing_label").isNull())               # skip already labeled
    .dropDuplicates(["text"])                              # deduplicate
)

df_enriched = df_needs_llm.withColumn("sentiment", llm_udf(col("text")))

# Join back to the original DataFrame
result = df.join(df_enriched.select("id", "sentiment"), on="id", how="left")
```

| Step | Rows removed | Why |
|------|-------------|-----|
| Null filter | ~5% | Empty text = no value |
| Length filter | ~10% | Too short for meaningful classification |
| Already labeled | ~30% | Don't re-process what you have |
| Deduplication | ~15% | Same text = same answer |
| **Total reduction** | **~50%** | **Half the API calls eliminated** |

---

# Strategy 3: Classify Without the LLM (Where Possible)

Use rules or simple ML for the easy cases. Reserve Claude for the hard ones.

```
  All 1B rows
      │
      ├── 70% "easy" rows  ──→  Rule-based / ML classifier  (free, instant)
      │     Keywords, regex, existing model
      │
      └── 30% "hard" rows  ──→  Claude API  (accurate, costs money)
            Ambiguous, complex, nuanced
```

```python
from pyspark.sql.functions import when, lower, col

df_classified = df.withColumn(
    "sentiment",
    when(lower(col("text")).rlike("love|amazing|excellent|perfect"), "POSITIVE")
    .when(lower(col("text")).rlike("terrible|awful|worst|hate|garbage"), "NEGATIVE")
    .otherwise(None)    # NULL = "not sure, ask Claude"
)

# Only send ambiguous rows to Claude
ambiguous = df_classified.filter(col("sentiment").isNull())
resolved  = ambiguous.withColumn("sentiment", llm_udf(col("text")))

# Combine
result = df_classified.filter(col("sentiment").isNotNull()).union(resolved)
```

<div class="callout">

**Impact:** If rules handle 70% of rows, you cut LLM calls from 1B to **300M** — saving both cost and time.

</div>

---

# Strategy 4: Sample → Label → Train → Apply

Use Claude to **build a training set**, then replace it with a local model.

```
┌──────────────────────────────────────────────────────────────────┐
│  Step 1: Sample        1B rows  →  random 50,000 rows           │
│  Step 2: Label         50K rows →  Claude labels all 50K        │
│  Step 3: Train         50K labeled rows → train sklearn / Spark  │
│  Step 4: Apply         Trained model scores all 1B rows (free)  │
└──────────────────────────────────────────────────────────────────┘
```

```python
# Step 1 — Sample
sample_df = df.sample(fraction=50_000 / df.count(), seed=42)

# Step 2 — Label with Claude (only 50K calls, not 1B)
labeled_df = sample_df.withColumn("label", llm_udf(col("text")))

# Step 3 — Train a local model (runs on your cluster, no API)
from pyspark.ml.feature import Tokenizer, HashingTF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

pipeline = Pipeline(stages=[Tokenizer(...), HashingTF(...), LogisticRegression()])
model    = pipeline.fit(labeled_df)

# Step 4 — Score all 1B rows (zero API calls, pure Spark)
result = model.transform(df)
```

<div class="callout-teal">

**Impact:** 50K Claude calls instead of 1B → **20,000× fewer API calls**. The trained model runs entirely on your Spark cluster at full speed.

</div>

---

# Strategy 5: Cache and Checkpoint LLM Results

Never call Claude twice for the same input.

```python
# ── Hash each input text → use as a cache key ─────────────────────
from pyspark.sql.functions import md5, concat

df = df.withColumn("text_hash", md5(col("text")))

# ── Load previous results (if any) ────────────────────────────────
try:
    done = spark.read.parquet("llm_cache/")
    already_done = set(r.text_hash for r in done.select("text_hash").collect())
except:
    already_done = set()

# ── Only process new rows ─────────────────────────────────────────
new_rows   = df.filter(~col("text_hash").isin(already_done))
new_result = new_rows.withColumn("sentiment", llm_udf(col("text")))

# ── Append to cache ───────────────────────────────────────────────
new_result.write.mode("append").parquet("llm_cache/")
```

### Why this matters at scale
- Daily batch jobs with **90% overlap** → only 10% new rows need LLM calls
- A failed job that crashes at row 500K → **restart skips the first 500K**
- Deduplication across runs → **never pay twice for the same text**

---

# Strategy 6: Use the Anthropic Batch API

For large offline jobs, the [Anthropic Batch API](https://docs.anthropic.com/en/docs/build-with-claude/batch-processing) processes requests asynchronously at **50% lower cost**.

```
Standard API                      Batch API
────────────                      ─────────
Synchronous (wait for each)       Asynchronous (submit all, poll later)
Full price                        50% discount
Real-time results                 Results within 24 hours
Good for < 10K rows               Good for > 10K rows
```

```python
# Conceptual workflow (simplified)
# 1. Export rows to JSONL
# 2. Submit batch to Anthropic
# 3. Poll until complete
# 4. Load results back into Spark

batch = client.batches.create(requests=[...])   # submit all at once
# ... wait (up to 24 hours) ...
results = client.batches.results(batch.id)       # download results
```

<div class="callout-teal">

**Impact:** Same results, **50% cost reduction**. Best for nightly batch jobs where you don't need instant results.

</div>

---

# Combining Strategies: A Realistic Pipeline

For a real 1B-row job, you combine multiple strategies:

```
1B raw rows
    │
    ├─ Filter nulls, short text, duplicates ──→  500M rows remain    (Strategy 2)
    │
    ├─ Rule-based classification ─────────────→  350M classified     (Strategy 3)
    │                                              150M ambiguous
    │
    ├─ Check LLM cache ──────────────────────→  120M already done   (Strategy 5)
    │                                              30M need Claude
    │
    ├─ Batch 20 rows per API call ────────────→  1.5M API calls      (Strategy 1)
    │
    └─ Use Batch API at 50% discount ────────→  $X instead of $2X   (Strategy 6)
```

| Metric | Naive (1 call/row) | Optimized pipeline |
|--------|-------------------|-------------------|
| API calls | 1,000,000,000 | **1,500,000** |
| Reduction | — | **99.85%** |
| Cost | ~$500,000 | ~$375 |

<div class="callout">

**The key insight:** The LLM is the most expensive operation in your pipeline. Every row you can handle *without* calling it is pure savings.

</div>

---

# When to Use Each Strategy

| Strategy | Best for | Effort |
|----------|----------|--------|
| **1. Batching** | Any LLM job (always use this) | Low — change your prompt |
| **2. Pre-filtering** | Dirty data with nulls/dupes/short text | Low — a few Spark filters |
| **3. Rules first** | Tasks with clear keyword signals | Medium — domain knowledge needed |
| **4. Sample + Train** | Classification on very large datasets | High — ML pipeline setup |
| **5. Caching** | Recurring batch jobs with overlap | Medium — checkpoint logic |
| **6. Batch API** | Large offline jobs (no real-time need) | Low — API change only |

### Start here
1. **Always** batch (Strategy 1) — there's no reason not to
2. **Always** filter obvious cases (Strategy 2) — free savings
3. **Add caching** (Strategy 5) if you run the same job repeatedly
4. **Consider sample+train** (Strategy 4) when row count exceeds 1M

---
<!-- _class: section-divider -->

# 05
## Key Takeaways

*What every practitioner must remember*

---

# Summary: The Rules

## Architecture
- **`pandas_udf`** is the correct bridge — it processes batches, not rows
- **`mapInPandas`** when you need multi-column output or access to partition metadata
- **Instantiate the Claude client inside the UDF** — never at module scope

## Performance
- **Right-size partitions** to your API rate limit, not to data size
- **Batch rows** within each UDF call to amortize HTTP overhead
- **Truncate input text** to the minimum needed — saves tokens and latency

## Reliability
- **Always wrap Claude calls in try/except** — return a sentinel value, never let exceptions propagate to the partition
- **Implement backoff** on `RateLimitError` — exponential is better than fixed sleep
- **Validate JSON output** before storing — Claude occasionally hallucinates structure

## Cost Control
- **Estimate cost before running** — `row_count × avg_tokens × price`
- **Use `when().otherwise()`** to skip rows that don't need enrichment
- **Cache enriched DataFrames** with `.cache()` to avoid re-calling the API

---

# Quick Reference: Pattern Selector

| You need to… | Use this pattern |
|---|---|
| Add **one text column** (sentiment, category) | `pandas_udf(StringType())` |
| Add **multiple columns** from one LLM call | `pandas_udf` returning JSON → `from_json()` |
| Access **full partition** (e.g. prior rows for context) | `mapInPandas` |
| Process **millions of rows** efficiently | Batched prompt inside `pandas_udf` |
| Handle **rate limits** gracefully | `try/except RateLimitError` + exponential backoff |
| **Skip** rows that don't need LLM | `when(condition, literal).otherwise(udf())` |
| **Parse structured output** from Claude | `from_json(col, schema).select("*", "llm.*")` |
| **Estimate cost** before running | `row_count × tokens/row × $/token` |
| **Scale to billions** of rows | Filter + rules + sample/train + cache (Section 04) |
| **Cut cost 50%** on large batch jobs | Anthropic Batch API (async, 50% discount) |

<div class="callout">

📁 **Companion notebooks:**
- `program_1_basics.py` — First integration, single column, small dataset
- `program_2_intermediate.py` — Multi-column JSON enrichment, error handling, cost estimation
- `program_3_intermediate_plus.py` — Batched prompts, schema validation, production patterns

</div>

---

<!-- _footer: "Big Data Modeling · PySpark + LLM Integration · Anthropic Claude" -->

# Resources

| Resource | URL |
|---|---|
| Anthropic API docs | `docs.anthropic.com` |
| Claude model names | `docs.anthropic.com/en/docs/about-claude/models` |
| `pandas_udf` reference | `spark.apache.org/docs/latest/api/python` |
| Rate limits by tier | `docs.anthropic.com/en/api/rate-limits` |
| `anthropic` Python SDK | `github.com/anthropic-ai/anthropic-sdk-python` |

### Versions used in this module
- `pyspark >= 3.3`
- `anthropic >= 0.25`
- `pandas >= 1.5`
- Model: `claude-sonnet-4-6`

<div class="callout-teal">

**Next module:** Structured Streaming + LLM — enriching Kafka streams in near real-time with Claude, handling back-pressure, and writing to Delta Lake.

</div>
