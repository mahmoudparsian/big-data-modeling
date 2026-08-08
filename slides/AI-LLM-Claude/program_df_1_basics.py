import marimo

__generated_with = "0.23.10"
app = marimo.App(
    width="full",
    app_title="PySpark + Claude LLM — Program 1: Basics",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 🚀 Program 1: PySpark + Claude LLM — Basics

    **Goal:** Add a single LLM-generated column (`sentiment`) to a PySpark DataFrame
    using `pandas_udf` and Anthropic's Claude.

    This notebook covers:
    - Loading configuration from a `.env` file
    - Writing a `pandas_udf` that calls Claude
    - Applying it to a small product-review dataset
    - Inspecting and saving the enriched DataFrame

    ---
    > **Setup:**
    > ```bash
    > cp .env.example .env   # then fill in your API key
    > pip install pyspark anthropic python-dotenv marimo
    > marimo edit program_1_basics.py
    > ```
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 0 — Imports
    """)
    return


@app.cell
def _():
    import os, time, pathlib
    import anthropic
    import pandas as pd
    import marimo as mo

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import pandas_udf, col, length, when
    from pyspark.sql.types import StringType, StructType, StructField

    # Anchor all relative paths (data/, .env) to the notebook's own directory
    NOTEBOOK_DIR = str(pathlib.Path(__file__).resolve().parent)

    return (
        SparkSession,
        StringType,
        NOTEBOOK_DIR,
        anthropic,
        col,
        length,
        mo,
        os,
        pandas_udf,
        pd,
        time,
        when,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 1 — Load Configuration from `.env`

    All secrets and tunables live in `.env` — never in code.

    ```
    # .env
    ANTHROPIC_API_KEY=sk-ant-...
    ANTHROPIC_MODEL=claude-sonnet-4-6
    LLM_MAX_TOKENS=16
    LLM_RATE_LIMIT_RPM=50
    SPARK_MASTER=local[*]
    SPARK_PARTITIONS=4
    ```
    """)
    return


@app.cell
def _(NOTEBOOK_DIR, os):
    from dotenv import load_dotenv

    # Use an explicit path anchored to the notebook's directory so that
    # .env is found regardless of the working directory Marimo launches from.
    # override=False means already-set env vars are NOT overwritten —
    # useful when CI/CD injects secrets directly into the environment.
    loaded = load_dotenv(os.path.join(NOTEBOOK_DIR, ".env"), override=False, verbose=True)
    print(f"{'✅' if loaded else '⚠️ '} .env {'loaded' if loaded else 'not found — falling back to shell environment'}")

    # ── Read every config value here, once, in one place ─────────────────
    ANTHROPIC_API_KEY  = os.environ.get("ANTHROPIC_API_KEY",  "")
    MODEL              = os.environ.get("ANTHROPIC_MODEL",     "claude-sonnet-4-6")
    MAX_TOKENS         = int(os.environ.get("LLM_MAX_TOKENS",  "16"))
    RATE_LIMIT_RPM     = int(os.environ.get("LLM_RATE_LIMIT_RPM", "50"))
    SPARK_MASTER       = os.environ.get("SPARK_MASTER",        "local[*]")
    SPARK_PARTITIONS   = int(os.environ.get("SPARK_PARTITIONS","4"))

    SLEEP_SEC = 60 / RATE_LIMIT_RPM   # seconds between API calls

    # ── Validation ────────────────────────────────────────────────────────
    if not ANTHROPIC_API_KEY:
        print("⚠️  ANTHROPIC_API_KEY is not set. Add it to your .env file.")
    else:
        print(f"✅  ANTHROPIC_API_KEY : {ANTHROPIC_API_KEY[:12]}...")

    print(f"   ANTHROPIC_MODEL    : {MODEL}")
    print(f"   LLM_MAX_TOKENS     : {MAX_TOKENS}")
    print(f"   LLM_RATE_LIMIT_RPM : {RATE_LIMIT_RPM}  → sleep {SLEEP_SEC:.2f}s between calls")
    print(f"   SPARK_MASTER       : {SPARK_MASTER}")
    print(f"   SPARK_PARTITIONS   : {SPARK_PARTITIONS}")
    return (
        ANTHROPIC_API_KEY,
        MAX_TOKENS,
        MODEL,
        SLEEP_SEC,
        SPARK_MASTER,
        SPARK_PARTITIONS,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 2 — Start Spark
    """)
    return


@app.cell
def _(SPARK_MASTER, SPARK_PARTITIONS, SparkSession):
    spark = (
        SparkSession.builder
        .appName("Program1_LLM_Basics")
        .master(SPARK_MASTER)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", str(SPARK_PARTITIONS))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print(f"✅  Spark {spark.version}  |  master: {spark.sparkContext.master}")
    return (spark,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 3 — Sample Dataset
    """)
    return


@app.cell
def _(SPARK_PARTITIONS, NOTEBOOK_DIR, os, spark):
    reviews_df = (
        spark.read.csv(os.path.join(NOTEBOOK_DIR, "data", "reviews.csv"), header=True, inferSchema=True)
            .repartition(min(SPARK_PARTITIONS, 2))
    )

    print(f"Dataset: {reviews_df.count()} reviews  |  "
          f"{reviews_df.rdd.getNumPartitions()} partition(s)")
    reviews_df.show(truncate=60)
    return (reviews_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 4 — Prompt Builder and Claude Call

    Two separate functions so each is independently testable:
    - `build_sentiment_prompt(text)` — pure, no side effects
    - `call_claude_sentiment(text)` — API call, error-safe
    """)
    return


@app.cell
def _(ANTHROPIC_API_KEY, MAX_TOKENS, MODEL, SLEEP_SEC, anthropic, time):
    def build_sentiment_prompt(text: str) -> str:
        return (
            "Classify the sentiment of the following product review.\n"
            "Reply with EXACTLY ONE WORD: POSITIVE, NEGATIVE, or NEUTRAL.\n"
            "No explanation, no punctuation — just the single word.\n\n"
            f"Review: {text}"
        )

    def call_claude_sentiment(text: str) -> str:
        """
        Returns 'POSITIVE', 'NEGATIVE', 'NEUTRAL', 'TOO_SHORT', or 'ERROR:<msg>'.
        The Claude client is instantiated HERE — never at module scope,
        because Spark cannot serialize a module-level Anthropic client
        to send across executor processes.
        """
        if not text or len(text.strip()) < 5:
            return "TOO_SHORT"
        try:
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            response = client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                messages=[{"role": "user",
                            "content": build_sentiment_prompt(text)}]
            )
            time.sleep(SLEEP_SEC)
            result = response.content[0].text.strip().upper()
            return result if result in {"POSITIVE", "NEGATIVE", "NEUTRAL"} \
                   else f"UNEXPECTED:{result[:30]}"
        except anthropic.RateLimitError:
            time.sleep(30)
            return call_claude_sentiment(text)
        except Exception as e:
            return f"ERROR:{str(e)[:60]}"

    # ── Local test (no Spark) ─────────────────────────────────────────────
    _test = "This product is absolutely wonderful!"
    print(f"Local test → {call_claude_sentiment(_test) if ANTHROPIC_API_KEY else '(skipped — no API key)'}")
    return (call_claude_sentiment,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 5 — `pandas_udf` and DataFrame Enrichment
    """)
    return


@app.cell
def _(
    StringType,
    call_claude_sentiment,
    col,
    length,
    pandas_udf,
    pd,
    reviews_df,
    when,
):
    @pandas_udf(StringType())
    def sentiment_udf(texts: pd.Series) -> pd.Series:
        return texts.apply(call_claude_sentiment)

    enriched_df = reviews_df.withColumn(
        "sentiment",
        when(length(col("review_text")) < 10, "TOO_SHORT")
        .otherwise(sentiment_udf(col("review_text")))
    )

    print("Enriched DataFrame:")
    enriched_df.select("id", "product_type", "review_text", "sentiment").show(truncate=55)
    return (enriched_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 6 — Summary and Save
    """)
    return


@app.cell
def _(col, enriched_df):
    from pyspark.sql.functions import count

    print("=== Sentiment Distribution ===")
    (enriched_df
     .groupBy("sentiment")
     .agg(count("*").alias("count"))
     .orderBy(col("count").desc())
    ).show()
    return


@app.cell
def _(enriched_df):
    import tempfile, os as _os
    out_path = _os.path.join(tempfile.gettempdir(), "reviews_enriched_p1")
    enriched_df.coalesce(1).write.mode("overwrite").option("header","true").csv(out_path)
    print(f"✅  Saved → {out_path}")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Summary

    | Rule | How it's applied here |
    |------|----------------------|
    | Secrets in `.env` | `load_dotenv()` + `os.environ.get()` |
    | Client inside UDF | `anthropic.Anthropic()` called in `call_claude_sentiment` body |
    | Error-safe | Returns sentinel string, never raises |
    | Short-text guard | `when(length < 10, "TOO_SHORT").otherwise(udf())` |
    | Rate limiting | `time.sleep(60 / RATE_LIMIT_RPM)` after each call |

    **Next:** Program 2 — multi-column JSON, cost estimation, exponential backoff.
    """)
    return


if __name__ == "__main__":
    app.run()
