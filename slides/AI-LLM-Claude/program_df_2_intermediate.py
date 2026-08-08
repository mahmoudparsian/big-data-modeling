import marimo

__generated_with = "0.23.16"
app = marimo.App(width="full", app_title="PySpark + Claude LLM — Program 2: Intermediate")


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        # 🧩 Program 2: PySpark + Claude LLM — Intermediate

        **Goal:** One Claude call per row → multiple enrichment columns via structured JSON.

        New in this notebook:
        - `.env`-driven configuration (all secrets and tunables)
        - **Multi-column enrichment** from a single Claude call per row
        - **Structured JSON output** parsed into typed Spark columns with `from_json`
        - **Cost estimation** before running
        - **Exponential backoff** on rate-limit errors
        - **`mapInPandas`** for full-partition processing

        Dataset: support tickets enriched with `priority`, `category`, `sentiment`, `summary`.
        """
    )
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 0 — Imports")
    return


@app.cell
def __():
    import os, time, json, pathlib
    import anthropic
    import pandas as pd
    import marimo as mo

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.functions import pandas_udf, col, from_json, length, when
    from pyspark.sql.types import (
        StringType, IntegerType,
        StructType, StructField,
    )

    NOTEBOOK_DIR = str(pathlib.Path(__file__).resolve().parent)

    return (
        F, IntegerType, SparkSession, StringType,
        StructField, StructType, NOTEBOOK_DIR,
        anthropic, col, from_json, json, length,
        mo, os, pd, pandas_udf, time, when,
    )


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Step 1 — Load `.env` Configuration

        ```ini
        # .env
        ANTHROPIC_API_KEY=sk-ant-...
        ANTHROPIC_MODEL=claude-sonnet-4-6
        LLM_MAX_TOKENS=300
        LLM_RATE_LIMIT_RPM=50
        SPARK_MASTER=local[*]
        SPARK_PARTITIONS=4
        ```
        """
    )
    return


@app.cell
def __(NOTEBOOK_DIR, os):
    from dotenv import load_dotenv

    load_dotenv(os.path.join(NOTEBOOK_DIR, ".env"), override=False, verbose=True)

    ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY",     "")
    MODEL             = os.environ.get("ANTHROPIC_MODEL",        "claude-sonnet-4-6")
    MAX_TOKENS        = int(os.environ.get("LLM_MAX_TOKENS",     "300"))
    RATE_LIMIT_RPM    = int(os.environ.get("LLM_RATE_LIMIT_RPM","50"))
    SPARK_MASTER      = os.environ.get("SPARK_MASTER",           "local[*]")
    SPARK_PARTITIONS  = int(os.environ.get("SPARK_PARTITIONS",   "4"))

    # Cost constants (verify at docs.anthropic.com/en/api/pricing)
    PRICE_IN_PER_M  = float(os.environ.get("LLM_PRICE_INPUT_PER_M",  "3.0"))
    PRICE_OUT_PER_M = float(os.environ.get("LLM_PRICE_OUTPUT_PER_M", "15.0"))
    AVG_IN_TOKENS   = 200   # estimated per row
    AVG_OUT_TOKENS  = 80    # estimated per row

    SLEEP_SEC = 60 / RATE_LIMIT_RPM

    ok = "✅" if ANTHROPIC_API_KEY else "⚠️ "
    print(f"{ok} ANTHROPIC_API_KEY  : {ANTHROPIC_API_KEY[:12]}..." if ANTHROPIC_API_KEY
          else f"{ok} ANTHROPIC_API_KEY  : NOT SET")
    print(f"   ANTHROPIC_MODEL    : {MODEL}")
    print(f"   LLM_MAX_TOKENS     : {MAX_TOKENS}")
    print(f"   LLM_RATE_LIMIT_RPM : {RATE_LIMIT_RPM}  (sleep {SLEEP_SEC:.2f}s/call)")
    print(f"   SPARK_MASTER       : {SPARK_MASTER}")
    print(f"   SPARK_PARTITIONS   : {SPARK_PARTITIONS}")
    return (
        ANTHROPIC_API_KEY, AVG_IN_TOKENS, AVG_OUT_TOKENS,
        MAX_TOKENS, MODEL, PRICE_IN_PER_M, PRICE_OUT_PER_M,
        RATE_LIMIT_RPM, SLEEP_SEC, SPARK_MASTER, SPARK_PARTITIONS,
        load_dotenv,
    )


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 2 — Spark Session")
    return


@app.cell
def __(SPARK_MASTER, SPARK_PARTITIONS, SparkSession):
    spark = (
        SparkSession.builder
        .appName("Program2_LLM_Intermediate")
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
def __(mo):
    mo.md("## Step 3 — Support Ticket Dataset")
    return


@app.cell
def __(SPARK_PARTITIONS, NOTEBOOK_DIR, os, spark):
    tickets_df = (
        spark.read.csv(os.path.join(NOTEBOOK_DIR, "data", "tickets.csv"), header=True, inferSchema=True)
             .na.fill("")
             .repartition(SPARK_PARTITIONS)
    )
    print(f"Dataset: {tickets_df.count()} tickets  |  {tickets_df.rdd.getNumPartitions()} partition(s)")
    tickets_df.show(truncate=65)
    return (tickets_df,)


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 4 — Cost Estimation")
    return


@app.cell
def __(AVG_IN_TOKENS, AVG_OUT_TOKENS, PRICE_IN_PER_M, PRICE_OUT_PER_M, tickets_df):
    _n          = tickets_df.count()
    _cost_in    = _n * AVG_IN_TOKENS  / 1_000_000 * PRICE_IN_PER_M
    _cost_out   = _n * AVG_OUT_TOKENS / 1_000_000 * PRICE_OUT_PER_M
    _cost_total = _cost_in + _cost_out

    print(f"{'='*48}")
    print(f"  COST ESTIMATE  ({_n} rows)")
    print(f"{'='*48}")
    print(f"  Input  tokens/row : ~{AVG_IN_TOKENS}")
    print(f"  Output tokens/row : ~{AVG_OUT_TOKENS}")
    print(f"  Input  cost       : ${_cost_in:.4f}")
    print(f"  Output cost       : ${_cost_out:.4f}")
    print(f"  TOTAL ESTIMATE    : ${_cost_total:.4f}")
    print(f"{'='*48}")
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Step 5 — Multi-Column Prompt & Claude Call

        Claude is asked to return a JSON object with four fields in one call.
        The prompt spells out the exact schema — this reduces hallucination.

        ```json
        {
          "priority":  "HIGH" | "MEDIUM" | "LOW",
          "category":  "BILLING" | "TECHNICAL" | "SHIPPING" | "GENERAL" | "FEEDBACK",
          "sentiment": "POSITIVE" | "NEGATIVE" | "NEUTRAL",
          "summary":   "one sentence, max 20 words"
        }
        ```
        """
    )
    return


@app.cell
def __(ANTHROPIC_API_KEY, MAX_TOKENS, MODEL, SLEEP_SEC, anthropic, json, time):
    _VALID = {
        "priority":  {"HIGH", "MEDIUM", "LOW"},
        "category":  {"BILLING", "TECHNICAL", "SHIPPING", "GENERAL", "FEEDBACK"},
        "sentiment": {"POSITIVE", "NEGATIVE", "NEUTRAL"},
    }
    _ERROR_JSON = json.dumps({
        "priority":"UNKNOWN","category":"UNKNOWN",
        "sentiment":"UNKNOWN","summary":"ERROR: failed to process"
    })

    def build_ticket_prompt(text: str) -> str:
        return (
            "Analyze this customer support ticket and respond with ONLY a JSON object.\n"
            "No markdown, no explanation — just the raw JSON.\n\n"
            f"Ticket: {text}\n\n"
            'Respond with exactly:\n'
            '{\n'
            '  "priority":  "HIGH" | "MEDIUM" | "LOW",\n'
            '  "category":  "BILLING" | "TECHNICAL" | "SHIPPING" | "GENERAL" | "FEEDBACK",\n'
            '  "sentiment": "POSITIVE" | "NEGATIVE" | "NEUTRAL",\n'
            '  "summary":   "one sentence, max 20 words"\n'
            '}'
        )

    def call_claude_multi(text: str, max_retries: int = 3) -> str:
        """
        Returns a validated JSON string.
        Falls back to error JSON — never raises.
        Client instantiated inside the function (required for Spark executor safety).
        """
        if not text or len(text.strip()) < 8:
            return json.dumps({"priority":"LOW","category":"GENERAL",
                               "sentiment":"NEUTRAL","summary":"Ticket too short."})

        client  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        backoff = 2

        for attempt in range(max_retries):
            try:
                r   = client.messages.create(model=MODEL, max_tokens=MAX_TOKENS,
                          messages=[{"role":"user","content":build_ticket_prompt(text)}])
                raw = r.content[0].text.strip()
                p   = json.loads(raw)

                for field in ("priority", "category", "sentiment"):
                    p[field] = str(p.get(field,"UNKNOWN")).upper()
                    if p[field] not in _VALID[field]:
                        p[field] = "UNKNOWN"
                p["summary"] = str(p.get("summary",""))[:120]

                time.sleep(SLEEP_SEC)
                return json.dumps(p)

            except anthropic.RateLimitError:
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)

            except json.JSONDecodeError:
                return _ERROR_JSON

            except Exception as e:
                return json.dumps({**json.loads(_ERROR_JSON),
                                   "summary": f"ERROR:{str(e)[:80]}"})

        return _ERROR_JSON

    _test = "Charged twice for my subscription. Need a refund!"
    print(f"Local test → {call_claude_multi(_test) if ANTHROPIC_API_KEY else '(skipped)'}")
    return build_ticket_prompt, call_claude_multi


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 6 — `pandas_udf` + `from_json` Expansion")
    return


@app.cell
def __(
    StringType, StructField, StructType,
    call_claude_multi, col, from_json,
    length, pd, pandas_udf, tickets_df, when,
):
    _llm_schema = StructType([
        StructField("priority",  StringType(), True),
        StructField("category",  StringType(), True),
        StructField("sentiment", StringType(), True),
        StructField("summary",   StringType(), True),
    ])

    @pandas_udf(StringType())
    def ticket_enrich_udf(texts: pd.Series) -> pd.Series:
        return texts.apply(call_claude_multi)

    # Apply UDF then parse JSON — zero extra API calls for the expansion
    enriched_df = (
        tickets_df
        .withColumn("_llm_json",
                    when(length(col("ticket_text")) < 8,
                         '{"priority":"LOW","category":"GENERAL",'
                         '"sentiment":"NEUTRAL","summary":"Too short."}')
                    .otherwise(ticket_enrich_udf(col("ticket_text"))))
        .withColumn("_llm", from_json(col("_llm_json"), _llm_schema))
        .select(
            col("id"), col("email"), col("ticket_text"),
            col("_llm.priority").alias("priority"),
            col("_llm.category").alias("category"),
            col("_llm.sentiment").alias("sentiment"),
            col("_llm.summary").alias("summary"),
        )
    )

    print("Enriched Tickets:")
    enriched_df.show(truncate=55)
    return enriched_df, ticket_enrich_udf


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 7 — Analytics")
    return


@app.cell
def __(F, col, enriched_df):
    print("=== Priority Distribution ===")
    (enriched_df.groupBy("priority")
     .agg(F.count("*").alias("n"))
     .orderBy(col("n").desc())).show()

    print("=== HIGH Priority Tickets ===")
    (enriched_df.filter(col("priority") == "HIGH")
     .select("id","email","summary")).show(truncate=60)
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 8 — Save (partitioned by priority)")
    return


@app.cell
def __(enriched_df):
    import tempfile, os as _os
    _out = _os.path.join(tempfile.gettempdir(), "tickets_enriched_p2")
    enriched_df.write.mode("overwrite").partitionBy("priority").parquet(_out)
    print(f"✅  Saved → {_out}")
    for _r, _d, _f in _os.walk(_out):
        for _name in _f:
            if not _name.startswith("."):
                print(f"   {_os.path.relpath(_os.path.join(_r,_name), _out)}")
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Summary

        | Pattern | Detail |
        |---------|--------|
        | `.env` config | `load_dotenv()` → `os.environ.get()` for every value |
        | Multi-column output | Single prompt → JSON string → `from_json()` → typed columns |
        | Exponential backoff | `backoff = min(backoff*2, 60)` on `RateLimitError` |
        | Cost estimate | Computed before job runs, printed for review |
        | Output partitioning | `partitionBy("priority")` for efficient downstream reads |

        **Next:** Program 3 — true multi-row batching, dead-letter log, idempotent checkpoint.
        """
    )
    return


if __name__ == "__main__":
    app.run()
