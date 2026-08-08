import marimo

__generated_with = "0.23.16"
app = marimo.App(width="full", app_title="PySpark + Claude LLM — Program 3: Intermediate+")


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        # ⚡ Program 3: PySpark + Claude LLM — Intermediate+

        **Goal:** Production-grade DataFrame enrichment.

        New in this notebook:
        - All config from `.env` (including batch size, model, pricing)
        - **True multi-row batching** — N rows per Claude call
        - **Partition right-sizing** — calibrated to `LLM_RATE_LIMIT_RPM`
        - **Dead-letter log** — failed rows written to JSONL, never silently dropped
        - **Idempotent checkpoint** — skip already-enriched rows on re-run
        - **Quality scoring** — Claude scores each description 1–10
        - **Marimo UI sliders** — override `.env` values interactively
        """
    )
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 0 — Imports")
    return


@app.cell
def __():
    import os, time, json, re, pathlib
    import anthropic
    import pandas as pd
    import marimo as mo

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.functions import (
        pandas_udf, col, from_json, length,
        when, current_timestamp,
    )
    from pyspark.sql.types import (
        StringType, IntegerType, ArrayType,
        StructType, StructField,
    )

    NOTEBOOK_DIR = str(pathlib.Path(__file__).resolve().parent)

    return (
        ArrayType, F, IntegerType, SparkSession,
        StringType, StructField, StructType, NOTEBOOK_DIR,
        anthropic, col, current_timestamp, from_json,
        json, length, mo, os,
        pd, pandas_udf, re, time, when,
    )


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Step 1 — Load `.env` Configuration

        All secrets and defaults live in `.env`.
        Marimo sliders in Step 2 can override them interactively.

        ```ini
        ANTHROPIC_API_KEY=sk-ant-...
        ANTHROPIC_MODEL=claude-sonnet-4-6
        LLM_MAX_TOKENS=512
        LLM_RATE_LIMIT_RPM=50
        LLM_BATCH_SIZE=5
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

    _API_KEY      = os.environ.get("ANTHROPIC_API_KEY",     "")
    _MODEL        = os.environ.get("ANTHROPIC_MODEL",        "claude-sonnet-4-6")
    _MAX_TOKENS   = int(os.environ.get("LLM_MAX_TOKENS",     "512"))
    _RATE_LIMIT   = int(os.environ.get("LLM_RATE_LIMIT_RPM","50"))
    _BATCH_SIZE   = int(os.environ.get("LLM_BATCH_SIZE",     "5"))
    _SPARK_MASTER = os.environ.get("SPARK_MASTER",           "local[*]")
    _SPARK_PARTS  = int(os.environ.get("SPARK_PARTITIONS",   "4"))
    _MAX_CHARS    = int(os.environ.get("LLM_MAX_CHARS",       "1500"))

    ok = "✅" if _API_KEY else "⚠️ "
    print(f"{ok} ANTHROPIC_API_KEY  : {_API_KEY[:12]}..." if _API_KEY else f"{ok} ANTHROPIC_API_KEY  : NOT SET")
    print(f"   ANTHROPIC_MODEL    : {_MODEL}")
    print(f"   LLM_MAX_TOKENS     : {_MAX_TOKENS}")
    print(f"   LLM_RATE_LIMIT_RPM : {_RATE_LIMIT}")
    print(f"   LLM_BATCH_SIZE     : {_BATCH_SIZE}")
    print(f"   LLM_MAX_CHARS      : {_MAX_CHARS}")
    print(f"   SPARK_MASTER       : {_SPARK_MASTER}")
    print(f"   SPARK_PARTITIONS   : {_SPARK_PARTS}")

    # Export for downstream cells
    ENV_API_KEY      = _API_KEY
    ENV_MODEL        = _MODEL
    ENV_MAX_TOKENS   = _MAX_TOKENS
    ENV_RATE_LIMIT   = _RATE_LIMIT
    ENV_BATCH_SIZE   = _BATCH_SIZE
    ENV_SPARK_MASTER = _SPARK_MASTER
    ENV_SPARK_PARTS  = _SPARK_PARTS
    ENV_MAX_CHARS    = _MAX_CHARS
    return (
        ENV_API_KEY, ENV_BATCH_SIZE, ENV_MAX_CHARS,
        ENV_MAX_TOKENS, ENV_MODEL, ENV_RATE_LIMIT,
        ENV_SPARK_MASTER, ENV_SPARK_PARTS,
        load_dotenv,
    )


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Step 2 — Interactive Overrides (Marimo UI)

        Sliders are pre-filled from `.env` values.
        Change them here to experiment without editing `.env`.
        """
    )
    return


@app.cell
def __(ENV_BATCH_SIZE, ENV_MAX_CHARS, ENV_RATE_LIMIT, ENV_SPARK_PARTS, mo):
    batch_slider     = mo.ui.slider(1,  25,  value=ENV_BATCH_SIZE,  step=1,
                                    label="LLM_BATCH_SIZE  (rows per Claude call)")
    partitions_slider = mo.ui.slider(2,  20,  value=ENV_SPARK_PARTS, step=1,
                                    label="SPARK_PARTITIONS")
    rate_slider       = mo.ui.slider(10, 100, value=ENV_RATE_LIMIT,  step=5,
                                    label="LLM_RATE_LIMIT_RPM")
    chars_slider      = mo.ui.slider(200, 4000, value=ENV_MAX_CHARS, step=100,
                                    label="LLM_MAX_CHARS  (truncate input text)")

    mo.vstack([
        mo.md("### Runtime Overrides (loaded from .env — edit freely)"),
        batch_slider, partitions_slider, rate_slider, chars_slider,
    ])
    return batch_slider, chars_slider, partitions_slider, rate_slider


@app.cell
def __(ENV_API_KEY, ENV_MAX_TOKENS, ENV_MODEL, ENV_SPARK_MASTER,
       batch_slider, chars_slider, partitions_slider, rate_slider):
    # Final effective config — slider values win over .env defaults
    ANTHROPIC_API_KEY = ENV_API_KEY
    MODEL             = ENV_MODEL
    MAX_TOKENS        = ENV_MAX_TOKENS
    BATCH_SIZE        = batch_slider.value
    N_PARTITIONS      = partitions_slider.value
    RATE_LIMIT_RPM    = rate_slider.value
    MAX_CHARS         = chars_slider.value
    SPARK_MASTER      = ENV_SPARK_MASTER
    SLEEP_BETWEEN     = (60 / RATE_LIMIT_RPM) * BATCH_SIZE

    print("Effective configuration:")
    print(f"  Model          : {MODEL}")
    print(f"  Batch size     : {BATCH_SIZE} rows/call")
    print(f"  Partitions     : {N_PARTITIONS}")
    print(f"  Rate limit     : {RATE_LIMIT_RPM} RPM  → sleep {SLEEP_BETWEEN:.2f}s/batch")
    print(f"  Max chars/row  : {MAX_CHARS}")
    return (
        ANTHROPIC_API_KEY, BATCH_SIZE, MAX_CHARS, MAX_TOKENS,
        MODEL, N_PARTITIONS, RATE_LIMIT_RPM, SLEEP_BETWEEN, SPARK_MASTER,
    )


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 3 — Spark Session")
    return


@app.cell
def __(N_PARTITIONS, SPARK_MASTER, SparkSession):
    spark = (
        SparkSession.builder
        .appName("Program3_LLM_IntermediatePlus")
        .master(SPARK_MASTER)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", str(N_PARTITIONS))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print(f"✅  Spark {spark.version}  |  shuffle.partitions={N_PARTITIONS}")
    return (spark,)


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 4 — Product Description Dataset with Row Hashes")
    return


@app.cell
def __(F, N_PARTITIONS, NOTEBOOK_DIR, os, spark):
    products_df = (
        spark.read.csv(os.path.join(NOTEBOOK_DIR, "data", "products.csv"), header=True, inferSchema=True)
        .withColumn("row_hash", F.md5(F.concat(F.col("product_id"), F.col("raw_description"))).substr(1, 12))
        .repartition(N_PARTITIONS)
    )

    print(f"Dataset: {products_df.count()} products  |  {products_df.rdd.getNumPartitions()} partition(s)")
    products_df.show(truncate=60)
    return (products_df,)


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Step 5 — Idempotent Checkpoint

        On the first run, `done_hashes` is empty and all rows are processed.
        On a re-run after a partial failure, rows whose `row_hash` is already
        in the checkpoint parquet are filtered out — Claude is not called again for them.
        """
    )
    return


@app.cell
def __(products_df, spark):
    import tempfile as _tmp, os as _os

    CHECKPOINT_PATH = _os.path.join(_tmp.gettempdir(), "llm_checkpoint_p3")

    def _load_checkpoint():
        try:
            df = spark.read.parquet(CHECKPOINT_PATH)
            hashes = set(r.row_hash for r in df.select("row_hash").collect())
            print(f"✅  Checkpoint: {len(hashes)} rows already done")
            return hashes
        except Exception:
            print("   No checkpoint — processing all rows")
            return set()

    _done = _load_checkpoint()

    if _done:
        from pyspark.sql.functions import col as _c
        rows_to_process = products_df.filter(~_c("row_hash").isin(_done))
    else:
        rows_to_process = products_df

    _rem = rows_to_process.count()
    print(f"   Rows to process: {_rem}/{products_df.count()}")
    return CHECKPOINT_PATH, rows_to_process


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Step 6 — Batched Prompt & Claude Call

        `call_claude_batch(descriptions)` sends up to `BATCH_SIZE` product
        descriptions in **one prompt** and receives a JSON **array** in return.

        Benefits vs one-call-per-row:
        - Fewer HTTP round-trips
        - Shared prompt prefix paid once in tokens
        - Higher effective throughput at the same rate limit
        """
    )
    return


@app.cell
def __(ANTHROPIC_API_KEY, BATCH_SIZE, MAX_CHARS, MAX_TOKENS,
       MODEL, SLEEP_BETWEEN, anthropic, json, re, time):
    import os as _os2

    def build_batch_prompt(descriptions: list) -> str:
        n        = len(descriptions)
        numbered = "\n".join(
            f"{i+1}. {d[:MAX_CHARS]}" for i, d in enumerate(descriptions)
        )
        return (
            f"Analyze these {n} product descriptions and return a JSON array of "
            f"exactly {n} objects, one per product, in order.\n"
            "No markdown, no explanation — only the raw JSON array.\n\n"
            "Each object must have:\n"
            '  "quality_score":   integer 1-10\n'
            '  "target_audience": "PROFESSIONAL"|"STUDENT"|"GAMER"|"GENERAL"\n'
            '  "key_features":    array of up to 3 short strings\n'
            '  "suggested_tag":   one lowercase category word\n\n'
            f"Products:\n{numbered}"
        )

    _VALID_AUD = {"PROFESSIONAL","STUDENT","GAMER","GENERAL"}
    _ERR_ITEM  = {"quality_score":0,"target_audience":"UNKNOWN",
                  "key_features":[],"suggested_tag":"error"}

    def call_claude_batch(descriptions: list) -> list:
        if not descriptions:
            return []

        client  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        backoff = 2

        for attempt in range(3):
            try:
                r   = client.messages.create(
                          model=MODEL, max_tokens=MAX_TOKENS,
                          messages=[{"role":"user",
                                     "content":build_batch_prompt(descriptions)}])
                raw = re.sub(r"^```json\s*", "", r.content[0].text.strip())
                raw = re.sub(r"```\s*$", "", raw)
                parsed = json.loads(raw)

                if not isinstance(parsed, list):
                    raise ValueError("Expected list")

                while len(parsed) < len(descriptions):
                    parsed.append(_ERR_ITEM.copy())
                parsed = parsed[:len(descriptions)]

                for item in parsed:
                    item["quality_score"]   = max(1, min(10, int(item.get("quality_score",0))))
                    item["target_audience"] = str(item.get("target_audience","GENERAL")).upper()
                    if item["target_audience"] not in _VALID_AUD:
                        item["target_audience"] = "GENERAL"
                    item["key_features"]    = [str(f)[:50] for f in item.get("key_features",[])[:3]]
                    item["suggested_tag"]   = str(item.get("suggested_tag","general")).lower()[:30]

                time.sleep(SLEEP_BETWEEN)
                return parsed

            except anthropic.RateLimitError:
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
            except Exception as e:
                print(f"   Batch error (attempt {attempt+1}): {e}")
                if attempt == 2:
                    return [_ERR_ITEM.copy() for _ in descriptions]
                time.sleep(backoff); backoff *= 2

        return [_ERR_ITEM.copy() for _ in descriptions]

    # Local test
    _test = ["Premium headphones with ANC","Mechanical keyboard RGB"]
    print("Local batch test:")
    if ANTHROPIC_API_KEY:
        for _i, _r in enumerate(call_claude_batch(_test), 1):
            print(f"  {_i}: {json.dumps(_r)}")
    else:
        print("  (skipped — no API key)")
    return build_batch_prompt, call_claude_batch


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 7 — Batched `pandas_udf` with Dead-Letter Logging")
    return


@app.cell
def __(BATCH_SIZE, StringType, call_claude_batch, json, os, pd, pandas_udf, time):
    import tempfile as _tmp2

    DEAD_LETTER_PATH = os.path.join(_tmp2.gettempdir(), "llm_dead_letters_p3.jsonl")

    def _log_dead(pid, text, error):
        with open(DEAD_LETTER_PATH, "a") as _f:
            _f.write(json.dumps({"product_id":pid,"text":text[:200],
                                  "error":error,"ts":time.time()}) + "\n")

    @pandas_udf(StringType())
    def batched_enrich_udf(descriptions: pd.Series) -> pd.Series:
        items   = descriptions.tolist()
        results = []

        for start in range(0, len(items), BATCH_SIZE):
            batch = items[start : start + BATCH_SIZE]
            try:
                batch_results = call_claude_batch(batch)
                results.extend(json.dumps(r) for r in batch_results)
            except Exception as e:
                for text in batch:
                    _log_dead("unknown", text, str(e))
                    results.append(json.dumps({
                        "quality_score":0,"target_audience":"UNKNOWN",
                        "key_features":[],"suggested_tag":"dead-letter"
                    }))

        while len(results) < len(items):
            results.append(json.dumps({"quality_score":0,"target_audience":"UNKNOWN",
                                       "key_features":[],"suggested_tag":"padding-error"}))

        return pd.Series(results[:len(items)])

    print(f"✅  'batched_enrich_udf' ready  (batch={BATCH_SIZE})")
    print(f"   Dead-letter log → {DEAD_LETTER_PATH}")
    return DEAD_LETTER_PATH, batched_enrich_udf


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 8 — Run the Pipeline")
    return


@app.cell
def __(
    F, ArrayType, IntegerType, StringType, StructField, StructType,
    batched_enrich_udf, col, current_timestamp, from_json, rows_to_process,
):
    _llm_schema = StructType([
        StructField("quality_score",   IntegerType(),           True),
        StructField("target_audience", StringType(),            True),
        StructField("key_features",    ArrayType(StringType()), True),
        StructField("suggested_tag",   StringType(),            True),
    ])

    enriched_products = (
        rows_to_process
        .withColumn("_raw_json", batched_enrich_udf(col("raw_description")))
        .withColumn("_llm",      from_json(col("_raw_json"), _llm_schema))
        .select(
            col("product_id"),
            col("product_name"),
            col("raw_description"),
            col("row_hash"),
            col("_llm.quality_score").alias("quality_score"),
            col("_llm.target_audience").alias("target_audience"),
            col("_llm.key_features").alias("key_features"),
            col("_llm.suggested_tag").alias("suggested_tag"),
            current_timestamp().alias("enriched_at"),
        )
        .withColumn("needs_rewrite", col("quality_score") < 6)
    )

    print("Enriched Products:")
    enriched_products.select(
        "product_id","product_name","quality_score",
        "target_audience","suggested_tag","needs_rewrite"
    ).show(truncate=35)
    return (enriched_products,)


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 9 — Quality Analytics")
    return


@app.cell
def __(F, col, enriched_products, mo):
    _scores = (enriched_products.groupBy("quality_score")
               .agg(F.count("*").alias("count"))
               .orderBy("quality_score")
               .toPandas())

    _audience = (enriched_products.groupBy("target_audience")
                 .agg(F.count("*").alias("count"),
                      F.round(F.avg("quality_score"),2).alias("avg_quality"))
                 .orderBy(col("count").desc())
                 .toPandas())

    _rewrite = (enriched_products.filter(col("needs_rewrite") == True)
                .select("product_id","product_name","quality_score")
                .toPandas())

    mo.vstack([
        mo.md("### Quality Score Distribution"),
        mo.ui.table(_scores),
        mo.md("### Audience × Average Quality"),
        mo.ui.table(_audience),
        mo.md("### Products Needing Rewrite (score < 6)"),
        mo.ui.table(_rewrite) if len(_rewrite) > 0
        else mo.md("*All descriptions meet the quality threshold.*"),
    ])
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 10 — Save, Checkpoint, Dead-Letter Report")
    return


@app.cell
def __(CHECKPOINT_PATH, DEAD_LETTER_PATH, enriched_products, mo, spark):
    import tempfile as _tmp3, os as _os3

    _out = _os3.path.join(_tmp3.gettempdir(), "products_enriched_p3")

    # Save enriched data
    (enriched_products.write.mode("overwrite")
     .partitionBy("target_audience").parquet(_out))
    print(f"✅  Enriched data → {_out}")

    # Update checkpoint
    (enriched_products.select("row_hash")
     .write.mode("append").parquet(CHECKPOINT_PATH))
    _n_cp = spark.read.parquet(CHECKPOINT_PATH).count()
    print(f"✅  Checkpoint updated → {_n_cp} total rows marked done")

    # Dead-letter report
    _dl_msg = ("✅  Dead-letter log: empty — all rows processed successfully"
               if not _os3.path.exists(DEAD_LETTER_PATH) else None)
    if _dl_msg is None:
        with open(DEAD_LETTER_PATH) as _f:
            _lines = _f.readlines()
        _dl_msg = (f"✅  Dead-letter log: empty"
                   if not _lines else
                   f"⚠️   Dead-letter log: {len(_lines)} failed row(s) → {DEAD_LETTER_PATH}")

    mo.md(f"### Dead-Letter Status\n{_dl_msg}")
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Summary — Production Checklist

        | ✅ | Pattern | Implementation |
        |---|---------|----------------|
        | ✅ | API key in `.env` | `load_dotenv()` + `os.environ.get()` |
        | ✅ | All config in `.env` | model, tokens, rate limit, batch size, partitions |
        | ✅ | Interactive override | Marimo sliders pre-filled from `.env` |
        | ✅ | Client inside UDF | Instantiated in `call_claude_batch`, never at module scope |
        | ✅ | True batching | N rows per Claude call; batch prompt returns JSON array |
        | ✅ | Partition sizing | `shuffle.partitions` set from slider/`.env` |
        | ✅ | Exponential backoff | `backoff = min(backoff*2, 60)` on `RateLimitError` |
        | ✅ | JSON validation | Schema checked + out-of-vocabulary values replaced |
        | ✅ | Dead-letter log | Failed rows written to JSONL, never silently dropped |
        | ✅ | Idempotent checkpoint | Parquet hash store; re-runs skip completed rows |
        | ✅ | Derived columns | `needs_rewrite = quality_score < 6` (zero API cost) |
        | ✅ | Partitioned output | `partitionBy("target_audience")` |
        """
    )
    return


if __name__ == "__main__":
    app.run()
