import marimo

__generated_with = "0.23.16"
app = marimo.App(width="full", app_title="PySpark RDD + Claude LLM — RDD Program 1: Basics")


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        # 🔷 RDD Program 1: PySpark RDD + Claude LLM — Basics

        **Goal:** Call Claude directly from an RDD pipeline using
        `mapPartitions()` — no DataFrame, no `pandas_udf`.

        This notebook shows the **RDD-native** integration path:
        - Why `mapPartitions()` is the right RDD hook for LLM calls
        - Loading config from `.env`
        - Building a minimal `map → LLM → collect` pipeline
        - Inspecting exactly which records land in which partition

        **Dataset:** 10 short news headlines → Claude adds a topic tag.

        ---
        > **Setup:**
        > ```bash
        > cp .env.example .env   # fill in ANTHROPIC_API_KEY
        > pip install pyspark anthropic python-dotenv marimo
        > marimo edit program_rdd1_basics.py
        > ```
        """
    )
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Why `mapPartitions()` for RDDs?

        ```
        rdd.map(call_claude)
        ┌──────────────────────────────────────────────────────┐
        │ record 1 → call_claude() → API call                 │
        │ record 2 → call_claude() → API call                 │  ← N calls
        │ record 3 → call_claude() → API call                 │    N client
        └──────────────────────────────────────────────────────┘    creations

        rdd.mapPartitions(enrich_partition)
        ┌──────────────────────────────────────────────────────┐
        │  Create client ONCE per partition                    │
        │  record 1 → call_claude()                           │  ← N calls
        │  record 2 → call_claude()                           │    1 client
        │  record 3 → call_claude()                           │    creation
        └──────────────────────────────────────────────────────┘
        ```

        `mapPartitions()` receives an **iterator** of all records in the partition.
        You create the Claude client **once** per partition, then iterate.
        This saves TLS handshake overhead and is the RDD equivalent of `pandas_udf`.
        """
    )
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 0 — Imports")
    return


@app.cell
def __():
    import os, time, pathlib
    import anthropic
    import marimo as mo
    from pyspark.sql import SparkSession

    NOTEBOOK_DIR = str(pathlib.Path(__file__).resolve().parent)

    return SparkSession, NOTEBOOK_DIR, anthropic, mo, os, time


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 1 — Load `.env` Configuration")
    return


@app.cell
def __(NOTEBOOK_DIR, os):
    from dotenv import load_dotenv

    load_dotenv(os.path.join(NOTEBOOK_DIR, ".env"), override=False, verbose=True)

    ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
    MODEL             = os.environ.get("ANTHROPIC_MODEL",    "claude-sonnet-4-6")
    MAX_TOKENS        = int(os.environ.get("LLM_MAX_TOKENS",      "12"))
    RATE_LIMIT_RPM    = int(os.environ.get("LLM_RATE_LIMIT_RPM",  "50"))
    SPARK_MASTER      = os.environ.get("SPARK_MASTER",             "local[*]")
    N_PARTITIONS      = int(os.environ.get("SPARK_PARTITIONS",     "3"))

    SLEEP_SEC = 60 / RATE_LIMIT_RPM

    ok = "✅" if ANTHROPIC_API_KEY else "⚠️ "
    print(f"{ok} ANTHROPIC_API_KEY  : {ANTHROPIC_API_KEY[:12]}..." if ANTHROPIC_API_KEY
          else f"{ok} ANTHROPIC_API_KEY  : NOT SET — add to .env")
    print(f"   ANTHROPIC_MODEL    : {MODEL}")
    print(f"   LLM_MAX_TOKENS     : {MAX_TOKENS}")
    print(f"   LLM_RATE_LIMIT_RPM : {RATE_LIMIT_RPM}  → sleep {SLEEP_SEC:.2f}s/call")
    print(f"   SPARK_MASTER       : {SPARK_MASTER}")
    print(f"   SPARK_PARTITIONS   : {N_PARTITIONS}")
    return (
        ANTHROPIC_API_KEY, MAX_TOKENS, MODEL,
        N_PARTITIONS, RATE_LIMIT_RPM, SLEEP_SEC,
        SPARK_MASTER, load_dotenv,
    )


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 2 — Spark Context")
    return


@app.cell
def __(SPARK_MASTER, SparkSession):
    spark = (
        SparkSession.builder
        .appName("RDD_Program1_Basics")
        .master(SPARK_MASTER)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext
    print(f"✅  Spark {spark.version}  |  master: {sc.master}")
    print(f"   Default parallelism: {sc.defaultParallelism}")
    return sc, spark


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 3 — Create the RDD")
    return


@app.cell
def __(N_PARTITIONS, NOTEBOOK_DIR, os, spark):
    headlines_rdd = (
        spark.read.csv(os.path.join(NOTEBOOK_DIR, "data", "headlines.csv"), header=True, inferSchema=True)
        .rdd.map(lambda row: (row["id"], row["headline_text"]))
        .repartition(N_PARTITIONS)
    )

    print(f"RDD: {headlines_rdd.count()} headlines  |  "
          f"{headlines_rdd.getNumPartitions()} partitions")

    # ── Prove partitioning with glom() ────────────────────────────────────
    # glom() returns one list per partition so we can see the layout
    print("\nPartition layout (via glom):")
    for _i, _part in enumerate(headlines_rdd.glom().collect()):
        _ids = [r[0] for r in _part]
        print(f"  Partition {_i}: headline IDs {_ids}  ({len(_part)} records)")
    return (headlines_rdd,)


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Step 4 — Prompt Builder and Single-Record Call

        We define these as plain functions first — easy to test locally
        before attaching them to Spark.
        """
    )
    return


@app.cell
def __(ANTHROPIC_API_KEY, MAX_TOKENS, MODEL, SLEEP_SEC, anthropic, time):
    def build_topic_prompt(headline: str) -> str:
        return (
            "Classify this news headline into exactly ONE topic word.\n"
            "Choose from: ECONOMY, SCIENCE, SPORTS, TECHNOLOGY, WEATHER, "
            "HEALTH, ENVIRONMENT, POLITICS, SPACE, OTHER.\n"
            "Reply with ONLY the single topic word — no punctuation, no explanation.\n\n"
            f"Headline: {headline}"
        )

    VALID_TOPICS = {
        "ECONOMY","SCIENCE","SPORTS","TECHNOLOGY","WEATHER",
        "HEALTH","ENVIRONMENT","POLITICS","SPACE","OTHER"
    }

    def call_claude_topic(headline: str) -> str:
        """
        Returns one topic word or 'ERROR:<msg>'.
        Client created here — called once per record inside mapPartitions.
        """
        if not headline or len(headline.strip()) < 5:
            return "OTHER"
        try:
            client   = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            response = client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                messages=[{"role": "user",
                            "content": build_topic_prompt(headline)}]
            )
            time.sleep(SLEEP_SEC)
            result = response.content[0].text.strip().upper()
            return result if result in VALID_TOPICS else f"UNKNOWN:{result[:20]}"
        except anthropic.RateLimitError:
            time.sleep(30)
            return call_claude_topic(headline)
        except Exception as e:
            return f"ERROR:{str(e)[:50]}"

    # ── Local test — no Spark needed ──────────────────────────────────────
    _test = "Scientists discover new exoplanet in the habitable zone"
    print("Local test (no Spark):")
    print(f"  Headline : {_test!r}")
    if ANTHROPIC_API_KEY:
        print(f"  Topic    : {call_claude_topic(_test)}")
    else:
        print(f"  Topic    : (skipped — ANTHROPIC_API_KEY not set)")
    return VALID_TOPICS, build_topic_prompt, call_claude_topic


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Step 5 — `mapPartitions()`: The RDD-Native LLM Bridge

        `mapPartitions(func)` calls `func(iterator)` once per partition.
        The iterator yields every record in the partition.

        Key advantage: **one Claude client per partition**, not one per record.
        The client is created inside the function body — never at module scope —
        so Spark can safely ship the function to any executor.
        """
    )
    return


@app.cell
def __(ANTHROPIC_API_KEY, MAX_TOKENS, MODEL, SLEEP_SEC, anthropic,
       build_topic_prompt, call_claude_topic, time, VALID_TOPICS):

    def enrich_partition(records):
        """
        Called once per partition with an iterator of (id, headline) tuples.
        Creates ONE Claude client for the whole partition.
        Yields (id, headline, topic) tuples.

        Note: we accept ANTHROPIC_API_KEY etc. from the enclosing scope.
        In production, read them from os.environ inside this function
        to avoid any serialisation issues with complex closure objects.
        """
        import os as _os
        from dotenv import load_dotenv as _load
        _load(override=False)
        _key = _os.environ.get("ANTHROPIC_API_KEY", ANTHROPIC_API_KEY)
        _client = anthropic.Anthropic(api_key=_key)   # ONE client per partition

        for record_id, headline in records:
            if not headline or len(headline.strip()) < 5:
                yield (record_id, headline, "OTHER")
                continue
            try:
                resp   = _client.messages.create(
                    model=MODEL,
                    max_tokens=MAX_TOKENS,
                    messages=[{"role":"user",
                                "content":build_topic_prompt(headline)}]
                )
                import time as _t; _t.sleep(SLEEP_SEC)
                result = resp.content[0].text.strip().upper()
                topic  = result if result in VALID_TOPICS else f"UNKNOWN:{result[:20]}"
            except Exception as e:
                topic = f"ERROR:{str(e)[:40]}"

            yield (record_id, headline, topic)

    print("✅  enrich_partition() defined")
    print("   Signature : iterator of (id, headline)")
    print("   Yields    : (id, headline, topic)")
    print("   Client    : ONE per partition (not one per record)")
    return (enrich_partition,)


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 6 — Apply `mapPartitions()` and Collect Results")
    return


@app.cell
def __(enrich_partition, headlines_rdd):
    # mapPartitions is lazy — nothing runs until an action is called
    enriched_rdd = headlines_rdd.mapPartitions(enrich_partition)

    # .collect() triggers execution — all partitions run in parallel
    results = enriched_rdd.collect()

    print("Enriched Headlines:")
    print(f"\n  {'ID':>3}  {'Topic':<14}  Headline")
    print(f"  {'-'*3}  {'-'*14}  {'-'*50}")
    for _record_id, _headline, _topic in sorted(results):
        print(f"  {_record_id:>3}  {_topic:<14}  {_headline[:55]}")
    return enriched_rdd, results


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 7 — Basic Aggregations on the Enriched RDD")
    return


@app.cell
def __(enriched_rdd):
    # ── Count by topic using RDD operations only (no DataFrame) ──────────
    # map to (topic, 1) then reduceByKey to sum
    topic_counts = (
        enriched_rdd
        .map(lambda r: (r[2], 1))                          # (topic, 1)
        .reduceByKey(lambda a, b: a + b)                   # (topic, count)
        .sortBy(lambda kv: kv[1], ascending=False)
        .collect()
    )

    print("=== Topic Distribution ===")
    for _topic, _count in topic_counts:
        _bar = "█" * _count
        print(f"  {_topic:<14}  {_count}  {_bar}")

    # ── Filter to only SCIENCE and SPACE headlines ─────────────────────────
    print("\n=== SCIENCE + SPACE Headlines ===")
    science_space = (
        enriched_rdd
        .filter(lambda r: r[2] in {"SCIENCE", "SPACE"})
        .map(lambda r: (r[0], r[1]))           # drop topic column
        .collect()
    )
    for _hid, _headline in sorted(science_space):
        print(f"  [{_hid:>2}] {_headline}")
    return science_space, topic_counts


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 8 — Promote to DataFrame (Optional)")
    return


@app.cell
def __(enriched_rdd, spark):
    # RDD → DataFrame is a one-liner when you have a consistent schema
    enriched_df = enriched_rdd.toDF(["id", "headline", "topic"])

    print("DataFrame view of the enriched RDD:")
    enriched_df.show(truncate=60)

    print("Schema:")
    enriched_df.printSchema()
    return (enriched_df,)


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 9 — Save the Enriched RDD")
    return


@app.cell
def __(enriched_rdd):
    import tempfile, os as _os

    # Save as plain text (native RDD action)
    out_rdd_path = _os.path.join(tempfile.gettempdir(), "headlines_enriched_rdd1_txt")
    enriched_rdd.map(lambda r: f"{r[0]}\t{r[2]}\t{r[1]}").saveAsTextFile(
        out_rdd_path
    )
    print(f"✅  Saved as text → {out_rdd_path}")

    # Save as Parquet via DataFrame
    out_parquet_path = _os.path.join(tempfile.gettempdir(), "headlines_enriched_rdd1_parquet")
    return out_parquet_path, out_rdd_path


@app.cell
def __(enriched_rdd, out_parquet_path, spark):
    (enriched_rdd
     .toDF(["id", "headline", "topic"])
     .write.mode("overwrite")
     .partitionBy("topic")
     .parquet(out_parquet_path)
    )
    print(f"✅  Saved as Parquet (partitioned by topic) → {out_parquet_path}")
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Summary

        | Concept | Detail |
        |---------|--------|
        | **RDD hook** | `mapPartitions(func)` — `func` receives an iterator of records |
        | **Client creation** | Once per partition inside `enrich_partition()`, not once per record |
        | **Visibility** | `glom().collect()` shows exactly which records are in which partition |
        | **Aggregation** | Pure RDD: `map` → `reduceByKey` → `sortBy` |
        | **DataFrame bridge** | `enriched_rdd.toDF(schema)` when you need SQL or Parquet |
        | **Config** | All from `.env` via `load_dotenv()` |

        ### `map()` vs `mapPartitions()` for LLM work

        | | `rdd.map(call_llm)` | `rdd.mapPartitions(enrich_partition)` |
        |-|---|---|
        | Client creations | 1 per record | 1 per partition |
        | TLS handshakes | 1 per record | 1 per partition |
        | Rate-limit control | Per-call sleep | Per-call sleep, same |
        | Code complexity | Minimal | Slightly more |
        | **Recommended?** | Only for tiny RDDs | ✅ Always prefer this |

        **Next:** RDD Program 2 — multi-field output, key-value pairs, groupByKey aggregation.
        """
    )
    return


if __name__ == "__main__":
    app.run()
