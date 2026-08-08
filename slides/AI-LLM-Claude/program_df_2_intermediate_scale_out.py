import marimo

__generated_with = "0.23.16"
app = marimo.App(width="full", app_title="PySpark + Claude LLM — Program 2 Scale-Out: Batched Enrichment")


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        # PySpark + Claude LLM — Program 2 Scale-Out

        **Goal:** Enrich support tickets with Claude — but instead of one API call
        per row, send **multiple rows per call** (true batching) so the approach
        scales to large datasets.

        ## What changed from Program 2 (Intermediate)?

        | | Program 2 (original) | Program 2 Scale-Out (this notebook) |
        |---|---|---|
        | **API calls** | 1 call per row | 1 call per BATCH_SIZE rows |
        | **Prompt** | Single ticket text | Numbered list of N tickets |
        | **Claude response** | Single JSON object | JSON **array** of N objects |
        | **HTTP overhead** | Paid on every row | Shared across batch |
        | **Token savings** | None | Prompt prefix shared across batch |
        | **At 1M rows** | 1M API calls | 1M / BATCH_SIZE calls |

        ## The Batching Algorithm (simple version)

        ```
        1. Spark splits the DataFrame into partitions
        2. Each partition runs a pandas_udf
        3. Inside the UDF, rows are grouped into batches of BATCH_SIZE
        4. Each batch becomes ONE prompt: "Here are 5 tickets, return 5 JSON objects"
        5. Claude returns a JSON array with one result per ticket
        6. We match results back to rows by position (1st result → 1st ticket, etc.)
        7. All results are collected into a single pandas Series and returned
        ```

        ---
        > **Setup:** same `.env` as Program 2 — just add `LLM_BATCH_SIZE=4`
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
        mo, os, pd, pandas_udf, re, time, when,
    )


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Step 1 — Load `.env` Configuration

        Same as Program 2, with the addition of `LLM_BATCH_SIZE`.

        ```ini
        # .env  (add this line)
        LLM_BATCH_SIZE=4
        ```

        `BATCH_SIZE` controls how many tickets are sent in a single Claude prompt.
        - Too small (1) = no batching benefit, same as Program 2
        - Too large (50+) = Claude may lose accuracy or hit token limits
        - Sweet spot for this task: **4–10 rows per batch**
        """
    )
    return


@app.cell
def __(NOTEBOOK_DIR, os):
    from dotenv import load_dotenv

    load_dotenv(os.path.join(NOTEBOOK_DIR, ".env"), override=False, verbose=True)

    ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY",     "")
    MODEL             = os.environ.get("ANTHROPIC_MODEL",        "claude-sonnet-4-6")
    MAX_TOKENS        = int(os.environ.get("LLM_MAX_TOKENS",     "600"))
    RATE_LIMIT_RPM    = int(os.environ.get("LLM_RATE_LIMIT_RPM","50"))
    BATCH_SIZE        = int(os.environ.get("LLM_BATCH_SIZE",     "4"))
    SPARK_MASTER      = os.environ.get("SPARK_MASTER",           "local[*]")
    SPARK_PARTITIONS  = int(os.environ.get("SPARK_PARTITIONS",   "2"))

    # Cost constants
    PRICE_IN_PER_M  = float(os.environ.get("LLM_PRICE_INPUT_PER_M",  "3.0"))
    PRICE_OUT_PER_M = float(os.environ.get("LLM_PRICE_OUTPUT_PER_M", "15.0"))
    AVG_IN_TOKENS   = 250   # larger because batched prompt is longer
    AVG_OUT_TOKENS  = 100   # larger because response is a JSON array

    SLEEP_SEC = (60 / RATE_LIMIT_RPM) * BATCH_SIZE

    ok = "✅" if ANTHROPIC_API_KEY else "⚠️ "
    print(f"{ok} ANTHROPIC_API_KEY  : {ANTHROPIC_API_KEY[:12]}..." if ANTHROPIC_API_KEY
          else f"{ok} ANTHROPIC_API_KEY  : NOT SET")
    print(f"   ANTHROPIC_MODEL    : {MODEL}")
    print(f"   LLM_MAX_TOKENS     : {MAX_TOKENS}")
    print(f"   LLM_BATCH_SIZE     : {BATCH_SIZE}  (rows per Claude call)")
    print(f"   LLM_RATE_LIMIT_RPM : {RATE_LIMIT_RPM}  → sleep {SLEEP_SEC:.2f}s/batch")
    print(f"   SPARK_MASTER       : {SPARK_MASTER}")
    print(f"   SPARK_PARTITIONS   : {SPARK_PARTITIONS}")
    return (
        ANTHROPIC_API_KEY, AVG_IN_TOKENS, AVG_OUT_TOKENS,
        BATCH_SIZE, MAX_TOKENS, MODEL,
        PRICE_IN_PER_M, PRICE_OUT_PER_M,
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
        .appName("Program2_ScaleOut_Batched")
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
    mo.md("## Step 3 — Support Ticket Dataset (same as Program 2)")
    return


@app.cell
def __(NOTEBOOK_DIR, SPARK_PARTITIONS, os, spark):
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
    mo.md(
        r"""
        ## Step 4 — Cost Estimation: Batched vs Unbatched

        With batching, the number of API calls drops by a factor of `BATCH_SIZE`.
        The prompt is slightly longer (numbered list), but the **shared prompt prefix**
        is paid only once per batch instead of once per row.
        """
    )
    return


@app.cell
def __(AVG_IN_TOKENS, AVG_OUT_TOKENS, BATCH_SIZE, PRICE_IN_PER_M, PRICE_OUT_PER_M, tickets_df):
    import math

    _n = tickets_df.count()

    # Unbatched estimate (Program 2 approach)
    _cost_unbatched = (
        _n * AVG_IN_TOKENS  / 1_000_000 * PRICE_IN_PER_M +
        _n * AVG_OUT_TOKENS / 1_000_000 * PRICE_OUT_PER_M
    )
    # Batched estimate: fewer calls, shared prompt prefix saves ~30% input tokens
    _n_batches = math.ceil(_n / BATCH_SIZE)
    _batch_in  = _n * AVG_IN_TOKENS * 0.7   # ~30% savings from shared prefix
    _batch_out = _n * AVG_OUT_TOKENS         # output stays roughly the same
    _cost_batched = (
        _batch_in  / 1_000_000 * PRICE_IN_PER_M +
        _batch_out / 1_000_000 * PRICE_OUT_PER_M
    )

    print(f"{'='*55}")
    print(f"  COST COMPARISON  ({_n} rows, batch_size={BATCH_SIZE})")
    print(f"{'='*55}")
    print(f"  Unbatched (Program 2): {_n:>6} API calls  |  ${_cost_unbatched:.4f}")
    print(f"  Batched   (this):      {_n_batches:>6} API calls  |  ${_cost_batched:.4f}")
    print(f"  Savings:               {_n - _n_batches:>6} fewer      |  ${_cost_unbatched - _cost_batched:.4f}")
    print(f"{'='*55}")
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Step 5 — The Batched Prompt

        ### How it works

        Instead of sending one ticket per prompt:
        ```
        "Analyze this ticket: My order was charged twice..."
        ```

        We send N tickets in a numbered list:
        ```
        "Analyze these 4 tickets. Return a JSON array of 4 objects.

        1. My order was charged twice...
        2. The app crashes when I upload...
        3. Package arrived 3 weeks late...
        4. How do I change my billing address?"
        ```

        Claude returns a **JSON array** — one object per ticket, **in order**:
        ```json
        [
          {"priority": "HIGH", "category": "BILLING", ...},
          {"priority": "HIGH", "category": "TECHNICAL", ...},
          {"priority": "MEDIUM", "category": "SHIPPING", ...},
          {"priority": "LOW", "category": "GENERAL", ...}
        ]
        ```

        The order is guaranteed because we number the tickets in the prompt
        and instruct Claude to return results "in order".
        """
    )
    return


@app.cell
def __(ANTHROPIC_API_KEY, BATCH_SIZE, MAX_TOKENS, MODEL, SLEEP_SEC,
       anthropic, json, re, time):

    _VALID = {
        "priority":  {"HIGH", "MEDIUM", "LOW"},
        "category":  {"BILLING", "TECHNICAL", "SHIPPING", "GENERAL", "FEEDBACK"},
        "sentiment": {"POSITIVE", "NEGATIVE", "NEUTRAL"},
    }
    _ERROR_ITEM = {
        "priority": "UNKNOWN", "category": "UNKNOWN",
        "sentiment": "UNKNOWN", "summary": "ERROR: batch processing failed"
    }
    _SHORT_ITEM = {
        "priority": "LOW", "category": "GENERAL",
        "sentiment": "NEUTRAL", "summary": "Ticket too short to analyze."
    }

    def build_batch_prompt(texts: list) -> str:
        """
        Build a single prompt that asks Claude to analyze N tickets at once.

        The numbered list format makes it clear which result maps to which ticket.
        """
        n = len(texts)
        numbered = "\n".join(f"{i+1}. {t}" for i, t in enumerate(texts))
        return (
            f"Analyze these {n} customer support tickets.\n"
            f"Return a JSON array of EXACTLY {n} objects, one per ticket, in order.\n"
            "No markdown, no explanation — ONLY the raw JSON array.\n\n"
            "Each object must have:\n"
            '  "priority":  "HIGH" | "MEDIUM" | "LOW"\n'
            '  "category":  "BILLING" | "TECHNICAL" | "SHIPPING" | "GENERAL" | "FEEDBACK"\n'
            '  "sentiment": "POSITIVE" | "NEGATIVE" | "NEUTRAL"\n'
            '  "summary":   "one sentence, max 20 words"\n\n'
            f"Tickets:\n{numbered}"
        )

    def call_claude_batch(texts: list, batch_label: str = "") -> list:
        """
        Send a list of ticket texts to Claude in ONE API call.
        Returns a list of validated JSON dicts — one per input text.

        Never raises: returns error items on failure.
        """
        if not texts:
            return []

        client  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        backoff = 2

        for attempt in range(3):
            try:
                prompt = build_batch_prompt(texts)
                r = client.messages.create(
                    model=MODEL,
                    max_tokens=MAX_TOKENS,
                    messages=[{"role": "user", "content": prompt}]
                )
                raw = r.content[0].text.strip()
                # Strip markdown code fences if Claude adds them
                raw = re.sub(r"^```json\s*", "", raw)
                raw = re.sub(r"```\s*$", "", raw)
                parsed = json.loads(raw)

                if not isinstance(parsed, list):
                    raise ValueError(f"Expected JSON array, got {type(parsed).__name__}")

                # Pad or trim to match input length
                while len(parsed) < len(texts):
                    parsed.append(_ERROR_ITEM.copy())
                parsed = parsed[:len(texts)]

                # Validate each item
                for item in parsed:
                    for field in ("priority", "category", "sentiment"):
                        item[field] = str(item.get(field, "UNKNOWN")).upper()
                        if item[field] not in _VALID[field]:
                            item[field] = "UNKNOWN"
                    item["summary"] = str(item.get("summary", ""))[:120]

                time.sleep(SLEEP_SEC)
                print(f"      {batch_label} OK — {len(texts)} tickets → {len(parsed)} results")
                return parsed

            except anthropic.RateLimitError:
                print(f"      {batch_label} rate limited (attempt {attempt+1}) — backing off {backoff}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)

            except json.JSONDecodeError as e:
                print(f"      {batch_label} JSON parse error: {e}")
                return [_ERROR_ITEM.copy() for _ in texts]

            except Exception as e:
                print(f"      {batch_label} error (attempt {attempt+1}): {e}")
                if attempt == 2:
                    return [_ERROR_ITEM.copy() for _ in texts]
                time.sleep(backoff)
                backoff *= 2

        return [_ERROR_ITEM.copy() for _ in texts]

    # ── Local test (no Spark) ───────────────────────────────────────────
    _test_batch = [
        "Charged twice for my subscription. Need a refund!",
        "Login button is broken on Chrome.",
    ]
    print("Local batch test (2 tickets in 1 call):")
    if ANTHROPIC_API_KEY:
        _results = call_claude_batch(_test_batch, batch_label="[test]")
        for _i, _r in enumerate(_results, 1):
            print(f"  ticket {_i}: {json.dumps(_r)}")
    else:
        print("  (skipped — no API key)")
    return build_batch_prompt, call_claude_batch


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Step 6 — The Batched `pandas_udf`

        ### How the UDF processes rows in batches

        Spark calls the `pandas_udf` once per partition, passing all rows in that
        partition as a `pd.Series`. Inside the UDF, we:

        ```
        Partition arrives (e.g. 10 rows)
            │
            ├─ Rows 1-4:  "too short" or empty?
            │     YES → assign default result immediately (no API call)
            │     NO  → add to current batch
            │
            ├─ Batch full (4 rows)?
            │     YES → send batch to Claude → get 4 results back
            │           match result[0] to row[0], result[1] to row[1], etc.
            │
            ├─ Rows 5-8:  same process...
            │
            ├─ Rows 9-10: only 2 rows left (less than BATCH_SIZE)
            │     → send partial batch of 2 to Claude → get 2 results
            │
            └─ Return pd.Series of 10 JSON strings (one per row, in order)
        ```

        **Key property:** The position of each result in the output Series matches
        the position of the input row. Spark joins them back by position.
        """
    )
    return


@app.cell
def __(BATCH_SIZE, StringType, call_claude_batch, json, pd, pandas_udf):

    @pandas_udf(StringType())
    def batched_ticket_udf(texts: pd.Series) -> pd.Series:
        """
        Receives ALL rows in one partition as a pd.Series of strings.
        Groups them into batches of BATCH_SIZE, sends each batch to Claude
        in a single API call, and returns one JSON string per row.

        The logging shows exactly what happens inside each partition.
        """
        items   = texts.tolist()
        results = [None] * len(items)    # pre-allocate: one slot per row

        # Track which rows need the LLM vs which are too short
        needs_llm = []     # list of (original_index, text)
        short_item = json.dumps({
            "priority": "LOW", "category": "GENERAL",
            "sentiment": "NEUTRAL", "summary": "Ticket too short to analyze."
        })

        # ── Pass 1: Separate short/empty rows from real ones ──────────
        for idx, text in enumerate(items):
            if not text or len(text.strip()) < 8:
                results[idx] = short_item
                print(f"      row {idx}: SKIPPED (too short: {repr(text[:30])})")
            else:
                needs_llm.append((idx, text))

        print(f"   Partition received {len(items)} rows: "
              f"{len(needs_llm)} need LLM, {len(items) - len(needs_llm)} skipped")

        # ── Pass 2: Send LLM rows in batches ─────────────────────────
        batch_num = 0
        for start in range(0, len(needs_llm), BATCH_SIZE):
            batch_num += 1
            chunk = needs_llm[start : start + BATCH_SIZE]
            batch_texts   = [text for _, text in chunk]
            batch_indices = [idx  for idx, _  in chunk]

            label = f"[batch {batch_num}, {len(batch_texts)} rows]"
            print(f"      Sending {label}: row indices {batch_indices}")

            batch_results = call_claude_batch(batch_texts, batch_label=label)

            # Match results back to original row positions
            for (orig_idx, _), result_dict in zip(chunk, batch_results):
                results[orig_idx] = json.dumps(result_dict)

        # Safety check: fill any None slots (should not happen)
        error_json = json.dumps({
            "priority": "UNKNOWN", "category": "UNKNOWN",
            "sentiment": "UNKNOWN", "summary": "ERROR: result not assigned"
        })
        for i in range(len(results)):
            if results[i] is None:
                results[i] = error_json
                print(f"      WARNING: row {i} had no result — filled with error")

        total_batches = batch_num
        total_calls = total_batches
        print(f"   Partition done: {len(items)} rows processed in "
              f"{total_calls} API call(s) (batch_size={BATCH_SIZE})")

        return pd.Series(results)

    print(f"✅  'batched_ticket_udf' ready (batch_size={BATCH_SIZE})")
    return (batched_ticket_udf,)


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Step 7 — Run the Pipeline

        ### What happens when we call `.show()`:

        1. Spark evaluates the DataFrame lazily — nothing runs until an action
        2. Each partition calls `batched_ticket_udf` with its rows
        3. Inside the UDF, rows are grouped into batches and sent to Claude
        4. The log output shows exactly which rows go into which batch
        5. Results come back as JSON strings → `from_json()` expands them into typed columns
        6. The `from_json()` step is pure Spark (JVM) — **zero additional API calls**
        """
    )
    return


@app.cell
def __(
    StringType, StructField, StructType,
    batched_ticket_udf, col, from_json,
    length, tickets_df, when,
):
    llm_schema = StructType([
        StructField("priority",  StringType(), True),
        StructField("category",  StringType(), True),
        StructField("sentiment", StringType(), True),
        StructField("summary",   StringType(), True),
    ])

    short_json = (
        '{"priority":"LOW","category":"GENERAL",'
        '"sentiment":"NEUTRAL","summary":"Too short."}'
    )

    enriched_df = (
        tickets_df
        # Step A: short-circuit rows < 8 chars (no API call needed)
        .withColumn("_llm_json",
                    when(length(col("ticket_text")) < 8, short_json)
                    .otherwise(batched_ticket_udf(col("ticket_text"))))
        # Step B: parse JSON string into typed struct (pure Spark, no API)
        .withColumn("_llm", from_json(col("_llm_json"), llm_schema))
        # Step C: flatten struct into top-level columns
        .select(
            col("id"), col("email"), col("ticket_text"),
            col("_llm.priority").alias("priority"),
            col("_llm.category").alias("category"),
            col("_llm.sentiment").alias("sentiment"),
            col("_llm.summary").alias("summary"),
        )
    )

    print("\n" + "="*70)
    print("  ENRICHED TICKETS (batched)")
    print("="*70)
    enriched_df.show(truncate=55)
    return (enriched_df,)


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Step 8 — Verify Results

        Let's confirm every row got a valid result — no UNKNOWN values
        (except for the edge-case rows that were intentionally too short).
        """
    )
    return


@app.cell
def __(F, col, enriched_df):
    print("=== Priority Distribution ===")
    (enriched_df.groupBy("priority")
     .agg(F.count("*").alias("n"))
     .orderBy(col("n").desc())).show()

    print("=== Category Distribution ===")
    (enriched_df.groupBy("category")
     .agg(F.count("*").alias("n"))
     .orderBy(col("n").desc())).show()

    print("=== Sentiment Distribution ===")
    (enriched_df.groupBy("sentiment")
     .agg(F.count("*").alias("n"))
     .orderBy(col("n").desc())).show()

    print("=== HIGH Priority Tickets ===")
    (enriched_df.filter(col("priority") == "HIGH")
     .select("id", "email", "category", "summary")).show(truncate=60)

    # Check for any UNKNOWN values (should only be on error rows)
    _unknown_count = enriched_df.filter(
        (col("priority") == "UNKNOWN") |
        (col("category") == "UNKNOWN") |
        (col("sentiment") == "UNKNOWN")
    ).count()
    if _unknown_count > 0:
        print(f"⚠️  {_unknown_count} row(s) have UNKNOWN values (check API errors above)")
    else:
        print("✅  All rows have valid enrichment values")
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 9 — Save (partitioned by priority)")
    return


@app.cell
def __(enriched_df):
    import tempfile, os as _os
    _out = _os.path.join(tempfile.gettempdir(), "tickets_enriched_p2_scaleout")
    enriched_df.write.mode("overwrite").partitionBy("priority").parquet(_out)
    print(f"✅  Saved → {_out}")
    for _r, _d, _f in _os.walk(_out):
        for _name in _f:
            if not _name.startswith("."):
                print(f"   {_os.path.relpath(_os.path.join(_r, _name), _out)}")
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Summary: How Batching Scales

        ### The algorithm in one picture

        ```
        Partition (N rows)
            │
            ├── Filter: short/empty rows → instant result (no API call)
            │
            ├── Batch 1: rows 0-3 ──→ 1 API call ──→ 4 JSON results
            ├── Batch 2: rows 4-7 ──→ 1 API call ──→ 4 JSON results
            ├── Batch 3: rows 8-9 ──→ 1 API call ──→ 2 JSON results  (partial batch)
            │
            └── Reassemble: 10 JSON strings returned in original row order
        ```

        ### Why this scales

        | Metric | Program 2 (1 call/row) | This notebook (batched) |
        |--------|------------------------|-------------------------|
        | 12 rows | 12 API calls | 3 API calls (batch=4) |
        | 10,000 rows | 10,000 API calls | 2,500 API calls |
        | 1,000,000 rows | 1,000,000 API calls | 250,000 API calls |

        ### What stays the same
        - Same output schema (priority, category, sentiment, summary)
        - Same `from_json()` expansion — zero extra API calls
        - Same error handling — never raises, returns error JSON
        - Same `.env` configuration pattern

        ### For even larger scale (billions of rows)
        Combine this batching with the strategies from the slides:
        1. **Pre-filter** — skip nulls, duplicates, already-labeled rows
        2. **Rules first** — handle easy cases with keywords, send only hard cases to Claude
        3. **Sample + Train** — label 50K rows with Claude, train a Spark ML model for the rest
        4. **Cache results** — never call Claude twice for the same text
        5. **Batch API** — use Anthropic's async batch endpoint for 50% cost savings
        """
    )
    return


if __name__ == "__main__":
    app.run()
