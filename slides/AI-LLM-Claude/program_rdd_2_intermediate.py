import marimo

__generated_with = "0.23.16"
app = marimo.App(width="full", app_title="PySpark RDD + Claude LLM — RDD Program 2: Intermediate")


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        # 🔶 RDD Program 2: PySpark RDD + Claude LLM — Intermediate

        **Goal:** Full MapReduce pipeline where Claude enriches records
        and the results feed directly into RDD key-value aggregations.

        This notebook demonstrates:
        - **Multi-field JSON output** from Claude, parsed inside `mapPartitions`
        - **True multi-record batching** inside a partition iterator
        - **Key-value RDD operations** on LLM output: `groupByKey`, `reduceByKey`,
          `flatMap`, `join`
        - **Dead-letter accumulator** — Spark's native counter for failed records
        - **Re-partitioning by LLM key** to co-locate same-topic records

        **Dataset:** 18 e-commerce product reviews across 3 categories.

        ---
        > MapReduce mindset: Claude is part of the **Map phase**.
        > Its output keys drive the **Shuffle and Reduce phases** downstream.
        """
    )
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Architecture

        ```
        Raw RDD (reviews)
            │
            ▼  mapPartitions(enrich_partition)
        Enriched RDD  [(id, category, sentiment, score, summary), ...]
            │
            ├──▶  map to (category, score)  →  reduceByKey(avg)
            │
            ├──▶  map to (sentiment, 1)     →  reduceByKey(sum)
            │
            ├──▶  flatMap to (category, review_id)  →  groupByKey
            │
            └──▶  filter(score < 4)  →  flag for follow-up
        ```

        The LLM output **keys** (`category`, `sentiment`) feed directly
        into standard MapReduce aggregations — Claude becomes a mapper.
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
    import marimo as mo
    from pyspark.sql import SparkSession
    from operator import add

    NOTEBOOK_DIR = str(pathlib.Path(__file__).resolve().parent)

    return SparkSession, NOTEBOOK_DIR, add, anthropic, json, mo, os, re, time


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
    MAX_TOKENS        = int(os.environ.get("LLM_MAX_TOKENS",      "300"))
    RATE_LIMIT_RPM    = int(os.environ.get("LLM_RATE_LIMIT_RPM",  "50"))
    BATCH_SIZE        = int(os.environ.get("LLM_BATCH_SIZE",       "3"))
    MAX_CHARS         = int(os.environ.get("LLM_MAX_CHARS",        "800"))
    SPARK_MASTER      = os.environ.get("SPARK_MASTER",             "local[*]")
    N_PARTITIONS      = int(os.environ.get("SPARK_PARTITIONS",     "3"))

    SLEEP_SEC = (60 / RATE_LIMIT_RPM) * BATCH_SIZE

    ok = "✅" if ANTHROPIC_API_KEY else "⚠️ "
    print(f"{ok} ANTHROPIC_API_KEY  : {ANTHROPIC_API_KEY[:12]}..." if ANTHROPIC_API_KEY
          else f"{ok} ANTHROPIC_API_KEY  : NOT SET")
    print(f"   ANTHROPIC_MODEL    : {MODEL}")
    print(f"   LLM_MAX_TOKENS     : {MAX_TOKENS}")
    print(f"   LLM_BATCH_SIZE     : {BATCH_SIZE}  (records per Claude call)")
    print(f"   LLM_RATE_LIMIT_RPM : {RATE_LIMIT_RPM}  → sleep {SLEEP_SEC:.2f}s/batch")
    print(f"   LLM_MAX_CHARS      : {MAX_CHARS}")
    print(f"   SPARK_MASTER       : {SPARK_MASTER}")
    print(f"   SPARK_PARTITIONS   : {N_PARTITIONS}")
    return (
        ANTHROPIC_API_KEY, BATCH_SIZE, MAX_CHARS, MAX_TOKENS,
        MODEL, N_PARTITIONS, RATE_LIMIT_RPM, SLEEP_SEC,
        SPARK_MASTER, load_dotenv,
    )


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 2 — Spark Context with Accumulator")
    return


@app.cell
def __(SPARK_MASTER, SparkSession):
    spark = (
        SparkSession.builder
        .appName("RDD_Program2_Intermediate")
        .master(SPARK_MASTER)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext

    # ── Accumulator: counts records that fail LLM enrichment ─────────────
    # Accumulators are Spark's distributed counter — executors increment them,
    # the driver reads the total. Safe for fire-and-forget counting.
    dead_letter_count = sc.accumulator(0)

    print(f"✅  Spark {spark.version}  |  master: {sc.master}")
    print(f"   Accumulator 'dead_letter_count' registered")
    return dead_letter_count, sc, spark


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 3 — Product Review Dataset")
    return


@app.cell
def __(N_PARTITIONS, NOTEBOOK_DIR, os, spark):
    reviews_rdd = (
        spark.read.csv(os.path.join(NOTEBOOK_DIR, "data", "product_reviews.csv"), header=True, inferSchema=True)
        .rdd.map(lambda row: (row["id"], row["product_category"], row["review_text"]))
        .repartition(N_PARTITIONS)
    )

    print(f"RDD: {reviews_rdd.count()} reviews  |  "
          f"{reviews_rdd.rdd.getNumPartitions() if hasattr(reviews_rdd, 'rdd') else reviews_rdd.getNumPartitions()} partitions")

    print("\nPartition layout:")
    for _i, _part in enumerate(reviews_rdd.glom().collect()):
        _ids = [r[0] for r in _part]
        _cats = sorted(set(r[1] for r in _part))
        print(f"  Partition {_i}: IDs {_ids}  |  categories {_cats}")
    return (reviews_rdd,)


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Step 4 — Batch Prompt and Claude Call

        We send `BATCH_SIZE` reviews in one prompt and receive a JSON **array**.
        One API call enriches multiple records — fewer round-trips, same rate limit.

        ### Output schema per record
        ```json
        {
          "sentiment":    "POSITIVE" | "NEGATIVE" | "NEUTRAL",
          "rating":       1–5,
          "summary":      "one sentence, max 15 words",
          "follow_up":    true | false
        }
        ```
        `follow_up=true` means the review signals a problem needing action.
        """
    )
    return


@app.cell
def __(ANTHROPIC_API_KEY, BATCH_SIZE, MAX_CHARS, MAX_TOKENS,
       MODEL, SLEEP_SEC, anthropic, json, re, time):

    _VALID_SENT = {"POSITIVE", "NEGATIVE", "NEUTRAL"}
    _ERR_ITEM   = {
        "sentiment": "UNKNOWN", "rating": 0,
        "summary": "Error during processing.", "follow_up": False
    }

    def build_batch_review_prompt(reviews_batch: list) -> str:
        n        = len(reviews_batch)
        numbered = "\n".join(
            f"{i+1}. [{cat}] {text[:MAX_CHARS]}"
            for i, (_, cat, text) in enumerate(reviews_batch)
        )
        return (
            f"Analyze these {n} product reviews. "
            f"Return a JSON array of exactly {n} objects, one per review, in order.\n"
            "No markdown — only raw JSON.\n\n"
            "Each object:\n"
            '  "sentiment":  "POSITIVE" | "NEGATIVE" | "NEUTRAL"\n'
            '  "rating":     integer 1-5 (1=terrible, 5=excellent)\n'
            '  "summary":    one sentence, max 15 words\n'
            '  "follow_up":  true if the review signals a problem needing action, else false\n\n'
            f"Reviews:\n{numbered}"
        )

    def call_claude_batch_reviews(reviews_batch: list) -> list:
        """
        Sends up to BATCH_SIZE (id, category, text) tuples to Claude.
        Returns a list of enrichment dicts — one per input record.
        Never raises: returns error items on any failure.
        """
        if not reviews_batch:
            return []

        client  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        backoff = 2

        for attempt in range(3):
            try:
                r   = client.messages.create(
                    model=MODEL, max_tokens=MAX_TOKENS,
                    messages=[{"role": "user",
                                "content": build_batch_review_prompt(reviews_batch)}]
                )
                raw = re.sub(r"^```json\s*", "", r.content[0].text.strip())
                raw = re.sub(r"```\s*$", "", raw)
                parsed = json.loads(raw)

                if not isinstance(parsed, list):
                    raise ValueError("Expected list")

                while len(parsed) < len(reviews_batch):
                    parsed.append(_ERR_ITEM.copy())
                parsed = parsed[:len(reviews_batch)]

                for item in parsed:
                    item["sentiment"] = str(item.get("sentiment","UNKNOWN")).upper()
                    if item["sentiment"] not in _VALID_SENT:
                        item["sentiment"] = "UNKNOWN"
                    item["rating"]    = max(1, min(5, int(item.get("rating", 0))))
                    item["summary"]   = str(item.get("summary",""))[:100]
                    item["follow_up"] = bool(item.get("follow_up", False))

                time.sleep(SLEEP_SEC)
                return parsed

            except anthropic.RateLimitError:
                time.sleep(backoff); backoff = min(backoff * 2, 60)
            except Exception as e:
                print(f"   Batch error attempt {attempt+1}: {e}")
                if attempt == 2:
                    return [_ERR_ITEM.copy() for _ in reviews_batch]
                time.sleep(backoff); backoff *= 2

        return [_ERR_ITEM.copy() for _ in reviews_batch]

    # ── Local test ────────────────────────────────────────────────────────
    _test_batch = [
        (1, "Electronics", "Fantastic headphones, sound is incredible."),
        (2, "Furniture",   "Chair broke after two days. Absolute rubbish."),
    ]
    print("Local batch test:")
    if ANTHROPIC_API_KEY:
        for _i, (_rec, _res) in enumerate(
            zip(_test_batch, call_claude_batch_reviews(_test_batch)), 1
        ):
            print(f"  {_i}: {json.dumps(_res)}")
    else:
        print("  (skipped — ANTHROPIC_API_KEY not set)")
    return build_batch_review_prompt, call_claude_batch_reviews


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Step 5 — `mapPartitions()` with Batching and Dead-Letter Accumulator

        Inside the partition function we:
        1. Collect records into batches of `BATCH_SIZE`
        2. Send each batch to Claude in one call
        3. Yield enriched tuples for successful records
        4. Increment the Spark accumulator for failed ones
        """
    )
    return


@app.cell
def __(ANTHROPIC_API_KEY, BATCH_SIZE, MAX_TOKENS, MODEL,
       call_claude_batch_reviews, dead_letter_count):

    def enrich_partition_batched(records):
        """
        RDD partition function.
        Receives an iterator of (id, category, text) tuples.
        Yields (id, category, text, sentiment, rating, summary, follow_up).

        Batches records → sends BATCH_SIZE at a time to Claude →
        increments dead_letter_count accumulator for any failures.
        """
        import os as _os
        from dotenv import load_dotenv as _ld
        _ld(override=False)

        # Re-read env inside the executor function — safest approach
        _key = _os.environ.get("ANTHROPIC_API_KEY", ANTHROPIC_API_KEY)

        buffer  = []   # accumulate up to BATCH_SIZE records
        records_list = list(records)   # materialise iterator for batching

        for start in range(0, len(records_list), BATCH_SIZE):
            batch      = records_list[start : start + BATCH_SIZE]
            enrichments = call_claude_batch_reviews(batch)

            for (rec_id, category, text), enrichment in zip(batch, enrichments):
                if enrichment.get("sentiment") in {"UNKNOWN"} \
                        or enrichment.get("rating") == 0:
                    dead_letter_count.add(1)   # Spark accumulator — thread-safe
                    # Still yield the record with error markers so no data is lost
                yield (
                    rec_id,
                    category,
                    text,
                    enrichment.get("sentiment", "UNKNOWN"),
                    enrichment.get("rating",    0),
                    enrichment.get("summary",   ""),
                    enrichment.get("follow_up", False),
                )

    print("✅  enrich_partition_batched() defined")
    print(f"   Batch size : {BATCH_SIZE} records per Claude call")
    print(f"   Output     : (id, category, text, sentiment, rating, summary, follow_up)")
    print(f"   Accumulator: dead_letter_count will track failures")
    return (enrich_partition_batched,)


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 6 — Run the Enrichment")
    return


@app.cell
def __(enrich_partition_batched, reviews_rdd):
    enriched_rdd = reviews_rdd.mapPartitions(enrich_partition_batched)

    # Trigger execution and cache — we'll query this RDD multiple times
    enriched_rdd.cache()
    all_enriched = enriched_rdd.collect()

    print("Enriched Reviews:")
    print(f"\n  {'ID':>3}  {'Cat':<12}  {'Sent':<10}  {'★':>2}  {'FU'}  Summary")
    print(f"  {'-'*3}  {'-'*12}  {'-'*10}  {'-'*2}  {'-'*2}  {'-'*38}")
    for _row in sorted(all_enriched):
        rid, _cat, _, _sent, rating, summary, follow_up = _row
        fu_flag = "⚠️" if follow_up else "  "
        print(f"  {rid:>3}  {_cat:<12}  {_sent:<10}  {rating:>2}  {fu_flag}  {summary[:40]}")
    return all_enriched, enriched_rdd


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 7 — Dead-Letter Report")
    return


@app.cell
def __(dead_letter_count, all_enriched):
    total   = len(all_enriched)
    failed  = dead_letter_count.value
    success = total - failed

    print(f"{'='*45}")
    print(f"  Enrichment Report")
    print(f"{'='*45}")
    print(f"  Total records : {total}")
    print(f"  Successful    : {success}  ({100*success/total:.0f}%)")
    print(f"  Dead letters  : {failed}   (accumulator value)")
    print(f"{'='*45}")
    return failed, success, total


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Step 8 — MapReduce Aggregations on the Enriched RDD

        The LLM output feeds directly into standard RDD key-value operations.
        Claude is the **mapper**; the aggregations below are the **reducer**.
        """
    )
    return


@app.cell
def __(add, enriched_rdd):
    # ── 8a: Average rating per category ──────────────────────────────────
    # map to (category, (rating, 1))  →  reduceByKey  →  divide
    avg_rating_by_cat = (
        enriched_rdd
        .map(lambda r: (r[1], (r[4], 1)))                  # (cat, (rating, count))
        .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))  # sum ratings + counts
        .map(lambda kv: (kv[0], round(kv[1][0]/kv[1][1], 2)))  # avg
        .sortBy(lambda kv: kv[1], ascending=False)
        .collect()
    )

    print("=== Average Rating per Category ===")
    for _cat, _avg in avg_rating_by_cat:
        _stars = "★" * round(_avg) + "☆" * (5 - round(_avg))
        print(f"  {_cat:<14}  {_avg:.2f}  {_stars}")
    return (avg_rating_by_cat,)


@app.cell
def __(add, enriched_rdd):
    # ── 8b: Sentiment distribution ────────────────────────────────────────
    sentiment_counts = (
        enriched_rdd
        .map(lambda r: (r[3], 1))
        .reduceByKey(add)
        .sortBy(lambda kv: kv[1], ascending=False)
        .collect()
    )

    print("=== Sentiment Distribution ===")
    _total = sum(c for _, c in sentiment_counts)
    for _sent, _count in sentiment_counts:
        _pct = 100 * _count / _total
        _bar = "█" * _count
        print(f"  {_sent:<10}  {_count:>2}  ({_pct:.0f}%)  {_bar}")
    return (sentiment_counts,)


@app.cell
def __(enriched_rdd):
    # ── 8c: All review IDs grouped by category  (groupByKey) ──────────────
    ids_by_category = (
        enriched_rdd
        .map(lambda r: (r[1], r[0]))          # (category, id)
        .groupByKey()
        .map(lambda kv: (kv[0], sorted(list(kv[1]))))
        .sortBy(lambda kv: kv[0])
        .collect()
    )

    print("=== Review IDs per Category (groupByKey) ===")
    for _cat, _ids in ids_by_category:
        print(f"  {_cat:<14}  IDs: {_ids}")
    return (ids_by_category,)


@app.cell
def __(enriched_rdd):
    # ── 8d: Negative reviews needing follow-up ────────────────────────────
    follow_up_rdd = (
        enriched_rdd
        .filter(lambda r: r[6] == True)        # follow_up flag
        .map(lambda r: {
            "id":       r[0],
            "category": r[1],
            "sentiment":r[3],
            "rating":   r[4],
            "summary":  r[5],
        })
        .sortBy(lambda d: d["rating"])         # worst-rated first
    )

    follow_ups = follow_up_rdd.collect()
    print(f"=== Reviews Flagged for Follow-Up ({len(follow_ups)}) ===")
    for _item in follow_ups:
        print(f"  [{_item['id']:>2}] ★{_item['rating']}  {_item['category']:<12}  {_item['summary'][:45]}")
    return follow_up_rdd, follow_ups


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Step 9 — Re-Partition by LLM Key

        After enrichment, we re-partition the RDD by `sentiment`
        using `partitionBy()` on a key-value RDD.

        This co-locates all POSITIVE records on one partition,
        all NEGATIVE on another — making subsequent per-sentiment
        operations free of shuffle.
        """
    )
    return


@app.cell
def __(enriched_rdd):
    # Convert to (key, value) pair RDD for partitionBy
    kv_rdd = enriched_rdd.map(lambda r: (r[3], r))   # key = sentiment

    # Hash-partition by sentiment into 3 buckets
    repartitioned = kv_rdd.partitionBy(3)

    print("After partitionBy(3) on sentiment key:")
    for _i, _part in enumerate(repartitioned.glom().collect()):
        _sentiments = sorted(set(kv[0] for kv in _part))
        _ids        = sorted(kv[1][0] for kv in _part)
        print(f"  Partition {_i}: sentiments={_sentiments}  ids={_ids}  ({len(_part)} records)")
    return kv_rdd, repartitioned


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 10 — Join: Enrich with a Second RDD")
    return


@app.cell
def __(enriched_rdd, sc):
    # Simulate a product metadata lookup table (normally from a file or DB)
    category_metadata = [
        ("Electronics", {"department": "Tech",     "return_window_days": 30}),
        ("Furniture",   {"department": "Home",     "return_window_days": 60}),
        ("Clothing",    {"department": "Fashion",  "return_window_days": 14}),
    ]
    meta_rdd = sc.parallelize(category_metadata)

    # Join on category key
    reviews_kv = enriched_rdd.map(lambda r: (r[1], r))   # key = category
    joined_rdd = reviews_kv.join(meta_rdd)

    # Flatten: (category, (review_tuple, meta_dict)) → flat dict
    joined = (
        joined_rdd
        .map(lambda kv: {
            "id":           kv[1][0][0],
            "category":     kv[0],
            "sentiment":    kv[1][0][3],
            "rating":       kv[1][0][4],
            "follow_up":    kv[1][0][6],
            "department":   kv[1][1]["department"],
            "return_days":  kv[1][1]["return_window_days"],
        })
        .sortBy(lambda d: d["id"])
        .collect()
    )

    print("Joined Dataset (review + category metadata):")
    print(f"\n  {'ID':>3}  {'Category':<12}  {'Dept':<8}  {'Return':>6}d  {'★':>2}  Sentiment")
    print(f"  {'-'*65}")
    for _row in joined:
        print(f"  {_row['id']:>3}  {_row['category']:<12}  {_row['department']:<8}  "
              f"{_row['return_days']:>6}   {_row['rating']:>2}  {_row['sentiment']}")
    return category_metadata, joined, joined_rdd, meta_rdd, reviews_kv


@app.cell(hide_code=True)
def __(mo):
    mo.md("## Step 11 — Promote to DataFrame and Save")
    return


@app.cell
def __(enriched_rdd, spark):
    import tempfile, os as _os

    # Promote to DataFrame for Parquet output
    enriched_df = enriched_rdd.toDF([
        "id","category","text","sentiment","rating","summary","follow_up"
    ])

    out_path = _os.path.join(tempfile.gettempdir(), "reviews_enriched_rdd2")

    (enriched_df
     .write
     .mode("overwrite")
     .partitionBy("category", "sentiment")    # two-level partitioning
     .parquet(out_path)
    )

    print(f"✅  Saved (partitioned by category + sentiment) → {out_path}")
    enriched_df.groupBy("category","sentiment").count().orderBy("category","sentiment").show()
    return enriched_df, out_path


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Summary

        | Pattern | Implementation |
        |---------|---------------|
        | **Config** | `.env` via `load_dotenv()`, re-read inside executor functions |
        | **RDD hook** | `mapPartitions()` — one iterator per partition |
        | **Batching** | `BATCH_SIZE` records per Claude call, inside the partition loop |
        | **Multi-field output** | JSON array from Claude, parsed and validated per record |
        | **Dead-letter count** | Spark `Accumulator` — incremented by executors, read by driver |
        | **Aggregation on LLM keys** | `reduceByKey`, `groupByKey`, `filter`, `sortBy` |
        | **Re-partition by LLM key** | `map(lambda r: (r.sentiment, r)).partitionBy(3)` |
        | **RDD join** | `reviews_kv.join(meta_rdd)` on category key |
        | **Output** | `toDF().write.partitionBy("category","sentiment").parquet(...)` |

        ### The MapReduce view

        ```
        PARTITION   →  mapPartitions(enrich_partition_batched)  [MAP  phase]
                    →  Claude returns (sentiment, rating, follow_up)
                    →  reduceByKey / groupByKey                  [REDUCE phase]
                    →  partitionBy(sentiment)                    [re-SHUFFLE]
                    →  join(meta_rdd)                            [secondary JOIN]
        ```

        Claude enriches each record in the **Map phase**.
        Its output fields (`sentiment`, `category`) become the **keys**
        that drive all subsequent shuffle and reduce operations.
        """
    )
    return


if __name__ == "__main__":
    app.run()
