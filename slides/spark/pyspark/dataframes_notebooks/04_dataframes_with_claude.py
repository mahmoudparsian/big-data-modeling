import marimo

__generated_with = "0.23.10"
app = marimo.App(width="medium", app_title="DataFrames 4 — Claude")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # DataFrames, Part 4: Spark + Claude

    **Notebook 4 of 5** · before: `01_dataframe_basics.py`, `02_dataframe_aggregations.py`, `03_dataframe_joins.py` · next: `05_dataframe_advanced.py`

    Spark is great with **structured** data: numbers, categories, dates. But a lot of
    business data is **free text**: customer reviews, support emails, notes. A large
    language model (LLM) such as **Claude** can read that text and turn it into structured
    columns, which Spark can then group, join, and count.

    **Our story:** *Bean & Leaf* (the coffee chain from Notebooks 1–3) collected customer
    reviews. Some are in English, some in Spanish. The manager wants to know: *what are
    customers happy or unhappy about, in each store?*

    **What you will learn**

    1. Reading a secret API key from a `.env` file (never from the code)
    2. Calling Claude from Python, and getting back **structured** answers (JSON that
       matches a schema) instead of free text
    3. Two ways to label a DataFrame with Claude: on the **driver** (small data) and
       inside **`mapPartitions()`** on the executors (bigger data)
    4. Analyzing Claude's labels with `groupBy()` and `join()` (Notebooks 2 and 3)
    5. Letting Claude **write Spark SQL** from an English question, safely
    6. Letting Claude **summarize** an aggregated result for a manager
    7. Cost, speed, and privacy: what to watch out for

    **Cost:** one run of this notebook makes about 30 small API calls. With the default
    settings that costs a few cents.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1. Setup: the API key in a `.env` file

    Claude is a web service. Every request must carry your **API key**, which is like a
    password: anyone who has it can spend your money. So:

    * **Never** type the key into a notebook or a `.py` file (it would end up on GitHub).
    * Put it in a file named **`.env`** in this folder, one `NAME=value` per line:

    ```bash
    cp .env.example .env      # then edit .env and paste your key
    ```

    * `.env` is listed in the repository's `.gitignore`, so git never commits it.
      (`.env.example` *is* committed: it shows which settings exist, without real values.)

    The package `python-dotenv` reads `.env` and puts its values into `os.environ`.
    We load the file **from this notebook's folder**, so it works no matter which folder you
    started marimo from. `override=True` means the file wins over an older key that may be
    set in your shell.

    **Install once:** `pip install anthropic python-dotenv pyspark marimo`
    """)
    return


@app.cell
def _():
    import json
    import os

    import marimo as mo
    from dotenv import load_dotenv

    ENV_PATH = mo.notebook_dir() / ".env"
    env_found = load_dotenv(ENV_PATH, override=True)

    MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-opus-5-5")
    EFFORT = os.environ.get("LLM_EFFORT", "low")
    has_key = os.environ.get("ANTHROPIC_API_KEY", "").startswith("sk-ant-")

    print(f".env file found:  {env_found}  ({ENV_PATH})")
    print(f"API key present:  {has_key}")  # never print the key itself!
    print(f"Model:            {MODEL}")
    print(f"Effort:           {EFFORT}")
    return EFFORT, MODEL, env_found, has_key, json, mo, os


@app.cell(hide_code=True)
def _(has_key, mo):
    mo.stop(
        not has_key,
        mo.callout(
            mo.md(
                "**No API key found.** Copy `.env.example` to `.env` in this folder, put your "
                "key after `ANTHROPIC_API_KEY=`, and re-run the notebook. The cells below "
                "need the key, so they will not run until then."
            ),
            kind="warn",
        ),
    )
    mo.callout(mo.md("API key loaded from `.env`. Ready to call Claude."), kind="success")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2. Start Spark

    We start Spark **after** loading `.env`. In local mode, Spark's Python workers inherit
    the environment of this process, so they can see `ANTHROPIC_API_KEY` too. (On a real
    cluster you would give the key to the executors through the cluster's secret manager,
    never inside your code.)
    """)
    return


@app.cell
def _(env_found, has_key):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    _ = (env_found, has_key)  # makes marimo run this cell after the .env cell

    spark = (
        SparkSession.builder
        .appName("dataframes-04-claude")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print("Spark version:", spark.version)
    return F, spark


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 3. Your first call to Claude

    The `anthropic` package talks to the Claude API. `anthropic.Anthropic()` creates a
    **client**; it finds `ANTHROPIC_API_KEY` in the environment by itself.

    A request has:

    * `model`: which Claude model answers
    * `max_tokens`: the most text the answer may contain (a *token* is about ¾ of a word)
    * `messages`: the conversation so far; here, a single user message
    * `output_config={"effort": ...}`: how much the model thinks before answering

    The answer, `response.content`, is a **list of blocks**. We keep the `text` blocks.
    `response.usage` tells you how many tokens you paid for.
    """)
    return


@app.cell
def _(EFFORT, MODEL, has_key, mo):
    import anthropic

    mo.stop(not has_key)
    client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from the environment

    try:
        first_response = client.messages.create(
            model=MODEL,
            max_tokens=16000,
            output_config={"effort": EFFORT},
            messages=[{
                "role": "user",
                "content": "In two sentences, explain to a student what a Spark DataFrame is.",
            }],
        )
    except anthropic.AuthenticationError:
        mo.stop(True, mo.callout(mo.md(
            "**The API rejected your key** (`401 invalid x-api-key`). It may be mistyped, "
            "revoked, or expired. Put a valid key in `.env` and re-run the notebook."
        ), kind="danger"))
    first_answer = "".join(b.text for b in first_response.content if b.type == "text")
    print(first_answer)
    print("\ntokens:", first_response.usage.input_tokens, "in,", first_response.usage.output_tokens, "out")
    return anthropic, client, first_answer, first_response


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 4. The data: customer reviews

    Twelve reviews from four stores. The text is messy, the way real reviews are: some are
    in Spanish, one mentions a price, some talk about two things at once.
    """)
    return


@app.cell
def _(spark):
    reviews = spark.createDataFrame(
        [
            (1,  "S01", "2026-10-01", 5, "Best latte in San Jose. The barista remembered my name and my order!"),
            (2,  "S01", "2026-10-02", 2, "Waited 20 minutes for a cold brew. The line was out the door and only one person was working."),
            (3,  "S02", "2026-10-01", 4, "Great place to study near campus, fast wifi. Croissants could be fresher."),
            (4,  "S02", "2026-10-03", 1, "Six dollars for a small espresso?! Way overpriced for students."),
            (5,  "S03", "2026-10-02", 5, "El chai latte está buenísimo y el personal es muy amable."),
            (6,  "S03", "2026-10-02", 3, "Coffee was fine. Tables were sticky and the trash was overflowing."),
            (7,  "S03", "2026-10-03", 4, "Nice view of the harbor, good cold brew, a bit pricey."),
            (8,  "S04", "2026-10-01", 2, "My muffin was stale and the staff seemed annoyed when I asked for a new one."),
            (9,  "S04", "2026-10-02", 5, "Quick service even at lunch rush. Green tea was perfect."),
            (10, "S04", "2026-10-03", 3, "Café correcto, pero la música estaba demasiado alta."),
            (11, "S01", "2026-10-03", 4, "Solid espresso, friendly staff, but seating is limited."),
            (12, "S02", "2026-10-02", 2, "They got my order wrong twice and the latte was lukewarm."),
        ],
        "review_id INT, store_id STRING, review_date STRING, rating INT, text STRING",
    )
    stores = spark.createDataFrame(
        [("S01", "Downtown", "San Jose"), ("S02", "Campus", "Santa Clara"),
         ("S03", "Harbor", "San Francisco"), ("S04", "Mall", "San Jose")],
        "store_id STRING, store_name STRING, city STRING",
    )
    reviews.show(truncate=60)
    return reviews, stores


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 5. Structured output: make Claude answer in columns

    If you ask *"What is this review about?"*, Claude answers in a sentence, and every answer
    is worded differently. Spark cannot `groupBy` sentences. We want **fixed columns with
    fixed values**, like a form:

    | Column | Allowed values |
    |---|---|
    | `sentiment` | `positive`, `neutral`, `negative` |
    | `topic` | `drinks`, `food`, `service`, `wait time`, `price`, `cleanliness`, `atmosphere` |
    | `language` | a two-letter code such as `en`, `es` |
    | `summary_en` | a short English summary |

    We describe this form as a **Pydantic** class. `client.messages.parse(...,
    output_format=ReviewLabels)` sends the schema to Claude, and the API **guarantees** the
    answer is valid JSON that matches it. `response.parsed_output` is then a `ReviewLabels`
    object. (`Literal[...]` lists the allowed values.)

    The **system prompt** gives the rules that apply to every review, like instructions to a
    new employee. Clear rules (e.g. *pick the main complaint*) make the labels consistent.
    """)
    return


@app.cell
def _(EFFORT, MODEL):
    from typing import Literal

    from pydantic import BaseModel, Field

    class ReviewLabels(BaseModel):
        sentiment: Literal["positive", "neutral", "negative"]
        topic: Literal["drinks", "food", "service", "wait time", "price", "cleanliness", "atmosphere"]
        language: str = Field(description="ISO 639-1 code of the review's language, e.g. en, es")
        summary_en: str = Field(description="English summary in at most 10 words")

    LABEL_SYSTEM = (
        "You label customer reviews for a coffee chain. "
        "sentiment: the customer's overall feeling. "
        "topic: what the review is mainly about; if it praises one thing and complains about "
        "another, choose the complaint, because the manager needs to act on it. "
        "Write summary_en in English, even if the review is in another language."
    )

    def label_review(llm, text):
        """Ask Claude to label one review. Returns a dict (None values if Claude declines)."""
        response = llm.messages.parse(
            model=MODEL,
            max_tokens=16000,
            output_config={"effort": EFFORT},
            system=LABEL_SYSTEM,
            messages=[{"role": "user", "content": text}],
            output_format=ReviewLabels,
        )
        if response.stop_reason == "refusal" or response.parsed_output is None:
            return {"sentiment": None, "topic": None, "language": None, "summary_en": None}
        return response.parsed_output.model_dump()
    return BaseModel, Field, LABEL_SYSTEM, Literal, ReviewLabels, label_review


@app.cell
def _(client, label_review, reviews):
    # Try it on one review first (always test on one row before running on all of them)
    one_text = reviews.filter("review_id = 6").first()["text"]
    print(one_text)
    print(label_review(client, one_text))
    return (one_text,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 6. Pattern A: label on the driver (small data)

    The simplest way, fine for hundreds of rows:

    1. `collect()` the text column to the driver (your Python process)
    2. call Claude once per row, in a normal Python loop
    3. turn the answers into a new DataFrame with `createDataFrame()`
    4. `join()` it back to the original on the key (`review_id`), as in Notebook 3

    The answers are **not deterministic**: if you run the notebook again, a borderline
    review may get a different label. In a real project you run the labeling **once**, save
    the result (e.g. `labels.write.parquet(...)`), and analyze the saved table.
    """)
    return


@app.cell
def _(client, label_review, reviews, spark):
    label_rows = []
    for _row in reviews.select("review_id", "text").collect():
        _labels = label_review(client, _row["text"])
        label_rows.append({"review_id": _row["review_id"], **_labels})

    labels_driver = spark.createDataFrame(
        label_rows,
        "review_id INT, sentiment STRING, topic STRING, language STRING, summary_en STRING",
    )
    reviews_labeled = reviews.join(labels_driver, on="review_id", how="left")
    (reviews_labeled.select("review_id", "store_id", "rating", "sentiment", "topic", "language", "summary_en")
                    .orderBy("review_id")
                    .show(truncate=False))
    return label_rows, labels_driver, reviews_labeled


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 7. Pattern B: label on the executors with `mapPartitions()` (bigger data)

    With a million reviews, `collect()` would overload the driver, and one-by-one calls
    would take days. Instead, let **each partition** label its own rows, in parallel, on
    the executors. You know the tool from RDDs: **`mapPartitions()`**.

    Why `mapPartitions()` and not `map()`? A client opens network connections. Creating
    one **per partition** (instead of one per row) reuses them for all rows in that
    partition. And the client cannot be created on the driver and shipped to the
    executors, so we create it **inside** the function.

    The number of partitions decides how many requests run **at the same time**. Keep it
    small enough to stay under your account's **rate limit** (the SDK automatically
    retries a request that hits the limit).
    """)
    return


@app.cell
def _(client, label_review, reviews):
    _ = client  # runs only if the key worked above (this cell makes its own clients)

    def label_partition(rows):
        import anthropic as _anthropic       # runs on the executor
        llm = _anthropic.Anthropic()         # one client per partition, key from the environment
        for r in rows:
            yield (r["review_id"], *label_review(llm, r["text"]).values())

    labels_parallel = (
        reviews.select("review_id", "text")
        .repartition(4)                      # 4 partitions = up to 4 requests at a time
        .rdd
        .mapPartitions(label_partition)
        .toDF("review_id INT, sentiment STRING, topic STRING, language STRING, summary_en STRING")
    )
    labels_parallel.orderBy("review_id").show(truncate=False)
    return label_partition, labels_parallel


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Compare this table with the one from Pattern A. Most labels agree, but a mixed review
    (praise *and* a complaint, like review 7 or 10) may get a different label, because each
    call is a new, independent answer. Another reason to label once and save the result.

    (You could also wrap `label_review` in a UDF. `mapPartitions()` is clearer here
    because it makes the one-client-per-partition idea visible.)

    ## 8. Analyze the labels with Spark

    Now the free text has become columns, and everything from Notebooks 2 and 3 works.
    **Question:** per store: number of reviews, average star rating, share of negative
    reviews, and the most common complaint.
    """)
    return


@app.cell
def _(F, reviews_labeled, stores):
    store_summary = (
        reviews_labeled
        .join(stores, on="store_id")
        .groupBy("store_id", "store_name", "city")
        .agg(
            F.count("*").alias("reviews"),
            F.round(F.avg("rating"), 2).alias("avg_rating"),
            F.round(F.avg((F.col("sentiment") == "negative").cast("int")) * 100, 0).alias("pct_negative"),
            F.array_sort(F.collect_list(F.when(F.col("sentiment") == "negative", F.col("topic")))).alias("complaints"),
        )
        .orderBy("store_id")
    )
    store_summary.show(truncate=False)

    # Topic counts across the whole chain
    (reviews_labeled.groupBy("topic", "sentiment").count()
                    .orderBy(F.desc("count"), "topic", "sentiment")
                    .show())
    return (store_summary,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Sanity check: do Claude's labels agree with the stars?** The customer chose the star
    rating; Claude only read the text. A 5-star review labeled *negative* (or the
    reverse) deserves a human look. This is how you **validate** LLM output: compare it
    with data you already trust.
    """)
    return


@app.cell
def _(F, reviews_labeled):
    (reviews_labeled.groupBy("rating")
                    .pivot("sentiment", ["positive", "neutral", "negative"])
                    .count()
                    .fillna(0)
                    .orderBy(F.desc("rating"))
                    .show())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 9. Pattern C: Claude writes the Spark SQL

    A manager does not write SQL, but can ask a question in English. We give Claude the
    **schema** of our tables (only column names and types, no data) and the question, and
    ask for one SQL query, again as structured output. Then **Spark** runs the query.

    **Safety rules for generated code**, always:

    * Run it only on **read-only temporary views** (nothing can be deleted).
    * Accept only a single `SELECT` (or `WITH`) statement; reject anything else.
    * Show the SQL to the user, so a human can check it.
    """)
    return


@app.cell
def _(BaseModel, EFFORT, MODEL, reviews_labeled, stores):
    reviews_labeled.createOrReplaceTempView("reviews_labeled")
    stores.createOrReplaceTempView("stores")

    def describe_view(name, df):
        cols = ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in df.schema.fields)
        return f"{name}({cols})"

    SCHEMA_TEXT = "\n".join([
        describe_view("reviews_labeled", reviews_labeled),
        describe_view("stores", stores),
    ])

    class SqlAnswer(BaseModel):
        sql: str
        explanation: str

    SQL_SYSTEM = (
        "You write one Spark SQL SELECT query that answers the user's question. "
        "Use only these tables and columns:\n" + SCHEMA_TEXT + "\n"
        "sentiment is one of positive, neutral, negative. "
        "Return exactly one statement, without a trailing semicolon. "
        "In explanation, say in one sentence what the query does."
    )

    def question_to_sql(llm, question):
        response = llm.messages.parse(
            model=MODEL,
            max_tokens=16000,
            output_config={"effort": EFFORT},
            system=SQL_SYSTEM,
            messages=[{"role": "user", "content": question}],
            output_format=SqlAnswer,
        )
        if response.stop_reason == "refusal" or response.parsed_output is None:
            raise ValueError("Claude did not return a query")
        return response.parsed_output

    def is_safe_select(sql):
        s = sql.strip().rstrip(";").lower()
        return s.startswith(("select", "with")) and ";" not in s

    print(SCHEMA_TEXT)
    return (
        SCHEMA_TEXT,
        SQL_SYSTEM,
        SqlAnswer,
        describe_view,
        is_safe_select,
        question_to_sql,
    )


@app.cell
def _(client, is_safe_select, question_to_sql, spark):
    sql_question = "Which city has the most negative reviews, and how many?"
    sql_answer = question_to_sql(client, sql_question)
    print("Question:", sql_question)
    print("SQL:     ", sql_answer.sql)
    print("Why:     ", sql_answer.explanation)
    if is_safe_select(sql_answer.sql):
        spark.sql(sql_answer.sql).show(truncate=False)
    else:
        print("Rejected: not a single SELECT statement.")
    return sql_answer, sql_question


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Try it: ask your own question

    Type a question and click the button (the button keeps marimo from calling Claude on
    every keystroke). Examples: *"Average rating per store name"*, *"Which reviews are in
    Spanish? Show their English summaries"*, *"Count reviews per topic in San Jose"*.
    """)
    return


@app.cell
def _(mo):
    user_question = mo.ui.text(
        value="Average rating per store name, best first",
        label="Your question",
        full_width=True,
    )
    ask_button = mo.ui.run_button(label="Ask Claude")
    mo.vstack([user_question, ask_button])
    return ask_button, user_question


@app.cell
def _(ask_button, client, is_safe_select, mo, question_to_sql, spark, user_question):
    mo.stop(not ask_button.value, mo.md("*Click **Ask Claude** to run your question.*"))
    _answer = question_to_sql(client, user_question.value)
    print("SQL:", _answer.sql)
    print("Why:", _answer.explanation)
    if is_safe_select(_answer.sql):
        spark.sql(_answer.sql).show(truncate=False)
    else:
        print("Rejected: not a single SELECT statement.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 10. Pattern D: Spark aggregates, Claude explains

    Claude should not read a million rows: that would be slow and expensive, and it is
    what Spark is for. The good division of labor is:

    1. **Spark** reduces the data to a small summary table (Section 8)
    2. **Claude** turns that small table into a short report for a human

    We convert the summary to plain text with `collect()` (only 4 rows) and ask for three
    recommendations. Note the instruction to use **only** the numbers given: it keeps
    Claude from inventing facts.
    """)
    return


@app.cell
def _(EFFORT, MODEL, client, json, mo, store_summary):
    summary_rows = [r.asDict() for r in store_summary.collect()]
    report_prompt = (
        "Here is this week's review summary for each store of the Bean & Leaf coffee chain, "
        "as JSON (pct_negative is a percentage; complaints lists the topics of negative reviews):\n"
        + json.dumps(summary_rows, indent=2)
        + "\n\nWrite a short report for the regional manager in Markdown: one sentence per store, "
        "then three concrete recommendations. Use only the numbers above; do not invent any."
    )
    report_response = client.messages.create(
        model=MODEL,
        max_tokens=16000,
        output_config={"effort": EFFORT},
        messages=[{"role": "user", "content": report_prompt}],
    )
    manager_report = "".join(b.text for b in report_response.content if b.type == "text")
    mo.callout(mo.md(manager_report), kind="info")
    return manager_report, report_prompt, report_response, summary_rows


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 11. Cost, speed, privacy: good practices

    | Topic | What to do |
    |---|---|
    | **Secrets** | Key in `.env` (git-ignored), never in code, never printed. On a cluster, use its secret manager. |
    | **Cost** | You pay per token, in and out. Test on **one row**, then a sample (`df.limit(20)`), then everything. Send only the columns the model needs. |
    | **Do it once** | LLM answers vary between runs. Label once, **save** the result to Parquet, and analyze the saved table. Don't re-label in every job. |
    | **Structured output** | Ask for a schema (`output_format=...`), not free text, whenever Spark will use the answer. |
    | **Validate** | Compare labels with data you trust (stars vs. sentiment); have a human check a sample. |
    | **Generated code** | Read-only views, `SELECT` only, show the SQL. Never run generated code blindly. |
    | **Parallelism** | `mapPartitions()` with one client per partition. Number of partitions = concurrent requests; stay under the rate limit. |
    | **Aggregate first** | Let Spark reduce big data to a small table; send only that to the model. |
    | **Privacy** | Do not send personal data (names, emails, card numbers) unless your organization allows it; remove it first. |
    | **Refusals** | Check `stop_reason`; the code above stores `NULL` labels instead of crashing. |
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 12. Exercises

    1. Add a field `mentions_staff: bool` to `ReviewLabels` (does the review talk about the
       employees?). Re-label the reviews and count how many mention staff, per store.
    2. Use Pattern C to answer: *"For each store name, how many reviews were in Spanish?"*
       Check the SQL before trusting the answer.
    3. Change Pattern D so the report is written **in Spanish** for a manager in Mexico,
       but keep the numbers unchanged.

    Exercise 2 is solved below (it makes one API call). For 1 and 3, change the code in the
    cells above.
    """)
    return


@app.cell
def _(client, is_safe_select, mo, question_to_sql, spark):
    import contextlib
    import io

    _printed = io.StringIO()
    with contextlib.redirect_stdout(_printed):
        _ex2 = question_to_sql(client, "For each store name, how many reviews were in Spanish?")
        print("SQL:", _ex2.sql)
        if is_safe_select(_ex2.sql):
            spark.sql(_ex2.sql).show(truncate=False)
    mo.accordion({"Show solution to exercise 2": mo.plain_text(_printed.getvalue())})
    return contextlib, io


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Stop Spark

    Uncomment the line below when you are done, to release the local Spark resources.
    """)
    return


@app.cell
def _():
    # spark.stop()
    return


if __name__ == "__main__":
    app.run()
