import marimo

__generated_with = "0.23.10"
app = marimo.App(width="medium", app_title="PySpark + Marimo — FIFA World Cup Analysis")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # ⚽ PySpark + Marimo: FIFA World Cup DataFrame Analysis

    This notebook re-creates, as a reactive
    [marimo](https://marimo.io) notebook, the FIFA World Cup
    match-events example from
    [*PySpark DataFrame Tutorial: Introduction to DataFrames*](https://dzone.com/articles/pyspark-dataframe-tutorial-introduction-to-datafra)
    (DZone).

    The tutorial loads a `fifa_players.csv` file of match events
    (37,784 rows) with `spark.read.csv(...)`. That file is the
    **`WorldCupPlayers.csv`** file from the
    [FIFA World Cup dataset](https://www.kaggle.com/datasets/abecklas/fifa-world-cup)
    on Kaggle (one row per player per World Cup match, 1930–2014) —
    it's included alongside this notebook (`./WorldCupPlayers.csv`)
    and has exactly the row count and `match_id`/`event` values
    (e.g. `match_id == 1096`, `event == "G40'"`) the tutorial uses.
    Its header row has been rewritten to `snake_case` (the original
    Kaggle file uses `RoundID`, `Team Initials`, etc.).

    See the column reference table below.

    This notebook walks through the tutorial's operations —
    schema inspection, `describe()`, `select()`/`distinct()`,
    `filter()`, `orderBy()` — against the real data, then finishes
    with two interactive widgets (a team-picker and a
    position-picker) that reactively re-filter the DataFrame.

    **Setup:**
    ```bash
    pip install marimo pyspark
    marimo edit fifa_marimo_analysis.py
    ```
    """)
    return


@app.cell
def _(mo):
    column_reference = mo.ui.table(
        [
            {"column": "round_id", "description": "Unique ID of the tournament round/stage"},
            {"column": "match_id", "description": "Unique ID of the match"},
            {"column": "team_initials", "description": "3-letter team code (e.g. BRA, ARG)"},
            {"column": "coach_name", "description": "Name of the team's coach for that match"},
            {"column": "line_up", "description": "S=starter, N=substitute"},
            {"column": "shirt_number", "description": "Player's shirt/jersey number"},
            {"column": "player_name", "description": "Player's name"},
            {
                "column": "position",
                "description": "blank/NULL for a regular outfield player, GK=goalkeeper, C=captain, GKC=goalkeeper and captain",
            },
            {
                "column": "event",
                "description": (
                    "In-game events for that player, e.g. \"G40'\" = Goal in the "
                    "40th minute, \"Y65'\" = Yellow card, \"R90'\" = Red card, "
                    "\"O46'\"/\"I46'\" = substituted Out/In"
                ),
            },
        ],
        label="WorldCupPlayers.csv column reference",
        selection=None,
        pagination=False,
    )
    column_reference
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 1 — Start a SparkSession
    """)
    return


@app.cell
def _():
    import marimo as mo
    from pathlib import Path
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, count, countDistinct, explode, regexp_extract, split, when

    spark = (
        SparkSession.builder
        .appName("pyspark-marimo-fifa")
        .master("local[*]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return (
        Path,
        col,
        count,
        countDistinct,
        explode,
        mo,
        regexp_extract,
        spark,
        split,
        when,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 2 — Read `WorldCupPlayers.csv` into a DataFrame

    Same call as the tutorial — `spark.read.csv(path, inferSchema=True,
    header=True)` — pointed at the CSV sitting next to this notebook.
    """)
    return


@app.cell
def _(Path, spark):
    csv_path = Path(__file__).resolve().parent / "WorldCupPlayers.csv"
    fifa_df = spark.read.csv(str(csv_path), inferSchema=True, header=True)
    fifa_df.show()
    return (fifa_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 3 — Inspect the Schema and Shape

    Same starting moves as the tutorial: `printSchema()`, `columns`,
    `count()`, and the number of columns.
    """)
    return


@app.cell
def _(fifa_df):
    fifa_df.printSchema()
    print("columns:", fifa_df.columns)
    print("row count:", fifa_df.count())
    print("column count:", len(fifa_df.columns))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 4 — Statistical Summary with `describe()`

    `describe()` on a string column reports count (nulls excluded),
    and (since these are non-numeric) `null` for mean/stddev, plus
    lexicographic min/max.
    """)
    return


@app.cell
def _(fifa_df):
    fifa_df.describe("coach_name").show()
    fifa_df.describe("position").show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 5 — `select()` and `distinct()`

    Project just the columns you need, then de-duplicate.
    """)
    return


@app.cell
def _(fifa_df):
    fifa_df.select("player_name", "coach_name").show()
    fifa_df.select("player_name", "coach_name").distinct().show()
    print(
        "distinct (player_name, coach_name) pairs:",
        fifa_df.select("player_name", "coach_name").distinct().count(),
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 6 — `filter()`

    Filter to one match — `match_id == 1096`, France vs. Mexico,
    the very first World Cup match (1930) — count the rows, then
    combine two conditions with `&`: captains (`position == "C"`)
    who scored in the 40th minute (`event == "G40'"`). That last
    filter is rare enough (2 rows, out of 37,784) to show why
    `filter()` combined with `&` is worth having.
    """)
    return


@app.cell
def _(fifa_df):
    fifa_df.filter(fifa_df.match_id == 1096).show()
    print("rows in match 1096:", fifa_df.filter(fifa_df.match_id == 1096).count())

    fifa_df.filter(
        (fifa_df.position == "C") & (fifa_df.event == "G40'")
    ).show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 7 — `orderBy()`
    """)
    return


@app.cell
def _(fifa_df):
    fifa_df.orderBy(fifa_df.match_id).show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 8 — Filter Interactively with Two Widgets

    Pick a team *and* a position. Because marimo is **reactive**,
    moving either dropdown automatically re-runs the filter cell
    below it — no "restart and run all" needed.
    """)
    return


@app.cell
def _(fifa_df, mo):
    team_choice = mo.ui.dropdown(
        options=["All"] + sorted(
            r["team_initials"]
            for r in fifa_df.select("team_initials").distinct().collect()
        ),
        value="All",
        label="Team Initials",
    )
    position_choice = mo.ui.dropdown(
        options=["All", "GK", "C", "GKC"],
        value="All",
        label="Position (GK=goalkeeper, C=captain, GKC=both)",
    )
    mo.hstack([team_choice, position_choice])
    return position_choice, team_choice


@app.cell
def _(col, fifa_df, position_choice, team_choice):
    fifa_filtered = fifa_df
    if team_choice.value != "All":
        fifa_filtered = fifa_filtered.filter(col("team_initials") == team_choice.value)
    if position_choice.value != "All":
        fifa_filtered = fifa_filtered.filter(col("position") == position_choice.value)
    fifa_filtered.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 🔍 Part 2 — Additional Analysis (Beyond the Tutorial)

    The DZone tutorial stops at `select()`/`distinct()`, `filter()`,
    and `orderBy()`. The `event` column has more to give: each row
    can carry **several** space-separated event codes (e.g.
    `"G36' G76' Y81' O86'"` — two goals, a yellow card, and a
    substitution, all for the same player in the same match), where
    the letters mean:

    | Code | Meaning |
    |---|---|
    | `G` | Goal |
    | `P` | Penalty goal |
    | `MP` | Missed penalty |
    | `OG` | Own goal (see note below) |
    | `Y` | Yellow card |
    | `R` | Red card |
    | `RSY` | Red card via second yellow |
    | `O` / `I` | Substituted out / in |
    | `OH` / `IH` | Substituted out / in at half-time |
    | `W` | Rare/undocumented code in the source data |

    **Note on `OG`:** it's part of this dataset family's documented
    code set, but no row in `WorldCupPlayers.csv` actually uses it —
    an own goal here is recorded as an ordinary `G` credited to the
    scoring team, so `event` alone can't distinguish an own goal from
    a regular one without cross-referencing which team benefited.

    The steps below use `split()` + `explode()` to turn each
    multi-code `event` string into one row per code, `regexp_extract()`
    to pull out the code and the minute, then aggregate — techniques
    the original tutorial doesn't touch.

    **Data-quality note:** `player_name` has a pre-existing encoding
    issue in the source file — accented characters in ~244 names
    (e.g. Pelé, Müller, Sánchez) were already replaced with the
    Unicode replacement character (`�`) *before* this CSV was
    published, by some earlier lossy conversion upstream. It's not
    introduced by anything in this notebook (the file itself is
    valid UTF-8), and it doesn't affect counts/aggregates below —
    just how a handful of names are displayed.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 9 — Parse `event` into One Row per Event Code

    `split(event, " ")` turns `"G36' G76' Y81'"` into an array;
    `explode()` turns that array into one row per element.
    `regexp_extract()` then pulls the leading letters (event type)
    and the digits (minute) out of each code.
    """)
    return


@app.cell
def _(col, explode, fifa_df, regexp_extract, split):
    events_exploded = (
        fifa_df
        .select("match_id", "team_initials", "player_name", "event")
        .filter(col("event").isNotNull())
        .withColumn("event_code", explode(split(col("event"), " ")))
        .withColumn("event_type", regexp_extract(col("event_code"), r"^([A-Za-z]+)", 1))
        .withColumn("minute", regexp_extract(col("event_code"), r"(\d+)", 1).cast("int"))
    )
    events_exploded.show(10)
    print("total individual events:", events_exploded.count())
    return (events_exploded,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 10 — Top Goal Scorers

    Filter to `G` (goal) and `P` (penalty goal) events, then
    `groupBy("player_name")` and count.
    """)
    return


@app.cell
def _(col, count, events_exploded):
    top_scorers = (
        events_exploded
        .filter(col("event_type").isin("G", "P"))
        .groupBy("player_name")
        .agg(count("*").alias("goals"))
        .orderBy(col("goals").desc())
    )
    top_scorers.show(15)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 11 — Yellow/Red Cards by Team, with `pivot()`

    `pivot()` turns distinct values of one column (`card_type`) into
    their own output columns — a different move than the `groupBy().agg()`
    from the marimo/ `intermediate` example, which aggregates into a
    single row per group instead of spreading values across columns.
    """)
    return


@app.cell
def _(col, events_exploded, when):
    cards_by_team = (
        events_exploded
        .filter(col("event_type").isin("Y", "R", "RSY"))
        .withColumn(
            "card_type",
            when(col("event_type") == "Y", "yellow").otherwise("red"),
        )
        .groupBy("team_initials")
        .pivot("card_type", ["yellow", "red"])
        .count()
        .na.fill(0)
        .orderBy(col("yellow").desc())
    )
    cards_by_team.show(15)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 12 — Matches Played per Team, with `countDistinct()`

    Each row is one *player* in one match, so counting rows
    over-counts matches — `countDistinct("match_id")` per team gives
    the real number of matches played.
    """)
    return


@app.cell
def _(col, countDistinct, fifa_df):
    appearances = (
        fifa_df.groupBy("team_initials")
        .agg(countDistinct("match_id").alias("matches_played"))
        .orderBy(col("matches_played").desc())
    )
    appearances.show(15)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 13 — Explore Events Interactively

    Pick an event type and drag a **range slider** (`mo.ui.range_slider`,
    a new widget type vs. the plain sliders used above) to narrow down
    by match minute — useful for questions like "how many goals were
    scored in stoppage time (minute > 90)?"
    """)
    return


@app.cell
def _(events_exploded, mo):
    event_type_choice = mo.ui.dropdown(
        options=["All"] + sorted(
            r["event_type"]
            for r in events_exploded.select("event_type").distinct().collect()
        ),
        value="G",
        label="Event type",
    )
    minute_range = mo.ui.range_slider(
        start=0, stop=120, value=[0, 120], step=1, label="Minute range"
    )
    mo.hstack([event_type_choice, minute_range])
    return event_type_choice, minute_range


@app.cell
def _(col, event_type_choice, events_exploded, minute_range):
    events_filtered = events_exploded.filter(
        (col("minute") >= minute_range.value[0])
        & (col("minute") <= minute_range.value[1])
    )
    if event_type_choice.value != "All":
        events_filtered = events_filtered.filter(
            col("event_type") == event_type_choice.value
        )
    print("matching events:", events_filtered.count())
    events_filtered.orderBy("minute").show(20)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Step 14 — Stop Spark

    Uncomment the line below when you're done experimenting, to
    release the local Spark cluster's resources.
    """)
    return


@app.cell
def _():
    # spark.stop()
    return


if __name__ == "__main__":
    app.run()
