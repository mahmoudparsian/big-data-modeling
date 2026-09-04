# FIFA World Cup Analysis by DataFrames

A [marimo](https://marimo.io) (reactive notebook) walkthrough of the
FIFA World Cup match-events example from
[*PySpark DataFrame Tutorial: Introduction to DataFrames*](https://dzone.com/articles/pyspark-dataframe-tutorial-introduction-to-datafra)
(DZone), run against the real dataset the tutorial's `fifa_players.csv`
was drawn from.

## Contents

| Name | Description |
|---|---|
| [`fifa_marimo_analysis.py`](./fifa_marimo_analysis.py) | Marimo notebook, in two parts — **Part 1** follows the tutorial (schema, `describe()`, `select()`/`distinct()`, `filter()`, `orderBy()`, plus a team/position dropdown pair); **Part 2** goes beyond it (`split()`/`explode()`/`regexp_extract()` to parse multi-code `event` strings, top goal scorers, cards-by-team with `pivot()`, matches-played with `countDistinct()`, and a range-slider event explorer) |
| `WorldCupPlayers.csv` | The dataset: one row per player per World Cup match, 1930–2014 (37,784 rows, 9 columns). From the [FIFA World Cup dataset](https://www.kaggle.com/datasets/abecklas/fifa-world-cup) on Kaggle |

## The Dataset

`WorldCupPlayers.csv` columns (renamed to `snake_case`; the original
Kaggle file uses `RoundID`, `Team Initials`, etc.):

| Column | Description |
|---|---|
| `round_id` | Unique ID of the tournament round/stage |
| `match_id` | Unique ID of the match |
| `team_initials` | 3-letter team code (e.g. `BRA`, `ARG`) |
| `coach_name` | Name of the team's coach for that match |
| `line_up` | `S`=starter, `N`=substitute |
| `shirt_number` | Player's shirt/jersey number |
| `player_name` | Player's name |
| `position` | blank/`NULL` for a regular outfield player, `GK`=goalkeeper, `C`=captain, `GKC`=goalkeeper *and* captain |
| `event` | In-game events for that player, e.g. `"G40'"` = Goal in the 40th minute, `"Y65'"` = Yellow card, `"R90'"` = Red card, `"O46'"`/`"I46'"` = substituted Out/In |

This is the same file the DZone tutorial reads as `fifa_players.csv`
— same 37,784-row count, and the same `match_id == 1096` /
`event == "G40'"` values it uses in its `filter()` examples.

### `event` Code Reference

A single `event` value can hold several space-separated codes (e.g.
`"G36' G76' Y81'"` — two goals and a yellow card for the same player
in the same match). Each code pairs a type with the match minute
(e.g. `G40'` = goal, minute 40):

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

**Note on `OG`:** it's part of this dataset family's documented code
set, but no row in `WorldCupPlayers.csv` actually uses it — an own
goal here is recorded as an ordinary `G` credited to the scoring
team, so `event` alone can't distinguish an own goal from a regular
one without cross-referencing which team benefited.

## What the Notebook Does

### Part 1 — Follows the Tutorial

1. Starts a local `SparkSession`.
2. Reads `WorldCupPlayers.csv` with
   `spark.read.csv(path, inferSchema=True, header=True)`.
3. Inspects the schema and shape: `printSchema()`, `columns`,
   `count()`, column count.
4. Runs `describe()` on `coach_name` and `position`.
5. Uses `select()` and `distinct()` to project and de-duplicate
   columns.
6. Filters rows by `match_id == 1096` (France vs. Mexico, the first
   World Cup match, 1930), and combines two conditions with `&`
   (captains who scored in the 40th minute).
7. Sorts with `orderBy()`.
8. Filters interactively with a `team_initials` dropdown and a
   `position` dropdown — moving either one reactively re-runs the
   filter, no manual re-run needed.

### Part 2 — Beyond the Tutorial

Each row's `event` column can hold several space-separated codes
(e.g. `"G36' G76' Y81'"` — two goals and a yellow card for the same
player in the same match), which Part 1 never unpacks. Part 2 does:

9. Parses `event` into one row per code with `split()` + `explode()`,
   then pulls out the event type and minute with `regexp_extract()`.
10. Finds the **top goal scorers** (`G`/`P` events) with
    `groupBy().agg(count())` + `orderBy(desc)`.
11. Builds a **yellow/red cards by team** table with `pivot()` —
    spreading `card_type` values into their own columns, instead of
    aggregating into one row per group.
12. Computes **matches played per team** with `countDistinct("match_id")`
    (counting rows would over-count, since each match has ~22 player
    rows).
13. Explores events interactively with an event-type dropdown and a
    **range slider** (`mo.ui.range_slider`) over match minute.

## Running It

```bash
pip install marimo pyspark
marimo edit fifa_marimo_analysis.py
```

`marimo edit` opens the notebook in your browser with live editing.
To just run it once and view the output (no editing UI):

```bash
marimo run fifa_marimo_analysis.py
```

or run it as a plain script:

```bash
python3 fifa_marimo_analysis.py
```

## See Also

* [`../../marimo/`](../../marimo) — the general-purpose
  PySpark + Marimo basic/intermediate pair this notebook follows the
  style of
* [PySpark DataFrame Tutorial: Introduction to DataFrames](https://dzone.com/articles/pyspark-dataframe-tutorial-introduction-to-datafra) — the source tutorial this notebook is based on
