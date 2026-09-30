# PySpark 4.2.0: Distinct Visitors per Day — RDD Solution

A complete RDD solution to counting each user once per day, regardless of
how many pages they visit or which partition holds the event. The 13
sample events produce **3 visitors on September 1 and 4 on September 2**.

A companion
[DataFrame solution](../../dataframes_examples/distinct_visitor_per_day/)
solves the same problem in its own sibling folder.

| File | Purpose |
|---|---|
| [RDD solution guide](Distinct_Visitors_RDD.md) | Key/value transformations, local combining, shuffles, and worked results |
| [distinct_visitors_rdd.py](distinct_visitors_rdd.py) | Runnable RDD application |
| [test_solutions.py](test_solutions.py) | Checks the RDD implementation on Spark 4.2.0 |
| [data/partition_1.csv](data/partition_1.csv) | Six input events |
| [data/partition_2.csv](data/partition_2.csv) | Seven input events, including the new U5 visit |
| [requirements.txt](requirements.txt) | PySpark version pin |

## 1. Setup

Use **classic PySpark 4.2.0**, Python **3.10+**, and Java **21** (a supported
choice for this lesson). Java must be on PATH or referenced by JAVA_HOME.
RDDs require classic Spark; do not use a Spark Connect-only environment.
See the official [Spark 4.2.0 overview](https://spark.apache.org/docs/4.2.0/)
and [PySpark installation guide](https://spark.apache.org/docs/4.2.0/api/python/getting_started/install.html).

Open a terminal in this folder. Use the course's existing **Spark 4.2.0**
installation, with `SPARK_HOME` pointing to its root directory. These
examples use the PySpark bundled with Spark; no separate pip installation
is needed when that distribution is already installed.

**macOS/Linux:**

```bash
python3 --version
java -version
"$SPARK_HOME/bin/spark-submit" --version
```

**Windows PowerShell:**

```powershell
python --version
java -version
& "$env:SPARK_HOME\bin\spark-submit.cmd" --version
```

If `SPARK_HOME` is unset or the launcher is missing, use the installation
path provided by your instructor. The launcher should report `4.2.0`.
The program prints both the running Spark version and the selected master.
`requirements.txt` records the PySpark version for reference; students using
the course Spark distribution do not need to install it separately.

## 2. Run the Solution

```text
"$SPARK_HOME/bin/spark-submit" --master "local[2]" distinct_visitors_rdd.py
"$SPARK_HOME/bin/spark-submit" --master "local[2]" test_solutions.py
```

The commands select `local[2]`; the script does not override the master.
The script resolves the bundled data relative
to the script file, so moving this whole folder does not break data paths.
Spark logs may appear before the results. No separate cluster is needed.

**Windows PowerShell equivalents:**

```powershell
& "$env:SPARK_HOME\bin\spark-submit.cmd" --master "local[2]" distinct_visitors_rdd.py
& "$env:SPARK_HOME\bin\spark-submit.cmd" --master "local[2]" test_solutions.py
```

To use another directory of CSV files with the same schema:

```text
"$SPARK_HOME/bin/spark-submit" --master "local[2]" distinct_visitors_rdd.py --input "path/to/data"
```

Place Spark options such as `--master` **before** the `.py` filename.
Place application options such as `--input` **after** it.
This is a local classroom example;
cluster use requires paths accessible to executors and a consistently
configured Python environment. The script defaults its worker Python
to the driver's interpreter if `PYSPARK_PYTHON` is not already set.

## 3. Data Contract

Each CSV file has the header `event_id,date,user_id,page`. Fields may be
CSV-quoted, but records must occupy one physical line for the RDD reader.
Dates are pre-normalized ISO reporting-date strings, not raw timestamps.
The solution trims date/user fields and excludes missing or blank values.
It does not validate calendar dates or infer time zones. Other malformed
records are outside this lesson's input contract; the reader is not a
complete data-quality pipeline.

Repeated events do not change the answer. A user returning on another
date counts once on each date. Dates without valid visits do not appear.
The count measures user IDs, not necessarily distinct human beings.

## 4. Expected Results

```text
date,distinct_visitors
2026-09-01,3
2026-09-02,4
```

The unique pairs are:

| Date | Users | Visit events | Distinct visitors |
|---|---|---:|---:|
| 2026-09-01 | U1, U2, U3 | 6 | 3 |
| 2026-09-02 | U1, U3, U4, U5 | 7 | 4 |

The daily sum is **7 user-days**, while the whole dataset contains **5
unique users**. Those are different metrics.

## 5. Verification and Troubleshooting

Verified locally on macOS with **Spark 4.2.0, Python 3.13.2, and Java
21.0.7**: the application and its checks passed when launched
with `"$SPARK_HOME/bin/spark-submit" --master "local[2]"`.
This host required the loopback networking workaround below. Windows
setup instructions have not been executed on a Windows machine.

The check script requires engine version 4.2.0 and exercises actual CSV
loading, the RDD implementation, repeated events, empty inputs, blank
keys, an added visitor, and different partition counts. It prints
`All checks passed ...` only after completion.

- If Java cannot start, check `java -version` and JAVA_HOME.
- If local networking is denied, Spark's Python/JVM connection cannot
  start; run in an environment that permits local socket communication.
- If Spark reports `Cannot assign requested address` while starting a
  local driver, the machine hostname may resolve to an unavailable address.
  For these local examples only, set `SPARK_LOCAL_IP=127.0.0.1`:
  `export SPARK_LOCAL_IP=127.0.0.1` in macOS/Linux shells, or
  `$env:SPARK_LOCAL_IP="127.0.0.1"` in PowerShell, then rerun.
  Do not use this loopback setting for a multi-machine cluster.
- If the version differs, check both your Python environment and SPARK_HOME.
- Input files are illustrative source splits. Spark decides actual input
  task boundaries; two CSV files do not guarantee exactly two Spark tasks.

This folder is self-contained: keep the scripts and `data/` together when
moving it. The guide links to local scripts with no required links back
to the original repository location, plus one link out to the companion
DataFrame guide in `../../dataframes_examples/distinct_visitor_per_day/`.
If you move this folder independently of that sibling, that one link
will break — update or remove it.
