# `$SPARK_HOME/bin/spark-submit` Example

This folder shows how to run a PySpark program correctly with
`$SPARK_HOME/bin/spark-submit` — the standard way to launch a Spark
application from the command line (as opposed to running code
interactively in a shell or notebook).

## Environment Variables

* `SPARK_HOME` is an environment variable that points to your Spark
  installation directory. An environment variable is a dynamic-named
  value that affects how running processes behave on a computer.
  For example:

````
export SPARK_HOME=/opt/spark
````

  (Set this to wherever Spark is actually installed on your machine —
  e.g. `/opt/spark`, `/usr/local/spark`, or a path under your home
  directory.)

## Files in This Folder

| Name | Description |
|---|---|
| [`word_count_driver.py`](./word_count_driver.py) | The PySpark word-count program submitted via `spark-submit` |
| [`word_count_driver.sh`](./word_count_driver.sh) | Shell script that runs `word_count_driver.py` via `$SPARK_HOME/bin/spark-submit` |
| [`word_count_driver.log`](./word_count_driver.log) | Sample output log from running the shell script |
| [`sample_file.txt`](./sample_file.txt) | Small sample text file used as input |
| [`running_a_pyspark_program_by_spark-submit.md`](./running_a_pyspark_program_by_spark-submit.md) | A second, illustrative walkthrough of running a PySpark program with `spark-submit` |

## How `spark-submit` Picks a Python Interpreter

`spark-submit` does **not** execute your `.py` file directly as a
script (so its shebang line is just documentation, not something
Spark reads) — it launches the program using the Python interpreter
found on `PATH`, or the one named by the `PYSPARK_PYTHON` environment
variable if you set it. If you need a specific interpreter, set:

````
export PYSPARK_PYTHON=/usr/bin/python3
````

## How to Run the Shell Script

1. Set `SPARK_HOME` as shown above.
2. Make the script executable (once): `chmod +x word_count_driver.sh`
3. Run it from this folder, or from anywhere by its full path:

````
./word_count_driver.sh
````

The script locates its own directory, so it will always find
`word_count_driver.py` and `sample_file.txt` next to it — no editing
required.

## Running It Manually

You can also invoke `spark-submit` yourself instead of using the
shell script:

````
$SPARK_HOME/bin/spark-submit word_count_driver.py sample_file.txt
````

Expected output is shown in [`word_count_driver.log`](./word_count_driver.log).

## Common `spark-submit` Options

The command above runs with all defaults, which is enough for a
local, single-machine example. In practice you'll usually pass extra
flags to control where and how the job runs — these go *before* the
script name:

````
$SPARK_HOME/bin/spark-submit \
    --master local[*] \
    --deploy-mode client \
    --name word-count-example \
    --executor-memory 2g \
    --num-executors 4 \
    word_count_driver.py \
    sample_file.txt
````

| Flag | Purpose |
|---|---|
| `--master` | Where to run: `local[*]` (all local cores), `spark://host:port` (standalone cluster), `yarn`, or `k8s://...` |
| `--deploy-mode` | `client` (driver runs on the machine you submit from) or `cluster` (driver runs on the cluster) |
| `--name` | A human-readable application name, shown in the Spark UI/history server |
| `--executor-memory` / `--driver-memory` | Memory to allocate per executor / for the driver |
| `--num-executors` | Number of executors to request (cluster managers that support it) |
| `--py-files` | Extra `.py`/`.zip`/`.egg` files to ship alongside your main script |

Anything after the script name (`word_count_driver.py` here) is
passed straight through to your program as `sys.argv` — that's how
`sample_file.txt` reaches `sys.argv[1]` in this example. See the
[Spark documentation on submitting applications](https://spark.apache.org/docs/latest/submitting-applications.html)
for the full flag reference.
