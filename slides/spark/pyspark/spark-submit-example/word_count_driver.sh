#!/bin/bash
#------------------------------------------------------------
# Shell script that shows how to run a PySpark program
# using "$SPARK_HOME/bin/spark-submit".
#------------------------------------------------------------
# Before running this script, set SPARK_HOME to your local
# Spark installation directory, for example:
#
#   export SPARK_HOME=/opt/spark
#
# This script figures out its own directory so it can be
# run from anywhere and will still find the .py program and
# the sample input file that live next to it.
#
# Usage:
#   ./word_count_driver.sh [pyspark-script] [input-file] [M] [N]
#     pyspark-script: PySpark program to run (default: word_count_driver.py)
#     input-file:     text file to word-count (default: sample_file.txt)
#     M: ignore words shorter than M characters (default: 3)
#     N: ignore words with a frequency less than N (default: 2)
#
#   e.g. ./word_count_driver.sh word_count_driver.py sample_file.txt 2 3
#------------------------------------------------------------
if [ -z "$SPARK_HOME" ]; then
    echo "Error: SPARK_HOME is not set."
    echo "Set it to your Spark installation directory, e.g.:"
    echo "  export SPARK_HOME=/opt/spark"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_PROG="${1:-$SCRIPT_DIR/word_count_driver.py}"
INPUT_FILE="${2:-$SCRIPT_DIR/sample_file.txt}"
M="${3:-3}"
N="${4:-2}"

# On some machines Spark can't bind its driver to a random port
# (java.net.BindException: ... 'sparkDriver' failed after 16 retries).
# Pinning the driver to localhost avoids this.
export SPARK_LOCAL_IP=127.0.0.1

# run the PySpark program:
# M: ignore words shorter than M characters
# N: ignore words with a frequency less than N
"$SPARK_HOME/bin/spark-submit" \
    --conf spark.driver.bindAddress=127.0.0.1 \
    --conf spark.driver.host=127.0.0.1 \
    "$SPARK_PROG" "$INPUT_FILE" "$M" "$N"
