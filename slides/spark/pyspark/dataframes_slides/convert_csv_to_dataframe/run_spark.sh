#!/bin/bash
#------------------------------------------------------------
# Shell script that runs dataframe_creation_cvs_with_header.py
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
#------------------------------------------------------------
if [ -z "$SPARK_HOME" ]; then
    echo "Error: SPARK_HOME is not set."
    echo "Set it to your Spark installation directory, e.g.:"
    echo "  export SPARK_HOME=/opt/spark"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_PROG="$SCRIPT_DIR/dataframe_creation_cvs_with_header.py"
INPUT_FILE="$SCRIPT_DIR/emps_with_header.txt"

#
# run the PySpark program by spark-submit:
#
#                              sys.argv[0]  sys.argv[1]
"$SPARK_HOME/bin/spark-submit" "$SPARK_PROG" "$INPUT_FILE"
