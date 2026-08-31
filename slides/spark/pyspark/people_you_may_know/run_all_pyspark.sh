#!/usr/bin/env bash
#
# run_all_pyspark.sh
#
# Runs all four PySpark "People You May Know" implementations in this
# folder -- pymk_pyspark_using_groupbykey.py, pymk_pyspark_using_reducebykey.py,
# pymk_pyspark_using_dataframes.py, and pymk_pyspark_using_graphframes.py --
# against both sample graphs, so you can see all four agree on the
# same output without having to type each command by hand.
#
# (pymk_pure_python.py isn't included here since it's a plain Python
# script, not a Spark job -- run it directly with
# `python3 pymk_pure_python.py data/friends.txt`, see README.md
# Section 5.)
#
# Usage:
#   ./run_all_pyspark.sh
#
# Each script is run locally with plain `python3` (SparkSession
# .getOrCreate() starts an in-process local Spark context when one
# isn't already running -- see README.md Section 6 for the
# spark-submit equivalent of each command below, and for
# pymk_pyspark_using_graphframes.py's extra `graphframes` dependency).

set -euo pipefail
cd "$(dirname "$0")"

SCRIPTS=(
    pymk_pyspark_using_groupbykey.py
    pymk_pyspark_using_reducebykey.py
    pymk_pyspark_using_dataframes.py
    pymk_pyspark_using_graphframes.py
)

for script in "${SCRIPTS[@]}"; do
    echo "=================================================================="
    echo "== ${script}  --  data/friends.txt"
    echo "=================================================================="
    python3 "${script}" data/friends.txt
    echo

    echo "=================================================================="
    echo "== ${script}  --  data/friends_larger.txt --top-k 2"
    echo "=================================================================="
    python3 "${script}" data/friends_larger.txt --top-k 2
    echo
done

echo "All four PySpark scripts ran; their recommendation lines above"
echo "should be byte-for-byte identical for each input file -- that's"
echo "the point (see README.md Section 7)."
