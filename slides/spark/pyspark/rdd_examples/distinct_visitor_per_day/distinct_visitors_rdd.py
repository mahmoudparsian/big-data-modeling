"""Exact daily distinct visitors using RDDs; Spark 4.2.0, classic mode."""
import argparse
import csv
import os
from operator import add
from pathlib import Path
import sys

from pyspark.sql import SparkSession


def parse_partition(lines):
    # One physical line per CSV record; headers repeat in each input file.
    for row in csv.reader(lines):
        if row == ["event_id", "date", "user_id", "page"]:
            continue
        if len(row) != 4:
            raise ValueError(f"Expected four CSV fields, got {len(row)}")
        event_id, date, user_id, page = row
        date, user_id = date.strip(), user_id.strip()
        if date and user_id:
            yield date, user_id


def distinct_visitors(pairs):
    """Input: RDD[(date, user_id)]. Output: RDD[(date, count)]."""
    unique_pairs = pairs.map(lambda pair: (pair, 1)).reduceByKey(max)
    return unique_pairs.map(lambda item: (item[0][0], 1)).reduceByKey(add)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=str(Path(__file__).resolve().parent / "data"))
    args = parser.parse_args()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = SparkSession.builder.appName("DistinctVisitorsRDD").getOrCreate()
    try:
        spark.sparkContext.setLogLevel("ERROR")
        print(f"Spark version: {spark.version}")
        print(f"Spark master: {spark.sparkContext.master}")
        lines = spark.sparkContext.textFile(args.input, minPartitions=2)
        pairs = lines.mapPartitions(parse_partition)
        result = distinct_visitors(pairs)
        # Only the tiny daily summary is collected for this teaching example.
        print("date,distinct_visitors")
        for date, count in sorted(result.collect()):
            print(f"{date},{count}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
