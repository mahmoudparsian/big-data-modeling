"""Exact daily distinct visitors using DataFrames; Spark 4.2.0."""
import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T

SCHEMA = T.StructType([
    T.StructField(name, T.StringType(), True)
    for name in ("event_id", "date", "user_id", "page")
])


def clean_pairs(events):
    return (
        events.select(F.trim("date").alias("date"), F.trim("user_id").alias("user_id"))
        .where(F.col("date").isNotNull() & F.col("user_id").isNotNull()
               & (F.col("date") != "") & (F.col("user_id") != ""))
    )


def distinct_visitors(events):
    unique_pairs = clean_pairs(events).dropDuplicates(["date", "user_id"])
    return unique_pairs.groupBy("date").agg(F.count("*").alias("distinct_visitors"))


def distinct_visitors_direct(events):
    # Equivalent exact aggregation, provided the same cleaning is applied.
    return clean_pairs(events).groupBy("date").agg(
        F.count_distinct("user_id").alias("distinct_visitors")
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=str(Path(__file__).resolve().parent / "data"))
    parser.add_argument("--explain", action="store_true")
    args = parser.parse_args()
    spark = (
        SparkSession.builder.appName("DistinctVisitorsDataFrame")
        .config("spark.sql.shuffle.partitions", "2").getOrCreate()
    )
    try:
        spark.sparkContext.setLogLevel("ERROR")
        print(f"Spark version: {spark.version}")
        print(f"Spark master: {spark.sparkContext.master}")
        events = spark.read.schema(SCHEMA).option("header", True).option("mode", "FAILFAST").csv(args.input)
        result = distinct_visitors(events).orderBy("date")
        if args.explain:
            result.explain("formatted")
        result.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
