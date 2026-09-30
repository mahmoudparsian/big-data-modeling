"""Run with spark-submit --master "local[2]" test_solutions.py on Spark 4.2.0."""
import csv
from pathlib import Path

from pyspark.sql import SparkSession
from distinct_visitors_dataframe import SCHEMA, distinct_visitors as df_count, distinct_visitors_direct


def main():
    spark = (SparkSession.builder.appName("DistinctVisitorsDataFrameChecks")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    try:
        spark.sparkContext.setLogLevel("ERROR")
        assert spark.version == "4.2.0", f"Expected Spark 4.2.0, got {spark.version}"
        folder = Path(__file__).resolve().parent
        data = folder / "data"
        rows = []
        for path in sorted(data.glob("*.csv")):
            with path.open(newline="", encoding="utf-8") as stream:
                reader = csv.reader(stream)
                next(reader)
                rows.extend(tuple(row) for row in reader)
        assert len(rows) == 13
        expected = {"2026-09-01": 3, "2026-09-02": 4}
        # Exercise the actual file-reading path, including repeated headers.
        loaded = spark.read.schema(SCHEMA).option("header", True).option("mode", "FAILFAST").csv(str(data))
        assert {r.date: r.distinct_visitors for r in df_count(loaded).collect()} == expected

        cases = [
            (rows, expected),
            (rows + rows, expected),
            ([], {}),
            ([rows[0], rows[0]], {"2026-09-01": 1}),
            (rows + [("E14", "2026-09-01", "U4", "/home")], {"2026-09-01": 4, "2026-09-02": 4}),
            (rows + [("E15", "2026-09-02", " ", "/home"),
                     ("E16", " ", "U9", "/home"),
                     ("E17", "2026-09-02", " U5 ", "/home")], expected),
        ]
        for records, answer in cases:
            for partitions in (1, 3):
                events = spark.createDataFrame(records, SCHEMA).repartition(partitions)
                for function in (df_count, distinct_visitors_direct):
                    actual = {r.date: r.distinct_visitors for r in function(events).collect()}
                    assert actual == answer, (actual, answer)
        print(f"All checks passed on Spark {spark.version}: file input, both DataFrame forms, duplicates, empty input, cleaning, and repartitioning.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
