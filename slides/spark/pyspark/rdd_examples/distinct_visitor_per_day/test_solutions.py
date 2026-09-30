"""Run with spark-submit --master "local[2]" test_solutions.py on Spark 4.2.0."""
import csv
import io
import os
from pathlib import Path
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
from pyspark.sql import SparkSession
from distinct_visitors_rdd import distinct_visitors as rdd_count, parse_partition


def main():
    spark = SparkSession.builder.appName("DistinctVisitorsRDDChecks").getOrCreate()
    try:
        spark.sparkContext.setLogLevel("ERROR")
        assert spark.version == "4.2.0", f"Expected Spark 4.2.0, got {spark.version}"
        folder = Path(__file__).resolve().parent
        spark.sparkContext.addPyFile(str(folder / "distinct_visitors_rdd.py"))
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
        pairs = spark.sparkContext.textFile(str(data)).mapPartitions(parse_partition)
        assert dict(rdd_count(pairs).collect()) == expected

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
                lines = []
                for row in records:
                    buffer = io.StringIO()
                    csv.writer(buffer).writerow(row)
                    lines.append(buffer.getvalue().rstrip("\r\n"))
                pairs = spark.sparkContext.parallelize(lines, partitions).mapPartitions(parse_partition)
                assert dict(rdd_count(pairs).collect()) == answer
        print(f"All checks passed on Spark {spark.version}: file input, RDD API, duplicates, empty input, cleaning, and repartitioning.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
