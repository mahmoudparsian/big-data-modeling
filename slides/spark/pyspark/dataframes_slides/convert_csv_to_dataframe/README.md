# convert_csv_to_dataframe

Demo script for reading a CSV file (with header) into a Spark DataFrame.

## Contents

| Name | Type | Description |
|---|---|---|
| [`dataframe_creation_cvs_with_header.py`](dataframe_creation_cvs_with_header.py) | py (4.2KB) | Reads `emps_with_header.txt` as a CSV (with header) into a DataFrame |
| [`emps_with_header.txt`](emps_with_header.txt) | txt (186B) | Sample employee CSV (`id,name,salary,dept`) used as input |
| [`run_spark.sh`](run_spark.sh) | sh (594B) | Runs `dataframe_creation_cvs_with_header.py` via `spark-submit` |
