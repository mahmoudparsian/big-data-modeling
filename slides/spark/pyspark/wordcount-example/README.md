# Word Count Example

Standalone PySpark word-count example script, sample input, and filtered output. 

## Contents

| Name | Type | Description |
|---|---|---|
| [`data/foxy.txt`](data/foxy.txt) | txt | Toy text |
| [`data/data.txt`](data/data.txt) | txt (78B) | Toy text ("crazy crazy fox jumped...") used as sample input |
| [`data/data_kv.txt`](data/data_kv.txt) | txt (44B) | Sample `id,name` key-value pairs |
| [`word_count_with_filters.md`](word_count_with_filters.md) | txt (3.6KB) | Walkthrough: word count with filtering, run interactively in the PySpark shell |
| [`wordcount-with-spark-submit.md`](wordcount-with-spark-submit.md) | txt (1.4KB) | Recorded terminal transcript of running `wordcount.py` via `spark-submit` |
| [`wordcount.py`](wordcount.py) | py (1.2KB) | Standalone PySpark word-count script, takes an input path as a CLI argument |
| [`wordcount_submit_job.sh`](wordcount_submit_job.sh) | txt (55B) | One-line usage note: `$SPARK_HOME/bin/spark-submit wordcount.py <input-path>` |
