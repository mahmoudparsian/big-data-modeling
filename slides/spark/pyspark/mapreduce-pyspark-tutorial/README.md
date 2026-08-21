# mapreduce-pyspark-tutorial

Tutorial 1: MapReduce thinking with PySpark RDDs — Marp slide deck plus a self-contained companion notebook that runs every example, from word count to a synthetic retail dataset.

## Contents

| Name | Type | Description |
|---|---|---|
| [`mapreduce-pyspark-tutorial.md`](mapreduce-pyspark-tutorial.md) | md | Marp slide deck: MapReduce model, PySpark RDDs, word count + retail business questions, `reduceByKey` vs `groupByKey`, broadcast variables, accumulators, caching, and the DataFrame equivalent |
| [`mapreduce-pyspark-notebook.ipynb`](mapreduce-pyspark-notebook.ipynb) | ipynb | Runnable notebook mirroring the slides — reads the sample data from `data/` |
| [`generate_retail_data.py`](generate_retail_data.py) | py | Standalone script that (re)generates `data/` — deterministic (seed=7), pure stdlib |
| [`data/`](data) | folder | 4 CSVs: customers, products, orders, items |

## Running

- Render slides to PDF: `marp mapreduce-pyspark-tutorial.md --pdf`
- Generate the sample data once: `python3 generate_retail_data.py`
- Run the notebook: `jupyter notebook mapreduce-pyspark-notebook.ipynb` (requires `pyspark`; see [Checking Your PySpark Installation](../../installation/checking_pyspark_installation.md))
