# Integrate PySpark programs with AI-LLM 

Environment:

* PySpark 4.2+
* Anthropic Claude 
* Marimo notebooks 

---

## Notebooks

| Notebook | API | Topic |
|----------|-----|-------|
| `program_df_1_basics.py` | DataFrame | Sentiment classification, 1 column, `pandas_udf` |
| `program_df_2_intermediate.py` | DataFrame | Multi-column JSON enrichment, `from_json`, cost estimation |
| `program_df_2_intermediate_scale_out.py` | DataFrame | **Scale-out:** batched `pandas_udf`, detailed per-batch logging |
| `program_df_3_intermediate_plus.py` | DataFrame | Batching + checkpoint + dead-letter + Marimo UI sliders |
| `program_rdd_1_basics.py` | RDD | Topic classification, `mapPartitions`, `glom()` |
| `program_rdd_2_intermediate.py` | RDD | Multi-field JSON, batching, accumulator, `reduceByKey`, `join` |
| `program_rdd_2_intermediate_scale_out.py` | RDD | **Scale-out:** batched `mapPartitions`, per-batch logging, cost comparison |

## Datasets

All 7 Marimo notebooks load their data from CSV files in the `data/` directory:

| CSV File | Rows | Used by |
|----------|------|---------|
| `data/reviews.csv` | 10 product reviews | `program_df_1_basics.py` |
| `data/tickets.csv` | 12 support tickets | `program_df_2_intermediate.py`, `program_df_2_intermediate_scale_out.py` |
| `data/products.csv` | 12 product descriptions | `program_df_3_intermediate_plus.py` |
| `data/headlines.csv` | 10 news headlines | `program_rdd_1_basics.py` |
| `data/product_reviews.csv` | 18 e-commerce reviews | `program_rdd_2_intermediate.py`, `program_rdd_2_intermediate_scale_out.py` |

To use different data, replace the CSV files keeping the same column headers.

## Other Files

| Name | Description |
|---|---|
| [`data/`](./data/) | CSV datasets loaded by the notebooks above |
| [`CLAUDE.md`](./CLAUDE.md) | Detailed reference: full repo structure, per-notebook walkthroughs, anti-patterns, and suggested teaching order |
| [`pyspark_llm_slides.md`](./pyspark_llm_slides.md) | MARP slide deck on integrating PySpark with LLMs |
| `.env.example` | Template for API keys/config — copy to `.env` and fill in |
| `program_*.log` | Sample output logs from running the corresponding `.py` notebook |
