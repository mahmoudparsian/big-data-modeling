# progs_table_level

Table-level PySpark + LLM example scripts (natural-language-to-SQL over one or many tables) with sample run logs.

## Contents

| Name | Type | Description |
|---|---|---|
| [`multi_table_analysis_nl_query_to_sql.log`](multi_table_analysis_nl_query_to_sql.log) | log (2.1KB) | Sample run log for the multi-table script |
| [`multi_table_analysis_nl_query_to_sql.py`](multi_table_analysis_nl_query_to_sql.py) | py (5.6KB) | Translates a natural-language question into SQL across multiple joined tables |
| [`single_table_analysis_nl_query_to_sql.log`](single_table_analysis_nl_query_to_sql.log) | log (849B) | Sample run log for the single-table script |
| [`single_table_analysis_nl_query_to_sql.py`](single_table_analysis_nl_query_to_sql.py) | py (5.5KB) | Translates a natural-language question into SQL over a single table |
| [`single_table_analysis_nl_query_to_sql_with_extra_prompt.log`](single_table_analysis_nl_query_to_sql_with_extra_prompt.log) | log (856B) | Sample run log for the "extra prompt" variant |
| [`single_table_analysis_nl_query_to_sql_with_extra_prompt.py`](single_table_analysis_nl_query_to_sql_with_extra_prompt.py) | py (5.9KB) | Same as `single_table_analysis_nl_query_to_sql.py`, with an extra prompt-engineering step for better SQL accuracy |
