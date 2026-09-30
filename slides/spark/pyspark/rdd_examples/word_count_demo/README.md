# Word Count Demo

The word count problem is the "Hello World"
program of distributed data processing. Given 
a massive collection of text documents, the 
goal is to count how many times each unique 
word appears across the entire dataset using 
a distributed MapReduce framework.

Word-count demo script and its input data (`data/`), with a sample run log.

## Contents

| Name | Type | Description |
|---|---|---|
| [`data/`](data/) | folder | 3 items |
| [`wordcount_demo.log`](wordcount_demo.log) | log (1.4KB) | Sample output from running the script |
| [`wordcount_demo.py`](wordcount_demo.py) | py (2.3KB) | PySpark word-count demo script, reads its input from `data/` |
