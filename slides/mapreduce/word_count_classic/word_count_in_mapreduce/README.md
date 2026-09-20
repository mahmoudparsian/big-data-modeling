# Word Count in MapReduce

Word count solved as a real MapReduce job — `mapper()` and
`reducer()` shown as Python functions, every mapper and
reducer call enumerated in full, mapper-side (`M`, word
length) and reducer-side (`N`, frequency) filters, a
partition-by-partition `combiner()` walkthrough, and a
preview of how it all becomes a PySpark job.

## Contents

| Name | Type | Description |
|---|---|---|
| [`word_count_in_mapreduce.md`](word_count_in_mapreduce.md) | md | Step-by-step walkthrough: input files → `(filename, record)` pairs → `mapper()` → shuffle/sort → `reducer()` → final output, with `M`/`N` filter sections, a 3-partition `combiner()` walkthrough, and a PySpark preview (with filters) |
| [`data/file1.txt`](data/file1.txt) | txt | 3 records, "fox jumped..." text — sample input for `word_count_in_mapreduce.md` |
| [`data/file2.txt`](data/file2.txt) | txt | 4 records, "fox jumped..." text — sample input for `word_count_in_mapreduce.md` |
| [`data/file3.txt`](data/file3.txt) | txt | 5 records, "fox jumped..." text — sample input for `word_count_in_mapreduce.md` |
