# Word Count in Python

**What is a A word count problem?**
A word count problem is a classic computing 
task that reads a set of texts (e.g. as a 
set of text files in a directory) and counts 
how many times each individual word appears.
So, the input is a set of text files, and output
is a dictionary of `(word-as-a-key, frequency-as-an-integer)`.

Minimal, dependency-free Python word-count 
scripts (no Spark/Hadoop) plus sample input 
files.

## Folder Contents

| Name | Type | Description |
|---|---|---|
| [`data/file1.txt`](data/file1.txt) | txt | 3 records, "fox jumped..." text — sample input for `word_count_dir_to_tsv.py` |
| [`data/file2.txt`](data/file2.txt) | txt | 4 records, "fox jumped..." text — sample input for `word_count_dir_to_tsv.py` |
| [`data/file3.txt`](data/file3.txt) | txt | 5 records, "fox jumped..." text — sample input for `word_count_dir_to_tsv.py` |
| [`test_file.txt`](test_file.txt) | txt | Toy text ("fox jumped...") used as sample input for `word_count_python.py` |
| [`word_count_single_file.py`](word_count_single_file.py) | py  | Plain-Python word-count script (reads a single text file, tokenizes, counts words, prints to stdout) |
| [`word_count_single_file_v2.py`](word_count_single_file_v2.py) | py | an improved version of `word_count_single_file.py` |
| [`word_count_dir_to_tsv.py`](word_count_dir_to_tsv.py) | py  | Plain-Python word-count script (reads all `.txt` files in an input directory, tokenizes, counts words, writes a `<word><TAB><count>` TSV file) |
| [`word_count_dir_to_tsv_with_filter.py`](word_count_dir_to_tsv_with_filter.py) | py  | Same as `word_count_dir_to_tsv.py`, plus two filter thresholds `M` and `N` — see [Filtering: `word_count_dir_to_tsv_with_filter.py`](#filtering-word_count_dir_to_tsv_with_filterpy) below |


## Word Count Sample Run: `word_count_dir_to_tsv.py`

```
% python3 word_count_dir_to_tsv.py data
input_dir= data
output_path= word_count_output.tsv
processing: data/file1.txt
processing: data/file2.txt
processing: data/file3.txt
Wrote 17 unique words to word_count_output.tsv

% cat word_count_output.tsv
and	7
cute	1
far	1
fox	22
gray	4
high	1
is	3
jumped	14
lazy	2
over	6
quick	1
ran	1
red	7
slept	1
smart	1
watched	1
while	1
```

## Filtering: `word_count_dir_to_tsv_with_filter.py`

`word_count_dir_to_tsv_with_filter.py` extends `word_count_dir_to_tsv.py` with two integer thresholds:

```
python3 word_count_dir_to_tsv_with_filter.py <input_dir> <M> <N> [output_tsv]
```

| Arg | Meaning |
|---|---|
| `M` | Minimum **word length**. Any word shorter than `M` characters is ignored — it is never counted at all (a "mapper-side" filter, applied before counting). |
| `N` | Minimum **frequency**. After counting, any word whose total count is less than `N` is dropped from the output (a "reducer-side" filter, applied after counting — the word was still counted, it's just not written out). |

Sample run, filtering out short words (< 3 chars) and rare words (count < 5), over [`data/`](data/):

```
% python3 word_count_dir_to_tsv_with_filter.py data 3 5 word_count_output_filtered.tsv
input_dir= data
M (min word length)= 3
N (min frequency)= 5
output_path= word_count_output_filtered.tsv
processing: data/file1.txt
processing: data/file2.txt
processing: data/file3.txt
Wrote 5 unique words (of 16 that passed the M filter) to word_count_output_filtered.tsv

% cat word_count_output_filtered.tsv
and	7
fox	22
jumped	14
over	6
red	7
```

* Some Notes:

	* `is` is dropped by the `M` filter — length 2, below the M=3 threshold. 
	* `cute`, `far`, `high`, etc. pass `M` but are dropped by the `N` filter — each occurs only once, below the N=5 threshold.
