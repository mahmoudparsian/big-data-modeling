# src

Java source (Driver/Mapper/Reducer) for the classic `word_count` MapReduce program.

## Contents

| Name | Type | Description |
|---|---|---|
| [`WordCountDriver.java`](WordCountDriver.java) | java (2.8KB) | Job driver: configures and submits the word-count MapReduce job |
| [`WordCountMapper.java`](WordCountMapper.java) | java (1.9KB) | Splits each input line into words, emits `(word, 1)` |
| [`WordCountReducer.java`](WordCountReducer.java) | java (940B) | Sums the input values per key |
