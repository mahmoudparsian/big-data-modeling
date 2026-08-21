# src

Java source (Driver/Mapper/Reducer) for the `log_handler` classic MapReduce program.

## Contents

| Name | Type | Description |
|---|---|---|
| [`LogHandlerDriver.java`](LogHandlerDriver.java) | java (2.8KB) | Job driver: configures and submits the log-handler MapReduce job |
| [`LogHandlerMapper.java`](LogHandlerMapper.java) | java (1.6KB) | Splits each input line into words, emits `(word, 1)` |
| [`LogHandlerReducer.java`](LogHandlerReducer.java) | java (1.2KB) | Sums the input values per key |
