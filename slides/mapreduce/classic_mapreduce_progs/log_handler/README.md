# log_handler

Classic Java MapReduce program that parses log files; see `src/` for code and `input/` for sample logs.

## Contents

| Name | Type | Description |
|---|---|---|
| [`input/`](input/) | folder | 5 items |
| [`src/`](src/) | folder | 3 items |
| [`env.sh`](env.sh) | sh (401B) | Sets `HADOOP_HOME`/`JAVA_HOME` and sources Hadoop's environment |
| [`run.sh`](run.sh) | sh (1.0KB) | Compiles the job, builds the classpath from Hadoop's jars, and runs it on Hadoop |
