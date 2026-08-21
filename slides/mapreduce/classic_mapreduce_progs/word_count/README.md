# word_count

Classic Java MapReduce word-count program, including a pre-built `wordcount.jar` and a sample run log.

## Contents

| Name | Type | Description |
|---|---|---|
| [`input/`](input/) | folder | 5 items |
| [`src/`](src/) | folder | 3 items |
| [`env.sh`](env.sh) | sh (404B) | Sets `HADOOP_HOME`/`JAVA_HOME` and sources Hadoop's environment |
| [`run.log`](run.log) | log (8.4KB) | Sample console output from running `run.sh` |
| [`run.sh`](run.sh) | sh (1.4KB) | Compiles the job, builds the classpath from Hadoop's jars, and runs it on Hadoop |
| [`wordcount.jar`](wordcount.jar) | jar (6.4KB) | Pre-built job JAR (compiled from `src/`) |
