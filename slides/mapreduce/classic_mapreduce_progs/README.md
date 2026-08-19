# Classic Hadoop MapReduce Programs

	Author: Mahmoud Parsian
	Last updated: 8/19/2026

## Why this folder exists

These are real, compilable **Hadoop MapReduce programs written in
Java** — the "classic" MapReduce API (`Mapper`, `Reducer`, `Job`,
`ToolRunner`). They are included for one reason: so you can
**appreciate the elegance of Spark/PySpark by contrast**.

Every program here needs three Java source files (a `Driver`, a
`Mapper`, and a `Reducer`), a compile-and-package step, and a shell
script that wires up `HADOOP_HOME`, `CLASSPATH`, and HDFS paths
before it can even run. Compare that to the equivalent PySpark for
Word Count:

```python
sc.textFile("input") \
  .flatMap(lambda line: line.split()) \
  .map(lambda word: (word, 1)) \
  .reduceByKey(lambda a, b: a + b) \
  .saveAsTextFile("output")
```

Five lines, no boilerplate class hierarchy, no manual `Job`
configuration, no separate compile/package/submit pipeline. Seeing
the Hadoop version first is what makes that contrast land.

**You will not write any Hadoop programs in this course.** These are
here to read, not to run or to reproduce. The `run.sh`/`env.sh`
scripts in each folder hardcode Mahmoud Parsian's own local paths
(e.g., `/Users/mparsian/zmp/zs/hadoop-2.6.0`) and won't work on your
machine as-is — they're kept for historical/illustrative reference,
not as instructions to follow.

## Programs

| Program | Problem it solves | Input → Output | Files |
|---|---|---|---|
| **Word Count** | Classic word count, with a length filter (ignores lines/words shorter than 3 characters) and trailing-punctuation stripping baked into the mapper. Uses the reducer as a combiner. | Text documents → `(word, count)` | [`word_count/`](./word_count/) ([Driver](./word_count/src/WordCountDriver.java), [Mapper](./word_count/src/WordCountMapper.java), [Reducer](./word_count/src/WordCountReducer.java)) |
| **Top Movie** | For each user, find the single movie they rated highest. | `userID,movieID,rating` → `(userID, movieID)` of their top-rated movie | [`top_movies/`](./top_movies/) ([Driver](./top_movies/src/TopMovieDriver.java), [Mapper](./top_movies/src/TopMovieMapper.java), [Reducer](./top_movies/src/TopMovieReducer.java)) |
| **Log Handler** | Scans log lines (case-insensitively) for the substrings `"error"`, `"exception"`, and `"warning"`, and counts how many lines contain each. A line matching more than one term is counted in each matching bucket. Uses the reducer as a combiner. | Raw log lines → `(level, count)` for `error`/`exception`/`warning` | [`log_handler/`](./log_handler/) ([Driver](./log_handler/src/LogHandlerDriver.java), [Mapper](./log_handler/src/LogHandlerMapper.java), [Reducer](./log_handler/src/LogHandlerReducer.java)) |
| **Telecom (CDR Analytics)** | From Call Data Records, finds every phone number that racked up **60+ minutes of long-distance (STD) call time**. The mapper filters to STD calls (`STDFlag == 1`) and computes each call's duration; the reducer sums duration per phone number and only emits numbers whose total is ≥ 60 minutes — a filter that can only run in the reducer, since it depends on the aggregate. | `FromPhoneNumber\|ToPhoneNumber\|CallStartTime\|CallEndTime\|STDFlag` → `(phoneNumber, totalSTDMinutes)` for qualifying numbers | [`telecom/`](./telecom/) (has its own [README](./telecom/README.md)) ([Driver](./telecom/src/TelecomDriver.java), [Mapper](./telecom/src/TelecomMapper.java), [Reducer](./telecom/src/TelecomReducer.java)) |

Each program folder follows the same layout:

```text
<program>/
├── src/        # Driver.java, Mapper.java, Reducer.java
├── input/      # sample input files
├── env.sh      # Hadoop/Java environment variables (hardcoded, author's machine)
└── run.sh      # compile -> jar -> copy to HDFS -> submit job
```

## Hadoop Installation Notes

[`hadoop_installation/`](./hadoop_installation/) contains
[`Hadoop-2-6.0-On-Macbook.md`](./hadoop_installation/Hadoop-2-6.0-On-Macbook.md),
a walkthrough for standing up a single-node Hadoop 2.6.0 cluster on
macOS, plus the `conf/` files and `bin/` scripts (`start-hadoop.sh`,
`stop-hadoop.sh`, `format-hadoop.sh`) that go with it. It's kept as
historical/reference material showing what stand-alone Hadoop setup
used to require — not something you need to do for this course, and
not something Spark/PySpark requires at all.

## Takeaway

Skim the `Driver`/`Mapper`/`Reducer` classes above and notice how
much of each one is infrastructure — `Job` configuration, `Tool`/
`ToolRunner` plumbing, `Writable` type wrappers (`Text`,
`IntWritable`, `LongWritable`) — rather than the actual logic of the
problem being solved. That gap between "lines of code" and "lines of
actual logic" is exactly what Spark/PySpark closes, and why the rest
of this course is taught in Spark rather than classic Hadoop
MapReduce.
