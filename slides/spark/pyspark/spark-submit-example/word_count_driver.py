#!/usr/bin/env python3
#-----------------------------------------------------
# This is a word count in PySpark.
# The goal is to show how "word count" works and how
# to submit it with "$SPARK_HOME/bin/spark-submit".
#
# Note: each stage below is deliberately printed with
# its own .count()/.collect() so you can see the RDD
# after every transformation. That means Spark runs a
# separate job per print -- fine for a small teaching
# example, but not how you'd write real production code
# (there you'd let the pipeline run to a single action).
#------------------------------------------------------
# Input Parameters:
#    argv[1]: String, input path
#    argv[2]: int, M: ignore words with length < M
#    argv[3]: int, N: ignore words with frequency < N
#-------------------------------------------------------
import sys
from pyspark.sql import SparkSession

if __name__ == '__main__':

    if len(sys.argv) != 4:
        print("Usage: word_count_driver.py  <input-file>  <M>  <N>", file=sys.stderr)
        sys.exit(1)

    spark = SparkSession\
        .builder\
        .appName("Word-Count-App")\
        .getOrCreate()

    #  sys.argv[0] is the name of the script.
    #  sys.argv[1] is the first parameter
    input_path = sys.argv[1]
    # ignore words with fewer than M characters
    M = int(sys.argv[2])
    # ignore words with a total frequency below N
    N = int(sys.argv[3])
    print("input_path: {}".format(input_path))
    print("M (minimum word length): {}".format(M))
    print("N (minimum word frequency): {}".format(N))

    # read input and create an RDD<String>
    records = spark.sparkContext.textFile(input_path)
    print("records.count(): ", records.count())
    print("records.collect(): ", records.collect())

    # Filter out non-empty lines from the loaded file
    non_empty_records = records.filter(lambda x: len(x) > 0)
    print("non_empty_records.count(): ", non_empty_records.count())
    print("non_empty_records.collect(): ", non_empty_records.collect())

    # convert all words to lowercase and flatten it to words
    words = non_empty_records.flatMap(lambda line: line.lower().split(" "))
    print("words.count(): ", words.count())
    print("words.collect(): ", words.collect())

    # ignore words shorter than M characters
    long_enough_words = words.filter(lambda word: len(word) >= M)
    print("long_enough_words.count(): ", long_enough_words.count())
    print("long_enough_words.collect(): ", long_enough_words.collect())

    # create a pair of (word, 1) for all remaining words
    pairs = long_enough_words.map(lambda word: (word, 1))
    print("pairs.count(): ", pairs.count())
    print("pairs.collect(): ", pairs.collect())

    # aggregate the frequencies of each unique word
    frequencies = pairs.reduceByKey(lambda a, b: a + b)
    print("frequencies.count(): ", frequencies.count())
    print("frequencies.collect(): ", frequencies.collect())

    # ignore words whose frequency is less than N
    frequent_enough = frequencies.filter(lambda pair: pair[1] >= N)
    print("frequent_enough.count(): ", frequent_enough.count())
    print("frequent_enough.collect(): ", frequent_enough.collect())

    # done!
    spark.stop()
