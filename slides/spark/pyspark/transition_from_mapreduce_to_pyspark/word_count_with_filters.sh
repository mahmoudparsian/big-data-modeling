PROG="word_count_with_filters.py"
INPUT_PATH="data/"
OUTPUT_PATH="output"
L=3
F=4
$SPARK_HOME/bin/spark-submit  $PROG $INPUT_PATH $OUTPUT_PATH $L $F
