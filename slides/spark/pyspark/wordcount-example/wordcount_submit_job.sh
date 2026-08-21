INPUT_PATH="data.txt"
PROG="wordcount.py"
#
$SPARK_HOME/bin/spark-submit $PROG ${INPUT_PATH}
