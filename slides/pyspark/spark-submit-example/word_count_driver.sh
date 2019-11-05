#------------------------------------------
# This is a shell script for word count in 
# PySpark using the "spark-submit" command.
#------------------------------------------
# @author Mahmoud Parsian
#------------------------------------------
export SPARK_HOME="/home/mparsian/spark-2.3.0"
export INPUT_FILE="/home/mparsian/code/sample_file.txt"
export SPARK_PROG="/home/mparsian/code/word_count_driver.py"
#
# run the PySpark program:
$SPARK_HOME/bin/spark-submit $SPARK_PROG $INPUT_FILE
