#-----------------------------------------------------
# This is a shell script to run dataframe_creation_cvs_with_header.py
#-----------------------------------------------------
# @author Mahmoud Parsian
#-----------------------------------------------------
export SPARK_HOME="/home/mp/spark-4.0.0"
export INPUT_FILE="/home/mp/dataframes/emps_with_header.txt"
export SPARK_PROG="/home/mp/dataframes/dataframe_creation_cvs_with_header.py"
#
# run the PySpark program by spark-submit:
#
#                            sys.argv[0] sys.argv[1]
$SPARK_HOME/bin/spark-submit $SPARK_PROG $INPUT_FILE
