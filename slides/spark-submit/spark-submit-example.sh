#!/bin/bash

# This is the path to the spark installation directory on my machine.
# This variable will differ for you based on your install location
MY_SPARK_PATH=/Users/pli/build/spark

# This is the path to my python scripts
MY_SCRIPT_PATH=/Users/pli/depot/skool/msis2627/hw

# Start spark 
$MY_SPARK_PATH/sbin/start-all.sh

# Print python script example
cat $MY_SCRIPT_PATH/2/hw2.py

# Print data example
cat $MY_SCRIPT_PATH/2/data.txt

# Run spark-submit and provide both a python script and data source as
# parameters
$MY_SPARK_PATH/bin/spark-submit \
  $MY_SCRIPT_PATH/2/hw2.py \
  $MY_SCRIPT_PATH/2/data.txt

# Stop spark
$MY_SPARK_PATH/sbin/stop-all.sh
