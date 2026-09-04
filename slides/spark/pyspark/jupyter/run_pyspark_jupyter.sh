#!/bin/bash
#------------------------------------------------------------
# Launch Jupyter Notebook configured to use PySpark, by
# starting Jupyter directly from $SPARK_HOME/bin/pyspark.
#------------------------------------------------------------
# Before running this script, set SPARK_HOME to your local
# Spark installation directory, for example:
#
#   export SPARK_HOME=/opt/spark
#------------------------------------------------------------
if [ -z "$SPARK_HOME" ]; then
    echo "Error: SPARK_HOME is not set."
    echo "Set it to your Spark installation directory, e.g.:"
    echo "  export SPARK_HOME=/opt/spark"
    exit 1
fi

# Tell pyspark's launcher to open Jupyter Notebook instead of
# the plain Python/IPython REPL:
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS=notebook

# Invoke PySpark; Jupyter opens in your browser with a SparkSession
# already available as `spark`:
"$SPARK_HOME/bin/pyspark"
