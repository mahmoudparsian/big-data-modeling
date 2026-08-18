# 1. define where is Python
export PYTHON_HOME=/Users/mparsian/Library/Python/3.10

# 2. define where is Spark
export SPARK_HOME=/Users/mparsian/spark-3.5.0

# 3. set PATH
export PATH=$PYTHON_HOME/bin:$SPARK_HOME/bin:$PATH

# 4. define PySpark driver
export PYSPARK_DRIVER_PYTHON=jupyter

# 5. define your notebook
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'

# 6. Invoke PySpark and use it from Jupyter
$SPARK_HOME/bin/pyspark
