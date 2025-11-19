#!/usr/bin/python
#-----------------------------------------------------
# Create a DataFrame 
# Input: CSV with header
#------------------------------------------------------
# Input Parameters:
#    argv[1]: String, input path
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 

if __name__ == '__main__':

    # make sure that we have 2 parameters after 
    # the spark-submit command
    if len(sys.argv) != 2:  
        print("Usage: dataframe_creation_csv_with_header.py <file>", file=sys.stderr)
        exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_creation_csv_with_header")\
        .getOrCreate()

    #  sys.argv[0] is the name of the script.
    #  sys.argv[1] is the first parameter
    input_path = sys.argv[1]  
    print("input_path: {}".format(input_path))

    # The following reads a CSV file with a 
    # header and infers a schema from the content of columns:
    df = spark\
          .read\
          .format("csv")\
          .option("header","true")\
          .option("inferSchema", "true")\
          .load(input_path) 
    #
    df.show()
    # +----+-----+------+--------+
    # |  id| name|salary|    dept|
    # +----+-----+------+--------+
    # |1001| alex| 67000|   SALES|
    # |1002|  bob| 24000|   SALES|
    # |1003| boby| 24000|   SALES|
    # |1004| jane| 69000|SOFTWARE|
    # |1005|betty| 55000|SOFTWARE|
    # |1006| jeff| 59000|SOFTWARE|
    # |1007| dara| 72000|SOFTWARE|
    # +----+-----+------+--------+

    df.printSchema()
    # root
    #  |-- id: integer (nullable = true)
    #  |-- name: string (nullable = true)
    #  |-- salary: integer (nullable = true)
    #  |-- dept: string (nullable = true)


    # To use a SQL query on a DataFrame, you have to 
    # register an alias table name (semantically, equivalent 
    # to a relational database table) for your DataFrame as:
    df.createOrReplaceTempView("emp_table")
    print("df=", df)
    # DataFrame[id: string, name: string, salary: string, dept: string]

    df3 = spark.sql("SELECT * FROM emp_table WHERE id > 1002")
    df3.show()
    # +----+-----+------+--------+
    # |  id| name|salary|    dept|
    # +----+-----+------+--------+
    # |1003| boby| 24000|   SALES|
    # |1004| jane| 69000|SOFTWARE|
    # |1005|betty| 55000|SOFTWARE|
    # |1006| jeff| 59000|SOFTWARE|
    # |1007| dara| 72000|SOFTWARE|
    # +----+-----+------+--------+


    # This is another SQL query filtering based on `salary`:
    df4 = spark.sql("SELECT * FROM emp_table WHERE salary < 60000")
    df4.show()
    # +----+-----+------+--------+
    # |  id| name|salary|    dept|
    # +----+-----+------+--------+
    # |1002|  bob| 24000|   SALES|
    # |1003| boby| 24000|   SALES|
    # |1005|betty| 55000|SOFTWARE|
    # |1006| jeff| 59000|SOFTWARE|
    # +----+-----+------+--------+

    # The next SQL query projects specific columns
    # based on a `salary` filter:
    df5 = spark.sql("SELECT name, salary FROM emp_table WHERE salary > 25000")
    print("df5=", df5)
    # DataFrame[name: string, salary: string]
    df5.show()
    # +-----+------+
    # | name|salary|
    # +-----+------+
    # | alex| 67000|
    # | jane| 69000|
    # |betty| 55000|
    # | jeff| 59000|
    # | dara| 72000|
    # +-----+------+

    # Also, using SQL query, you can project, filter and sort:
    df6 = spark.sql("SELECT name, salary FROM emp_table WHERE salary > 55000 ORDER BY salary")
    print("df6=", df6)
    # DataFrame[name: string, salary: string]
    df6.show()
    # +----+------+
    # |name|salary|
    # +----+------+
    # |jeff| 59000|
    # |alex| 67000|
    # |jane| 69000|
    # |dara| 72000|
    # +----+------+

    # The next SQL query uses "GROUP BY" to group values of `dept` column:
    df7 = spark.sql("SELECT dept, count(*) as count FROM emp_table GROUP BY dept")
    print("df7=", df7)
    # DataFrame[dept: string, count: bigint]
    df7.show()
    # +--------+-----+
    # |    dept|count|
    # +--------+-----+
    # |   SALES|    3|
    # |SOFTWARE|    4|
    # +--------+-----+


    # done!
    spark.stop()
