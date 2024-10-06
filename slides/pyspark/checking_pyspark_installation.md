# Checking PySpark Installation

`1.` Make sure that the Java 11/17 is installed:

~~~
% echo $JAVA_HOME
/Library/Java/JavaVirtualMachines/jdk-11.jdk/Contents/Home

% java --version
java 11.0.19 2023-04-18 LTS
Java(TM) SE Runtime Environment 18.9 (build 11.0.19+9-LTS-224)
Java HotSpot(TM) 64-Bit Server VM 18.9 (build 11.0.19+9-LTS-224, mixed mode)

~~~

`2.` Make sure that the Python3 is installed:

~~~
% python3 --version
Python 3.12.0
~~~

`3.` Make sure that the Spark is installed:

~~~
# change your working directory to
# where yoh have installed Spark

% cd spark-3.5.3
% ls -la
drwxr-xr-x@  18 mparsian  staff    576 Oct  5 17:21 .
drwxr-xr-x+ 144 mparsian  staff   4608 Oct  5 17:54 ..
-rw-r--r--@   1 mparsian  staff  22916 Sep  8 22:33 LICENSE
-rw-r--r--@   1 mparsian  staff  57842 Sep  8 22:33 NOTICE
drwxr-xr-x@   3 mparsian  staff     96 Sep  8 22:33 R
-rw-r--r--@   1 mparsian  staff   4605 Sep  8 22:33 README.md
-rw-r--r--@   1 mparsian  staff    166 Sep  8 22:33 RELEASE
drwxr-xr-x@  30 mparsian  staff    960 Sep  8 22:33 bin
drwxr-xr-x@   8 mparsian  staff    256 Sep  8 22:33 conf
drwxr-xr-x@   6 mparsian  staff    192 Sep  8 22:33 data
drwxr-xr-x@   4 mparsian  staff    128 Sep  8 22:33 examples
drwxr-xr-x@ 254 mparsian  staff   8128 Sep  8 22:33 jars
drwxr-xr-x@   4 mparsian  staff    128 Sep  8 22:33 kubernetes
drwxr-xr-x@  58 mparsian  staff   1856 Sep  8 22:33 licenses
drwxr-xr-x@  19 mparsian  staff    608 Sep  8 22:33 python
drwxr-xr-x@  31 mparsian  staff    992 Sep  8 22:33 sbin
drwxr-xr-x@   3 mparsian  staff     96 Sep  8 22:33 yarn
~~~

`4.` Next, invoke `PySpark` program:


~~~
% ./bin/pyspark
Python 3.12.0 (v3.12.0:0fb18b02c8, Oct  2 2023, 09:45:56) [Clang 13.0.0 (clang-1300.0.29.30)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
Setting default log level to "WARN".
24/10/05 17:55:31 WARN Utils: Service 'SparkUI' could not bind on port 4041. Attempting port 4042.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.3
      /_/

Using Python version 3.12.0 (v3.12.0:0fb18b02c8, Oct  2 2023 09:45:56)
Spark context Web UI available at http://192.168.0.108:4042
Spark context available as 'sc' (master = local[*], app id = local-1728176131775).
SparkSession available as 'spark'.
~~~

Note that 

* `spark` is created as a SparkSession object
* `sc` is created as a SparkContext object


`5.` Next create some Python collection and then create
an RDD from your collection:

~~~
>>> some_data = [1, 3, 5, 6, 8, 20, 30, 90]
>>> some_data
[1, 3, 5, 6, 8, 20, 30, 90]

>>> spark.version
'3.5.3'

>>> sc.version
'3.5.3'

>>> rdd = sc.parallelize(some_data)
>>> rdd.count()
8
>>> rdd.collect()
[1, 3, 5, 6, 8, 20, 30, 90]
>>>
~~~