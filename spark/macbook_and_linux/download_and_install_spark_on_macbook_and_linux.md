# Install Spark on MacBook/Linux & PySpark Shell

* Note that these instructions are for MacBook and Linux.

* To install Spark on Windows, 
[Follow This Link](https://github.com/mahmoudparsian/big-data-mapreduce-course/blob/master/spark/windows/Installing_Spark_On_VirtualBox_Ubuntu_Hrishikesh_Panchbhai.pdf)


* If there an error in this document, please notify me to correct it. Thanks.


## How to install Spark on Macbook and Linux

1. Download Spark from [Apache Spark](http://spark.apache.org/downloads.html)

For example, you may download it from [here](https://www.apache.org/dyn/closer.lua/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz)


2. Open the downloaded file: `spark-2.3.0-bin-hadoop2.7.tgz` file
   in a direcory: Let's say that we installed it at `/home/alex/spark-2.3.0`
   
With this, now your `SPARK_HOME` is `/home/alex/spark-2.3.0`

Note that in your case your installed directory can be different than
from "/home/alex", so you have to update accordingly to suit your directory name

ALSO NOTE that here I have renamed directory `spark-2.3.0-bin-hadoop2.7`
to `spark-2.3.0` (as a short hand name)

3. Now you should be able to see the following:

````
ls -l /home/alex/spark-2.3.0

-rw-r--r--@   ...  LICENSE
-rw-r--r--@   ...  NOTICE
...
drwxr-xr-x@   ...  bin
drwxr-xr-x@   ...  conf
...
drwxr-xr-x@   ...  python
drwxr-xr-x@   ...  sbin
...
````

4. Create a directory called  `/home/alex/spark-2.3.0/zbin`
where we will put some scripts for ease of use.

5. Create a text file called `/home/alex/spark-2.3.0/zbin/env_setup.sh`
which will have the following content


````
$ cat /home/alex/spark-2.3.0/zbin/env_setup.sh
unset HADOOP_HOME
unset HADOOP_CONF_DIR
unset JAVA_LIBRARY_PATH
unset YARN_CONF_DIR
unset HADOOP_CLASSPATH
unset YARN_HOME
unset HADOOP_MAPRED_HOME
unset HADOOP_PREFIX
unset HADOOP_DATANODE_OPTS
unset HADOOP_SECURE_DN_PID_DIR
unset HADOOP_IDENT_STRING
unset HADOOP_LOG_DIR
unset HADOOP_HEAPSIZE
unset HADOOP_CLIENT_OPTS
unset HADOOP_PORTMAP_OPTS
unset HADOOP_OPTS
unset HADOOP_SECONDARYNAMENODE_OPTS
unset HADOOP_NAMENODE_OPTS
unset HADOOP_HOME_WARN_SUPPRESS
unset HADOOP_NFS3_OPTS
unset HADOOP_PID_DIR
#
export SPARK_HOME=/home/alex/spark-2.3.0
#
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_144.jdk/Contents/Home
#
export PATH=.:$JAVA_HOME/bin:/Library/Frameworks/Python.framework/Versions/3.6/bin:$SPARK_HOME/sbin:$SPARK_HOME/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/opt/X11/bin:$PATH
````

6. Create a file called `/home/alex/spark-2.3.0/spark-env.sh`

````
cat /home/alex/spark-2.3.0/spark-env.sh
export SPARK_HOME="/Users/mparsian/spark-2.2.1"
export JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk1.8.0_144.jdk/Contents/Home"
````


7. Create a file called `/home/alex/spark-2.3.0/zbin/start-spark.sh`

````
cat /home/alex/spark-2.3.0/zbin/start-spark.sh
#
export SPARK_HOME=/home/alex/spark-2.3.0
#
source $SPARK_HOME/zbin/env_setup.sh
#
rm -fr $SPARK_HOME/metastore_db
#
$SPARK_HOME/sbin/stop-all.sh
$SPARK_HOME/sbin/start-all.sh
````

8. Create a file called `/home/alex/spark-2.3.0/zbin/stop-spark.sh`

````
cat /home/alex/spark-2.3.0/zbin/stop-spark.sh
#
export SPARK_HOME=/home/alex/spark-2.3.0
#
source $SPARK_HOME/zbin/env_setup.sh
#
$SPARK_HOME/sbin/stop-all.sh
````

9. Create a file called `/home/alex/spark-2.3.0/zbin/start-pyspark-shell.sh`

````
cat /home/alex/spark-2.3.0/zbin/start-pyspark-shell.sh
#
export SPARK_HOME=/home/alex/spark-2.3.0
#
source $SPARK_HOME/zbin/env_setup.sh
#
$SPARK_HOME/bin/pyspark
````

10. Make all your scripts executable

````
chmod a+rx /home/alex/spark-2.3.0/zbin/*
````

11. Now Start your Spark cluster

````
/home/alex/spark-2.3.0/zbin/start-spark.sh
````

NOTE on Error running `start-all.sh` Connection refused

If you are on a MacBook and run into the following error when running 


````
/home/alex/spark-2.3.0/zbin/start-spark.sh

starting org.apache.spark.deploy.master.Master, logging to ...
localhost: ssh: connect to host localhost port 22: Connection refused
````



THEN you need to enable "Remote Login" for your machine. 
From `System Preferences`, select `Sharing`, and then turn on `Remote Login`.




12. Verify that your Spark cluster is running:
check:  `http://localhost:8080`


13. Now Stop your Spark cluster

````
/home/alex/spark-2.3.0/zbin/stop-spark.sh
````


14. Now again Start your Spark cluster

````
/home/alex/spark-2.3.0/zbin/start-spark.sh
````

15. Now Start PySpark Shell:

````
/home/alex/spark-2.3.0/zbin/start-pyspark-shell.sh
````

Now you will enter shell and see the following:

````
Python 2.7.10 (default, Apr  7 2018, 00:08:15)
Setting default log level to "WARN".
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.3.0
      /_/

SparkSession available as 'spark'.
>>>
>>>
>>>
>>> spark
<pyspark.sql.session.SparkSession object at 0x10193da50>
>>>
>>> sc = spark.sparkContext
>>>
>>> sc
<SparkContext master=local[*] appName=PySparkShell>
>>>
>>> mylist = [('fox', 1), ('fox', 2), ('cat', 10), ('cat', 20), ('cat', 30), ('fox', 3)]
>>> mylist
[('fox', 1), ('fox', 2), ('cat', 10), ('cat', 20), ('cat', 30), ('fox', 3)]
>>>
>>> rdd = sc.parallelize(mylist)
>>>
>>> rdd.count()
6
>>> rdd.collect()
[('fox', 1), ('fox', 2), ('cat', 10), ('cat', 20), ('cat', 30), ('fox', 3)]
>>>
>>>
>>>
>>> frequency = rdd.reduceByKey(lambda x, y : x+y)
>>> frequency.collect()
[('fox', 6), ('cat', 60)]
>>>
>>> grouped = rdd.groupByKey()
>>> grouped.collect()
[
 ('fox', <pyspark.resultiterable.ResultIterable object at 0x1019913d0>),
 ('cat', <pyspark.resultiterable.ResultIterable object at 0x101991450>)
]
>>>
>>> grouped.mapValues(lambda values : list(values)).collect()
[
 ('fox', [1, 2, 3]),
 ('cat', [10, 20, 30])
]
>>>
>>> sumofvalues = rdd.groupByKey().mapValues(lambda values : sum(values))
>>> sumofvalues.collect()
[
 ('fox', 6),
 ('cat', 60)
]
>>>
````

