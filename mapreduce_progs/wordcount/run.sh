#/bin/bash
MP=/Users/mparsian
WORDCOUNT=$MP/zmp/BigData-MapReduce-Course/programs/wordcount
# define the installation dir for hadoop
export HADOOP_HOME=$MP/zmp/zs/hadoop-2.6.0
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_HOME_WARN_SUPPRESS=true

# define your Java
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.7.0_60.jdk/Contents/Home
echo "JAVA_HOME=$JAVA_HOME"

# define PATH: where programs will be found
PATH=.:/usr/bin:/bin:/usr/sbin:/sbin:/usr/local/bin:/usr/X11/bin
export PATH=$PATH:$HADOOP_HOME/bin:$PATH:$JAVA_HOME/bin

# set up CLASSPATH
CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar
jars=`find $HADOOP_HOME -name '*.jar'`
for j in $jars ; do
  CLASSPATH=$CLASSPATH:$j
done

# define your custom library
APP_JAR=$WORDCOUNT/wordcount.jar
export CLASSPATH=$CLASSPATH:$APP_JAR:$HADOOP_CONF_DIR
export HADOOP_CLASSPATH=$CLASSPATH

# defines some environment for hadoop
source $HADOOP_CONF_DIR/hadoop-env.sh

# compile my source files
javac src/*.java

# create a custom library
jar cvf $APP_JAR  -C src/ .

# define input/output for Hadoop/HDFS
INPUT=/wordcount/input 
OUTPUT=/wordcount/output

# remove all files under input
$HADOOP_HOME/bin/hadoop fs -rmr $INPUT/*

# copy local files to Hadoop
$HADOOP_HOME/bin/hadoop fs -copyFromLocal input/*  $INPUT/

# remove all files under output
$HADOOP_HOME/bin/hadoop fs -rmr $OUTPUT

# run the program 
$HADOOP_HOME/bin/hadoop jar  $APP_JAR WordCountDriver $INPUT $OUTPUT
