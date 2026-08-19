#/bin/bash
MP=/Users/mparsian
LOGHANDLER=$MP/zmp/BigData-MapReduce-Course/programs/loghandler
export HADOOP_HOME=$MP/zmp/zs/hadoop-2.6.0
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_HOME_WARN_SUPPRESS=true

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.7.0_60.jdk/Contents/Home
echo "JAVA_HOME=$JAVA_HOME"

PATH=.:/usr/bin:/bin:/usr/sbin:/sbin:/usr/local/bin:/usr/X11/bin
export PATH=$PATH:$HADOOP_HOME/bin:$PATH:$JAVA_HOME/bin

CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar
jars=`find $HADOOP_HOME -name '*.jar'`
for j in $jars ; do
  CLASSPATH=$CLASSPATH:$j
done

APP_JAR=$LOGHANDLER/loghandler.jar
export CLASSPATH=$CLASSPATH:$APP_JAR:$HADOOP_CONF_DIR
export HADOOP_CLASSPATH=$CLASSPATH

source $HADOOP_CONF_DIR/hadoop-env.sh

javac src/*.java
jar cvf $APP_JAR  -C src/ .
INPUT=/logs/input 
OUTPUT=/logs/output
$HADOOP_HOME/bin/hadoop fs -rmr $INPUT/*
$HADOOP_HOME/bin/hadoop fs -copyFromLocal input/*  $INPUT/
$HADOOP_HOME/bin/hadoop fs -rmr $OUTPUT
$HADOOP_HOME/bin/hadoop jar  $APP_JAR LogHandlerDriver $INPUT $OUTPUT
