#/bin/bash

export HADOOP_HOME="/Users/mparsian/zmp/zs/hadoop-2.8.0"
export HADOOP_HOME_WARN_SUPPRESS=true

export JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk1.8.0_144.jdk/Contents/Home"
echo "JAVA_HOME=$JAVA_HOME"

#PATH=.:/usr/bin:/bin:/usr/sbin:/sbin:/usr/local/bin:/usr/X11/bin
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH:$JAVA_HOME/bin

source $HADOOP_HOME/etc/hadoop/hadoop-env.sh
