rm -fr /Users/mparsian/dev/hdfs/name/*
rm -fr /Users/mparsian/dev/hdfs/data/*
rm -fr /Users/mparsian/dev/hdfs/tmp/*
#
HADOOP_HOME=/Users/mparsian/hadoop-2.6.0
source $HADOOP_HOME/etc/hadoop/hadoop-env.sh
$HADOOP_HOME/bin/hadoop namenode -format
