# Hadoop MapReduce - Setting up a Single Node Cluster

## 0. Introduction
This document describes how to set up and configure a single-node Hadoop 
installation so that you can quickly perform simple operations using Hadoop 
MapReduce and the Hadoop Distributed File System (HDFS). These instructions
are specific for macbook (but easily can be adopted to Linux).


## 1. Make sure Java is Installed
Download Java and install it. I have Java installed at the following location.
Check the version of Java ($ is OS prompt).

````
$ export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.7.0_60.jdk/Contents/Home
$ echo "JAVA_HOME=$JAVA_HOME"
$ export PATH=.:$JAVA_HOME/bin:$PATH
$ java -version
java version "1.7.0_60"
Java(TM) SE Runtime Environment (build 1.7.0_60-b19)
Java HotSpot(TM) 64-Bit Server VM (build 24.60-b09, mixed mode)
````

## 2. Install Hadoop-2.6.0 

### 2.1 You may download Hadoop from this URL: 

````
http://mirror.metrocast.net/apache/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz
````

Move your downloaded copy to /Users/mparsian/hadoop-2.6.0.tar.gz (I am assuming 
that your username is "mparsian" -- if your username is "alex" then replace 
"mparsian" with "alex").

Check that you have the file:

````
$ ls -l /Users/mparsian/hadoop-2.6.0.tar.gz
-rw-r-----@ 1 alex  ...  195257604 ... hadoop-2.6.0.tar.gz
````

### 2.2 Install Hadoop

````
$ cd /Users/mparsian/
$ tar zvfx hadoop-2.6.0.tar.gz
````

This will create a directory called: /Users/mparsian/hadoop-2.6.0
this will be the installed directory for Hadoop-2.6.0


## 3. Create Directories for HDFS
These directories will be used in configuring Hadoop

````
$ mkdir -p /Users/mparsian/dev/hdfs/name
$ mkdir -p /Users/mparsian/dev/hdfs/data
$ mkdir -p /Users/mparsian/dev/hdfs/tmp
$ mkdir -p /Users/mparsian/dev/hdfs/mr-history
$ mkdir -p /Users/mparsian/dev/hdfs/snn
````

## 4. Configure Hadoop's configuration directory

4.1 Delete the existing config files

````
$ rm -f /Users/mparsian/hadoop-2.6.0/etc/hadoop/*
````

### 4.2 Copy files from https://github.com/mahmoudparsian/big-data-mapreduce-course/hadoop/conf/* 
to /Users/mparsian/hadoop-2.6.0/etc/hadoop/

### 4.3 Replace "mparsian" with "alex" for all files in 
/Users/mparsian/hadoop-2.6.0/etc/hadoop/*


## 5. Copy Scripts File from GitHub

### 5.1 Create a bin directory
$ mkdir -p /Users/mparsian/bin

### 5.2 Copy files from https://github.com/mahmoudparsian/big-data-mapreduce-course/hadoop/bin/* 
to /Users/mparsian/bin/

### 5.3 Replace "mparsian" with "alex" for all files in 
/Users/mparsian/bin/*.sh

### 5.4
chmod a+rx  /Users/mparsian/bin/*.sh

## 6. Make sure "ssh localhost" works

````
$ ssh-keygen -t rsa -P '' -f $HOME/.ssh/id_rsa
$ cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys
$ chmod 700 $HOME/.ssh/authorized_keys  $HOME/.ssh/id_rsa.pub
````

## 7. Reformat HDFS

````
$ cd /Users/mparsian/bin/
$ chmod a+rx *.sh
$ ./format-hadoop.sh
````


## 8. Start Hadoop Cluster of one node

````
$ cd /Users/mparsian/bin/
$ ./start-hadoop.sh
````

## 9. View the NameNode URL

````
http://localhost:50070/dfshealth.html#tab-overview
````

## 10. Access HDFS from Command Line

````
$ export HADOOP_HOME=/Users/mparsian/hadoop-2.6.0
$ source $HADOOP_HOME/etc/hadoop/hadoop-env.sh
$ hadoop fs -ls /
$ hadoop fs -mkdir /wordcount
$ hadoop fs -mkdir /wordcount/input
$ hadoop fs -mkdir /wordcount/output
$ hadoop fs -put /etc/hosts  /
$ hadoop fs -cat  /hosts
````

## 11. Browse the web interface for the ResourceManager

````
http://localhost:8088/
````

## 12. Enjoy MapReducing!



