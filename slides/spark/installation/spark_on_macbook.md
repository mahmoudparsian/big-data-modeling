# Installing Spark & PySpark on macOS / Linux (and Cloud VMs)

These instructions cover installing and running Apache Spark / PySpark
on **macOS or Linux**, plus two cloud-VM alternatives (Google Cloud,
Amazon AWS) for when you'd rather run on a hosted Ubuntu machine than
your own laptop.

> Installing on **Windows without a VM**? See
> [`spark_on_windows.md`](spark_on_windows.md) instead.

---

## 1. Prerequisites

* **Java** (JDK 8, or 11/17 for newer Spark releases) —
  [download here](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* **Python 3** — verify with `python3 --version`

---

## 2. Download and Install Spark

1. Download a Spark release from the
   [Apache Spark downloads page](http://spark.apache.org/downloads.html)
   — for example:
   `https://www.apache.org/dyn/closer.lua/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz`
   (substitute whichever current version you actually downloaded).

2. Extract the archive into a directory of your choice. For example,
   if you extract it to `/home/alex/spark-2.3.0`, then your
   `SPARK_HOME` is `/home/alex/spark-2.3.0`.

   > Your actual install path will differ — substitute your own
   > directory throughout these instructions. It's also common to
   > rename the extracted `spark-2.3.0-bin-hadoop2.7` folder to a
   > shorter `spark-2.3.0` for convenience.

3. Confirm the extraction worked:

   ```text
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
   ```

---

## 3. Set Up Convenience Scripts

Rather than exporting environment variables by hand every session,
create a small `zbin/` folder of scripts inside your Spark home.

1. Create a directory: `/home/alex/spark-2.3.0/zbin`

2. `zbin/env_setup.sh` — clears any stray Hadoop env vars and sets the
   Spark/Java/PATH variables you actually need:

   ```bash
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
   export JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk1.8.0_144.jdk/Contents/Home"
   #
   export PATH=.:$JAVA_HOME/bin:/Library/Frameworks/Python.framework/Versions/3.6/bin:$SPARK_HOME/sbin:$SPARK_HOME/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/opt/X11/bin:$PATH
   ```

3. `zbin/start-spark.sh` — sources the env script, clears any stale
   Derby metastore, and (re)starts the cluster:

   ```bash
   #
   export SPARK_HOME=/home/alex/spark-2.3.0
   #
   source $SPARK_HOME/zbin/env_setup.sh
   #
   rm -fr $SPARK_HOME/metastore_db
   #
   $SPARK_HOME/sbin/stop-all.sh
   $SPARK_HOME/sbin/start-all.sh
   ```

4. `zbin/stop-spark.sh`:

   ```bash
   #
   export SPARK_HOME=/home/alex/spark-2.3.0
   #
   source $SPARK_HOME/zbin/env_setup.sh
   #
   $SPARK_HOME/sbin/stop-all.sh
   ```

5. `zbin/start-pyspark-shell.sh`:

   ```bash
   #
   export SPARK_HOME=/home/alex/spark-2.3.0
   #
   source $SPARK_HOME/zbin/env_setup.sh
   #
   $SPARK_HOME/bin/pyspark
   ```

6. Make the scripts executable:

   ```bash
   chmod a+rx /home/alex/spark-2.3.0/zbin/*
   ```

7. Start your Spark cluster:

   ```bash
   /home/alex/spark-2.3.0/zbin/start-spark.sh
   ```

   (See [Troubleshooting](#troubleshooting) below if this fails with a
   `port 22: Connection refused` error.)

8. Verify the cluster is running by checking `http://localhost:8080`
   in your browser.

9. Stop it with `/home/alex/spark-2.3.0/zbin/stop-spark.sh`, and start
   it again any time with `start-spark.sh`.

### The full `spark-env.sh` reference

Spark also reads a `conf/spark-env.sh` file for cluster-wide settings
(separate from the `zbin/env_setup.sh` convenience script above,
which just sets up your shell for launching Spark). Copy the template
below into `$SPARK_HOME/conf/spark-env.sh` and edit it for your own
site:

```bash
#!/usr/bin/env bash

# This file is sourced when running various Spark programs.
# Copy it as spark-env.sh and edit that to configure Spark for your site.

# Options read when launching programs locally with
# ./bin/run-example or ./bin/spark-submit
# - HADOOP_CONF_DIR, to point Spark towards Hadoop configuration files
# - SPARK_LOCAL_IP, to set the IP address Spark binds to on this node
# - SPARK_PUBLIC_DNS, to set the public dns name of the driver program
# - SPARK_CLASSPATH, default classpath entries to append

# Options read by executors and drivers running inside the cluster
# - SPARK_LOCAL_IP, to set the IP address Spark binds to on this node
# - SPARK_PUBLIC_DNS, to set the public DNS name of the driver program
# - SPARK_CLASSPATH, default classpath entries to append
# - SPARK_LOCAL_DIRS, storage directories to use on this node for shuffle and RDD data
# - MESOS_NATIVE_LIBRARY, to point to your libmesos.so if you use Mesos

# Options read in YARN client mode
# - HADOOP_CONF_DIR, to point Spark towards Hadoop configuration files
# - SPARK_EXECUTOR_INSTANCES, Number of workers to start (Default: 2)
# - SPARK_EXECUTOR_CORES, Number of cores for the workers (Default: 1).
# - SPARK_EXECUTOR_MEMORY, Memory per Worker (e.g. 1000M, 2G) (Default: 1G)
# - SPARK_DRIVER_MEMORY, Memory for Master (e.g. 1000M, 2G) (Default: 512 Mb)
# - SPARK_YARN_APP_NAME, The name of your application (Default: Spark)
# - SPARK_YARN_QUEUE, The hadoop queue to use for allocation requests (Default: 'default')
# - SPARK_YARN_DIST_FILES, Comma separated list of files to be distributed with the job.
# - SPARK_YARN_DIST_ARCHIVES, Comma separated list of archives to be distributed with the job.

# Options for the daemons used in the standalone deploy mode
# - SPARK_MASTER_IP, to bind the master to a different IP address or hostname
# - SPARK_MASTER_PORT / SPARK_MASTER_WEBUI_PORT, to use non-default ports for the master
# - SPARK_MASTER_OPTS, to set config properties only for the master (e.g. "-Dx=y")
# - SPARK_WORKER_CORES, to set the number of cores to use on this machine
# - SPARK_WORKER_MEMORY, to set how much total memory workers have to give executors (e.g. 1000m, 2g)
# - SPARK_WORKER_PORT / SPARK_WORKER_WEBUI_PORT, to use non-default ports for the worker
# - SPARK_WORKER_INSTANCES, to set the number of worker processes per node
# - SPARK_WORKER_DIR, to set the working directory of worker processes
# - SPARK_WORKER_OPTS, to set config properties only for the worker (e.g. "-Dx=y")
# - SPARK_HISTORY_OPTS, to set config properties only for the history server (e.g. "-Dx=y")
# - SPARK_DAEMON_JAVA_OPTS, to set config properties for all daemons (e.g. "-Dx=y")
# - SPARK_PUBLIC_DNS, to set the public dns name of the master or workers

# Generic options for the daemons used in the standalone deploy mode
# - SPARK_CONF_DIR      Alternate conf dir. (Default: ${SPARK_HOME}/conf)
# - SPARK_LOG_DIR       Where log files are stored.  (Default: ${SPARK_HOME}/logs)
# - SPARK_PID_DIR       Where the pid file is stored. (Default: /tmp)
# - SPARK_IDENT_STRING  A string representing this instance of spark. (Default: $USER)
# - SPARK_NICENESS      The scheduling priority for daemons. (Default: 0)

export SPARK_ROOT_DIR=/opt/spark-1.3.0
export SPARK_WORKER_INSTANCES=1
export SPARK_WORKER_MEMORY=2g
export SPARK_WORKER_CORES=2
export SPARK_WORKER_DIR=$SPARK_ROOT_DIR/work
export SPARK_DAEMON_MEMORY=2048m
export SPARK_HOME=$SPARK_ROOT_DIR
export PYSPARK_PYTHON=/usr/bin/python3
#
export SPARK_MASTER_IP=localhost
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_144.jdk/Contents/Home
```

(Adjust `SPARK_ROOT_DIR`, `PYSPARK_PYTHON`, and `JAVA_HOME` to match
your own install paths and Python version.)

---

## 4. Start the PySpark Shell

```bash
/home/alex/spark-2.3.0/zbin/start-pyspark-shell.sh
```

You should see the Spark banner and a `>>>` prompt, with `spark`
already available as your `SparkSession` and `sc` as your
`SparkContext`:

```text
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.3.0
      /_/

SparkSession available as 'spark'.
>>> spark
<pyspark.sql.session.SparkSession object at 0x10193da50>
>>> sc = spark.sparkContext
>>> sc
<SparkContext master=local[*] appName=PySparkShell>
```

Try a quick RDD example — group-by-key style aggregation on a small
list of `(word, count)` pairs:

```python
>>> mylist = [('fox', 1), ('fox', 2), ('cat', 10), ('cat', 20), ('cat', 30), ('fox', 3)]
>>> rdd = sc.parallelize(mylist)
>>> rdd.count()
6

>>> frequency = rdd.reduceByKey(lambda x, y: x + y)
>>> frequency.collect()
[('fox', 6), ('cat', 60)]

>>> grouped = rdd.groupByKey()
>>> grouped.mapValues(lambda values: list(values)).collect()
[('fox', [1, 2, 3]), ('cat', [10, 20, 30])]

>>> rdd.groupByKey().mapValues(lambda values: sum(values)).collect()
[('fox', 6), ('cat', 60)]
```

---

## 5. Checking Your Installation

A quick end-to-end sanity check, useful any time you set up a new
machine or a new Spark version:

```text
% echo $JAVA_HOME
/Library/Java/JavaVirtualMachines/jdk-11.jdk/Contents/Home

% java --version
java 11.0.19 2023-04-18 LTS

% python3 --version
Python 3.12.0

% cd spark-3.5.3
% ./bin/pyspark
...
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.3
      /_/

Spark context Web UI available at http://<your-ip>:4040
SparkSession available as 'spark'.
```

Then confirm `spark`/`sc` both report the version you expect and that
a basic RDD works:

```python
>>> spark.version
'3.5.3'
>>> sc.version
'3.5.3'

>>> some_data = [1, 3, 5, 6, 8, 20, 30, 90]
>>> rdd = sc.parallelize(some_data)
>>> rdd.count()
8
>>> rdd.collect()
[1, 3, 5, 6, 8, 20, 30, 90]
```

---

## 6. Alternative: Running Spark on a Cloud VM (Google Cloud / AWS)

If you'd rather not install Spark locally at all, you can spin up an
Ubuntu VM on Google Cloud or AWS and install Spark there instead.

> **Free-tier note:** Amazon gives you a year of free-tier cloud
> services, but with limited VM size/memory options. Google gives you
> $300 of free credit for 2 months, after which you'll be billed for
> whatever you keep running — for a class assignment, a minimal VM
> typically costs well under $1/day beyond the free period. Keep your
> configuration choices inside the free tier and remember to **stop**
> your instance when you're done with it each session.

### Option A: Google Cloud

1. Go to [cloud.google.com](https://cloud.google.com/) and sign up
   with a Google account (a credit card is required, even for the
   free tier).
2. Click **Go to console** → create a new project (e.g.
   `big-data-vm`).
3. Under **Compute Engine**, click **Create Instance**.
4. Under **Firewall**, allow HTTP and HTTPS access.
5. Pick a small machine type (e.g. 1 vCPU / 4 GB memory) — check the
   estimated price shown on the right against your free-tier budget.
6. Under **Boot disk**, choose an Ubuntu LTS image.
7. Click **Create** and wait for the instance to start.
8. To connect, click the **SSH** dropdown next to your instance and
   choose **Open in browser window** — this opens a terminal directly
   in your browser, no local SSH client needed.
9. Once connected, follow the Ubuntu/Spark install steps in
   [Alternative Path B](spark_on_windows.md#alternative-path-b-run-spark-inside-a-linux-vm-virtualbox--ubuntu)
   starting from "download the latest Spark version" — everything
   after that point is identical whether you're on a local VirtualBox
   VM or a cloud VM.

### Option B: Amazon AWS (EC2)

1. Go to [aws.amazon.com](http://aws.amazon.com/) and sign in (or
   create an account with a credit card).
2. Go to **EC2** → **Instances** → **Launch Instance**.
3. Choose an Ubuntu image, and confirm it's labeled **Free tier
   eligible**.
4. Choose instance type `t2.micro` (the free-tier size).
5. Leave "Configure Instance Details" at its defaults.
6. Leave storage at the default 8GB.
7. Tag your instance with any name you like (e.g. `big_data_sample_VM`).
8. Under **Security Groups**, add rules for HTTP and HTTPS with
   source "Anywhere."
9. Click **Launch**, then create (or reuse) a key-pair `.pem` file —
   **save it somewhere you'll remember**, you'll need it to connect.
10. Wait for the instance to launch. When you're done using it each
    session, stop it via **Actions → Instance State → Stop** (you can
    start it again later — this avoids burning through your free
    usage hours).

**Connecting to your EC2 instance:**

* **macOS/Linux** — connect directly from your terminal:

  ```bash
  ssh -i "<name of .pem file>.pem" ubuntu@<your-instance-public-dns>
  ```

  (Find your instance's exact address by selecting it in the AWS
  console and clicking **Connect** → **A standalone SSH client**.)

* **Windows** — see
  [`spark_on_windows.md`](spark_on_windows.md#connecting-to-an-aws-ec2-instance-from-windows)
  for a Windows-specific SSH client walkthrough.

Once connected, follow the same Ubuntu/Spark install steps referenced
above (Google Cloud Option A, step 9) to install and run Spark on your
instance.

---

## Troubleshooting

### `start-all.sh` fails with "Connection refused" on port 22

If you run `start-all.sh` (or your own `start-spark.sh` wrapper) and see:

```text
starting org.apache.spark.deploy.master.Master, logging to ...
localhost: ssh: connect to host localhost port 22: Connection refused
```

Spark's standalone-cluster start script uses `ssh` to `localhost` even
for a single-machine setup, so you need an SSH *server* running
locally, not just an SSH client.

* **On macOS:** enable **Remote Login**. Go to **System Preferences**
  → **Sharing**, and turn on **Remote Login**.

* **On Linux/Ubuntu** (including inside a VirtualBox VM or a cloud
  VM): install and start an SSH server:

  ```bash
  sudo apt-get install ssh
  service ssh restart
  ```

  If the problem persists, try removing and reinstalling OpenSSH
  entirely:

  ```bash
  sudo apt-get remove openssh-client openssh-server
  sudo apt-get install openssh-client openssh-server
  ```

### Too much INFO-level logging in the PySpark shell

Spark logs a lot of `INFO`-level noise by default. To quiet it down:

```bash
cd $SPARK_HOME/conf
cp log4j2.properties.template log4j2.properties
vi log4j2.properties
```

Change:

```text
rootLogger.level = info
```

to:

```text
rootLogger.level = error
```

> Spark switched from `log4j.properties` (`log4j.rootCategory = INFO,
> console`) to `log4j2.properties` (`rootLogger.level = info`) around
> Spark 3.3. If you're on an older Spark release and `conf/` has a
> `log4j.properties.template` instead of `log4j2.properties.template`,
> copy that file to `log4j.properties` and change its
> `log4j.rootCategory = INFO, console` line to
> `log4j.rootCategory = ERROR, console` instead.
