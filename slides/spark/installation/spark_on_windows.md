# Installing Spark & PySpark on Windows

There are three ways to get Spark/PySpark running on a Windows
machine, roughly ordered from "closest to how Linux/macOS users do
it" to "simplest, if all you need is PySpark from Python":

* **[Path A](#path-a-install-natively-on-windows-no-vm)** — install Java, Spark, and Hadoop's `winutils.exe` directly on Windows. Most control, most manual steps.
* **[Path B](#alternative-path-b-run-spark-inside-a-linux-vm-virtualbox--ubuntu)** — run a Ubuntu VM inside VirtualBox and follow the standard Linux install instead. Recommended if Path A gives you trouble with environment variables or `winutils`.
* **[Path C](#alternative-path-c-pip-install-pyspark-simplest)** — just `pip install pyspark`. The fastest path if you only need the Python API and don't need a standalone cluster (`start-all.sh`/`stop-all.sh`) or the Spark shell binaries.

> Installing on **macOS or Linux**? See
> [`spark_on_macbook.md`](spark_on_macbook.md) instead — it also
> covers running Spark on a Google Cloud / AWS VM, which works from
> Windows too (see [below](#connecting-to-an-aws-ec2-instance-from-windows)).

---

## Path A: Install Natively on Windows (no VM)

### 1. Install Java

1. Download and install the JDK from the
   [Oracle downloads page](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html).
2. Add a `JAVA_HOME` **system variable** pointing at your JDK install
   directory (e.g. `C:\Program Files\Java\jdk1.8.0_131`).
3. Edit the `Path` system variable and add `%JAVA_HOME%\bin` so the
   command line can find `java`.
4. Verify: open a command prompt and run `java --version` (or `java
   -version`).

### 2. Install Python (and, optionally, Anaconda + Jupyter)

* Plain Python: download the current Python 3 release from
  [python.org/downloads](https://www.python.org/downloads/) and
  verify with `python --version`.
* **Or**, for a batteries-included data-science setup, install
  [Anaconda](https://www.anaconda.com/download/), which bundles
  Python + Jupyter. Locate `python.exe` and `jupyter.exe` under your
  Anaconda install (typically
  `C:\Users\<user_name>\Anaconda3\python.exe` and
  `C:\Users\<user_name>\Anaconda3\Scripts\jupyter.exe`) — you'll need
  these paths in step 5 below.

### 3. Download and Extract Spark

1. Download a pre-built Spark release from the
   [Apache Spark downloads page](http://spark.apache.org/downloads.html).
2. Extract the `.tgz` (you may need 7-Zip or WinRAR, and may need to
   extract twice if you end up with an intermediate `.tar` file) to
   somewhere like `C:\Spark\spark-2.2.0-bin-hadoop2.7`.

   > ⚠️ **Important:** make sure no folder in the extraction path
   > contains a space (e.g. avoid `Program Files`). Use
   > `Program_Files` or `ProgramFiles` instead — Spark's scripts can
   > choke on spaces in the path.

### 4. Install Hadoop's `winutils.exe`

Spark's Windows build still needs a small Hadoop compatibility binary:

1. Create a folder, e.g. `C:\winutils\bin`.
2. Download `winutils.exe` for your Hadoop version from
   [github.com/steveloughran/winutils](https://github.com/steveloughran/winutils/tree/master/hadoop-2.7.1/bin)
   and place it in `C:\winutils\bin`.

### 5. Set Environment Variables

Go to **Control Panel → System and Security → System → Advanced
System Settings → Environment Variables**, and add these **user**
variables:

| Variable | Example value |
|---|---|
| `JAVA_HOME` | `C:\Program Files\Java\jdk1.8.0_131` |
| `HADOOP_HOME` | `C:\winutils` |
| `SPARK_HOME` | `C:\spark\spark-2.2.0-bin-hadoop2.7` |
| `SCALA_HOME` *(optional, only if you installed Scala/sbt)* | `C:\Program Files (x86)\sbt` |
| `PYSPARK_PYTHON` | path to your `python.exe` |
| `PYSPARK_DRIVER_PYTHON` *(optional, to launch Jupyter instead of the plain shell)* | path to your `jupyter.exe` |
| `PYSPARK_DRIVER_PYTHON_OPTS` *(pairs with the variable above)* | `notebook` |

Then edit the `Path` system variable and append the `bin` directory
for **each** of: `winutils`, Scala (if installed), Spark, Java,
Python, and Jupyter. Double-check `winutils`'s `bin` folder in
particular is on the path, since it's easy to miss.

### 6. Run Spark

1. Open a command prompt, `cd` to your Spark install's `bin`
   directory, and run:

   ```text
   spark-shell
   ```

   You should see `Spark session available as 'spark'` — that
   confirms Spark itself is working.

2. Open another command prompt and run:

   ```text
   pyspark
   ```

   If you set `PYSPARK_DRIVER_PYTHON`/`PYSPARK_DRIVER_PYTHON_OPTS` to
   launch Jupyter, this opens a Jupyter Notebook already connected to
   PySpark.

3. **(Optional) Start a standalone cluster.** From the same `bin`
   directory:

   ```text
   spark-class org.apache.spark.deploy.master.Master
   ```

   Then open `http://localhost:8080` in a browser — you should see
   the Spark master's web UI confirming it's running.

### Common errors

* **`Failed to locate the winutils binary in the hadoop binary path:
  java.io.IOException: Could not locate executable
  null\bin\winutils.exe`** — this is harmless if you aren't actually
  using HDFS/Hadoop for storage; PySpark will still run local jobs
  fine. If it bothers you, double check `HADOOP_HOME` and the `Path`
  entry for `winutils\bin` from step 4/5.

### Optional: Scala + Eclipse (for Java/Scala development)

If you'll be writing Spark programs in Scala or Java rather than just
PySpark, you may also want:

* **Scala**: download the current installer from
  [scala-lang.org/download](https://www.scala-lang.org/download/),
  install it, then set a `SCALA_HOME` user variable and add
  `%SCALA_HOME%\bin` to `Path`.
* **Eclipse**: download from [eclipse.org/downloads](https://eclipse.org/downloads/),
  set an `ECLIPSE_HOME` variable pointing at the install directory,
  and (for older Eclipse versions without a `bin` folder) add its
  install directory directly to `Path`.

This step is optional and not needed for PySpark-only work.

---

## Alternative Path B: Run Spark Inside a Linux VM (VirtualBox + Ubuntu)

If the native Windows path above gives you trouble (environment
variables, `winutils`, path-with-spaces issues), running Spark inside
a real Linux VM sidesteps all of it — you just follow the standard
Linux install once you're inside the VM.

### 1. Set Up the VM

1. Download and install
   [VirtualBox](https://www.virtualbox.org/wiki/Downloads).
2. Download an [Ubuntu Desktop image](http://releases.ubuntu.com/16.04/)
   (any reasonably current LTS release works).
3. Follow a guide like
   [wikiHow: Install Ubuntu on VirtualBox](http://www.wikihow.com/Install-Ubuntu-on-VirtualBox)
   to create the VM and install Ubuntu on it.

### 2. Install Java on Ubuntu

Open a terminal inside the VM:

```bash
su root                      # you may need to reset the root password first, see below
sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer
sudo apt-get install default-jre
sudo apt-get install default-jdk
```

> If you're locked out of the root account on a fresh Ubuntu install,
> see a guide like
> [linuxconfig.org: reset lost root password on Ubuntu](https://linuxconfig.org/how-to-reset-lost-root-password-on-ubuntu-16-04-xenial-xerus-linux).

If the `apt-get install` for Java fails or times out (Oracle
occasionally moves the download), install manually instead:

```bash
# download the appropriate .tar.gz from Oracle's Java archive, then:
tar -xvf jdk-8-linux-x64.tar.gz          # or the 32-bit .tar.gz
sudo mkdir -p /usr/lib/jvm
sudo mv ./jdk1.8.0 /usr/lib/jvm/
```

Then set `JAVA_HOME` for the whole system by editing two files:

```bash
sudo vi /etc/environment
```
```text
export JAVA_HOME=/usr/lib/jvm/
```
```bash
sudo vi /etc/profile
```
```text
JAVA_HOME=/usr/lib/jvm/
PATH=$PATH:$HOME/bin:$JAVA_HOME/bin
export JAVA_HOME
export PATH
```

(In `vi`, press `Esc` then type `:wq!` to save and quit.)

### 3. Download, Extract, and Run Spark

```bash
# download a "Pre-built for Apache Hadoop 2.7 and later" release from
# http://spark.apache.org/downloads.html, then:
tar zvfx spark-2.2.1-bin-hadoop2.7.tgz
cd spark-2.2.1-bin-hadoop2.7
cd sbin
./stop-all.sh
./start-all.sh
```

Check `http://localhost:8080` in a browser inside the VM to confirm
the master started. If you instead see:

```text
localhost: ssh: connect to host localhost port 22: Connection refused
```

see the [Troubleshooting](#troubleshooting) section below — this is
the same issue macOS/Linux users hit, with the same fix.

Then start PySpark:

```bash
cd ..                    # back to the spark-2.2.1-bin-hadoop2.7 folder
./bin/pyspark
```

### 4. Quiet the Logging (optional, but recommended)

```bash
cd conf
cp log4j2.properties.template log4j2.properties
vi log4j2.properties
```

Change `rootLogger.level = info` to `rootLogger.level = error`.

> On Spark releases older than ~3.3, look for `log4j.properties.template`
> instead and change `log4j.rootCategory = INFO, console` to
> `log4j.rootCategory = ERROR, console`.

### 5. Continuing on a Cloud VM Instead

Once you're comfortable with these Ubuntu steps, you can run the exact
same "download, extract, `start-all.sh`, quiet the logging" sequence
(steps 3–4 above) on a **Google Cloud** or **AWS** Ubuntu VM instead of
a local VirtualBox VM — see
[`spark_on_macbook.md`'s Cloud VM section](spark_on_macbook.md#6-alternative-running-spark-on-a-cloud-vm-google-cloud--aws)
for how to provision the instance itself.

#### Connecting to an AWS EC2 instance from Windows

Once your EC2 instance is running (see the AWS steps in
`spark_on_macbook.md`), connect to it with an SSH client — Windows
doesn't have `ssh` built into `cmd` the way macOS/Linux terminals do,
so use one of:

* A GUI SSH client such as PuTTY (convert your `.pem` key to PuTTY's
  `.ppk` format with PuTTYgen first), or
* The AWS Console's **Connect → EC2 Instance Connect** browser-based
  terminal (no local SSH client needed at all), or
* Follow a walkthrough video such as
  [this one](https://www.youtube.com/watch?v=8Dsq4MeVh8M).

---

## Alternative Path C: `pip install pyspark` (simplest)

If you only need the PySpark Python API — no standalone cluster
scripts, no Spark shell binaries — this is by far the fastest path.

1. **Install Python**: download from
   [python.org](https://www.python.org/downloads/) and make sure to
   check **"Add Python to PATH"** during installation. Verify with
   `python --version`.

2. **Install the JDK** (Spark still needs a JVM under the hood): same
   as [Path A, step 1](#1-install-java) above.

3. **Install PySpark via pip:**

   ```text
   pip install pyspark
   ```

4. **(Optional) Set up `winutils`** if you plan to read/write local
   files in ways that touch Hadoop's filesystem layer — see
   [Path A, step 4](#4-install-hadoops-winutilsexe).

5. **Verify the installation** — open a Python shell or Jupyter
   notebook and run:

   ```python
   from pyspark.sql import SparkSession

   spark = SparkSession.builder \
       .master("local") \
       .appName("PySpark Installation Test") \
       .getOrCreate()

   df = spark.createDataFrame([(1, "Hello"), (2, "World")], ["id", "message"])
   df.show()
   ```

   ```text
   +---+-------+
   | id|message|
   +---+-------+
   |  1|  Hello|
   |  2|  World|
   +---+-------+
   ```

**A few things to keep in mind with this approach:**
* Make sure your `pip install pyspark` version matches whatever Spark
  cluster (if any) you'll eventually connect to.
* Restart your command prompt after changing any environment
  variables — Windows won't pick up changes in an already-open shell.
* Check the
  [official PySpark documentation](https://spark.apache.org/docs/latest/api/python/index.html)
  for the current release, since installation details evolve.

---

## Troubleshooting

### `start-all.sh` fails with "Connection refused" on port 22 (inside a Linux VM)

See [`spark_on_macbook.md`'s Troubleshooting section](spark_on_macbook.md#start-allsh-fails-with-connection-refused-on-port-22) —
the fix (`sudo apt-get install ssh; service ssh restart`, or removing
and reinstalling `openssh-client`/`openssh-server`) is identical
whether you hit this on a native Linux machine, inside a VirtualBox
Ubuntu VM, or on a cloud VM.
