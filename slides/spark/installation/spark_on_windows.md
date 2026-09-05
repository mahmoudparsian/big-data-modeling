# Installing Spark 4.2.0 & PySpark on Windows

There are three ways to get Spark 4.2.0 / PySpark running on Windows,
roughly ordered from "closest to how Linux/macOS users do it" to
"simplest, if all you need is PySpark from Python":

* **[Path A](#path-a-install-natively-on-windows-no-vm)** — install Java, Spark, and Hadoop's `winutils.exe` directly on Windows. Most control, most manual steps.
* **[Path B](#path-b-run-spark-inside-ubuntu-via-wsl2)** — run Ubuntu via WSL2 and follow the standard Linux install. Recommended if Path A gives you trouble with environment variables or `winutils`.
* **[Path C](#path-c-pip-install-pyspark-simplest)** — just `pip install pyspark==4.2.0`. The fastest path if you only need the Python API and don't need a standalone cluster or the Spark shell binaries.

> Installing on **macOS or Linux**? See
> [`spark_on_macbook.md`](spark_on_macbook.md) instead — it also
> covers running Spark on a Google Cloud / AWS VM, which works from
> Windows too (see [below](#connecting-to-an-aws-ec2-instance-from-windows)).

## Exact versions — do not substitute

| Component | Required version |
|---|---|
| Apache Spark | **4.2.0** (no other version) |
| Java (JDK) | **17 LTS** (Eclipse Temurin) |
| Python | **3.10 or newer** |

Spark 4.2.0 runs only on Java 17, 21, or 25. This guide standardizes
on **Java 17** because it's the most widely supported LTS release.
Do not use Java 8 or Java 11 — Spark 4.x does not support them.

---

## Path A: Install Natively on Windows (no VM)

### Step 1 — Install Java 17

1. Download **Eclipse Temurin JDK 17 (LTS)** for Windows x64 from
   [adoptium.net/temurin/releases/?version=17](https://adoptium.net/temurin/releases/?version=17)
   and run the `.msi` installer.
2. During install, enable the options **"Set JAVA_HOME variable"**
   and **"Add to PATH"** (Temurin's installer offers both as
   checkboxes) — this does steps 2–3 below for you automatically.
3. If you installed manually without those options, add a `JAVA_HOME`
   **system variable** pointing at the JDK folder (e.g.
   `C:\Program Files\Eclipse Adoptium\jdk-17.0.x-hotspot`), and add
   `%JAVA_HOME%\bin` to your `Path` system variable.
4. Verify in a **new** command prompt:

   ```text
   java -version
   ```

   ```text
   openjdk version "17.0.x" ...
   ```

### Step 2 — Install Python 3.10+

Download the current Python 3 release from
[python.org/downloads](https://www.python.org/downloads/). During
install, check **"Add python.exe to PATH"**. Verify:

```text
python --version
```

```text
Python 3.10.x
```

### Step 3 — Download and extract Spark 4.2.0

1. Download
   [`spark-4.2.0-bin-hadoop3.tgz`](https://downloads.apache.org/spark/spark-4.2.0/spark-4.2.0-bin-hadoop3.tgz)
   from the [Apache Spark downloads page](https://spark.apache.org/downloads.html)
   (older releases live at `archive.apache.org` instead — do not
   substitute a different Spark version).
2. Extract it (7-Zip or WinRAR; you may need to extract twice if you
   end up with an intermediate `.tar`) to `C:\spark\spark-4.2.0`.

   > ⚠️ **Important:** no folder in the extraction path may contain a
   > space (e.g. avoid `Program Files`). Spark's scripts can choke on
   > spaces in the path.

### Step 4 — Install Hadoop's `winutils.exe`

Spark's Windows build needs a small Hadoop compatibility binary:

1. Create a folder, e.g. `C:\hadoop\bin`.
2. Download `winutils.exe` (and `hadoop.dll`) matching a Hadoop 3.x
   release from
   [github.com/cdarlint/winutils](https://github.com/cdarlint/winutils)
   and place both files in `C:\hadoop\bin`.

### Step 5 — Set environment variables

Go to **Control Panel → System and Security → System → Advanced
System Settings → Environment Variables**, and add these **user**
variables (skip `JAVA_HOME` if Step 1 already set it):

| Variable | Value |
|---|---|
| `JAVA_HOME` | `C:\Program Files\Eclipse Adoptium\jdk-17.0.x-hotspot` |
| `HADOOP_HOME` | `C:\hadoop` |
| `SPARK_HOME` | `C:\spark\spark-4.2.0` |
| `PYSPARK_PYTHON` | path to your `python.exe` |

Then edit the `Path` system variable and append `%HADOOP_HOME%\bin`,
`%SPARK_HOME%\bin`, and `%JAVA_HOME%\bin` (skip any already added by
an installer).

### Step 6 — Run Spark and verify

1. Open a **new** command prompt and run:

   ```text
   pyspark
   ```

2. Confirm the banner shows **version 4.2.0**, and check it in Python:

   ```python
   >>> spark.version
   '4.2.0'
   ```

3. **(Optional)** Start a standalone master:

   ```text
   spark-class org.apache.spark.deploy.master.Master
   ```

   Open `http://localhost:8080` to confirm the master's web UI is running.

### Common errors

* **`Failed to locate the winutils binary in the hadoop binary path`**
  — harmless if you aren't using HDFS; PySpark still runs local jobs
  fine. If it bothers you, double-check `HADOOP_HOME` and the `Path`
  entry from Step 4/5.

---

## Path B: Run Spark Inside Ubuntu via WSL2

If Path A gives you trouble (environment variables, `winutils`,
path-with-spaces issues), WSL2 sidesteps all of it — you get a real
Ubuntu environment and follow the standard Linux install.

### Step 1 — Install WSL2 + Ubuntu

In an **administrator** PowerShell:

```powershell
wsl --install -d Ubuntu
```

Restart when prompted, then launch **Ubuntu** from the Start menu and
create a UNIX username/password.

### Step 2 — Install Java 17

Inside the Ubuntu terminal:

```bash
sudo apt-get update
sudo apt-get install -y openjdk-17-jdk
java -version
```

### Step 3 — Download, extract, and run Spark 4.2.0

```bash
curl -O https://downloads.apache.org/spark/spark-4.2.0/spark-4.2.0-bin-hadoop3.tgz
tar -xzf spark-4.2.0-bin-hadoop3.tgz
mv spark-4.2.0-bin-hadoop3 ~/spark-4.2.0
export SPARK_HOME=~/spark-4.2.0
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$JAVA_HOME/bin:$PATH
$SPARK_HOME/bin/pyspark
```

Confirm `spark.version` reports `'4.2.0'`. Add the three `export`
lines to `~/.bashrc` so they persist across sessions.

> **`Connection refused` on port 22 when running `start-all.sh`?**
> Install and start an SSH server:
> `sudo apt-get install ssh && sudo service ssh restart`.

### Step 4 — (Optional) Quiet the logging

```bash
cd $SPARK_HOME/conf
cp log4j2.properties.template log4j2.properties
```

Change `rootLogger.level = info` to `rootLogger.level = error` in
`log4j2.properties`.

### Step 5 — Continuing on a Cloud VM instead

Once comfortable with these Ubuntu steps, you can run the exact same
sequence (Steps 2–4 above) on a **Google Cloud** or **AWS** Ubuntu VM
instead of WSL2 — see
[`spark_on_macbook.md`'s Cloud VM section](spark_on_macbook.md#step-8--optional-run-on-a-cloud-vm-instead-google-cloud--aws)
for how to provision the instance itself.

#### Connecting to an AWS EC2 instance from Windows

Once your EC2 instance is running (see the AWS steps in
`spark_on_macbook.md`), connect with an SSH client:

* WSL2's Ubuntu terminal has `ssh` built in — just use the same
  `ssh -i "<key>.pem" ubuntu@<public-dns>` command as macOS/Linux.
* Or use the AWS Console's **Connect → EC2 Instance Connect**
  browser-based terminal (no local SSH client needed).

---

## Path C: `pip install pyspark` (simplest)

If you only need the PySpark Python API — no standalone cluster
scripts, no Spark shell binaries — this is the fastest path.

### Step 1 — Install Python 3.10+

Download from [python.org](https://www.python.org/downloads/) and
check **"Add python.exe to PATH"** during install. Verify with
`python --version`.

### Step 2 — Install Java 17

Same as [Path A, Step 1](#step-1--install-java-17) above.

### Step 3 — Install PySpark 4.2.0

```text
pip install pyspark==4.2.0
```

Pinning the version avoids accidentally installing a different Spark
release than the rest of the course uses.

### Step 4 — Verify

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("PySpark Installation Test") \
    .getOrCreate()

print(spark.version)   # must print 4.2.0

df = spark.createDataFrame([(1, "Hello"), (2, "World")], ["id", "message"])
df.show()
```

```text
4.2.0
+---+-------+
| id|message|
+---+-------+
|  1|  Hello|
|  2|  World|
+---+-------+
```

**Keep in mind:** restart your command prompt after changing
environment variables — Windows won't pick up changes in an
already-open shell.

---

## Troubleshooting

### `start-all.sh` fails with "Connection refused" on port 22 (inside WSL2)

See [`spark_on_macbook.md`'s Troubleshooting note](spark_on_macbook.md#step-6--optional-start-a-standalone-cluster) —
the fix (`sudo apt-get install ssh && sudo service ssh restart`) is
identical whether you hit this in WSL2 or on a native Linux/cloud VM.
