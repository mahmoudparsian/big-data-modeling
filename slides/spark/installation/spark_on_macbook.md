# Installing Spark 4.2.0 & PySpark on macOS / Linux (and Cloud VMs)

These instructions install **one exact, tested combination of versions**
on **macOS or Linux**, plus two cloud-VM alternatives (Google Cloud,
Amazon AWS) for when you'd rather run on a hosted Ubuntu machine than
your own laptop.

> Installing on **Windows without a VM**? See
> [`spark_on_windows.md`](spark_on_windows.md) instead.

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

## Step 1 — Install Java 17

**macOS (Homebrew):**

```bash
brew install openjdk@17
sudo ln -sfn "$(brew --prefix openjdk@17)/libexec/openjdk.jdk" \
  /Library/Java/JavaVirtualMachines/openjdk-17.jdk
```

**Linux (Ubuntu/Debian):**

```bash
sudo apt-get update
sudo apt-get install -y openjdk-17-jdk
```

**Verify** (both platforms):

```bash
java -version
```

```text
openjdk version "17.0.x" ...
```

---

## Step 2 — Install Python 3.10+

Verify the Python 3 already on your system is new enough:

```bash
python3 --version
```

```text
Python 3.10.x
```

If it's older than 3.10, install a current Python 3 from
[python.org/downloads](https://www.python.org/downloads/) (macOS) or
`sudo apt-get install python3` (Ubuntu/Debian).

---

## Step 3 — Download and extract Spark 4.2.0

```bash
curl -O https://downloads.apache.org/spark/spark-4.2.0/spark-4.2.0-bin-hadoop3.tgz
tar -xzf spark-4.2.0-bin-hadoop3.tgz
mv spark-4.2.0-bin-hadoop3 ~/spark-4.2.0
```

This makes `~/spark-4.2.0` your `SPARK_HOME`. Confirm the extraction worked:

```bash
ls ~/spark-4.2.0
```

```text
LICENSE  NOTICE  README.md  bin  conf  data  examples  jars  licenses  python  sbin  ...
```

> If [Apache's downloads page](https://spark.apache.org/downloads.html)
> ever stops serving 4.2.0 directly (older releases move to
> `archive.apache.org`), fetch it from
> `https://archive.apache.org/dist/spark/spark-4.2.0/spark-4.2.0-bin-hadoop3.tgz`
> instead — same file, same checksum. Do **not** download a different
> Spark version.

---

## Step 4 — Set environment variables

**macOS:**

```bash
export SPARK_HOME=~/spark-4.2.0
export JAVA_HOME=$(/usr/libexec/java_home -v17)
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$JAVA_HOME/bin:$PATH
```

**Linux:**

```bash
export SPARK_HOME=~/spark-4.2.0
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$JAVA_HOME/bin:$PATH
```

Add these three lines to your shell profile (`~/.zshrc` on macOS,
`~/.bashrc` on Linux) so they persist in every new terminal:

```bash
echo 'export SPARK_HOME=~/spark-4.2.0' >> ~/.zshrc
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v17)' >> ~/.zshrc
echo 'export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$JAVA_HOME/bin:$PATH' >> ~/.zshrc
```

(Use `~/.bashrc` in place of `~/.zshrc` on Linux.)

---

## Step 5 — Start the PySpark shell and verify

```bash
$SPARK_HOME/bin/pyspark
```

```text
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 4.2.0
      /_/

Using Python version 3.10.x
SparkSession available as 'spark'.
```

Confirm both `spark` and `sc` report **exactly 4.2.0**:

```python
>>> spark.version
'4.2.0'
>>> sc = spark.sparkContext
>>> sc.version
'4.2.0'
```

Then try a quick RDD sanity check:

```python
>>> mylist = [('fox', 1), ('fox', 2), ('cat', 10), ('cat', 20), ('cat', 30), ('fox', 3)]
>>> rdd = sc.parallelize(mylist)
>>> rdd.count()
6

>>> frequency = rdd.reduceByKey(lambda x, y: x + y)
>>> frequency.collect()
[('fox', 6), ('cat', 60)]
```

Exit the shell with `exit()` or `Ctrl-D`.

---

## Step 6 — (Optional) Start a standalone cluster

If you want the Spark master/worker web UI rather than just the shell:

```bash
$SPARK_HOME/sbin/start-all.sh
```

Check `http://localhost:8080` in your browser to confirm the master
and worker are running. Stop it when you're done:

```bash
$SPARK_HOME/sbin/stop-all.sh
```

> **`Connection refused` on port 22?** Spark's `start-all.sh` uses
> `ssh localhost` even for a single machine, so you need an SSH
> *server* running locally (not just a client):
> * **macOS:** System Preferences → **Sharing** → enable **Remote Login**.
> * **Linux:** `sudo apt-get install ssh && sudo service ssh restart`.

---

## Step 7 — (Optional) Quiet the shell's logging

Spark logs a lot of `INFO`-level noise by default:

```bash
cd $SPARK_HOME/conf
cp log4j2.properties.template log4j2.properties
```

Edit `log4j2.properties` and change:

```text
rootLogger.level = info
```

to:

```text
rootLogger.level = error
```

---

## Step 8 — (Optional) Run on a Cloud VM instead (Google Cloud / AWS)

If you'd rather not install Spark locally at all, spin up an Ubuntu
VM and repeat **Steps 1–7 above** on it — the install itself is
identical once you're connected.

> **Free-tier note:** AWS gives a year of free-tier service with
> limited VM sizes. Google gives $300 of credit for 2 months, after
> which you're billed — a minimal VM for coursework typically costs
> well under $1/day. Stop your instance when you're not using it.

**Google Cloud:**

1. Sign up at [cloud.google.com](https://cloud.google.com/) (a credit
   card is required, even for the free tier).
2. **Go to console** → create a new project.
3. **Compute Engine** → **Create Instance**.
4. Under **Firewall**, allow HTTP and HTTPS.
5. Pick a small machine type (e.g. 1 vCPU / 4 GB) and an **Ubuntu
   LTS** boot disk.
6. Click **Create**, then connect via the **SSH** dropdown → **Open
   in browser window** (no local SSH client needed).
7. Once connected, follow Steps 1–7 above.

**Amazon AWS (EC2):**

1. Sign in at [aws.amazon.com](http://aws.amazon.com/).
2. **EC2** → **Instances** → **Launch Instance**.
3. Choose an **Ubuntu** image labeled **Free tier eligible**, instance
   type `t2.micro`.
4. Leave instance details and the default 8 GB storage as-is.
5. Under **Security Groups**, allow HTTP and HTTPS from "Anywhere."
6. **Launch**, then create (or reuse) a `.pem` key pair — save it
   somewhere you'll remember.
7. Connect from your terminal:

   ```bash
   ssh -i "<your-key>.pem" ubuntu@<your-instance-public-dns>
   ```

   (Find the exact address via **Connect** → **A standalone SSH
   client** in the AWS console. Windows users without a terminal
   `ssh` client: see
   [`spark_on_windows.md`](spark_on_windows.md#connecting-to-an-aws-ec2-instance-from-windows).)

8. Once connected, follow Steps 1–7 above.
9. Stop the instance via **Actions → Instance State → Stop** when
   you're done for the session (you can restart it later).
