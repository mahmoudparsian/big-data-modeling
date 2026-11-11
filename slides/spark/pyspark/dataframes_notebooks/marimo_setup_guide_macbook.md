# Complete Setup Guide: Installing Marimo Notebooks on macOS

This step-by-step guide walks you through setting up Python, `marimo`, PySpark, and DuckDB on **macOS** (MacBook / Mac Desktop).

---

## Step 1: Install Python & Java (via Homebrew)

Before installing Marimo, you need **Python 3.9+** and **OpenJDK 17** (for PySpark).

### Option A: Using Homebrew (Recommended)

1. Open **Terminal** (press `Cmd + Space`, type `Terminal`, and press `Enter`).
2. If you don't have Homebrew installed, paste this in Terminal:
   ```bash
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
   ```
3. Install Python and OpenJDK:
   ```bash
   brew install python openjdk@17
   ```

### Option B: Official Python Installer

1. Go to the [Python Downloads for macOS](https://www.python.org/downloads/macos/).
2. Download and run the macOS 64-bit universal installer (`.pkg`).
3. Follow installer instructions.

---

## Step 2: Verify Installation

In your Terminal, run:

```bash
python3 --version
java -version
```

*(Note: macOS requires typing `python3` instead of `python`).*

---

## Step 3: Set Up a Virtual Environment

Using a virtual environment keeps your project packages clean and isolated.

Open **Terminal** and run:

```bash
# 1. Create a project folder
mkdir marimo_projects
cd marimo_projects

# 2. Create a virtual environment named 'venv'
python3 -m venv venv

# 3. Activate the virtual environment
source venv/bin/activate
```

*(You should now see `(venv)` at the beginning of your command prompt line).*

---

## Step 4: Install Marimo & Required Data Libraries

With `(venv)` active in Terminal, run:

```bash
pip install --upgrade pip
pip install marimo pyspark duckdb pandas polars pyarrow
```

---

## Step 5: Running & Editing Marimo Notebooks

### 1. Launch a New Blank Notebook
```bash
marimo edit
```

### 2. Edit an Existing Marimo Python File
Navigate to the directory containing your file and run:
```bash
marimo edit pyspark_sql_csv_tutorial.py
```
Marimo will automatically open your default browser at `http://localhost:2718`.

### 3. Run Notebook as an Interactive Web App
```bash
marimo run pyspark_sql_csv_tutorial.py
```

---

## Quick Command Reference (macOS)

| Action | Command |
| :--- | :--- |
| **Check Python** | `python3 --version` |
| **Create Venv** | `python3 -m venv venv` |
| **Activate Venv** | `source venv/bin/activate` |
| **Deactivate Venv** | `deactivate` |
| **Launch Editor** | `marimo edit notebook.py` |