# Complete Setup Guide: Installing Marimo Notebooks on Windows

This step-by-step guide walks you through setting up Python, `marimo`, PySpark, and DuckDB on **Windows** operating systems.

---

## Step 1: Install Python

Before installing Marimo, you need **Python 3.9 or higher** installed.

1. **Download Python:**
   * Go to the official [Python Downloads for Windows](https://www.python.org/downloads/windows/).
   * Download the latest stable Python 3 release (e.g., Python 3.11 or 3.12).

2. **Run the Installer (CRITICAL):**
   * Double-click the downloaded `.exe` file.
   * **⚠️ Check the box that says "Add python.exe to PATH"** at the bottom of the first setup screen.
   * Click **Install Now**.

3. **Verify Installation:**
   * Open **Command Prompt** (press `Win + R`, type `cmd`, and hit `Enter`).
   * Type the following command and press `Enter`:
     ```cmd
     python --version
     ```
   * You should see `Python 3.x.x` printed.

---

## Step 2: Install Java OpenJDK (Required for PySpark)

PySpark requires Java to run its local engine.

1. Download **Eclipse Temurin (OpenJDK 17)** from [Adoptium.net](https://adoptium.net/).
2. Run the `.msi` installer and accept default settings (ensure "Set JAVA_HOME variable" is enabled).
3. Verify in Command Prompt:
   ```cmd
   java -version
   ```

---

## Step 3: Set Up a Virtual Environment

Using a virtual environment keeps your data science projects organized and clean.

Open **Command Prompt** and run:

```cmd
:: 1. Create a project folder
mkdir marimo_projects
cd marimo_projects

:: 2. Create a virtual environment named 'venv'
python -m venv venv

:: 3. Activate the virtual environment
venv\Scripts\activate
```

*(You should now see `(venv)` at the beginning of your prompt line).*

---

## Step 4: Install Marimo & Required Data Libraries

With `(venv)` active in Command Prompt, run:

```cmd
python -m pip install --upgrade pip
pip install marimo pyspark duckdb pandas polars pyarrow
```

---

## Step 5: Running & Editing Marimo Notebooks

### 1. Launch a New Blank Notebook
```cmd
marimo edit
```

### 2. Edit an Existing Marimo Python File
Navigate to the directory containing your file and run:
```cmd
marimo edit pyspark_sql_csv_tutorial.py
```
Marimo will automatically open a browser window at `http://localhost:2718`.

### 3. Run Notebook as an Interactive Web App
```cmd
marimo run pyspark_sql_csv_tutorial.py
```

---

## Quick Command Reference (Windows)

| Action | Command |
| :--- | :--- |
| **Check Python** | `python --version` |
| **Create Venv** | `python -m venv venv` |
| **Activate Venv** | `venv\Scripts\activate` |
| **Deactivate Venv** | `deactivate` |
| **Launch Editor** | `marimo edit notebook.py` |