# Git and GitHub — A Short Guide for ISBA-2413

This guide has one goal: help you get the course repository onto your
laptop, and keep it up to date all quarter.

Course repository: **https://github.com/mahmoudparsian/big-data-modeling**

You do **not** need to be a Git expert for this course. Sections 1–5 are
enough. The rest is there when you are curious.

---

## 1. What is Git?

**Git is a program that saves the history of a folder of files.**

Think of it like "Track Changes" in Microsoft Word, but for a whole
project folder instead of one document. Git remembers every saved
version, who made it, and when.

Git runs on *your* computer. It does not need the internet.

Two words you will see:

* **Repository** (or **repo**) — a folder that Git is tracking.
* **Commit** — one saved snapshot of that folder.

---

## 2. What is GitHub?

**GitHub is a website that stores Git repositories online.**

Git is the tool. GitHub is the place where copies of repositories live
so people can share them.

| | Git | GitHub |
|---|---|---|
| What it is | Software on your laptop | A website (github.com) |
| Needs internet? | No | Yes |
| Made by | The Git project | GitHub, Inc. (owned by Microsoft) |

For this course, GitHub is where all slides, worked examples, PySpark
code, datasets, and the syllabus are published. When the instructor adds
new material, it appears there first.

---

## 3. Installing Git

First, check whether you already have it. Open a terminal and type:

```bash
git --version
```

If you see something like `git version 2.39.5`, you are done — skip to
Section 4.

### 3.1 On a MacBook (macOS) or Linux

Open the **Terminal** app (press `Command + Space`, type `Terminal`,
press `Return`), then choose **one** of these:

**Option A — Apple's developer tools (simplest, no extra software):**

```bash
xcode-select --install
```

A window will pop up. Click **Install** and wait a few minutes.

**Option B — Homebrew (if you already use `brew`):**

```bash
brew install git
```

**Option C — Official installer:** download it from
[git-scm.com/download/mac](https://git-scm.com/download/mac) and
double-click the package.

On Linux, use your package manager instead:

```bash
sudo apt install git        # Debian / Ubuntu
sudo dnf install git        # Fedora / RHEL
```

Then check it worked:

```bash
git --version
```

### 3.2 On Windows

**Option A — Official installer (recommended):**

1. Go to [git-scm.com/download/win](https://git-scm.com/download/win).
2. The download starts automatically. Run the `.exe` file.
3. Click **Next** on every screen — the default choices are fine.
4. Finish the installation.

This also installs **Git Bash**, a terminal window that understands the
same commands as a Mac or Linux terminal. Use Git Bash for everything in
this guide.

To open it: click **Start**, type `Git Bash`, press **Enter**.

**Option B — From the command line (Windows 10/11):**

Open PowerShell and run:

```powershell
winget install --id Git.Git -e
```

Then close and reopen the terminal, and check:

```bash
git --version
```

### 3.3 One-time setup (both systems)

Tell Git who you are. Do this once, ever:

```bash
git config --global user.name "Your Name"
git config --global user.email "your_email@scu.edu"
```

---

## 4. Getting the Course Repository (Cloning)

**Cloning** means downloading a full copy of the repository, with its
history, onto your laptop.

Pick a folder to keep it in, then run:

```bash
cd ~/Desktop
git clone https://github.com/mahmoudparsian/big-data-modeling.git
```

That creates a folder named `big-data-modeling` on your Desktop. Go into
it and look around:

```bash
cd big-data-modeling
ls
```

You only clone **once** per computer. After that, you *refresh* it (next
section).

> **This repository is large.** It holds many slide decks (PDF and PPTX),
> so the first clone downloads several hundred megabytes and can take a
> few minutes. Be patient, and use a good network connection.
>
> **Faster option:** if you only want the current files and not the full
> history, add `--depth 1`:
>
> ```bash
> git clone --depth 1 https://github.com/mahmoudparsian/big-data-modeling.git
> ```
>
> This downloads much less. `git pull` still works normally afterward.

> **No Git? No problem.** You can also click the green **Code** button on
> the GitHub page and choose **Download ZIP**. But then you must
> re-download the whole thing every time something changes. Cloning is
> much easier over ten weeks.

---

## 5. Refreshing Your Copy (Getting New Material)

The instructor adds new files every week. To get them:

```bash
cd ~/Desktop/big-data-modeling
git pull
```

That is the whole command. `git pull` downloads what is new and leaves
everything else alone.

Run `git pull` **before every class**.

### 5.1 Important habit: do not edit files inside the clone

If you change a file that the instructor later changes too, `git pull`
will stop and complain about a *conflict*. This confuses everyone.

**The safe habit:** treat your `big-data-modeling` folder as read-only.
When you want to run or modify a PySpark example, **copy it** to your own
work folder first:

```bash
mkdir -p ~/Desktop/my_isba2413_work
cp ~/Desktop/big-data-modeling/slides/spark/pyspark/rdd_wordcount_demo/*.py \
   ~/Desktop/my_isba2413_work/
```

Then edit the copy. Your clone stays clean, and `git pull` always works.

### 5.2 If `git pull` fails

If you did edit files in the clone and `git pull` refuses to run, you
have two choices.

**Keep your changes** — save them aside, pull, then bring them back:

```bash
git stash
git pull
git stash pop
```

**Throw your changes away** — reset the folder to exactly match GitHub:

```bash
git reset --hard origin/master
git pull
```

⚠️ `git reset --hard` **permanently deletes** your edits in that folder.
Copy anything you want to keep somewhere else first.

If you are stuck, the simplest fix always works: delete the
`big-data-modeling` folder and clone it again (Section 4).

---

## 6. Basic Git Commands

These are the ones worth knowing. You will mostly use the first two.

| Command | What it does |
|---|---|
| `git clone <url>` | Download a repository for the first time |
| `git pull` | Get the newest changes from GitHub |
| `git status` | Show which files you changed |
| `git log --oneline` | List recent commits, one per line |
| `git diff` | Show exactly what you changed |
| `git add <file>` | Mark a file to be included in the next commit |
| `git commit -m "message"` | Save a snapshot of the marked files |
| `git push` | Upload your commits to GitHub |
| `git restore <file>` | Undo your changes to one file |

A few notes:

* `git add` + `git commit` + `git push` is the normal "save my work
  online" sequence, in that order.
* A **commit message** should say *what* you changed:
  `"add week 3 PySpark exercise"`, not `"stuff"`.
* This repository's default branch is named **`master`** — the main line
  of history. Newer repositories often call it `main` instead. Both are
  just names for the same idea.

---

## 7. Optional: Using Git for Your Own Work

You cannot `push` to the course repository — it is read-only for
students. But you can use Git for your own projects, and it is a good
habit for your career. Data and analytics teams run on Git.

Start tracking a folder of your own:

```bash
cd ~/Desktop/my_isba2413_work
git init
git add .
git commit -m "first version of my work"
```

To put it on GitHub:

1. Create a free account at [github.com](https://github.com).
2. Click **+** → **New repository**, give it a name, click
   **Create repository**.
3. Follow the "push an existing repository" commands GitHub shows you.

Two cautions for a big-data course:

* **Never commit credentials.** API keys, `.env` files, AWS keys, and
  passwords do not belong in a repository — public or private.
* **Never commit large datasets.** Git is built for text and code, not
  for gigabytes of Parquet. Commit the *code* that reads the data, and a
  small sample if you need one.

Employers can see public GitHub repositories. That is a feature, not a
bug — but only put work there that you are allowed to share.

---

## 8. Quick Troubleshooting

| Problem | Try this |
|---|---|
| `git: command not found` | Git is not installed — see Section 3 |
| `fatal: not a git repository` | You are in the wrong folder — `cd` into `big-data-modeling` first |
| `Your local changes would be overwritten` | See Section 5.2 |
| `Permission denied` / asks for a password on `push` | You cannot push to the course repo — that is expected |
| The clone is very slow | See the `--depth 1` option in Section 4 |
| Everything is broken | Delete the folder, clone again (Section 4) |

To see where you are, use `pwd` (print working directory). To list files,
use `ls`. These two commands solve most "I am lost" moments.

---

## 9. Learning More

* [GitHub's "Hello World" guide](https://docs.github.com/en/get-started/quickstart/hello-world) — 15 minutes, no install needed
* [Git official documentation](https://git-scm.com/doc)
* [Pro Git book](https://git-scm.com/book/en/v2) — free, complete, well written

---

## Summary — The Three Commands You Actually Need

```bash
# once, ever:
git clone https://github.com/mahmoudparsian/big-data-modeling.git

# before every class:
cd big-data-modeling
git pull
```

---
*ISBA-2413 — Big Data Modeling & Analytics — Fall 2026*
