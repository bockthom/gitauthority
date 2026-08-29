# Implication Evaluation

Scripts for extracting commit data from a git repository and classifying developers into core and peripheral contributors using a 20/80 heuristic. GitAuthority is used for de-aliasing authors; its config file is passed through directly, so different GitAuthority configurations can be compared in terms of their effect on core/peripheral developer classification outcomes.

## Setup

**Requirements:** Python 3.7+

Install the dependencies:

```bash
pip install -r requirements.txt
```

Note that [gitAuthority](../gitAuthority.py) must also be set up (see the root README) as it is called by `extract_commits.py`.

## Workflow

```
extract_commits.py  →  classify_developers.py
```

`extract_commits.py` and `classify_developers.py` share the same `--output-dir` so output from the first is automatically picked up by the second. Run the two scripts for each GitAuthority configuration (e.g. privacy-aware and privacy-agnostic) into separate output directories.

---

## `extract_commits.py`

Extracts all commits from a git repository, de-aliases authors using [gitAuthority](../gitAuthority.py), and writes the result to a CSV file.

### Usage

```
python extract_commits.py <project> [--output-dir <dir>] [--config <file>]
```

### Arguments

| Argument | Description |
|---|---|
| `project` | Path to the git repository |
| `--output-dir`, `-d` | Directory for output files (default: current directory) |
| `--config`, `-c` | Path to a gitAuthority config file; passed through to gitAuthority (default: `merge_config.txt` next to `gitAuthority.py`) |

### Output CSV columns

| Column | Description |
|---|---|
| `project` | Project name (basename of the repository path) |
| `dealiased_author` | Canonical author identity in `Name <email>` format |
| `commit_hash` | Full SHA-1 commit hash |
| `commit_date` | Commit date in git default format (`Mon Jan 01 12:00:00 2024 +0000`) |

Output is written to `<output-dir>/commit_analysis_<project>.csv`.

### Example

```
python extract_commits.py /repos/myproject --output-dir ./results --config merge_config.txt
```

---

## `classify_developers.py`

Reads the CSV produced by `extract_commits.py` and classifies developers into **core** and **peripheral** using a 20/80 heuristic: the smallest group of developers responsible for at least 80% of commits are labelled core.

### Usage

```
python classify_developers.py <project> [--output-dir <dir>] [--year <year>]
```

### Arguments

| Argument | Description |
|---|---|
| `project` | Project name (must match the name used in `extract_commits.py`) |
| `--output-dir`, `-d` | Directory to read input from and write output to (default: current directory) |
| `--year` | Restrict classification to commits from this calendar year |

Reads `<output-dir>/commit_analysis_<project>.csv` and writes to `<output-dir>/classification_<project>[_<year>].csv`.

### Output

Descriptive statistics are always printed to stdout:

```
20/80 Core Developer Classification
Total developers     : 42
  Core developers    : 8 (19.0%)
  Peripheral devs    : 34 (81.0%)
Total commits        : 1530
  Core commits       : 1228 (80.3%)
  Peripheral commits : 302 (19.7%)
```

Two CSV files are written:

**`classification_<project>.csv`** — per-developer classification:

| Column | Description |
|---|---|
| `dealiased_author` | Canonical author identity |
| `commit_count` | Number of commits by this author (in the filtered period) |
| `cumulative_pct` | Cumulative commit share up to and including this author (%) |
| `is_core` | `True` if the author is a core developer |

**`classification_<project>_stats.csv`** — aggregate statistics:

| Metric | Description |
|---|---|
| `total_developers` | Total number of unique developers |
| `core_developers` | Number of core developers |
| `peripheral_developers` | Number of peripheral developers |
| `total_commits` | Total commit count |
| `core_commits` | Commits attributed to core developers |
| `peripheral_commits` | Commits attributed to peripheral developers |
| `core_developer_pct` | Core developers as a percentage of all developers |
| `core_commit_pct` | Core commits as a percentage of all commits |

### Example

```
# Classify over all time
python classify_developers.py myproject --output-dir ./results

# Classify based on commits from 2023 only
python classify_developers.py myproject --output-dir ./results --year 2023
```

