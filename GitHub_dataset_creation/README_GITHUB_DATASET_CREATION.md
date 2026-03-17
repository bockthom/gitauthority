# GitHub Ground-Truth Dataset Creation

This directory contains scripts for processing repository-to-author (p2a) data from World of Code, identifying single-author repositories, checking for organization accounts on GitHub, and deduplicating authors across multiple GitHub usernames.

## Pipeline Overview

The pipeline processes World-of-Code data through 5 main steps to identify authors with multiple aliases that belong to the same GitHub user:

```
World-of-Code Server Data
    ↓
[Step 1] Extract & Clean Raw Data
    ↓
[Step 2] Find Single-Author Repos → Extract Duplicate GitHub usernames
    ↓
[Step 3] Filter Duplicate GitHub usernames
    ↓
[Step 4] Check Organization Accounts
    ↓
[Step 5] Deduplicate Authors
    ↓
Final Output: Users with Multiple Aliases + Clean Dataset
```

## Pipeline Flow

1. **Start**: Raw compressed p2a files from World of Code server (`/da7_data/basemaps/gz/p2aFull.V2412.{0..31}.s`)
2. **Step 1**: Extract and clean the data, removing invalid UTF-8 lines
3. **Step 2**: Process to find single-author repos, then identify GitHub usernames that appear with multiple aliases
4. **Step 3**: Filter to keep only duplicate GitHub usernames and exclude problematic ones
5. **Step 4**: Check which GitHub usernames are organization accounts (to filter them out)
6. **Step 5**: Separate authors into two groups: those with multiple GitHub usernames (discarded for this work) and those with single GitHub usernames (kept as clean data)
7. **Final Output**: Clean dataset with single-username authors only (multi-username authors are separated out but not used in this work)

---

## Step-by-Step Script Details

### Step 1: Data Creation (`1_data_creation.sh`)

**Purpose**: Extracts and cleans raw p2a data files from World-of-Code server.

**What it does**:

- Concatenates 32 compressed p2a files (numbered 0-31) into a single file
- Identifies lines with invalid UTF-8 encoding
- Removes invalid UTF-8 lines to create a clean dataset

**Input**:

- Compressed files from World of Code: `/da7_data/basemaps/gz/p2aFull.V2412.{0..31}.s`
- Format: `repo;author` (one per line)
- Example: `githubUser123_repo-name;John Doe <jd@example.com>`

**Output**:

- `listofAuthors_justp.txt` - Raw extracted data (all 32 files concatenated)
- `non_utf8_lines_justp.txt` - Lines with invalid UTF-8 encoding (for debugging)
- `listofAuthors_clean_justp.txt` - **Clean data used in next step** (invalid lines removed)

**Usage**:

```bash
./1_data_creation.sh
```

**Note**: Requires access to the World-of-Code (WoC) servers. How to get access can be figured out at https://worldofcode.org/docs/#/getting_started. If you have logged in to the WoC servers, you can access the p2a data there. Run in a `tmux` session as it may take many hours.

---

### Step 2: Process Large File (`2_process_large_p2a_file.py`)

**Purpose**: Processes the cleaned data in two passes:

1. **First Pass**: Counts authors per repo to identify single-author repositories
2. **Second Pass**: Extracts repository owners (i.e., GitHub usernames) from single-author repos and finds usernames that appear with multiple different git handles (i.e., author aliases)

**What it does**:

- **Pass 1**: Reads entire file, counts how many different author aliases each repo has, identifies repos with exactly one author alias
- **Pass 2**: Only processes single-author repos, extracts GitHub username (everything before first `_` in repo name, stored as `GitHub_username` column), finds GitHub usernames that appear with multiple different author aliases
- Handles errors gracefully: identifies problematic lines using automated checks (control characters, missing semicolons, unescaped quotes, etc.), excludes them from main processing, and saves them to `problematic_lines_justp.txt` for review
- Processes in chunks to handle large files efficiently

**Input**:

- `listofAuthors_clean_justp.txt` - Clean p2a data from Step 1
- Format: `repo;author` (one per line)
- Example: `githubUser123_repo-name;John Doe <jd@example.com>`

**Output**:

- `Dealiased_Master_Dataset_cleaned_justp.csv` - **Main output**: CSV with `GitHub_username` and `author` columns, containing only duplicate GitHub usernames (GitHub usernames that appear with multiple authors)
- Example: `githubUser123,John Doe <jd@example.com>`

**Intermediate Files** (created during processing):

- `problematic_lines_justp.txt` - Lines identified as problematic during processing. These lines were found using automated checks for: control characters, missing semicolons, too many semicolons, unescaped quotes, extremely long lines/fields, and runtime processing errors. This checking was implemented based on suspicion of errors during processing or with World of Code data. In the end, only lines with unescaped sequences, control characters, or runtime errors were saved to this file; all other issues were accounted for and handled appropriately during processing.
- `single_author_repos_justp.txt` - List of all repos that have exactly one author alias
- `intermediate_repo_counts_*.txt` - Intermediate repo counts (saved periodically, cleaned up at end)
- `temp_chunk_*.csv` - Temporary chunk files (saved every 10 chunks, cleaned up at end)
- `processing_progress.txt` - Progress tracking file

**Usage**:

```bash
python3 2_process_large_p2a_file.py \
    --input listofAuthors_clean_justp.txt \
    --output Dealiased_Master_Dataset_cleaned_justp.csv \
    --chunksize 100000 \
    --save-frequency 1000000
```

**Arguments**:

- `--input`: Input file path (default: `listofAuthors_clean_justp.txt`)
- `--output`: Output CSV file path (default: `Dealiased_Master_Dataset_cleaned_justp.csv`)
- `--chunksize`: Chunk size for processing (default: 100000)
- `--save-frequency`: Save frequency in lines (default: 1000000)

**Note**: This step may take several days. Progress is saved periodically, so you can monitor progress and resume if interrupted.

---

### Step 3: Filter Dataset (`3_quality_filtering.py`)

**Purpose**: Filters the dataset to keep only entries with duplicate GitHub usernames and removes known problematic GitHub usernames.

**What it does**:

- Removes exact duplicate rows
- Ensures only duplicate GitHub usernames are kept (redundant check, but acts as safety filter)
- Excludes Bitbucket and GitLab usernames (default: `bb`, `gl`) because the organization check in Step 4 only applies to GitHub accounts, not Bitbucket or GitLab

**Input**:

- `Dealiased_Master_Dataset_cleaned_justp.csv` - Duplicate GitHub usernames from Step 2
- Format: CSV with columns `GitHub_username`, `author`
- Example: `githubUser123,John Doe <jd@example.com>`

**Output**:

- `Dealiased_Master_Dataset_filtered_justp.csv` - **Filtered dataset** with only duplicate GitHub usernames (excluding problematic ones)
- Example: `githubUser123,John Doe <jd@example.com>`
- **Note**: Even though this step filters to keep only entries with duplicate GitHub usernames, the final clean dataset (after Step 5) may still contain some GitHub usernames that appear only once. This happens because Step 5 deduplicates by author (not by GitHub username), and some authors with single GitHub usernames may end up being the only author associated with that username after multi-username authors are removed. See Step 5 for detailed explanation with examples.

**Intermediate Files**: None (processes in memory)

**Usage**:

```bash
python3 3_quality_filtering.py \
    --input Dealiased_Master_Dataset_cleaned_justp.csv \
    --output Dealiased_Master_Dataset_filtered_justp.csv \
    --exclude bb gl
```

**Arguments**:

- `--input`: Input CSV file (default: `Dealiased_Master_Dataset_cleaned_justp.csv`)
- `--output`: Output CSV file (default: `Dealiased_Master_Dataset_filtered_justp.csv`)
- `--exclude`: GitHub usernames to exclude (default: `bb gl` for Bitbucket and GitLab, which are excluded because organization check only applies to GitHub)

---

### Step 4: Organization Check (`4_orgcheck_graphql.py`)

**Purpose**: Identifies which GitHub usernames are organization accounts (vs. personal accounts) using GitHub GraphQL API.

**What it does**:

- Queries GitHub GraphQL API to check account type for each unique GitHub username
- Can query up to 100 GitHub usernames per request (much faster than REST API)
- Supports multiple tokens for parallel processing (Nx speedup with N tokens)
- Adds `organization_account` column: `1` = Organization, `0` = User/Not found

**Why this matters**: Organization accounts shouldn't be considered as "duplicate GitHub usernames" for the same person, so we need to identify and potentially filter them out.

**Input**:

- `Dealiased_Master_Dataset_filtered_justp.csv` - Filtered duplicate GitHub usernames from Step 3
- Format: CSV with columns `GitHub_username`, `author`
- Example: `githubUser123,John Doe <jd@example.com>`

**Output**:

- `Dealiased_Master_Dataset_with_orgs.csv` - **Annotated dataset** with `organization_account` column added
- Example: `githubUser123,John Doe <jd@example.com>,0`

**Intermediate Files**:

- `{output}.tmp` - Temporary file used during atomic writes (replaced with final output)

**Configuration**: Before running, edit `4_orgcheck_graphql.py` and add your GitHub tokens as shown below or provide them in command line as shown later:

```python
GITHUB_TOKENS = [
    "your_token_1",
    "your_token_2",
    # Add more tokens for parallel processing
]
```

**Usage**:

```bash
python3 4_orgcheck_graphql.py \
    --input Dealiased_Master_Dataset_filtered_justp.csv \
    --output Dealiased_Master_Dataset_with_orgs.csv \
    --column GitHub_username \
    --batch-size 100 \
    --tokens token1 token2 token3
```

**Arguments**:

- `--input`: Input CSV file (required)
- `--output`: Output CSV file (required)
- `--column`: Column with GitHub usernames (default: `GitHub_username`)
- `--batch-size`: Batch size for GraphQL queries (default: 100, max: 100)
- `--tokens`: GitHub tokens (space-separated, optional if set in script)

**Note**: Multiple tokens enable parallel processing for faster execution. The script automatically handles rate limiting and retries.

---

### Step 5: Deduplication (`5_dedup.py`)

**Purpose**: Separates author aliases with multiple GitHub usernames from author aliases with single GitHub usernames.

**What it does**:

- **Pass 1**: Reads entire dataset and maps each author alias to all their GitHub usernames
- **Analysis**: Identifies author aliases that have more than one unique GitHub username
- **Pass 2**: Writes rows to appropriate output files (multi-username vs. single-username)
- **Pass 3**: Creates a sorted version of multi-username entries (sorted by author alias, then GitHub username)

**Input**:

- `Dealiased_Master_Dataset_with_orgs.csv` - Annotated dataset from Step 4
- Format: CSV with columns `GitHub_username`, `author`, `organization_account`
- Example: `githubUser123,John Doe <jd@example.com>,0`

**Output**:

- `*_authors_with_multiple_usernames.csv` - **All entries for author aliases with multiple GitHub usernames** (unsorted)
- `*_authors_with_multiple_usernames_sorted.csv` - **Same as above, sorted by author alias then GitHub username** (easier to review)
- `*_after_dedup.csv` - **Clean dataset**: All entries for author aliases with single GitHub username
- Example: `githubUser789,Jane Smith <jsmith@example.com>,0`

**Intermediate Files**: None (processes in memory)

**Usage**:

```bash
python3 5_dedup.py \
    --input Dealiased_Master_Dataset_with_orgs.csv \
    --output-multi authors_with_multiple_usernames.csv \
    --output-clean dataset_after_dedup.csv \
    --output-sorted authors_with_multiple_usernames_sorted.csv
```

**Arguments**:

- `--input`: Input CSV file (required)
- `--output-multi`: Output file for author aliases with multiple GitHub usernames (optional, auto-generated if not provided)
- `--output-clean`: Output file for clean entries (optional, auto-generated if not provided)
- `--output-sorted`: Output file for sorted multi-username entries (optional, auto-generated if not provided)

---

## Final Outputs

After completing all 5 steps, you will have:

### 1. Author aliases with Multiple GitHub Usernames (Separated Out, Not Used)

- **File**: `*_authors_with_multiple_usernames_sorted.csv`
- **Content**: All entries where an author alias appears with multiple different GitHub usernames
- **Columns**: `GitHub_username`, `author`, `organization_account`
- **Note**: This file is created for reference but is not used in this work (discarded)

### 2. Clean Dataset (Single GitHub Username per Author Alias) - **USED IN PAPER**

- **File**: `*_after_dedup.csv`
- **Content**: All entries where each author alias has only one GitHub username
- **Columns**: `GitHub_username`, `author`, `organization_account`
- **Purpose**: **This is the final dataset used in the paper** - clean dataset where each author alias is uniquely identified by a single GitHub username

### Example Output Interpretation

**Multi-username file (separated out, not used):**
If you see in the multi-username file:

```csv
GitHub_username,author,organization_account
githubUser123,John Doe <jd@example.com>,0
githubUser456,John Doe <jd@example.com>,0
```

This means the author `John Doe <jd@examle.com>` is associated with two different GitHub usernames (`githubUser123` and `githubUser456`), suggesting these might be aliases for the same person. These entries are separated out but not used in this work.

**Clean dataset (used in paper):**
The clean dataset contains only author aliases with a single GitHub username, ensuring each author is uniquely identified:

```csv
GitHub_username,author,organization_account
githubUser789,Jane Smith <jsmith@example.com>,0
```

This is the final dataset used in the paper.

### Why Some GitHub Usernames Appear Only Once in the Clean Dataset

Even though Step 3 filters to keep only duplicate GitHub usernames, the clean dataset after Step 5 may contain GitHub usernames that appear only once. This happens because:

1. **Step 5 deduplicates by author, not by GitHub username**: Author aliases with multiple GitHub usernames are removed from the clean dataset, leaving only author aliases with single GitHub usernames.

2. **Multiple author aliases share the same GitHub username**: Different author aliases are associated with the same GitHub username (i.e., from different repositories). When author aliases that appear with multiple GitHub usernames are removed, some GitHub usernames may end up with only one remaining author alias.

**Example scenario:**

Note that at this point (due to the filtering in Step 3), all entries appearing in the input to Step 5 have GitHub usernames that occur more than once in the dataset (i.e., every GitHub username has multiple entries in the file). For example, consider this input to Step 5:

```csv
GitHub_username,author,organization_account
githubUser123,John Doe <jd@example.com>,0
githubUser123,Jane Smith <jsmith@example.com>,0
githubUser456,John Doe <jd@example.com>,0
githubUser789,Alice Brown <alice@example.com>,0
```

- `John Doe` has 2 GitHub usernames (`githubUser123` and `githubUser456`) → **removed** (goes to multi-username file)
- `Jane Smith` has 1 GitHub username (`githubUser123`) → **kept** in clean file
- `Alice Brown` has 1 GitHub username (`githubUser789`) → **kept** in clean file

**Result in clean file:**

```csv
GitHub_username,author,organization_account
githubUser123,Jane Smith <jsmith@example.com>,0
githubUser789,Alice Brown <alice@example.com>,0
```

Notice that `githubUser123` now appears only once (with Jane), even though it originally appeared twice (with both John and Jane). This is because John was removed due to having multiple GitHub usernames, leaving only Jane associated with `githubUser123`.

---

## Requirements

### System Requirements

- Linux/Unix system with bash
- Python 3.7+
- Access to `/da7_data/basemaps/gz/` directory on the World-of-Code servers (for Step 1)
- Sufficient disk space for large data files (tens of GBs)

### Python Dependencies

```bash
pip install -r GitHub_dataset_creation/requirements.txt
```

### GitHub API Access

- One or more GitHub personal access tokens with appropriate permissions
- Tokens should be added to `4_orgcheck_graphql.py` (see Step 4 Configuration section)

---

## Installation

1. Clone or download this replication package
2. Install Python dependencies:
   ```bash
   pip install pandas numpy requests
   ```
3. Make scripts executable:
   ```bash
   chmod +x 1_data_creation.sh
   chmod +x *.py
   ```

---

## Complete Pipeline Execution

Run all steps in sequence:

```bash
# Step 1: Extract and clean data
./1_data_creation.sh

# Step 2: Process to find duplicate GitHub usernames
python3 2_process_large_p2a_file.py \
    --input listofAuthors_clean_justp.txt \
    --output Dealiased_Master_Dataset_cleaned_justp.csv

# Step 3: Filter duplicate GitHub usernames
python3 3_quality_filtering.py \
    --input Dealiased_Master_Dataset_cleaned_justp.csv \
    --output Dealiased_Master_Dataset_filtered_justp.csv

# Step 4: Check organization accounts
python3 4_orgcheck_graphql.py \
    --input Dealiased_Master_Dataset_filtered_justp.csv \
    --output Dealiased_Master_Dataset_with_orgs.csv

# Step 5: Separate authors by GitHub username count
python3 5_dedup.py \
    --input Dealiased_Master_Dataset_with_orgs.csv
```

---

## Data Format Summary

### Step 1 Input

- Format: Compressed files from World of Code
- Content: `repo;author` (one per line)
- Example: `githubUser123_repo-name;John Doe <jd@example.com>`

### Step 1-2 Output

- Format: Text file (`repo;author`) or CSV (`GitHub_username,author`)
- Content: Cleaned p2a data
- Example: `githubUser123_repo-name;John Doe <jd@example.com>` (text) or `githubUser123,John Doe <jd@example.com>` (CSV)

### Step 2-3 Output

- Format: CSV
- Columns: `GitHub_username`, `author`
- Content: Duplicate GitHub usernames only
- Example: `githubUser123,John Doe <jd@example.com>`

### Step 4 Output

- Format: CSV
- Columns: `GitHub_username`, `author`, `organization_account`
- Content: Duplicate GitHub usernames with org flags
- Example: `githubUser123,John Doe <jd@example.com>,0`

### Step 5 Final Output

- Format: CSV
- Columns: `GitHub_username`, `author`, `organization_account`
- Content:
  - Multi-username file: Author aliases with multiple GitHub usernames
  - Clean file: Author aliases with single GitHub username
- Example: `githubUser123,Jane Smith <jsmith@example.com>,0`
