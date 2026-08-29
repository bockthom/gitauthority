# GitHub Single-Author Repository Evaluation Scripts

This directory contains scripts to evaluate the **Gambit**, **GitAuthority**, and **ALFAA** algorithms against GitHub single-author repository data as ground truth.

## Evaluation Methodology

These scripts use the relationship between GitHub usernames and their author aliases in single-author repositories as ground truth, based on the assumption that **all author aliases associated with the same GitHub username belong to the same person**. The evaluation checks whether each algorithm correctly merges all author aliases belonging to the same GitHub username.

## Scripts

- `evaluate_gambit.py` - Evaluates Gambit algorithm on GitHub data
- `evaluate_gitAuthority.py` - Evaluates GitAuthority algorithm on GitHub data
- `evaluate_alfaa.py` - Evaluates ALFAA algorithm on GitHub data (requires World of Code)
- `compute_confidence_intervals.py` - Computes confidence intervals for the aggregated metrics of an evaluation run (see [Computing Confidence Intervals](#computing-confidence-intervals) below)
- `sample_stratified_by_alias_count.py` - Draws a stratified sample of GitHub usernames by alias count, for building a manually-verified ground truth (see [Building a Manually-Verified Precision Ground Truth](#building-a-manually-verified-precision-ground-truth) below)
- `build_manual_precision_ground_truth.py` - Turns that sample into a DRE-format ground truth file for whole-pool evaluation (see [Building a Manually-Verified Precision Ground Truth](#building-a-manually-verified-precision-ground-truth) below)

All three evaluation scripts (`evaluate_gambit.py`, `evaluate_gitAuthority.py`, `evaluate_alfaa.py`) compute precision, recall, F1 score, accuracy, balanced accuracy, splitting, and lumping metrics on a per-GitHub-username basis and in aggregate.

## GitHub Dataset Configurations

All scripts support a dataset configuration as follows (see `DATASET_CONFIGS` in each script):

### GitHub_data

- **Path**: `GitHub_data/dataset_after_dedup.csv`
- **Description**: GitHub usernames with associated author aliases (personal accounts only); output of Step 5 of `GitHub_dataset_creation/README_GITHUB_DATASET_CREATION.md`.
- **Format**: CSV with columns: `GitHub_username`, `author`, `organization_account`
- **Filter**: `organization_account=0` (personal accounts only)

**Note**:

- The obtain the file `dataset_after_dedup.csv`, please follow the instructions in `GitHub_dataset_creation/README_GITHUB_DATASET_CREATION.md` to create the GitHub dataset.
- To apply the same tool (i.e., GitAuthority) in privacy-preserving mode, use the dataset configuration `GitHub_data_privacy`, which shall contain exactly the same data but indicates to the evaluation script that privacy-preserving should be included in the resulting file names, as otherwise only `GitAuthority` would be part of the file name. Make sure to create `GitHub_data_privacy/dataset_after_dedup.csv` manually before selecting this configuration. In addition, make sure to pass a privacy-aware config file via `--config` (see the Configuration section in the main README for details). For other tools, this is irrelevant, though.

## Other datasets

To run the scripts on other dataset in exactly the same format as in `GitHub_data`, also another path can be specified. The scripts allow to have multiple directories with different or equal data, and based on these directory names, the output files are generated.

## Data Format

All GitHub datasets contain:

- **GitHub_username**: GitHub username (unique identifier)
- **author**: Author alias in "Name &lt;email&gt;" format
- **organization_account**: 0 for personal accounts, 1 for organization accounts

**Ground Truth Assumption**: All author aliases with the same `GitHub_username` and `organization_account=0` should be merged into a single identity.

## Usage

Run the scripts from the parent directory (GitAuthority root directory). The scripts will automatically change to the correct working directory.

### Evaluate Gambit

```bash
python GitHub_dataset_evaluation/evaluate_gambit.py <dataset_name> [limit] [output_csv] [num_workers] [max_entries]
```

**Arguments**:

- `dataset_name`: Dataset configuration to use (required)
- `limit`: Optional limit on number of GitHub usernames to process (for testing)
- `output_csv`: Path to output CSV (optional, default: `evaluation_results_gambit_<dataset>[_limit<N>][_max<M>].csv`)
- `num_workers`: Number of parallel workers (default: 1, 0 = number of CPUs)
- `max_entries`: Maximum number of author aliases per Github username (default: no limit)

**Examples**:

```bash
# Evaluate on GitHub_data dataset
python GitHub_dataset_evaluation/evaluate_gambit.py GitHub_data

# Evaluate first 100 GitHub usernames with 4 workers
python GitHub_dataset_evaluation/evaluate_gambit.py GitHub_data 100 "" 4

# Evaluate with max 50 author aliases per GitHub username
python GitHub_dataset_evaluation/evaluate_gambit.py GitHub_data "" "" 1 50

# Use all available CPUs
python GitHub_dataset_evaluation/evaluate_gambit.py GitHub_data "" "" 0
```

### Evaluate GitAuthority

```bash
python GitHub_dataset_evaluation/evaluate_gitAuthority.py <dataset_name> [limit] [output_csv] [num_workers] [max_entries] [--config <config_file>]
```

**Arguments**: Same as Gambit evaluation, plus the following optional parameter:

- `--config` (optional): Path to config file (default: `merge_config.txt` in script directory)

**Examples**:

```bash
# Evaluate on GitHub_data dataset
python GitHub_dataset_evaluation/evaluate_gitAuthority.py GitHub_data

# Evaluate first 100 GitHub usernames with 4 workers
python GitHub_dataset_evaluation/evaluate_gitAuthority.py GitHub_data 100 "" 4

# Evaluate on privacy-preserving dataset
python GitHub_dataset_evaluation/evaluate_gitAuthority.py GitHub_data_privacy

# Use all available CPUs
python GitHub_dataset_evaluation/evaluate_gitAuthority.py GitHub_data "" "" 0
```

If you want to read the config from a different file than the default config (i.e., `merge_config.txt`), you can pass an additional parameter:

```bash
python GitHub_dataset_evaluation/evaluate_gitAuthority.py GitHub_data "" "" 0 --config my_config.txt
```

### Evaluate ALFAA

ALFAA evaluation requires a two-step process since it depends on World of Code (WoC) for identity resolution. For more information on how to access World of Code, see the note in Step 1 of `GitHub_dataset_creation/README_GITHUB_DATASET_CREATION.md`.

**Step 1: Prepare input for World of Code**

```bash
python GitHub_dataset_evaluation/evaluate_alfaa.py <dataset_name> --prepare-only [--woc-input <file>] [--max-entries <N>]
```

This generates a text file with unique author aliases (one per line in "Name &lt;email&gt;" format) for WoC lookup.

**Examples**:

```bash
# Prepare WoC input for GitHub_data dataset
python GitHub_dataset_evaluation/evaluate_alfaa.py GitHub_data --prepare-only

# Prepare with custom output file
python GitHub_dataset_evaluation/evaluate_alfaa.py GitHub_data --prepare-only --woc-input my_woc_input.txt

# Prepare with max 50 author aliases per GitHub username
python GitHub_dataset_evaluation/evaluate_alfaa.py GitHub_data --prepare-only --max-entries 50
```

**Step 2: Run World of Code lookup**

Use World of Code to process the author aliases in the WoC input file.
To do so, log in to the World of Code servers is necessary. For more information on how to access World of Code, see the note in Step 1 of `GitHub_dataset_creation/README_GITHUB_DATASET_CREATION.md`.

On the World-of-Code server, run the a2A-lookup script (which can be found here in this replication package in directory `descriptive_statistics`) with providing the input file, the output file, and the number of parallel threads that should be used for lookup:

```
./woc-a2A-lookup-parallel-xargs.sh woc_input.txt woc_results.txt 75
```

WoC should output assignments in this format:
```
searched name <searched email>;assigned name <assigned email>
```

Example WoC command:
```bash
cat GitHub_dataset_evaluation/woc_input_alfaa_GitHub_data.txt | ~/lookup/getValues -vV2409 a2A > GitHub_dataset_evaluation/woc_output_alfaa_GitHub_data.txt
```

**Step 3: Evaluate using WoC results**

```bash
python GitHub_dataset_evaluation/evaluate_alfaa.py <dataset_name> --woc-output <file> [--output <file>] [--limit <N>] [--num-workers <N>]
```

**Examples**:

```bash
# Evaluate using WoC output
python GitHub_dataset_evaluation/evaluate_alfaa.py GitHub_data \
  --woc-output woc_output_alfaa_GitHub_data.txt \
  --output results_alfaa_GitHub_data.csv

# Evaluate with 4 workers
python GitHub_dataset_evaluation/evaluate_alfaa.py GitHub_data \
  --woc-output woc_output_alfaa_GitHub_data.txt \
  --num-workers 4

# Evaluate first 100 GitHub usernames
python GitHub_dataset_evaluation/evaluate_alfaa.py GitHub_data \
  --woc-output woc_output_alfaa_GitHub_data.txt \
  --limit 100
```

## Output Files

All scripts produce multiple output files:

### 1. Main Results CSV (`<output_name>.csv`)

Contains per-GitHub-username results:

- **GitHub_username**: GitHub username
- **all_merged**: Boolean - whether all author aliases were merged
- **reason**: If not merged, description of the clusters formed

### 2. Per-username Metrics CSV (`<output_name>_per_username.csv`)

Contains detailed metrics for each GitHub username:

- **GitHub_username**: GitHub username
- **num_authors**: Number of author aliases for this username
- **merge_percentage**: Percentage of author pairs correctly merged
- **precision**: Precision score
- **recall**: Recall score
- **f1**: F1 score
- **accuracy**: Accuracy score
- **balanced_accuracy**: Balanced accuracy score
- **splitting**: Splitting error rate
- **lumping**: Lumping error rate

### 3. Summary Statistics CSV (`<output_name>_summary.csv`)

Contains aggregate statistics:

- Total GitHub usernames processed
- Number/percentage of usernames with all author aliases merged
- Number/percentage of usernames not fully merged
- Multi-author statistics
- **Aggregate metrics** (averaged across all usernames):
  - Merge Percentage
  - Precision
  - Recall
  - F1 Score
  - Accuracy
  - Balanced Accuracy
  - Splitting
  - Lumping
- **Filtering statistics** (if applicable):
  - Number of usernames removed due to organizational accounts
  - Number of usernames removed due to max threshold
  - Statistics about removed and remaining usernames, including average, median, standard deviation, maximum, and percentiles (p10, p25, p50, p75, p90, p95, p99, p99.5, p99.9, p99.99) of authors per identity

### 4. Chunk Results (parallel processing only)

When using multiple workers, individual chunk results are saved as:

- `<output_name>_chunk1.csv`
- `<output_name>_chunk2.csv`
- etc.

## Console Output

All scripts display:

```
=== SUMMARY ===
Total GitHub_usernames: <count>
All authors merged: <count> (<percentage>%)
Not all merged: <count> (<percentage>%)

GitHub_usernames with multiple authors: <count>
Successfully merged (multi-author only): <count> (<percentage>%)

=== AGGREGATE METRICS (AVERAGE ACROSS ALL USERNAMES) ===
Merge Percentage:  <value>%
Precision:         <value>
Recall:            <value>
F1 Score:          <value>
Accuracy:          <value>
Balanced Accuracy: <value>
Splitting:         <value>
Lumping:           <value>

=== FILTERING STATISTICS ===
Initial GitHub_usernames (before filtering): <count>
After org != 0 filter: <count>
After max threshold filter: <count>

Identities removed (org != 0): <count>
Average authors per removed org identity: <value>
Median authors per removed org identity: <value>
Std dev authors per removed org identity: <value>
Maximum authors per removed org identity: <value>
Percentiles of authors per removed org identity:
  p10: <value>
  p25: <value>
  p50: <value>
  p75: <value>
  p90: <value>
  p95: <value>
  p99: <value>
  p99.5: <value>
  p99.9: <value>
  p99.99: <value>

Identities removed (max threshold): <count>
Average authors per remaining identity (before max): <value>
Median authors per remaining identity (before max): <value>
Std dev authors per remaining identity (before max): <value>
Maximum authors per remaining identity (before max): <value>
Percentiles of authors per remaining identity (before max filtering):
  p10: <value>
  p25: <value>
  p50: <value>
  p75: <value>
  p90: <value>
  p95: <value>
  p99: <value>
  p99.5: <value>
  p99.9: <value>
  p99.99: <value>
```

## Evaluation Metrics

The scripts calculate metrics on two levels:

### Per-Username Metrics

For each GitHub_username, the ground truth is that **all authors should be merged**. Metrics are calculated based on pairwise comparisons:

- **True Positive (TP)**: Pairs of author aliases correctly merged
- **False Negative (FN)**: Pairs of author aliases that should be merged but weren't (splitting)
- **False Positive (FP)**: N/A for single-author ground truth (always 0)
- **True Negative (TN)**: N/A for single-author ground truth (always 0)

**Metrics**:

- **Precision**: TP / (TP + FP) - Always 1.0 in single-author evaluation (no false positives possible)
- **Recall**: TP / (TP + FN) - Proportion of author pairs correctly merged
- **F1 Score**: 2 × (Precision × Recall) / (Precision + Recall) - Harmonic mean
- **Accuracy**: (TP + TN) / (TP + FP + TN + FN) - Overall correctness
- **Balanced Accuracy**: (Recall + Specificity) / 2
- **Splitting**: FN / (TP + FN) - Rate of false negatives (1 - Recall)
- **Lumping**: FP / (TP + FN) - Always 0.0 in single-author evaluation
- **Merge Percentage**: 100 × TP / total_pairs - Percentage of pairs merged

### Aggregate Metrics

Average of per-username metrics across all GitHub usernames.

## Parallel Processing

All scripts support parallel processing to speed up evaluation:

- **Serial processing** (default): `num_workers=1`
- **Parallel processing**: `num_workers=N` where N > 1
- **Use all CPUs**: `num_workers=0`

Parallel processing is useful for large datasets but may consume significant memory.

## Filtering Options

### Organization Account Filter
All scripts automatically filter to `organization_account=0` (personal accounts only). Organization accounts are excluded because they may represent multiple developers.

### Max Entries Threshold
Use `max_entries` to exclude GitHub usernames with an excessive number of author aliases:

```bash
# Exclude GitHub usernames with more than 50 author aliases
python GitHub_dataset_evaluation/evaluate_gambit.py GitHub_data "" "" 1 50
```

This is useful for:

- Handling outliers (e.g., generic email addresses)
- Reducing memory usage
- Focusing on typical cases

The summary statistics report how many usernames were filtered and their characteristics.

## Implementation Details

### Ground-Truth Assumption

The evaluation assumes that all author aliases with the same `GitHub_username` (and `organization_account=0`) represent the same person. This is a reasonable assumption for personal GitHub accounts, where:

- The user controls all commits made under their account
- Different author aliases (name/email combinations) arise from:
  - Different computers/configurations
  - Email address changes
  - Name variations (nicknames, full names, etc.)

### Cluster Assignment

- **Gambit**: Uses the `author_id` column from gambit output to identify clusters
- **GitAuthority**: Builds clusters from the merging dictionary using transitive closure
- **ALFAA**: Uses World of Code assignments (searched alias → assigned alias mapping)

### Evaluation Process

For each GitHub username:

1. Extract all associated author aliases
2. Run the algorithm to produce clusters
3. Check if all author aliases are in a single cluster
4. Calculate per-username metrics based on pairwise comparisons
5. Aggregate metrics across all usernames

## Requirements

The scripts require Python 3.7+ and the following packages:

```bash
pip install -r GitHub_dataset_evaluation/requirements.txt
```

- `pandas`, `numpy`, `regex`, `scipy`, `nicknames`
- `gambit` (for Gambit evaluation — install according to its own documentation)
- `mergeAliases` module (included in this repository, no separate install needed)
- World of Code access (for ALFAA evaluation — see `GitHub_dataset_creation/README_GITHUB_DATASET_CREATION.md`)

## Notes

- Scripts automatically change to the parent directory for proper module imports
- All output files are saved in the `GitHub_dataset_evaluation/` directory
- The scripts filter out organization accounts (`organization_account != 0`) automatically
- Parallel processing creates temporary chunk files that persist after completion
- For ALFAA evaluation, WoC coverage statistics are displayed to show how many author aliases were found in WoC


## Computing Confidence Intervals

After running an evaluation, you can compute confidence intervals for the aggregated metrics using the `compute_confidence_intervals.py` script. This provides statistical bounds on the mean values of metrics.

### Usage

```bash
python GitHub_dataset_evaluation/compute_confidence_intervals.py <input_csv> [OPTIONS]
```

**Arguments**:

- `input_csv`: Path to the `_per_username.csv` file from evaluation (required)

**Options**:

- `-m, --metrics`: Metrics to compute CIs for (default: all metrics)
- `-n, --n-bootstrap`: Number of bootstrap samples (default: 10000)
- `-c, --confidence`: Confidence level in percent (default: 95)
- `-s, --seed`: Random seed for reproducibility (default: 42)
- `-o, --output`: Output CSV file for results (optional, auto-generated if not provided)

**Examples**:

```bash
# Compute 95% CIs for all metrics using default settings
python GitHub_dataset_evaluation/compute_confidence_intervals.py \
  GitHub_dataset_evaluation/evaluation_results_gitAuthority_GitHub_data_per_username.csv

# Compute 99% CIs with 20000 bootstrap samples
python GitHub_dataset_evaluation/compute_confidence_intervals.py \
  GitHub_dataset_evaluation/evaluation_results_gambit_GitHub_data_per_username.csv \
  -c 99 -n 20000

# Compute CIs for specific metrics only
python GitHub_dataset_evaluation/compute_confidence_intervals.py \
  GitHub_dataset_evaluation/evaluation_results_alfaa_GitHub_data_per_username.csv \
  -m recall precision f1

# Specify custom output file
python GitHub_dataset_evaluation/compute_confidence_intervals.py \
  GitHub_dataset_evaluation/evaluation_results_gitAuthority_GitHub_data_per_username.csv \
  -o my_confidence_intervals.csv
```

### Methods

The script computes confidence intervals using two methods:

1. **Bootstrap Method** (recommended):
   - Resamples the per-username metrics with replacement
   - Computes the mean for each bootstrap sample
   - Uses percentiles of bootstrap distribution as confidence bounds
   - More robust for non-normal distributions
   - Default: 10,000 bootstrap samples

2. **Normal Approximation**:
   - Uses t-distribution with standard error of the mean
   - Assumes approximately normal distribution of the mean
   - Faster but less robust for skewed distributions

### Output

The script produces:

1. **Console output** with formatted table, such as in the following examples (dummy values here in the example):

```
================================================================================
CONFIDENCE INTERVALS (95% confidence level)
================================================================================

Metric               Mean       Std Dev    Bootstrap CI              Normal CI
-------------------- ---------- ---------- ------------------------- -------------------------
recall               0.8234     0.1456     [0.8198, 0.8270]         [0.8195, 0.8273]
precision            0.9876     0.0234     [0.9871, 0.9881]         [0.9870, 0.9882]
f1                   0.8912     0.1123     [0.8881, 0.8943]         [0.8880, 0.8944]
accuracy             0.8456     0.1345     [0.8422, 0.8490]         [0.8420, 0.8492]
balanced_accuracy    0.8678     0.1234     [0.8645, 0.8711]         [0.8644, 0.8712]
splitting            0.1766     0.1456     [0.1730, 0.1802]         [0.1727, 0.1805]
lumping              0.0000     0.0000     [0.0000, 0.0000]         [0.0000, 0.0000]
merge_percentage     82.3400    14.5600    [81.9800, 82.7000]       [81.9500, 82.7300]
================================================================================
```

2. **CSV file** (`_confidence_intervals.csv`) with columns:

        - `metric`: Metric name
        - `n`: Sample size (number of GitHub usernames)
        - `mean`: Mean value across all usernames
        - `std`: Standard deviation
        - `median`: Median value
        - `bootstrap_ci_XX_lower`: Lower bound of bootstrap CI
        - `bootstrap_ci_XX_upper`: Upper bound of bootstrap CI
        - `normal_ci_XX_lower`: Lower bound of normal CI
        - `normal_ci_XX_upper`: Upper bound of normal CI

### Interpretation

- **Mean**: Average metric value across all GitHub usernames
- **Confidence Interval**: Range within which the true population mean is likely to fall
- **95% CI**: If you repeated the evaluation many times, 95% of the computed intervals would contain the true mean
- **Narrower intervals**: More precise estimate (larger sample size or less variability)
- **Wider intervals**: Less precise estimate (smaller sample size or more variability)

### When to Use

Compute confidence intervals when:

- Comparing different algorithms (check if CIs overlap)
- Reporting results in papers/publications
- Assessing statistical significance of observed differences
- Understanding uncertainty in aggregate metrics

### Example Workflow

```bash
# Step 1: Run evaluation for GitAuthority
python GitHub_dataset_evaluation/evaluate_gitAuthority.py GitHub_data

# Step 2: Compute confidence intervals
python GitHub_dataset_evaluation/compute_confidence_intervals.py \
  GitHub_dataset_evaluation/evaluation_results_GitHub_data_per_username.csv

# Step 1: Run evaluation for gambit
python GitHub_dataset_evaluation/evaluate_gambit.py GitHub_data

# Step 2: Compute confidence intervals
python GitHub_dataset_evaluation/compute_confidence_intervals.py \
  GitHub_dataset_evaluation/evaluation_results_gambit_GitHub_data_per_username.csv

# Step 1: Run evaluation for ALFAA
python GitHub_dataset_evaluation/evaluate_alfaa.py GitHub_data

# Step 2: Compute confidence intervals
python GitHub_dataset_evaluation/compute_confidence_intervals.py \
  GitHub_dataset_evaluation/evaluation_results_alfaa_GitHub_data_per_username.csv
```

## Building a Manually-Verified Precision Ground Truth

The GitHub-username-based ground truth used above is only as trustworthy as the assumption "same username implies same person" — it can miss real false positives that a per-username evaluation can never expose in the first place, because merges are structurally impossible across usernames. `sample_stratified_by_alias_count.py` and `build_manual_precision_ground_truth.py` build a small, manually-audited ground truth that instead evaluates GitAuthority once over an entire pool of aliases (spanning many usernames), via `tool_evaluation/evaluate_gitAuthority_groundtruth.py --dre` (see `tool_evaluation/README_TOOL_EVALUATION.md`). This makes cross-username merges real, checkable pairs instead of being structurally impossible.

### Step 1: Draw a stratified sample of usernames

```bash
python GitHub_dataset_evaluation/sample_stratified_by_alias_count.py [OPTIONS]
```

Streams a GitHub dataset CSV (columns `git_username`, `author`, `organization_account`, one row per alias) and reservoir-samples a fixed number of usernames for each alias-count stratum in a given range, without loading the full file into memory (usernames must be grouped contiguously, though not globally sorted).

**Options**:

- `--input`: Path to the source CSV (default: `GitHub_data_final/evaluation_dataset_only_users_after_filtering_org_after_dedup.csv`)
- `--min-aliases`, `--max-aliases`: Alias-count range to sample from (default: 2–10)
- `--n-per-stratum`: Target number of usernames per alias count (default: 20)
- `--seed`: Random seed for reproducibility (default: 42)
- `--extend-from`: Path to a previous `*_summary.csv` whose usernames must be kept as-is; only the additional slots needed to reach `--n-per-stratum` are newly sampled from the remaining pool — use this to grow an already partially-labeled sample without re-sampling (and re-auditing) usernames you already kept
- `--output`: Output CSV with the sampled usernames' full alias rows (default: `sampled_usernames_stratified.csv`)
- `--summary-output`: Output CSV listing just `num_aliases`, `git_username` for the sample (default: `sampled_usernames_stratified_summary.csv`)
- `--stats-output`: Output text file with the sampling statistics (default: `sampled_usernames_stratified_stats.txt`)

**Example**:

```bash
python GitHub_dataset_evaluation/sample_stratified_by_alias_count.py \
  --min-aliases 2 --max-aliases 10 --n-per-stratum 20 \
  --output sampled_usernames_stratified_40.csv \
  --summary-output sampled_usernames_stratified_40_summary.csv
```

### Step 2: Manually audit the sample

Inspect the sampled usernames (e.g., via the summary CSV) and correct any cases where "same username implies same person" doesn't hold (shared/bot/org accounts, or one real person using two accounts).

### Step 3: Build the DRE-format ground truth

```bash
python GitHub_dataset_evaluation/build_manual_precision_ground_truth.py [OPTIONS]
```

Turns the stratified sample into a DRE-format ground truth dataset for whole-pool evaluation. Each distinct GitHub username becomes one `dre_id` (an anonymized integer, not the literal username — assignment is shuffled by `--seed` so ids don't leak username order), and every alias of that username becomes one `woc_id` row under that `dre_id`.

**Options**:

- `--input`: Stratified sample CSV with columns `git_username`, `author`, `organization_account` (default: `sampled_usernames_stratified_40.csv`, i.e., the `--output` of Step 1)
- `--prefix`: Output filename prefix, should describe the source dataset and sample size (default: `github_final_sample360`)
- `--output-dir`: Directory to write the output files into (default: current directory)
- `--seed`: Random seed used to shuffle `dre_id` assignment (default: 42)

**Produces three files**:

- `<prefix>_raw.csv`: DRE file reflecting the sample as-is (one `dre_id` per original username, unmodified)
- `<prefix>_corrected.csv`: Byte-identical starting copy of the same file, meant to be hand-edited — change an alias's `dre_id` to a value used nowhere else to split it into its own identity, or to another group's `dre_id` to merge it there, in order to fix wrongly-grouped aliases found during the manual audit. Never edit `dre_id` in the `_raw.csv` file.
- `<prefix>_username_map.csv`: `dre_id` -> original GitHub username, kept separately for your own reference/auditing. This is not part of the DRE format and not read by the evaluation script — the two files above never carry the literal username.

**Example**:

```bash
python GitHub_dataset_evaluation/build_manual_precision_ground_truth.py \
  --input sampled_usernames_stratified_40.csv \
  --prefix github_final_sample360 \
  --output-dir GitHub_dataset_evaluation/
```

### Step 4: Evaluate against the corrected ground truth

```bash
python tool_evaluation/evaluate_gitAuthority_groundtruth.py \
  --dre GitHub_dataset_evaluation/github_final_sample360_corrected.csv \
  --output results_gitAuthority_precision_sample360.csv
```

See `tool_evaluation/README_TOOL_EVALUATION.md` for details on `evaluate_gitAuthority_groundtruth.py` and the DRE ground-truth format.


