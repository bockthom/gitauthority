# Ground-Truth Evaluation Scripts

This directory contains scripts to evaluate the **Gambit**, **GitAuthority**, and **ALFAA** algorithms against ground-truth data.

## Scripts

- `evaluate_gambit_groundtruth.py` - Evaluates Gambit algorithm
- `evaluate_gitAuthority_groundtruth.py` - Evaluates GitAuthority algorithm
- `evaluate_alfaa_groundtruth.py` - Evaluates ALFAA algorithm (requires World of Code)

All three scripts support four ground-truth formats (Matrix, Pairwise, DRE, and Wiese) and compute precision, recall, F1 score, accuracy, balanced accuracy, splitting, and lumping metrics. The ALFAA script requires World of Code (WoC) for identity resolution.

**Note**: Make sure that the actual ground-truth data are available in the specified paths and directories before running the scripts. Information on how to access the datasets and where to store them can be found in the main README file of this replication package, in a section called "Datasets". In addition, also any other dataset can be used as long as it follows one of the supported ground-truth formats described below.

## Ground-Truth Formats

### 1. Matrix Format (Gambit Data)

Used by the gnome_gtk dataset:

- **Developers file**: CSV with columns `rec_name,rec_email` (list of aliases)
- **Ground-truth file**: NxN CSV matrix where non-zero values indicate aliases should be merged

Example:
```
dataset_GnomeGTK_Gote/
  ├── gnome_gtk_DEVELOPERS.csv      # List of 1896 aliases
  └── gnome_gtk_MANUAL_GROUNDTRUTH.csv  # 1896x1896 matrix
```

### 2. Pairwise Format (ALFAA Data)

Used by the ALFAA dataset:

- Each row contains: `alias1;alias2;label` where label is 1 (merge) or 0 (don't merge)

Example:
```
dataset_OpenStack_Amreen/
  └── crossRater0.csv  # Pairwise comparisons
```

### 3. DRE Format (DREUser Data)

Used by the DRE dataset:
- **CSV file** with columns: `dre_id,woc_id,commit,time`
- Each `dre_id` represents a canonical developer identity (ground-truth cluster)
- Each `woc_id` represents an alias (in "Name <email>" format)
- All `woc_id` values sharing the same `dre_id` belong to the same cluster
- The loader creates aliases from both unique `dre_id` and unique `woc_id` values

Example:
```
dataset_DRE_Amreen/
  └── DREUser.csv  # DRE ground-truth dataset
```

Format example:
```csv
dre_id,woc_id,commit,time
prefix@domain,First Last <first.last@gmail.com>,21fdb6e2151ff856ac28b341a1c3261e0a2096f9,1535735928
prefix@domain,First Last <first.last@gmail.com>,3b045967877d82990918f3b74fc385f0011b8e58,1535736707
```

### 4. Wiese Format (Apache Community)

Used by the apache.community dataset:

- **Text file** where each line represents one person with all their name/email variants
- Format: `id#=#name1#;#name2#;#...##=#email1#;#email2#;#...`
- All names and emails on the same line belong to the same person (ground-truth cluster)

There are two modes depending on whether `--asf-developers` is provided:                                                                                                                
                                                                                                                                                                                        
- **With `--asf-developers`** (recommended): Loads real git commit aliases (with actual names) from `AsfDevelopers.txt` and cross-references their emails against the ground truth to assign cluster IDs. Only aliases whose email appears in the ground truth are kept (~1572 aliases). All name-based matching rules are active. This is the preferred mode for meaningful evaluation.                                                                                                                                                                             
                                                                                                                                                                                        
- **Without `--asf-developers`** (email-only): Extracts emails directly from the ground truth file, creating one alias per email occurrence with an **empty name**. Results in ~3677 aliases with no names, so only email-based rules can fire. Recall is inherently low and not comparable to the with-names mode.
                                                                                                                                                                                        
Example:
```
dataset_ASF_Wiese/masterDegreeAnalisys/datasets/                                                                                                                                                
  └── apache.community              # Apache developer identities (ground truth)                                                                                                        
dataset_ASF_Wiese/masterDegreeAnalisys/resources/                                                                                                                                               
  └── AsfDevelopers.txt             # Real git commit aliases with names                                                                                                                
```

Format example:
```
prefix@domain#=#First Middle Last#;##=#name@xy.com#;#nickname@domain#;#
prefix2@domain#=#first last#;#first last/fist last#;##=#nickname@domain2#;#nickname2@domain2#;#name2@domain3#;#
```

## Usage

Run the scripts from the parent directory (GitAuthority root directory). All file paths should be relative to the `tool_evaluation/` directory, as the scripts will automatically change to that directory when running.

### Evaluate Gambit with Matrix Format

```bash
python tool_evaluation/evaluate_gambit_groundtruth.py \
  --developers dataset_GnomeGTK_Gote/gnome_gtk_DEVELOPERS.csv \
  --groundtruth dataset_GnomeGTK_Gote/gnome_gtk_MANUAL_GROUNDTRUTH.csv \
  --output results_gambit_gnome.csv
```

### Evaluate Gambit with Pairwise Format

```bash
python tool_evaluation/evaluate_gambit_groundtruth.py \
  --pairwise dataset_OpenStack_Amreen/crossRater0.csv \
  --output results_gambit_alfaa.csv
```

### Evaluate Gambit with DRE Format

```bash
python tool_evaluation/evaluate_gambit_groundtruth.py \
  --dre dataset_DRE_Amreen/DREUser.csv \
  --output results_gambit_dre.csv
```

### Evaluate Gambit with Wiese Format

```bash
python tool_evaluation/evaluate_gambit_groundtruth.py \
  --wiese dataset_ASF_Wiese/masterDegreeAnalisys/datasets/apache.community \
  --asf-developers dataset_ASF_Wiese/masterDegreeAnalisys/resources/AsfDevelopers.txt \
  --output results_gambit_wiese.csv
```

This uses the recommended mode with real git commit aliases (names + emails). To run the email-only mode instead (no names, lower recall), omit `--asf-developers`.

### Evaluate GitAuthority with Matrix Format

```bash
python tool_evaluation/evaluate_gitAuthority_groundtruth.py \
  --developers dataset_GnomeGTK_Gote/gnome_gtk_DEVELOPERS.csv \
  --groundtruth dataset_GnomeGTK_Gote/gnome_gtk_MANUAL_GROUNDTRUTH.csv \
  --output results_gitAuthority_gnome.csv
```

### Evaluate GitAuthority with Pairwise Format

```bash
python tool_evaluation/evaluate_gitAuthority_groundtruth.py \
  --pairwise dataset_OpenStack_Amreen/crossRater0.csv \
  --output results_gitAuthority_alfaa.csv
```

### Evaluate GitAuthority with DRE Format

```bash
python tool_evaluation/evaluate_gitAuthority_groundtruth.py \
  --dre dataset_DRE_Amreen/DREUser.csv \
  --output results_gitAuthority_dre.csv
```

### Evaluate GitAuthority with Wiese Format

```bash
python tool_evaluation/evaluate_gitAuthority_groundtruth.py \
  --wiese dataset_ASF_Wiese/masterDegreeAnalisys/datasets/apache.community \
  --asf-developers dataset_ASF_Wiese/masterDegreeAnalisys/resources/AsfDevelopers.txt \
  --output results_gitAuthority_wiese.csv
```

This uses the recommended mode with real git commit aliases (names + emails). To run the email-only mode instead (no names, lower recall), omit `--asf-developers`.

If you want to read the config from a different file than the default config (i.e., `merge_config.txt`), you can pass an additional parameter:

```bash
  --config my_config.txt
```

**Note**: For GitAuthority evaluation, the script also generates two additional CSV files with detailed error analysis:

- `false_positives_<output_name>.csv` - Pairs incorrectly merged
- `false_negatives_<output_name>.csv` - Pairs that should have been merged but weren't

### Evaluate ALFAA with World of Code

ALFAA evaluation requires a two-step process since it depends on World of Code (WoC) for identity resolution.

ALFAA supports all four ground-truth formats (pairwise, matrix, DRE, and Wiese).

**Step 1: Prepare input for World of Code**

Pairwise format:
```bash
python tool_evaluation/evaluate_alfaa_groundtruth.py \
  --pairwise dataset_OpenStack_Amreen/crossRater0.csv \
  --prepare-only \
  --woc-input woc_input.txt
```

Matrix format:
```bash
python tool_evaluation/evaluate_alfaa_groundtruth.py \
  --developers dataset_GnomeGTK_Gote/gnome_gtk_DEVELOPERS.csv \
  --groundtruth dataset_GnomeGTK_Gote/gnome_gtk_MANUAL_GROUNDTRUTH.csv \
  --prepare-only \
  --woc-input woc_input_gnome.txt
```

This generates the specified WoC input file (e.g., `woc_input.txt`) with one alias per line in format: `Name <email>`

**Note**: The `--woc-input` parameter is optional and defaults to `woc_input.txt`. Use it to specify a custom filename for the WoC input file.

**Step 2: Run World of Code lookup**

Use World of Code to process the aliases in the WoC input file.
To do so, log in to the World of Code servers is necessary. For more information on how to access World of Code, see the note in Step 1 of `GitHub_dataset_creation/README_GITHUB_DATASET_CREATION.md`.

On the World-of-Code server, run the a2A-lookup script (which can be found here in this replication package in directory `descriptive_statistics`) with providing the input file, the output file, and the number of parallel threads that should be used for lookup:

```
./woc-a2A-lookup-parallel-xargs.sh woc_input.txt woc_results.txt 75
```

WoC should output assignments in this format:
```
searched name <searched email>;assigned name <assigned email>
```

Save the WoC output to a file (e.g., `woc_results.txt`).

**Step 3: Evaluate using WoC results**

**Pairwise format:**
```bash
python tool_evaluation/evaluate_alfaa_groundtruth.py \
  --pairwise dataset_OpenStack_Amreen/crossRater0.csv \
  --woc-output woc_results.txt \
  --output results_alfaa.csv
```

**Matrix format:**
```bash
python tool_evaluation/evaluate_alfaa_groundtruth.py \
  --developers dataset_GnomeGTK_Gote/gnome_gtk_DEVELOPERS.csv \
  --groundtruth dataset_GnomeGTK_Gote/gnome_gtk_MANUAL_GROUNDTRUTH.csv \
  --woc-output woc_results.txt \
  --output results_alfaa_gnome.csv
```

**Note**: Like GitAuthority, the ALFAA script also generates error analysis files:

- `false_positives_<output_name>.csv` - Pairs incorrectly merged
- `false_negatives_<output_name>.csv` - Pairs that should have been merged but weren't

**DRE Format:**

Prepare WoC input:
```bash
python tool_evaluation/evaluate_alfaa_groundtruth.py \
  --dre dataset_DRE_Amreen/DREUser.csv \
  --prepare-only \
  --woc-input dataset_DRE_Amreen/woc-a-input-dre-groundtruth.txt
```

Then run World of Code lookup (on WoC server) and evaluate:
```bash
python tool_evaluation/evaluate_alfaa_groundtruth.py \
  --dre dataset_DRE_Amreen/DREUser.csv \
  --woc-output dataset_DRE_Amreen/woc-a-input-dre-groundtruth-dre-output.txt \
  --output results_alfaa_dre.csv
```

**Wiese Format:**

Prepare WoC input:
```bash
python tool_evaluation/evaluate_alfaa_groundtruth.py \
  --wiese dataset_ASF_Wiese/masterDegreeAnalisys/datasets/apache.community \
  --asf-developers dataset_ASF_Wiese/masterDegreeAnalisys/resources/AsfDevelopers.txt \
  --prepare-only \
  --woc-input dataset_ASF_Wiese/woc-a-input-wiese-groundtruth.txt
```

Then run World of Code lookup (on WoC server) and evaluate:
```bash
python tool_evaluation/evaluate_alfaa_groundtruth.py \
  --wiese dataset_ASF_Wiese/masterDegreeAnalisys/datasets/apache.community \
  --asf-developers dataset_ASF_Wiese/masterDegreeAnalisys/resources/AsfDevelopers.txt \
  --woc-output dataset_ASF_Wiese/woc-a-input-wiese-groundtruth-output.txt \
  --output results_alfaa_wiese.csv
```

To run in email-only mode instead (no names, lower recall), omit `--asf-developers` from both commands.

## Output

Both scripts produce:

1. **Console output** with metrics:

        - Precision
        - Recall
        - F1 Score
        - Accuracy
        - Splitting
        - Lumping
        - Confusion matrix (TP, FP, TN, FN)

2. **CSV file** (if `--output` specified) with same metrics in tabular format

### Example Output Format

```
=== EVALUATION RESULTS ===
Precision: <value>
Recall:    <value>
F1 Score:  <value>
Accuracy:  <value>
Splitting: <value>
Lumping:   <value>

Confusion Matrix:
  True Positives:  <count>
  False Positives: <count>
  True Negatives:  <count>
  False Negatives: <count>
  Total Pairs:     <count>
```

## Switching Ground-Truth Datasets

To use a different ground-truth dataset:

### For Matrix Format:

1. Place your developers CSV in a directory (e.g., `my_data/developers.csv`)
2. Place your ground-truth matrix in the same directory (e.g., `my_data/groundtruth.csv`)
3. Run with `--developers my_data/developers.csv --groundtruth my_data/groundtruth.csv`

### For Pairwise Format:

1. Create a CSV with format: `alias1;alias2;label` (semicolon-separated)
2. Ensure `label` column contains only 0 or 1
3. Run with `--pairwise my_data/pairwise.csv`

### For DRE Format:

1. Create a CSV with columns: `dre_id,woc_id,commit,time`
2. Each `dre_id` should represent a canonical developer identity
3. Each `woc_id` should be an alias in "Name <email>" format
4. Run with `--dre my_data/DREUser.csv`

### For Wiese Format:

1. Create a text file where each line represents one person
2. Format: `id#=#name1#;#name2#;#...##=#email1#;#email2#;#...`
3. The ID represents the cluster identifier
4. All names and emails on the same line belong to the same person
5. Run with `--wiese my_data/community.txt`
6. Optionally provide `--asf-developers my_data/developers.txt` with real git commit aliases (`Name <email>` format, one per line) to enable name-based matching rules

## Implementation Details

### Evaluation Metrics

- **Precision**: TP / (TP + FP) - Of all predicted merges, how many were correct?
- **Recall**: TP / (TP + FN) - Of all actual merges, how many were found?
- **F1 Score**: 2 × (Precision × Recall) / (Precision + Recall) - Harmonic mean
- **Accuracy**: (TP + TN) / (TP + FP + TN + FN) - Overall correctness
- **Splitting**: FN / (TP + FN) - Rate of false negatives (aliases that should be merged but aren't)
- **Lumping**: FP / (TP + FN) - Rate of false positives relative to true merges (aliases incorrectly merged)

### Pair Generation

For matrix format, pairs are generated from the upper triangle of the ground-truth matrix (avoiding duplicates).

For pairwise format, pairs are read directly from the CSV.

### Cluster Assignment

- **Gambit**: Uses the `author_id` column from gambit output
- **GitAuthority**: Builds transitive closure from the merging dictionary
- **ALFAA**: Uses World of Code assignments (searched alias → assigned alias mapping)

## Requirements

The scripts require Python 3.7+ and the following packages:

```bash
pip install -r tool_evaluation/requirements.txt
```

- `pandas`, `numpy`, `regex`
- `gambit` (for Gambit evaluation — install according to its own documentation)
- `mergeAliases` module (included in this repository, no separate install needed)
- World of Code access (for ALFAA evaluation — see `GitHub_dataset_creation/README_GITHUB_DATASET_CREATION.md`)

## Notes

- Scripts automatically change to the evaluation directory
- The output CSV is saved relative to the evaluation directory
- Ground-truth files can use relative paths (relative to evaluation directory)
