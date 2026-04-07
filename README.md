
## GitAuthority + Evaluation

This repository contains the implementation of GitAuthority as well as the corresponding evaluation scripts.

Note that the datasets used to validate GitAuthority are deliberately not part of this replication package. For privacy reasons, we cannot publish these datasets as they contain personal name and email addresses of real people. Anonymizing them is not possible because their actual spellings etc. are necessary to serve as a proper ground truth. However, this replication package contains detailed information on how to get access to the used datasets or how to create these datasets on your own. In case of questions regarding that, please reach out to the authors of this replication package.

___

## GitAuthority

GitAuthority is a configurable tool for de-aliasing author information based on author aliases that consist of name and email pairs. It uses configurable normalization steps and matching rules (i.e., heuristics) to group author identities that belong to the same person.

### Setup

**Requirements:** Python 3.7+

Install the dependencies:

```bash
pip install -r requirements.txt
```

### Usage

GitAuthority supports two modes of operation. To run GitAuthority locally on your own data, use **Mode 1 (File Mode)**. **Mode 2 (WoC Mode)** fetches author data directly from World of Code and only works on the World of Code servers.

#### Mode 1: File Mode

Process author data from a file containing author information:

```bash
python gitAuthority.py --file <authors_file> [--name <project_name>] [--output-dir <dir>] [--config <config_file>] [--username] [--drop-boolean-column]
```

**Parameters:**

- `--file`, `-f` (required): Path to file containing author data
    - Supported formats: `Name <email>` or `Name;email` (one per line)
- `--name`, `-n` (optional): Project name for output file (default: filename without extension)
- `--output-dir`, `-o` (optional): Output directory for results (default: current directory `.`)
- `--config`, `-c` (optional): Path to config file (default: `merge_config.txt` in script directory)
- `--username`, `-u` (optional): Expect a username as the last semicolon-separated field on each input line (e.g. `Name;email;username` or `Name <email>;username`). When set, the output CSV will include a `username` column.
- `--drop-boolean-column` (optional): Drop the `is_original_author_weird_id` column from the output CSV.

**Example:**

```bash
python gitAuthority.py --file authors.txt --name my_project --output-dir output/
```

If you want to read the config from a different file than the default config (i.e., `merge_config.txt`), you can pass an additional parameter:

```bash
python gitAuthority.py --file authors.txt --name my_project --output-dir output/ --config my_config.txt
```

#### Mode 2: World of Code (WoC) Mode

Fetch author data directly from World of Code for a specific project:

```bash
python gitAuthority.py --project <project_name> [--woc-version <version>] [--output-dir <dir>] [--config <config_file>] [--drop-boolean-column]
```

**Parameters:**

- `--project`, `-p` (required): GitHub project identifier in the format `owner_name` (e.g., `bockthom_gitauthority`)
- `--woc-version`, `-v` (optional): World of Code dataset version (default: `V2412`)
- `--output-dir`, `-o` (optional): Output directory for results (default: current directory `.`)
- `--config`, `-c` (optional): Path to config file (default: `merge_config.txt` in script directory)
- `--drop-boolean-column` (optional): Drop the `is_original_author_weird_id` column from the output CSV.

**Example:**

```bash
python gitAuthority.py --project bockthom_gitauthority --woc-version V2412 --output-dir output/
```

If you want to read the config from a different file than the default config (i.e., `merge_config.txt`), you can pass an additional parameter:

```bash
python gitAuthority.py --project bockthom_gitauthority --woc-version V2412 --output-dir output/ --config my_config.txt
```

> **Note:** This mode requires access to World of Code servers and their lookup tools. See `GitHub_dataset_creation/README_GITHUB_DATASET_CREATION.md` for details on how to get access.

#### Backward Compatibility

The tool maintains backward compatibility with positional arguments:

```bash
python gitAuthority.py <project> [woc_version] [output_dir]
```

**Output:**

The tool generates a CSV file `merged_authors_<project>.csv` containing:

- `project`: Project name
- `original_author_id`: Original author identity (Name <email>)
- `dealiased_author_id`: Merged/canonical author identity
- `is_original_author_weird_id` (only if `--drop-boolean-column` is not set): Flag indicating suspicious identities
- `username` (only if `--username` is set): Propagated username for the canonical identity

### Configuration

Configure normalization steps, matching rules, thresholds, and blacklist by editing the configuration file `merge_config.txt`. Configuration options include:

**Matching Rules:**

- `EMAIL`, `FULL_NAME`, `DOMAIN` - Match by email, name, or domain
- `COMP_EMAIL_PREFIX`, `SIMPLE_EMAIL_PREFIX` - Match by compound or simple email prefix (punctuation normalized)
- `DOMAIN_MAIN_PART`, `DOMAIN_NAME_MATCH` - Match when domain main part or full domain matches normalized name/prefix
- `SWITCHED_NAME` - Match aliases where first and last name are swapped
- `COMMA_SUFFIX_MATCH` - Match names with comma suffix variations
- `PREFIX_NAME`, `LOGIN_NAME`, `PREFIX_LOGIN` - Match email prefix to name, username, or each other
- `ABBREV_FIRST_NAME`, `ABBREV_LAST_NAME`, `ABBREV_MIDDLE_NAME` - Handle name abbreviations
- `GITHUB_USERNAME_MATCH` - Match GitHub usernames (ignores privacy if enabled)

**Thresholds:**

- `THR_MIN`, `THR_MAX` - Cluster size limits (default: 1-40)
- `THR_MIN_LENGTH` - Minimum length of any token (name or email prefix) used for matching (default: 3)

**Normalization:**

- `NORMALIZE_UNICODE_NFC` - Unicode normalization
- `NORMALIZE_LEETSPEAK` - Handle leetspeak (e.g., "c0d3r" → "coder")
- `ENABLE_GERMAN_DETECTION`, `ENABLE_DUAL_NORMALIZATION` - Detect and handle German umlauts with optional dual normalization
- `REMOVE_TIMESTAMPS`, `REMOVE_TIMEZONES`, `REMOVE_YEARS` - Clean temporal patterns
- `REMOVE_TITLES`, `REMOVE_PUNCTUATION` - Clean formatting
- `REMOVE_MIDDLE_NAMES` - Remove single-character or common first name middle names

**Blacklists:**

- `BLACKLIST_GIT_HOSTING`, `BLACKLIST_EMAIL_PROVIDERS` - Filter git hosting platforms and email providers
- `BLACKLIST_GENERIC_TERMS` - Filter generic administrative and system terms
- `BLACKLIST_NO_NAMES` - Filter terms indicating no real name
- `BLACKLIST_MACHINE_NAMES` - Filter machine/hostname patterns
- `BLACKLIST_PRIVACY_EMAILS` - Define privacy-preserving email domains that are only excluded from merging in privacy mode (see below how to enable privacy mode)
- `BLACKLIST_INCLUDE_FIRST_NAMES` - Control whether common first names are included in the blacklist for name-based checks

**Privacy mode:**

`EXCLUDE_PRIVACY_EMAILS_FROM_MERGING` - Decide whether to exclude privacy-preserving emails or not (notice that also matching rule `GITHUB_USERNAME_MATCH` needs to be disabled in addition to achieve privacy mode).

See `merge_config.txt` for the complete list of options and their descriptions.

The default version of GitAuthority with its given configuration file is the privacy-agnostic (i.e., full-accuracy) version of GitAuthority (it enables all rules and normalization steps and sets `EXCLUDE_PRIVACY_EMAILS_FROM_MERGING = false`). For a privacy-aware configuration, set `EXCLUDE_PRIVACY_EMAILS_FROM_MERGING = true` as well as `GITHUB_USERNAME_MATCH = false` while keeping all other configuration options as they are. To do so, you can either change these configuration options in the provided config file, or create a separate config file with these settings and pass it via `--config`.
For other needs, configure all the configuration options in the configuration file to your specific needs, either by editing the provided config file or by creating a separate config file and passing it via `--config`.

### Acknowledgement

To prevent merging identities by names that only consist of common first names, GitAuthority uses a dataset of common first names from the Wiktionary Names Appendix (which contains more than 72 980 first names from different languages). To that aim, the files `Female_given_names.txt` and `Male_given_names.txt` have been extracted from [https://github.com/solvenium/names-dataset/](https://github.com/solvenium/names-dataset/) .
If you would like to prevent other first names being merged than those provided by these datasets, edit the corresponding .txt files manually. If you would like to merge aliases that consist only of common first names, you can configure this in the configuration file by adjusting the value for configuration option `BLACKLIST_INCLUDE_FIRST_NAMES`.

---

## Evaluation

This repository includes several analysis and evaluation components in separate directories. Each directory contains its own README file with detailed instructions. Please find more information on the individual directories below after the description of the datasets.

### Datasets

We have used four ground-truth datasets from the literature to evaluate our tool GitAuthority and compare it with two existing developer-identity de-aliasing approaches (ALFAA and gambit) to demonstrate that it is similarly accurate as established state-of-the-art tools. Note that the four datasets that we used to evaluate GitAuthority and the other two tools are deliberately not part of this replication package. For privacy reasons, we cannot publish these datasets as they contain personal name and email addresses of real people. Anonymizing them is not possible because their actual spellings etc. are necessary to serve as a proper ground truth. Please reach out to the authors of the papers that proposed these datasets (as referenced in our paper) to ask for access to their datasets.

In addition, we created our own ground-truth dataset that contains privacy-sensitive `@users.noreply.github.com` email addresses based on GitHub single-author repository data extracted from World of Code. Similar to the datasets from the literature, for privacy reasons, we cannot publish this dataset as it contains personal name and email addresses of real people. However, you can get access to World of Code for research purposes and then you can create this dataset on your own based on the scripts in this repository as described below. How to get access to World of Code is described in the corresponding README file that is linked below in the section on GitHub Dataset Creation.

If you have access to the datasets, please put the datasets into the following directories:

- The Gnome GTK dataset should be placed into directory `tool_evaluation/dataset_GnomeGTK_Gote/`.
- The WoC+DRE dataset should be placed into directory `tool_evaluation/dataset_DRE_Amreen/`.
- The OpenStack dataset should be placed into directory `tool_evaluation/dataset_OpenStack_Amreen/`.
- The ASF dataset should be placed into directory `tool_evaluation/dataset_ASF_Wiese/`.
- The GitHub ground-truth dataset based on single-author repositories (as created by the scripts in this repository) should be placed into directory `GitHub_dataset_evaluation/GitHub_data/` and should also be copied into directory `GitHub_dataset_evaluation/GitHub_data_privacy-preserving/`. (The reason for adding the data twice is that the corresponding evaluation scripts create filenames for output files based on the directory the data is contained in. Without storing the data twice, result data might be overwritten.)

Similarly, new datasets can easily be added as separate directories following the structure of the existing evaluation scripts. Notice that some of the evaluation scripts even contain configuration dictionaries that allow to easily add new datasets without changing the code itself and selecting the dataset to evaluate via command-line arguments.

### Tool Evaluation

The `tool_evaluation/` directory contains scripts for evaluating the three developer-identity de-aliasing approaches (ALFAA, gambit, and GitAuthority) against various ground-truth datasets from the literature.

See [tool_evaluation/README_TOOL_EVALUATION.md](tool_evaluation/README_TOOL_EVALUATION.md) for:

- Evaluation against multiple ground-truth formats (Matrix format for Gnome GTK dataset, Pairwise format for OpenStack dataset, DRE format for WoC+DRE dataset, and Wiese format for ASF dataset)
- Detailed metrics and performance analysis
- Instructions for each evaluation script

**Notice:** In order to evaluate different variants (i.e., differently configured instances) of GitAuthority, it is necessary to run GitAuthority separately for each configuration. You can either change the configuration file `merge_config.txt` manually in-between runs, or create separate config files and pass them via `--config`. Make sure to not overwrite your previous result data and adjust the corresponding output paths of the tool run accordingly by adjusting the command-line arguments of the evaluation scripts.


### GitHub Dataset Creation

The `GitHub_dataset_creation/` directory contains scripts for creating ground-truth datasets from World of Code data by identifying single-author GitHub repositories.

See [GitHub_dataset_creation/README_GITHUB_DATASET_CREATION.md](GitHub_dataset_creation/README_GITHUB_DATASET_CREATION.md) for:

- Pipeline for extracting and processing World-of-Code data
- Filtering duplicate GitHub usernames and organization accounts
- Creating clean datasets for evaluation

### GitHub Dataset Evaluation

The `GitHub_dataset_evaluation/` directory contains scripts for evaluating GitAuthority, Gambit, and ALFAA against GitHub single-author repository data as ground truth.

See [GitHub_dataset_evaluation/README_GITHUB_DATASET_EVALUATION.md](GitHub_dataset_evaluation/README_GITHUB_DATASET_EVALUATION.md) for:

- Evaluation methodology using GitHub usernames as ground truth
- Scripts for running evaluations with parallel processing
- Computing precision, recall, F1, accuracy, and other metrics
- Confidence interval calculation

**Notice:** In order to evaluate different variants (i.e., differently configured instances) of GitAuthority, it is necessary to run GitAuthority separately for each configuration. You can either change the configuration file `merge_config.txt` manually in-between runs, or create separate config files and pass them via `--config`. Make sure to not overwrite your previous result data and adjust the corresponding output paths of the tool run accordingly.

### Descriptive Statistics

The `descriptive_statistics/` directory contains scripts for extracting and analyzing commit data from World of Code (e.g., the overall number of author aliases in World of Code, the number of commits with `@users.noreply.github.com` email addresses, and similar descriptive statistics based on World of Code that are reported in Table 3 of the paper).

See [descriptive_statistics/README_DESCRIPTIVE_STATISTICS.md](descriptive_statistics/README_DESCRIPTIVE_STATISTICS.md) for:

- Extracting GitHub noreply users from World of Code
- Computing temporal statistics on commit data
- Visualization scripts for commits per year
- World-of-Code author lookup utilities (used to get ALFAA de-aliasing output for the evaluation scripts in the directories explained above).

### Implication Evaluation

The `implication_evaluation/` directory contains scripts for extracting commit data from a local git repository and classifying developers into core and peripheral contributors using a 20/80 heuristic. It uses GitAuthority for de-aliasing authors, and the GitAuthority config file is passed through directly, allowing different configurations to be compared in terms of their effect on core/peripheral developer classification outcomes.

See [implication_evaluation/README_IMPLICATION_EVALUATION.md](implication_evaluation/README_IMPLICATION_EVALUATION.md) for:

- Extracting all commits from a git repository and de-aliasing authors using GitAuthority
- Classifying developers into core and peripheral based on commit counts
- Filtering classification by calendar year
- Descriptive statistics on developer and commit distributions

---

## Questions

If you have any further questions regarding GitAuthority, the used datasets, or the evaluation analyses, please reach out to the authors of this replication package.


