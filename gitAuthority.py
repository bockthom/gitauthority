# -*- coding: utf-8 -*-

import sys
import os
import csv
import subprocess
import re
import argparse
import io
from collections import defaultdict
from datetime import datetime
import numpy as np
import pandas as pd
import math
from mergeAliases import *


def get_git_log(repo_path = "."):
    """Fetches the git log with commit time and author."""
    previous_path = os.getcwd()
    os.chdir(repo_path)
    cmd = ["git", "log", "--no-merges", "--all", "--pretty=format:%an;%ae;%ad;%at"]
    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", check=True)
    os.chdir(previous_path)
    return result.stdout

def get_author_data_from_woc(project, woc_version, with_username=False):
    """Fetches the author data from the WOC repository.
    Args:
        project (str): The project name (owner_name).
        woc_version (str): The WOC version.
        with_username (bool): If True, add an empty username column.
    Returns:
    """

    command = f"echo '{project}' | ~/lookup/getValues -f p2a -v{woc_version} | cut -d\\; -f2 | perl -I ~/lookup/ -ane 'use woc; chop();print \"\".(join \";\", git_signature_parse($_)).\"\n\"'"
    project_authors = []

    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    for line in io.TextIOWrapper(proc.stdout, encoding="utf-8", errors="ignore"):
        parts = line.split(";")
        author, email = parts[0], parts[1]

        project_authors.append((author.strip(), email.strip()))

    # transform project_authors to a dataframe with columns author and email
    project_authors_df = pd.DataFrame(project_authors, columns=["author", "email"]).drop_duplicates()
    project_authors_df.insert(0, 'uid', range(1, len(project_authors_df) + 1))
    if with_username:
        project_authors_df.columns = ['uid', 'name', 'email']
        project_authors_df['login'] = ''
    else:
        project_authors_df.columns = ['uid', 'name', 'email']
    return project_authors_df

def get_author_data_from_file(file_path, with_username=False):
    """Reads author data from a file containing author name <author-email> format.
    Args:
        file_path (str): Path to the file containing author data.
        with_username (bool): If True, expect a username as the last semicolon-separated
            field on each line (e.g. "author;email;username" or "author <email>;username").
    Returns:
        DataFrame: DataFrame with columns uid, name, email[, username]
    """
    project_authors = []

    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            username = ''
            if with_username:
                # Username is always the last semicolon-separated field
                last_semi = line.rfind(';')
                if last_semi != -1:
                    username = line[last_semi + 1:].strip()
                    line = line[:last_semi].strip()

            # Parse format: "author name <author-email>"
            # Handle both semicolon-separated and angle-bracket format
            if ';' in line:
                # Format: author;email
                parts = line.split(';')
                if len(parts) >= 2:
                    author = parts[0].strip()
                    email = parts[1].strip()
                    project_authors.append((author, email, username))
            elif '<' in line and '>' in line:
                # Format: author name <email>
                match = re.match(r'^(.+?)\s*<(.+?)>$', line)
                if match:
                    author = match.group(1).strip()
                    email = match.group(2).strip()
                    project_authors.append((author, email, username))
            else:
                # Try to split by whitespace and assume last token is email
                parts = line.rsplit(None, 1)
                if len(parts) == 2:
                    author, email = parts[0].strip(), parts[1].strip()
                    project_authors.append((author, email, username))

    # Transform to DataFrame
    if with_username:
        project_authors_df = pd.DataFrame(project_authors, columns=["author", "email", "login"]).drop_duplicates()
        project_authors_df.insert(0, 'uid', range(1, len(project_authors_df) + 1))
        project_authors_df.columns = ['uid', 'name', 'email', 'login']
        # If name is empty but login is set, use login as name so the identity participates in matching
        mask = (project_authors_df['name'] == '') & (project_authors_df['login'] != '')
        project_authors_df.loc[mask, 'name'] = project_authors_df.loc[mask, 'login']
    else:
        project_authors_df = pd.DataFrame([(a, e) for a, e, _ in project_authors], columns=["author", "email"]).drop_duplicates()
        project_authors_df.insert(0, 'uid', range(1, len(project_authors_df) + 1))
        project_authors_df.columns = ['uid', 'name', 'email']
    return project_authors_df

def parse_log(log):
    """Parses the git log into a dictionary of commit times by committer."""
    commit_data = pd.DataFrame(columns=["author", "email", "date", "timestamp"])
    for line in log.strip().split("\n"):
        author, email, date_str, timestamp = line.split(";", 3)
        date = date_str #datetime.strptime(date_str, "%a %b %d %H:%M:%S %Y %z")
        # add author, email, date, timestamp to the DataFrame
        new_row = pd.DataFrame({"author": [author], "email": [email], "date": [date], "timestamp": [timestamp]})
        commit_data = pd.concat([commit_data, new_row], ignore_index=True)
        #commit_times[author].append(local_time.hour)

        author_data = commit_data[["author", "email"]].drop_duplicates()
        author_data.insert(0, 'uid', range(1, len(author_data) + 1))
        author_data.columns = ['uid', 'name', 'email']
        #print(author_data)

    return author_data


def perform_merging(author_data, file_path, config_path=None):
    """Performs the merging of author data with the provided file.
    Args:
        author_data (DataFrame): The author data to merge.
        file_path (str): The path where the output CSV files will be saved.
        config_path (str): Path to the config file (default: merge_config.txt in script directory).
    """
    merging_dict = do_merging(author_data, file_path, config_path)
    return merging_dict

def store_commit_data_to_csv(commit_data, filename):
    """Stores the commit data to a CSV file."""
    commit_data.to_csv(filename, sep=';', index=False, quoting=csv.QUOTE_NONNUMERIC)
    print(f"Commit data stored in {filename}")

def read_commit_data_from_csv(filename):
    """Reads the commit data from a CSV file."""
    commit_data = pd.read_csv(filename, sep=';', quoting=csv.QUOTE_NONNUMERIC)
    print(f"Commit data read from {filename}")

    return commit_data

def merge_author_data_per_project(author_data, project_name, output_path, config_path=None, append=False, drop_boolean_column=False):
    """Merges author data for a specific project.
    Args:
        author_data (DataFrame): The author data to merge.
        project_name (str): The name of the project (used in output filename).
        output_path (str): The path where the output CSV file will be saved.
        config_path (str): Path to the config file (default: merge_config.txt in script directory).
        append (bool): If True, append to existing output file instead of overwriting.
        drop_boolean_column (bool): If True, drop the is_original_author_weird_id column from the output.
    """

    output_dir = "."
    merged_author_data = perform_merging(author_data, output_dir, config_path)

    # Iterate through the mapping dictionary
    has_username = False
    rows = []
    for key, value in merged_author_data.items():
        # Unpack the key and value tuples
        key_data = json.loads(key)
        value_data = json.loads(value)

        # Extract all values at once
        uid1, author1, email1, is_weird_id1 = key_data["uid"], key_data["name"], key_data["email"], key_data["is_weird_id"]
        uid2, author2, email2 = value_data["uid"], value_data["name"], value_data["email"]
        login1 = key_data.get("login", None)
        if login1 is not None:
            has_username = True

        composed_author1 = f"{author1} <{email1}>"
        composed_author2 = f"{author2} <{email2}>"

        rows.append({"project": project_name,
                     "original_author_id": composed_author1,
                     "dealialized_author_id": composed_author2,
                     "is_original_author_weird_id": is_weird_id1,
                     "username": login1 if login1 is not None else "",
                     "cluster_id": uid2})

    author_data = pd.DataFrame(rows)
    if has_username:
        # Propagate the most frequent non-empty username within each cluster.
        # Group by cluster_id (the representative's uid) rather than dealialized_author_id,
        # so that distinct singleton aliases that happen to share the same canonical string
        # are never conflated with each other.
        def most_frequent_username(x):
            non_empty = [v for v in x if v]
            if not non_empty:
                return ""
            unique = set(non_empty)
            if len(unique) > 1:
                print(f"Warning: multiple usernames found for the same identity: {unique}. Picking most frequent.")
            return max(unique, key=non_empty.count)
        author_data["username"] = author_data.groupby("cluster_id")["username"].transform(most_frequent_username)
        author_data = author_data.drop(columns=["cluster_id"], errors='ignore')
    else:
        author_data = author_data.drop(columns=["username", "cluster_id"], errors='ignore')

    if drop_boolean_column:
        author_data = author_data.drop(columns=["is_original_author_weird_id"], errors='ignore')

    # Sort by original_author_id (case-insensitive) before writing to disk
    if not author_data.empty:
        author_data = author_data.sort_values('original_author_id', key=lambda x: x.str.lower())
    # write author_data to disk
    if append and os.path.exists(output_path):
        mode = 'a'
    else:
        mode = 'w'
    header = not append
    author_data.to_csv(output_path, sep=';', mode=mode, header=header, index=False, quoting=csv.QUOTE_NONNUMERIC)


def main():
    parser = argparse.ArgumentParser(
        description='Process author data from World of Code or from a file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # WoC mode with project and version
  %(prog)s --project torvalds_linux --woc-version V2412

  # File mode with author data file
  %(prog)s --file authors.txt --name my_project

  # File mode with author data file with username column (last semicolon-separated field)
  %(prog)s --file authors.txt --username
  # Input line examples with --username:
  #   author;email;username
  #   author name <email>;username
  #   author name email;username

  # Use a custom config file
  %(prog)s --file authors.txt --config /path/to/my_config.txt

  # Drop the is_original_author_weird_id boolean column from the output
  %(prog)s --file authors.txt --drop-boolean-column

  # Backward compatibility (positional arguments)
  %(prog)s torvalds_linux V2412
        """
    )

    # Create mutually exclusive group for WoC vs file mode
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--project', '-p', type=str, help='Project name for WoC lookup (e.g., torvalds_linux)')
    mode_group.add_argument('--file', '-f', type=str, help='Path to file containing author data (format: "name <email>" or "name;email")')

    # Additional arguments
    parser.add_argument('--woc-version', '-v', type=str, default='V2412', help='WoC version (default: V2412)')
    parser.add_argument('--output-dir', '-o', type=str, default='.', help='Output directory for results (default: current directory)')
    parser.add_argument('--name', '-n', type=str, help='Project name for file mode (used in output filename)')
    parser.add_argument('--config', '-c', type=str, default=None, help='Path to config file (default: merge_config.txt in script directory)')
    parser.add_argument('--username', '-u', action='store_true', help='Expect and propagate a username field. In file mode, username is the last semicolon-separated field on each line (e.g. "author;email;username" or "author name <email>;username").')
    parser.add_argument('--drop-boolean-column', action='store_true', help='Drop the is_original_author_weird_id column from the output')

    # Positional arguments for backward compatibility
    parser.add_argument('positional_args', nargs='*', help=argparse.SUPPRESS)

    args = parser.parse_args()

    try:
        # Handle backward compatibility with positional arguments
        if args.positional_args and not args.project and not args.file:
            if len(args.positional_args) >= 1:
                args.project = args.positional_args[0]
            if len(args.positional_args) >= 2:
                args.woc_version = args.positional_args[1]
            if len(args.positional_args) >= 3:
                args.output_dir = args.positional_args[2]

        # Determine mode and get author data
        if args.file:
            # File mode
            if not os.path.exists(args.file):
                print(f"Error: File '{args.file}' not found")
                sys.exit(1)

            print(f"Reading author data from file: {args.file}")
            author_data = get_author_data_from_file(args.file, with_username=args.username)

            # Determine project name for output
            if args.name:
                project_name = args.name
            else:
                # Use filename without extension as project name
                project_name = os.path.splitext(os.path.basename(args.file))[0]
        elif args.project:
            # WoC mode
            print(f"Fetching author data from WoC for project: {args.project} (version: {args.woc_version})")
            author_data = get_author_data_from_woc(args.project, args.woc_version, with_username=args.username)
            project_name = args.project
        else:
            parser.print_help()
            print("\nError: Either --project or --file must be specified")
            sys.exit(1)

        print(f"Found {len(author_data)} unique authors")

        # Create output directory if it doesn't exist
        if args.output_dir and not os.path.exists(args.output_dir):
            os.makedirs(args.output_dir)
            print(f"Created output directory: {args.output_dir}")

        # Generate output path
        output_path = os.path.join(args.output_dir, f"merged_authors_{project_name}.csv")

        # Perform merging
        merge_author_data_per_project(author_data, project_name, output_path, args.config, drop_boolean_column=args.drop_boolean_column)

        print(f"Results written to: {output_path}")

    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running git log: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
