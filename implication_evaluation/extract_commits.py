# -*- coding: utf-8 -*-
import sys
import os
import csv
import subprocess
import tempfile
import argparse
import pandas as pd


def get_git_log(repo_path="."):
    """Fetches the git log with commit hash, author, email, and date."""
    previous_path = os.getcwd()
    os.chdir(repo_path)
    cmd = ["git", "log", "--no-merges", "--all", "--pretty=format:%H;%an;%ae;%ad"]
    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", check=True)
    os.chdir(previous_path)
    return result.stdout


def parse_commits(log):
    """Parses git log output into a DataFrame with columns: hash, author, email, date."""
    rows = []
    for line in log.strip().split("\n"):
        if not line.strip():
            continue
        commit_hash, author, email, date_str = line.split(";", 3)
        rows.append({"hash": commit_hash, "author": author, "email": email, "date": date_str})
    return pd.DataFrame(rows, columns=["hash", "author", "email", "date"])


def main():
    parser = argparse.ArgumentParser(description='Extract commits and de-alias authors using gitAuthority')
    parser.add_argument('project', type=str, help='Path to the git repository')
    parser.add_argument('--output-dir', '-d', type=str, default='.',
                        help='Directory for output files (default: current directory)')
    parser.add_argument('--config', '-c', type=str, default=None,
                        help='Path to gitAuthority config file (passed through to gitAuthority)')
    args = parser.parse_args()

    repo_path = args.project
    project_name = os.path.basename(os.path.normpath(repo_path))
    output_dir = args.output_dir

    os.makedirs(output_dir, exist_ok=True)

    print(f"Fetching git log for {project_name}...")
    log = get_git_log(repo_path)
    commit_data = parse_commits(log)
    print(f"Found {len(commit_data)} commits.")

    gitauthority_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "gitAuthority.py")
    unique_authors = commit_data[["author", "email"]].drop_duplicates()

    with tempfile.TemporaryDirectory() as tmpdir:
        authors_file = os.path.join(tmpdir, "authors.txt")
        with open(authors_file, "w", encoding="utf-8") as f:
            for _, row in unique_authors.iterrows():
                f.write(f"{row['author']};{row['email']}\n")

        cmd = [sys.executable, gitauthority_path,
               "--file", authors_file,
               "--name", project_name,
               "--output-dir", tmpdir]
        if args.config:
            cmd += ["--config", args.config]

        print("Calling gitAuthority to de-alias authors...")
        subprocess.run(cmd, check=True)

        merged_csv = os.path.join(tmpdir, f"merged_authors_{project_name}.csv")
        merged = pd.read_csv(merged_csv, sep=';', quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8')

    mapping = {}
    for _, row in merged.iterrows():
        mapping[row["original_author_id"]] = row["dealiased_author_id"]

    def dealias(author, email):
        key = f"{author} <{email}>"
        return mapping.get(key, key)

    commit_data["dealiased_author"] = commit_data.apply(
        lambda r: dealias(r["author"], r["email"]), axis=1
    )

    output_data = pd.DataFrame({
        "project": project_name,
        "dealiased_author": commit_data["dealiased_author"],
        "commit_hash": commit_data["hash"],
        "commit_date": commit_data["date"],
    })

    output_file = os.path.join(output_dir, f"commit_analysis_{project_name}.csv")
    output_data.to_csv(output_file, sep=';', index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8')
    print(f"Written {len(output_data)} rows to {output_file}")


if __name__ == "__main__":
    main()
