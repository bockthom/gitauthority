#!/usr/bin/env python3
"""
Filter and clean the master dataset.
Removes duplicates and filters for duplicate GitHub usernames.
"""

import pandas as pd
import argparse
import sys
import os


def filter_dataset(input_file, output_file, exclude_usernames=None):
    """
    Filter the dataset to keep only entries with duplicate GitHub usernames.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
        exclude_usernames: List of GitHub usernames to exclude (default: ['bb', 'gl'] for Bitbucket and GitLab, which are excluded because organization check only applies to GitHub)
    """
    if exclude_usernames is None:
        exclude_usernames = ['bb', 'gl']
    
    print(f"Reading {input_file}...")
    df = pd.read_csv(input_file)
    print(f"  Loaded {len(df):,} rows")
    
    # Remove duplicates
    print("\nRemoving duplicates...")
    df = df.drop_duplicates()
    print(f"  After removing duplicates: {len(df):,} rows")
    
    # Find usernames that appear more than once
    print("\nFiltering for duplicate usernames...")
    username_counts = df['GitHub_username'].value_counts()
    duplicate_usernames = username_counts[username_counts > 1].index
    df_filtered = df[df['GitHub_username'].isin(duplicate_usernames)]
    print(f"  Usernames with duplicates: {len(duplicate_usernames):,}")
    print(f"  Rows with duplicate usernames: {len(df_filtered):,}")
    
    # Exclude specific usernames
    if exclude_usernames:
        print(f"\nExcluding usernames: {exclude_usernames}")
        before_exclude = len(df_filtered)
        df_filtered = df_filtered[~df_filtered['GitHub_username'].isin(exclude_usernames)]
        after_exclude = len(df_filtered)
        print(f"  Excluded {before_exclude - after_exclude:,} rows")
        print(f"  Remaining rows: {len(df_filtered):,}")
    
    # Save filtered dataset
    print(f"\nSaving filtered dataset to {output_file}...")
    df_filtered.to_csv(output_file, index=False)
    print(f"  ✓ Saved {len(df_filtered):,} rows to {output_file}")
    
    return df_filtered


def main():
    parser = argparse.ArgumentParser(
        description="Filter dataset to keep only entries with duplicate GitHub usernames"
    )
    parser.add_argument(
        "--input",
        type=str,
        default="Dealiased_Master_Dataset_cleaned_justp.csv",
        help="Input CSV file (default: Dealiased_Master_Dataset_cleaned_justp.csv)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="Dealiased_Master_Dataset_filtered_justp.csv",
        help="Output CSV file (default: Dealiased_Master_Dataset_filtered_justp.csv)"
    )
    parser.add_argument(
        "--exclude",
        nargs="+",
        default=['bb', 'gl'],
        help="GitHub usernames to exclude (default: bb gl for Bitbucket and GitLab, which are excluded because organization check only applies to GitHub)"
    )
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input):
        print(f"Error: Input file '{args.input}' not found!", file=sys.stderr)
        sys.exit(1)
    
    filter_dataset(args.input, args.output, args.exclude)
    print("\n✓ Filtering complete!")


if __name__ == "__main__":
    main()

