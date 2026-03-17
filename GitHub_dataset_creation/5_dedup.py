#!/usr/bin/env python3
"""
Deduplication script to identify authors associated with multiple GitHub usernames.

This script:
1. Reads a CSV file with GitHub_username, author, organization_account columns
2. Identifies authors that appear with multiple different GitHub usernames
3. Generates two output files:
   - One with all entries for authors that have multiple GitHub usernames
   - One with all entries for authors that have only one GitHub username (cleaned data)
"""

import csv
from collections import defaultdict
import sys
import os
import time
from datetime import timedelta

def dedup_gitid_username(input_file, output_multi_usernames, output_clean, output_sorted_multi):
    """
    Process the CSV file to separate authors with multiple GitHub usernames.
    
    Args:
        input_file: Path to input CSV file
        output_multi_usernames: Path to output file for authors with multiple GitHub usernames
        output_clean: Path to output file for clean entries (single GitHub username per author)
        output_sorted_multi: Path to output file for authors with multiple GitHub usernames (sorted by author, then GitHub username)
    """
    print(f"\n[PASS 1/2] Reading input file: {input_file}")
    start_time = time.time()
    
    # Dictionary to store all usernames for each author
    author_to_usernames = defaultdict(set)
    
    # List to store all rows for processing
    all_rows = []
    
    # First pass: read all data and collect usernames per author
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            row_count = 0
            last_update_time = time.time()
            
            for row in reader:
                github_username = row['GitHub_username']
                author = row['author']
                
                # Store the username for this author
                author_to_usernames[author].add(github_username)
                
                # Store the row for later processing
                all_rows.append(row)
                
                row_count += 1
                
                # Show progress every 100k rows or every 5 seconds
                current_time = time.time()
                if row_count % 100000 == 0 or (current_time - last_update_time) >= 5:
                    elapsed = current_time - start_time
                    rate = row_count / elapsed if elapsed > 0 else 0
                    unique_authors = len(author_to_usernames)
                    
                    print(f"  ├─ Rows: {row_count:,} | "
                          f"Unique authors: {unique_authors:,} | "
                          f"Rate: {rate:,.0f} rows/sec | "
                          f"Elapsed: {timedelta(seconds=int(elapsed))}", 
                          flush=True)
                    last_update_time = current_time
            
            elapsed = time.time() - start_time
            print(f"  └─ ✓ Total rows read: {row_count:,} in {timedelta(seconds=int(elapsed))}")
            print(f"     Unique authors found: {len(author_to_usernames):,}")
    
    except Exception as e:
        print(f"✗ Error reading input file: {e}")
        sys.exit(1)
    
    # Identify authors with multiple usernames
    print(f"\n[ANALYSIS] Analyzing author-username relationships...")
    analysis_start = time.time()
    
    authors_with_multiple_usernames = {
        author for author, usernames in author_to_usernames.items() 
        if len(usernames) > 1
    }
    
    analysis_time = time.time() - analysis_start
    print(f"  └─ ✓ Analysis complete in {timedelta(seconds=int(analysis_time))}")
    print(f"\n{'='*70}")
    print(f"  Authors with single username:   {len(author_to_usernames) - len(authors_with_multiple_usernames):,}")
    print(f"  Authors with multiple usernames: {len(authors_with_multiple_usernames):,}")
    print(f"{'='*70}")
    
    # Second pass: write to appropriate output files
    print(f"\n[PASS 2/2] Writing output files...")
    write_start = time.time()
    
    multi_count = 0
    clean_count = 0
    total_rows = len(all_rows)
    
    # Collect multi-username rows for sorting
    multi_username_rows = []
    
    try:
        with open(output_multi_usernames, 'w', encoding='utf-8', newline='') as f_multi, \
             open(output_clean, 'w', encoding='utf-8', newline='') as f_clean:
            
            fieldnames = ['GitHub_username', 'author', 'organization_account']
            writer_multi = csv.DictWriter(f_multi, fieldnames=fieldnames)
            writer_clean = csv.DictWriter(f_clean, fieldnames=fieldnames)
            
            # Write headers
            writer_multi.writeheader()
            writer_clean.writeheader()
            
            last_update_time = time.time()
            
            # Write rows to appropriate file
            for i, row in enumerate(all_rows, 1):
                author = row['author']
                
                if author in authors_with_multiple_usernames:
                    writer_multi.writerow(row)
                    multi_username_rows.append(row)  # Collect for sorting
                    multi_count += 1
                else:
                    writer_clean.writerow(row)
                    clean_count += 1
                
                # Show progress every 100k rows or every 5 seconds
                current_time = time.time()
                if i % 100000 == 0 or (current_time - last_update_time) >= 5:
                    elapsed = current_time - write_start
                    rate = i / elapsed if elapsed > 0 else 0
                    percent = (i / total_rows) * 100
                    eta_seconds = (total_rows - i) / rate if rate > 0 else 0
                    
                    print(f"  ├─ Progress: {i:,}/{total_rows:,} ({percent:.1f}%) | "
                          f"Multi: {multi_count:,} | Clean: {clean_count:,} | "
                          f"Rate: {rate:,.0f} rows/sec | "
                          f"ETA: {timedelta(seconds=int(eta_seconds))}", 
                          flush=True)
                    last_update_time = current_time
            
            elapsed = time.time() - write_start
            print(f"  └─ ✓ All rows written in {timedelta(seconds=int(elapsed))}")
            
    except Exception as e:
        print(f"✗ Error writing output files: {e}")
        sys.exit(1)
    
    # Third pass: create sorted multi-username file
    print(f"\n[PASS 3/3] Creating sorted multi-username file...")
    sort_start = time.time()
    
    try:
        # Sort by author first, then by GitHub_username
        multi_username_rows.sort(key=lambda x: (x['author'], x['GitHub_username']))
        
        with open(output_sorted_multi, 'w', encoding='utf-8', newline='') as f_sorted:
            fieldnames = ['GitHub_username', 'author', 'organization_account']
            writer_sorted = csv.DictWriter(f_sorted, fieldnames=fieldnames)
            
            writer_sorted.writeheader()
            for row in multi_username_rows:
                writer_sorted.writerow(row)
        
        sort_elapsed = time.time() - sort_start
        print(f"  └─ ✓ Sorted file created in {timedelta(seconds=int(sort_elapsed))}")
        
    except Exception as e:
        print(f"✗ Error creating sorted file: {e}")
        sys.exit(1)
    
    print(f"\n{'='*70}")
    print(f"  OUTPUT FILES:")
    print(f"  ├─ Multiple usernames (unsorted): {output_multi_usernames}")
    print(f"  │  Rows: {multi_count:,}")
    print(f"  ├─ Multiple usernames (sorted):   {output_sorted_multi}")
    print(f"  │  Rows: {multi_count:,}")
    print(f"  └─ Clean (single username):       {output_clean}")
    print(f"     Rows: {clean_count:,}")
    print(f"{'='*70}")
    
    # Print some statistics about authors with multiple usernames
    print("\n[STATISTICS] Distribution of usernames per author:")
    multi_username_counts = {}
    for author in authors_with_multiple_usernames:
        username_count = len(author_to_usernames[author])
        if username_count not in multi_username_counts:
            multi_username_counts[username_count] = 0
        multi_username_counts[username_count] += 1
    
    for username_count in sorted(multi_username_counts.keys())[:10]:  # Show top 10
        print(f"  ├─ {username_count} usernames: {multi_username_counts[usernames_count]:,} authors")
    
    if len(multi_username_counts) > 10:
        print(f"  └─ ... and {len(multi_username_counts) - 10} more categories")
    
    # Show some examples
    print("\n[EXAMPLES] Sample authors with multiple usernames (first 10):")
    for i, author in enumerate(list(authors_with_multiple_usernames)[:10], 1):
        usernames = sorted(author_to_usernames[author])
        username_preview = usernames[:3]
        if len(usernames) > 3:
            username_preview_str = f"{', '.join(username_preview)}... (+{len(usernames)-3} more)"
        else:
            username_preview_str = ', '.join(username_preview)
        print(f"  {i:2d}. '{author}' -> {len(usernames)} usernames: [{username_preview_str}]")
    
    # Overall timing
    total_time = time.time() - start_time
    print(f"\n{'='*70}")
    print(f"✓ DEDUPLICATION COMPLETE!")
    print(f"  Total time: {timedelta(seconds=int(total_time))}")
    print(f"{'='*70}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Deduplicate authors by identifying those associated with multiple GitHub usernames"
    )
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Input CSV file with GitHub_username, author, organization_account columns"
    )
    parser.add_argument(
        "--output-multi",
        type=str,
        default=None,
        help="Output file for authors with multiple usernames (default: <input>_authors_with_multiple_usernames.csv)"
    )
    parser.add_argument(
        "--output-clean",
        type=str,
        default=None,
        help="Output file for clean entries (default: <input>_after_dedup.csv)"
    )
    parser.add_argument(
        "--output-sorted",
        type=str,
        default=None,
        help="Output file for sorted multi-username entries (default: <input>_authors_with_multiple_usernames_sorted.csv)"
    )
    
    args = parser.parse_args()
    
    # Generate default output file names if not provided
    base_name = args.input.rsplit('.', 1)[0] if '.' in args.input else args.input
    
    output_multi_usernames = args.output_multi or f"{base_name}_authors_with_multiple_usernames.csv"
    output_clean = args.output_clean or f"{base_name}_after_dedup.csv"
    output_sorted_multi = args.output_sorted or f"{base_name}_authors_with_multiple_usernames_sorted.csv"
    
    # Check if input file exists
    if not os.path.exists(args.input):
        print(f"Error: Input file '{args.input}' not found!", file=sys.stderr)
        sys.exit(1)
    
    print("\n" + "=" * 70)
    print("  GitHub username to Author Deduplication Script")
    print("=" * 70)
    print(f"  Input:  {args.input}")
    print(f"  Output: {output_multi_usernames}")
    print(f"          {output_clean}")
    print(f"          {output_sorted_multi}")
    print("=" * 70)
    
    script_start = time.time()
    dedup_gitid_username(args.input, output_multi_usernames, output_clean, output_sorted_multi)
    
    print(f"\n✓ Script completed successfully!")
    print(f"  Total execution time: {timedelta(seconds=int(time.time() - script_start))}\n")

