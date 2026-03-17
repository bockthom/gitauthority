#!/usr/bin/env python3
"""
Extract GitHub noreply users from World of Code (WoC) data.

This script:
1. Runs zcat command to search for @users.noreply.github.com in a2PFull files
2. Reads the resulting semicolon-separated data
3. Extracts and processes user data into multiple files:
   - uniqueAuthorStrings.csv: unique author strings (name + email)
   - uniqueEmailStrings.csv: unique email addresses (cleaned, full email)
   - uniqueUsernameStrings.csv: unique username strings (id+username part before @)
   - uniqueUsernames.csv: unique usernames (merged id+username with username)
   - uniqueNumericIds.csv: unique numeric IDs extracted from id+username patterns
   - summary.csv: statistics for each processing step
"""

import subprocess
import csv
import os
import sys
import re
import unicodedata

def remove_control_characters(text):
    """Remove control characters and normalize unicode."""
    if not isinstance(text, str):
        return text

    # Normalize to NFC form
    text = unicodedata.normalize('NFC', text)

    # Remove control characters except newline, tab, carriage return
    cleaned = ''.join(char for char in text if unicodedata.category(char)[0] != 'C' or char in '\n\t\r')

    return cleaned

def clean_email_string(email):
    """
    Clean email address:
    - Remove control characters
    - Remove various quotation marks around the address
    - Strip whitespace
    """
    if not isinstance(email, str) or not email:
        return email

    # Remove control characters
    email = remove_control_characters(email)

    # Remove various quotation marks (straight quotes, angle brackets, etc.)
    quotation_marks = ['"', "'", '<', '>', '\u201c', '\u201d', '\u2018', '\u2019', '`']
    for mark in quotation_marks:
        email = email.strip(mark)

    # Strip whitespace
    email = email.strip()

    return email

def extract_username_from_email(email):
    """
    Extract the username part (id+username before @) from an email address.
    Returns the cleaned username or None if not a valid email.
    """
    if not isinstance(email, str) or '@' not in email:
        return None

    # Split at @ and take the part before
    username = email.split('@')[0]

    # Clean the username
    username = remove_control_characters(username)
    username = username.strip()

    return username if username else None

def extract_numeric_id(username):
    """
    Extract numeric ID from username strings like '12345+username'.
    Returns the numeric ID or None if no ID prefix exists.
    """
    if not isinstance(username, str):
        return None

    # Check for ID+username pattern (digits followed by +)
    match = re.match(r'^(\d+)\+', username)

    if match:
        return match.group(1)

    return None

def normalize_username_with_id(username):
    """
    Normalize usernames that have ID prefixes like '12345+username'.
    Returns a tuple: (normalized_username, had_id_prefix)

    Examples:
    - '12345+john' -> ('john', True)
    - 'john' -> ('john', False)
    - '123456789+bob-smith' -> ('bob-smith', True)
    """
    if not isinstance(username, str):
        return username, False

    # Check for ID+username pattern (digits followed by + and then username)
    match = re.match(r'^(\d+)\+(.+)$', username)

    if match:
        # Extract the username part (after the +)
        normalized = match.group(2)
        return normalized, True

    return username, False

def main():
    # Increase CSV field size limit to handle large fields
    csv.field_size_limit(sys.maxsize)
    # Output file prefix
    OUTPUT_PREFIX = "WoC_githubNoreplyUsers"

    # Step 1: Run zcat command to extract noreply users
    print("Running zcat command to extract GitHub noreply users...")
    output_file = f"{OUTPUT_PREFIX}_a2P.csv"

    command = 'zcat /da?_data/basemaps/gz/a2PFull.V2412.*.s | grep "@users.noreply.github.com"'

    try:
        with open(output_file, 'w') as f:
            result = subprocess.run(
                command,
                shell=True,
                stdout=f,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
        print(f"Successfully created {output_file}")
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {e}")
        print(f"Error output: {e.stderr}")
        return

    # Step 2: Read the CSV file and extract unique elements
    print(f"Reading {output_file}...")

    # Statistics tracking
    stats = {
        'total_author_strings': 0,
        'total_projects': 0,
        'unique_author_strings': 0,
        'unique_email_strings_cleaned': 0,
        'unique_username_strings': 0,
        'unique_numeric_ids': 0,
        'unique_usernames_with_id': 0,
        'unique_usernames_without_id': 0,
        'unique_usernames_merged': 0,
        'id_prefixes_found': 0,
        'successful_merges': 0
    }

    unique_author_strings = set()
    unique_email_addresses = set()
    unique_projects = set()

    # Regular expression to extract email from <...> at the end of the string
    email_pattern = re.compile(r'<([^>]+)>\s*$')

    try:
        with open(output_file, 'r') as f:
            reader = csv.reader(f, delimiter=';')
            for row in reader:
                if len(row) >= 2:
                    stats['total_author_strings'] += 1
                    author_string = row[0]
                    unique_author_strings.add(author_string)

                    # Extract email address from <...> at the end
                    match = email_pattern.search(author_string)
                    if match:
                        email = match.group(1)
                        # Clean the email string immediately after extraction
                        email = clean_email_string(email)
                        unique_email_addresses.add(email)

                    unique_projects.add(row[1])

        stats['unique_author_strings'] = len(unique_author_strings)
        stats['unique_email_strings_cleaned'] = len(unique_email_addresses)
        stats['total_projects'] = len(unique_projects)

        print(f"Found {stats['total_author_strings']} total author strings")
        print(f"Found {stats['unique_author_strings']} unique author strings")
        print(f"Found {stats['unique_email_strings_cleaned']} unique email addresses (cleaned)")
        print(f"Found {stats['total_projects']} unique projects")
    except FileNotFoundError:
        print(f"Error: {output_file} not found")
        return
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    # Step 3: Extract usernames from cleaned email addresses
    print("\nExtracting usernames from email addresses...")
    username_strings = set()
    numeric_ids = set()

    for email in unique_email_addresses:
        # Extract username string (part before @)
        username = extract_username_from_email(email)
        if username:
            username_strings.add(username)

            # Extract numeric ID if present
            numeric_id = extract_numeric_id(username)
            if numeric_id:
                numeric_ids.add(numeric_id)
                stats['id_prefixes_found'] += 1

    stats['unique_username_strings'] = len(username_strings)
    stats['unique_numeric_ids'] = len(numeric_ids)

    print(f"  Cleaned {stats['unique_email_strings_cleaned']} unique email addresses")
    print(f"  Extracted {stats['unique_username_strings']} unique username strings")
    print(f"  Found {stats['unique_numeric_ids']} unique numeric IDs")

    # Step 4: Normalize usernames (merge ID+ versions with non-ID versions)
    print("\nNormalizing usernames (merging id+username with username)...")
    username_mapping = {}  # Maps normalized -> list of original variants
    usernames_with_id = set()
    usernames_without_id = set()

    for username in username_strings:
        normalized, had_id = normalize_username_with_id(username)

        if normalized not in username_mapping:
            username_mapping[normalized] = []

        username_mapping[normalized].append(username)

        if had_id:
            usernames_with_id.add(username)
        else:
            usernames_without_id.add(username)

    stats['unique_usernames_with_id'] = len(usernames_with_id)
    stats['unique_usernames_without_id'] = len(usernames_without_id)
    stats['unique_usernames_merged'] = len(username_mapping)

    # Count successful merges (where ID+ and non-ID versions were merged)
    for normalized, variants in username_mapping.items():
        if len(variants) > 1:
            stats['successful_merges'] += 1

    print(f"  Usernames with ID prefix: {stats['unique_usernames_with_id']}")
    print(f"  Usernames without ID prefix: {stats['unique_usernames_without_id']}")
    print(f"  Unique usernames after merging: {stats['unique_usernames_merged']}")
    print(f"  Successful merges: {stats['successful_merges']}")

    # Step 5: Write all output files
    print("\nWriting output files...")

    # 5.1: Write unique author strings
    author_strings_output = f"{OUTPUT_PREFIX}_uniqueAuthorStrings.csv"
    try:
        with open(author_strings_output, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['author_string'])
            for author_string in sorted(unique_author_strings):
                writer.writerow([author_string])
        print(f"  ✓ {author_strings_output} ({len(unique_author_strings)} records)")
    except Exception as e:
        print(f"  ✗ Error writing author strings file: {e}")
        return

    # 5.2: Write unique email strings (cleaned)
    email_strings_output = f"{OUTPUT_PREFIX}_uniqueEmailStrings.csv"
    try:
        with open(email_strings_output, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['email'])
            for email in sorted(unique_email_addresses):
                writer.writerow([email])
        print(f"  ✓ {email_strings_output} ({stats['unique_email_strings_cleaned']} records)")
    except Exception as e:
        print(f"  ✗ Error writing email strings file: {e}")
        return

    # 5.3: Write unique username strings (id+username part before @)
    username_strings_output = f"{OUTPUT_PREFIX}_uniqueUsernameStrings.csv"
    try:
        with open(username_strings_output, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['username_string'])
            for username in sorted(username_strings):
                writer.writerow([username])
        print(f"  ✓ {username_strings_output} ({len(username_strings)} records)")
    except Exception as e:
        print(f"  ✗ Error writing username strings file: {e}")
        return

    # 5.4: Write unique usernames (merged)
    usernames_output = f"{OUTPUT_PREFIX}_uniqueUsernames.csv"
    try:
        with open(usernames_output, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['username', 'variant_count', 'variants', 'has_id_prefix', 'has_plain_form', 'merged'])
            for normalized in sorted(username_mapping.keys()):
                variants = username_mapping[normalized]
                has_id_variant = any(re.match(r'^\d+\+', v) for v in variants)
                has_plain_variant = any(not re.match(r'^\d+\+', v) for v in variants)
                writer.writerow([
                    normalized,
                    len(variants),
                    ';'.join(sorted(set(variants))),
                    has_id_variant,
                    has_plain_variant,
                    has_id_variant and has_plain_variant
                ])
        print(f"  ✓ {usernames_output} ({len(username_mapping)} records)")
    except Exception as e:
        print(f"  ✗ Error writing usernames file: {e}")
        return

    # 5.5: Write unique numeric IDs
    numeric_ids_output = f"{OUTPUT_PREFIX}_uniqueNumericIds.csv"
    try:
        with open(numeric_ids_output, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['numeric_id'])
            for numeric_id in sorted(numeric_ids, key=int):
                writer.writerow([numeric_id])
        print(f"  ✓ {numeric_ids_output} ({len(numeric_ids)} records)")
    except Exception as e:
        print(f"  ✗ Error writing numeric IDs file: {e}")
        return

    # 5.6: Write unique projects
    projects_output = f"{OUTPUT_PREFIX}_uniqueProjects.csv"
    try:
        with open(projects_output, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['project'])
            for project in sorted(unique_projects):
                writer.writerow([project])
        print(f"  ✓ {projects_output} ({len(unique_projects)} records)")
    except Exception as e:
        print(f"  ✗ Error writing projects file: {e}")
        return

    # 5.7: Write summary statistics
    summary_output = f"{OUTPUT_PREFIX}_summary.csv"
    try:
        with open(summary_output, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['step', 'count', 'description'])

            summary_records = [
                ('Total author strings', stats['total_author_strings'], 'Original author strings in input data'),
                ('Unique author strings', stats['unique_author_strings'], 'Unique author strings'),
                ('Total unique projects', stats['total_projects'], 'Unique projects'),
                ('', '', ''),
                ('Unique emails (cleaned)', stats['unique_email_strings_cleaned'], 'Unique email addresses (cleaned during extraction)'),
                ('', '', ''),
                ('Unique username strings', stats['unique_username_strings'], 'Unique username strings extracted from emails'),
                ('Unique numeric IDs', stats['unique_numeric_ids'], 'Unique numeric IDs extracted from id+username patterns'),
                ('Usernames with ID prefix', stats['unique_usernames_with_id'], 'Usernames with digit+username pattern'),
                ('Usernames without ID prefix', stats['unique_usernames_without_id'], 'Usernames without ID prefix'),
                ('ID prefixes found', stats['id_prefixes_found'], 'Total ID+ patterns detected'),
                ('', '', ''),
                ('Unique usernames (merged)', stats['unique_usernames_merged'], 'Unique usernames after merging ID+ with plain'),
                ('Successful merges', stats['successful_merges'], 'Cases where ID+ and plain versions were merged'),
                ('Reduction from merging', stats['unique_username_strings'] - stats['unique_usernames_merged'],
                 'Username count reduction from merging')
            ]

            for step, count, description in summary_records:
                writer.writerow([step, count, description])
        print(f"  ✓ {summary_output}")
    except Exception as e:
        print(f"  ✗ Error writing summary file: {e}")
        return

    # Print summary to console
    print("\n" + "="*80)
    print("SUMMARY STATISTICS")
    print("="*80)
    print(f"Total author strings .......................... {stats['total_author_strings']:>10,}")
    print(f"Unique author strings ......................... {stats['unique_author_strings']:>10,}")
    print(f"Unique projects ............................... {stats['total_projects']:>10,}")
    print()
    print(f"Unique emails (cleaned) ....................... {stats['unique_email_strings_cleaned']:>10,}")
    print()
    print(f"Unique username strings ....................... {stats['unique_username_strings']:>10,}")
    print(f"Unique numeric IDs ............................ {stats['unique_numeric_ids']:>10,}")
    print(f"  - Usernames with ID prefix .................. {stats['unique_usernames_with_id']:>10,}")
    print(f"  - Usernames without ID prefix ............... {stats['unique_usernames_without_id']:>10,}")
    print()
    print(f"Unique usernames (merged) ..................... {stats['unique_usernames_merged']:>10,}")
    print(f"  - Successful merges ......................... {stats['successful_merges']:>10,}")
    print(f"  - Reduction from merging .................... {stats['unique_username_strings'] - stats['unique_usernames_merged']:>10,}")
    print("="*80)
    print("\n✓ All files written successfully!")

if __name__ == "__main__":
    main()
