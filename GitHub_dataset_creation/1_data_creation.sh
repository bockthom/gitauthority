#!/bin/bash
#
# Data Creation Script
# =====================
# This script extracts and cleans the p2a (person-to-author) data files from World of Code.
# It performs three main steps:
#   1. Concatenates 32 compressed p2a files into a single file
#   2. Identifies lines with invalid UTF-8 encoding
#   3. Removes invalid UTF-8 lines to create a clean output file
#
# NOTE: This script requires access to the World of Code servers.
#       How to get access can be figured out at https://worldofcode.org/docs/#/getting_started.
#       If you have logged in to the WoC servers, you can access the p2a data there.
#       Once you have access, you can run the following:
#       ./1_data_creation.sh
#
# It is recommended to run this in a tmux terminal session as it may take a long time.

# ============================================================================
# Configuration Variables
# ============================================================================

# Input directory containing the compressed p2a files
INPUT_DIR="/da7_data/basemaps/gz"

# Base name pattern for input files (files are numbered 0-31)
INPUT_FILE_PATTERN="p2aFull.V2412"

# Output file names
OUTPUT_FILE="listofAuthors_justp.txt"
NON_UTF8_FILE="non_utf8_lines_justp.txt"
CLEAN_OUTPUT_FILE="listofAuthors_clean_justp.txt"

# ============================================================================
# Step 1: Extract and concatenate compressed files
# ============================================================================
# Loop through all 32 files (numbered 0-31) and concatenate them into OUTPUT_FILE
for i in {0..31}; do
    zcat "${INPUT_DIR}/${INPUT_FILE_PATTERN}.${i}.s" >> "${OUTPUT_FILE}"
done

# ============================================================================
# Step 2: Extract lines with invalid UTF-8 encoding
# ============================================================================
# Use Perl to decode each line as UTF-8 and capture lines that fail
# FB_CROAK causes decode to die on error, which we catch with eval
# $.:$_ prints the line number followed by the line content
perl -MEncode=decode,FB_CROAK -ne 'eval { decode("UTF-8", $_, FB_CROAK); 1 } or print "$.:$_"' \
  "${OUTPUT_FILE}" > "${NON_UTF8_FILE}"

# ============================================================================
# Step 3: Remove invalid UTF-8 lines from the original file
# ============================================================================
# awk explanation:
# -F: sets field separator to colon (since non_utf8_lines has format "line_number:content")
# NR==FNR: true for first file (non_utf8_lines_justp.txt)
#   bad[$1]: store line numbers in bad array (first field is line number)
#   next: skip to next line
# !(FNR in bad): for second file, only print lines whose line number is NOT in bad array
awk -F: 'NR==FNR { bad[$1]; next } !(FNR in bad)' \
  "${NON_UTF8_FILE}" "${OUTPUT_FILE}" > "${CLEAN_OUTPUT_FILE}"
