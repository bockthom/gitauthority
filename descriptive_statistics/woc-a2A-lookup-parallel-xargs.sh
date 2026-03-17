#!/bin/bash

# Parallelized version of woc-a2A-lookup script using xargs
# This version doesn't require GNU parallel
# Usage: ./woc-a2A-lookup-parallel-xargs.sh <input_file> <output_file> [num_jobs]

if [ $# -lt 2 ] || [ $# -gt 3 ]; then
    echo "Usage: $0 <input_file> <output_file> [num_jobs]"
    echo "  input_file: File containing lines to lookup"
    echo "  output_file: File to write the results to"
    echo "  num_jobs: Number of parallel jobs (default: number of CPU cores)"
    exit 1
fi

INPUT_FILE="$1"
OUTPUT_FILE="$2"
NUM_JOBS="${3:-0}"  # Default to 0 (which means use all cores)

# Check if input file exists
if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: Input file '$INPUT_FILE' not found"
    exit 1
fi

# Determine number of jobs
if [ "$NUM_JOBS" -eq 0 ]; then
    NUM_JOBS=$(nproc)
    echo "Using all $NUM_JOBS CPU cores"
else
    echo "Using $NUM_JOBS parallel jobs"
fi

# Get total lines for progress reporting
TOTAL_LINES=$(wc -l < "$INPUT_FILE")
echo "Processing $TOTAL_LINES lines from $INPUT_FILE..."

# Create a temporary file for unsorted output
TEMP_OUTPUT=$(mktemp)

# Process lines in parallel using xargs
# Use null-terminated strings to handle special characters properly
cat "$INPUT_FILE" | \
    grep -v '^[[:space:]]*$' | \
    tr '\n' '\0' | \
    xargs -0 -P "$NUM_JOBS" -I {} -n 1 \
        sh -c 'echo "$1" | ~/lookup/getValues -vV2409 a2A' _ {} >> "$TEMP_OUTPUT"

# Sort the output and write to final output file
echo "Sorting results..."
sort "$TEMP_OUTPUT" > "$OUTPUT_FILE"

# Clean up temporary file
rm -f "$TEMP_OUTPUT"

echo "Processing complete. Results written to $OUTPUT_FILE (sorted)"
