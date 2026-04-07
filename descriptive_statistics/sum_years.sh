#!/bin/bash

# Script to sum counts by year from 128 CSV files
# Input files: *_0.csv to *_127.csv (space-separated: count year)
# Output: aggregated_by_year.csv (comma-separated: count,year)

# Check if base filename is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <base_filename>"
    echo "Example: $0 data (for files data_0.csv to data_127.csv)"
    exit 1
fi

BASE_NAME="$1"
OUTPUT_FILE="aggregated_by_year.csv"

# Create a temporary file for aggregation
TEMP_FILE=$(mktemp)

# Read all 128 files
for i in {0..127}; do
    INPUT_FILE="${BASE_NAME}_${i}.csv"

    if [ ! -f "$INPUT_FILE" ]; then
        echo "Warning: File $INPUT_FILE not found, skipping..."
        continue
    fi

    # Read the file, trim leading spaces, and extract count and year
    while read -r line; do
        # Skip empty lines
        [ -z "$line" ] && continue

        # Trim leading/trailing spaces and extract count and year
        read -r count year <<< "$line"

        # Skip if count or year is empty
        [ -z "$count" ] || [ -z "$year" ] && continue

        # Write to temp file
        echo "$year $count" >> "$TEMP_FILE"
    done < "$INPUT_FILE"
done

# Sort by year and sum counts
awk '{sum[$1] += $2} END {for (year in sum) print sum[year] "," year}' "$TEMP_FILE" | \
    sort -t',' -k2,2n > "$OUTPUT_FILE"

# Clean up
rm -f "$TEMP_FILE"

echo "Aggregation complete. Output written to $OUTPUT_FILE"
echo "Total years processed: $(wc -l < "$OUTPUT_FILE")"
