#!/usr/bin/env python3
"""
Extract unique values from the second column of a semicolon-separated CSV file.
"""
import argparse
import csv
import sys


def main():
    # Increase CSV field size limit to handle large fields
    csv.field_size_limit(sys.maxsize)

    parser = argparse.ArgumentParser(
        description='Extract unique values from second column of a CSV file'
    )
    parser.add_argument('input_file', help='Input CSV file path')
    parser.add_argument('output_file', help='Output CSV file path')
    args = parser.parse_args()

    unique_values = set()

    try:
        # Read input file and collect unique values from second column
        with open(args.input_file, 'r', encoding='utf-8') as infile:
            reader = csv.reader(infile, delimiter=';')
            for row in reader:
                if len(row) >= 2:
                    unique_values.add(row[1])

        # Sort the unique values
        sorted_values = sorted(unique_values)

        # Write to output file
        with open(args.output_file, 'w', encoding='utf-8', newline='') as outfile:
            writer = csv.writer(outfile, delimiter=';')
            for value in sorted_values:
                writer.writerow([value])

        print(f"Successfully extracted {len(sorted_values)} unique values from second column")
        print(f"Output written to: {args.output_file}")

    except FileNotFoundError:
        print(f"Error: Input file '{args.input_file}' not found", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
