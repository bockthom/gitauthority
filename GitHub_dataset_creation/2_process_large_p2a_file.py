import pandas as pd
import numpy as np
from collections import defaultdict, Counter
import re
import io
import os
import time
import sys
import signal
from datetime import datetime

def log_with_timestamp(message):
    """Print message with timestamp for better tmux monitoring"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")
    sys.stdout.flush()  # Force immediate output

def process_large_file_with_error_handling(filename, chunksize=100000, save_frequency=1000000, output_file=None):
    """Process large file in chunks, save both problematic lines and results frequently"""
    
    repo_counts = Counter()
    single_author_data = []
    problematic_lines_buffer = []
    total_processed = 0
    total_errors = 0
    problematic_file = 'problematic_lines_justp.txt'
    results_file = output_file or 'Dealiased_Master_Dataset_cleaned_justp.csv'
    temp_results_file = 'temp_results_justp.csv'
    last_save_point = 0
    start_time = time.time()
    
    # Initialize files
    initialize_problematic_file(problematic_file)
    initialize_results_file(temp_results_file)
    
    log_with_timestamp(f"Processing chunks with error handling (saving every {save_frequency:,} lines)...")
    log_with_timestamp(f"Problematic lines: {problematic_file}")
    log_with_timestamp(f"Temp results: {temp_results_file}")
    
    # Create progress tracking
    progress_file = 'processing_progress.txt'
    
    # Read file line by line first, clean it, then process in chunks
    cleaned_lines = []
    current_chunk_lines = []
    
    with open(filename, 'rb') as f:
        for line_num, line_bytes in enumerate(f):
            total_processed += 1
            
            if line_num % 1000000 == 0:
                elapsed = time.time() - start_time
                rate = line_num / elapsed if elapsed > 0 else 0
                
                log_with_timestamp(f"📊 Pre-processing: {line_num:,} lines, "
                                 f"errors: {total_errors}, "
                                 f"buffer: {len(problematic_lines_buffer)}, "
                                 f"rate: {rate:.0f} lines/sec")
                
                # Save progress to file
                with open(progress_file, 'w') as pf:
                    pf.write(f"Lines processed: {line_num:,}\n")
                    pf.write(f"Errors found: {total_errors:,}\n")
                    pf.write(f"Processing rate: {rate:.0f} lines/sec\n")
                    pf.write(f"Elapsed time: {elapsed/3600:.1f} hours\n")
                    pf.write(f"Last updated: {datetime.now()}\n")
            
            try:
                # Try to decode the line
                try:
                    line = line_bytes.decode('utf-8').strip()
                except UnicodeDecodeError:
                    line = line_bytes.decode('latin1', errors='ignore').strip()
                
                # Skip empty lines
                if not line:
                    continue
                
                # Check for problematic patterns
                is_problematic = False
                error_reasons = []
                
                # Check for control characters (except normal whitespace)
                if re.search(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', line):
                    is_problematic = True
                    error_reasons.append("CONTROL_CHARS")
                
                # Check for missing semicolon
                if ';' not in line:
                    is_problematic = True
                    error_reasons.append("NO_SEMICOLON")
                
                # Check for too many semicolons (might indicate malformed data)
                if line.count(';') > 10:
                    is_problematic = True
                    error_reasons.append(f"TOO_MANY_SEMICOLONS({line.count(';')})")
                
                # Check for unescaped quotes that would break CSV parsing
                if '"' in line:
                    # Simple heuristic: if quotes aren't properly paired, it's problematic
                    quote_count = line.count('"')
                    if quote_count % 2 != 0:  # Odd number of quotes
                        is_problematic = True
                        error_reasons.append("UNESCAPED_QUOTES")
                
                # Check for extremely long lines
                if len(line) > 10000:
                    is_problematic = True
                    error_reasons.append(f"VERY_LONG_LINE({len(line)})")
                
                # Check for very long fields
                if ';' in line:
                    parts = line.split(';', 1)
                    if len(parts[0]) > 1000:
                        is_problematic = True
                        error_reasons.append(f"LONG_REPO({len(parts[0])})")
                    if len(parts[1]) > 5000:
                        is_problematic = True
                        error_reasons.append(f"LONG_AUTHOR({len(parts[1])})")
                
                if is_problematic:
                    total_errors += 1
                    problematic_lines_buffer.append({
                        'line_number': line_num + 1,
                        'errors': ', '.join(error_reasons),
                        'line_content': line[:500] + ('...' if len(line) > 500 else ''),
                        'original_line': line
                    })
                else:
                    # Clean the line for safe CSV processing
                    clean_line = line
                    
                    # Remove control characters
                    clean_line = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', clean_line)
                    
                    # Handle quotes by escaping them properly
                    if ';' in clean_line:
                        parts = clean_line.split(';', 1)
                        repo = parts[0].strip()
                        author = parts[1].strip()
                        
                        # Escape quotes properly for CSV
                        repo = repo.replace('"', '""')
                        author = author.replace('"', '""')
                        
                        # If field contains comma or quote, wrap in quotes
                        if ',' in repo or '"' in repo:
                            repo = f'"{repo}"'
                        if ',' in author or '"' in author:
                            author = f'"{author}"'
                        
                        clean_line = f"{repo};{author}"
                        current_chunk_lines.append(clean_line)
                
                # Process in chunks
                if len(current_chunk_lines) >= chunksize:
                    process_chunk(current_chunk_lines, repo_counts, len(current_chunk_lines))
                    current_chunk_lines = []
                
                # Save both problematic lines and intermediate results frequently
                if line_num - last_save_point >= save_frequency:
                    if problematic_lines_buffer:
                        append_problematic_lines(problematic_file, problematic_lines_buffer)
                        log_with_timestamp(f"💾 Saved {len(problematic_lines_buffer):,} problematic lines (total errors: {total_errors:,})")
                        problematic_lines_buffer = []
                    
                    # Save intermediate repo counts
                    save_intermediate_repo_counts(repo_counts, f"intermediate_repo_counts_{line_num}.txt")
                    log_with_timestamp(f"💾 Saved intermediate repo counts at line {line_num:,}")
                    
                    last_save_point = line_num
                    
            except Exception as e:
                total_errors += 1
                problematic_lines_buffer.append({
                    'line_number': line_num + 1,
                    'errors': f'PROCESSING_ERROR: {str(e)}',
                    'line_content': str(line_bytes)[:500],
                    'original_line': str(line_bytes)
                })
    
    # Process remaining lines
    if current_chunk_lines:
        process_chunk(current_chunk_lines, repo_counts, len(current_chunk_lines))
    
    # Save any remaining problematic lines
    if problematic_lines_buffer:
        append_problematic_lines(problematic_file, problematic_lines_buffer)
        log_with_timestamp(f"💾 Final save: {len(problematic_lines_buffer):,} problematic lines")
    
    total_time = time.time() - start_time
    log_with_timestamp(f"\nPre-processing complete:")
    log_with_timestamp(f"Total lines processed: {total_processed:,}")
    log_with_timestamp(f"Problematic lines found: {total_errors:,}")
    log_with_timestamp(f"Clean lines for processing: {total_processed - total_errors:,}")
    log_with_timestamp(f"Processing time: {total_time/3600:.1f} hours")
    
    # Generate summary
    generate_error_summary(problematic_file)
    
    log_with_timestamp("First pass complete. Finding single-author repos...")
    
    # Find repos with only one author
    single_author_repos = {repo for repo, count in repo_counts.items() if count == 1}
    log_with_timestamp(f"Found {len(single_author_repos):,} single-author repos")
    
    # Save single-author repos list
    save_single_author_repos(single_author_repos, 'single_author_repos_justp.txt')
    
    # Second pass: extract single-author repo data with incremental saving
    log_with_timestamp("Second pass: extracting single-author data...")
    single_author_data = []
    processed_chunks = 0
    second_pass_start = time.time()
    
    with open(filename, 'rb') as f:
        current_chunk_lines = []
        
        for line_num, line_bytes in enumerate(f):
            if line_num % 1000000 == 0:
                elapsed = time.time() - second_pass_start
                rate = line_num / elapsed if elapsed > 0 else 0
                log_with_timestamp(f"📊 Second pass: {line_num:,} lines, rate: {rate:.0f} lines/sec")
            
            try:
                # Same cleaning process as before
                try:
                    line = line_bytes.decode('utf-8').strip()
                except UnicodeDecodeError:
                    line = line_bytes.decode('latin1', errors='ignore').strip()
                
                if not line or ';' not in line:
                    continue
                
                # Skip if it would be problematic
                if (re.search(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', line) or
                    line.count(';') > 10 or len(line) > 10000 or
                    ('"' in line and line.count('"') % 2 != 0)):
                    continue
                
                # Clean and parse
                clean_line = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', line)
                parts = clean_line.split(';', 1)
                
                if len(parts) == 2:
                    repo = parts[0].strip().replace('"', '')
                    author = parts[1].strip().replace('"', '')
                    
                    if repo in single_author_repos and author:
                        current_chunk_lines.append(f"{repo};{author}")
                
                # Process in chunks and save incrementally
                if len(current_chunk_lines) >= chunksize:
                    chunk_df = process_single_author_chunk(current_chunk_lines)
                    if not chunk_df.empty:
                        single_author_data.append(chunk_df)
                        processed_chunks += 1
                        
                        # Save results every few chunks
                        if processed_chunks % 10 == 0:  # Every 10 chunks
                            save_incremental_results(single_author_data, temp_results_file, processed_chunks)
                            log_with_timestamp(f"💾 Saved {processed_chunks} chunks to temp results")
                    
                    current_chunk_lines = []
                    
            except Exception:
                continue
        
        # Process remaining lines
        if current_chunk_lines:
            chunk_df = process_single_author_chunk(current_chunk_lines)
            if not chunk_df.empty:
                single_author_data.append(chunk_df)
    
    # Final processing and save
    if single_author_data:
        log_with_timestamp("Final processing: combining all chunks...")
        df_single = pd.concat(single_author_data, ignore_index=True)
        df_single = df_single.drop_duplicates()
        
        # Find duplicates
        username_counts = df_single['GitHub_username'].value_counts()
        duplicate_usernames = username_counts[username_counts > 1].index
        df_duplicates = df_single[df_single['GitHub_username'].isin(duplicate_usernames)]
        df_duplicates = df_duplicates.sort_values('GitHub_username').reset_index(drop=True)
        
        # Save final results
        df_duplicates.to_csv(results_file, index=False)
        log_with_timestamp(f"✅ Final results saved to {results_file}")
        
        # Clean up temp files
        cleanup_temp_files()
        
        return df_duplicates
    
    return pd.DataFrame()

def initialize_problematic_file(filename):
    """Initialize the problematic lines file with headers"""
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("Line_Number,Error_Types,Line_Content\n")
    log_with_timestamp(f"📝 Initialized problematic lines file: {filename}")

def append_problematic_lines(filename, problematic_lines):
    """Append problematic lines to the file"""
    with open(filename, 'a', encoding='utf-8') as f:
        for prob in problematic_lines:
            # Escape commas and quotes for CSV
            content = prob['line_content'].replace('"', '""')
            if ',' in content:
                content = f'"{content}"'
            
            f.write(f"{prob['line_number']},{prob['errors']},\"{content}\"\n")

def process_chunk(lines, repo_counts, chunk_size):
    """Process a chunk of clean lines"""
    try:
        # Create a string buffer that pandas can read
        chunk_data = '\n'.join(lines)
        chunk_df = pd.read_csv(io.StringIO(chunk_data), sep=';', header=None, 
                              names=['repo', 'author'], na_filter=False)
        
        # Remove empty rows
        chunk_df = chunk_df[(chunk_df['repo'] != '') & (chunk_df['author'] != '')]
        
        # Count repos in this chunk
        chunk_repo_counts = chunk_df['repo'].value_counts()
        repo_counts.update(chunk_repo_counts.to_dict())
        
    except Exception as e:
        log_with_timestamp(f"Error processing chunk of {chunk_size} lines: {e}")

def process_single_author_chunk(lines):
    """Process a chunk for single-author extraction"""
    try:
        chunk_data = '\n'.join(lines)
        chunk_df = pd.read_csv(io.StringIO(chunk_data), sep=';', header=None, 
                              names=['repo', 'author'], na_filter=False)
        
        if not chunk_df.empty:
            chunk_df['GitHub_username'] = chunk_df['repo'].str.split('_').str[0]
            return chunk_df[['GitHub_username', 'author']].drop_duplicates()
    except Exception as e:
        log_with_timestamp(f"Error processing single-author chunk: {e}")
    
    return pd.DataFrame()

def generate_error_summary(filename):
    """Generate and display summary of error types from the saved file"""
    if not os.path.exists(filename):
        log_with_timestamp("No problematic lines file found for summary.")
        return
    
    log_with_timestamp(f"\n📊 Generating error summary from {filename}...")
    
    error_summary = {}
    total_lines = 0
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            f.readline()  # Skip header
            
            for line in f:
                total_lines += 1
                try:
                    parts = line.strip().split(',', 2)  # Split into max 3 parts
                    if len(parts) >= 2:
                        errors = parts[1].split(', ')
                        for error in errors:
                            error_type = error.split('(')[0]  # Remove details in parentheses
                            error_summary[error_type] = error_summary.get(error_type, 0) + 1
                except Exception:
                    continue
        
        log_with_timestamp(f"\n📈 Error Summary ({total_lines:,} total problematic lines):")
        log_with_timestamp("-" * 50)
        for error_type, count in sorted(error_summary.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total_lines) * 100 if total_lines > 0 else 0
            log_with_timestamp(f"  {error_type:<20}: {count:>8,} ({percentage:5.1f}%)")
            
    except Exception as e:
        log_with_timestamp(f"Error generating summary: {e}")

def initialize_results_file(filename):
    """Initialize results file with headers"""
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("GitHub_username,author\n")
    log_with_timestamp(f"📝 Initialized temp results file: {filename}")

def save_intermediate_repo_counts(repo_counts, filename):
    """Save intermediate repo counts"""
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("repo,count\n")
        for repo, count in repo_counts.most_common():
            f.write(f"{repo},{count}\n")

def save_single_author_repos(single_repos, filename):
    """Save list of single-author repos"""
    with open(filename, 'w', encoding='utf-8') as f:
        for repo in sorted(single_repos):
            f.write(f"{repo}\n")
    log_with_timestamp(f"💾 Saved {len(single_repos):,} single-author repos to {filename}")

def save_incremental_results(single_author_data, temp_file, chunk_num):
    """Save incremental results"""
    if single_author_data:
        df_temp = pd.concat(single_author_data, ignore_index=True)
        df_temp = df_temp.drop_duplicates()
        df_temp.to_csv(f"temp_chunk_{chunk_num}.csv", index=False)

def cleanup_temp_files():
    """Clean up temporary files"""
    import glob
    temp_files = glob.glob("intermediate_repo_counts_*.txt") + glob.glob("temp_chunk_*.csv")
    for temp_file in temp_files:
        try:
            os.remove(temp_file)
            log_with_timestamp(f"🧹 Cleaned up {temp_file}")
        except:
            pass

# Signal handling for graceful shutdown
def signal_handler(sig, frame):
    log_with_timestamp('🛑 Received interrupt signal. Saving current progress...')
    log_with_timestamp('Process interrupted by user')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Main execution
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Process large p2a file to identify single-author repos and extract duplicates"
    )
    parser.add_argument(
        "--input",
        type=str,
        default="listofAuthors_clean_justp.txt",
        help="Input file path (default: listofAuthors_clean_justp.txt)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="Dealiased_Master_Dataset_cleaned_justp.csv",
        help="Output CSV file path (default: Dealiased_Master_Dataset_cleaned_justp.csv)"
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=100000,
        help="Chunk size for processing (default: 100000)"
    )
    parser.add_argument(
        "--save-frequency",
        type=int,
        default=1000000,
        help="Save frequency in lines (default: 1000000)"
    )
    
    args = parser.parse_args()
    
    log_with_timestamp("🚀 Starting dealiasing process")
    log_with_timestamp(f"Process ID: {os.getpid()}")
    log_with_timestamp(f"Input file: {args.input}")
    log_with_timestamp(f"Output file: {args.output}")
    
    if not os.path.exists(args.input):
        log_with_timestamp(f"❌ ERROR: Input file '{args.input}' not found!")
        sys.exit(1)
    
    try:
        # Update the results file path in the function
        df_result_cleaned = process_large_file_with_error_handling(
            args.input,
            chunksize=args.chunksize,
            save_frequency=args.save_frequency,
            output_file=args.output
        )
        
        if not df_result_cleaned.empty:
            log_with_timestamp(f"✅ SUCCESS! Saved {len(df_result_cleaned)} rows to {args.output}")
        else:
            log_with_timestamp("⚠️  No results generated")
            
    except Exception as e:
        log_with_timestamp(f"💥 FATAL ERROR: {str(e)}")
        import traceback
        log_with_timestamp(f"Traceback: {traceback.format_exc()}")
        raise
    
    log_with_timestamp("🎯 Script completed successfully")
