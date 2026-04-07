#!/usr/bin/env python3
"""
Optimized GitHub org checker using GraphQL API.
Can query up to 100 users per request instead of 1.
Supports multiple GitHub tokens for faster processing.

Returns binary flag:
  - 1 = Organization
  - 0 = User, not found, or anything else

Usage:
python3 5_orgcheck_graphql.py --input file.csv --output output.csv --batch-size 100 --tokens token1 token2 token3
"""

import os
import time
import argparse
import pandas as pd
import requests
from typing import Dict, List
import concurrent.futures
import threading
from queue import Queue

# GitHub tokens for faster processing - add your tokens here
# NOTE: Replace these with your own GitHub tokens
GITHUB_TOKENS = [
    # Add your GitHub tokens here
    # Example: "github_pat_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
]

class GitHubOrgChecker:
    def __init__(self, tokens: List[str] = None, batch_size: int = 100):
        # Use provided tokens or fall back to GITHUB_TOKENS
        if tokens:
            self.tokens = tokens
        elif GITHUB_TOKENS:
            self.tokens = GITHUB_TOKENS
        else:
            raise ValueError("No GitHub tokens provided. Please set GITHUB_TOKENS or pass tokens argument.")
        self.batch_size = min(batch_size, 100)  # GraphQL allows max 100 nodes
        self.url = "https://api.github.com/graphql"
        self.results_lock = threading.Lock()
        self.progress_lock = threading.Lock()
        
    def get_headers(self, token: str) -> Dict[str, str]:
        """Get headers with specific token."""
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        
    def build_query(self, usernames: List[str]) -> str:
        """Build GraphQL query for multiple users at once."""
        # Create aliases for each user
        user_queries = []
        for i, username in enumerate(usernames):
            alias = f"user{i}"
            # Escape usernames for GraphQL (replace special chars)
            safe_username = username.replace('"', '\\"')
            user_queries.append(f'{alias}: user(login: "{safe_username}") {{ login __typename }}')
        
        query_body = "\n    ".join(user_queries)
        query = f"""
        query {{
            {query_body}
        }}
        """
        return query
    
    def fetch_batch_with_token(self, usernames: List[str], token: str) -> Dict[str, int]:
        """Fetch org info for a batch of usernames using a specific token."""
        query = self.build_query(usernames)
        session = requests.Session()
        
        while True:
            try:
                response = session.post(
                    self.url,
                    json={"query": query},
                    headers=self.get_headers(token),
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Check for errors
                    if "errors" in data:
                        # GraphQL can have partial errors - log them but continue
                        for error in data.get("errors", []):
                            print(f"GraphQL warning (token {token[:8]}...): {error.get('message', 'Unknown error')}")
                    
                    results = {}
                    if "data" in data and data["data"]:
                        for i, username in enumerate(usernames):
                            alias = f"user{i}"
                            user_data = data["data"].get(alias)
                            
                            if user_data is None:
                                # User not found - treat as non-org (0)
                                results[username] = 0
                            elif user_data.get("__typename") == "Organization":
                                results[username] = 1
                            else:
                                # User or anything else - treat as non-org (0)
                                results[username] = 0
                    else:
                        # No data returned, mark all as non-org (0)
                        results = {h: 0 for h in usernames}
                    
                    return results
                
                elif response.status_code in (403, 429):  # Rate limited
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        sleep_for = int(float(retry_after)) + 1
                    else:
                        reset = response.headers.get("X-RateLimit-Reset")
                        sleep_for = max(5, int(reset) - int(time.time()) + 1) if reset else 60
                    print(f"Rate limited (token {token[:8]}...). Sleeping {sleep_for}s...")
                    time.sleep(sleep_for)
                    continue
                
                elif 500 <= response.status_code < 600:  # Server error
                    print(f"Server error {response.status_code} (token {token[:8]}...), retrying in 5s...")
                    time.sleep(5)
                    continue
                
                else:
                    print(f"HTTP {response.status_code} (token {token[:8]}...): {response.text[:200]}")
                    # Return non-org (0) for all usernames in this batch
                    return {h: 0 for h in usernames}
                    
            except Exception as e:
                print(f"Request error (token {token[:8]}...): {e}, retrying in 5s...")
                time.sleep(5)
                continue
    
    # def fetch_batch(self, usernames: List[str]) -> Dict[str, int]:
    #     """
    #     Fetch org info for a batch of usernames.
    #     Returns dict: {username: org_flag}
    #     """
    #     query = self.build_query(usernames)
        
    #     while True:
    #         try:
    #             response = self.session.post(
    #                 self.url,
    #                 json={"query": query},
    #                 headers=self.get_headers(),
    #                 timeout=30
    #             )
                
    #             if response.status_code == 200:
    #                 data = response.json()
                    
    #                 # Check for errors
    #                 if "errors" in data:
    #                     # GraphQL can have partial errors - log them but continue
    #                     for error in data.get("errors", []):
    #                         print(f"GraphQL warning: {error.get('message', 'Unknown error')}")
                    
    #                 results = {}
    #                 if "data" in data and data["data"]:
    #                     for i, username in enumerate(usernames):
    #                         alias = f"user{i}"
    #                         user_data = data["data"].get(alias)
                            
    #                         if user_data is None:
    #                             # User not found
    #                             results[username] = -1
    #                         elif user_data.get("__typename") == "Organization":
    #                             results[username] = 1
    #                         elif user_data.get("__typename") == "User":
    #                             results[username] = 0
    #                         else:
    #                             results[username] = -1
    #                 else:
    #                     # No data returned, mark all as unknown
    #                     results = {h: -1 for h in usernames}
                    
    #                 return results
                
    #             elif response.status_code in (403, 429):  # Rate limited
    #                 retry_after = response.headers.get("Retry-After")
    #                 if retry_after:
    #                     sleep_for = int(float(retry_after)) + 1
    #                 else:
    #                     reset = response.headers.get("X-RateLimit-Reset")
    #                     sleep_for = max(5, int(reset) - int(time.time()) + 1) if reset else 60
    #                 print(f"Rate limited. Sleeping {sleep_for}s...")
    #                 time.sleep(sleep_for)
    #                 continue
                
    #             elif 500 <= response.status_code < 600:  # Server error
    #                 print(f"Server error {response.status_code}, retrying in 5s...")
    #                 time.sleep(5)
    #                 continue
                
    #             else:
    #                 print(f"HTTP {response.status_code}: {response.text[:200]}")
    #                 # Return unknown for all usernames in this batch
    #                 return {h: -1 for h in usernames}
                    
    #         except Exception as e:
    #             print(f"Request error: {e}, retrying in 5s...")
    #             time.sleep(5)
    #             continue
    
    def process_usernames_parallel(
        self,
        usernames: List[str],
        save_callback=None,
        progress_callback=None
    ) -> Dict[str, int]:
        """
        Process all usernames in parallel using multiple tokens.
        Returns dict: {username: org_flag}
        """
        total = len(usernames)
        results = {}
        processed_count = 0
        
        # Split usernames into batches
        batches = []
        for i in range(0, total, self.batch_size):
            batch = usernames[i:i + self.batch_size]
            batches.append(batch)
        
        print(f"Processing {len(batches)} batches with {len(self.tokens)} tokens in parallel...")
        
        # Create work queue with (batch, token) pairs
        work_queue = Queue()
        for i, batch in enumerate(batches):
            token = self.tokens[i % len(self.tokens)]
            work_queue.put((batch, token, i))
        
        # Process batches in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.tokens)) as executor:
            # Submit all work
            future_to_batch = {}
            while not work_queue.empty():
                batch, token, batch_idx = work_queue.get()
                future = executor.submit(self.fetch_batch_with_token, batch, token)
                future_to_batch[future] = (batch, batch_idx, token)
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_batch):
                batch, batch_idx, token = future_to_batch[future]
                try:
                    batch_results = future.result()
                    
                    # Thread-safe update of results
                    with self.results_lock:
                        results.update(batch_results)
                        processed_count += len(batch)
                    
                    # Progress callback with token info
                    if progress_callback:
                        with self.progress_lock:
                            progress_callback(processed_count, total, batch_results, token)
                    
                    # Save checkpoint (thread-safe)
                    if save_callback and (processed_count % (self.batch_size * 10) == 0 or processed_count == total):
                        with self.results_lock:
                            save_callback(results, processed_count, total)
                    
                except Exception as e:
                    print(f"Error processing batch {batch_idx} (token {token[:8]}...): {e}")
                    # Mark all usernames in this batch as non-org (0)
                    batch_results = {h: 0 for h in batch}
                    with self.results_lock:
                        results.update(batch_results)
                        processed_count += len(batch)
        
        return results
    
    def process_usernames(
        self,
        usernames: List[str],
        save_callback=None,
        progress_callback=None
    ) -> Dict[str, int]:
        """
        Process all usernames - uses parallel processing if multiple tokens available.
        Returns dict: {usernames: org_flag}
        """
        if len(self.tokens) > 1:
            return self.process_usernames_parallel(usernames, save_callback, progress_callback)
        else:
            # Fall back to sequential processing for single token
            return self._process_usernames_sequential(usernames, save_callback, progress_callback)
    
    def _process_usernames_sequential(
        self,
        usernames: List[str],
        save_callback=None,
        progress_callback=None
    ) -> Dict[str, int]:
        """
        Sequential processing fallback for single token.
        """
        results = {}
        total = len(usernames)
        
        # Process in batches
        for i in range(0, total, self.batch_size):
            batch = usernames[i:i + self.batch_size]
            batch_results = self.fetch_batch_with_token(batch, self.tokens[0])
            results.update(batch_results)
            
            # Progress
            processed = min(i + self.batch_size, total)
            if progress_callback:
                progress_callback(processed, total, batch_results, self.tokens[0])
            
            # Save checkpoint
            if save_callback and (processed % (self.batch_size * 10) == 0 or processed == total):
                save_callback(results, processed, total)
        
        return results


def add_org_flag_graphql(
    df: pd.DataFrame,
    username_col: str = "GitHub_username",
    batch_size: int = 100,
    save_every_batches: int = 10,
    output_path: str = "annotated.csv",
    tokens: List[str] = None,
):
    """
    Add organization_account column using GraphQL API with parallel processing.
    Much faster than REST API - queries 100 users per request.
    Supports multiple tokens for true parallel processing (Nx speedup with N tokens).
    Each token processes different batches simultaneously using ThreadPoolExecutor.
    
    Returns binary flag:
      - 1 = Organization
      - 0 = User, not found, or anything else
    """
    
    # Normalize and deduplicate usernames
    print("Normalizing usernames...")
    norm_usernames = (
        df[username_col].astype("string")
        .str.strip()
        .str.replace("^@", "", regex=True)
        .str.lower()
    )
    unique_usernames = norm_usernames.dropna().unique().tolist()
    print(f"Found {len(unique_usernames):,} unique usernames to check")
    
    # Initialize checker
    checker = GitHubOrgChecker(tokens=tokens, batch_size=batch_size)
    print(f"Using {len(checker.tokens)} GitHub token(s) for faster processing")
    
    # Callbacks
    def save_checkpoint(results, processed, total):
        df_out = df.copy()
        df_out["organization_account"] = norm_usernames.map(results)
        tmp = output_path + ".tmp"
        df_out.to_csv(tmp, index=False)
        os.replace(tmp, output_path)
        print(f"[checkpoint] {processed:,}/{total:,} usernames done ({processed/total*100:.1f}%), saved to {output_path}")
    
    def show_progress(processed, total, batch_results, token_info=None):
        # Show some examples from this batch
        examples = list(batch_results.items())[:3]
        example_str = ", ".join([f"{h}={v}" for h, v in examples])
        percentage = (processed / total) * 100
        
        # Add token info if available
        token_str = f" (token {token_info[:8]}...)" if token_info else ""
        print(f"[{processed:,}/{total:,}] ({percentage:.1f}%) Processed batch{token_str}: {example_str}")
    
    # Process all usernames
    print("Starting GraphQL queries...")
    start_time = time.time()
    results = checker.process_usernames(
        unique_usernames,
        save_callback=save_checkpoint,
        progress_callback=show_progress
    )
    
    elapsed = time.time() - start_time
    print(f"\n✅ Completed in {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
    print(f"   Rate: {len(unique_usernames)/elapsed:.1f} usernames/sec")
    print(f"   Parallel processing with {len(checker.tokens)} token(s)")
    if len(checker.tokens) > 1:
        theoretical_speedup = len(checker.tokens)
        print(f"   Theoretical speedup: {theoretical_speedup}x faster than single token")
    
    # Final save
    df_out = df.copy()
    df_out["organization_account"] = norm_usernames.map(results)
    tmp = output_path + ".tmp"
    df_out.to_csv(tmp, index=False)
    os.replace(tmp, output_path)
    print(f"[done] Final output saved to {output_path}")
    
    # Summary
    org_count = sum(1 for v in results.values() if v == 1)
    non_org_count = sum(1 for v in results.values() if v == 0)
    print(f"\nSummary:")
    print(f"  Organizations: {org_count:,}")
    print(f"  Non-organizations (users/not found): {non_org_count:,}")
    
    return df_out


def main():
    parser = argparse.ArgumentParser(
        description="Check GitHub org/user status using GraphQL API with multiple tokens (much faster!)"
    )
    parser.add_argument("--input", required=True, help="Input CSV file")
    parser.add_argument("--output", required=True, help="Output CSV file")
    parser.add_argument("--column", default="GitHub_username", help="Column with GitHub usernames")
    parser.add_argument("--batch-size", type=int, default=100, 
                       help="Batch size (max 100 for GraphQL)")
    parser.add_argument("--tokens", nargs="+", help="GitHub tokens (space-separated, optional)")
    args = parser.parse_args()
    
    print(f"Reading {args.input}...")
    df = pd.read_csv(args.input)
    print(f"Loaded {len(df):,} rows")
    
    df = df.dropna(subset=[args.column])
    print(f"After dropping NaN in '{args.column}': {len(df):,} rows")
    
    add_org_flag_graphql(
        df,
        username_col=args.column,
        batch_size=args.batch_size,
        output_path=args.output,
        tokens=args.tokens
    )


if __name__ == "__main__":
    main()

