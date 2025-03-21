import concurrent.futures
import os
import math
from array import array
import io
from collections import defaultdict

# Use all available cores plus additional workers for I/O-bound operations
NUM_THREADS = max(8, os.cpu_count() * 2)

def round_to_infinity(x, digits=1):
    """Fastest implementation of ceiling rounding"""
    factor = 10 ** digits
    return math.ceil(x * factor) / factor

def process_file_chunk(file_path, start_pos, chunk_size):
    """Process a chunk of file based on byte position with minimal operations"""
    results = {}
    
    with open(file_path, 'rb') as f:
        # Seek to the starting position
        f.seek(start_pos)
        
        # Skip to the next line if not at the beginning
        if start_pos > 0:
            f.readline()
        
        # Read the chunk
        data = f.read(chunk_size)
        if not data:
            return {}
            
        # Split lines and process
        lines = data.split(b'\n')
        
        # Fast processing with minimal operations
        for line in lines:
            if not line:
                continue
                
            # Fast split using byte operations
            parts = line.split(b';', 1)  # Split only on first semicolon
            if len(parts) != 2:
                continue
                
            try:
                city = parts[0].decode('ascii', errors='ignore').strip()
                if not city:
                    continue
                    
                score = float(parts[1])
                
                # Initialize city entry if needed
                if city not in results:
                    # Store as (count, sum, min, max) for efficient updates
                    results[city] = [0, 0.0, float('inf'), float('-inf')]
                
                # Update running statistics
                stats = results[city]
                stats[0] += 1        # count
                stats[1] += score    # sum
                stats[2] = min(stats[2], score)  # min
                stats[3] = max(stats[3], score)  # max
                
            except (ValueError, UnicodeDecodeError):
                continue
    
    return results

def merge_results(results_list):
    """Merge results from multiple chunks efficiently"""
    merged = {}
    
    for results in results_list:
        for city, stats in results.items():
            if city not in merged:
                merged[city] = stats.copy()
            else:
                # Update merged statistics
                merged[city][0] += stats[0]      # count
                merged[city][1] += stats[1]      # sum
                merged[city][2] = min(merged[city][2], stats[2])  # min
                merged[city][3] = max(merged[city][3], stats[3])  # max
    
    return merged

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    # Get file size for chunking
    file_size = os.path.getsize(input_file_name)
    
    # Calculate optimal chunk size (aiming for ~100MB per chunk)
    chunk_size = max(1024 * 1024, file_size // NUM_THREADS)  # At least 1MB
    
    # Create chunks based on byte positions
    chunks = []
    for start_pos in range(0, file_size, chunk_size):
        end_pos = min(start_pos + chunk_size, file_size)
        chunks.append((start_pos, end_pos - start_pos))
    
    # Process chunks in parallel, storing only running statistics
    results_list = []
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=NUM_THREADS) as executor:
        # Using ProcessPoolExecutor instead of ThreadPoolExecutor for better CPU utilization
        future_to_chunk = {
            executor.submit(process_file_chunk, input_file_name, start_pos, size): 
            (start_pos, size) for start_pos, size in chunks
        }
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_chunk):
            results_list.append(future.result())
    
    # Merge results efficiently
    merged_results = merge_results(results_list)
    
    # Write results directly to file
    with open(output_file_name, "w") as output_file:
        # Pre-sort cities for faster output generation
        for city in sorted(merged_results.keys()):
            count, total, min_val, max_val = merged_results[city]
            if count == 0:
                continue
                
            mean_val = total / count
            
            # Format with minimal string operations
            output_file.write(f"{city}={round_to_infinity(min_val, 1)}/"
                             f"{round_to_infinity(mean_val, 1)}/"
                             f"{round_to_infinity(max_val, 1)}\n")

if __name__ == "__main__":
    main()