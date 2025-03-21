import concurrent.futures
import os
import math
from collections import defaultdict

# Hard-code 2 cores since that's what the execution environment has
NUM_THREADS = 2

def round_to_infinity(x, digits=1):
    """Rounds x upward (toward +∞) to the specified number of decimal places."""
    factor = 10 ** digits
    return math.ceil(x * factor) / factor

def process_file_chunk(file_path, start_byte, end_byte):
    """Process a chunk of the file with minimal operations."""
    # Use running statistics instead of storing all scores
    city_stats = {}  # {city: (count, sum, min, max)}
    
    with open(file_path, 'rb') as f:
        # Seek to start position
        f.seek(start_byte)
        
        # Skip partial line if not at file start
        if start_byte > 0:
            f.readline()
        
        current_pos = f.tell()
        
        # Process until end byte or EOF
        while current_pos < end_byte:
            line = f.readline()
            if not line:  # EOF
                break
                
            current_pos = f.tell()
            
            # Fast split and processing
            try:
                semicolon_idx = line.find(b';')
                if semicolon_idx == -1:
                    continue
                    
                city = line[:semicolon_idx].decode('ascii', errors='ignore').strip()
                if not city:
                    continue
                    
                score_str = line[semicolon_idx+1:].strip()
                score = float(score_str)
                
                # Update running statistics
                if city not in city_stats:
                    # (count, sum, min, max)
                    city_stats[city] = [1, score, score, score]
                else:
                    stats = city_stats[city]
                    stats[0] += 1  # count
                    stats[1] += score  # sum
                    if score < stats[2]:
                        stats[2] = score  # min
                    if score > stats[3]:
                        stats[3] = score  # max
                        
            except (ValueError, UnicodeDecodeError):
                continue
    
    return city_stats

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    """Main function optimized for a dual-core system with nogil Python."""
    file_size = os.path.getsize(input_file_name)
    
    # For very small files, don't bother with multi-threading
    if file_size < 10 * 1024 * 1024:  # 10MB
        city_stats = process_file_chunk(input_file_name, 0, file_size)
        
    else:
        # Divide file into two chunks for the two cores
        mid_point = file_size // 2
        
        # Use ProcessPoolExecutor for true parallelism with nogil Python
        with concurrent.futures.ProcessPoolExecutor(max_workers=NUM_THREADS) as executor:
            future1 = executor.submit(process_file_chunk, input_file_name, 0, mid_point)
            future2 = executor.submit(process_file_chunk, input_file_name, mid_point, file_size)
            
            # Get results from both processes
            chunk1_stats = future1.result()
            chunk2_stats = future2.result()
            
            # Merge results from both chunks
            city_stats = chunk1_stats.copy()
            for city, stats2 in chunk2_stats.items():
                if city in city_stats:
                    stats1 = city_stats[city]
                    # Merge count, sum, min, max
                    stats1[0] += stats2[0]
                    stats1[1] += stats2[1]
                    stats1[2] = min(stats1[2], stats2[2])
                    stats1[3] = max(stats1[3], stats2[3])
                else:
                    city_stats[city] = stats2
    
    # Write results to file
    with open(output_file_name, 'w') as f:
        for city in sorted(city_stats.keys()):
            count, total, min_val, max_val = city_stats[city]
            if count == 0:
                continue
                
            mean_val = total / count
            f.write(f"{city}={round_to_infinity(min_val, 1)}/"
                   f"{round_to_infinity(mean_val, 1)}/"
                   f"{round_to_infinity(max_val, 1)}\n")

if __name__ == "__main__":
    main()