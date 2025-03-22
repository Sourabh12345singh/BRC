import concurrent.futures
import os
from collections import defaultdict
import math
from itertools import islice

def round_to_infinity(x, digits=1):
    """Round up to specified decimal places"""
    factor = 10 ** digits
    return math.ceil(x * factor) / factor

# Determine optimal thread count - balance between performance and resource usage
# Limit to between 2 and min(4, cpu_count) to avoid excessive threading
NUM_THREADS = max(2, min(4, os.cpu_count() or 1))

def process_chunk(lines):
    """Process a chunk of lines to extract city statistics"""
    # Use direct stats tracking: [min, sum, max, count]
    city_stats = defaultdict(lambda: [float('inf'), 0, float('-inf'), 0])
    
    for line in lines:
        try:
            # Split only once for efficiency
            parts = line.strip().split(";", 1)
            if len(parts) != 2:
                continue
                
            city, score_str = parts
            city = city.strip()
            score = float(score_str.strip())
            
            # Update stats in a single pass
            stats = city_stats[city]
            stats[0] = min(stats[0], score)  # min
            stats[1] += score                # sum
            stats[2] = max(stats[2], score)  # max
            stats[3] += 1                    # count
        except (ValueError, IndexError):
            # Skip invalid lines silently
            continue
            
    return city_stats

def read_in_chunks(file_path, chunk_size=50000):
    """Memory-efficient file reading with configurable chunk size"""
    with open(file_path, "r") as file:
        while True:
            lines = list(islice(file, chunk_size))
            if not lines:
                break
            yield lines

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    """Main function to process city statistics from input file"""
    # Initialize global stats with same structure as chunk stats
    global_stats = defaultdict(lambda: [float('inf'), 0, float('-inf'), 0])
    
    try:
        # Process file in chunks using thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            futures = []
            
            # Submit chunks for processing
            for chunk in read_in_chunks(input_file_name):
                futures.append(executor.submit(process_chunk, chunk))
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    # Merge chunk results into global stats
                    local_stats = future.result()
                    for city, stats in local_stats.items():
                        global_city = global_stats[city]
                        global_city[0] = min(global_city[0], stats[0])  # min
                        global_city[1] += stats[1]                      # sum
                        global_city[2] = max(global_city[2], stats[2])  # max
                        global_city[3] += stats[3]                      # count
                except Exception as e:
                    print(f"Error processing chunk: {e}")
        
        # Write results to output file
        with open(output_file_name, "w") as output_file:
            for city in sorted(global_stats.keys()):
                stats = global_stats[city]
                if stats[3] == 0:  # Skip if no valid scores
                    continue
                    
                min_score = stats[0]
                mean_score = stats[1] / stats[3]  # sum / count
                max_score = stats[2]
                
                output_file.write(f"{city}={round_to_infinity(min_score, 1)}/"
                               f"{round_to_infinity(mean_score, 1)}/"
                               f"{round_to_infinity(max_score, 1)}\n")
                
        return True
    except Exception as e:
        print(f"Error in processing: {e}")
        return False

if __name__ == "__main__":
    # Allow for command-line arguments if needed
    import sys
    input_file = "testcase.txt"
    output_file = "output.txt"
    
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
        
    success = main(input_file, output_file)
    if not success:
        print("Processing failed")


        
# import concurrent.futures
# import os 
# from collections import defaultdict
# import math
# from itertools import islice

# # Adjust thread count with a reasonable minimum and maximum
# NUM_THREADS = max(2, min(4, os.cpu_count() or 1))  # Limit to 2-4 threads

# def round_to_infinity(x, digits=1):
#     factor = 10 ** digits
#     return math.ceil(x * factor) / factor

# def process_chunk(lines):
#     city_scores = defaultdict(list)
#     for line in lines:
#         try:
#             city, score = line.strip().split(";", 1)  # More efficient split
#             score = float(score.strip())
#             city_scores[city.strip()].append(score)
#         except (ValueError, IndexError):
#             continue
#     return city_scores

# def read_in_chunks(file, chunk_size=8192):
#     """Memory-efficient file reading"""
#     while True:
#         lines = list(islice(file, chunk_size))
#         if not lines:
#             break
#         yield lines

# def main(input_file_name="testcase.txt", output_file_name="output.txt"):
#     city_data = defaultdict(list)
#     chunk_results = defaultdict(list)

#     # Process file in chunks to reduce memory usage
#     with open(input_file_name, "r", buffering=2**16) as input_file:
#         with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
#             futures = []
#             for chunk in read_in_chunks(input_file, chunk_size=10000):
#                 futures.append(executor.submit(process_chunk, chunk))
            
#             # Process results as they complete
#             for future in concurrent.futures.as_completed(futures):
#                 chunk_result = future.result()
#                 for city, scores in chunk_result.items():
#                     chunk_results[city].extend(scores)

#     # Calculate statistics in a single pass
#     with open(output_file_name, "w", buffering=2**16) as output_file:
#         for city in sorted(chunk_results.keys()):
#             scores = chunk_results[city]
#             if not scores:
#                 continue
            
#             min_score = min(scores)
#             max_score = max(scores)
#             mean_score = sum(scores) / len(scores)
            
#             output_file.write(f"{city}={round_to_infinity(min_score, 1)}/"
#                             f"{round_to_infinity(mean_score, 1)}/"
#                             f"{round_to_infinity(max_score, 1)}\n")

# if __name__ == "__main__":
#     main()
# raat ka code 
