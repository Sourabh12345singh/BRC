

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

import concurrent.futures
from collections import defaultdict
import math
import os

NUM_THREADS = os.cpu_count() or 4

def round_to_infinity(x, digits=1):
    factor = 10 ** digits
    return math.ceil(x * factor) / factor

def process_chunk(lines):
    """Process a chunk of lines to compute stats."""
    city_stats = defaultdict(lambda: [float('inf'), 0, float('-inf'), 0])  # [min, sum, max, count]

    for line in lines:
        try:
            city, score = line.strip().split(";", 1)
            score = float(score.strip())
            stats = city_stats[city]
            stats[0] = min(stats[0], score)  # Update min
            stats[1] += score               # Update sum
            stats[2] = max(stats[2], score)  # Update max
            stats[3] += 1                   # Update count
        except (ValueError, IndexError):
            continue

    return city_stats

def merge_results(global_stats, local_stats):
    """Merge stats from a chunk into the global stats."""
    for city, local in local_stats.items():
        global_city = global_stats[city]
        global_city[0] = min(global_city[0], local[0])  # Update min
        global_city[1] += local[1]                      # Update sum
        global_city[2] = max(global_city[2], local[2])  # Update max
        global_city[3] += local[3]                      # Update count

def read_in_chunks(file, chunk_size=100000):  # Larger chunk size for better performance
    while True:
        lines = file.readlines(chunk_size)
        if not lines:
            break
        yield lines

def main(input_file_name="testcase.txt"):
    global_stats = defaultdict(lambda: [float('inf'), 0, float('-inf'), 0])  # [min, sum, max, count]

    with open(input_file_name, "r", buffering=2**20) as input_file:  # Large buffer for fast I/O
        with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            futures = []
            for chunk in read_in_chunks(input_file):
                futures.append(executor.submit(process_chunk, chunk))

            for future in concurrent.futures.as_completed(futures):
                local_stats = future.result()
                merge_results(global_stats, local_stats)

    # Print sorted results
    for city in sorted(global_stats.keys()):
        stats = global_stats[city]
        mean = stats[1] / stats[3] if stats[3] > 0 else 0  # Calculate mean
        print(f"{city}={round_to_infinity(stats[0], 1)}/"
              f"{round_to_infinity(mean, 1)}/"
              f"{round_to_infinity(stats[2], 1)}")

if __name__ == "__main__":
    main()
