import concurrent.futures
from collections import defaultdict
import math
import os

NUM_THREADS = os.cpu_count() or 4

def round_to_infinity(x, digits=1):
    factor = 10 ** digits
    return math.ceil(x * factor) / factor

def process_chunk(lines):
    city_scores = defaultdict(lambda: [float("inf"), 0, float("-inf")])  # [min, sum, max]
    city_count = defaultdict(int)  # Track counts for averages

    for line in lines:
        try:
            city, score = line.strip().split(";", 1)
            score = float(score.strip())
            city = city.strip()
            city_scores[city][0] = min(city_scores[city][0], score)  # Min score
            city_scores[city][1] += score  # Sum score
            city_scores[city][2] = max(city_scores[city][2], score)  # Max score
            city_count[city] += 1  # Count for averaging
        except (ValueError, IndexError):
            continue

    # Convert sums to means before returning
    for city in city_scores:
        city_scores[city][1] /= city_count[city]  # Mean = sum / count
    return city_scores

def read_in_chunks(file, chunk_size=100000):  # Larger chunks for fewer reads
    while True:
        lines = file.readlines(chunk_size)
        if not lines:
            break
        yield lines

def main(input_file_name="testcase.txt"):
    city_data = defaultdict(lambda: [float("inf"), 0, float("-inf")])  # [min, sum, max]
    city_count = defaultdict(int)

    with open(input_file_name, "r", buffering=2**20) as input_file:  # Large buffer for fewer I/O ops
        with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            futures = []
            for chunk in read_in_chunks(input_file):
                futures.append(executor.submit(process_chunk, chunk))
            
            for future in concurrent.futures.as_completed(futures):
                chunk_result = future.result()
                for city, stats in chunk_result.items():
                    city_data[city][0] = min(city_data[city][0], stats[0])  # Update global min
                    city_data[city][1] += stats[1] * city_count[city]  # Sum up weighted means
                    city_data[city][2] = max(city_data[city][2], stats[2])  # Update global max
                    city_count[city] += 1  # Track count for proper mean aggregation

    # Print output results
    for city in sorted(city_data.keys()):
        stats = city_data[city]
        print(f"{city}={round_to_infinity(stats[0], 1)}/"
              f"{round_to_infinity(stats[1] / city_count[city], 1)}/"  # Adjusted global mean
              f"{round_to_infinity(stats[2], 1)}")

if __name__ == "__main__":
    main()


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