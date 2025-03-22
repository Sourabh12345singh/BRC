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
# # raat ka code 
import concurrent.futures
import os
from collections import defaultdict
import math
import mmap
import io

# Optimal thread count for I/O bound operations
NUM_THREADS = 2  # Fixed to 2 as this is I/O bound task

def round_to_infinity(x, digits=1):
    factor = 10 ** digits
    return math.ceil(x * factor) / factor

def process_chunk(chunk_data):
    city_scores = defaultdict(list)
    start, size, mm = chunk_data
    
    # Process the chunk using memoryview for faster access
    view = memoryview(mm[start:start + size]).tobytes()
    buffer = io.StringIO(view.decode('utf-8'))
    
    # Fast line processing
    for line in buffer:
        if ';' not in line:
            continue
        try:
            city, score = line.rstrip('\n').split(';', 1)
            city_scores[city].append(float(score))
        except (ValueError, IndexError):
            continue
    return city_scores

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    # Use memory mapping for fastest possible file reading
    with open(input_file_name, 'rb') as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        file_size = mm.size()
        
        # Calculate optimal chunk sizes
        chunk_size = file_size // NUM_THREADS
        chunks = []
        
        # Create chunk boundaries at newline characters
        start = 0
        for i in range(NUM_THREADS - 1):
            end = start + chunk_size
            while end < file_size and mm[end] != ord('\n'):
                end += 1
            chunks.append((start, end - start, mm))
            start = end + 1
        chunks.append((start, file_size - start, mm))

        # Process chunks concurrently
        results = defaultdict(list)
        with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
            
            # Aggregate results efficiently
            for future in concurrent.futures.as_completed(futures):
                for city, scores in future.result().items():
                    results[city].extend(scores)

        # Write results efficiently
        with open(output_file_name, 'w', buffering=io.DEFAULT_BUFFER_SIZE) as out:
            for city in sorted(results):
                scores = results[city]
                if not scores:
                    continue
                
                # Calculate statistics in one pass
                min_val = float('inf')
                max_val = float('-inf')
                total = 0
                count = len(scores)
                
                for score in scores:
                    min_val = min(min_val, score)
                    max_val = max(max_val, score)
                    total += score
                
                out.write(f"{city}={round_to_infinity(min_val, 1)}/"
                         f"{round_to_infinity(total/count, 1)}/"
                         f"{round_to_infinity(max_val, 1)}\n")
        
        mm.close()

if __name__ == "__main__":
    main()