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
import os
import math
from collections import defaultdict
import concurrent.futures

# Helper function to replace lambda for pickle compatibility
def default_stats():
    return [math.inf, -math.inf, 0, 0]

def process_chunk(args):
    filename, start, end = args
    city_stats = defaultdict(default_stats)
    with open(filename, "rb") as f:
        f.seek(start)
        buffer = b""
        while f.tell() < end:
            buffer += f.read(4096)
            while True:
                nl_pos = buffer.find(b'\n')
                if nl_pos < 0:
                    break
                line = buffer[:nl_pos]
                buffer = buffer[nl_pos+1:]
                if not line:
                    continue
                
                semicolon = line.find(b';')
                if semicolon < 0:
                    continue
                
                city = line[:semicolon].strip()
                score_str = line[semicolon+1:].strip()
                
                try:
                    score = float(score_str)
                except ValueError:
                    continue
                
                stats = city_stats[city]
                stats[0] = min(stats[0], score)
                stats[1] = max(stats[1], score)
                stats[2] += score
                stats[3] += 1
    return city_stats

def main(input_file="testcase.txt", output_file="output.txt"):
    # Determine file chunks
    with open(input_file, "rb") as f:
        file_size = os.fstat(f.fileno()).st_size
        num_workers = os.cpu_count() or 1
        chunk_size = file_size // num_workers
        chunks = []
        start = 0
        
        for _ in range(num_workers - 1):
            end_candidate = min(start + chunk_size, file_size)
            f.seek(end_candidate)
            while f.read(1) != b'\n' and f.tell() < file_size:
                pass
            end = f.tell()
            chunks.append((input_file, start, end))
            start = end
        
        chunks.append((input_file, start, file_size))

    # Process chunks in parallel
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
        
        merged = defaultdict(default_stats)
        for future in concurrent.futures.as_completed(futures):
            chunk_result = future.result()
            for city, stats in chunk_result.items():
                m = merged[city]
                m[0] = min(m[0], stats[0])
                m[1] = max(m[1], stats[1])
                m[2] += stats[2]
                m[3] += stats[3]

    # Write results
    round_up = lambda x: math.ceil(x * 10) / 10
    with open(output_file, "w") as f:
        for city in sorted(merged.keys()):
            cmin, cmax, total, count = merged[city]
            avg = total / count
            line = f"{city.decode()}={round_up(cmin)}/{round_up(avg)}/{round_up(cmax)}\n"
            f.write(line)

if __name__ == "__main__":
    main()