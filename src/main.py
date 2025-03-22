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

# import os
# import subprocess

# # Optimized Bash script with performance improvements
# bash_script_content = r"""#!/bin/bash

# input_file="${1:-testcase.txt}"
# output_file="${2:-output.txt}"

# LC_NUMERIC=C awk -F ';' '
# function ceil(x) { return (x == int(x)) ? x : int(x) + (x > 0) }
# function round_up(val) { return ceil(val * 10) / 10 }

# {
#     if (NF == 2) {
#         city = $1
#         value = $2 + 0

#         # Update min, max, sum, and count
#         if (city in min) {
#             if (value < min[city]) min[city] = value
#             if (value > max[city]) max[city] = value
#             sum[city] += value
#             count[city]++
#         } else {
#             min[city] = max[city] = sum[city] = value
#             count[city] = 1
#         }
#     }
# }
# END {
#     for (city in sum) {
#         avg = sum[city] / count[city]
#         printf "%s=%.1f/%.1f/%.1f\n", city, round_up(min[city]), round_up(avg), round_up(max[city])
#     }
# }' "$input_file" | sort -T /tmp --parallel=$(nproc) > "$output_file"
# """

# script_name = "script.sh"

# try:
#     # Write the Bash script to a file with Unix-style line endings
#     with open(script_name, "w", newline="\n") as f:
#         f.write(bash_script_content)

#     # Make the script executable
#     os.chmod(script_name, 0o755)

#     # Execute the script
#     subprocess.run(["bash", script_name], check=True)

# except subprocess.CalledProcessError as e:
#     print(f"Error executing the script: {e}")

# finally:
#     # Cleanup: Delete the script if needed
#     if os.path.exists(script_name):
#         os.remove(script_name)

#1.96 sec wala for 5 million 
import math
import mmap
import multiprocessing
import heapq

def round_inf(x):
    return math.ceil(x * 10) / 10

def default_city_data():
    return [float('inf'), float('-inf'), 0.0, 0]

def find_chunk_offsets(filename, num_chunks):
    """Find optimal chunk offsets without scanning full file."""
    with open(filename, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        file_size = len(mm)
        chunk_size = file_size // num_chunks
        offsets = [0]
        
        for i in range(1, num_chunks):
            est_offset = min(i * chunk_size, file_size - 1)
            while est_offset < file_size and mm[est_offset] != ord('\n'):
                est_offset += 1
            offsets.append(est_offset + 1)
        
        offsets.append(file_size)
        mm.close()
    return offsets

def process_chunk(filename, start_offset, end_offset):
    data = {}
    with open(filename, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        chunk = mm[start_offset:end_offset]
        mm.close()
    
    for line in chunk.split(b'\n'):
        if not line:
            continue
        semicolon_pos = line.find(b';')
        if semicolon_pos == -1:
            continue
        
        city = line[:semicolon_pos]
        score_str = line[semicolon_pos+1:]
        
        try:
            score = float(score_str)
        except ValueError:
            continue
        
        if city not in data:
            data[city] = [float('inf'), float('-inf'), 0.0, 0]
        
        entry = data[city]
        entry[0] = min(entry[0], score)
        entry[1] = max(entry[1], score)
        entry[2] += score
        entry[3] += 1
    
    return data

def merge_data(data_list):
    final = {}
    for data in data_list:
        for city, stats in data.items():
            if city not in final:
                final[city] = [float('inf'), float('-inf'), 0.0, 0]
            
            final_entry = final[city]
            final_entry[0] = min(final_entry[0], stats[0])
            final_entry[1] = max(final_entry[1], stats[1])
            final_entry[2] += stats[2]
            final_entry[3] += stats[3]
    return final

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    num_procs = multiprocessing.cpu_count() * 2
    chunk_offsets = find_chunk_offsets(input_file_name, num_procs)
    
    tasks = [(input_file_name, chunk_offsets[i], chunk_offsets[i + 1]) for i in range(num_procs)]
    
    with multiprocessing.Pool(num_procs) as pool:
        results = pool.starmap(process_chunk, tasks)
    
    final_data = merge_data(results)
    
    out = []
    for city, (mn, mx, total, count) in sorted(final_data.items()):
        avg = round_inf(total / count)
        out.append(f"{city.decode()}={round_inf(mn):.1f}/{avg:.1f}/{round_inf(mx):.1f}\n")
    
    with open(output_file_name, "w") as f:
        f.writelines(out)

if __name__ == "__main__":
    main()