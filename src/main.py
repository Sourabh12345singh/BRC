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
import subprocess

# Optimized Bash script for handling large datasets
bash_script_content = r"""#!/bin/bash

input_file="${1:-testcase.txt}"
output_file="${2:-output.txt}"

# Ensure input file exists
if [ ! -f "$input_file" ]; then
    echo "Error: Input file '$input_file' not found!"
    exit 1
fi

LC_NUMERIC=C gawk -F ';' '
function ceil(x) { return (x == int(x)) ? x : int(x) + (x > 0) }
function round_up(val) { return ceil(val * 10) / 10 }

{
    # Debugging: Print skipped lines
    if (NF != 2) { print "Skipping (wrong format): " $0 > "/dev/stderr"; next }
    if ($2 !~ /^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$/) { print "Skipping (invalid number): " $0 > "/dev/stderr"; next }

    city = $1
    value = $2 + 0

    # Update statistics
    min[city] = (city in min) ? (value < min[city] ? value : min[city]) : value
    max[city] = (city in max) ? (value > max[city] ? value : max[city]) : value
    sum[city] += value
    count[city]++
}
END {
    if (length(sum) == 0) {
        print "Error: No valid data found!" > "/dev/stderr"
        exit 1
    }
    
    for (city in sum) {
        avg = sum[city] / count[city]
        printf "%s=%.1f/%.1f/%.1f\n", 
            city, 
            round_up(min[city]), 
            round_up(avg), 
            round_up(max[city])
    }
}' "$input_file" | sort -T /tmp --parallel=$(nproc) > "$output_file"

if [ ! -s "$output_file" ]; then
    echo "Error: Output file is empty!"
    exit 1
fi
"""

script_name = "script.sh"

try:
    # Write the Bash script to a file with Unix-style line endings
    with open(script_name, "w", newline="\n") as f:
        f.write(bash_script_content)

    # Make the script executable
    os.chmod(script_name, 0o755)

    # Execute the script and capture errors
    subprocess.run(["bash", script_name], check=True)

except subprocess.CalledProcessError as e:
    print(f"Error executing the script: {e}")

finally:
    # Cleanup: Delete the script if needed
    if os.path.exists(script_name):
        os.remove(script_name)
