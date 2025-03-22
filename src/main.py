import math
import sys
import threading
from collections import defaultdict

def round_to_inf(x, d=1):
    """Rounds up to the nearest decimal place."""
    factor = 10 ** d
    return math.ceil(x * factor) / factor

def process_chunk(lines, cs_lock, cs):
    """Processes a chunk of lines and updates global statistics."""
    local_cs = defaultdict(lambda: [float('inf'), float('-inf'), 0.0, 0])
    
    for line in lines:
        c, s = line.strip().split(";")
        s = float(s)
        stats = local_cs[c]
        stats[0] = min(stats[0], s)  # Min
        stats[1] = max(stats[1], s)  # Max
        stats[2] += s  # Sum
        stats[3] += 1  # Count
    
    with cs_lock:
        for c, stats in local_cs.items():
            merged = cs[c]
            merged[0] = min(merged[0], stats[0])
            merged[1] = max(merged[1], stats[1])
            merged[2] += stats[2]
            merged[3] += stats[3]

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    """Main function to read input, process data, and write output."""
    with open(input_file_name, "r") as input_file:
        lines = input_file.readlines()
    
    num_threads = min(8, len(lines) // 1000 or 1)  # Adjust thread count dynamically
    chunk_size = max(1, len(lines) // num_threads)
    chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]
    
    cs = defaultdict(lambda: [float('inf'), float('-inf'), 0.0, 0])
    cs_lock = threading.Lock()
    threads = []
    
    for chunk in chunks:
        thread = threading.Thread(target=process_chunk, args=(chunk, cs_lock, cs))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    with open(output_file_name, "w") as output_file:
        for c in sorted(cs.keys()):
            mini = round_to_inf(cs[c][0])
            mean = round_to_inf(cs[c][2] / cs[c][3])
            maxi = round_to_inf(cs[c][1])
            output_file.write(f"{c}={mini:.1f}/{mean:.1f}/{maxi:.1f}\n")

if __name__ == "__main__":
    main()
