import math
import mmap
import multiprocessing
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

def round_up(x):
    """Rounds up to the nearest tenth."""
    return math.ceil(x * 10) / 10  

def process_sub_chunk(chunk):
    """Process a sub-chunk in a separate thread."""
    local_data = defaultdict(lambda: [float('inf'), float('-inf'), 0.0, 0])

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

        stats = local_data[city]
        stats[0] = min(stats[0], score)  # Min
        stats[1] = max(stats[1], score)  # Max
        stats[2] += score  # Sum
        stats[3] += 1  # Count
    
    return local_data

def process_chunk(start, end, filename):
    """Processes a chunk of the file using multiple threads."""
    chunk_data = defaultdict(lambda: [float('inf'), float('-inf'), 0.0, 0])

    with open(filename, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        size = len(mm)

        # Adjust start and end to full lines
        if start != 0:
            while start < size and mm[start] != ord('\n'):
                start += 1
            start += 1
        
        while end < size and mm[end] != ord('\n'):
            end += 1
        if end < size:
            end += 1

        chunk = mm[start:end]
        mm.close()

    # Split chunk into 2 sub-chunks for threading
    mid = len(chunk) // 2
    while mid < len(chunk) and chunk[mid] != ord('\n'):
        mid += 1
    mid += 1

    sub_chunks = [chunk[:mid], chunk[mid:]]
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = executor.map(process_sub_chunk, sub_chunks)

    # Merge results from threads
    for data in results:
        for city, stats in data.items():
            entry = chunk_data[city]
            entry[0] = min(entry[0], stats[0])
            entry[1] = max(entry[1], stats[1])
            entry[2] += stats[2]
            entry[3] += stats[3]

    return chunk_data

def merge_data(results):
    """Merge results from multiple processes."""
    final_data = defaultdict(lambda: [float('inf'), float('-inf'), 0.0, 0])

    for data in results:
        for city, stats in data.items():
            entry = final_data[city]
            entry[0] = min(entry[0], stats[0])
            entry[1] = max(entry[1], stats[1])
            entry[2] += stats[2]
            entry[3] += stats[3]

    return final_data

def main(input_file="testcase.txt", output_file="output.txt"):
    """Main function with multiprocessing and threading."""
    with open(input_file, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        file_size = len(mm)
        mm.close()

    num_procs = min(multiprocessing.cpu_count(), 16)
    chunk_size = file_size // num_procs
    chunks = [(i * chunk_size, (i + 1) * chunk_size if i < num_procs - 1 else file_size)
              for i in range(num_procs)]

    with multiprocessing.Pool(num_procs) as pool:
        results = pool.starmap(process_chunk, [(start, end, input_file) for start, end in chunks])

    final_data = merge_data(results)

    output_lines = [
        f"{city.decode()}={round_up(min_val):.1f}/{round_up(sum_val / count):.1f}/{round_up(max_val):.1f}\n"
        for city, (min_val, max_val, sum_val, count) in sorted(final_data.items())
    ]

    with open(output_file, "w") as f:
        f.writelines(output_lines)

if __name__ == "__main__":
    main()

