import math
import mmap
import multiprocessing
from concurrent.futures import ThreadPoolExecutor

def round_inf(x):
    """Round up to 1 decimal place."""
    return math.ceil(x * 10) / 10  

def process_chunk(filename, start, end):
    """Processes a file chunk efficiently using threads."""
    city_data = {}

    with open(filename, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        size = len(mm)

        # Adjust start offset to next line
        if start != 0:
            while start < size and mm[start] != ord('\n'):
                start += 1
            start += 1
        
        # Adjust end offset to end of line
        while end < size and mm[end] != ord('\n'):
            end += 1
        if end < size:
            end += 1

        chunk = mm[start:end]
        mm.close()

    lines = chunk.split(b'\n')
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = executor.map(process_line, lines)

    for city, stats in filter(None, results):
        if city in city_data:
            entry = city_data[city]
            entry[0] = min(entry[0], stats[0])  # Min
            entry[1] = max(entry[1], stats[1])  # Max
            entry[2] += stats[2]  # Sum
            entry[3] += stats[3]  # Count
        else:
            city_data[city] = stats

    return city_data

def process_line(line):
    """Processes a single line and extracts statistics."""
    if not line:
        return None
    semicolon = line.find(b';')
    if semicolon == -1:
        return None
    
    city = line[:semicolon]
    try:
        score = float(line[semicolon + 1:])
    except ValueError:
        return None

    return city, [score, score, score, 1]

def merge_data(results):
    """Merge results from multiple processes."""
    final_data = {}
    for data in results:
        for city, stats in data.items():
            if city in final_data:
                entry = final_data[city]
                entry[0] = min(entry[0], stats[0])  # Min
                entry[1] = max(entry[1], stats[1])  # Max
                entry[2] += stats[2]  # Sum
                entry[3] += stats[3]  # Count
            else:
                final_data[city] = stats
    return final_data

def main(input_file="testcase.txt", output_file="output.txt"):
    """Main function for efficient file processing."""
    with open(input_file, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        file_size = len(mm)
        mm.close()

    num_procs = min(multiprocessing.cpu_count(), 16)  # Optimized CPU utilization
    chunk_size = file_size // num_procs
    chunks = [(i * chunk_size, (i + 1) * chunk_size if i < num_procs - 1 else file_size)
              for i in range(num_procs)]

    with multiprocessing.Pool(num_procs) as pool:
        results = pool.starmap(process_chunk, [(input_file, start, end) for start, end in chunks])

    final_data = merge_data(results)

    with open(output_file, "w") as f:
        f.writelines(
            f"{city.decode()}={round_inf(stats[0]):.1f}/{round_inf(stats[2] / stats[3]):.1f}/{round_inf(stats[1]):.1f}\n"
            for city, stats in sorted(final_data.items())
        )

if __name__ == "__main__":
    main()
