import math
import mmap
import multiprocessing

def round_up(x):
    return math.ceil(x * 10) / 10  

def process_chunk(filename, start_offset, end_offset):
    """Process a chunk of the file to extract city weather data."""
    data = {}
    
    with open(filename, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        size = len(mm)

        # Adjust start_offset to next newline if not at file start
        if start_offset != 0:
            while start_offset < size and mm[start_offset] != ord('\n'):
                start_offset += 1
            start_offset += 1

        # Adjust end_offset to include the full last line
        while end_offset < size and mm[end_offset] != ord('\n'):
            end_offset += 1
        if end_offset < size:
            end_offset += 1

        chunk = mm[start_offset:end_offset]
        mm.close()

    # Process lines quickly using splitlines()
    for line in chunk.splitlines():
        if not line:
            continue

        # Use partition for fast splitting
        city, sep, score_str = line.partition(b';')
        if sep != b';':
            continue

        try:
            score = float(score_str)
        except ValueError:
            continue

        # Update city statistics efficiently
        if city in data:
            stats = data[city]
            stats[0] = min(stats[0], score)  # Min
            stats[1] = max(stats[1], score)  # Max
            stats[2] += score  # Sum
            stats[3] += 1  # Count
        else:
            data[city] = [score, score, score, 1]

    return data

def merge_data(results):
    """Merge city weather data from multiple processes."""
    final_data = {}
    
    for data in results:
        for city, stats in data.items():
            if city in final_data:
                final_stats = final_data[city]
                final_stats[0] = min(final_stats[0], stats[0])  # Min
                final_stats[1] = max(final_stats[1], stats[1])  # Max
                final_stats[2] += stats[2]  # Sum
                final_stats[3] += stats[3]  # Count
            else:
                final_data[city] = stats.copy()
    
    return final_data

def main(input_file="testcase.txt", output_file="output.txt"):
    """Main function that manages multiprocessing and writes output."""
    with open(input_file, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        file_size = len(mm)
        mm.close()

    num_procs = min(multiprocessing.cpu_count() * 2, 16)  # Limit to avoid excessive overhead
    chunk_size = file_size // num_procs
    chunks = [(i * chunk_size, (i + 1) * chunk_size if i < num_procs - 1 else file_size)
              for i in range(num_procs)]

    with multiprocessing.Pool(num_procs) as pool:
        results = pool.starmap(process_chunk, [(input_file, start, end) for start, end in chunks])

    final_data = merge_data(results)

    # Generate output in sorted order
    output_lines = [
        f"{city.decode()}={round_up(min_val):.1f}/{round_up(sum_val / count):.1f}/{round_up(max_val):.1f}\n"
        for city, (min_val, max_val, sum_val, count) in sorted(final_data.items(), key=lambda c: c[0].decode())
    ]

    with open(output_file, "w") as f:
        f.writelines(output_lines)

if __name__ == "__main__":
    main()

