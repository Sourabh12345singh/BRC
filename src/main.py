import math
import mmap
import multiprocessing
import threading

def round_up(x):
    return math.ceil(x * 10) / 10  

def process_chunk(filename, start_offset, end_offset, result_dict):
    """Processes a file chunk and updates the shared dictionary."""
    local_data = {}

    with open(filename, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        size = len(mm)

        # Move start_offset to next newline if not at file start
        if start_offset != 0:
            while start_offset < size and mm[start_offset] != ord('\n'):
                start_offset += 1
            start_offset += 1

        # Extend end_offset to complete last line
        while end_offset < size and mm[end_offset] != ord('\n'):
            end_offset += 1
        if end_offset < size:
            end_offset += 1

        chunk = mm[start_offset:end_offset]
        mm.close()

    # Process lines using splitlines() for speed
    for line in chunk.splitlines():
        if not line:
            continue

        city, sep, score_str = line.partition(b';')
        if sep != b';':
            continue
        
        try:
            score = float(score_str)
        except ValueError:
            continue

        # Update dictionary (local cache to minimize locks)
        if city in local_data:
            stats = local_data[city]
            stats[0] = min(stats[0], score)
            stats[1] = max(stats[1], score)
            stats[2] += score
            stats[3] += 1
        else:
            local_data[city] = [score, score, score, 1]

    # Merge into shared dictionary
    with result_dict_lock:
        for city, stats in local_data.items():
            if city in result_dict:
                shared_stats = result_dict[city]
                shared_stats[0] = min(shared_stats[0], stats[0])
                shared_stats[1] = max(shared_stats[1], stats[1])
                shared_stats[2] += stats[2]
                shared_stats[3] += stats[3]
            else:
                result_dict[city] = stats.copy()

def main(input_file="testcase.txt", output_file="output.txt"):
    """Main function using multiprocessing with threading per process."""
    with open(input_file, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        file_size = len(mm)
        mm.close()

    num_procs = min(multiprocessing.cpu_count() * 2, 16)
    chunk_size = file_size // num_procs
    chunks = [(i * chunk_size, (i + 1) * chunk_size if i < num_procs - 1 else file_size)
              for i in range(num_procs)]

    # Shared dictionary with a lock for thread safety
    manager = multiprocessing.Manager()
    result_dict = manager.dict()
    global result_dict_lock
    result_dict_lock = manager.Lock()

    # Spawn processes with threading inside
    processes = []
    for start, end in chunks:
        p = multiprocessing.Process(target=process_chunk, args=(input_file, start, end, result_dict))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    # Generate sorted output
    output_lines = [
        f"{city.decode()}={round_up(min_val):.1f}/{round_up(sum_val / count):.1f}/{round_up(max_val):.1f}\n"
        for city, (min_val, max_val, sum_val, count) in sorted(result_dict.items(), key=lambda c: c[0].decode())
    ]

    with open(output_file, "w") as f:
        f.writelines(output_lines)

if __name__ == "__main__":
    main()
