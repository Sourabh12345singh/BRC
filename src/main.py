import math
import mmap
import multiprocessing
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

def round_inf(x):
    return math.ceil(x * 10) / 10  

def default_city_data():
    return [float('inf'), float('-inf'), 0.0, 0]

def process_sub_chunk(chunk):
    """Process a sub-chunk of the file in a thread."""
    data = defaultdict(default_city_data)
    
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
        
        entry = data[city]
        entry[0] = min(entry[0], score)
        entry[1] = max(entry[1], score)
        entry[2] += score
        entry[3] += 1
    
    return data

def process_chunk(filename, start_offset, end_offset):
    """Processes a chunk using two threads."""
    data = defaultdict(default_city_data)
    
    with open(filename, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        
        size = len(mm)
        if start_offset != 0:
            while start_offset < size and mm[start_offset] != ord('\n'):
                start_offset += 1
            start_offset += 1
        
        end = min(end_offset, size)
        while end < size and mm[end] != ord('\n'):
            end += 1
        if end < size:
            end += 1
        
        chunk = mm[start_offset:end]
        mm.close()
    
    # Split the chunk into two sub-chunks for multithreading
    mid = len(chunk) // 2
    while mid < len(chunk) and chunk[mid] != ord('\n'):
        mid += 1
    mid += 1
    
    sub_chunks = [chunk[:mid], chunk[mid:]]
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = executor.map(process_sub_chunk, sub_chunks)
    
    for result in results:
        for city, stats in result.items():
            entry = data[city]
            entry[0] = min(entry[0], stats[0])
            entry[1] = max(entry[1], stats[1])
            entry[2] += stats[2]
            entry[3] += stats[3]
    
    return data

def merge_data(data_list):
    final = defaultdict(default_city_data)  
    for data in data_list:
        for city, stats in data.items():
            final_entry = final[city]
            final_entry[0] = min(final_entry[0], stats[0])
            final_entry[1] = max(final_entry[1], stats[1])
            final_entry[2] += stats[2]
            final_entry[3] += stats[3]
    return final

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    with open(input_file_name, "rb") as f:
        file_size = f.seek(0, 2)
    
    num_procs = multiprocessing.cpu_count()
    chunk_size = file_size // num_procs
    chunks = [(i * chunk_size, (i + 1) * chunk_size if i < num_procs - 1 else file_size)
              for i in range(num_procs)]
    
    with multiprocessing.Pool(num_procs) as pool:
        tasks = [(input_file_name, start, end) for start, end in chunks]
        results = pool.starmap(process_chunk, tasks)
    
    final_data = merge_data(results)
    
    out = []
    for city in sorted(final_data.keys()):
        mn, mx, total, count = final_data[city]
        avg = round_inf(total / count)
        out.append(f"{city.decode()}={round_inf(mn):.1f}/{avg:.1f}/{round_inf(mx):.1f}\n")
    
    with open(output_file_name, "w") as f:
        f.writelines(out)

if __name__ == "__main__":
    main()
