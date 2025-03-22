import math
import mmap
import multiprocessing
from array import array
from typing import Dict, List, Tuple

class CityStats:
    __slots__ = ('min_val', 'max_val', 'sum_val', 'count')
    
    def __init__(self, value: float):
        self.min_val = value
        self.max_val = value
        self.sum_val = value
        self.count = 1

def round_inf(x: float) -> float:
    return math.ceil(x * 10) / 10

def process_chunk(filename: str, start_offset: int, end_offset: int) -> Dict[bytes, List[float]]:
    city_stats: Dict[bytes, List[float]] = {}
    
    with open(filename, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            # Optimize chunk boundaries
            if start_offset > 0:
                while start_offset < end_offset and mm[start_offset-1] != ord('\n'):
                    start_offset += 1
            
            while end_offset < len(mm) and mm[end_offset-1] != ord('\n'):
                end_offset += 1
            
            # Process chunk using memoryview for better performance
            view = memoryview(mm[start_offset:end_offset])
            chunk = view.tobytes()
            
            # Process lines efficiently
            for line in chunk.split(b'\n'):
                if b';' not in line:
                    continue
                
                try:
                    city, score_str = line.split(b';', 1)
                    score = float(score_str)
                    
                    if city in city_stats:
                        stats = city_stats[city]
                        stats[0] = min(stats[0], score)
                        stats[1] = max(stats[1], score)
                        stats[2] += score
                        stats[3] += 1
                    else:
                        city_stats[city] = [score, score, score, 1]
                except (ValueError, IndexError):
                    continue
                
    return city_stats

def merge_results(results: List[Dict[bytes, List[float]]]) -> Dict[bytes, List[float]]:
    final_stats: Dict[bytes, List[float]] = {}
    
    for chunk_stats in results:
        for city, stats in chunk_stats.items():
            if city in final_stats:
                final = final_stats[city]
                final[0] = min(final[0], stats[0])
                final[1] = max(final[1], stats[1])
                final[2] += stats[2]
                final[3] += stats[3]
            else:
                final_stats[city] = stats.copy()
    
    return final_stats

def main(input_file_name: str = "testcase.txt", output_file_name: str = "output.txt"):
    # Get optimal number of processes
    num_procs = min(32, multiprocessing.cpu_count() * 2)
    
    # Calculate file size and chunk boundaries
    with open(input_file_name, "rb") as f:
        file_size = os.path.getsize(input_file_name)
    
    chunk_size = max(1048576, file_size // num_procs)  # Minimum 1MB chunks
    chunks = [(i * chunk_size, min((i + 1) * chunk_size, file_size))
              for i in range((file_size + chunk_size - 1) // chunk_size)]
    
    # Process chunks in parallel
    with multiprocessing.Pool(num_procs) as pool:
        results = pool.starmap(process_chunk, 
                             ((input_file_name, start, end) for start, end in chunks))
    
    # Merge results and write output
    final_stats = merge_results(results)
    
    with open(output_file_name, "wb", buffering=8192) as f:
        for city in sorted(final_stats.keys()):
            stats = final_stats[city]
            mean = stats[2] / stats[3]
            f.write(f"{city.decode()}={round_inf(stats[0]):.1f}/"
                   f"{round_inf(mean):.1f}/{round_inf(stats[1]):.1f}\n".encode())

if __name__ == "__main__":
    main()


    #hello