import math
import mmap
import multiprocessing

def round_up(value):
    return math.ceil(value * 10) / 10  

def process_segment(file_path, start_pos, end_pos):
    city_stats = {}
    with open(file_path, "rb") as file:
        memory_map = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        file_size = len(memory_map)
        
        if start_pos != 0:
            while start_pos < file_size and memory_map[start_pos] != ord('\n'):
                start_pos += 1
            start_pos += 1
        
        segment_end = end_pos
        while segment_end < file_size and memory_map[segment_end] != ord('\n'):
            segment_end += 1
        if segment_end < file_size:
            segment_end += 1
        
        segment = memory_map[start_pos:segment_end]
        memory_map.close()
    
    for entry in segment.splitlines():
        if not entry:
            continue
        
        city, sep, temperature = entry.partition(b';')
        if sep != b';':
            continue
        
        try:
            temp_value = float(temperature)
        except ValueError:
            continue
        
        if city in city_stats:
            current = city_stats[city]
            if temp_value < current[0]:
                current[0] = temp_value
            if temp_value > current[1]:
                current[1] = temp_value
            current[2] += temp_value
            current[3] += 1
        else:
            city_stats[city] = [temp_value, temp_value, temp_value, 1]
    
    return city_stats

def combine_results(result_list):
    consolidated = {}
    for record in result_list:
        for city, stats in record.items():
            if city in consolidated:
                combined = consolidated[city]
                if stats[0] < combined[0]:
                    combined[0] = stats[0]
                if stats[1] > combined[1]:
                    combined[1] = stats[1]
                combined[2] += stats[2]
                combined[3] += stats[3]
            else:
                consolidated[city] = stats.copy()
    return consolidated

def main(input_file="testcase.txt", output_file="output.txt"):
    with open(input_file, "rb") as file:
        memory_map = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        total_size = len(memory_map)
        memory_map.close()
    
    num_workers = multiprocessing.cpu_count() * 2  
    segment_size = total_size // num_workers
    segments = [(i * segment_size, (i + 1) * segment_size if i < num_workers - 1 else total_size)
                for i in range(num_workers)]
    
    with multiprocessing.Pool(num_workers) as pool:
        tasks = [(input_file, start, end) for start, end in segments]
        results = pool.starmap(process_segment, tasks)
    
    final_output = combine_results(results)
    
    sorted_results = []
    for city in sorted(final_output.keys(), key=lambda name: name.decode()):
        min_temp, max_temp, total_temp, occurrences = final_output[city]
        avg_temp = round_up(total_temp / occurrences)
        sorted_results.append(f"{city.decode()}={round_up(min_temp):.1f}/{avg_temp:.1f}/{round_up(max_temp):.1f}\n")
    
    with open(output_file, "w") as file:
        file.writelines(sorted_results)

if __name__ == "__main__":
    main()


    #hello