import sys
from os.path import *
from os import mkdir
from heapq import heapify, heappop, heappush
# Global Variables
chunk_size = 1000000
valid_char_set = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'}
report_times = 20

def handle_inputs():
    '''
    Handle inputs from STDIN or Data File Path.
    '''
    arguments = sys.argv  
    # Both X and data file path are not provided.
    if len(arguments) < 2:
        print("The arguments provided are not enough. Please provide X and the content of data file.")
    # X is provided, data file path is not provided. Data are passed via STDIN
    elif len(arguments) == 2:
        X = arguments[1]
        # When X is invalid
        if not X.isdigit():
            print("The argument X is invalid. It must be a non-negative integer.")
        else:
            data = sys.stdin
            processing_data(int(X), data)
    # Both X and data file path are provided.
    elif len(arguments) == 3:
        X, file_path = arguments[1], arguments[2]
        # When X is invalid
        if not X.isdigit():
            print("The argument X is invalid. It must be a non-negative integer.")
        # When data file path does not exist or the path is a directory
        elif not exists(file_path) or not isfile(file_path):
            print("The argument file path is invalid. Please provide a path of an existed file.")
        else:
            with open(file_path, encoding="utf-8") as data:
                processing_data(int(X), data)
    
    return
            

def processing_data(X: int, data):  
    '''
    Processing possibly large amounts of data.  
    1. Scan the data file, generate data chunks, Initialize a heap.  
    2. Maintain the heap, keep pop elements from the heap and add elements to the heap. Output elements
    '''
    # Deal with corner cases
    if X == 0:
        output = open("output.txt", "w", encoding="utf-8")
        output.close()
        print("Since you did not want any values, an empty file output.txt has been generated. Program ended.")
        return
    
    global chunk_size
    global report_times
    total_row_count = 0
    total_error_row_count = 0
    chunk_id = 0
    curr_chunk_row_count = 0
    curr_chunk_data = []
    heap = []
    chunk_file_pointers = []

    for row in data:
        # Remove the ending '\n'
        row = row[:-1]
        # Current data row is invalid
        if not is_valid_data(row):
            total_row_count += 1
            total_error_row_count += 1
            continue
        unique_id, value = row.split(" ")
        value = int(value)
        # Temporarily store the data into a list, this step uses memory space
        curr_chunk_data.append((unique_id, value))
        # When the data in the memory could form a new data chunk
        if curr_chunk_row_count == chunk_size - 1:
            # Sort the data in descending order
            curr_chunk_data.sort(key= lambda x: x[1], reverse=True)
            first_id, first_value = curr_chunk_data[0]
            chunk_file = "chunk_data_" + str(chunk_id) + ".txt"
            # Write the data from memory to data chunk file on the disk
            chunk_file = write_chunk_data(chunk_file, curr_chunk_data)
            # We need a new file pointer to track this newly created chunk file
            curr_pointer = open(chunk_file, "r", encoding="utf-8")
            curr_pointer.readline()
            chunk_file_pointers.append(curr_pointer)
            # Add the first element of the chunk file into heap
            # Python 3 only supports min heap, in order to achieve max heap, we should store the opposite number of the value here
            heap.append((0 - first_value, first_id, chunk_id))
            # Release the space in memory
            curr_chunk_data.clear()
            curr_chunk_row_count = 0
            chunk_id += 1
            print("Progress: Have scanned {0} rows of data; have created {1} data chunks.".format(total_row_count + 1, chunk_id))
        else:
            curr_chunk_row_count += 1
        total_row_count += 1
    # Finished scanning the data files. Write the remaining data in the memory to a new data chunk
    if curr_chunk_data:
        # Completely the same process as above
        curr_chunk_data.sort(key= lambda x: x[1], reverse=True)
        first_id, first_value = curr_chunk_data[0]
        chunk_file = "chunk_data_" + str(chunk_id) + ".txt"
        chunk_file = write_chunk_data(chunk_file, curr_chunk_data)
        curr_pointer = open(chunk_file, "r", encoding="utf-8")
        curr_pointer.readline()
        chunk_file_pointers.append(curr_pointer)
        heap.append((0 - first_value, first_id, chunk_id))
        curr_chunk_data.clear()
        curr_chunk_row_count = 0
        chunk_id += 1
        print("Progress: Have scanned {0} rows of data; have created {1} data chunks.".format(total_row_count, chunk_id))
    # Report summary logs to SDTOUT console
    total_valid_row_count = total_row_count - total_error_row_count
    print("\nData scanning complete.\n \
            Total rows: {0}\n \
            Total error rows: {1}\n \
            Total valid rows: {2}\n \
            Total chunks splitted: {3}\n \
            Pre-defined chunk size: {4}\n" \
            .format(total_row_count, total_error_row_count, total_valid_row_count, chunk_id, chunk_size))
    # Adjust the list to a heap.
    heapify(heap)
    # Deal with corner cases, when X >= L (L is the number of rows in the data files)
    if X > total_valid_row_count:
        print("The value X={0} is greater than the total_valid_row_count={1} I've found. Will output all the valid rows.\n" \
        .format(X, total_valid_row_count))
        X = total_valid_row_count
    
    output_row_count = 0
    closed_chunk_files = set()

    should_report_progress = X >= report_times
    report_interval = X // report_times
    # Start to output data
    with open("output.txt", "w", encoding="utf-8") as output:
        progress = 0.0
        print("Start to output data. Progress: {0:.1%}".format(progress))
        while heap and output_row_count < X:
            # The top element should be the overall biggest one at present
            curr_value, curr_unique_id, curr_chunk= heappop(heap)
            curr_value = 0 - curr_value
            output.write(curr_unique_id + "\n")
            # Try to find the next element which is in the same chunk as the top element
            next_row = chunk_file_pointers[curr_chunk].readline()
            # This means the current top element is already the last one in its chunk
            if not next_row:
                chunk_file_pointers[curr_chunk].close()
                closed_chunk_files.add(curr_chunk)
            else:
                # We found the next element, add it to the heap
                next_row = next_row[:-1]
                next_id, next_value = next_row.split(" ")
                next_value = int(next_value)
                heappush(heap,(0 - next_value, next_id, curr_chunk))
            
            output_row_count += 1
            # Output progress logs when reaching to a certain number of rows
            if should_report_progress and output_row_count % report_interval == 0:
                progress = float(output_row_count) / X
                print("Have output {0} rows of data. Progress: {1:.1%}".format(output_row_count, progress))
    
    print("Complete outputting data. Progress: 100.0%\n")
    print("Data processing complete. Total output rows: {0}. Please check the output.txt file to view the results.\n" \
    .format(output_row_count))

    # Data output is finished. Close all the rest of the chunk files which are still open.
    remained_open_count = len(chunk_file_pointers) - len(closed_chunk_files)
    print("There are {0} chunk file(s) remained open. Try to close them.".format(remained_open_count))

    for i in range(len(chunk_file_pointers)):
        if i not in closed_chunk_files:
            chunk_file_pointers[i].close()
            closed_chunk_files.add(i)
    
    print("All chunk files closed. Program ended.")

    # Complexity Analysis: 
    # Time Complexity: O(L * log(C) + X * log(L / C))
    # Space Complexity: O(C) + O(L / C)
    # Please check README.md for more details about the complexity analysis.
    return

def write_chunk_data(chunk_file: str, chunk_data: list):
    '''
    Write the data in memory to a new data chunk file.
    '''
    curr_dir = dirname(realpath(__file__))
    save_dir = curr_dir + "/chunk_data/"
    if not exists(save_dir):
        mkdir(save_dir)
    chunk_file = save_dir + chunk_file
    with open(chunk_file, "w", encoding="utf-8") as file:
        for row in chunk_data:
            unique_id, value = row
            curr_data = unique_id + " " + str(value) + "\n"
            file.write(curr_data)
    return chunk_file

def is_valid_data(curr_row) -> bool:
    '''
    Judge wheter a data row is valid.
    '''
    row_data = curr_row.split(" ")
    if len(row_data) != 2:
        return False
    unique_id, value = row_data
    if not (unique_id and value):
        return False
    if not is_hex_uuid(unique_id):
        return False
    if not is_integer(value):
        return False
    return True

def is_integer(num: str) -> bool:
    '''
    Judge whether a string could be converted to an integer.
    '''
    return num.isdigit() or (len(num) > 1 and num[0] == '-' and num[1:].isdigit())

def is_hex_uuid(unique_id: str) -> bool:
    '''
    Judge whether a string is a valid hexadecimal uuid.
    '''
    if len(unique_id) != 32:
        return False
    global valid_char_set

    for digit in unique_id:
        if digit not in valid_char_set:
            return False
    
    return True
    
if __name__ == '__main__':
    handle_inputs()
    
        
    