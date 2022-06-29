from uuid import uuid4
from random import choices, randint
from os.path import *
from os import mkdir

report_times = 25

def generate_sample_data(size: int, lower_bound: int, upper_bound: int, error_rate: float = 0.0, data_file: str = "sample_data.txt"):
    '''
    Generate various data files for the select_x_values.py to run. Could generate both valid & invalid data.
    '''
    global report_times
    curr_dir = dirname(realpath(__file__))
    save_dir = curr_dir + "/input_data/"
    add_error_choices = [True, False]
    error_weights = [error_rate, 1.0 - error_rate]
    should_report_progress = size >= report_times
    report_interval = size // report_times
    error_row_count = 0
    if not exists(save_dir):
        mkdir(save_dir)
    # Start to write data to the target data file
    with open(save_dir + data_file, "w", encoding="utf-8") as file:
        progress = 0.0
        print("Start to generate sample data. Progress: {0:.1%}".format(progress))
        # Each valid data row is generated by a uuid combined with a random integer
        for i in range(size):
            id = uuid4().hex
            value = randint(lower_bound, upper_bound)
            row = str(id) + " " + str(value) + "\n"
            file.write(row)
            # Randomly decide whether we should add an invalid record here, using the error_rate as probability
            should_add_error = choices(add_error_choices, error_weights)[0]
            if should_add_error:
                error_row = generate_error_data() + "\n"
                file.write(error_row)
                error_row_count += 1
            # Output progress logs when reaching to a certain number of rows
            if should_report_progress and (i + 1) % report_interval == 0:
                progress = float((i + 1)) / size
                print("Have generated {0} rows of valid sample data. Progress: {1:.1%}".format(i + 1, progress))
        
        print("Complete generating sample data. Progress: 100.0%\n")
        print("Total rows: {0}\nError rows: {1}\nValid rows: {2}\n".format(size + error_row_count, error_row_count, size))
    # Time Complexity: O(L), where L = the size of the data file
    return

def generate_error_data() -> str:
    '''
    Generate a row of invalid data.
    '''
    n = randint(1, 5)
    error_data = []
    i = 0
    while i < n:
        error_field = generate_error_field()
        error_data.append(error_field)
        i += 1
    return " ".join(error_data)

def generate_error_field() -> str:
    '''
    Generate an invalid field, which is a part of an invalid row.
    '''
    error_field = ""
    field_length = randint(1, 32)
    for _ in range(field_length):
        ascii_code = randint(0, 255)
        ch = chr(ascii_code)
        while ch.isspace():
            ascii_code = randint(0, 255)
            ch = chr(ascii_code)
        error_field += ch
    return error_field

if __name__ == '__main__':
    generate_sample_data(10, -10, 10, 0.0)