from os import remove
import unittest
from generate_sample_data import *
from select_x_values import is_hex_uuid, is_integer, is_valid_data
from os.path import exists

class TestGenerateSampleData(unittest.TestCase):
    def test_generate_error_field(self):
        error_field = generate_error_field()
        self.assertFalse(is_hex_uuid(error_field))
        self.assertFalse(is_integer(error_field))
    
    def test_generate_error_data(self):
        error_data = generate_error_data()
        self.assertFalse(is_valid_data(error_data))
    
    def test_generate_sample_data(self):
        generate_sample_data(5, -5, 5, 0.0, "sample_data2.txt")
        output_path = ".\\input_data\\sample_data2.txt"
        self.assertTrue(exists(output_path))
        row_count = 0
        with open(output_path, "r", encoding="utf-8") as output:
            for line in output:
                self.assertTrue(is_valid_data(line[:-1]))
                row_count += 1
        self.assertEqual(row_count, 5)
        remove(output_path)
        self.assertFalse(exists(output_path))

if __name__ == '__main__':
    unittest.main()

