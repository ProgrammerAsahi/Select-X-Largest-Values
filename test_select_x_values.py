from os import remove
import unittest
from select_x_values import handle_inputs, is_hex_uuid, is_integer, is_valid_data
from os.path import exists
import sys
class TestSelectXValues(unittest.TestCase):
    def test_handle_inputs_with_valid_inputs(self):
        output_path = ".\\output.txt"
        if exists(output_path):
            remove(output_path)
        sys.argv.append('3')
        sys.argv.append('.\\input_data\\sample_data.txt')
        handle_inputs()
        self.assertTrue(exists(output_path))
        unique_id_set = set()
        with open(output_path, "r", encoding="utf-8") as output:
            for line in output:
                unique_id_set.add(line[:-1])
        self.assertTrue(len(unique_id_set) == 3)
        self.assertTrue("ae2de84e35644c929c69b5ac0a793d27" in unique_id_set)
        self.assertTrue("16760fbf5eb14e149f5fa2fea38b5bb3" in unique_id_set)
        self.assertTrue("9c9b751a6cbc492cad99e25f0202b5da" in unique_id_set)
    
    def test_is_valid_data_with_valid_row(self):
        curr_row = "103bdd6e2ad544f49e008977d62f83bf -9"
        result = is_valid_data(curr_row)
        self.assertTrue(result)
    
    def test_is_valid_data_with_invalid_row(self):
        curr_row = "g;awefj;akldjf;akw ;oia;dvokjad;flkvaj; a;eiorgjao;v.aemfk"
        result = is_valid_data(curr_row)
        self.assertFalse(result)
    
    def test_is_integer_with_integer(self):
        num = "-52789"
        result = is_integer(num)
        self.assertTrue(result)
    
    def test_is_integer_with_non_integer(self):
        num = "5a2b7c8d9"
        result = is_integer(num)
        self.assertFalse(result)
    
    def test_is_hex_uuid_with_valid_id(self):
        id = "6f428ad9e2dd46dbb7da463bc895194b"
        result = is_hex_uuid(id)
        self.assertTrue(result)
    
    def test_is_hex_uuid_with_valid_id(self):
        id = "6g428ad9e2dd46dhb7da463bc895194b"
        result = is_hex_uuid(id)
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main()
        