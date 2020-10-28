# Second one

# ef check_for_drink_size(split_copy_list):
#     return_list = ['Large Flavoured latte', 'Gingerbread', '2.85']
#     for split_info in split_copy_list:
#         print(split_info)
#         split_copy = split_info.copy() #['Large Flavoured latte', 'Gingerbread', '2.85']
#         looking_for_space = split_copy[0].find(" ")
#         stripped_string = split_copy[0].strip()
#         remove_drink_size = stripped_string[(looking_for_space+1):]     
#         if "Large " in split_copy[0].title():
#             split_copy.insert(0, "Large")
#             split_copy.insert(1, remove_drink_size)
#             del split_copy[2]
#             return_list.append(split_copy)
#         elif "Regular " in split_copy[0].title():
#             split_copy.insert(0, "Regular")
#             split_copy.insert(1, remove_drink_size)
#             del split_copy[2]
#             return_list.append(split_copy)
#         else:
#             split_copy.insert(0, None)
#             return_list.append(split_copy)
#     print(return_list)
import unittest
from unittest.mock import Mock,patch
from transform_purchase import check_for_drink_size

class Test_for_drink_size(unittest.TestCase):
    @patch('transform_purchase.check_for_drink_size')
    def test_check_correct_drink_size(self, mock_test_if_drink_size):
        #Arrange
        mock_return_list = []
        expect = []

        #Act
        actual = check_for_drink_size(mock_return_list)

        #Assert
        self.assertEqual(expect,actual)

if __name__ == '__main__':
    unittest.main()