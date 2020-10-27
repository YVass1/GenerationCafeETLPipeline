import unittest
from unittest.mock import Mock,patch
from transform_purchase import check_for_flavour

# def check_for_flavour(split_info_list):
#     return_list = []
    
#     for split_info in split_info_list:
#         info_copy = split_info.copy()
        
#         if len(info_copy) == 3:
#             return_list.append(info_copy)
#         else:
#             info_copy.insert(1, None)
#             return_list.append(info_copy)

#     return return_list 
#return_list: [['Large Flavoured latte', 'Gingerbread', '2.85'], ['Large Flavoured latte', None, '2.85'], ['Large Flavoured latte', 'Gingerbread', '2.85']]

class Test_check_flavour(unittest.TestCase):
    @patch('transform_purchase.check_for_flavour')
    def test_check_flavour_correct(self, mock_test_check_flavour):
        #Arrange        
        mock_return_list = []
        expect = []

        #Act
        actual = check_for_flavour(mock_return_list)
        #Assert
        self.assertEqual(actual,expect)




if __name__ == "__main__":
    unittest.main()
