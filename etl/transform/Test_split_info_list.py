import unittest
from unittest.mock import Mock,patch
from transform_purchase import make_split_info_list

# split_info_list = [["Large Flavoured latte", "Gingerbread", "2.85"], ["Large Flavoured latte", "Vanilla", "2.85"], ["Large Flavoured latte", "Gingerbread", "2.85"]]

# def make_split_info_list(split_info_list): #split_info_list:[["Large Flavoured latte", "Gingerbread", "2.85"], ["Large Flavoured latte", "Vanilla", "2.85"], ["Large Flavoured latte", "Gingerbread", "2.85"]]
#     drink_size = []
#     drink_type_list = []
#     drink_flavour_list = []
#     drink_price_list = []
    
#     for split_info in split_info_list:
#         drink_size.append (split_info[0])
#         drink_type_list.append(split_info[1])
#         drink_flavour_list.append(split_info[2])
#         drink_price_list.append(split_info[3])

#     return (drink_size, drink_type_list, drink_flavour_list, drink_price_list)
# print(drink_size, drink_type_list, drink_flavour_list, drink_price_list)

class Test_make_split_info_list(unittest.TestCase):
    @patch('transform_purchase.make_split_info_list')
    def test_if_make_split_info_list_returns_output(self, mock_make_split_info_list):
        #Arrange        
        mock_split_info_list = [["Large", "Flavoured latte", "Gingerbread", "2.85"], ["Large", "Flavoured latte", "Vanilla", "2.85"], \
            ["Large", "Flavoured latte", "Gingerbread", "2.85"]]
        expect = (['Large', 'Large', 'Large'], ['Flavoured latte', 'Flavoured latte', 'Flavoured latte'], \
            ['Gingerbread', 'Vanilla', 'Gingerbread'], ['2.85', '2.85', '2.85'])
        #Act
        actual = make_split_info_list(mock_split_info_list)
        #Assert
        self.assertEqual(actual, expect)


if __name__ == "__main__":
    unittest.main()
