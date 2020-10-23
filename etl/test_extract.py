import unittest
from unittest.mock import patch as patch, Mock as Mock

import extract_from_csv


class TestExtract(unittest.TestCase):
    def test_convert_string_to_list(self):
        pass
        #ARRANGE
        mock_csv_string = "['1/2/3', 'Place', 'John Doe', 'Tea - Earl Grey - 1.00', '5.55', 'CARD', '123456789']"
        expected_return = ['1/2/3', 'Place', 'John Doe', 'Tea - Earl Grey - 1.00', '5.55', 'CARD', '123456789']

        #ACT
        actual_return = extract_from_csv.convert_csv_string_to_list(mock_csv_string)
        
        #ASSERT
        self.assertEqual(actual_return, expected_return)


    def test_generate_dictionary(self):
        #ARRANGE
        mock_csv_strings1 = ["['11/10/2020 08:11', 'Aberdeen', 'Joan Pickles', 'Large Flavoured latte - Gingerbread - 2.85, Large Flavoured latte - Vanilla - 2.85, Large Latte - 2.45, Large Flavoured latte - Gingerbread - 2.85', '11.00', 'CARD', '5359353452578239']"]
        expected_dictionary1 = {"datetime": ['11/10/2020 08:11'], "location": ['Aberdeen'], "customer_name": ['Joan Pickles'], "purchase": ['Large Flavoured latte - Gingerbread - 2.85, Large Flavoured latte - Vanilla - 2.85, Large Latte - 2.45, Large Flavoured latte - Gingerbread - 2.85'], "total_price": ['11.00'], "payment_method": ['CARD'], "card_number": ['5359353452578239']}

        #mock_csv_strings2 = 
        #expected_dictionary2 = 

        #ACT
        actual_dictionary1 = extract_from_csv.generate_dictionary(mock_csv_strings1)

        #ASSERT
        self.assertEqual(actual_dictionary1, expected_dictionary1)

if __name__ == "__main__":
    unittest.main()