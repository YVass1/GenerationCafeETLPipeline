import unittest
from unittest.mock import patch as patch, Mock as Mock

import extract_from_csv


class TestExtract(unittest.TestCase):
    @patch("extract_from_csv.convert_csv_string_to_list")
    def test_convert_string_to_list(self, mock_convert_string_to_list):
        pass
        #ARRANGE
        mock_csv_string = "['1/2/3', 'Place', 'John Doe', 'Tea - Earl Grey - 1.00', '5.55', 'CARD', '123456789']"
        expected_return = ['1/2/3', 'Place', 'John Doe', 'Tea - Earl Grey - 1.00', '5.55', 'CARD', '123456789']

        #ACT
        #actual_return = mock_convert_string_to_list(mock_csv_string)
        actual_return = extract_from_csv.convert_csv_string_to_list(mock_csv_string)

        print(actual_return)
        print(expected_return)

        #ASSERT
        self.assertEqual(actual_return, expected_return)


if __name__ == "__main__":
    unittest.main()