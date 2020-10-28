import unittest
from unittest.mock import Mock, patch
from card_format import card_num_format

class Test_card_number_format(unittest.TestCase):
    @patch("card_format.card_num_format")
    def test_last_four_digits_returned_from_card_number_format(self, mock_card_num_format):
        # Arrange
        mock_num_star = ['1234567891234567', '123456789123']
        expect = ['************4567', '********9123']

        # Act
        actual = card_num_format(mock_num_star)

        # Assert
        self.assertEqual(actual, expect)

if __name__ == "__main__":
    unittest.main()

