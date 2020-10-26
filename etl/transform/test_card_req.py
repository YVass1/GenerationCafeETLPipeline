import unittest
from unittest.mock import Mock, patch
from card_format import card_num_format

class Test_card_number_req(unittest.TestCase):
    @patch("card_format.card_num_format")
    def test_last_four_digits_returned_from_card_number_format(self, mock_card_num_format):
        # Arrange
        num_star = ['12345678912345678', '123456789123']
        mock_card_num_format.return_value = 1
        expect = f"Card Number does not meet requirement: {num_star}"

        # Act
        actual = card_num_format(num_star)

        # Assert
        self.assertEqual(actual, expect)

if __name__ == "__main__":
    unittest.main()

