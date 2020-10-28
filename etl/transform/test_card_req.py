import unittest
from unittest.mock import Mock, patch
from card_format import card_num_format

class Test_card_number_req(unittest.TestCase):
    @patch("card_format.card_num_format")
    def test_if_number_entered_incorrectly_returns_does_not_meet_requirement_message(self, mock_card_num_format):
        # Arrange
        mock_num_star = ['12345678912345678', '12345678912', 'abcdefg']
        expect = []

        # Act
        actual = card_num_format(mock_num_star)

        # Assert
        self.assertEqual(actual, expect)

if __name__ == "__main__":
    unittest.main()

