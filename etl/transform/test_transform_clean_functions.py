import unittest
from unittest.mock import Mock, patch
from transform_functions_johnny import clean_total_price

class Test_clean_total_price(unittest.TestCase):
    @patch("transform_functions_johnny.clean_total_price")
    def test_if_total_price_returns(self, mock_clean_total_price):
        # Arrange
        mock_list = [11, 2.80]
        expect = [float(11), float(2.80)]

        # Act
        actual = clean_total_price(mock_list)

        # Assert
        self.assertEqual(actual, expect)

if __name__ == "__main__":
    unittest.main()
