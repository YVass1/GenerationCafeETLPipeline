# Tests if the flavour has the right resulting output when two empty square brackets are used
import unittest
from unittest.mock import Mock,patch
from transform_purchase import check_for_flavour

class Test_check_flavour(unittest.TestCase):
    @patch('transform_purchase.check_for_flavour')
    def test_check_flavour_with_empty_list(self, mock_test_check_flavour):
        #Arrange        
        mock_return_list = []
        expect = []

        #Act
        actual = check_for_flavour(mock_return_list)
        #Assert
        self.assertEqual(actual,expect)




if __name__ == "__main__":
    unittest.main()
