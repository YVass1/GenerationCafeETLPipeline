import unittest
from unittest.mock import Mock,patch
from transform_purchase import check_for_flavour

class Test_check_flavour(unittest.TestCase):
    @patch('transform_purchase.check_for_flavour')
    def test_check_flavour_work(self, mock_test_check_flavour):
        #Arrange        
        mock_return_list = ['Large Flavoured latte', 'Gingerbread', '2.85'], ['Large Flavoured latte', None, '2.85'], ['Large Flavoured latte', 'Gingerbread', '2.85']
        expect = [['Large Flavoured latte', 'Gingerbread', '2.85'], ['Large Flavoured latte', None, '2.85'], ['Large Flavoured latte', 'Gingerbread', '2.85']]

        #Act
        actual = check_for_flavour(mock_return_list)
        #Assert
        self.assertEqual(actual,expect)




if __name__ == "__main__":
    unittest.main()
