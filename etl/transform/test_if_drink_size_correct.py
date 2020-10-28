import unittest
from unittest.mock import Mock,patch
from transform_purchase import check_for_drink_size

class Test_for_drink_size(unittest.TestCase):
    @patch('transform_purchase.check_for_drink_size')
    def test_check_correct_drink_size(self, mock_test_if_drink_size):
        #Arrange
        size = [['Large Flavoured latte', 'Gingerbread', '2.85']]
        # mock_test_if_drink_size.return_value  = []
        # mock_return_list = ['Large Flavoured latte', 'Gingerbread', '2.85'], ['Speciality Tea', 'Green', '1.30'], ['Regular Flavoured latte', 'Vanilla', '3.85'], ['Tea', 'Mint tea', '1.85'], ['Large Hot chocolate', None, '2.90'], ['Smoothies', None, '2.75']


        expect = [['Large', 'Flavoured latte', 'Gingerbread', '2.85']]

        #Act
        actual = check_for_drink_size(size)

        #Assert
        self.assertEqual(expect,actual)

if __name__ == '__main__':
    unittest.main()