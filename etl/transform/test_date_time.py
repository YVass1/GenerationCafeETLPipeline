import unittest
from unittest.mock import Mock,patch
from transform_functions_johnny import clean_datetime

class Test_clean_date_time(unittest.TestCase):
    @patch('transform_functions_johnny.clean_datetime')
    def test_if_date_time_works(self,mock_clean_date_time):
        #Arrange
        mock_list = ["11/10/2020 08:11", "11/10/2020 08:11"]
        expect = ["2020-10-11 08:11:00", "2020-10-11 08:11:00"]

        # Act
        actual = clean_datetime(mock_list)

        # Assert
        self.assertEqual(actual, expect) 


if __name__ == '__main__':
    unittest.main()
    


