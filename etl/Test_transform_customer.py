import unittest
from unittest.mock import patch as patch, Mock as Mock
import etl.transform_customer as transform_customer

class Test_clean_customer_name(unittest.TestCase):
    @patch("transform_customer.clean_customer_name")
    def test_transform_fullname(self, mock_transform_fullname):
        pass
        #ARRANGE
        mock_customer_name = [" joan b pickles", " Ross Waits", "Ann Vanburen", "Linda Motes", "Jerome Guinyard"]
        expected_return = (['Joan', 'Ross', 'Ann', 'Linda', 'Jerome'], ['B Pickles', 'Waits', 'Vanburen', 'Motes', 'Guinyard'])

        #ACT
        actual_return = transform_customer.transform_fullname(mock_customer_name)
        print(actual_return)
        print(expected_return)

        #ASSERT
        self.assertEqual(actual_return, expected_return)
   

if __name__ == "__main__":
    unittest.main()