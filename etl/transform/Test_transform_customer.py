import unittest
from unittest.mock import patch as patch, Mock as Mock
import etl.transform_customer

class Test_clean_customer_name(unittest.TestCase):
    def test_transform_fullname(self):
        #ARRANGE
        mock_customer_name = [" joan b pickles", " Ross Waits", "Ann Vanburen", "Linda Motes", "Jerome Guinyard"]
        expected_return = (['Joan', 'Ross', 'Ann', 'Linda', 'Jerome'], ['B Pickles', 'Waits', 'Vanburen', 'Motes', 'Guinyard'])

        #ACT
        actual_return = etl.transform_customer.clean_customer_name(mock_customer_name)

        #ASSERT
        self.assertEqual(actual_return, expected_return)
   
if __name__ == "__main__":
    unittest.main()