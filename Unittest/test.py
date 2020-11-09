import unittest
from unittest.mock import patch as patch, Mock as Mock
from etl.extract import extract_from_csv
from etl.transform.card_format import card_num_format
from etl.transform.transform_purchase import check_for_flavour
from etl.transform.transform_datetime import clean_datetime,clean_total_price
from etl.transform.transform_purchase import make_drink_info_list,check_for_drink_size,transform_purchases,make_split_info_list
import etl.transform.transform_customer

#====================Extract=========================================================================
class TestExtract(unittest.TestCase):
    def test_convert_string_to_list(self):
        pass
        #ARRANGE
        mock_csv_string = "['1/2/3', 'Place', 'John Doe', 'Tea - Earl Grey - 1.00', '5.55', 'CARD', '123456789']"
        expected_return = ['1/2/3', 'Place', 'John Doe', 'Tea - Earl Grey - 1.00', '5.55', 'CARD', '123456789']

        #ACT
        actual_return = extract_from_csv.convert_csv_string_to_list(mock_csv_string)
        
        #ASSERT
        self.assertEqual(actual_return, expected_return)


    def test_generate_dictionary(self):
        #ARRANGE
        mock_csv_strings1 = ["['11/10/2020 08:11', 'Aberdeen', 'Joan Pickles', 'Large Flavoured latte - Gingerbread - 2.85, Large Flavoured latte - Vanilla - 2.85, Large Latte - 2.45, Large Flavoured latte - Gingerbread - 2.85', '11.00', 'CARD', '5359353452578239']"]
        expected_dictionary1 = {"datetime": ['11/10/2020 08:11'], "location": ['Aberdeen'], "customer_name": ['Joan Pickles'], "purchase": ['Large Flavoured latte - Gingerbread - 2.85, Large Flavoured latte - Vanilla - 2.85, Large Latte - 2.45, Large Flavoured latte - Gingerbread - 2.85'], "total_price": ['11.00'], "payment_method": ['CARD'], "card_number": ['5359353452578239']}

        #mock_csv_strings2 = 
        #expected_dictionary2 = 

        #ACT
        actual_dictionary1 = extract_from_csv.generate_dictionary(mock_csv_strings1)

        #ASSERT
        self.assertEqual(actual_dictionary1, expected_dictionary1)


#====================Transform=========================================================================
class Test_card_number_format(unittest.TestCase):
    @patch("etl.transform.card_format.card_num_format")
    def test_last_four_digits_returned_from_card_number_format(self, mock_card_num_format):
        # Arrange
        mock_num_star = ['1234567891234567', '123456789123']
        expect = ['************4567', '********9123']

        # Act
        actual = card_num_format(mock_num_star)

        # Assert
        self.assertEqual(actual, expect)

class Test_card_number_req(unittest.TestCase):
    @patch("etl.transform.card_format.card_num_format")
    def test_if_number_entered_incorrectly_returns_does_not_meet_requirement_message(self, mock_card_num_format):
        # Arrange
        mock_num_star = ['12345678912345678', '12345678912', 'abcdefg']
        expect = []

        # Act
        actual = card_num_format(mock_num_star)

        # Assert
        self.assertEqual(actual, expect)

class Test_check_flavour(unittest.TestCase):
    @patch('etl.transform.transform_purchase.check_for_flavour')
    def test_check_flavour_with_empty_list(self, mock_test_check_flavour):
        #Arrange        
        mock_return_list = []
        expect = []

        #Act
        actual = check_for_flavour(mock_return_list)
        #Assert
        self.assertEqual(actual,expect)

class Test_check_flavour(unittest.TestCase):
    @patch('etl.transform.transform_purchase.check_for_flavour')
    def test_check_flavour_work(self, mock_test_check_flavour):
        #Arrange        
        mock_return_list = ['Large Flavoured latte', 'Gingerbread', '2.85'], ['Large Flavoured latte', None, '2.85'], ['Large Flavoured latte', 'Gingerbread', '2.85']
        expect = [['Large Flavoured latte', 'Gingerbread', '2.85'], ['Large Flavoured latte', None, '2.85'], ['Large Flavoured latte', 'Gingerbread', '2.85']]

        #Act
        actual = check_for_flavour(mock_return_list)
        #Assert
        self.assertEqual(actual,expect)
class Test_clean_date_time(unittest.TestCase):
    @patch('etl.transform.transform_datetime.clean_datetime')
    def test_if_date_time_works(self,mock_clean_date_time):
        #Arrange
        mock_list = ["11/10/2020 08:11", "11/10/2020 08:11"]
        expect = ["2020-10-11 08:11:00", "2020-10-11 08:11:00"]

        # Act
        actual = clean_datetime(mock_list)

        # Assert
        self.assertEqual(actual, expect)
class Test_make_drink_info_list(unittest.TestCase):
    @patch("etl.transform.transform_purchase.make_drink_info_list")
    def test_drink_list_returns_seperated(self, mock_make_drink_info_list):
        # Arrange
        mock_split_info_list = ["Large Flavoured latte - Gingerbread - 2.85", "Large Flavoured latte - Vanilla - 2.85", "Large Flavoured latte - Gingerbread - 2.85"]
        expect = [['Large Flavoured latte', 'Gingerbread', '2.85'], ['Large Flavoured latte', 'Vanilla', '2.85'], ['Large Flavoured latte', 'Gingerbread', '2.85']]

        # Act
        actual = make_drink_info_list(mock_split_info_list)

        # Assert
        self.assertEqual(actual, expect)

class Test_for_drink_size(unittest.TestCase):
    @patch('etl.transform.transform_purchase.check_for_drink_size')
    def test_check_correct_drink_size(self, mock_test_if_drink_size):
        #Arrange
        mock_return_list = []
        expect = []

        #Act
        actual = check_for_drink_size(mock_return_list)

        #Assert
        self.assertEqual(expect,actual)

class Test_for_drink_size(unittest.TestCase):
    @patch('etl.transform.transform_purchase.check_for_drink_size')
    def test_check_correct_drink_size(self, mock_test_if_drink_size):
        #Arrange
        size = [['Large Flavoured latte', 'Gingerbread', '2.85']]

        expect = [['Large', 'Flavoured latte', 'Gingerbread', '2.85']]

        #Act
        actual = check_for_drink_size(size)

        #Assert
        self.assertEqual(expect,actual)

class Test_empty_transform_purchases(unittest.TestCase):
    @patch('etl.transform.transform_purchase.transform_purchases')
    def test_if_transform_purchases_returns_list_of_dicts_seperated_by_comma(self, mock_transform_purchases):
        # Arrange
        mock_purchases = []
        expect = []

        # Act
        actual = transform_purchases(mock_purchases)

        # Assert
        self.assertEqual(actual, expect)

class Test_for_drink_size(unittest.TestCase):
    @patch('etl.transform.transform_purchase.check_for_drink_size')
    def test_check_correct_drink_size(self, mock_test_if_drink_size):
        #Arrange
        size = [['Large Flavoured latte', 'Gingerbread', '2.85']]

        expect = [['Large', 'Flavoured latte', 'Gingerbread', '2.85']]

        #Act
        actual = check_for_drink_size(size)

        #Assert
        self.assertEqual(expect,actual)

class Test_make_split_info_list(unittest.TestCase):
    @patch('etl.transform.transform_purchase.make_split_info_list')
    def test_if_make_split_info_list_returns_output(self, mock_make_split_info_list):
        #Arrange        
        mock_split_info_list = [["Large", "Flavoured latte", "Gingerbread", "2.85"], ["Large", "Flavoured latte", "Vanilla", "2.85"], \
            ["Large", "Flavoured latte", "Gingerbread", "2.85"]]
        expect = (['Large', 'Large', 'Large'], ['Flavoured latte', 'Flavoured latte', 'Flavoured latte'], \
            ['Gingerbread', 'Vanilla', 'Gingerbread'], ['2.85', '2.85', '2.85'])
        #Act
        actual = make_split_info_list(mock_split_info_list)
        #Assert
        self.assertEqual(actual, expect)

class Test_clean_total_price(unittest.TestCase):
    @patch("etl.transform.transform_datetime.clean_total_price")
    def test_if_total_price_returns(self, mock_clean_total_price):
        # Arrange
        mock_list = [11, 2.80]
        expect = [float(11), float(2.80)]

        # Act
        actual = clean_total_price(mock_list)

        # Assert
        self.assertEqual(actual, expect)

class Test_clean_customer_name(unittest.TestCase):
    def test_transform_fullname(self):
        #ARRANGE
        mock_customer_name = [" joan b pickles", " Ross Waits", "Ann Vanburen", "Linda Motes", "Jerome Guinyard"]
        expected_return = (['Joan', 'Ross', 'Ann', 'Linda', 'Jerome'], ['B Pickles', 'Waits', 'Vanburen', 'Motes', 'Guinyard'])

        #ACT
        actual_return = etl.transform.transform_customer.clean_customer_name(mock_customer_name)

        #ASSERT
        self.assertEqual(actual_return, expected_return)

class Test_transform_purchases(unittest.TestCase):
    @patch('etl.transform.transform_purchase.transform_purchases')
    def test_if_transform_purchases_returns_list_of_dicts_seperated_by_comma(self, mock_transform_purchases):
        # Arrange
          mock_purchases = ["Large Flavoured latte - Gingerbread - 2.85", "Large Flavoured latte - Vanilla - 2.85", \
            "Large Flavoured latte - Gingerbread - 2.85"]
     
          expect = [{'drink_flavour': ['Gingerbread'],
          'drink_price': ['2.85'],
          'drink_size': ['Large'],
          'drink_type': ['Flavoured latte']},
          {'drink_flavour': ['Vanilla'],
          'drink_price': ['2.85'],
          'drink_size': ['Large'],
          'drink_type': ['Flavoured latte']},
          {'drink_flavour': ['Gingerbread'],
          'drink_price': ['2.85'],
          'drink_size': ['Large'],
          'drink_type': ['Flavoured latte']}]

          # Act
          actual = transform_purchases(mock_purchases)

          # Assert
          self.assertEqual(actual, expect)
if __name__ == "__main__":
    unittest.main()