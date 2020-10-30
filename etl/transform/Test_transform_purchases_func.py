import unittest
from unittest.mock import Mock, patch
from transform_purchase import transform_purchases

# def transform_purchases(purchases):
    
#     list_of_dicts = []
    
#     for purchase in purchases:
#         new_dict = {}
        
#         stripped_purchase = purchase.strip()
#         drink_info_list = stripped_purchase.split(", ") #

#         split_info_list = make_drink_info_list(drink_info_list)
#         split_info_list_with_nones = check_for_flavour(split_info_list) #this adds None to flavour index when no flavour is provided
#         split_info_with_size = check_for_drink_size(split_info_list_with_nones)
#         drink_info_lists = make_split_info_list(split_info_with_size)

#         drink_size_list = drink_info_lists[0]
#         drink_type_list = drink_info_lists[1]
#         drink_flavour_list = drink_info_lists[2]
#         drink_price_list = drink_info_lists[3]

#         new_dict["drink_size"] = drink_size_list
#         new_dict["drink_type"] = drink_type_list
#         new_dict["drink_flavour"] = drink_flavour_list
#         new_dict["drink_price"] = drink_price_list

#         list_of_dicts.append(new_dict)
 
#     return list_of_dicts


# test = transform_purchases(["Large Flavoured latte - Gingerbread - 2.85", "Speciality Tea - Green - 1.30", "Regular Flavoured latte - Vanilla - 3.85", "Tea - Mint tea - 1.85", "Large Hot chocolate - 2.90", "Smoothies - 2.75"])

class Test_transform_purchases(unittest.TestCase):
    @patch('transform_purchase.transform_purchases')
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