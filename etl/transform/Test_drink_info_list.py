import unittest
from unittest.mock import Mock, patch
from transform_purchase import make_drink_info_list

# def make_drink_info_list(drink_info_list):
#     split_info_list = []

#     for drink_info in drink_info_list:
#         split_info_list.append(drink_info.split(" - ")) #drink_info_list: ["Large Flavoured latte - Gingerbread - 2.85", "Large Flavoured latte - Vanilla - 2.85", "Large Flavoured latte - Gingerbread - 2.85"]

#     return split_info_list

class Test_make_drink_info_list(unittest.TestCase):
    @patch("transform_purchase.make_drink_info_list")
    def test_drink_list_returns_seperated(self, mock_make_drink_info_list):
        # Arrange
        mock_split_info_list = ["Large Flavoured latte - Gingerbread - 2.85", "Large Flavoured latte - Vanilla - 2.85", "Large Flavoured latte - Gingerbread - 2.85"]
        expect = [['Large Flavoured latte', 'Gingerbread', '2.85'], ['Large Flavoured latte', 'Vanilla', '2.85'], ['Large Flavoured latte', 'Gingerbread', '2.85']]

        # Act
        actual = make_drink_info_list(mock_split_info_list)

        # Assert
        self.assertEqual(actual, expect)

if __name__ == "__main__":
    unittest.main()
