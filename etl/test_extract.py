import unittest
from unittest.mock import patch as patch, Mock as Mock

import extract_from_csv


class TestExtract(unittest.TestCase):
    @patch("extract_from_csv.convert_csv_string_to_list")
    def test_convert_string_to_list(self, mock_convert_string_to_list):
        pass
        #ARRANGE
        mock

        #ACT


        #ASSERT