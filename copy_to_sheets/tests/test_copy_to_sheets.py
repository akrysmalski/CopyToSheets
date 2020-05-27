import unittest
import pandas as pd
from copy_to_sheets import (
    Config,
    LocalSpreadsheet,
    GoogleSpreadsheet,
    num_to_col
)


class TestConfig(unittest.TestCase):

    def test_load_raises_file_not_found(self):
        """
        Check if FileNotFoundError raises if path to the config file
        does not exist
        """
        config_path = './that_config_path_does_not_exist.json'
        self.assertRaises(FileNotFoundError, Config, config_path)

    def test_load_raises_lookup_error(self):
        """
        Check if LookupError raises if config does not contains
        necessary attribute
        """
        config_path = './tests/test_config_wrong.json'
        self.assertRaises(LookupError, Config, config_path)

    def test_load_creates_attributes(self):
        """
        Check if load method creates attributes from config
        """
        config_path = './tests/test_config.json'
        config = Config(config_path)

        for attribute in Config.attributes:
            self.assertTrue(
                hasattr(config, attribute)
            )


class TestLocalSpreadsheet(unittest.TestCase):

    def test_load_raises_file_not_found(self):
        """
        Check if FileNotFoundError raises if path to the config file
        does not exist
        """
        excel_path = './that_excel_file_does_not_exist.xlsx'
        self.assertRaises(FileNotFoundError, LocalSpreadsheet, excel_path)

    def test_active_sheet_setter_raises_value_error(self):
        """
        Check if ValueError raises if user provides wrong value for
        active_sheet attribute
        """
        excel_path = './tests/test_data.xlsx'
        spreadsheet = LocalSpreadsheet(excel_path)

        with self.assertRaises(ValueError) as error:
            spreadsheet.active_sheet = -1

        with self.assertRaises(ValueError) as error:
            spreadsheet.active_sheet = len(spreadsheet.sheets_names)

    def test_dataframe_property(self):
        """
        Check if dataframe property returns pandas dataframe object
        """
        excel_path = './tests/test_data.xlsx'
        spreadsheet = LocalSpreadsheet(excel_path)

        self.assertTrue(isinstance(spreadsheet.dataframe, pd.DataFrame))


class TestGoogleSpreadsheet(unittest.TestCase):

    def setUp(self):
        self.config = Config('./tests/test_config.json')
        self.google_spreadsheet = GoogleSpreadsheet(
            self.config.destination,
            self.config.credentials
        )

    def test_auth_raises_file_not_found(self):
        """
        Check if FileNotFoundError raises if path to the credentials file
        does not exist
        """
        credentials_path = './that_credentials_file_does_not_exist.xlsx'
        spreadsheet_id = ''
        self.assertRaises(
            FileNotFoundError,
            GoogleSpreadsheet,
            spreadsheet_id,
            credentials_path
        )

    def test_get_sheets(self):
        """
        Check if get_sheets returns a list of sheets names
        """
        self.assertTrue(isinstance(self.google_spreadsheet.sheets, list))
        self.assertTrue(len(self.google_spreadsheet.sheets) > 0)

    def test_append_sheet(self):
        """
        Check if append_sheet method adds new sheet
        """
        sheets = len(self.google_spreadsheet.sheets)
        result = self.google_spreadsheet.append_sheet('Sheet1')
        self.assertTrue(isinstance(result, dict))
        self.assertEqual(sheets, len(self.google_spreadsheet.sheets) - 1)

    def test_add_data(self):
        """
        Check if add_data adds values in a given range
        """
        result = self.google_spreadsheet.add_data([{
            'range': 'Sheet1!A1:A2',
            'values': [[1], [2]]
        }])
        self.assertTrue(isinstance(result, dict))

    def test_num_to_col(self):
        """
        Check if num_to_col function coverts index to lettr properly
        """
        test_data = {
            0: 'A',
            1: 'B',
            2: 'C',
            26: 'AA',
            27: 'AB',
            28: 'AC'
        }
        for key, value in test_data.items():
            self.assertEqual(value, num_to_col(key))


if __name__ == '__main__':
    unittest.main()
