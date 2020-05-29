"""
Copy whole excel spreadsheet into Google Sheets

@author: Arkadiusz Krysmalski
"""


import os
import sys
import json
import copy
import logging
import requests
import numpy as np
import pandas as pd

from threading import Thread
from datetime import datetime as dt
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials


# Default config path
CONFIG_PATH = './config.json'

# Setup logging
LOGGER_FORMAT = logging.Formatter(
    '%(asctime)s [%(levelname)s] - %(message)s',
    '%Y-%m-%d %H:%M:%S'
)
LOGGER_HANDLER = logging.StreamHandler()
LOGGER_HANDLER.setLevel(logging.INFO)
LOGGER_HANDLER.setFormatter(LOGGER_FORMAT)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(LOGGER_HANDLER)


class Config:
    """
    Config
    Attributes:
        source (str): path to a local excel file that will be copied
                        to Google spreadsheet
        destination (str): id of Google spreadsheet where the local file
                            should be copied
        credentials (str): path to google credentials needed by oauth
    """
    attributes = [
        'source',
        'destination',
        'credentials'
    ]

    def __init__(self, path):
        self.load(path)

    def load(self, path):
        """
        Read json config file and create attributes in the instance
        Params:
            path (str): path to the config file
        Raises:
            FileNotFoundError: if file does not exists
            LookupError: if file does not contain necessary attributes
        """
        # Check if path exists
        if not os.path.isfile(path):
            raise FileNotFoundError(
                'Path to config file does not exists ({})'.format(
                    path
            ))

        # Load json to dict
        with open(path) as config_file:
            config = json.load(config_file)

        # Iterate over list of class attributes (strings)
        for attribute in self.attributes:

            # Get value for the attribute from config (dict)
            value = config.get(attribute)

            # Raise if not found right key in the config
            if not value:
                raise LookupError(
                    'Attribute not found in the config ({})'.format(
                        attribute
                ))

            # Set attribute in the instance
            setattr(self, attribute, value)


class LocalSpreadsheet:
    """
    Local excel spreadsheet
    Attributes:
        excel (object): pandas excel object
        sheets_names (list): list of sheets' names that the excel contains
        active_sheet (int): index of active sheet
        dataframe (object): pandas dataframe object of active sheet
    """
    def __init__(self, path):
        self.excel = self.load(path)
        self.sheets_names = self.excel.sheet_names
        self._active_sheet = 0
        self._dataframes = self.load_dataframes()

    def __str__(self):
        return self._dataframes[self.active_sheet].__str__()

    @property
    def dataframe(self):
        return self._dataframes[self.active_sheet]

    @property
    def active_sheet(self):
        return self._active_sheet

    @active_sheet.setter
    def active_sheet(self, active_sheet):
        if active_sheet < 0 or active_sheet > len(self.sheets_names) - 1:
            raise ValueError(
                'Active sheet index must not be less than 0 or greater than ' \
                'sheets number in the excel file'
            )
        else:
            self._active_sheet = active_sheet

    def load_dataframes(self):
        """
        Parse all sheets in excel file intopandas dataframe objects
        """
        dataframes = []

        for sheet in self.sheets_names:
            dataframe = self.excel.parse(sheet)

            # Convert NaN value to empty string
            dataframe = dataframe.replace(np.nan, '', regex=True)

            # Convert datetime objects to string
            for column in dataframe.columns:
                dataframe[column] = dataframe[column].apply(
                    lambda x: str(x) if isinstance(x, dt) else x
                )

            dataframes.append(dataframe)

        return dataframes

    def load(self, path):
        """
        Load excel file via pandas
        Params:
            path (str): path to the excel file
        Raises:
            FileNotFoundError: if file does not exists
        """
        # Check if path exists
        if not os.path.isfile(path):
            raise FileNotFoundError(
                'Path to excel file does not exists ({})'.format(
                    path
            ))

        return pd.ExcelFile(path)


class GoogleSpreadsheet:
    """
    Google spreadsheet
    Attributes:
        spreadsheet_id (str): id of Google spreadsheet
        auth (object): Google auth object needed to build a scope
        api (object): api object for talking with Google Sheets
        sheets (list): list of sheets' names nad ids that
                                the google spreadsheet contains
        active_sheet (int): index of active sheet
    """
    def __init__(self, spreadsheet_id, path):
        self.spreadsheet_id = spreadsheet_id
        self.auth = self.autheticate(path)
        self.api = build('sheets', 'v4', credentials=self.auth)
        self.sheets = self.get_sheets()
        self.active_sheet = 0

    def autheticate(self, path):
        """
        Return google service account credentials object
        from credentials json file
        Params:
            path (string): path to service account file
        Returns:
            object: google credentials from service account file object
        Raises:
            FileNotFoundError: if path to credentials file does not exists
        """
        # Check if path to the credentials file exists
        if not os.path.isfile(path):
            raise FileNotFoundError(
                'Path to credentials file does not exists ({})'.format(
                    path
            ))

        return Credentials.from_service_account_file(path)

    def get_sheets(self):
        result = self.api.spreadsheets().get(
            spreadsheetId=self.spreadsheet_id,
            fields='sheets'
        ).execute()

        return list(map(
            lambda x: {
                'name': x['properties']['title'],
                'id': x['properties']['sheetId']
            },
            result['sheets']
        ))

    def append_sheet(self, name):
        """
        Append new sheet into google spreadsheet
        Params:
            name (str): title of the new sheet
        Returns:
            dict: raw response from sheets api
        """
        # Add suffix to the new sheet name if a sheet with
        # such name already exists
        suffix = 1
        sheet_name = name
        while sheet_name in list(map(lambda x: x['name'], self.sheets)):
            sheet_name = '{}_{}'.format(name, suffix)
            suffix += 1

        # Body for sheets api request
        body = {
            'requests': {
                'addSheet': {
                    'properties': {
                        'title': sheet_name
                    }
                }
            }
        }

        result = self.api.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body=body
        ).execute()

        # Add new sheet to the list of sheets
        self.sheets.append({
            'id': result['replies'][0]['addSheet']['properties']['sheetId'],
            'name': sheet_name
        })

        return result

    def append_columns(self, sheet_id, number):
        """
        Append empty columns
        """
        # Body for sheets api request
        body = {
            'requests': {
                'appendDimension': {
                    'sheetId': sheet_id,
                    'dimension': 'COLUMNS',
                    'length': number
                }
            }
        }

        result = self.api.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body=body
        ).execute()

        return result

    def append_rows(self, sheet_id, number):
        """
        Append empty rows
        """
        # Body for sheets api request
        body = {
            'requests': {
                'appendDimension': {
                    'sheetId': sheet_id,
                    'dimension': 'ROWS',
                    'length': number
                }
            }
        }

        result = self.api.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body=body
        ).execute()

        return result

    def add_data(self, data):
        """
        Add data into spreadsheet
        Params:
            data (list): list of ranges and its new values
                            [{'range': 'Sheet!A1:A2', 'values': [[1], [2]]}]
        Returns:
            dict: raw response from sheets api
        """
        body = {
            'valueInputOption': 'RAW',
            'data': data
        }

        result = self.api.spreadsheets().values().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body=body
        ).execute()

        return result


def num_to_col(index):
    """
    Convert column's index to its letter
    Params:
        index (int): column's index
    Returns:
        string: column's letter
    Raises:
        Exception: if column is less than 0
    """
    if index < 0:
        raise Exception('Column number cannot be less than 0')

    index += 1
    letter = ''

    while index:

        # Modulo 26 because alphabet has 26 letters
        remainder = index % 26

        if remainder == 0:
            remainder = 26

        # Get uppercase letter from ascii
        col_letter = chr(65 + remainder - 1)

        # Append letter to final result
        letter = col_letter + letter

        # Decresase index
        index = int((index - 1) / 26)

    return letter


def copy_to_google(name, dataframe, google_spreadsheet):
    """
    Copy data from dataframe into google spreadsheet.
    Params:
        name (str): name of the sheet that will be copied to google
        dataframe (object): pandas dataframe object representing single sheet
                            in a local excel spreadsheet
        google_spreadsheet (object): GoogleSpreadsheet object
    """
    # Add new sheet into google spreadsheet
    result = google_spreadsheet.append_sheet(name)

    # Get new google sheet's name and id from the result
    google_sheet = result['replies'][0]['addSheet']['properties']['title']
    google_sheet_id = result['replies'][0]['addSheet']['properties']['sheetId']

    # Compose sheet range e.g. Sheet!A1:Z100
    values_range = '{0}!A1:{1}{2}'.format(
        google_sheet,
        num_to_col(dataframe.shape[1] - 1),
        dataframe.shape[0]
    )

    # Append new empty columns if we go to the end of alphabet
    # to avoid grid limit exceded error
    if dataframe.shape[1] > 26:
        google_spreadsheet.append_columns(
            google_sheet_id,
            dataframe.shape[1] - 26
        )

    # Append new empty rows to avoid grid limit exceded error
    if dataframe.shape[0] > 1000:
        google_spreadsheet.append_rows(
            google_sheet_id,
            dataframe.shape[0] - 1000
        )

    google_spreadsheet.add_data({
        'range': values_range,
        'majorDimension': 'ROWS',
        'values': dataframe.values.tolist()
    })


if __name__ == '__main__':

    # Change workdir to directory of this python file
    work_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    os.chdir(work_dir)

    try:
        # Try to get config directory from cmd params
        config_path = sys.argv[1]
    except IndexError:
        # If exception, use default config path
        config_path = CONFIG_PATH
    finally:
        config = Config(config_path)

    # For measuring executing time
    start = dt.now()

    LOGGER.info('Loading local spreadsheet...')
    local_spreadsheet = LocalSpreadsheet(config.source)

    LOGGER.info('Loading google spreadsheet...')
    google_spreadsheet = GoogleSpreadsheet(
        config.destination,
        config.credentials
    )

    LOGGER.info('Copying local spreadsheet into google...')

    threads = []

    # Iterate over sheets in local spreadsheet
    for index, sheet in enumerate(local_spreadsheet.sheets_names):

        # Get sheet index
        sheet_index = local_spreadsheet.sheets_names.index(sheet)

        # Set active sheet
        local_spreadsheet.active_sheet = sheet_index

        LOGGER.info('Starting thread-{}'.format(
            index)
        )

        # Use threads because it is not CPU intensive task, so GIL will
        # not be a problem, it just reaches sheets api endpoints.
        # Testing showed that difference between processes and threads
        # in this case was very small
        thread = Thread(
            target=copy_to_google,
            args=(
                sheet,
                copy.deepcopy(local_spreadsheet.dataframe),
                copy.deepcopy(google_spreadsheet),
            )
        )
        thread.start()
        threads.append(thread)

    # Wait until all threads finish work
    for thread in threads:
        thread.join()

    LOGGER.info('Copying finished successfully in {}'.format(
        dt.now() - start)
    )
