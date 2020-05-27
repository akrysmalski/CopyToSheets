# CopyToSheets
Copy Excel spreadsheet into Google Sheets

### Features

- Create same sheets in Google spreadsheet as in Excel  (with the same name or with added suffix if sheet with such name already exists in Google spreadsheet),
- Copy whole data from each sheet in Excel into sheet in Google spreadsheet

### Usage

 - cd /path/to/CopyToSheets
 - pip install -r requirements.txt
 - cd copy_to_sheets
 - create config (see "Config" chapter)
 - python copy_to_sheets.py (optional argument: path to config, default: ./config.json)

### Config
A json file with following entries:
 - source: path to Excel file that sould be copied to Google
 - destination: id of Google spreadsheet where data will be copied to
 - credentials: path to Google service account credentials (json)

### Unit Tests

 - Ensure you have a testing spreadsheet in Google Sheets
 - Put an excel file, config and google service accout credentials in "tests" folder
 - cd /path/to/CopyToSheets/copy_to_sheets
 - python -m unittest

### Requirements

 - Python 3.x (64bit)
