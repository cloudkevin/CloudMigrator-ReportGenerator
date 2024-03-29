# CloudMigrator-ReportGenerator
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/db27982d6e9e4c1b9b2e565fb3ac2ba2)](https://www.codacy.com/manual/cloudkevin/CloudMigrator-ReportGenerator?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=cloudkevin/CloudMigrator-ReportGenerator&amp;utm_campaign=Badge_Grade)

*This has not been fully tested on Windows. You should consider using Darwin or Linux instead.*
## Script Setup and Requirements

Python3

Navigate to the directory folder and run: python3 setup.py install

Usage: cmReportGenerator [OPTIONS]

You may also run: pip3 install -r requirements.txt
*This will only install the requirements*

Requirements: If uploading to Google Drive a credentials.json file must be present in your home directory

## Inputs and Format
--help
Display input options

--prefix = string value (default=blank)  
Typically client name but may be any desired naming prefix

--cleanup = 'yes' or blank (default=no)  
If set to yes the ZIP and temp CSV files will be deleted after running
The report folders and FinalReport will remain

--docmap = 'yes' or blank (default=no)  
This will compile all of the document mapping reports and leave only the most recent Drive URL

--path = string (If no value is entered you will be prompted later)  
The path to the directoy which contains your CloudMigrator reports

--logging = debug/info/warning/error/critical (default=INFO)  
This will log to a file called PREFIX_cmrg.log in your home directory

--overlap = 'yes' or blank (default=no)  
If your logs contain multiple runs with the same target date (EMAIL ONLY) it will take the highest value and remove the duplicates

--todrive = 'yes' or blank (default=no)  
Upload FinalReport to Drive and convert

## Demo
[![asciicast](https://asciinema.org/a/rTppzDBtRw1dVg33IFu4m0eyN.svg)](https://asciinema.org/a/rTppzDBtRw1dVg33IFu4m0eyN)
