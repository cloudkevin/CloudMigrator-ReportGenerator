#!/usr/bin/env python3
import os, sys, zipfile, csv, glob, click, pickle, platform
import pandas as pd
from pathlib import Path
from progress.bar import Bar
from datetime import datetime
import logging as l
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from apiclient.http import MediaFileUpload
from subprocess import call
from bs4 import BeautifulSoup

extension = '.zip'
header = ['UserId','State','Status','TotalImportSuccess','TotalImportFailure','TotalExportSuccess','TotalExportFailure','EmailExportSuccess','EmailExportFailure','AppointmentExportSuccess','AppointmentExportFailure','TaskExportSuccess','TaskExportFailure','ContactExportSuccess','ContactExportFailure','GroupExportSuccess','GroupExportFailure','GroupMemberExportSuccess','GroupMemberExportFailure','DocumentExportSuccess','DocumentExportFailure','OtherExportSuccess','OtherExportFailure','FolderExportFailure','EmailImportSuccess','EmailImportFailure','AppointmentImportSuccess','AppointmentImportFailure','TaskImportSuccess','TaskImportFailure','ContactImportSuccess','ContactImportFailure','GroupImportSuccess','GroupImportFailure','GroupMemberImportSuccess','GroupMemberImportFailure','DocumentImportSuccess','DocumentImportFailure','OtherImportSuccess','OtherImportFailure','StartTime','EndTime','Duration','SizeImported','ServerId','BLANK','CSV File Path','Email From Date','Email To Date']
summaryHeader = ['UserId','TotalErrors','ErrorPercentage','TotalImportFailure','TotalExportFailure']
logFiles = []
importFailures = []
exportFailures = []
documentMaps = []

def unzipArchive(path,extension,cleanup): # unzip all migration reports in directory
    l.info('Starting unzipArchive')
    for item in os.listdir(path): # loop through items in dir
        l.debug(f"Checking file: {item}")
        if item.endswith(extension): # check for ".zip" extension
            file_name = os.path.abspath(item) # get full path of files
            stripped_name = os.path.splitext(item)[0] # strip file extension for new folder name
            zip_ref = zipfile.ZipFile(file_name) # create zipfile object
            new_path = os.path.join(path, stripped_name) # create path that points to the folder that we will create
            l.info(f"Unzipping file: {item}")
            try:
                os.mkdir(stripped_name) # create folder removing .zip from name
                zip_ref.extractall(new_path) # extract file to dir
                if cleanup == 'yes':
                    os.remove(file_name) # delete zipped file
            except OSError as error:
                l.info(error)
            zip_ref.close() # close file
    for filename in Path(path).glob('**/UserStatistics.csv'):
        l.debug(f"Adding file to list of UserStatistics: {filename}")
        logFiles.append(str(filename)) # create list of all extracted UserStatistics.csv files
    for imfail in Path(path).glob('**/ItemResultImport-*.csv'):
        l.debug(f"Adding file to list of ItemResultImport: {imfail}")
        importFailures.append(str(imfail))
    for exfail in Path(path).glob('**/ItemResultExport-*.csv'):
        l.debug(f"Adding file to list of ItemResultExport: {exfail}")
        exportFailures.append(str(exfail))
    for docMap in Path(path).glob('**/DocumentMappings.csv'):
        l.debug(f"Adding file to list of DocumentMappings: {docMap}")
        documentMaps.append(str(docMap))
    l.info('Finished unzipArchive')
    totalBar.next()

def rawReport():
    l.info('Starting rawReport')
    lineCount = 0
    for file in logFiles:
        l.debug(f"Checking File: {file}")
        if file.endswith('.csv'):
            l.info(f"Adding file to RawReport: {file}")
            with open(file,'r') as f1:
                currentPath = os.path.dirname(file)
                l.debug('Starting search for MigrationReports*.html')
                for r in Path(currentPath).glob('**/MigrationReport*.html'):
                    with open(r,'r') as report:
                        l.debug(f'Opening MigrationReport: {r}')
                        soup = BeautifulSoup(report, 'html.parser')
                        emailFrom = soup.find('td', text='Migrate Email From').find_next_sibling("td").text.split(' ', 2)[0]
                        emailTo = soup.find('td', text='Migrate Email To').find_next_sibling("td").text.split(' ', 2)[0]
                csv_reader = csv.reader(f1, delimiter=',')
                with open('RawReport.csv','a', newline='') as f2:
                    csv_writer = csv.writer(f2,delimiter=',')
                    if lineCount == 0:
                        csv_writer.writerow(header)
                        lineCount += 1
                    next(csv_reader,None)
                    for row in csv_reader:
                        userState = str(row[1].lower())
                        l.debug(f"USER STATE: {userState}")
                        if userState != 'none' and userState != 'failed' and row[2] != 'Processing...':
                            data = row + [file[:-4]]
                            data.append(emailFrom)
                            data.append(emailTo)
                            csv_writer.writerow(data)
                        elif userState == 'none' or 'failed':
                            l.debug(f"Skipping Row: {row}")
    l.info('Finished rawReport')
    totalBar.next()

def importFailureReport():
    l.info('Starting importFailureReport')
    global imErrorCount
    lineCount = 0
    for file in importFailures:
        l.debug(f"Checking item: {file}")
        if file.endswith('.csv'):
            l.info(f"Adding to ImportFailureReport: {file}")
            # date_time = os.path.basename(os.path.dirname(file)) # get datetime for comparison
            # newDate = datetime.strptime(date_time, '%d-%m-%Y-%H-%M-%S').date()
            with open(file,'r') as f1:
                csv_reader = csv.reader(f1, delimiter=',')
                with open('ImportFailureReport.csv','a', newline='') as f2:
                    csv_writer = csv.writer(f2,delimiter=',')
                    if lineCount == 0:
                        csv_writer.writerow(['DetailedType','ResponseCode','GUID','Reason','UserId','Failure','BLANK','CSV File Path'])
                        lineCount += 1
                    next(csv_reader,None)
                    for row in csv_reader:
                        if row[0] != 'Other':
                            data = row + [file[:-4]]
                            csv_writer.writerow(data)
                        elif row[0] == 'Other':
                            l.debug(f"OTHER error found, Skipping row: {row}")
                            next(csv_reader,None)
    l.info('Cleaning ImportFailureReport')
    df = pd.read_csv('ImportFailureReport.csv')
    df2 = df.sort_values(by=['UserId', 'Failure'])
    df2.drop_duplicates(subset="Failure", keep = 'first', inplace = True)
    imErrorCount = df2.groupby(['UserId']).count()
    imErrorCount = imErrorCount.drop(['ResponseCode','GUID','Reason','Failure','BLANK','CSV File Path'], axis=1)
    imErrorCount = imErrorCount.rename(columns={"UserId": "UserId", "DetailedType": "ImportErrorCount"})
    l.info('Finished importFailureReport')
    totalBar.next()

def exportFailureReport():
    l.info('Starting exportFailureReport')
    global exErrorCount
    lineCount = 0
    for file in exportFailures:
        l.debug(f"Checking item: {file}")
        if file.endswith('.csv'):
            l.info(f"Adding to ExportFailureReport: {file}")
            with open(file,'r') as f1:
                csv_reader = csv.reader(f1, delimiter=',')
                with open('ExportFailureReport.csv','a', newline='') as f2:
                    csv_writer = csv.writer(f2,delimiter=',')
                    if lineCount == 0:
                        csv_writer.writerow(['DetailedType','ResponseCode','GUID','Reason','UserId','Failure','BLANK','CSV File Path'])
                        lineCount += 1
                    next(csv_reader,None)
                    for row in csv_reader:
                        data = row + [file[:-4]]
                        csv_writer.writerow(data)
    l.info('Cleaning ExportFailureReport')
    df = pd.read_csv('ExportFailureReport.csv')
    df = (df[df['Reason'] != 'The remote server returned an error: (500) Internal Server Error.']).sort_values(by=['UserId', 'Failure']) # exclude specific errors from dataframe
    exErrorCount = df.groupby(['UserId']).count() # count number of errors by UserId
    exErrorCount = exErrorCount.drop(['ResponseCode','GUID','Reason','Failure','BLANK','CSV File Path'], axis=1) # drop columns unneeded columns
    exErrorCount = exErrorCount.rename(columns={"UserId": "UserId", "DetailedType": "ExportErrorCount"}) # rename columns to allow proper join
    l.info('Finished exportFailureReport')
    totalBar.next()

def combineDuplicates(overlap): # combine duplicates using UserId / also calculate total duration in minutes
    l.info('Starting combineDuplicates')
    df = pd.read_csv('RawReport.csv')
    for i in range(len(df)):
        durationSplit = df.loc[i, "Duration"].split(':')
        seconds = durationSplit[2]
        days = 0
        hours = durationSplit[0]
        minutes = int(durationSplit[1])
        if '.' in seconds: # split days and hours then combine into one number
            secSplit = durationSplit[2].split('.')
            seconds = int(secSplit[0])
        if '.' in hours:
            hourSplit = hours.split('.')
            days = hourSplit[0]
            hours = hourSplit[1]
        days = int(days) * 86400
        hours = int(hours) * 3600
        minutes = minutes * 60
        daysHours =  days + hours
        totalTime = (daysHours+seconds+minutes)
        totalTime = round(totalTime/60, 1)
        df.loc[i, "TotalDuration"] = totalTime
    if overlap == 'yes':
        l.info('Removing overlapping date ranges')
        df2 = df.sort_values(by=['UserId','Email From Date','Email To Date','TotalImportSuccess'],ascending=[True,True, False, False]).drop_duplicates(subset=['UserId','Email From Date','Email To Date'], keep='first') # take largest migration run if date ranges overlap
        df2 = df2.groupby(['UserId']).sum()
    elif overlap == 'no':
        df2 = df.groupby(['UserId']).sum()  # sort by UserId and take sum of numberical columns
    df2.to_csv('CombinedReport.csv',header=True)
    l.info('Finished combineDuplicates')
    totalBar.next()

def generateSummary():
    l.info('Starting generateSummary')
    summary = imErrorCount.join(exErrorCount, on='UserId') # combine import and export counts on UserId
    summary = summary.fillna(0) # if value is NaN replace with 0
    summary['TotalErrorCount'] = summary['ImportErrorCount'] + summary['ExportErrorCount'] # create and populate TotalErrorCount column
    cr = pd.read_csv('CombinedReport.csv',index_col=['UserId']) # open CombinedReport and sort by UserId
    cr = cr.join(summary, on='UserId') # add newly calculated values to summary report
    cr['Error Percentage'] = cr['ImportErrorCount']/cr['TotalImportSuccess']
    cr = cr.filter(['UserId','ImportErrorCount','ExportErrorCount','TotalErrorCount','TotalImportSuccess','Error Percentage','SizeImported','TotalDuration']) # remove columns that we don't need
    cr.to_csv('MigrationSummary.csv',header=True) # convert to CSV
    l.info('Finished generateSummary')
    totalBar.next()

def mergeToExcel(path,client_name):
    global finalReport
    l.info('Starting mergeToExcel')
    generatedReports = []
    writer = pd.ExcelWriter(client_name+'_MigrationReport.xlsx', engine='xlsxwriter',options={'strings_to_urls': False}) # write URLs as strings to avoid 65,530 URL limit
    for item in os.listdir(path):
        l.debug(f"Checking file: {item}")
        if item.endswith('.csv'):
            l.info(f"Adding to FinalDocumentMap: {item}")
            if 'FinalDocumentMap.csv' not in item:
                generatedReports.append(item)
    for r in generatedReports:
        df = pd.read_csv(r)
        stripped_name = os.path.splitext(r)[0]
        df.to_excel(writer, sheet_name=stripped_name,index=False)
        l.debug(f"Writing file to excel report: {r}")
    writer.save()
    l.info('Finished mergeToExcel')
    totalBar.next()
    totalBar.finish()
    finalReport = os.path.abspath(writer)
    print("\n\n#############################")
    print("Report Generated Successfully")
    print("#############################")
    print(f"\nFinal Report Location: {finalReport}\n")

def cleanArtifacts():
    l.info('Starting cleanArtifacts')
    os.remove('MigrationSummary.csv')
    l.debug('MigrationSummary deleted')
    os.remove('ImportFailureReport.csv')
    l.debug('ImportFailureReport deleted')
    os.remove('ExportFailureReport.csv')
    l.debug('ExportFailureReport deleted')
    os.remove('CombinedReport.csv')
    l.debug('CombinedReport deleted')
    l.info('Finished cleanArtifacts')

def clean_document_maps():
    l.info('Starting clean_document_maps')
    lineCount = 0
    documentMaps.sort()
    for dm in documentMaps:
        l.debug(f"Checking file {dm}")
        if dm.endswith('.csv'):
            l.info(f"Adding file to document map: {dm}")
            date_time = os.path.basename(os.path.dirname(dm)) # get datetime for comparison
            newDate = datetime.strptime(date_time, '%d-%m-%Y-%H-%M-%S').date()
            with open(dm,'r') as m:
                csv_reader = csv.reader(m,delimiter=',')
                with open('FinalDocumentMap.csv','a',newline='') as fd:
                    csv_writer = csv.writer(fd,delimiter=',')
                    l.debug("Writing FinalDocumentMap.csv")
                    if lineCount == 0:
                        csv_writer.writerow(['OriginalLocation','DestinationLocation','DestinationOwner','MimeType','Timestamp'])
                        lineCount += 1
                    next(csv_reader,None)
                    for row in csv_reader:
                        data = row + [newDate]
                        csv_writer.writerow(data)
                        lineCount +=1
    l.info('Cleaning FinalDocumentMap')
    pandaMap = pd.read_csv('FinalDocumentMap.csv')
    pandaMap = pandaMap.sort_values(by=['DestinationOwner','OriginalLocation','Timestamp'],ascending=[True, True, False])
    pandaMap2 = pandaMap.drop_duplicates(subset='OriginalLocation', keep= 'first')
    pandaMap2.to_csv('FinalDocumentMap.csv',index=False)
    l.info('Finished cleaning FinalDocumentMap')

def loadingSplash(prefix,cleanup,path,docmap,overlap):
    print('\n')
    print('####################################################################################################')
    print(r'   _____ __  __   _____                       _      _____                           _             ')
    print(r'  / ____|  \/  | |  __ \                     | |    / ____|                         | |            ')
    print(r' | |    | \  / | | |__) |___ _ __   ___  _ __| |_  | |  __  ___ _ __   ___ _ __ __ _| |_ ___  _ __ ')
    print(r" | |    | |\/| | |  _  // _ \ '_ \ / _ \| '__| __| | | |_ |/ _ \ '_ \ / _ \ '__/ _` | __/ _ \| '__|")
    print(r' | |____| |  | | | | \ \  __/ |_) | (_) | |  | |_  | |__| |  __/ | | |  __/ | | (_| | || (_) | |   ')
    print(r'  \_____|_|  |_| |_|  \_\___| .__/ \___/|_|   \__|  \_____|\___|_| |_|\___|_|  \__,_|\__\___/|_|   ')
    print(r'                            | |                                                                    ')
    print(r'                            |_|                                                                    ')
    print('####################################################################################################')
    print('\n')
    print('\n')
    if prefix != '':
        print(f"Domain Pefix: {prefix}")
    print(f"Remove ZIP and TMP files: {cleanup.upper()}")
    if docmap != '':
        print(f"Map Cleanup: {docmap.upper()}")
    if overlap == 'yes':
        print(f"Removing overlap date ranges: {overlap.upper()}")
    if path != '':
        print(f"Dir Path: {path}")

def set_logging_level(loglevel,prefix):
    logFileName = prefix + '_cmrg.log'
    homePath = os.path.expanduser('~')
    logPath = homePath + '/' + logFileName
    loglevel = loglevel.upper()
    if loglevel == 'INFO':
        ll = l.INFO
    elif loglevel == 'DEBUG':
        ll = l.DEBUG
    elif loglevel == 'WARNING':
        ll = l.WARNING
    elif loglevel == 'ERROR':
        ll = l.ERROR
    elif loglevel == 'CRITICAL':
        ll = l.CRITICAL
    l.basicConfig(filename=logPath, level=ll,filemode='w', format='%(asctime)s:%(levelname)s:%(message)s')
    l.getLogger('googleapicliet.discovery_cache').setLevel(l.ERROR)
    l.debug(f"Current Logging Level: {loglevel}")
    l.info("Finished set_logging_level")

def upload_to_drive(report, path):
    l.info("Starting upload_to_drive")
    SCOPES = ['https://www.googleapis.com/auth/drive.file']
    creds = None
    homePath = os.path.expanduser('~')
    credsPath = homePath + '/credentials.json'
    picklePath = homePath + '/token.pickle'
    if os.path.exists(picklePath):
        l.debug("token.pickle already exists")
        with open(picklePath, 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        l.debug("Prompting user to login")
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            try:
                l.debug(f"Trying credential flow")
                flow = InstalledAppFlow.from_client_secrets_file(
                    credsPath, SCOPES)
                l.debug(f"Flow: {flow}")
                creds = flow.run_local_server(port=0)
                l.debug("Creds: {creds}")
            except:
                print(f"Error: credentials.json file does not exist. Unable to upload report.\n")
                l.debug("Unable location credentials.json file. Aborting function")
                return
        # Save the credentials for the next run
        with open(picklePath, 'wb') as token:
            pickle.dump(creds, token)
    drive_service = build('drive', 'v3', credentials=creds)
    report = report.split('.')[0]
    file_metadata = {
    'name' : report,
    'mimeType' : 'application/vnd.google-apps.spreadsheet'
    }
    media = MediaFileUpload(path,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        resumable=True)
    file = drive_service.files().create(body=file_metadata,
                                        media_body=media,
                                        fields='id').execute()
    fileId = file.get('id')
    fileUrl = 'https://drive.google.com/open?id=' + fileId
    protect_the_pickle(picklePath)
    print(f"Final Report uploaded to Drive")
    print(f"\nUploaded file to URL: {fileUrl}  \n")
    l.info("Finished upload_to_drive")

def protect_the_pickle(pickle):
    l.info("Starting protect_the_pickle")
    if operatingSystem == "darwin":
        call(["chflags", "hidden", pickle])
    elif operatingSystem == "windows":
        call(["attrib", "+H", pickle])
    l.info("Finished protect_the_pickle")

def startupCheck(prefix,path):
    global operatingSystem
    if path == 'none':
        l.debug(f"Prompting user for path")
        path = input("\nLog File Directory Path: ")
    l.info("Starting startupCheck")
    pythonVersion = platform.python_version()
    l.debug(f"Python Version: {pythonVersion}")
    if pythonVersion.startswith(str(3)) != True:
        print(f"Run this program using Python3. Quitting Report Generator.")
        l.debug("Wrong version of Python")
        l.info("Closing Program")
        sys.exit()
    operatingSystem = platform.system().lower()
    l.debug(f"Current OS: {operatingSystem}")
    os.chdir(path)
    reportName = prefix + '_MigrationReport.xlsx'
    if os.path.exists(reportName): # do not run if previous report already generated
        l.info(f"Report with name {reportName} already exists")
        l.info(f"Closing Program")
        print("\nERROR: Report has already been generated in this directory.\n")
        sys.exit()
    l.info("Finished startupCheck")


@click.command()
@click.option('--prefix', default='', help='Final report name prefix.')
@click.option('--cleanup', default='no', help='Enter True to remove ZIP files and generated CSVs.')
@click.option('--path', default='none', help='Enter the directory path of your log files')
@click.option('--docmap', default='', help='Cleanup document mapping reports')
@click.option('--logging', default='INFO', help='Set the logging level')
@click.option('--todrive', default='', help='Upload Final Report to Google Drive')
@click.option('--overlap', default='no', help='This will remove identical date ranges keeping max')
def main(prefix,cleanup,path,docmap,logging,todrive,overlap):
    set_logging_level(logging,prefix)
    l.info(f"Staring main")
    loadingSplash(prefix,cleanup,path,docmap,overlap)
    startupCheck(prefix,path)
    global totalBar
    print('\n')
    totalBar = Bar('Processing',max=7,fill='$')
    print('\n')
    unzipArchive(path,extension,cleanup)
    rawReport()
    importFailureReport()
    exportFailureReport()
    combineDuplicates(overlap)
    generateSummary()
    mergeToExcel(path,prefix)
    if  docmap == 'yes':
        clean_document_maps()
    if cleanup == 'yes':
        cleanArtifacts()
    if todrive == 'yes':
        upload_to_drive(reportName, finalReport)
    l.info(f"Program Finished Running")
