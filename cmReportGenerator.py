#!/usr/bin/env python3
import os, zipfile, csv, glob, click
import pandas as pd
from pathlib import Path
from progress.bar import Bar
from datetime import datetime
import logging as l

extension = '.zip'
header = ['UserId','State','Status','TotalImportSuccess','TotalImportFailure','TotalExportSuccess','TotalExportFailure','EmailExportSuccess','EmailExportFailure','AppointmentExportSuccess','AppointmentExportFailure','TaskExportSuccess','TaskExportFailure','ContactExportSuccess','ContactExportFailure','GroupExportSuccess','GroupExportFailure','GroupMemberExportSuccess','GroupMemberExportFailure','DocumentExportSuccess','DocumentExportFailure','OtherExportSuccess','OtherExportFailure','FolderExportFailure','EmailImportSuccess','EmailImportFailure','AppointmentImportSuccess','AppointmentImportFailure','TaskImportSuccess','TaskImportFailure','ContactImportSuccess','ContactImportFailure','GroupImportSuccess','GroupImportFailure','GroupMemberImportSuccess','GroupMemberImportFailure','DocumentImportSuccess','DocumentImportFailure','OtherImportSuccess','OtherImportFailure','StartTime','EndTime','Duration','SizeImported','ServerId','BLANK','CSV File Path']
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
                print(error)
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
                csv_reader = csv.reader(f1, delimiter=',')
                with open('RawReport.csv','a', newline='') as f2:
                    csv_writer = csv.writer(f2,delimiter=',')
                    if lineCount == 0:
                        csv_writer.writerow(header)
                        lineCount += 1
                    next(csv_reader,None)
                    for row in csv_reader:
                        data = row + [file[:-4]]
                        csv_writer.writerow(data)
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
                        data = row + [file[:-4]]
                        csv_writer.writerow(data)
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


def combineDuplicates(): # combine duplicates using UserId, add all numerical values and drop the rest
    l.info('Starting combineDuplicates')
    df = pd.read_csv('RawReport.csv')
    df2 = df.groupby(['UserId']).max()
    df2.to_csv('CombinedReport.csv',header=True)
    l.info('Finished combineDuplicates')
    totalBar.next()


def generateSummary():
    l.info('Starting generateSummary')
    summary = imErrorCount.join(exErrorCount, on='UserId')
    summary = summary.fillna(0)
    summary['TotalErrorCount'] = summary['ImportErrorCount'] + summary['ExportErrorCount']
    cr = pd.read_csv('CombinedReport.csv',index_col=['UserId'])
    cr = cr.join(summary, on='UserId')
    cr['Error Percentage'] = cr['ImportErrorCount']/cr['TotalImportSuccess']
    cr = cr.filter(['UserId','ImportErrorCount','ExportErrorCount','TotalErrorCount','TotalImportSuccess','Error Percentage','SizeImported'])
    cr.to_csv('MigrationSummary.csv',header=True)
    l.info('Finished generateSummary')
    totalBar.next()


def mergeToExcel(path,client_name):
    l.info('Starting mergeToExcel')
    generatedReports = []
    writer = pd.ExcelWriter(client_name+'_MigrationReport.xlsx', engine='xlsxwriter')
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
    print("\n\n#############################")
    print("Report Generated Successfully")
    print("#############################")
    print("\nFinal Report Location: "+ os.path.abspath(writer)+'\n')

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


def loadingSplash(prefix,cleanup,path,docmap):
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
    if path != '':
        print(f"Dir Path: {path}")

def set_logging_level(loglevel,prefix):
    logFileName = prefix + '_cmrg.log'
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
    l.basicConfig(filename=logFileName, level=ll,filemode='w', format='%(asctime)s:%(levelname)s:%(message)s')
    l.debug(f"Current Logging Level: {loglevel}")

@click.command()
@click.option('--prefix', default='', help='Final report name prefix.')
@click.option('--cleanup', default='', help='Enter True to remove ZIP files and generated CSVs.')
@click.option('--path', default='none', help='Enter the directory path of your log files')
@click.option('--docmap', default='', help='Cleanup document mapping reports')
@click.option('--logging', default='INFO', help='Set the logging level')
def main(prefix,cleanup,path,docmap,logging):
    set_logging_level(logging,prefix)
    global totalBar
    loadingSplash(prefix,cleanup,path,docmap)
    if path == 'none':
        path = input("Log File Directory Path: ")
    print('\n')
    os.chdir(path) # change directory from working dir to dir with files
    previousReport = prefix + '_MigrationReport.xlsx'
    if os.path.exists(previousReport): # do not run if previous report already generated
        print("\nERROR: Report has already been generated in this directory.\n")
    else:
        totalBar = Bar('Processing',max=7,fill='$')
        print('\n')
        unzipArchive(path,extension,cleanup)
        rawReport()
        importFailureReport()
        exportFailureReport()
        combineDuplicates()
        generateSummary()
        mergeToExcel(path,prefix)
        if  docmap == 'yes':
            clean_document_maps()
        if cleanup == 'yes':
            cleanArtifacts()
