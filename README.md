# CloudMigrator-ReportGenerator
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/db27982d6e9e4c1b9b2e565fb3ac2ba2)](https://www.codacy.com/manual/cloudkevin/CloudMigrator-ReportGenerator?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=cloudkevin/CloudMigrator-ReportGenerator&amp;utm_campaign=Badge_Grade)

## Script Requirements
```
* Python3
* glob
* progress
* pathlib
* pandas
```

## Required Inputs and Format
```
* Log file directory path - This can contain either ZIP or extracted migration report folders
* Client prefix - This is used when generating the final report
```

## Notes/Warnings
```
After generating reports the temporary CSVs will be deleted along with the referenced ZIP files 
Migration report folders remain in place. To disable this functionality comment out line 24
```
