# Archival Jobs
The Jobs will archive the data from the source database to the destination database
in mosip_credential db.

## Prerequisites
## Steps to GO:
1. If we don't have mosip_archive DB than go ahead and continue with the [db_scripts](../../../data-archive/db_scripts)
2. Once we have the mosip_archive DB ready we need to continue with the archive_jobs.
3. Install python3.9 virtual env.
4. Switch to virtual env.
5. Install required psycopg dependency. `sudo yum install python3-psycopg

## Archive jobs based on no of days
Run the following command to run the archive jobs based on no of days
    
    ```
    sh mosip_archive_credential_table_base-on-no-of-days.sh
    ```
 set the no of days to archive the data in mosip_archive_credential.ini file.
 ```archive_older_than_days = 7```
This will archive the data from the source database to the destination database in mosip_archive db.

## Archive jobs based on status code.
Run the following command to run the archive jobs based on status code
    
    ```
    sh mosip_archive_credential_table_base-on-status-code.sh
    ```
 set the status code to archive the data in mosip_archive_credential.ini file.
 ```credential.endstatus.list=STORED,printing```
Above property will archive the mosip_credential table data based on the status code.

```credential.status.time.map=ISSUED:2,FAILED:7```
Above property will archive the mosip_credential table data based on the status code and number of days.