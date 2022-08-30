# Archival Jobs
The Jobs will archive the data from the source database to the destination database
in mosip_credential db.

## Pre-requisite steps:

1. If we don't have mosip_archive DB than go ahead and continue with the [db_scripts](../../../data-archive/db_scripts)
2. Once we have the mosip_archive DB ready we need to continue with the archive_jobs.
3. Install python3.9 virtual env.
4. Switch to virtual env.
5. Install required psycopg dependency. `sudo yum install python3-psycopg


## Archive jobs based on status code(Recommended way to archive).
Run the following command to run the archive jobs based on status code
    
```
sh mosip_archive_credential_table_base-on-status-code.sh
```
Note:- To Archive based on status_code make sure
the required status is added to the [mosip_archive_credential.ini](mosip_archive_credential.ini).

 Set the status code to archive the data in [mosip_archive_credential.ini](mosip_archive_credential.ini) file.
 ```credential.endstatus.list=STORED,printing```
Above property will archive the mosip_credential table data based on the status code.

```credential.status.time.map=ISSUED:2,FAILED:7```
Above property will archive the mosip_credential table data based on the status code and number of days.

## Archive jobs based on no of days
Run the following command to run the archive jobs based on no of days

```
sh mosip_archive_credential_table_base-on-no-of-days.sh
```
set the no of days to archive the data in [mosip_archive_credential.ini](mosip_archive_credential.ini) file.

```archive_older_than_days = 7```
This will archive the data from the source database to the destination database in mosip_archive db.

## Properties file variable details
**source_db_serverip:** Contains the source database server ip. 

**source_db_port:** Contains the source database port.

**source_db_name:** Contains the source database name.

**source_schema_name:** Contains the source schema name.

**source_db_uname:** Contains the source database username.

**source_db_pass:** Contains the source database password.

**archive_table1:** Contains the archive table name.

**archive_db_serverip:** Contains the archive database server ip.

**archive_db_port:** Contains the archive database port.

**archive_db_name:** Contains the archive database name.

**archive_schema_name:** Contains the archive schema name.

**archive_db_uname:** Contains the archive database username.

**archive_db_pass:** Contains the archive database password.

**archive_older_than_days:** Contains the number of days to archive the data.

**credential.status.time.map:** Contains the status code and number of days to archive the data.

**credential.endstatus.list:** Contains the status code to archive the data.

