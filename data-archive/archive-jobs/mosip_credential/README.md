# Archival Jobs
The Jobs will archive the data from the **credential_transactions** table in mosip_credential db to the table in archive database.

## Pre-requisite steps:

1. If we don't have mosip_archive DB than go ahead and continue with the [db_scripts](../../../data-archive/db_scripts)
2. Once we have the mosip_archive DB ready we need to continue with the archive_jobs.
3. Install python3.9 virtual env.
4. Switch to virtual env.
5. Install required psycopg dependency. `sudo yum install python3-psycopg.


## Archive jobs based on status code (Recommended):
Run the following command to run the archive jobs based on status code
    
```
sh mosip_archive_credential_table_base-on-status-code.sh
```
Note:- To archive the table based on status_code make sure
the required status codes are added to the configuration in [mosip_archive_credential.ini](mosip_archive_credential.ini).

 Set the status code to archive the data as below:
 
 ```credential.endstatus.list=STORED,printing```

Above property will archive the mosip_credential table data based on the status code.

```credential.status.time.map=ISSUED:2,FAILED:7```

Above property will archive the mosip_credential table data based on the status code and number of days after the records were created (cr_dtimes column).

## Archive jobs based on no of days after the records were created:
Run the following command to run the archive jobs based on no of days after the records were created (cr_dtimes coulmn)

```
sh mosip_archive_credential_table_base-on-no-of-days.sh
```
Note:- To archive the table based on archive_older_than_days make sure
the required archive_older_than_days property is added to the configuration in [mosip_archive_credential.ini](mosip_archive_credential.ini).

For Example:- If today Date is 01-Sept-2022 then if ```archive_older_than_days = 2```. 

Then it will archive records 2 days before that is before 31-Aug-2022.




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

