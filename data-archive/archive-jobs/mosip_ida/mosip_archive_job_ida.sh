### -- ---------------------------------------------------------------------------------------------------------
### -- Script Name		: IDA Archive Job
### -- Deploy Module 	: IDA
### -- Purpose    		: To Archive IDA tables which are marked for archive.       
### -- Create By   		: Sadanandegowda DM
### -- Created Date		: Dec-2020
### -- Modified Date        Modified By         Comments / Remarks
### -- ----------------------------------------------------------------------------------------
#export SOURCE_IDA_DB_SERVERIP=172.16.0.162
#export SOURCE_IDA_DB_PORT=30093
#export SOURCE_IDA_DB_UNAME=dbuser
#export SOURCE_IDA_DB_PASS=mosip123
#export ARCHIVE_DB_SERVERIP=172.16.0.162
#export ARCHIVE_DB_PORT=30093
#export ARCHIVE_DB_UNAME=archiveuser
#export ARCHIVE_DB_PASS=mosip123

python3 mosip_archive_ida_table.py


#===============================================================================================
