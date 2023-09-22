
### -- ---------------------------------------------------------------------------------------------------------
### -- Script Name		: ID Repository Archive Job
### -- Deploy Module 	: Pre registration
### -- Purpose    		: To Archive ID Repository tables which are marked for archive.       
### -- Create By   		: Sadanandegowda DM
### -- Created Date		: Dec-2020
### -- 
### -- Modified Date        Modified By         Comments / Remarks
### -- ----------------------------------------------------------------------------------------
#export SOURCE_IDREPO_DB_SERVERIP=172.16.0.162
#export SOURCE_IDREPO_DB_PORT=30093
#export SOURCE_IDREPO_DB_UNAME=dbuser
#export SOURCE_IDREPO_DB_PASS=mosip123
#export ARCHIVE_DB_SERVERIP=172.16.0.162
#export ARCHIVE_DB_PORT=30093
#export ARCHIVE_DB_UNAME=archiveuser
#export ARCHIVE_DB_PASS=mosip123

python3 mosip_archive_idrepo_table.py

#===============================================================================================
