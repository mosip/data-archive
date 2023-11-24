CREATE DATABASE mosip_archive
	ENCODING = 'UTF8'
	LC_COLLATE = 'en_US.UTF-8'
	LC_CTYPE = 'en_US.UTF-8'
	TABLESPACE = pg_default
	OWNER = postgres
	TEMPLATE  = template0;
COMMENT ON DATABASE mosip_archive IS 'Pre-registration database to store the data that is captured as part of pre-registration process';

\c mosip_archive

CREATE SCHEMA archive;
ALTER SCHEMA archive OWNER TO postgres;
ALTER DATABASE mosip_archive SET search_path TO archive,pg_catalog,public;
