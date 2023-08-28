-- Check if the database 'mosip_archive' exists
SELECT 1 FROM pg_database WHERE datname = 'mosip_archive';

-- If the database doesn't exist, create it
CREATE DATABASE mosip_archive
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TABLESPACE = pg_default
    OWNER = sysadmin
    TEMPLATE = template0;

-- Connect to the 'mosip_archive' database
\c mosip_archive sysadmin;

-- Check if the 'archive' schema exists
SELECT 1 FROM  pg_catalog.pg_namespace WHERE nspname = 'archive';

-- If the schema doesn't exist, create it
CREATE SCHEMA archive;

-- Set ownership of the 'archive' schema
ALTER SCHEMA archive OWNER TO sysadmin;

-- Set the search path
ALTER DATABASE mosip_archive SET search_path TO archive,pg_catalog,public;

-- Additional SQL statements for schema setup

-- End of the script
