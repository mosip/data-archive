DECLARE
    db_exists boolean;
BEGIN
    SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'mosip_archive') INTO db_exists;

    IF db_exists THEN
        RAISE NOTICE 'Database already exists. Skipping creation.';
    END IF;
END $$;

-- If the database doesn't exist, create it
CREATE DATABASE mosip_archive
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TABLESPACE = pg_default
    OWNER = sysadmin
    TEMPLATE  = template0;

-- Switch to the created database
\connect mosip_archive sysadmin;

-- Check if the archive schema exists
DO $$ 
DECLARE
    schema_exists boolean;
BEGIN
    SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'archive') INTO schema_exists;

    IF schema_exists THEN
        RAISE NOTICE 'Schema already exists. Skipping creation.';
    END IF;
END $$;

-- If the schema doesn't exist, create it
CREATE SCHEMA IF NOT EXISTS archive;

-- Alter schema owner
ALTER SCHEMA archive OWNER TO sysadmin;

-- Alter database search path
ALTER DATABASE mosip_archive SET search_path TO archive,pg_catalog,public;
