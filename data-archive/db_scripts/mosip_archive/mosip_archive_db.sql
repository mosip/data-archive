DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'mosip_archive') THEN
        CREATE DATABASE mosip_archive
            ENCODING = 'UTF8'
            LC_COLLATE = 'en_US.UTF-8'
            LC_CTYPE = 'en_US.UTF-8'
            TABLESPACE = pg_default
            OWNER = sysadmin
            TEMPLATE  = template0;

        COMMENT ON DATABASE mosip_archive IS 'Database to store all archive data, Data is archived from multiple tables from each module.';
    END IF;
END $$;

\c mosip_archive sysadmin

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'archive') THEN
        CREATE SCHEMA archive;
        ALTER SCHEMA archive OWNER TO sysadmin;
    END IF;
END $$;

ALTER DATABASE mosip_archive SET search_path TO archive,pg_catalog,public;
-- REVOKECONNECT ON DATABASE mosip_archive FROM PUBLIC;
-- REVOKEALL ON SCHEMA archive FROM PUBLIC;
-- REVOKEALL ON ALL TABLES IN SCHEMA archive FROM PUBLIC ;
