-- -------------------------------------------------------------------------------------------------
-- Database Name:    mosip_resident
-- Release Version 	: 1.2.1
-- Purpose    		: Database scripts for Resident Service DB.
-- Create By   		: Loganathan Sekar
-- Created Date		: Jan-2023
--
-- Modified Date        Modified By         Comments / Remarks
-- --------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------

-- This Table is used to save the  user actions for the user actions table.

CREATE TABLE archive.mosip_resident_session(
	session_id character varying(128) NOT NULL,
    ida_token character varying(128) NOT NULL,
    login_dtimes timestamp,
	ip_address character varying(128),
	host character varying(128),
	machine_type character varying(30),
    CONSTRAINT pk_session_id PRIMARY KEY (session_id)
);

COMMENT ON TABLE archive.mosip_resident_session IS 'This Table is used to save the  user sessions.';
COMMENT ON COLUMN archive.mosip_resident_session.session_id IS 'The unique session identifier for each login';
COMMENT ON COLUMN archive.mosip_resident_session.ida_token IS 'The unique identifier for each user';
COMMENT ON COLUMN archive.mosip_resident_session.login_dtimes IS 'The time when the user last logged in';
COMMENT ON COLUMN archive.mosip_resident_session.ip_address IS 'The ip_address of device from which the user logged in';
COMMENT ON COLUMN archive.mosip_resident_session.host IS 'The host of the site';
COMMENT ON COLUMN archive.mosip_resident_session.machine_type IS 'The OS of device used for accessing the portal/app';

-- Adding index to ida_token column
CREATE INDEX idx_resident_session_ida_token ON archive.mosip_resident_session (ida_token);
