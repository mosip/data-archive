

-- object: archive.mosip_regprc_additional_info_request | type: TABLE --
-- DROP TABLE IF EXISTS archive.mosip_regprc_additional_info_request CASCADE;
CREATE TABLE archive.mosip_regprc_additional_info_request(
	additional_info_process character varying(64),
	reg_id character varying(39),
	workflow_instance_id character varying(36),
	timestamp timestamp,
	additional_info_iteration integer,
	additional_info_req_id character varying(256),
	CONSTRAINT pk_addl_info_req PRIMARY KEY (workflow_instance_id , additional_info_req_id)

);
-- ddl-end --
