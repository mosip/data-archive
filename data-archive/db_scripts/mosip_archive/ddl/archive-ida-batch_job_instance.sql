-- Table:mosip_ida_batch_job_instance 

-- DROP TABLE archive.mosip_ida_batch_job_instance;

CREATE TABLE archive.mosip_ida_batch_job_instance  (
  JOB_INSTANCE_ID BIGINT  PRIMARY KEY ,
  VERSION BIGINT,
  JOB_NAME VARCHAR(100) NOT NULL ,
  JOB_KEY VARCHAR(2500)
)
WITH (
    OIDS = FALSE
);
