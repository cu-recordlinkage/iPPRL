DROP TABLE IF EXISTS aim4.chd_overall_cast;

CREATE TABLE aim4.chd_overall_cast
(
    indexcaseid character varying(50) COLLATE pg_catalog."default",
    recnum character varying(50) COLLATE pg_catalog."default",
    seqnum character varying(50) COLLATE pg_catalog."default",
    encnum character varying(50) COLLATE pg_catalog."default",
    reportingsite character varying(50) COLLATE pg_catalog."default",
    ageatencounter integer,
    fqe2distance integer,
    datasource character varying(50) COLLATE pg_catalog."default",
    datasourcetype character varying(50) COLLATE pg_catalog."default",
    startdateday integer,
    startdatemonth integer,
    startdatequarter integer,
    startdateyear integer,
    startdate date,
    enddateday integer,
    enddatemonth integer,
    enddatequarter integer,
    enddateyear integer,
    enddate date,
    encountertype character varying(50) COLLATE pg_catalog."default",
    encountertypeother character varying(50) COLLATE pg_catalog."default",
    lengthofstay integer,
    insselfpay character varying(50) COLLATE pg_catalog."default",
    insprivate character varying(50) COLLATE pg_catalog."default",
    insmedicaid character varying(50) COLLATE pg_catalog."default",
    insmedicare character varying(50) COLLATE pg_catalog."default",
    insothergovt character varying(50) COLLATE pg_catalog."default",
    insother character varying(50) COLLATE pg_catalog."default",
    insotherdesc character varying(50) COLLATE pg_catalog."default",
    insunavailable character varying(50) COLLATE pg_catalog."default",
    insunknown character varying(50) COLLATE pg_catalog."default",
    providertype character varying(50) COLLATE pg_catalog."default",
    providertypeother character varying(50) COLLATE pg_catalog."default",
    heightinches integer,
    weightpounds integer,
    uniqueid character varying(50) COLLATE pg_catalog."default",
    study_id bigint,
    mrn_chd_overall character varying(50) COLLATE pg_catalog."default",
    siteid character varying(50) COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE aim4.chd_overall_cast
    OWNER to postgres;
