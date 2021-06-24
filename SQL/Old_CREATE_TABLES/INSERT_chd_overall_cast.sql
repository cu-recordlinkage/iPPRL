CREATE OR REPLACE FUNCTION last_day(date)
RETURNS DATE AS
$$
	SELECT (DATE_TRUNC('MONTH',$1) + INTERVAl '1 month - 1 day')::DATE
$$ LANGUAGE 'sql' IMMUTABLE STRICT;

-- Integer values for dates
-- new columns: startdate, enddate
-- when endate fields are null, fills in with startdate fields to eliminate null enddates

INSERT INTO aim4.chd_overall_cast
  SELECT
    indexcaseid ,
    recnum ,
    seqnum ,
    encnum ,
    reportingsite ,
    cast(ageatencounter as integer) ,
    cast(fqe2distance as integer),
    datasource ,
    datasourcetype ,
    cast(startdateday as integer),
    cast(startdatemonth as integer),
    cast(startdatequarter as integer),
    cast(startdateyear as integer),
    make_date(startdateyear::int, startdatemonth::int, 1),
    -- If no values for enddates, ust values in startdate
    coalesce(cast(enddateday as integer),cast(startdateday as integer)),
    coalesce(cast(enddatemonth as integer),cast(startdatemonth as integer)),
    coalesce(cast(enddatequarter as integer), cast(startdatequarter as integer)),
    coalesce(cast(enddateyear as integer), cast(startdateyear as integer)),
    coalesce(last_day(make_date(enddateyear::int, enddatemonth::int,1)), last_day(make_date(startdateyear::int,startdatemonth::int,1))),
    encountertype ,
    encountertypeother , 
    cast(lengthofstay as integer),
    insselfpay ,
    insprivate ,
    insmedicaid ,
    insmedicare ,
    insothergovt ,
    insother ,
    insotherdesc , 
    insunavailable ,
    insunknown , 
    providertype ,
    providertypeother ,
    cast(heightinches as integer),
    cast(weightpounds as integer),
    uniqueid ,
    study_id,
    mrn_chd_overall ,
    siteid
  FROM tz.chd_overall

;