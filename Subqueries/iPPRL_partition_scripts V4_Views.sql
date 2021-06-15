
/*Create views:
 * 
 * CHD_OVERAL_CAST: Clinical data with strings converted to integers or dates.
 *                  Sets enddate to last day of endmonth-endyear if present else last day of startmonth-startyear
 * 
 * {DATE}_CHD_CLINVS: Clinical data in CHD_OVERALL_CAST from 1/1/2011 up to the interval end date
 *    eg: year_2011_chd_clinvs from 1/1/2011 - 12/31/2011
 *        quarter1_2021_chd_clinvs from 1/1/2011 - 3/31/2011
 * 
 *            
*/    

CREATE OR REPLACE FUNCTION last_day(date)
RETURNS DATE AS
$$
	SELECT (DATE_TRUNC('MONTH',$1) + INTERVAl '1 month - 1 day')::DATE
$$ LANGUAGE 'sql' IMMUTABLE STRICT;

-- Integer values for dates
-- new columns: startdate, enddate
-- when endate fields are null, fills in with startdate fields to eliminate null enddates

drop view if exists aim4.chd_overall_cast cascade;

CREATE OR REPLACE VIEW aim4.chd_overall_cast as
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
    1::int as startdateday ,
    cast(startdatemonth as integer),
    cast(startdatequarter as integer),
    cast(startdateyear as integer),
    make_date(startdateyear::int, startdatemonth::int, 1::int) as startdate,
    1::int as enddateday ,
    cast(enddatemonth as integer),
    cast(enddatequarter as integer),
    cast(enddateyear as integer),
    make_date(enddateyear::int, enddatemonth::int, 1::int) as enddate ,
  -- ENDDATE2: non-null endates based on enddates if not null or startdates if null
    coalesce(make_date(enddateyear::int, enddatemonth::int, 1::int), make_date(startdateyear::int, startdatemonth::int, 1::int)) as enddate2 ,
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
    co.study_id,
    mrn_chd_overall ,
    siteid ,
    rp.mrn as mrn
  FROM tz.chd_overall co join tz.raw_person rp on co.study_id = rp.study_id
 ;
  
-- RAW PERSON: Person-level clear text linkage vars
drop view if exists aim4.raw_person;

CREATE OR REPLACE VIEW aim4.raw_person as
  SELECT * from tz.raw_person rp
;

-- _chd_clinvs contains chd clinical data from 1-1-2011 to end of incremental load

drop view if exists aim4.year_2011_chd_clinvs;
drop view if exists aim4.quarter1_2012_chd_clinvs;
drop view if exists aim4.quarter2_2012_chd_clinvs;
drop view if exists aim4.quarter3_2012_chd_clinvs;
drop view if exists aim4.quarter4_2012_chd_clinvs;

drop view if exists aim4.month1_2013_chd_clinvs;
drop view if exists aim4.month2_2013_chd_clinvs;
drop view if exists aim4.month3_2013_chd_clinvs;
drop view if exists aim4.month4_2013_chd_clinvs;
drop view if exists aim4.month5_2013_chd_clinvs;
drop view if exists aim4.month6_2013_chd_clinvs;
drop view if exists aim4.month7_2013_chd_clinvs;
drop view if exists aim4.month8_2013_chd_clinvs;
drop view if exists aim4.month9_2013_chd_clinvs;
drop view if exists aim4.month10_2013_chd_clinvs;
drop view if exists aim4.month11_2013_chd_clinvs;
drop view if exists aim4.month12_2013_chd_clinvs;


CREATE OR REPLACE VIEW aim4.year_2011_chd_clinvs as 
 select *, least(enddate2, '12-31-2011'::date) as enddate_trunc from aim4.chd_overall_cast  where startdate between '01-01-2011' and '12-31-2011';
CREATE OR REPLACE VIEW aim4.quarter1_2012_chd_clinvs as 
 select *, least(enddate2, '03-31-2012'::date) as enddate_trunc from aim4.chd_overall_cast where startdate between '01-01-2011' and '3-31-2012';
CREATE OR REPLACE VIEW aim4.quarter2_2012_chd_clinvs as 
 select *, least(enddate2, '06-30-2012'::date) as enddate_trunc from aim4.chd_overall_cast where startdate between  '01-01-2011' and '06-30-2012';
CREATE OR REPLACE VIEW aim4.quarter3_2012_chd_clinvs as 
 select *, least(enddate2, '09-30-2012'::date) as enddate_trunc from aim4.chd_overall_cast where startdate between '01-01-2011' and '09-30-2012';
CREATE OR REPLACE VIEW aim4.quarter4_2012_chd_clinvs as 
 select *, least(enddate2, '12-31-2012'::date) as enddate_trunc from aim4.chd_overall_cast where startdate between '01-01-2011' and '12-31-2012';
CREATE OR REPLACE VIEW aim4.month1_2013_chd_clinvs as 
 select *, least(enddate2, '01-31-2013'::date) as enddate_trunc from aim4.chd_overall_cast where startdate between '01-01-2011' and  '01-31-2013';
CREATE OR REPLACE VIEW aim4.month2_2013_chd_clinvs as 
 select *, least(enddate2, '02-28-2013'::date) as enddate_trunc from aim4.chd_overall_cast where startdate between '01-01-2011' and '02-28-2013';
CREATE OR REPLACE VIEW aim4.month3_2013_chd_clinvs as 
 select *, least(enddate2, '03-31-2013'::date) as enddate_trunc from aim4.chd_overall_cast where startdate between '01-01-2011' and '03-31-2013';
CREATE OR REPLACE VIEW aim4.month4_2013_chd_clinvs as 
 select *, least(enddate2, '04-30-2013'::date) as enddate_trunc from aim4.chd_overall_cast where startdate between '01-01-2011' and '04-30-2013';
CREATE OR REPLACE VIEW aim4.month5_2013_chd_clinvs as 
 select *, least(enddate2, '05-31-2013'::date) as enddate_trunc from aim4.chd_overall_cast where startdate between '01-01-2011' and '05-31-2013';
CREATE OR REPLACE VIEW aim4.month6_2013_chd_clinvs as 
 select *, least(enddate2, '06-30-2013'::date) as enddate_trunc from aim4.chd_overall_cast where startdate between '01-01-2011' and '06-30-2013';
CREATE OR REPLACE VIEW aim4.month7_2013_chd_clinvs as 
 select *, least(enddate2, '07-31-2011'::date) as enddate_trunc from aim4.chd_overall_cast where startdate between '01-01-2011' and '07-31-2013';
CREATE OR REPLACE VIEW aim4.month8_2013_chd_clinvs as 
 select *, least(enddate2, '08-31-2013'::date) as enddate_trunc from aim4.chd_overall_cast where startdate between '01-01-2011' and '08-31-2013';
CREATE OR REPLACE VIEW aim4.month9_2013_chd_clinvs as 
 select *, least(enddate2, '09-30-2013'::date) as enddate_trunc from aim4.chd_overall_cast where startdate between '01-01-2011' and '09-30-2013';
CREATE OR REPLACE VIEW aim4.month10_2013_chd_clinvs as 
 select *, least(enddate2, '10-31-2013'::date) as enddate_trunc from aim4.chd_overall_cast where startdate between '01-01-2011' and '10-31-2013';
CREATE OR REPLACE VIEW aim4.month11_2013_chd_clinvs as 
 select *, least(enddate2, '11-30-2013'::date) as enddate_trunc from aim4.chd_overall_cast where startdate between '01-01-2011' and '11-30-2013';
CREATE OR REPLACE VIEW aim4.month12_2013_chd_clinvs as 
 select *, least(enddate2, '12-31-2013'::date) as enddate_trunc from aim4.chd_overall_cast where startdate between '01-01-2011' and '12-31-2013';

