
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


-- _clinvs contains clinical data from 1-1-2016 to end of fourth incremental load

drop view if exists aim4.year_2016_clinvs;
drop view if exists aim4.quarter1_2017_clinvs;
drop view if exists aim4.quarter2_2017_clinvs;
drop view if exists aim4.quarter3_2017_clinvs;
CREATE OR REPLACE VIEW aim4.year_2016_clinvs as 
 select *, least(encounter_date, '12-31-2016'::date) as enddate_trunc from aim4.raw_encounter  where encounter_date between '01-01-2016' and '12-31-2016';
CREATE OR REPLACE VIEW aim4.quarter1_2017_clinvs as 
 select *, least(encounter_date, '03-31-2017'::date) as enddate_trunc from aim4.raw_encounter where encounter_date between '01-01-2017' and '3-31-2017';
CREATE OR REPLACE VIEW aim4.quarter2_2017_clinvs as 
 select *, least(encounter_date, '06-30-2017'::date) as enddate_trunc from aim4.raw_encounter where encounter_date between  '01-01-2017' and '06-30-2017';
CREATE OR REPLACE VIEW aim4.quarter3_2017_clinvs as 
 select *, least(encounter_date, '09-30-2017'::date) as enddate_trunc from aim4.raw_encounter where encounter_date between '01-01-2017' and '09-30-2017';
