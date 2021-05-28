with uid_groups as (
-- counts of UIDS per NID, partion() needed to keep UID
	select run_id, network_id, uid, count(uid) over (partition by run_id,network_id) as cardinality
	from aim4.network_id )
-- Filter out UIDs that do not participate in at least one record linakage (Count(uid>1) per NID)
, linked_uids as (
    select run_id, network_id, uid, cardinality
    from uid_groups
    where cardinality > 1
)
-- All UIDs seen from runs <= :run_to_analyze
, linked_uids_run as (
    select :run_to_analyze as run_to_analyze, run_id, network_id, uid, cardinality 
    from linked_uids
    where run_id <= :run_to_analyze
)
-- Last run for every UID to local last NID
, lastrun as (
   select uid, max(run_id) as last_run
   from linked_uids_run
   group by uid 
)
-- Use the network_id from the last run (based on conversation with Toan about using last NID NID)
, linked_uids_lastrun as (
   select lur.run_to_analyze as run_to_analyze, last_run, lur.uid, lur.network_id as last_network_id, cardinality 
   from linked_uids_run lur join lastrun lr on (lur.uid = lr.uid and lur.run_id = lr.last_run)
)
-- Add linkage and clinical vars to UIDs in this run using run cummulative data set
, linked_clinical_lastrun as (
	select distinct run_to_analyze, last_network_id, ms.uid as uid, ms.id, mrn, d_source, first_name, last_name,sex, dob,ssn,address, city, state,zip, phone,ssn4
	, startdate, enddate, heightinches, weightpounds, siteid
	from linked_uids_lastrun lur join aim4.merged_source ms on (lur.uid = ms.uid)
 	join :cummulative_to_use c on (ms.id = c.study_id)
)
-- obs period = min/max date in months using stackoverflow formula for #months.
-- ATTN: max_enddate set to max(startdate)
, obs_period_uid as (
	select 'Unlinked' as type, uid as uid, min(startdate) as min_startdate, max(startdate) as max_enddate
    , EXTRACT(year FROM age(max(startdate),min(startdate)))*12 + EXTRACT(month FROM age(max(startdate),min(startdate))) + 1 as obs_months
	from linked_clinical_lastrun
	group by uid)
, obs_period_nid as (
	select 'Linked' as type, last_network_id, min(startdate) as min_startdate, max(startdate) as max_enddate
    , EXTRACT(year FROM age(max(startdate),min(startdate)))*12 + EXTRACT(month FROM age(max(startdate),min(startdate))) + 1 as obs_months
	from linked_clinical_lastrun
	group by last_network_id
)
/*
, obs_period_stats as (
select 'unlinked' as type
	, min(min_startdate) as min_startdate
	, max(max_enddate) as max_enddate
	, min(obs_months) as min_obs_months
	, max(obs_months) as max_obs_months
	, avg(obs_months)::numeric(7,1) as avg_obs_months
		  from obs_period_uid
UNION ALL
select 'linked'
	, min(min_startdate)
	  , max(max_enddate)
	  , min(obs_months)
	  , max(obs_months)
	  , avg(obs_months)::numeric(7,1)
	  from obs_period_nid
)
*/
-- Histogram viewof observation periods summarized above
/*
, obs_period_histo as (
select 'Unlinked' as type
	, obs_months, count(uid) as num_IDs
	from obs_period_uid  -- THIS IS UNLINKED data
	group by obs_months
UNION ALL
select 'Linked'
	, obs_months, count(last_network_id)
	from obs_period_nid -- THIS IS LINKED data
	group by obs_months
Order by type asc, obs_months asc
)
select * from obs_period_histo
*/
select * from obs_period_uid order by min_startdate asc

