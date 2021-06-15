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
-- All UIDs seen from runs <= {run_to_analyze}
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
, linked_clinical_dates_lastrun as (
	select distinct lur.run_to_analyze, lur.last_network_id, ms.uid as uid, ms.id as study_id
	, c.startdate, c.enddate_trunc
	from linked_uids_lastrun lur join aim4.merged_source ms on (lur.uid = ms.uid)
 	join :cumulative_to_use c on (ms.id = c.study_id)
)
-- obs period = min/max date in months using stackoverflow formula for #months.
-- obs_period_nid are observation periods from clinical data based on NID
-- obs_period_sid are observation periods from raw_persons based in STUDY_ID
, obs_period_sid as (
	select 'Person/SID' as type, study_id as ID, min(startdate) as min_startdate, max(enddate_trunc) as max_enddate
    , EXTRACT(year FROM age(max(enddate_trunc),min(startdate)))*12 + EXTRACT(month FROM age(max(enddate_trunc),min(startdate))) + 1 as obs_months
	from linked_clinical_dates_lastrun
	group by type, study_id
)
, obs_period_nid as (
	select 'Network/NID' as type, last_network_id as ID, min(startdate) as min_startdate, max(enddate_trunc) as max_enddate
    , EXTRACT(year FROM age(max(enddate_trunc),min(startdate)))*12 + EXTRACT(month FROM age(max(enddate_trunc),min(startdate))) + 1 as obs_months
	from linked_clinical_dates_lastrun
	group by type, last_network_id
)
, obs_period_stats as (
select type
	, min(min_startdate) as min_startdate
	, max(max_enddate) as max_enddate
	, min(obs_months) as min_obs_months
	, max(obs_months) as max_obs_months
	, avg(obs_months)::numeric(7,1) as avg_obs_months
	from obs_period_sid
	group by type
UNION ALL
select type
	, min(min_startdate)
	  , max(max_enddate)
	  , min(obs_months)
	  , max(obs_months)
	  , avg(obs_months)::numeric(7,1)
	  from obs_period_nid
	  group by type
)
select type, min_startdate, max_enddate, min_obs_months, max_obs_months, avg_obs_months from obs_period_stats;