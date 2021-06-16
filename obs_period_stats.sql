with nid_sid as (
     select distinct ni.run_id, ni.network_id as nid, rp.study_id as sid
        from aim4.network_id ni join aim4.merged_source ms on ni.uid=ms.uid
        join tz.raw_person rp on ms.id::int = rp.study_id 
)
, sid_groups as (
    -- counts of studyids per NID, partion() needed to keep SID
        select run_id, nid, sid, count(sid) over (partition by run_id,nid) as cardinality
        from nid_sid
)    
-- Filter out SIDs that do not participate in at least one record linakage (Count(SID>1) per NID)
-- All SIDs seen from run <= runs_to_analyze
, linked_sids_run as (
        select :run_to_analyze as run_id, nid, sid, cardinality
        from sid_groups
        where cardinality > 1 and run_id <= :run_to_analyze
)
    -- Last run for every SID to local last NID
    , lastrun as (
       select sid, max(run_id) as last_run
       from linked_sids_run
       group by sid 
    )
    -- Use the nid from the last run (based on technical note)
    , linked_sids_lastrun as (
       select lsr.run_id, lr.last_run, lsr.sid, lsr.nid as last_nid, cardinality 
       from linked_sids_run lsr join lastrun lr on (lsr.sid = lr.sid and lsr.run_id = lr.last_run)
 -- last_run isn't used anywhere after this query
 -- Add clinical vars using cumulative_to_use clinical data
    )    
   , enc_key as (
    	select row_number() over() as encid, c.* from :cumulative_to_use c
    )
    , clinvs_lastrun as (
        select distinct run_id, last_nid, ek.study_id as sid, encid, startdate, enddate, enddate2, enddate_trunc, encountertype as enc_type
            , providertype as prov_type, heightinches as hgt, weightpounds as wgt, siteid
        from linked_sids_lastrun lsr join enc_key ek on (lsr.sid = ek.study_id)
    )    
-- obs period = min/max date in months using stackoverflow formula for #months.
-- obs_period_nid are observation periods from clinical data based on NID
-- obs_period_sid are observation periods from raw_persons based in STUDY_ID
-- enddate_trunc is either true enddate (if < incremental load enddate), incremental load enddate (if > incremental load enddate) or startdate (enddate is null)
, obs_period_sid as (
	select 'person' as type, sid as ID, min(startdate) as min_startdate, max(enddate_trunc) as max_enddate
    , EXTRACT(year FROM age(max(enddate_trunc),min(startdate)))*12 + EXTRACT(month FROM age(max(enddate_trunc),min(startdate))) + 1 as obs_months
	from clinvs_lastrun
	group by type, sid
)
, obs_period_nid as (
	select 'network' as type, last_nid as ID, min(startdate) as min_startdate, max(enddate_trunc) as max_enddate
    , EXTRACT(year FROM age(max(enddate_trunc),min(startdate)))*12 + EXTRACT(month FROM age(max(enddate_trunc),min(startdate))) + 1 as obs_months
	from clinvs_lastrun
	group by type, last_nid
)
select startdate, enddate, enddate2, enddate_trunc from clinvs_lastrun where enddate is not null and enddate_trunc < enddate


select startdate, enddate from aim4.chd_overall_cast where enddate is not null and enddate < startdate

	
	
	

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