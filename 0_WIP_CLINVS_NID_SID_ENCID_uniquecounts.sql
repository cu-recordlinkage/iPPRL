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
	, clinvs_counts_by_nid_sid_encid as (
    select run_id, encid as id, 'encounter' as type, 'enc_id' as id_field
          , count(*) as denominator
          , count(distinct startdate) as n_startdt
          , count(distinct enddate) as n_enddt
          , count(distinct enc_type) as n_enc_type
          , count(distinct prov_type) as n_prov_type
          , count(distinct hgt) as n_hgt
          , count(distinct wgt) as n_wgt
          , count(distinct siteid) as n_siteid
    from clinvs_lastrun group by run_id, encid
    union all
    select run_id, sid as id, 'patient' as type, 'sid' as id_field
          , count(*) as denominator
          , count(distinct startdate) as n_startdt
          , count(distinct enddate) as n_enddt
          , count(distinct enc_type) as n_enc_type
          , count(distinct prov_type) as n_prov_type
          , count(distinct hgt) as n_hgt
          , count(distinct wgt) as n_wgt
          , count(distinct siteid) as n_siteid
    from clinvs_lastrun group by run_id, sid
    union all
   select run_id, last_nid as id, 'network' as type, 'nid' as id_field
          , count(*) as denominator
          , count(distinct startdate) as n_startdt
          , count(distinct enddate) as n_enddt
          , count(distinct enc_type) as n_enc_type
          , count(distinct prov_type) as n_prov_type
          , count(distinct hgt) as n_hgt
          , count(distinct wgt) as n_wgt
          , count(distinct siteid) as n_siteid
    from clinvs_lastrun group by run_id, last_nid
    order by type desc
    )
    select *
    from clinvs_counts_by_nid_sid_encid