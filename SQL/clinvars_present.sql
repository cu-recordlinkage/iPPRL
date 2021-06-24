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
, linked_sids as (
        select run_id, nid, sid, cardinality
        from sid_groups
        where cardinality > 1
) 
    -- All SIDs seen from runs <= {run_to_analyze}
    , linked_sids_run as (
        select :run_to_analyze as run_to_analyze, run_id, nid, sid, cardinality 
        from linked_sids
        where run_id <= :run_to_analyze
    )
    -- Last run for every SID to local last NID
    , lastrun as (
       select sid, max(run_id) as last_run
       from linked_sids_run
       group by sid 
    )
    -- Use the network_id from the last run (based on technical note)
    , linked_sids_lastrun as (
       select lsr.run_to_analyze as run_to_analyze, last_run, lsr.sid, lsr.nid as last_nid, cardinality 
       from linked_sids_run lsr join lastrun lr on (lsr.sid = lr.sid and lsr.run_id = lr.last_run)
    -- Add linkage and clinical vars to UIDs in this run using run cummulative data set
    )
    -- Link UIDs to ClinVs
    , clinvs_by_sid_lastrun as (
        select distinct run_to_analyze, last_nid, c.study_id as sid, startdate, enddate, encountertype as enc_type
            , providertype as prov_type, heightinches as hgt, weightpounds as wgt, siteid
        from linked_sids_lastrun lsr join :cumulative_to_use c on (lsr.sid = c.study_id)
    )
    , clinvs_by_nid_rows as (
        select last_nid
            , case when count(startdate)=0 then 0 else 1 end as startdt_by_nid_present_flag
            , case when count(enddate)=0 then 0 else 1 end as enddt_by_nid_present_flag
            , case when count(enc_type)=0 then 0 else 1 end as enc_type_by_nid_present_flag
            , case when count(prov_type)=0 then 0 else 1 end as prov_type_by_nid_present_flag
            , case when count(hgt) =0 then 0 else 1 end as hgt_by_nid_present_flag
            , case when count(wgt)=0 then 0 else 1 end as wgt_by_nid_present_flag
            , case when count(siteid)=0 then 0 else 1 end as siteid_by_nid_present_flag
        from clinvs_by_sid_lastrun
        group by last_nid
    )
    , clinvs_by_nid_counts as (
       select 'linked' as type,
           count(last_nid) as denominator
           , sum(startdt_by_nid_present_flag) as n_startdt
           , sum(enddt_by_nid_present_flag) as n_enddt
           , sum(enc_type_by_nid_present_flag) as n_enc_type
           , sum(prov_type_by_nid_present_flag) as n_prov_type
           , sum(hgt_by_nid_present_flag) as n_hgt
           , sum(wgt_by_nid_present_flag) as n_wgt
           , sum(siteid_by_nid_present_flag) as n_siteid
           from clinvs_by_nid_rows
    )
    , clinvs_by_sid_counts as (
        select 'unlinked' as type
           , count(sid) as denominator
           , count(startdate) as n_startdt
           , count(enddate) as n_enddt
           , count(enc_type) as n_enc_type
           , count(prov_type) as n_prov_type
           , count(hgt) as n_hgt
           , count(wgt) as n_wgt
           , count(siteid) as n_siteid
    from clinvs_by_sid_lastrun
    )
    , clinvcounts as (
        select type, denominator, n_startdt, n_enddt, n_enc_type, n_prov_type, n_hgt, n_wgt, n_siteid 
        from clinvs_by_sid_counts
        union all
        select type, denominator, n_startdt, n_enddt, n_enc_type, n_prov_type, n_hgt, n_wgt, n_siteid 
        from clinvs_by_nid_counts
    )
    select type, denominator, n_startdt, n_enddt, n_enc_type, n_prov_type, n_hgt, n_wgt, n_siteid 
    from clinvcounts;
    