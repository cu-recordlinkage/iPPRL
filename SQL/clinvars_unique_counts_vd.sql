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
    -- Use the nid from the last run (based on technical note)
    , linked_sids_lastrun as (
       select lsr.run_to_analyze as run_to_analyze, lr.last_run, lsr.sid, lsr.nid as last_nid, cardinality 
       from linked_sids_run lsr join lastrun lr on (lsr.sid = lr.sid and lsr.run_id = lr.last_run)
    -- Add linkage vars using RAW_PERSON
    )
    , clinvs_lastrun as (
        select distinct run_to_analyze, last_nid, c.study_id as sid, startdate, enddate, encountertype as enc_type
            , providertype as prov_type, heightinches as hgt, weightpounds as wgt, siteid
        from linked_sids_lastrun lsr join :cumulative_to_use c on (lsr.sid = c.study_id)
    )
    -- Data count by sid (unlinked)  This is a count of missingness
    , clinvs_counts_by_sid as (
        select run_to_analyze, sid
            , count(distinct startdate) as n_startdt
            , count(distinct enddate) as n_enddt
            , count(distinct enc_type) as n_enc_type
            , count(distinct prov_type) as n_prov_type
            , count(distinct hgt) as n_hgt
            , count(distinct wgt) as n_wgt
            , count(distinct siteid) as n_siteid
        from clinvs_lastrun
        group by run_to_analyze, sid
    )
    -- Data Counts by nid (linked)
    , clinvs_counts_by_nid as (
        select run_to_analyze, last_nid
            , count(distinct startdate) as n_startdt
            , count(distinct enddate) as n_enddt
            , count(distinct enc_type) as n_enc_type
            , count(distinct prov_type) as n_prov_type
            , count(distinct hgt) as n_hgt
            , count(distinct wgt) as n_wgt
            , count(distinct siteid) as n_siteid
        from clinvs_lastrun
        group by run_to_analyze, last_nid
    )
    -- Clinical Value Density (clin_vd)
    , clin_value_density as (
    select 'Unlinked' as type
        , run_to_analyze
        , round(avg(n_startdt),2) as startdt_vd
        , round(avg(n_enddt),2) as enddt_vd
        , round(avg(n_enc_type),2) as enc_type_vd
        , round(avg(n_prov_type),2) as prov_type_vd
        , round(avg(n_hgt),2) as hgt_vd
        , round(avg(n_wgt),2) as wgt_vd
        , round(avg(n_siteid),2) as siteid_vd
    from clinvs_counts_by_sid 
    group by type, run_to_analyze
    union 
    select 'Linked' as type
        , run_to_analyze
        , round(avg(n_startdt),2) as startdt_vd
        , round(avg(n_enddt),2) as enddt_vd
        , round(avg(n_enc_type),2) as enc_type_vd
        , round(avg(n_prov_type),2) as prov_type_vd
        , round(avg(n_hgt),2) as hgt_vd
        , round(avg(n_wgt),2) as wgt_vd
        , round(avg(n_siteid),2) as siteid_vd
    from clinvs_counts_by_nid 
    group by type, run_to_analyze
    )
    select type
          , startdt_vd
          , enddt_vd
          , enc_type_vd
          , prov_type_vd
          , hgt_vd
          , wgt_vd
          , siteid_vd
    from clin_value_density
    order by type desc;



