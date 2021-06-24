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
 -- Add linkage variables stored in TZ.RAW_PERSON
    )    
, lvs_lastrun as (
        select distinct run_id, last_nid, study_id as sid
        ,first_name, last_name, gender, dob, ssn, address_line1 as address, city, state, zip, prim_phone as phone, ssn4
        from linked_sids_lastrun lsr join tz.raw_person rp on (lsr.sid=rp.study_id)      
    )    
    , lvs_counts_by_sid as (
        select run_id, sid as id, 'patient' as type, 'sid' as id_field
            , count(*) as denominator
            , count(first_name) as n_fn
            , count(last_name) as n_ln
            , count(gender) as n_gender
            , count(dob) as n_dob
            , count(ssn) as n_ssn
            , count(address) as n_address
            , count(city) as n_city
            , count(state) as n_state
            , count(zip) as n_zip
            , count(phone) as n_phone
            , count(ssn4) as n_ssn4
        from lvs_lastrun
        group by run_id, sid
    )
    -- Data Counts by nid (linked)
    , lvs_counts_by_nid as (
        select run_id, last_nid as id, 'network' as type, 'nid' as id_field
            , count(*) as denominator
            , count(first_name) as n_fn
            , count(last_name) as n_ln
            , count(gender) as n_gender
            , count(dob) as n_dob
            , count(ssn) as n_ssn
            , count(address) as n_address
            , count(city) as n_city
            , count(state) as n_state
            , count(zip) as n_zip
            , count(phone) as n_phone
            , count(ssn4) as n_ssn4
        from lvs_lastrun
        group by run_id, last_nid
    )	
    , lvs_counts_by_nid_sid AS (
    	SELECT * FROM lvs_counts_by_sid
    	UNION ALL
    	SELECT * FROM lvs_counts_by_nid
    )
    SELECT  * FROM lvs_counts_by_nid_sid