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
    -- Use the network_id from the last run (based on technical note)
    , linked_uids_lastrun as (
       select lur.run_to_analyze as run_to_analyze, last_run, lur.uid, lur.network_id as last_network_id, cardinality 
       from linked_uids_run lur join lastrun lr on (lur.uid = lr.uid and lur.run_id = lr.last_run)
    -- Add linkage and clinical vars to UIDs in this run using run cummulative data set
    )
    , lvs_lastrun as (
        select distinct run_to_analyze, last_network_id, lur.uid, study_id,first_name, last_name, gender, dob, ssn, address_line1 as address, city, state, zip, prim_phone as phone, ssn4
        from linked_uids_lastrun lur join aim4.merged_source ms on (lur.uid=ms.uid)
        join aim4.raw_person rp on ms.id=rp.study_id
     )
    , lvs_by_nid_rows as (
        select 'linked' as type, last_network_id
            , case when count(first_name)=0 then 0 else 1 end as fn_by_nid_present_flag
            , case when count(last_name)=0 then 0 else 1 end as ln_by_nid_present_flag
            , case when count(gender)=0 then 0 else 1 end as gender_by_nid_present_flag
            , case when count(dob)=0 then 0 else 1 end as dob_by_nid_present_flag
            , case when count(ssn) =0 then 0 else 1 end as ssn_by_nid_present_flag
            , case when count(address)=0 then 0 else 1 end as address_by_nid_present_flag
            , case when count(city)=0 then 0 else 1 end as city_by_nid_present_flag
            , case when count(state)=0 then 0 else 1 end as state_by_nid_present_flag
            , case when count(zip)= 0 then 0 else 1 end as zip_by_nid_present_flag
            , case when count(phone)=0 then 0 else 1 end as phone_by_nid_present_flag
            , case when count(ssn4)=0 then 0 else 1 end as ssn4_by_nid_present_flag
        from lvs_lastrun
        group by last_network_id
    )
    , lvs_by_nid_counts as (
        select type
            , count(last_network_id) as denominator
            , sum(fn_by_nid_present_flag) as n_fn
            , sum(ln_by_nid_present_flag) as n_ln
            , sum(gender_by_nid_present_flag) as n_gender
            , sum(dob_by_nid_present_flag) as n_dob
            , sum(ssn_by_nid_present_flag) as n_ssn
            , sum(address_by_nid_present_flag) as n_address
            , sum(city_by_nid_present_flag) as n_city
            , sum(state_by_nid_present_flag) as n_state
            , sum(zip_by_nid_present_flag) as n_zip
            , sum(phone_by_nid_present_flag) as n_phone
            , sum(ssn4_by_nid_present_flag) as n_ssn4
        from lvs_by_nid_rows
        group by type
    )
    , lvs_by_uid_counts as (
    select 'unlinked' as type
           , count(uid) as denominator
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
) 
,lvcounts as (
    select denominator, n_fn, n_ln, n_gender, n_dob, n_ssn, n_address, n_city, n_state, n_zip, n_phone, n_ssn4 from lvs_by_uid_counts
    union all
    select denominator, n_fn, n_ln, n_gender, n_dob, n_ssn, n_address, n_city, n_state, n_zip, n_phone, n_ssn4  from lvs_by_nid_counts
)
select denominator, n_fn, n_ln, n_gender, n_dob, n_ssn, n_address, n_city, n_state, n_zip, n_phone, n_ssn4  from lvcounts;
